"""Fixes from the final live champion run.

Covers: masking functions keep no SP secret in Terraform state, always use
the current credentials from the layer's auth.auto.tfvars (failing early when
they're unusable), and existing state migrates without dropping a function;
the LLM spinner stays quiet in logs.
"""

import io
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import deploy_masking_functions as dmf
import generate_abac

SHARED = Path(__file__).parents[1]
MODULE_TF = SHARED / "modules/data_access/main.tf"
ROOT_TF = SHARED / "roots/data_access/main.tf"
HOST = "https://dbc-example.cloud.databricks.com"
AUTH_TEXT = (
    f'databricks_workspace_host = "{HOST}"\n'
    'databricks_client_id = "sp-client"\n'
    'databricks_client_secret = "current-secret"\n'
)


def _real_layout(tmp_path: Path, auth_text: str = AUTH_TEXT) -> Path:
    """Build envs/dev as make setup does; return the data_access env_dir."""
    env = tmp_path / "aws" / "envs" / "dev"
    data_access = env / "data_access"
    data_access.mkdir(parents=True)
    (env / "auth.auto.tfvars").write_text(auth_text)
    (data_access / "auth.auto.tfvars").symlink_to("../auth.auto.tfvars")
    (data_access / "masking_functions.sql").write_text("-- sql\n")
    (env / "generated").mkdir()
    (env / "generated" / "masking_functions.sql").write_text("-- draft\n")
    return data_access


def _stale_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", HOST)
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "sp-client")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "stale-secret")


def _resource_block(tf: str, header: str) -> str:
    start = tf.index(header)
    return tf[start: tf.index("\n}\n", start) + 3]


# ── credentials come from the layer's auth file ──────────────────────────────


def test_root_passes_the_layer_auth_file_beside_the_masking_sql():
    root = ROOT_TF.read_text()
    assert 'masking_sql_file                = "${var.env_dir}/masking_functions.sql"' in root
    assert 'auth_file                       = "${var.env_dir}/auth.auto.tfvars"' in root


def test_real_layout_loads_current_credentials(tmp_path, monkeypatch):
    _stale_env(monkeypatch)
    env_dir = _real_layout(tmp_path)  # what the root passes as var.env_dir

    dmf.load_credentials(f"{env_dir}/auth.auto.tfvars", HOST + "/")

    assert os.environ["DATABRICKS_CLIENT_SECRET"] == "current-secret"
    assert os.environ["DATABRICKS_CLIENT_ID"] == "sp-client"
    assert os.environ["DATABRICKS_HOST"] == HOST


@pytest.mark.parametrize(
    "auth_text, expected",
    [
        (None, "credentials file not found"),
        ("databricks_client_id = \n", "could not parse"),
        (AUTH_TEXT.replace('databricks_client_secret = "current-secret"\n', ""),
         "no value for databricks_client_secret"),
        (AUTH_TEXT.replace('"current-secret"', '""'), "no value for databricks_client_secret"),
        (AUTH_TEXT.replace('databricks_client_id = "sp-client"\n', ""),
         "no value for databricks_client_id"),
        (AUTH_TEXT.replace(f'databricks_workspace_host = "{HOST}"\n', ""),
         "no value for databricks_workspace_host"),
        (AUTH_TEXT.replace(HOST, "https://other.cloud.databricks.com"),
         "sets databricks_workspace_host = https://other.cloud.databricks.com"),
    ],
    ids=["missing", "malformed", "no-secret", "empty-secret", "no-client-id",
         "no-host", "host-mismatch"],
)
def test_unusable_auth_file_fails_early_without_stale_fallback(
    tmp_path, monkeypatch, auth_text, expected
):
    _stale_env(monkeypatch)
    env_dir = _real_layout(tmp_path)
    auth_file = env_dir / "auth.auto.tfvars"
    if auth_text is None:
        (env_dir.parent / "auth.auto.tfvars").unlink()
    else:
        (env_dir.parent / "auth.auto.tfvars").write_text(auth_text)

    with pytest.raises(SystemExit) as exc:
        dmf.load_credentials(str(auth_file), HOST)

    message = str(exc.value)
    assert expected in message
    assert str(auth_file) in message
    assert os.environ["DATABRICKS_CLIENT_SECRET"] == "stale-secret"


@pytest.mark.parametrize("drop", [False, True])
def test_main_requires_auth_file_and_loads_it_before_any_statement(
    tmp_path, monkeypatch, drop
):
    _stale_env(monkeypatch)
    env_dir = _real_layout(tmp_path)
    seen = {}
    monkeypatch.setattr(dmf, "deploy", lambda *a: seen.setdefault("secret", os.environ["DATABRICKS_CLIENT_SECRET"]))
    monkeypatch.setattr(dmf, "drop", lambda *a: seen.setdefault("secret", os.environ["DATABRICKS_CLIENT_SECRET"]))
    base = ["deploy_masking_functions.py", "--sql-file", str(env_dir / "masking_functions.sql"),
            "--warehouse-id", "wh"] + (["--drop"] if drop else [])

    monkeypatch.setattr(sys, "argv", base)
    with pytest.raises(SystemExit):
        dmf.main()  # argparse: --auth-file/--host are required
    assert not seen

    monkeypatch.setattr(sys, "argv", base + ["--auth-file", str(env_dir / "auth.auto.tfvars"),
                                             "--host", HOST])
    dmf.main()
    assert seen == {"secret": "current-secret"}


# ── Terraform: no secret in state, identity still replaces ───────────────────


def test_masking_resource_keeps_no_secret_and_ignores_nothing():
    block = _resource_block(MODULE_TF.read_text(), 'resource "terraform_data" "masking_functions" {')
    triggers = re.search(r"triggers_replace = \{(.*?)\n  \}", block, re.S).group(1)
    keys = set(re.findall(r"^\s*(\w+)\s*=", triggers, re.M))
    assert keys == {"sql_hash", "sql_file", "script", "auth_file", "warehouse_id", "host", "client_id"}
    assert "secret" not in block
    assert "ignore_changes" not in block
    assert block.count("--auth-file ${self.triggers_replace.auth_file}") == 2


def test_old_null_resource_is_forgotten_not_destroyed():
    tf = MODULE_TF.read_text()
    assert 'resource "null_resource" "deploy_masking_functions"' not in tf
    removed = re.search(r"removed \{(.*?)\n\}", tf, re.S).group(1)
    assert "from = null_resource.deploy_masking_functions" in removed
    assert re.search(r"lifecycle \{\s*destroy = false\s*\}", removed)
    minor = re.search(r'required_version = ">= 1\.(\d+)"', ROOT_TF.read_text()).group(1)
    assert int(minor) >= 7  # removed blocks need Terraform 1.7


# ── State transition: real blocks against a pre-fix state ───────────────────

STUB = """\
import json, os, sys
with open(os.environ["STUB_LOG"], "a") as f:
    f.write(json.dumps(sys.argv[1:]) + "\\n")
"""

OLD_STATE = {
    "version": 4,
    "terraform_version": "1.11.4",
    "serial": 1,
    "lineage": "00000000-0000-0000-0000-000000000000",
    "outputs": {},
    "resources": [{
        "mode": "managed",
        "type": "null_resource",
        "name": "deploy_masking_functions",
        "provider": 'provider["registry.terraform.io/hashicorp/null"]',
        "instances": [{
            "schema_version": 0,
            "attributes": {
                "id": "8313817037003248902",
                "triggers": {
                    "client_id": "sp-client",
                    "client_secret": "OLD-REVOKED-SECRET",
                    "host": HOST,
                    "script": "SCRIPT",
                    "sql_file": "SQL",
                    "sql_hash": "HASH",
                    "warehouse_id": "wh",
                },
            },
            "sensitive_attributes": [],
        }],
    }],
    "check_results": None,
}


def _fixture_root(tmp_path: Path) -> tuple[Path, Path, dict]:
    """A root holding the module's real masking blocks, fed fixture values."""
    tf = MODULE_TF.read_text()
    block = _resource_block(tf, 'resource "terraform_data" "masking_functions" {')
    block = re.sub(r"\n  depends_on = \[.*?\n  \]\n", "\n", block, flags=re.S)
    removed = re.search(r"removed \{.*?\n\}\n", tf, re.S).group(0)
    root = tmp_path / "root"
    root.mkdir()
    env_dir = _real_layout(tmp_path)
    stub = tmp_path / "stub.py"
    stub.write_text(STUB)
    (root / "main.tf").write_text(
        'terraform {\n  required_providers {\n'
        '    null = { source = "hashicorp/null", version = "~> 3.2" }\n  }\n}\n'
        'variable "masking_sql_file" {}\nvariable "deploy_masking_script" {}\n'
        'variable "auth_file" {}\nvariable "databricks_workspace_host" {}\n'
        'variable "databricks_client_id" {}\n'
        'locals {\n  effective_warehouse_id = "wh"\n}\n'
        + block + "\n" + removed
    )
    sql_file = env_dir / "masking_functions.sql"
    state = json.loads(json.dumps(OLD_STATE).replace('"SCRIPT"', json.dumps(str(stub)))
                       .replace('"SQL"', json.dumps(str(sql_file))))
    (root / "terraform.tfstate").write_text(json.dumps(state))
    tfvars = {
        "masking_sql_file": str(sql_file),
        "deploy_masking_script": str(stub),
        "auth_file": f"{env_dir}/auth.auto.tfvars",
        "databricks_workspace_host": HOST,
        "databricks_client_id": "sp-client",
    }
    return root, env_dir, tfvars


def _tf(root: Path, *args: str, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(["terraform", *args], cwd=root, text=True,
                          capture_output=True, env=env, timeout=300)


def _plan_actions(root: Path, tfvars: dict, env: dict) -> dict:
    var_args = [f"-var={k}={v}" for k, v in tfvars.items()]
    plan = _tf(root, "plan", "-input=false", "-out=plan.bin", *var_args, env=env)
    assert plan.returncode == 0, plan.stdout + plan.stderr
    shown = _tf(root, "show", "-json", "plan.bin", env=env)
    assert shown.returncode == 0, shown.stderr
    return {rc["address"]: rc["change"]["actions"]
            for rc in json.loads(shown.stdout).get("resource_changes", [])
            if rc["change"]["actions"] != ["no-op"]}


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_existing_state_migrates_without_dropping_and_sheds_the_secret(tmp_path):
    root, env_dir, tfvars = _fixture_root(tmp_path)
    log = tmp_path / "stub.log"
    env = {**os.environ, "STUB_LOG": str(log), "TF_IN_AUTOMATION": "1"}
    init = _tf(root, "init", "-input=false", env=env)
    if init.returncode != 0:
        pytest.skip(f"terraform init failed (offline?): {init.stderr[-300:]}")

    actions = _plan_actions(root, tfvars, env)
    assert actions == {
        "null_resource.deploy_masking_functions": ["forget"],
        "terraform_data.masking_functions": ["create"],
    }

    var_args = [f"-var={k}={v}" for k, v in tfvars.items()]
    apply = _tf(root, "apply", "-input=false", "-auto-approve", *var_args, env=env)
    assert apply.returncode == 0, apply.stdout + apply.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    assert len(calls) == 1 and "--drop" not in calls[0]  # create only, nothing dropped
    assert calls[0][calls[0].index("--auth-file") + 1] == f"{env_dir}/auth.auto.tfvars"
    state = (root / "terraform.tfstate").read_text()
    assert "OLD-REVOKED-SECRET" not in state and "current-secret" not in state

    # Rotating the secret is invisible to Terraform: nothing to replace.
    (env_dir.parent / "auth.auto.tfvars").write_text(AUTH_TEXT.replace("current-secret", "rotated"))
    assert _plan_actions(root, tfvars, env) == {}

    # A different SP identity still replaces the functions.
    actions = _plan_actions(root, {**tfvars, "databricks_client_id": "other-sp"}, env)
    assert actions == {"terraform_data.masking_functions": ["delete", "create"]}


# ── spinner ──────────────────────────────────────────────────────────────────


def test_spinner_writes_one_line_when_not_a_tty(monkeypatch):
    buf = io.StringIO()  # isatty() is False, like a CI log or `make ... > log`
    monkeypatch.setattr(generate_abac.sys, "stderr", buf)
    with generate_abac.Spinner("Calling LLM"):
        generate_abac.time.sleep(0.25)

    out = buf.getvalue()
    assert out.count("Calling LLM") == 1
    assert "\r" not in out
    assert not any(frame in out for frame in generate_abac.Spinner.FRAMES)
