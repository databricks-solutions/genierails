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


def _skip_if_providers_unavailable(init: subprocess.CompletedProcess) -> None:
    if init.returncode == 0:
        return
    if "Failed to query available provider packages" in init.stderr or "could not connect" in init.stderr:
        pytest.skip("hashicorp/null provider not downloadable (offline)")
    raise AssertionError(init.stdout + init.stderr)


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
    _skip_if_providers_unavailable(init)

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


# ── Genie agent: adopt instead of duplicating; secret-free migration ─────────

GENIE_SCRIPT = SHARED / "scripts/genie_space.sh"
WORKSPACE_TF = SHARED / "modules/workspace/main.tf"


def _run_create(tmp_path: Path, get_code: str, id_text: str | None = "01live\n"):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "curl.log"
    # GET /genie/spaces/<id> answers get_code; a POST would create a new agent.
    (bin_dir / "curl").write_text(f"""#!/bin/bash
echo "$*" >> {calls}
if [[ " $* " == *" POST "* ]]; then printf '{{"space_id":"01new"}}\\n200'; exit 0; fi
if [[ " $* " == *"/api/2.0/genie/spaces/"* ]]; then printf '{get_code}'; exit 0; fi
printf '{{}}\\n200'
""")
    (bin_dir / "curl").chmod(0o755)
    id_file = tmp_path / ".genie_space_id_agent"
    if id_text is not None:
        id_file.write_text(id_text)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
           "DATABRICKS_HOST": "https://ws", "DATABRICKS_TOKEN": "t",
           "GENIE_ID_FILE": str(id_file), "GENIE_TABLES_CSV": "cat.s.t",
           "GENIE_WAREHOUSE_ID": "wh", "GENIE_TITLE": "Agent"}
    result = subprocess.run(["bash", str(GENIE_SCRIPT), "create"], env=env,
                            capture_output=True, text=True, timeout=60)
    posts = [c for c in calls.read_text().splitlines() if " POST " in f" {c} "] if calls.exists() else []
    return result, posts, id_file


def test_create_adopts_the_agent_named_in_its_id_file(tmp_path):
    result, posts, id_file = _run_create(tmp_path, "200")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "adopting it, no new agent created" in result.stdout
    assert posts == []
    assert id_file.read_text().strip() == "01live"


def test_create_makes_a_new_agent_only_after_a_confirmed_404(tmp_path):
    result, posts, id_file = _run_create(tmp_path, "404")
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(posts) == 1
    assert id_file.read_text().strip() == "01new"


def test_create_refuses_to_duplicate_when_the_check_fails(tmp_path):
    result, posts, id_file = _run_create(tmp_path, "503")
    assert result.returncode != 0
    assert "not creating a duplicate" in result.stderr
    assert posts == []
    assert id_file.read_text().strip() == "01live"


def test_create_without_an_id_file_creates_as_before(tmp_path):
    result, posts, _ = _run_create(tmp_path, "200", id_text=None)
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(posts) == 1


def test_rename_safety_sees_the_migrated_genie_resource(tmp_path):
    from scripts import remap_env_config

    (tmp_path / "terraform.tfstate").write_text(json.dumps({"resources": [
        {"module": "module.workspace", "type": "terraform_data", "name": "genie_space",
         "instances": [{"index_key": "agent_prod"}]},
        {"module": "module.workspace", "type": "null_resource", "name": "genie_space_config",
         "instances": [{"index_key": "agent_prod"}]},
    ]}))
    assert remap_env_config._deployed_space_keys(str(tmp_path)) == {
        "agent_prod": ["module.workspace.terraform_data.genie_space",
                       "module.workspace.null_resource.genie_space_config"],
    }


GENIE_OLD_STATE = {
    "version": 4, "terraform_version": "1.11.4", "serial": 1,
    "lineage": "00000000-0000-0000-0000-000000000001", "outputs": {},
    "resources": [{
        "mode": "managed", "type": "null_resource", "name": "genie_space_create",
        "provider": 'provider["registry.terraform.io/hashicorp/null"]',
        "instances": [{
            "index_key": "agent", "schema_version": 0,
            "attributes": {"id": "6408157866410471460", "triggers": {
                "client_id": "sp-client", "client_secret": "OLD-REVOKED-SECRET",
                "host": HOST, "id_file": "ID_FILE", "script": "SCRIPT"}},
            "sensitive_attributes": [],
        }],
    }],
    "check_results": None,
}


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_existing_genie_state_migrates_without_trashing_the_agent(tmp_path):
    tf = WORKSPACE_TF.read_text()
    block = _resource_block(tf, 'resource "terraform_data" "genie_space" {')
    block = re.sub(r"\n  depends_on = \[.*?\n  \]\n", "\n", block, flags=re.S)
    stub = tmp_path / "stub.py"
    block = block.replace('"bash ../../scripts/genie_space.sh trash"', json.dumps(f"python3 {stub} trash"))
    removed = re.search(r"removed \{\n  from = null_resource\.genie_space_create.*?\n\}\n", tf, re.S).group(0)
    root = tmp_path / "root"
    root.mkdir()
    stub.write_text(STUB)
    prefix = tmp_path / ".genie_space_id"
    (tmp_path / ".genie_space_id_agent").write_text("01live\n")
    (root / "main.tf").write_text(
        'terraform {\n  required_providers {\n'
        '    null = { source = "hashicorp/null", version = "~> 3.2" }\n  }\n}\n'
        'variable "databricks_workspace_host" {}\nvariable "databricks_client_id" {}\n'
        'variable "databricks_client_secret" { sensitive = true }\n'
        'variable "genie_id_file_prefix" {}\nvariable "genie_script_path" {}\n'
        'locals {\n  shared_warehouse_id = "wh"\n'
        '  new_spaces = { agent = { uc_tables = ["c.s.t"], sql_warehouse_id = "", name = "Agent",'
        ' config = { title = "" } } }\n}\n'
        + block + "\n" + removed
    )
    state = json.dumps(GENIE_OLD_STATE).replace('"ID_FILE"', json.dumps(f"{prefix}_agent"))
    (root / "terraform.tfstate").write_text(state.replace('"SCRIPT"', json.dumps(str(stub))))
    tfvars = {"databricks_workspace_host": HOST, "databricks_client_id": "sp-client",
              "databricks_client_secret": "current-secret", "genie_id_file_prefix": str(prefix),
              "genie_script_path": f"python3 {stub}"}
    log = tmp_path / "stub.log"
    env = {**os.environ, "STUB_LOG": str(log), "TF_IN_AUTOMATION": "1"}
    init = _tf(root, "init", "-input=false", env=env)
    _skip_if_providers_unavailable(init)

    assert _plan_actions(root, tfvars, env) == {
        'null_resource.genie_space_create["agent"]': ["forget"],
        'terraform_data.genie_space["agent"]': ["create"],
    }
    var_args = [f"-var={k}={v}" for k, v in tfvars.items()]
    apply = _tf(root, "apply", "-input=false", "-auto-approve", *var_args, env=env)
    assert apply.returncode == 0, apply.stdout + apply.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    assert calls == [["create"]]  # create ran once (it adopts; tested above); trash never ran
    state_text = (root / "terraform.tfstate").read_text()
    assert "OLD-REVOKED-SECRET" not in state_text and "current-secret" not in state_text
    assert (tmp_path / ".genie_space_id_agent").read_text().strip() == "01live"

    # Credential rotation is a no-op; a different workspace host replaces.
    assert _plan_actions(root, {**tfvars, "databricks_client_secret": "rotated"}, env) == {}
    assert _plan_actions(root, {**tfvars, "databricks_workspace_host": "https://other"}, env) == {
        'terraform_data.genie_space["agent"]': ["delete", "create"],
    }
