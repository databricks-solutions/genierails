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
from tests.terraform_helpers import skip_if_providers_unavailable as _skip_if_providers_unavailable  # noqa: E402
from tests.terraform_helpers import tf as _tf  # noqa: E402
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
    assert block.count("--auth-file ${self.triggers_replace.auth_file}") == 1
    # Replacing it (any SQL change) must never drop the functions live policies use.
    assert "when    = destroy" not in block and "--drop" not in block


def test_functions_are_dropped_only_by_a_resource_that_never_replaces():
    block = _resource_block(MODULE_TF.read_text(), 'resource "terraform_data" "masking_functions_drop" {')
    assert "triggers_replace" not in block and "filemd5" not in block
    assert "secret" not in block
    assert block.count("--drop") == 1 and "when    = destroy" in block
    assert "--auth-file ${self.input.auth_file}" in block
    deploy = _resource_block(MODULE_TF.read_text(), 'resource "terraform_data" "masking_functions" {')
    assert "terraform_data.masking_functions_drop" in deploy  # destroyed after the deploy


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


def _masking_root(tf: str) -> str:
    """A root holding the masking blocks of module source `tf`, fed fixture values."""
    blocks = "\n".join(
        re.sub(r"\n  depends_on = \[.*?\n  \]\n", "\n", _resource_block(tf, header), flags=re.S)
        for header in ('resource "terraform_data" "masking_functions" {',
                       'resource "terraform_data" "masking_functions_drop" {')
        if header in tf)  # the drop resource only exists from PR #76 on
    removed = re.search(r"removed \{.*?\n\}\n", tf, re.S).group(0)
    return (
        'terraform {\n  required_providers {\n'
        '    null = { source = "hashicorp/null", version = "~> 3.2" }\n  }\n}\n'
        'variable "masking_sql_file" {}\nvariable "deploy_masking_script" {}\n'
        'variable "auth_file" {}\nvariable "databricks_workspace_host" {}\n'
        'variable "databricks_client_id" {}\n'
        'locals {\n  effective_warehouse_id = "wh"\n}\n'
        + blocks + "\n" + removed
    )


def _fixture_root(tmp_path: Path, tf: str | None = None, old_state: bool = True) -> tuple[Path, Path, dict]:
    """A root holding the masking blocks (the current module's unless `tf` is
    given), with the pre-terraform_data state unless old_state is False."""
    root = tmp_path / "root"
    root.mkdir()
    env_dir = _real_layout(tmp_path)
    stub = tmp_path / "stub.py"
    stub.write_text(STUB)
    (root / "main.tf").write_text(_masking_root(MODULE_TF.read_text() if tf is None else tf))
    sql_file = env_dir / "masking_functions.sql"
    if old_state:
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


# The last main before PR #76: masking_functions itself drops on destroy, so a
# SQL change (a replacement) dropped every live mask function.
PRE_DROP_SPLIT_COMMIT = "0c08fb2de41213a260699f95667434216a5dcab9"


def _pre_drop_split_module() -> str:
    """modules/data_access/main.tf as of PRE_DROP_SPLIT_COMMIT, from git history."""
    show = subprocess.run(["git", "show", f"{PRE_DROP_SPLIT_COMMIT}:shared/modules/data_access/main.tf"],
                          cwd=SHARED.parent, text=True, capture_output=True)
    if show.returncode != 0:
        message = (f"commit {PRE_DROP_SPLIT_COMMIT} is not in this clone (shallow checkout?); "
                   "fetch full history to run the upgrade proof")
        if os.environ.get("REQUIRE_TERRAFORM_TESTS") == "1":
            pytest.fail(message)
        pytest.skip(message)
    return show.stdout


def _plan_actions(root: Path, tfvars: dict, env: dict) -> dict:
    var_args = [f"-var={k}={v}" for k, v in tfvars.items()]
    plan_file = root / "plan.bin"  # embeds prior state: owner-only, then deleted
    try:
        plan = _tf(root, "plan", "-input=false", f"-out={plan_file}", *var_args, env=env)
        assert plan.returncode == 0, plan.stdout + plan.stderr
        plan_file.chmod(0o600)
        shown = _tf(root, "show", "-json", str(plan_file), env=env)
        assert shown.returncode == 0, shown.stderr
    finally:
        plan_file.unlink(missing_ok=True)
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
        "terraform_data.masking_functions_drop": ["create"],
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


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_upgraded_masking_state_survives_a_sql_change_without_dropping(tmp_path):
    """The prod risk: masking_functions created by the pre-#76 module (which
    drops on destroy) is replaced by the first SQL change after the upgrade.
    That replacement must re-run CREATE OR REPLACE without any --drop; only
    destroying the layer drops the functions, exactly once."""
    root, env_dir, tfvars = _fixture_root(tmp_path, tf=_pre_drop_split_module(), old_state=False)
    log = tmp_path / "stub.log"
    env = {**os.environ, "STUB_LOG": str(log), "TF_IN_AUTOMATION": "1"}
    _skip_if_providers_unavailable(_tf(root, "init", "-input=false", env=env))
    var_args = [f"-var={k}={v}" for k, v in tfvars.items()]
    apply = _tf(root, "apply", "-input=false", "-auto-approve", *var_args, env=env)
    assert apply.returncode == 0, apply.stdout + apply.stderr
    created = json.loads((root / "terraform.tfstate").read_text())
    assert [r["name"] for r in created["resources"]] == ["masking_functions"]

    # Upgrade to this code on the same state, with a changed SQL (sql_hash).
    (root / "main.tf").write_text(_masking_root(MODULE_TF.read_text()))
    (env_dir / "masking_functions.sql").write_text("-- sql with one more function\n")
    actions = _plan_actions(root, tfvars, env)
    apply = _tf(root, "apply", "-input=false", "-auto-approve", *var_args, env=env)
    assert apply.returncode == 0, apply.stdout + apply.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    assert len(calls) == 2 and not any("--drop" in call for call in calls), calls
    assert actions == {
        "terraform_data.masking_functions": ["delete", "create"],
        "terraform_data.masking_functions_drop": ["create"],
    }

    # Later SQL changes stay drop-free; a new host (or warehouse, script,
    # file) updates the drop's settings in place.
    (env_dir / "masking_functions.sql").write_text("-- sql changed again\n")
    apply = _tf(root, "apply", "-input=false", "-auto-approve", *var_args, env=env)
    assert apply.returncode == 0, apply.stdout + apply.stderr
    assert _plan_actions(root, {**tfvars, "databricks_workspace_host": "https://other.invalid"}, env) == {
        "terraform_data.masking_functions": ["delete", "create"],
        "terraform_data.masking_functions_drop": ["update"],
    }
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    assert len(calls) == 3 and not any("--drop" in call for call in calls), calls

    destroy = _tf(root, "destroy", "-input=false", "-auto-approve", *var_args, env=env)
    assert destroy.returncode == 0, destroy.stdout + destroy.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    assert len(calls) == 4 and [("--drop" in call) for call in calls] == [False, False, False, True], calls
    assert calls[3][calls[3].index("--auth-file") + 1] == f"{env_dir}/auth.auto.tfvars"


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
    block = block.replace("local.managed_created_spaces", "local.new_spaces")
    block = block.replace("var.genie_destroy_script", '"bash ../../scripts/genie_space.sh"')
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


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_state_loss_apply_adopts_existing_title_instead_of_posting(tmp_path):
    """No Terraform state or ID file: target title identity still prevents a duplicate."""
    tf = WORKSPACE_TF.read_text()
    block = _resource_block(tf, 'resource "terraform_data" "genie_space" {')
    block = block.replace("local.managed_created_spaces", "local.new_spaces")
    block = block.replace("var.genie_destroy_script", '"bash ../../scripts/genie_space.sh"')
    block = re.sub(r"\n  depends_on = \[.*?\n  \]\n", "\n", block, flags=re.S)
    root = tmp_path / "root"
    root.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "curl.log"
    (bin_dir / "curl").write_text(f'''#!/bin/bash
echo "$*" >> {calls}
if [[ " $* " == *" POST "* ]]; then printf '{{"space_id":"duplicate"}}\\n201'; exit 0; fi
if [[ " $* " == *"/api/2.0/genie/spaces"* ]]; then
  printf '{{"spaces":[{{"space_id":"existing-by-title","title":"Agent"}}]}}\\n200'; exit 0
fi
printf '{{}}\\n200'
''')
    (bin_dir / "curl").chmod(0o755)
    prefix = tmp_path / ".genie_space_id"
    (root / "main.tf").write_text(
        'variable "databricks_workspace_host" {}\n'
        'variable "databricks_client_id" {}\n'
        'variable "databricks_client_secret" { sensitive = true }\n'
        'variable "genie_id_file_prefix" {}\nvariable "genie_script_path" {}\n'
        'locals {\n  shared_warehouse_id = "wh"\n'
        '  new_spaces = {\n    agent = {\n      uc_tables = ["c.s.t"]\n'
        '      sql_warehouse_id = ""\n      name = "Agent"\n'
        '      config = { title = "" }\n    }\n  }\n}\n' + block
    )
    tfvars = {
        "databricks_workspace_host": "https://target",
        "databricks_client_id": "client",
        "databricks_client_secret": "secret",
        "genie_id_file_prefix": str(prefix),
        "genie_script_path": f"bash {GENIE_SCRIPT}",
    }
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "DATABRICKS_TOKEN": "token",
        "TF_IN_AUTOMATION": "1",
    }
    init = _tf(root, "init", "-input=false", env=env)
    _skip_if_providers_unavailable(init)
    assert _plan_actions(root, tfvars, env) == {
        'terraform_data.genie_space["agent"]': ["create"]
    }
    var_args = [f"-var={key}={value}" for key, value in tfvars.items()]
    apply = _tf(root, "apply", "-input=false", "-auto-approve", *var_args, env=env)
    assert apply.returncode == 0, apply.stdout + apply.stderr
    assert (tmp_path / ".genie_space_id_agent").read_text().strip() == "existing-by-title"
    assert not any(" POST " in f" {line} " for line in calls.read_text().splitlines())


# ── Adoption required during the migration (missing ID / 404 / auth) ────────

from scripts import genie_adopt_preflight as gap  # noqa: E402

KEY = "walkthrough_prod_cat_demo"


def _legacy_env(tmp_path: Path, agent_id: str | None = "01live", host: str = HOST) -> Path:
    """envs/prod as the pre-fix code left it: legacy state + ID file + auth."""
    env_dir = tmp_path / "aws" / "envs" / "prod"
    env_dir.mkdir(parents=True)
    (env_dir / "auth.auto.tfvars").write_text(AUTH_TEXT)
    state = json.loads(json.dumps(GENIE_OLD_STATE))
    instance = state["resources"][0]["instances"][0]
    instance["index_key"] = KEY
    instance["attributes"]["triggers"]["host"] = host
    instance["attributes"]["triggers"]["id_file"] = str(env_dir / f".genie_space_id_{KEY}")
    state["resources"][0]["module"] = "module.workspace"
    (env_dir / "terraform.tfstate").write_text(json.dumps(state))
    if agent_id is not None:
        (env_dir / f".genie_space_id_{KEY}").write_text(agent_id + "\n")
    return env_dir


def test_preflight_lists_each_legacy_agent_and_arms_adoption(tmp_path, monkeypatch, capsys):
    env_dir = _legacy_env(tmp_path)
    monkeypatch.setattr(gap, "get_status", lambda auth, agent_id: "200")

    assert gap.main([str(env_dir), "--arm"]) == 0

    out = capsys.readouterr().out
    assert f"1 legacy agent(s) on {HOST}" in out
    assert f"OK    {KEY}  id=01live  .genie_space_id_{KEY}  GET 200" in out
    assert "current-secret" not in out
    assert gap.marker_for(env_dir, KEY).exists()


@pytest.mark.parametrize(
    "agent_id, host, status, problem",
    [
        (None, HOST, "200", "ID file missing or empty"),
        ("", HOST, "200", "ID file missing or empty"),
        ("01live", "https://other.cloud.databricks.com", "200", "created on https://other"),
        ("01live", HOST, "404", "GET returned 404"),
        ("01live", HOST, "403", "GET returned 403"),
        ("01live", HOST, "401", "GET returned 401"),
    ],
    ids=["no-id-file", "empty-id-file", "other-workspace", "404", "403", "401"],
)
def test_preflight_aborts_unless_every_agent_answers_200(
    tmp_path, monkeypatch, capsys, agent_id, host, status, problem
):
    env_dir = _legacy_env(tmp_path, agent_id=agent_id, host=host)
    monkeypatch.setattr(gap, "get_status", lambda auth, agent_id: status)

    assert gap.main([str(env_dir), "--arm", "--quiet"]) == 1

    out = capsys.readouterr().out
    assert f"FAIL  {KEY}" in out and problem in out
    assert "nothing was applied" in out
    assert not gap.marker_for(env_dir, KEY).exists()


def test_preflight_is_silent_without_legacy_agents(tmp_path, capsys):
    env_dir = tmp_path / "envs" / "prod"
    env_dir.mkdir(parents=True)
    assert gap.main([str(env_dir), "--arm"]) == 0
    assert gap.main([str(env_dir), "--arm", "--quiet"]) == 0
    assert capsys.readouterr().out == ""


def _run_strict_create(tmp_path: Path, get_code: str, *, write_id: bool = True,
                       marker: bool = True, extra_env: dict | None = None):
    env_dir = _legacy_env(tmp_path, agent_id="01live" if write_id else None)
    if marker:
        gap.marker_for(env_dir, KEY).write_text("adoption required\n")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "curl.log"
    (bin_dir / "curl").write_text(f"""#!/bin/bash
echo "$*" >> {calls}
if [[ " $* " == *" POST "* ]]; then printf '{{"space_id":"01dupe"}}\\n200'; exit 0; fi
if [[ " $* " == *"/api/2.0/genie/spaces/"* ]]; then printf '{get_code}'; exit 0; fi
printf '{{}}\\n200'
""")
    (bin_dir / "curl").chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
           "DATABRICKS_HOST": HOST, "DATABRICKS_TOKEN": "t",
           "GENIE_ID_FILE": str(env_dir / f".genie_space_id_{KEY}"),
           "GENIE_TABLES_CSV": "cat.s.t", "GENIE_WAREHOUSE_ID": "wh", "GENIE_TITLE": "Agent",
           **(extra_env or {})}
    result = subprocess.run(["bash", str(GENIE_SCRIPT), "create"], env=env,
                            capture_output=True, text=True, timeout=60)
    log = calls.read_text().splitlines() if calls.exists() else []
    return result, [c for c in log if " POST " in f" {c} "], env_dir


@pytest.mark.parametrize("code", ["404", "401", "403", "500"])
def test_adoption_required_accepts_only_200(tmp_path, code):
    result, posts, env_dir = _run_strict_create(tmp_path, code)
    assert result.returncode != 0
    assert f"returned HTTP {code}" in result.stderr and "not creating a new agent" in result.stderr
    assert posts == []
    assert gap.marker_for(env_dir, KEY).exists()  # stays armed for the retry


def test_adoption_required_with_missing_id_file_creates_nothing(tmp_path):
    result, posts, _ = _run_strict_create(tmp_path, "200", write_id=False)
    assert result.returncode != 0
    assert "ID file" in result.stderr and "missing or empty" in result.stderr
    assert posts == []


def test_adoption_required_by_env_var_without_marker(tmp_path):
    result, posts, _ = _run_strict_create(tmp_path, "404", marker=False,
                                          extra_env={"GENIE_ADOPT_REQUIRED": "1"})
    assert result.returncode != 0 and posts == []


def test_successful_adoption_disarms_the_marker(tmp_path):
    result, posts, env_dir = _run_strict_create(tmp_path, "200")
    assert result.returncode == 0, result.stdout + result.stderr
    assert posts == []
    assert not gap.marker_for(env_dir, KEY).exists()
    assert (env_dir / f".genie_space_id_{KEY}").read_text().strip() == "01live"


def test_normal_create_still_replaces_a_deleted_agent(tmp_path):
    result, posts, _ = _run_strict_create(tmp_path, "404", marker=False)
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(posts) == 1


def test_make_runs_the_preflight_before_any_layer_is_applied():
    makefile = (SHARED / "Makefile.shared").read_text()
    apply = makefile[makefile.index("\napply: "):]
    apply = apply[: apply.index("\n\n")]
    assert apply.index("genie_adopt_preflight.py\" \"$(ENV_DIR)\" --arm") < apply.index("LAYER=data_access")
    layer = makefile[makefile.index("\n_apply-layer:"):]
    workspace = layer[layer.index('if [ "$$layer" = "workspace" ]; then'):]
    assert workspace.index('"$(GENIE_ADOPT_PREFLIGHT_SCRIPT)"') < workspace.index("apply -parallelism=1")
    assert "GENIE_ADOPT_PREFLIGHT_SCRIPT ?= $(SHARED_ROOT)/scripts/genie_adopt_preflight.py" in makefile
    assert "\ngenie-adopt-preflight: " in makefile


def test_combined_apply_never_skips_data_access_before_workspace_can_run():
    makefile = (SHARED / "Makefile.shared").read_text()
    apply = makefile[makefile.index("\napply: "):]
    apply = apply[: apply.index("\n\n")]
    data_access = (
        '_apply-layer LAYER=data_access TARGET_ENV=$(ENV) '
        'LAYER_ENV_DIR="$(ENV_DIR)/$(DATA_ACCESS_SUBDIR)" FORCE_APPLY=1'
    )
    assert data_access in apply
    assert apply.index(data_access) < apply.index("LAYER=workspace")


def test_no_saved_plan_file_is_written_by_the_product():
    # A saved plan embeds prior state, which may still hold the legacy secret.
    for path in (SHARED / "Makefile.shared", SHARED / "scripts/terraform_layer.sh"):
        assert not re.search(r"-out[= ]", path.read_text()), path


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_migration_with_missing_id_file_fails_and_creates_nothing(tmp_path):
    """The real genie_space.sh under Terraform, as make apply runs it."""
    env_dir = _legacy_env(tmp_path, agent_id=None)
    # Step 1, as make apply runs it: the preflight refuses before any apply.
    assert gap.main([str(env_dir), "--arm"]) == 1
    assert not gap.marker_for(env_dir, KEY).exists()

    # Defence in depth: armed, then the ID file vanished before the apply.
    gap.marker_for(env_dir, KEY).write_text("adoption required\n")
    tf = WORKSPACE_TF.read_text()
    block = _resource_block(tf, 'resource "terraform_data" "genie_space" {')
    block = block.replace("local.managed_created_spaces", "local.new_spaces")
    block = block.replace("var.genie_destroy_script", '"bash ../../scripts/genie_space.sh"')
    block = re.sub(r"\n  depends_on = \[.*?\n  \]\n", "\n", block, flags=re.S)
    block = block.replace('"bash ../../scripts/genie_space.sh trash"', '"echo TRASH >> trash.log"')
    removed = re.search(r"removed \{\n  from = null_resource\.genie_space_create.*?\n\}\n", tf, re.S).group(0)
    (env_dir / "main.tf").write_text(
        'terraform {\n  required_providers {\n'
        '    null = { source = "hashicorp/null", version = "~> 3.2" }\n  }\n}\n'
        'variable "databricks_workspace_host" {}\nvariable "databricks_client_id" {}\n'
        'variable "databricks_client_secret" { sensitive = true }\n'
        'variable "genie_id_file_prefix" {}\nvariable "genie_script_path" {}\n'
        'locals {\n  shared_warehouse_id = "wh"\n'
        f'  new_spaces = {{ {KEY} = {{ uc_tables = ["c.s.t"], sql_warehouse_id = "", name = "Agent",'
        ' config = { title = "" } } }\n}\n'
        + block.replace("module.workspace.", "") + "\n" + removed
    )
    state = json.loads((env_dir / "terraform.tfstate").read_text())
    state["resources"][0].pop("module")
    (env_dir / "terraform.tfstate").write_text(json.dumps(state))
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "curl.log"
    (bin_dir / "curl").write_text(f'#!/bin/bash\necho "$*" >> {calls}\nprintf \'{{"space_id":"01dupe"}}\\n200\'\n')
    (bin_dir / "curl").chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
           "DATABRICKS_TOKEN": "t", "TF_IN_AUTOMATION": "1"}
    tfvars = [f"-var=databricks_workspace_host={HOST}", "-var=databricks_client_id=sp-client",
              "-var=databricks_client_secret=current-secret",
              f"-var=genie_id_file_prefix={env_dir}/.genie_space_id",
              f"-var=genie_script_path=bash {GENIE_SCRIPT}"]
    _skip_if_providers_unavailable(_tf(env_dir, "init", "-input=false", env=env))

    for _attempt in (1, 2):  # the retry (tainted resource) must refuse too
        apply = _tf(env_dir, "apply", "-input=false", "-auto-approve", *tfvars, env=env)
        assert apply.returncode != 0
        assert "Adoption required" in apply.stdout + apply.stderr
    assert not calls.exists() or " POST " not in f" {calls.read_text()} "
    assert not (env_dir / f".genie_space_id_{KEY}").exists()
    assert not (env_dir / "trash.log").exists()
    assert gap.marker_for(env_dir, KEY).exists()
