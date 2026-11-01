"""terraform import with a passing coverage check, on the real roots through the runner.

terraform import evaluates the whole configuration but runs no plan, so
plantimestamp() is unknown there. coverage_gate_check's refresh-time checks
use it, so a passing result left the gate status unknown, and with it every
for_each filtered on that status: data_access databricks_grant.table_access,
the workspace Genie CAN_RUN ACLs. The import then failed with "Invalid
for_each argument" and wrote no state. terraform_layer.sh now sets
import_walk=true on import only (a runner variable no caller may set), which
gives the checks the known status "import".
"""

import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.terraform_helpers import OFFLINE_MARKERS, TIMEOUT, required, session_plugin_cache, shared_copy

pytestmark = pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")

AUTH = """databricks_account_id     = "account"
databricks_client_id      = "service-principal"
databricks_client_secret  = "secret"
databricks_workspace_host = "https://example.invalid"
databricks_workspace_id   = "123"
"""
# Imports that need no API call: the time and built-in providers.
WAIT = "module.data_access.time_sleep.wait_for_tag_propagation"
MASKS = "module.data_access.terraform_data.masking_functions"
NEW_SPACE = 'module.workspace.terraform_data.genie_space["new"]'


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Env:
    def __init__(self, tmp_path: Path, layer: str):
        self.layer = layer
        self.runner = shared_copy(tmp_path) / "scripts" / "terraform_layer.sh"
        self.env_dir = tmp_path / "envs" / "dev"
        self.layer_dir = self.env_dir / "data_access" if layer == "data_access" else self.env_dir
        self.layer_dir.mkdir(parents=True, exist_ok=True)
        (self.env_dir / "data_access").mkdir(exist_ok=True)
        (self.layer_dir / "auth.auto.tfvars").write_text(AUTH)
        self.environ = {k: v for k, v in os.environ.items()
                        if not k.startswith(("TF_CLI_ARGS", "TF_VAR_", "DATABRICKS_", "GENIERAILS_"))}
        self.environ |= {"LAYER_ENV_DIR": str(self.layer_dir), "ENVS_DIR": str(self.env_dir.parent),
                         "TF_IN_AUTOMATION": "1", "GIT_CONFIG_GLOBAL": os.devnull,
                         "TF_PLUGIN_CACHE_DIR": str(session_plugin_cache())}

    def run(self, *args: str, stdin: str | None = None) -> subprocess.CompletedProcess:
        result = subprocess.run([str(self.runner), self.layer, "dev", *args], input=stdin, text=True,
                                capture_output=True, env=self.environ, timeout=TIMEOUT)
        output = result.stdout + result.stderr
        if result.returncode != 0 and not required() and any(m in output for m in OFFLINE_MARKERS):
            pytest.skip("Terraform providers not downloadable (offline)")
        return result

    def console(self, expression: str) -> str:
        result = self.run("console", stdin=expression + "\n")
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout.strip())

    def state(self) -> dict:
        return json.loads((self.layer_dir / "terraform.tfstate").read_text())

    def addresses(self) -> set[str]:
        return {
            f"{r['module']}.{r['type']}.{r['name']}" + (f'["{i["index_key"]}"]' if "index_key" in i else "")
            for r in self.state()["resources"] if r["mode"] == "managed" for i in r["instances"]
        }


@pytest.fixture
def data_access(tmp_path):
    env = Env(tmp_path, "data_access")
    (env.layer_dir / "abac.auto.tfvars").write_text(
        'sql_warehouse_id = "warehouse"\nuc_tables = ["cat.sch.customers"]\ngroups = { analysts = {} }\n')
    (env.layer_dir / "masking_functions.sql").write_text("")
    return env


def _pass_gate(env: Env) -> None:
    fingerprint = env.console("module.data_access.coverage_gate_inputs.fingerprint")
    (env.layer_dir / ".coverage_gate.json").write_text(json.dumps(
        {"fingerprint": fingerprint, "status": "pass", "refreshed_at": _now()}))


def _imported(result: subprocess.CompletedProcess) -> None:
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert "Import successful!" in output and "Invalid for_each" not in output, output


def test_data_access_import_works_while_the_coverage_check_passes(data_access):
    _pass_gate(data_access)
    result = data_access.run("import", "-no-color", WAIT, "30s,")
    _imported(result)
    assert data_access.addresses() == {WAIT}
    assert "-var=import_walk=true" in result.stderr  # the runner's "+ terraform import ..." line

    # Plan never gets import_walk: the same passing result still plans the business SELECT.
    plan = data_access.run("plan", "-no-color", "-refresh=false")
    assert plan.returncode == 0, plan.stdout + plan.stderr
    assert "-var=import_walk=false" in plan.stderr and "import_walk=true" not in plan.stderr
    assert 'module.data_access.databricks_grant.table_access["cat.sch.customers|analysts"] will be created' in plan.stdout


def test_an_import_keeps_the_last_applies_coverage_record(data_access):
    _imported(data_access.run("import", "-no-color", WAIT, "30s,"))
    # What an apply with a passing check and one business grant records.
    state = data_access.state()
    state["outputs"]["coverage_gate"]["value"] |= {"status": "pass", "table_grant_count": 1}
    state["serial"] += 1
    (data_access.layer_dir / "terraform.tfstate").write_text(json.dumps(state))
    before = state["outputs"]["coverage_gate"]["value"]
    # The workspace layer reads this record as what the last apply was checked with
    # (legacy CAN_RUN): an import applies nothing, so it keeps it, not status "import".
    _pass_gate(data_access)
    _imported(data_access.run("import", "-no-color", MASKS, "imported"))
    assert data_access.addresses() == {WAIT, MASKS}
    assert data_access.state()["outputs"]["coverage_gate"]["value"] == before


def test_workspace_import_works_while_the_agent_coverage_check_passes(tmp_path):
    env = Env(tmp_path, "workspace")
    # Sales's CAN_RUN ACL is filtered on the check's status; New is created, so it has a terraform_data.
    (env.layer_dir / "env.auto.tfvars").write_text(
        'sql_warehouse_id = "warehouse"\ngovernance_mode = "deterministic"\n'
        'genie_spaces = [\n'
        '  { name = "Sales", genie_space_id = "space-1", uc_tables = ["cat.sch.orders"], acl_groups = ["partial"] },\n'
        '  { name = "New", uc_tables = ["cat.sch.orders"], acl_groups = ["partial"] },\n'
        ']\n')
    (env.layer_dir / "abac.auto.tfvars").write_text("groups = {}\n")
    fingerprint = env.console("local.agent_coverage_fingerprint")
    (env.layer_dir / ".agent_coverage.json").write_text(json.dumps({
        "fingerprint": fingerprint, "status": "pass", "refreshed_at": _now(),
        "agents": {"sales": {"status": "pass"}, "new": {"status": "pass"}}}))
    _imported(env.run("import", "-no-color", NEW_SPACE, "space-2"))
    assert env.addresses() == {NEW_SPACE}


@pytest.mark.parametrize("command", ["plan", "apply", "import"])
@pytest.mark.parametrize("source", ["arg", "tf_var", "tf_cli_args", "tfvars"])
def test_a_caller_cannot_set_import_walk(data_access, command, source):
    args = [command] + ([WAIT, "30s,"] if command == "import" else [])
    if source == "arg":
        args.insert(1, "-var=import_walk=true")
    elif source == "tf_var":
        data_access.environ["TF_VAR_import_walk"] = "true"
    elif source == "tf_cli_args":
        data_access.environ["TF_CLI_ARGS"] = "-var=import_walk=true"
    else:
        (data_access.layer_dir / "abac.auto.tfvars").write_text(
            (data_access.layer_dir / "abac.auto.tfvars").read_text() + "import_walk = true\n")

    result = data_access.run(*args)
    assert result.returncode == 2, result.stdout + result.stderr
    assert "sets import_walk" in result.stderr and "+ terraform" not in result.stderr
    assert not (data_access.layer_dir / "terraform.tfstate").exists()
