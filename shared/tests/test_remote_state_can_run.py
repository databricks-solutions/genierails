"""The workspace root's CAN_RUN inputs under GENIERAILS_STATE_DIR and remote state.

The root reads its own and the data_access state with file(): the CAN_RUN on
record, the applied coverage check and grants. With the state outside the env
dir it must get them from terraform_layer.sh (snapshots read through
scripts/state_layout.py), or it sees nothing applied: CAN_RUN is withheld and
an ACL already granted would be removed. Real Terraform (terraform console on
a copy of the root); no provider is configured against anything live.
"""

import base64
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED / "scripts"))

from tests.terraform_helpers import TIMEOUT, session_plugin_cache, shared_copy, skip_if_providers_unavailable  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")

EXPRESSION = ("base64encode(jsonencode({groups = module.workspace.genie_space_acls_groups, "
              "blocker = local.genie_exposure_static_blocker, widening = local.genie_space_can_run_widening, "
              "applied = local.applied_can_run_groups}))")
NULL_PROVIDER = 'provider["registry.terraform.io/hashicorp/null"]'
FAKE_TERRAFORM = """#!/usr/bin/env bash
printf '%s\\n' "$*" >> "$FAKE_TF_LOG"
if [ "$1 $2" = "state pull" ]; then
  [ -n "${FAKE_TF_FAIL:-}" ] && { echo "backend unreachable" >&2; exit 1; }
  case "$TF_DATA_DIR" in
    */layers/workspace/*) cat "$FAKE_WORKSPACE_STATE" ;;
    */layers/data_access/*) cat "$FAKE_DATA_ACCESS_STATE" ;;
  esac
fi
exit 0
"""


@pytest.fixture(scope="module")
def copy(tmp_path_factory):
    """A copy of shared/ whose workspace root is initialized once (local backend), and its TF data dir."""
    shared = shared_copy(tmp_path_factory.mktemp("shared"))
    probe = tmp_path_factory.mktemp("probe")
    init = subprocess.run([str(shared / "scripts" / "terraform_layer.sh"), "workspace", "probe", "print-cmd", "plan"],
                          env=_tf_env(LAYER_ENV_DIR=str(probe)), text=True, capture_output=True, timeout=TIMEOUT)
    skip_if_providers_unavailable(init)
    return shared, probe / ".terraform"


def _tf_env(**extra) -> dict:
    env = {k: v for k, v in os.environ.items()
           if k not in ("GENIERAILS_STATE_DIR", "CI", "TF_BUILD") and not k.startswith(("DATABRICKS_", "ARM_", "AWS_"))}
    return env | {"TF_IN_AUTOMATION": "1", "TF_PLUGIN_CACHE_DIR": str(session_plugin_cache())} | extra


def _env_dir(tmp_path: Path) -> Path:
    env = tmp_path / "aws" / "envs" / "dev"
    (env / "data_access").mkdir(parents=True)
    (env / "abac.auto.tfvars").write_text(
        'databricks_account_id = "account"\ndatabricks_client_id = "sp"\n'
        'databricks_client_secret = "secret"\ndatabricks_workspace_id = "123"\n'
        'databricks_workspace_host = "https://example.invalid"\nsql_warehouse_id = "warehouse"\n'
        'groups = { analysts = {} }\n'
        'genie_spaces = [{ name = "Sales", genie_space_id = "space-1", uc_tables = ["cat.sch.customers"] }]\n'
        'genie_space_configs = { Sales = { acl_groups = ["analysts"] } }\n')
    (env / "data_access" / ".coverage_gate.json").write_text(json.dumps({
        "status": "pass", "fingerprint": "applied",
        "refreshed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}))
    return env


def _output(value) -> dict:
    """A state output with the type Terraform records for ``value`` (strings, numbers, lists of strings)."""
    def type_of(v):
        if isinstance(v, dict):
            return ["object", {k: type_of(x) for k, x in v.items()}]
        if isinstance(v, list):
            return ["list", "string"]
        return "number" if isinstance(v, (int, float)) else "string"
    return {"type": type_of(value), "value": value}


def _data_access_state(**gate) -> dict:
    """An applied data_access layer: a passing coverage check and the SELECT grant CAN_RUN needs."""
    return {"version": 4, "terraform_version": "1.11.4", "serial": 3, "lineage": "da", "resources": [], "outputs": {
        "coverage_gate": _output({"fingerprint": "applied", "status": "pass", "max_age": "6h",
                                  "table_grant_count": 1} | gate),
        "table_grant_resource_keys": _output(["cat.sch.customers|analysts"])}}


def _workspace_state() -> dict:
    """An applied workspace layer whose Sales agent already grants CAN_RUN to analysts."""
    return {"version": 4, "terraform_version": "1.11.4", "serial": 5, "lineage": "ws", "outputs": {}, "resources": [{
        "module": "module.workspace", "mode": "managed", "type": "null_resource", "name": "genie_space_acls",
        "provider": NULL_PROVIDER, "instances": [{"index_key": "sales", "schema_version": 0, "attributes": {
            "id": "1", "triggers": {"space_id": "space-1", "groups": "analysts"}}}]}]}


def _console(proc: subprocess.CompletedProcess) -> dict:
    assert proc.returncode == 0, proc.stdout + proc.stderr
    line = [line for line in proc.stdout.splitlines() if line.strip().startswith('"')][-1]
    return json.loads(base64.b64decode(json.loads(line)))


NO_CAN_RUN_CHANGE = {"groups": {"sales": "analysts"}, "blocker": "", "widening": {"sales": []},
                     "applied": {"sales": ["analysts"]}}


def test_state_dir_root_sees_the_applied_grants_and_keeps_can_run(tmp_path, copy):
    copy, _ = copy
    env = _env_dir(tmp_path)
    layers = tmp_path / "state" / "aws" / "dev" / "layers"
    for layer, state in (("data_access", _data_access_state()), ("workspace", _workspace_state())):
        (layers / layer).mkdir(parents=True)
        (layers / layer / "terraform.tfstate").write_text(json.dumps(state))
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    proc = subprocess.run(
        [str(copy / "scripts" / "terraform_layer.sh"), "workspace", "dev", "console"], input=EXPRESSION + "\n",
        env=_tf_env(LAYER_ENV_DIR=str(env), ENVS_DIR=str(env.parent), GENIERAILS_STATE_DIR=str(tmp_path / "state")),
        text=True, capture_output=True, timeout=TIMEOUT)
    assert _console(proc) == NO_CAN_RUN_CHANGE
    assert not (env / "terraform.tfstate").exists() and not (env / "data_access" / "terraform.tfstate").exists()


def _fake_terraform(tmp_path: Path) -> dict:
    bin_dir = tmp_path / "fakebin"
    bin_dir.mkdir()
    (bin_dir / "terraform").write_text(FAKE_TERRAFORM)
    (bin_dir / "terraform").chmod(0o755)
    log, workspace, data_access = tmp_path / "tf.log", tmp_path / "ws.json", tmp_path / "da.json"
    log.touch()
    workspace.write_text(json.dumps(_workspace_state()))
    data_access.write_text(json.dumps(_data_access_state()))
    return {"PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "FAKE_TF_LOG": str(log),
            "FAKE_WORKSPACE_STATE": str(workspace), "FAKE_DATA_ACCESS_STATE": str(data_access)}


def _run_with_fake(tmp_path, env, fake, **extra):
    return subprocess.run([str(SHARED / "scripts" / "terraform_layer.sh"), "workspace", "dev", "console"],
                          input="1\n", env=_tf_env(LAYER_ENV_DIR=str(env), ENVS_DIR=str(env.parent), **fake, **extra),
                          text=True, capture_output=True, timeout=TIMEOUT)


def _var(command: str, name: str) -> str:
    prefix = f"-var={name}="
    return next(arg[len(prefix):] for arg in command.split() if arg.startswith(prefix))


def test_remote_root_gets_the_pulled_states_and_keeps_can_run(tmp_path, copy):
    copy, data_dir = copy
    env = _env_dir(tmp_path)
    (env / "remote_state.hcl").write_text('backend = "s3"\nbucket = "b"\nregion = "r"\nuse_lockfile = true\n')
    fake = _fake_terraform(tmp_path)
    proc = _run_with_fake(tmp_path, env, fake)
    assert proc.returncode == 0, proc.stderr
    console = [c for c in Path(fake["FAKE_TF_LOG"]).read_text().splitlines() if c.startswith("console")][-1]
    snapshots = {name: Path(_var(console, name)) for name in ("workspace_state_file", "data_access_state_file")}
    assert json.loads(snapshots["workspace_state_file"].read_text()) == _workspace_state()
    assert json.loads(snapshots["data_access_state_file"].read_text()) == _data_access_state()
    for path in snapshots.values():
        assert not path.is_relative_to(env) and oct(path.stat().st_mode & 0o777) == "0o600"
    # Real Terraform on exactly what the runner passed: the root sees the
    # applied grants and the CAN_RUN on record, so nothing changes.
    real = subprocess.run(
        ["terraform", "console", f"-var-file={env / 'abac.auto.tfvars'}", f"-var=env_dir={env}",
         *(f"-var={name}={path}" for name, path in snapshots.items())],
        cwd=copy / "roots" / "workspace", input=EXPRESSION + "\n", text=True, capture_output=True, timeout=TIMEOUT,
        env=_tf_env(TF_DATA_DIR=str(data_dir)))
    assert _console(real) == NO_CAN_RUN_CHANGE


@pytest.mark.parametrize("broken", ["state-dir workspace", "state-dir data_access", "remote pull"])
def test_unreadable_state_refuses_before_terraform_runs(tmp_path, broken):
    env = _env_dir(tmp_path)
    fake = _fake_terraform(tmp_path)
    extra = {}
    if broken.startswith("state-dir"):
        layer = broken.split()[1]
        layers = tmp_path / "state" / "aws" / "dev" / "layers"
        for name, state in (("data_access", _data_access_state()), ("workspace", _workspace_state())):
            (layers / name).mkdir(parents=True)
            (layers / name / "terraform.tfstate").write_text("{truncated" if name == layer else json.dumps(state))
        (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
        extra["GENIERAILS_STATE_DIR"] = str(tmp_path / "state")
    else:
        (env / "remote_state.hcl").write_text('backend = "s3"\nbucket = "b"\nregion = "r"\nuse_lockfile = true\n')
        extra["FAKE_TF_FAIL"] = "1"
    proc = _run_with_fake(tmp_path, env, fake, **extra)
    # The scheduled-job move reads the workspace state first and refuses on its own.
    assert proc.returncode != 0 and ("can't be read" in proc.stderr or "cannot read" in proc.stderr), proc.stderr
    commands = Path(fake["FAKE_TF_LOG"]).read_text().splitlines()
    assert not any(c.startswith(("console", "plan", "apply")) for c in commands), commands


def test_in_checkout_state_passes_no_state_inputs(tmp_path):
    env = _env_dir(tmp_path)
    fake = _fake_terraform(tmp_path)
    proc = _run_with_fake(tmp_path, env, fake)
    assert proc.returncode == 0, proc.stderr
    console = [c for c in Path(fake["FAKE_TF_LOG"]).read_text().splitlines() if c.startswith("console")][-1]
    assert "state_file=" not in console
    assert sorted(p.name for p in env.rglob("*")) == [".coverage_gate.json", "abac.auto.tfvars", "data_access"]


def test_state_dir_data_access_root_sees_its_applied_grants(tmp_path, copy):
    # The data_access root's own record of applied SELECT (and masks, which
    # its guards protect): from the state dir, not "nothing applied".
    import hashlib

    copy, _ = copy
    env = _env_dir(tmp_path)
    (env / "data_access" / "auth.auto.tfvars").write_text(
        'databricks_account_id = "account"\ndatabricks_client_id = "sp"\ndatabricks_client_secret = "secret"\n'
        'databricks_workspace_id = "123"\ndatabricks_workspace_host = "https://example.invalid"\n')
    binding = hashlib.sha256(json.dumps({"workspace_host": "https://example.invalid", "workspace_id": "123"},
                                        separators=(",", ":"), sort_keys=True).encode()).hexdigest()
    state = _data_access_state(deployment_binding=binding)
    state["resources"] = [{
        "module": "module.data_access", "mode": "managed", "type": "databricks_grant", "name": "table_access",
        "provider": 'provider["registry.terraform.io/databricks/databricks"]',
        "instances": [{"index_key": "cat.sch.customers|analysts", "schema_version": 0, "attributes": {"id": "g"}}]}]
    layer = tmp_path / "state" / "aws" / "dev" / "layers" / "data_access"
    layer.mkdir(parents=True)
    (layer / "terraform.tfstate").write_text(json.dumps(state))
    proc = subprocess.run(
        [str(copy / "scripts" / "terraform_layer.sh"), "data_access", "dev", "console"],
        input="base64encode(jsonencode(local.applied_table_grants))\n",
        env=_tf_env(LAYER_ENV_DIR=str(env / "data_access"), ENVS_DIR=str(env.parent),
                    GENIERAILS_STATE_DIR=str(tmp_path / "state")),
        text=True, capture_output=True, timeout=TIMEOUT)
    assert _console(proc) == ["cat.sch.customers|analysts"]
