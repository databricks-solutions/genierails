"""Withdrawing access never waits on the coverage check; widening it does.

Without a pass, Terraform withholds new table SELECT grants and new Genie
CAN_RUN groups while applying everything else; make then exits non-zero
naming what it withheld (coverage_gate.py withheld), never skips the next
apply while that's pending (skip-key), and removing a group or agent takes
back the CAN_RUN GenieRails granted (genie_space.sh revoke-acls, run by the
ACL resources' destroy-time provisioner).
"""

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED))

from scripts import coverage_gate as cg  # noqa: E402
from tests.terraform_helpers import tf, tf_env, tf_init  # noqa: E402

TABLE = "cat.sch.customers"
GENIE_SCRIPT = SHARED / "scripts" / "genie_space.sh"


def _grant_state(path, keys, status="pass", tainted=(), binding=None):
    """A data_access state; recorded for the layer's own deployment unless `binding`."""
    binding = cg.deployment_binding(path.parent) if binding is None else binding
    path.write_text(json.dumps({"version": 4, "outputs": {
        "coverage_gate": {"value": {"status": status, "fingerprint": "applied", "deployment_binding": binding}},
        "table_grant_resource_keys": {"value": list(keys)},
    }, "resources": [{
        "module": "module.data_access", "mode": "managed", "type": "databricks_grant", "name": "table_access",
        "instances": [{"index_key": key, "attributes": {"id": key}, **({"status": "tainted"} if key in tainted else {})}
                      for key in keys],
    }]}))


def _acl_state(path, groups_by_key):
    path.write_text(json.dumps({"version": 4, "outputs": {}, "resources": [{
        "module": "module.workspace", "mode": "managed", "type": "null_resource", "name": "genie_space_acls",
        "instances": [{"index_key": key, "attributes": {"triggers": {"space_id": key, "groups": groups}}}
                      for key, groups in groups_by_key.items()],
    }]}))


# ── what make reports after the apply ────────────────────────────────────────

def test_data_access_reports_grants_the_apply_withheld(tmp_path, capsys):
    layer = tmp_path / "data_access"
    layer.mkdir()
    (layer / cg.GATE_FILENAME).write_text(json.dumps(
        {"status": "fail", "grant_keys": [f"{TABLE}|analysts", f"{TABLE}|viewers"]}))
    # auditors was revoked, analysts kept, viewers withheld.
    _grant_state(layer / "terraform.tfstate", [f"{TABLE}|analysts"])
    assert cg.report_withheld("data_access", layer, "prod") == 1
    err = capsys.readouterr().err
    assert f"SELECT {TABLE}|viewers" in err and "analysts" not in err
    assert "removals and existing access were applied" in err

    _grant_state(layer / "terraform.tfstate", [f"{TABLE}|analysts", f"{TABLE}|viewers"])
    assert cg.report_withheld("data_access", layer, "prod") == 0
    # A tainted object is not access in place.
    _grant_state(layer / "terraform.tfstate", [f"{TABLE}|analysts", f"{TABLE}|viewers"],
                 tainted={f"{TABLE}|viewers"})
    assert cg.report_withheld("data_access", layer, "prod") == 1
    # So is a state copied from another deployment.
    _grant_state(layer / "terraform.tfstate", [f"{TABLE}|analysts", f"{TABLE}|viewers"], binding="elsewhere")
    assert cg.report_withheld("data_access", layer, "prod") == 1


def test_workspace_reports_can_run_the_apply_withheld(tmp_path, capsys):
    (tmp_path / cg.CAN_RUN_FILENAME).write_text(json.dumps(
        {"version": 1, "desired": {"sales": "analysts,auditors", "hr": "", "ops": "analysts"}}))
    _acl_state(tmp_path / "terraform.tfstate", {"sales": "analysts", "ops": "analysts"})
    assert cg.report_withheld("workspace", tmp_path, "prod") == 1
    err = capsys.readouterr().err
    assert "CAN_RUN sales: auditors" in err and "ops" not in err and "hr" not in err
    _acl_state(tmp_path / "terraform.tfstate", {"sales": "analysts,auditors", "ops": "analysts"})
    assert cg.report_withheld("workspace", tmp_path, "prod") == 0


def test_no_record_means_nothing_to_report(tmp_path):
    assert cg.report_withheld("data_access", tmp_path, "prod") == 0
    assert cg.report_withheld("workspace", tmp_path, "prod") == 0


# ── the "inputs unchanged" apply skip (finding 3) ────────────────────────────

def test_skip_key_never_skips_data_access_while_its_state_records_no_pass(tmp_path):
    layer = tmp_path / "data_access"
    layer.mkdir()
    (layer / cg.GATE_FILENAME).write_text(json.dumps({"status": "pass", "fingerprint": "fp"}))
    assert cg.skip_key("data_access", layer) == "noskip"  # no state yet
    # A revoke-only apply went through under a failing gate ...
    _grant_state(layer / "terraform.tfstate", [f"{TABLE}|analysts"], status="fail")
    # ... so a later pass is applied (and recorded) even with unchanged inputs.
    assert cg.skip_key("data_access", layer) == "noskip"
    _grant_state(layer / "terraform.tfstate", [f"{TABLE}|analysts"], status="pass")
    passing = cg.skip_key("data_access", layer)
    assert passing.startswith("gate pass fp")
    (layer / cg.GATE_FILENAME).write_text(json.dumps({"status": "fail", "fingerprint": "fp"}))
    assert cg.skip_key("data_access", layer) not in (passing, "noskip")
    # A passing state copied from another deployment is no record of ours.
    _grant_state(layer / "terraform.tfstate", [f"{TABLE}|analysts"], status="pass", binding="elsewhere")
    assert cg.skip_key("data_access", layer) == "noskip"


def test_skip_key_never_skips_workspace_while_can_run_is_withheld(tmp_path):
    (tmp_path / "data_access").mkdir()
    _grant_state(tmp_path / "data_access" / "terraform.tfstate", [f"{TABLE}|analysts"])
    (tmp_path / cg.CAN_RUN_FILENAME).write_text(json.dumps({"version": 1, "desired": {"sales": "analysts,auditors"}}))
    _acl_state(tmp_path / "terraform.tfstate", {"sales": "analysts"})
    assert cg.skip_key("workspace", tmp_path) == "noskip"
    _acl_state(tmp_path / "terraform.tfstate", {"sales": "analysts,auditors"})
    before = cg.skip_key("workspace", tmp_path)
    assert before.startswith("data_access ")
    # A new data_access apply (more grants, a new gate status) re-applies the workspace.
    _grant_state(tmp_path / "data_access" / "terraform.tfstate", [f"{TABLE}|analysts", f"{TABLE}|auditors"])
    assert cg.skip_key("workspace", tmp_path) != before


def _stub_runner(tmp_path, inputs):
    """Answers console with `inputs`; records applies and leaves state alone."""
    import base64
    log = tmp_path / "runner.log"
    runner = tmp_path / "runner"
    encoded = base64.b64encode(json.dumps(inputs).encode()).decode()
    runner.write_text(
        "#!/bin/sh\n"
        f'printf "%s\\n" "$*" >> "{log}"\n'
        f'[ "$3" = "console" ] && echo \'"{encoded}"\'\n'
        "exit 0\n"
    )
    runner.chmod(0o755)
    return runner, log


def _env_dir(tmp_path):
    env = tmp_path / "prod"
    (env / "data_access").mkdir(parents=True)
    (env / "data_access" / "abac.auto.tfvars").write_text("tag_assignments = []\n")
    (env / "data_access" / "masking_functions.sql").write_text("-- masks\n")
    (env / "env.auto.tfvars").write_text(f'uc_tables = ["{TABLE}"]\n')
    (env / "auth.auto.tfvars").write_text('databricks_workspace_host = "https://example.invalid"\n')
    return env


def _apply_layer(env, runner, *extra):
    clean = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "APPLY_FLAGS")}
    return subprocess.run(
        ["make", "--no-print-directory", "_apply-layer", "LAYER=data_access", "TARGET_ENV=prod",
         f"LAYER_ENV_DIR={env / 'data_access'}", f"ROOT_RUNNER={runner}", "IMPORT_EXISTING_SCRIPT=true", *extra],
        cwd=SHARED.parent / "aws", text=True, capture_output=True, env=clean,
    )


def _inputs(**extra):
    return {"fingerprint": "fp-1", "grant_tables": [TABLE], "acknowledged_columns": [],
            "needs_gate": False, "new_grants": [], "grant_keys": [f"{TABLE}|analysts"], **extra}


@pytest.mark.gnu_make
def test_revoke_only_apply_under_a_failing_gate_is_never_skipped_later(tmp_path):
    env = _env_dir(tmp_path)
    runner, log = _stub_runner(tmp_path, _inputs())
    state = env / "data_access" / "terraform.tfstate"
    # The failing gate (the bare config fails validation) recorded "fail" in state.
    _grant_state(state, [f"{TABLE}|analysts"], status="fail")
    first = _apply_layer(env, runner)
    assert first.returncode == 0, first.stdout + first.stderr
    second = _apply_layer(env, runner)
    assert second.returncode == 0, second.stdout + second.stderr
    assert "inputs unchanged" not in second.stdout
    assert sum(" apply " in f" {line} " for line in log.read_text().splitlines()) == 2
    # Once the state records a pass, unchanged inputs skip again.
    _grant_state(state, [f"{TABLE}|analysts"], status="pass")
    _apply_layer(env, runner)
    assert "inputs unchanged" in _apply_layer(env, runner).stdout


@pytest.mark.gnu_make
def test_mixed_change_under_a_failing_gate_applies_then_fails_naming_the_addition(tmp_path):
    env = _env_dir(tmp_path)
    # Config: analysts (kept) and viewers (new); auditors is being revoked.
    runner, log = _stub_runner(tmp_path, _inputs(
        new_grants=[f"{TABLE}|viewers"], grant_keys=[f"{TABLE}|analysts", f"{TABLE}|viewers"]))
    _grant_state(env / "data_access" / "terraform.tfstate", [f"{TABLE}|analysts", f"{TABLE}|auditors"])
    result = _apply_layer(env, runner)
    output = result.stdout + result.stderr
    assert any(line.split()[2] == "apply" for line in log.read_text().splitlines()), output
    assert "1 new SELECT grant(s) are withheld" in output
    assert result.returncode != 0
    assert f"SELECT {TABLE}|viewers" in output
    # make apply defers the report until the workspace layer has applied too.
    deferred = _apply_layer(env, runner, "DEFER_WITHHELD=1")
    assert deferred.returncode == 0, deferred.stdout + deferred.stderr


@pytest.mark.gnu_make
def test_weakened_protection_under_a_failing_gate_applies_nothing(tmp_path):
    env = _env_dir(tmp_path)
    runner, log = _stub_runner(tmp_path, _inputs(needs_gate=True))
    result = _apply_layer(env, runner)
    assert result.returncode != 0
    assert "keeps business SELECT with weaker (or unrecorded) protection" in result.stderr
    assert all(line.split()[2] == "console" for line in log.read_text().splitlines())


def test_make_apply_reports_withheld_access_after_both_layers():
    source = (SHARED / "Makefile.shared").read_text()
    apply = source[source.index("\napply: "):source.index("\napply-governance:")]
    data_access = apply.index("_apply-layer LAYER=data_access")
    workspace = apply.index("_apply-layer LAYER=workspace")
    assert "DEFER_WITHHELD=1" in apply[data_access:workspace]
    report = apply.index("withheld --layer data_access")
    assert workspace < report < apply.index("withheld --layer workspace")


# ── genie_space.sh revoke-acls (finding 6) ───────────────────────────────────

FAKE_CURL = r"""#!/bin/bash
method=GET; body=""
while [ $# -gt 0 ]; do
  case "$1" in
    -X) method="$2"; shift ;;
    -d) body="$2"; shift ;;
  esac
  shift
done
echo "$method" >> "$CURL_LOG"
if [ "$method" = "PUT" ]; then printf '%s' "$body" > "$PUT_BODY"; printf '{}\n200'; exit 0; fi
printf '%s\n%s' "$(cat "$ACL_JSON")" "${GET_STATUS:-200}"
"""

ACL = {"object_id": "genie/space-1", "access_control_list": [
    {"group_name": "analysts", "all_permissions": [{"permission_level": "CAN_RUN", "inherited": False}]},
    {"group_name": "auditors", "all_permissions": [{"permission_level": "CAN_RUN", "inherited": False}]},
    {"group_name": "bi_team", "all_permissions": [{"permission_level": "CAN_EDIT", "inherited": False}]},
    {"group_name": "admins", "all_permissions": [
        {"permission_level": "CAN_MANAGE", "inherited": True, "inherited_from_object": ["/workspace"]}]},
    {"user_name": "owner@example.com", "all_permissions": [{"permission_level": "IS_OWNER", "inherited": False}]},
    {"service_principal_name": "sp-1", "all_permissions": [{"permission_level": "CAN_RUN", "inherited": False}]},
]}


def _revoke(tmp_path, groups, **env_extra):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    (bin_dir / "curl").write_text(FAKE_CURL)
    (bin_dir / "curl").chmod(0o755)
    (tmp_path / "acl.json").write_text(json.dumps(ACL))
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "DATABRICKS_HOST": "https://ws",
           "DATABRICKS_TOKEN": "t", "GENIE_SPACE_OBJECT_ID": "space-1", "GENIE_REVOKE_GROUPS_CSV": groups,
           "CURL_LOG": str(tmp_path / "curl.log"), "PUT_BODY": str(tmp_path / "put.json"),
           "ACL_JSON": str(tmp_path / "acl.json")}
    env.pop("LAYER_ENV_DIR", None)
    env.update(env_extra)
    return subprocess.run(["bash", str(GENIE_SCRIPT), "revoke-acls"], env=env, text=True, capture_output=True)


def test_revoke_acls_removes_only_the_groups_it_granted(tmp_path):
    # No ambient environment variable may turn the production revoke into a no-op.
    result = _revoke(tmp_path, "analysts,auditors", GENIERAILS_TERRAFORM_TEST="1")
    assert result.returncode == 0, result.stdout + result.stderr
    assert (tmp_path / "curl.log").read_text().split() == ["GET", "PUT"]
    kept = json.loads((tmp_path / "put.json").read_text())["access_control_list"]
    # Other direct entries stay; inherited ones (admins) and the owner aren't PUT.
    assert kept == [
        {"group_name": "bi_team", "permission_level": "CAN_EDIT"},
        {"service_principal_name": "sp-1", "permission_level": "CAN_RUN"},
    ]


def test_revoke_acls_with_no_groups_calls_nothing(tmp_path):
    result = _revoke(tmp_path, "")
    assert result.returncode == 0, result.stdout + result.stderr
    assert not (tmp_path / "curl.log").exists()


def test_revoke_acls_on_a_deleted_agent_succeeds(tmp_path):
    result = _revoke(tmp_path, "analysts", GET_STATUS="404")
    assert result.returncode == 0, result.stdout + result.stderr
    assert (tmp_path / "curl.log").read_text().split() == ["GET"]


def test_revoke_acls_fails_loudly_when_the_permissions_cannot_be_read(tmp_path):
    result = _revoke(tmp_path, "analysts", GET_STATUS="403")
    assert result.returncode != 0
    assert "Could not read permissions" in result.stderr


def test_revoke_acls_reads_the_created_agent_id_and_credentials_from_the_layer(tmp_path):
    layer = tmp_path / "env"
    layer.mkdir()
    (layer / ".genie_space_id_sales").write_text("space-9\n")
    (layer / "auth.auto.tfvars").write_text(
        'databricks_workspace_host = "https://ws"\ndatabricks_client_id = "id"\ndatabricks_client_secret = "s"\n')
    result = _revoke(tmp_path, "analysts", GENIE_SPACE_OBJECT_ID="", LAYER_ENV_DIR=str(layer),
                     GENIE_ID_BASENAME=".genie_space_id_sales")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Revoking Genie CAN_RUN on agent space-9 for groups: analysts" in result.stdout


# ── the ACL resources take back CAN_RUN when destroyed (finding 6) ───────────

def _acl_module(tmp_path):
    """modules/workspace's genie_space_acls block, fed by a plain map."""
    source = (SHARED / "modules/workspace/main.tf").read_text()
    start = source.index('resource "null_resource" "genie_space_acls" {')
    block = source[start:source.index("\n}\n", start) + 3]
    block = re.sub(r"\n  depends_on = \[.*?\]\n", "\n", block, flags=re.S)
    root = tmp_path / "roots" / "workspace"
    root.mkdir(parents=True)
    (root / "main.tf").write_text(
        'variable "spaces" { type = map(string) }\n'
        'variable "genie_script_path" { default = "" }\n'
        'variable "genie_destroy_script" { default = "bash ../../scripts/genie_space.sh" }\n'
        'variable "genie_space_acl_created_handoffs" { default = {} }\n'
        'variable "databricks_workspace_host" { default = "https://ws" }\n'
        'variable "databricks_client_id" { default = "id" }\n'
        'variable "databricks_client_secret" {\n  default   = "s"\n  sensitive = true\n}\n'
        "locals {\n"
        '  existing_spaces        = { for key, groups in var.spaces : key => { genie_space_id = "id-${key}", name = key } }\n'
        "  genie_space_acl_keys   = keys(var.spaces)\n"
        "  genie_space_groups     = var.spaces\n"
        "  genie_space_acl_groups = var.spaces\n"
        "}\n" + block
    )
    stub = tmp_path / "scripts" / "genie_space.sh"
    stub.parent.mkdir()
    stub.write_text('#!/bin/sh\necho "$1 space=$GENIE_SPACE_OBJECT_ID revoke=$GENIE_REVOKE_GROUPS_CSV" >> "$GENIE_STUB_LOG"\n')
    stub.chmod(0o755)
    return root


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_removing_groups_or_an_agent_revokes_its_can_run(tmp_path):
    root = _acl_module(tmp_path)
    log = tmp_path / "genie.log"
    env = tf_env(tmp_path, GENIE_STUB_LOG=str(log))
    tf_init(root, env=env)

    def apply(spaces):
        result = tf(root, "apply", "-auto-approve", "-input=false", f"-var=spaces={json.dumps(spaces)}", env=env)
        assert result.returncode == 0, result.stdout + result.stderr
        calls = log.read_text().splitlines() if log.exists() else []
        log.unlink(missing_ok=True)
        return calls

    assert apply({"sales": "analysts,auditors", "hr": "analysts"}) == []  # create: set-acls only
    # A group removed: the ACL is replaced, revoking the old groups first.
    assert apply({"sales": "analysts", "hr": "analysts"}) == ["revoke-acls space=id-sales revoke=analysts,auditors"]
    # An agent removed (or all its groups): its CAN_RUN is taken back.
    assert apply({"sales": "analysts"}) == ["revoke-acls space=id-hr revoke=analysts"]
    assert apply({}) == ["revoke-acls space=id-sales revoke=analysts"]
