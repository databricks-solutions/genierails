"""Retiring business_access_enabled must not rebuild an already-released env.

Live dev and prod were released by the code before the retirement, with
business_access_enabled = true. Here that pre-retirement code (from git
history) applies the data_access and workspace modules with mock providers,
then the current code plans against the very same state (terraform test
state_key). A released env must plan no change at all: no grant, policy, mask
function, Genie agent or Genie permission is destroyed, replaced, re-keyed or
even updated. An env applied with the flag false (never released) must plan
its business SELECT / CAN_RUN only through the coverage gate.
"""

import io
import os
import re
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

from tests.terraform_helpers import tf, tf_env, tf_init

ROOT = Path(__file__).parents[2]
SHARED = ROOT / "shared"
# origin/main before the retirement: #70 masks-before-grants, #71 coverage
# gate in Terraform, #72 unified make release; the flag still a control.
PRE_RETIREMENT_COMMIT = "27398e4195c6ba9eaac802d3714ff8a2d3a95dba"

pytestmark = pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")

SPACE_CONFIG = (
    'config = { title = "", description = "", sample_questions = [], instructions = "", '
    "benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], "
    'acl_groups = ["analysts"] }'
)
# A non-empty description makes the legacy module manage the attached agent's
# config, so the upgrade plan exposes the new warehouse trigger behavior.
ATTACHED_SPACE_CONFIG = SPACE_CONFIG.replace('description = ""', 'description = "attached"')
WORKSPACE_SPACES = (
    "{ "
    f'sales = {{ name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], {ATTACHED_SPACE_CONFIG} }}, '
    f'ops = {{ name = "Ops", genie_space_id = "", sql_warehouse_id = "warehouse", uc_tables = ["cat.sch.customers"], {SPACE_CONFIG} }} '
    "}"
)
PROVIDERS = """  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    %s
  }
"""

DATA_ACCESS_TEST = """
mock_provider "databricks" {
  alias = "account"
}
mock_provider "databricks" {
  alias = "workspace"
}
mock_provider "time" {}

variables {
  databricks_account_id     = "account"
  databricks_client_id      = "service-principal"
  databricks_client_secret  = "secret"
  databricks_workspace_host = "https://example.invalid"
  sql_warehouse_id          = "warehouse"
  masking_sql_file          = "tests/fixtures/mask_email.sql"
  deploy_masking_script     = "tests/fixtures/noop_deploy_masking.py"
  auth_file                 = "tests/.tmp/upgrade/data_access/auth.auto.tfvars"
  coverage_gate_file        = "tests/.tmp/upgrade/data_access/.coverage_gate.json"
  coverage_ddl_file         = "tests/.tmp/upgrade/ddl/_fetched.sql"
  groups                    = { analysts = {} }
  uc_tables                 = ["cat.sch.customers"]
  admin_uc_tables           = ["cat.sch.customers"]
  tag_assignments = [{
    entity_type = "columns"
    entity_name = "cat.sch.customers.email"
    tag_key     = "gr_treatment"
    tag_value   = "mask_email"
  }]
  fgac_policies = [{
    name             = "mask_email"
    policy_type      = "POLICY_TYPE_COLUMN_MASK"
    catalog          = "cat"
    to_principals    = ["analysts"]
    match_condition  = "hasTagValue('gr_treatment', 'mask_email')"
    match_alias      = "email"
    function_name    = "mask_email"
    function_catalog = "cat"
    function_schema  = "sch"
  }]
}

run "setup" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/upgrade/ddl/_fetched.sql"                = "CREATE TABLE cat.sch.customers (\\n  id BIGINT,\\n  email STRING\\n);\\n"
      "tests/.tmp/upgrade/data_access/.coverage_gate.json" = null
    }
  }
}

run "legacy_gate_inputs" {
  state_key = "probe"
  command   = plan
  module {
    source = "./legacy/data_access"
  }
PROVIDERS_TIME
  variables {
    business_access_enabled = false
  }
}

run "legacy_gate_passes" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/upgrade/ddl/_fetched.sql"                = "CREATE TABLE cat.sch.customers (\\n  id BIGINT,\\n  email STRING\\n);\\n"
      "tests/.tmp/upgrade/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = run.legacy_gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
    }
  }
}

# --- Already released (flag true) -------------------------------------------
run "released_by_legacy_code" {
  state_key = "released"
  module {
    source = "./legacy/data_access"
  }
PROVIDERS_TIME
  variables {
    business_access_enabled = true
  }
  assert {
    condition     = keys(databricks_grant.table_access) == ["cat.sch.customers|analysts"]
    error_message = "the legacy release must have granted SELECT"
  }
}

run "released_upgrade_plan" {
  state_key = "released"
  command   = plan
PROVIDERS_TIME
  assert {
    condition     = keys(databricks_grant.table_access) == ["cat.sch.customers|analysts"]
    error_message = "the current code must keep the released grant under the same key"
  }
  assert {
    condition     = output.coverage_gate_inputs.fingerprint == run.legacy_gate_inputs.coverage_gate_inputs.fingerprint
    error_message = "the gate fingerprint must not change, so the recorded pass stays valid"
  }
}

run "legacy_gate_expires" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/upgrade/ddl/_fetched.sql"                = "CREATE TABLE cat.sch.customers (\\n  id BIGINT,\\n  email STRING\\n);\\n"
      "tests/.tmp/upgrade/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = run.legacy_gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "2000-01-01T00:00:00Z" })
    }
  }
}

# The usual live case between releases: the gate result has expired.
run "released_upgrade_plan_after_gate_expiry" {
  state_key = "released"
  command   = plan
PROVIDERS_TIME
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts"]
    applied_protection_fingerprint = run.released_by_legacy_code.coverage_gate.protection_fingerprint
  }
  assert {
    condition     = output.coverage_gate.status == "expired" && keys(databricks_grant.table_access) == ["cat.sch.customers|analysts"]
    error_message = "the released grant must stay planned (not revoked) after the gate expires"
  }
}

# --- Never released (flag false) --------------------------------------------
run "never_released_by_legacy_code" {
  state_key = "closed"
  module {
    source = "./legacy/data_access"
  }
PROVIDERS_TIME
  variables {
    business_access_enabled = false
  }
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "the closed legacy apply must have granted nothing"
  }
}

run "never_released_upgrade_without_a_current_gate_withholds_select" {
  state_key = "closed"
  command   = plan
PROVIDERS_TIME
  assert {
    condition     = length(databricks_grant.table_access) == 0 && length(output.withheld_table_grants.grants) == 1
    error_message = "without a current pass the first grant must be withheld"
  }
}

run "gate_passes_again" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/upgrade/ddl/_fetched.sql"                = "CREATE TABLE cat.sch.customers (\\n  id BIGINT,\\n  email STRING\\n);\\n"
      "tests/.tmp/upgrade/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = run.legacy_gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
    }
  }
}

run "never_released_upgrade_grants_through_the_gate" {
  state_key = "closed"
  command   = plan
PROVIDERS_TIME
  assert {
    condition     = keys(databricks_grant.table_access) == ["cat.sch.customers|analysts"]
    error_message = "with a current pass, the never-released env must plan its business SELECT"
  }
}
""".replace("PROVIDERS_TIME\n", PROVIDERS % "time                 = time")

WORKSPACE_TEST = """
mock_provider "databricks" {
  alias = "account"
}
mock_provider "databricks" {
  alias = "workspace"
}
mock_provider "null" {}

override_data {
  target = data.databricks_group.existing
  values = {
    id = 123
  }
}

variables {
  databricks_account_id     = "account"
  databricks_client_id      = "service-principal"
  databricks_client_secret  = "secret"
  databricks_workspace_id   = "123"
  databricks_workspace_host = "https://example.invalid"
  sql_warehouse_id          = "warehouse"
  groups                    = { analysts = {} }
  genie_id_file_prefix      = "tests/.tmp/upgrade/.genie_space_id"
  genie_script_path         = "true"
  genie_spaces              = SPACES
}

# --- Already released (flag true) -------------------------------------------
run "released_by_legacy_code" {
  state_key = "released"
  module {
    source = "./legacy/workspace"
  }
PROVIDERS_NULL
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    business_access_enabled      = true
    genie_exposure_blocker       = ""
    genie_space_can_run_widening = { sales = ["analysts"], ops = ["analysts"] }
    genie_space_missing_grants   = { sales = [], ops = [] }
  }
  assert {
    condition     = keys(null_resource.genie_space_acls) == ["sales"] && keys(null_resource.genie_space_acls_created) == ["ops"]
    error_message = "the legacy release must have granted CAN_RUN on both agents"
  }
}

# What the root computes for the released state: nothing beyond what is
# applied, so no widening.
run "released_upgrade_plan" {
  state_key = "released"
  command   = plan
PROVIDERS_NULL
  variables {
    genie_exposure_blocker       = ""
    genie_space_can_run_widening = { sales = [], ops = [] }
    genie_space_missing_grants   = { sales = [], ops = [] }
  }
  assert {
    condition     = keys(null_resource.genie_space_acls) == ["sales"] && keys(null_resource.genie_space_acls_created) == ["ops"]
    error_message = "the current code must keep both CAN_RUN ACLs under the same keys"
  }
}

run "released_upgrade_plan_while_exposure_is_blocked" {
  state_key = "released"
  command   = plan
PROVIDERS_NULL
  variables {
    genie_exposure_blocker       = "the data_access config changed after its last checked apply"
    genie_space_can_run_widening = { sales = [], ops = [] }
    genie_space_missing_grants   = { sales = [], ops = [] }
  }
}

# --- Never released (flag false) --------------------------------------------
run "never_released_by_legacy_code" {
  state_key = "closed"
  module {
    source = "./legacy/workspace"
  }
PROVIDERS_NULL
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    business_access_enabled      = false
    genie_exposure_blocker       = "the data_access layer was last applied with business_access_enabled = false"
    genie_space_can_run_widening = { sales = ["analysts"], ops = ["analysts"] }
    genie_space_missing_grants   = { sales = [], ops = [] }
  }
  assert {
    condition     = length(null_resource.genie_space_acls) == 0 && length(null_resource.genie_space_acls_created) == 0
    error_message = "the closed legacy apply must have granted no CAN_RUN"
  }
}

run "never_released_upgrade_while_blocked_withholds_can_run" {
  state_key = "closed"
  command   = plan
PROVIDERS_NULL
  variables {
    genie_exposure_blocker       = "the data_access layer has no business table grants in place"
    genie_space_can_run_widening = { sales = ["analysts"], ops = ["analysts"] }
    genie_space_missing_grants   = { sales = [], ops = [] }
  }
  assert {
    condition     = length(null_resource.genie_space_acls) == 0 && length(null_resource.genie_space_acls_created) == 0 && length(output.genie_space_can_run_withheld) == 2
    error_message = "while blocked, both agents' CAN_RUN must be withheld"
  }
}

run "never_released_upgrade_grants_through_the_gate" {
  state_key = "closed"
  command   = plan
PROVIDERS_NULL
  variables {
    genie_exposure_blocker       = ""
    genie_space_can_run_widening = { sales = ["analysts"], ops = ["analysts"] }
    genie_space_missing_grants   = { sales = [], ops = [] }
  }
  assert {
    condition     = keys(null_resource.genie_space_acls) == ["sales"] && keys(null_resource.genie_space_acls_created) == ["ops"]
    error_message = "once the gate allows it, the never-released env must plan CAN_RUN"
  }
}
""".replace("SPACES", WORKSPACE_SPACES).replace("PROVIDERS_NULL\n", PROVIDERS % "null                 = null")

CHANGE = re.compile(r"# (\S+) (will be (?:created|destroyed|updated in-place|read during apply)|must be replaced)")


def _legacy_tree(tmp_path: Path) -> Path:
    """shared/modules as of PRE_RETIREMENT_COMMIT, extracted into tmp_path."""
    present = subprocess.run(["git", "cat-file", "-e", f"{PRE_RETIREMENT_COMMIT}^{{commit}}"],
                             cwd=ROOT, capture_output=True)
    if present.returncode != 0:
        message = (f"pre-retirement commit {PRE_RETIREMENT_COMMIT} is not in this clone "
                   "(shallow checkout?); fetch full history to run the upgrade proof")
        if os.environ.get("REQUIRE_TERRAFORM_TESTS") == "1":
            pytest.fail(message)
        pytest.skip(message)
    archive = subprocess.run(["git", "archive", "--format=tar", PRE_RETIREMENT_COMMIT, "shared/modules"],
                             cwd=ROOT, capture_output=True, check=True)
    legacy = tmp_path / "legacy"
    with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as tar:
        tar.extractall(legacy, filter="data")
    return legacy / "shared" / "modules"


def _sections(output: str) -> dict[str, str]:
    sections: dict[str, str] = {}
    current = None
    for line in output.splitlines():
        match = re.match(r'\s*run "([^"]+)"\.\.\. (\w+)', line)
        if match:
            current = match.group(1)
            sections[current] = match.group(2) + "\n"
        elif current:
            sections[current] += line + "\n"
    return sections


def _upgrade_runs(tmp_path: Path, module: str, test_body: str) -> dict[str, str]:
    """Run test_body in a copy of the current module whose ./legacy/<module>
    is the pre-retirement code; return each run's verbose section."""
    legacy = _legacy_tree(tmp_path)
    current = tmp_path / "current" / "modules"
    shutil.copytree(SHARED / "modules", current,
                    ignore=shutil.ignore_patterns(".terraform", ".tmp", "*.tfstate*"))
    for name in ("sql_tokenizer.py", "masking_sql_blocks.py"):
        shutil.copy(SHARED / name, current.parent / name)
    root = current / module
    # Module sources must sit under the root under test; the legacy modules
    # keep their sibling layout (../coverage_gate_check).
    shutil.copytree(legacy, root / "legacy", ignore=shutil.ignore_patterns("tests"))
    shutil.copytree(SHARED / "roots/data_access/tests/file_writer", root / "tests/file_writer")
    if module == "data_access":
        for name in ("mask_email.sql", "noop_deploy_masking.py"):
            shutil.copy(SHARED / "modules/data_access/tests/fixtures" / name, root / "tests/fixtures" / name)
    # Destroy-time provisioners (test teardown) call ../../scripts/genie_space.sh;
    # a no-op stands in, so nothing ever reaches Databricks.
    stub = current.parent / "scripts" / "genie_space.sh"
    stub.parent.mkdir(parents=True)
    stub.write_text("#!/bin/sh\nexit 0\n")
    for existing in (root / "tests").glob("*.tftest.hcl"):
        existing.unlink()
    (root / "tests/upgrade.tftest.hcl").write_text(test_body)
    env = tf_env(tmp_path)
    tf_init(root, env=env)
    result = tf(root, "test", "-no-color", "-verbose", env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    runs = _sections(result.stdout)
    assert all(section.startswith("pass") for section in runs.values()), result.stdout
    return runs


def _changes(section: str) -> set[tuple[str, str]]:
    return set(CHANGE.findall(section))


def _assert_no_change(section: str, *also_allowed: str, added: frozenset = frozenset(),
                      changes: frozenset = frozenset()) -> None:
    """No resource change at all but creating `added` addresses. The only
    output changes allowed are the data_access outputs dropping their
    business_access_enabled key, needs_gate turning false for grants already
    in place, and output attributes that are only added (what protects the
    grants, what is withheld), plus also_allowed."""
    assert _changes(section) == {
        *((address, "will be created") for address in added), *changes
    }, section
    if "No changes. Your infrastructure matches the configuration." in section:
        return
    if added and not changes:
        assert f"Plan: {len(added)} to add, 0 to change, 0 to destroy." in section, section
        section = section.partition("Changes to Outputs:")[2]
    elif not added and not changes:
        assert "without changing any real infrastructure" in section, section
    else:
        section = section.partition("Changes to Outputs:")[2]
    # Additions (+) are allowed; every removal or change must be one of these.
    diff = [line.strip() for line in section.splitlines() if re.match(r"\s+[-~] ", line)]
    assert set(diff) <= {
        "~ coverage_gate                         = {",
        "~ coverage_gate_inputs                  = {",
        "- business_access_enabled = true",
        "~ needs_gate              = true -> false",
        *also_allowed,
    }, section


def test_released_data_access_state_plans_no_change_after_the_retirement(tmp_path):
    runs = _upgrade_runs(tmp_path, "data_access", DATA_ACCESS_TEST)

    # The normalized trigger replaces the old filemd5 trigger once. That
    # migration is drop-free; the dependent wait only refreshes its ID.
    drop = frozenset({"terraform_data.masking_functions_drop"})
    trigger_upgrade = frozenset({
        ("terraform_data.masking_functions", "must be replaced"),
        ("time_sleep.wait_for_policy_enforcement", "will be updated in-place"),
        # The deployer SP's own grant gains SELECT (verify-access reads as it).
        ('databricks_grant.terraform_sp_manage_catalog["cat"]', "will be updated in-place"),
    })
    _assert_no_change(runs["released_upgrade_plan"], added=drop, changes=trigger_upgrade)
    _assert_no_change(runs["released_upgrade_plan_after_gate_expiry"],
                      '~ status                  = "pass" -> "expired"',
                      "~ needs_gate              = true -> false", added=drop,
                      changes=trigger_upgrade)

    # Never released: withheld without a pass; with one, only business SELECT
    # (and the drop resource) is added and nothing is destroyed or replaced.
    assert runs["never_released_upgrade_without_a_current_gate_withholds_select"].startswith("pass")
    assert _changes(runs["never_released_upgrade_grants_through_the_gate"]) == {
        ('databricks_grant.table_access["cat.sch.customers|analysts"]', "will be created"),
        ("terraform_data.masking_functions_drop", "will be created"),
        *trigger_upgrade,
    }


def test_released_workspace_state_plans_no_change_after_the_retirement(tmp_path):
    runs = _upgrade_runs(tmp_path, "workspace", WORKSPACE_TEST)

    # Adding the effective warehouse to config triggers causes one safe
    # in-place update of the created agent; agents and ACLs are never replaced.
    warehouse_refresh = {
        ('null_resource.genie_space_config["ops"]', "will be updated in-place"),
        ('null_resource.genie_space_config_existing["sales"]', "will be updated in-place"),
    }
    assert _changes(runs["released_upgrade_plan"]) == warehouse_refresh
    assert _changes(runs["released_upgrade_plan_while_exposure_is_blocked"]) == warehouse_refresh
    # The module's top-level warehouse is only a creation default. The attached
    # sales agent has no raw per-space override, so its trigger remains empty
    # and update-config receives no warehouse to send.
    module_source = (SHARED / "modules/workspace/main.tf").read_text()
    existing_block = module_source[
        module_source.index('resource "null_resource" "genie_space_config_existing"'):
        module_source.index("# ── New spaces: create")
    ]
    assert "GENIE_WAREHOUSE_ID       = self.triggers.warehouse_id" in existing_block
    assert "GENIE_WAREHOUSE_EXPLICIT = self.triggers.warehouse_explicit" in existing_block
    assert "GENIE_WAREHOUSE_ID       = each.value.sql_warehouse_id" not in existing_block

    # Never released: withheld while blocked; once the gate allows it, only the
    # CAN_RUN ACLs are added and no agent is replaced.
    assert runs["never_released_upgrade_while_blocked_withholds_can_run"].startswith("pass")
    assert _changes(runs["never_released_upgrade_grants_through_the_gate"]) == {
        ('null_resource.genie_space_acls["sales"]', "will be created"),
        ('null_resource.genie_space_acls_created["ops"]', "will be created"),
        ('null_resource.genie_space_config["ops"]', "will be updated in-place"),
        ('null_resource.genie_space_config_existing["sales"]', "will be updated in-place"),
    }
