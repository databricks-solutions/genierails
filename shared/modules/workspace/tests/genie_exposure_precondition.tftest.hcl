# While the root reports a genie_exposure_blocker, or a space's CAN_RUN groups
# lack their SELECT grants (genie_space_missing_grants), the module withholds
# the groups that ACL adds beyond what is applied (new or existing space),
# without failing the plan: an empty, unchanged or shrunk ACL, and every other
# space's change, still apply.
#
# Teardown destroys the applied ACL, whose destroy-time provisioner runs
# ../../scripts/genie_space.sh revoke-acls: run this through pytest
# (shared/tests), which tests a copy with that script stubbed.

mock_provider "databricks" {
  alias = "account"
}
mock_provider "databricks" {
  alias = "workspace"
}
mock_provider "null" {}

variables {
  databricks_account_id     = "account"
  databricks_client_id      = "service-principal"
  databricks_client_secret  = "secret"
  databricks_workspace_id   = "123"
  databricks_workspace_host = "https://example.invalid"
  sql_warehouse_id          = "warehouse"
  groups                    = { analysts = {} }
}

run "blocked_exposure_withholds_can_run_on_existing_space" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix         = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path            = "true"
    genie_exposure_blocker       = "the data_access layer has no readable state"
    genie_space_can_run_widening = { sales = ["analysts"] }
    genie_space_missing_grants   = { sales = [] }
    genie_spaces                 = { sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts"] } } }
  }
  assert {
    condition     = !output.genie_space_acls_applied && toset(output.genie_space_can_run_withheld["sales"]) == toset(["analysts"])
    error_message = "blocked exposure must withhold CAN_RUN on an existing space"
  }
  assert {
    condition     = toset(output.genie_space_acl_removal_only) == toset(["sales"])
    error_message = "fully withheld existing/adopted agents must still sync an empty ACL to remove hand-added access"
  }
}

run "blocked_exposure_withholds_can_run_on_new_space" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix         = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path            = "true"
    genie_exposure_blocker       = "the data_access layer has no readable state"
    genie_space_can_run_widening = { sales = ["analysts"] }
    genie_space_missing_grants   = { sales = [] }
    genie_spaces                 = { sales = { name = "Sales", genie_space_id = "", sql_warehouse_id = "warehouse", uc_tables = ["cat.sch.customers"], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts"] } } }
  }
  assert {
    condition     = !output.genie_space_acls_applied && toset(output.genie_space_can_run_withheld["sales"]) == toset(["analysts"])
    error_message = "blocked exposure must withhold CAN_RUN on a new space"
  }
  assert {
    condition     = toset(output.genie_space_acl_removal_only) == toset(["sales"])
    error_message = "a title-adopted agent on the create path must still remove hand-added access while all grants are withheld"
  }
}

run "blocked_exposure_still_clears_can_run" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix         = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path            = "true"
    genie_exposure_blocker       = "the data_access layer has no readable state"
    genie_space_can_run_widening = { sales = [] }
    genie_space_missing_grants   = { sales = [] }
    genie_spaces                 = { sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = [] } } }
  }
  assert {
    condition     = output.genie_space_acls_applied
    error_message = "an empty ACL must still be applied to clear CAN_RUN while exposure is blocked"
  }
}

run "missing_space_grants_withhold_can_run" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix         = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path            = "true"
    genie_exposure_blocker       = ""
    genie_space_can_run_widening = { sales = ["analysts"] }
    genie_space_missing_grants   = { sales = ["cat.sch.customers|analysts"] }
    genie_spaces                 = { sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts"] } } }
  }
  assert {
    condition     = !output.genie_space_acls_applied && toset(output.genie_space_can_run_withheld["sales"]) == toset(["analysts"])
    error_message = "missing SELECT grants must withhold CAN_RUN"
  }
}

run "space_absent_from_the_grant_check_withholds_can_run" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix         = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path            = "true"
    genie_exposure_blocker       = ""
    genie_space_can_run_widening = { sales = ["analysts"] }
    genie_space_missing_grants   = {}
    genie_spaces                 = { sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts"] } } }
  }
  assert {
    condition     = !output.genie_space_acls_applied && toset(output.genie_space_can_run_withheld["sales"]) == toset(["analysts"])
    error_message = "a space absent from the grant check must withhold CAN_RUN"
  }
}

run "ready_layer_and_space_grants_allow_can_run" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix         = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path            = "true"
    genie_exposure_blocker       = ""
    genie_space_can_run_widening = { sales = ["analysts"] }
    genie_space_missing_grants   = { sales = [] }
    genie_spaces                 = { sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts"] } } }
  }
  assert {
    condition     = output.genie_space_acls_applied
    error_message = "with the layer ready and the space's grants in place, CAN_RUN must be planned"
  }
}

run "blocked_exposure_keeps_an_unchanged_acl" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix         = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path            = "true"
    genie_exposure_blocker       = "the data_access layer has no readable state"
    genie_space_can_run_widening = { sales = [] }
    genie_space_missing_grants   = { sales = [] }
    genie_spaces                 = { sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts"] } } }
  }
  assert {
    condition     = output.genie_space_acls_applied
    error_message = "an ACL that adds nothing to what is applied must plan while exposure is blocked"
  }
}

run "space_absent_from_the_widening_map_counts_as_widening" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix         = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path            = "true"
    genie_exposure_blocker       = "the data_access layer has no readable state"
    genie_space_can_run_widening = {}
    genie_space_missing_grants   = { sales = [] }
    genie_spaces                 = { sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts"] } } }
  }
  assert {
    condition     = !output.genie_space_acls_applied && toset(output.genie_space_can_run_withheld["sales"]) == toset(["analysts"])
    error_message = "a space absent from the widening map must withhold every desired group"
  }
}

# Apply, let the gate expire, re-plan: the unchanged ACL plans (test_data_access_grants.py
# checks it is a no-op); widening it still withholds the new group.
run "apply_while_exposure_is_ready" {
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix         = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path            = "true"
    genie_exposure_blocker       = ""
    genie_space_can_run_widening = { sales = ["analysts"] }
    genie_space_missing_grants   = { sales = [] }
    genie_spaces                 = { sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts"] } } }
  }
  assert {
    condition     = output.genie_space_acls_applied
    error_message = "with the layer ready and the space's grants in place, CAN_RUN must be planned"
  }
}

run "replan_unchanged_acl_after_the_gate_expires" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix         = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path            = "true"
    genie_exposure_blocker       = "the data_access layer has no readable state"
    genie_space_can_run_widening = { sales = [] }
    genie_space_missing_grants   = { sales = [] }
    genie_spaces                 = { sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts"] } } }
  }
}

run "widened_acl_after_the_gate_expires_keeps_only_the_applied_groups" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix         = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path            = "true"
    genie_exposure_blocker       = "the data_access layer has no readable state"
    groups                       = { analysts = {}, auditors = {} }
    genie_space_can_run_widening = { sales = ["auditors"] }
    genie_space_missing_grants   = { sales = [] }
    genie_spaces                 = { sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts", "auditors"] } } }
  }
  assert {
    condition     = null_resource.genie_space_acls["sales"].triggers.groups == "analysts" && toset(output.genie_space_can_run_withheld["sales"]) == toset(["auditors"])
    error_message = "the ACL must keep the applied group and withhold the new one"
  }
}

# One space's withheld widening doesn't hold up another's revocation.
run "withheld_widening_does_not_hold_up_another_spaces_removal" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix         = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path            = "true"
    genie_exposure_blocker       = "the data_access layer was last applied without a passing coverage check"
    groups                       = { analysts = {}, auditors = {} }
    genie_space_can_run_widening = { sales = ["auditors"], hr = [] }
    genie_space_missing_grants   = { sales = [], hr = [] }
    genie_spaces = {
      sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["auditors"] } }
      hr    = { name = "HR", genie_space_id = "space-2", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts"] } }
    }
  }
  assert {
    condition     = keys(null_resource.genie_space_acls) == ["hr"] && null_resource.genie_space_acls["hr"].triggers.groups == "analysts"
    error_message = "the other space's shrunk ACL must still be planned"
  }
  assert {
    condition     = keys(output.genie_space_can_run_withheld) == ["sales"]
    error_message = "only the widening space is withheld"
  }
}
