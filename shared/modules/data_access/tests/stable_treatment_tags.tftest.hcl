mock_provider "databricks" { alias = "account" }
mock_provider "databricks" { alias = "workspace" }
mock_provider "time" {}

variables {
  databricks_account_id     = "account"
  databricks_client_id      = ""
  databricks_client_secret  = "secret"
  databricks_workspace_host = "https://example.invalid"
  sql_warehouse_id          = "warehouse"
  masking_sql_file          = "tests/fixtures/mask_email.sql"
  deploy_masking_script     = "tests/fixtures/noop_deploy_masking.py"
  auth_file                 = "tests/fixtures/missing-auth.auto.tfvars"
  coverage_gate_file        = "tests/fixtures/missing.json"
  coverage_ddl_file         = "tests/fixtures/mask_email.sql"
  tag_assignments = [
    { entity_type = "columns", entity_name = "cat.sch.orders.email", tag_key = "gr_treatment", tag_value = "email_partial" },
    { entity_type = "tables", entity_name = "cat.sch.orders", tag_key = "row_scope", tag_value = "anz" },
  ]
}

run "deterministic_keys_by_column_and_tag_key" {
  command   = plan
  providers = { databricks.account = databricks.account, databricks.workspace = databricks.workspace, time = time }
  variables { governance_mode = "deterministic" }
  assert {
    condition     = keys(databricks_entity_tag_assignment.treatment) == ["columns|cat.sch.orders.email|gr_treatment"]
    error_message = "deterministic treatment tags are keyed entity_type|entity_name|tag_key, never by value"
  }
  assert {
    condition     = keys(databricks_entity_tag_assignment.assignments) == ["tables|cat.sch.orders|row_scope|anz"]
    error_message = "non-treatment tags stay on the original resource"
  }
}

run "treatment_value_changes_at_same_address" {
  command   = plan
  providers = { databricks.account = databricks.account, databricks.workspace = databricks.workspace, time = time }
  variables {
    governance_mode = "deterministic"
    tag_assignments = [{ entity_type = "columns", entity_name = "cat.sch.orders.email", tag_key = "gr_treatment", tag_value = "redact" }]
  }
  assert {
    condition     = databricks_entity_tag_assignment.treatment["columns|cat.sch.orders.email|gr_treatment"].tag_value == "redact"
    error_message = "a treatment change must keep the resource address and update only its value"
  }
}

# A legacy env applied with the module before stable tags ...
run "legacy_apply_with_old_module" {
  command   = apply
  state_key = "legacy_upgrade"
  providers = { databricks.account = databricks.account, databricks.workspace = databricks.workspace, time = time }
  module { source = "./tests/fixtures/pre_stable_tags" }
}

# ... re-plans with the current module on the same state with no changes
# (test_sticky_governance.py checks the verbose plan says so).
run "legacy_replan_with_new_module" {
  command   = plan
  state_key = "legacy_upgrade"
  providers = { databricks.account = databricks.account, databricks.workspace = databricks.workspace, time = time }
  assert {
    condition     = length(databricks_entity_tag_assignment.treatment) == 0 && length(databricks_entity_tag_assignment.assignments) == 2
    error_message = "legacy environments must keep every tag on the original resource"
  }
}
