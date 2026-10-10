mock_provider "databricks" { alias = "account" }
mock_provider "databricks" { alias = "workspace" }
mock_provider "time" {}

variables {
  databricks_account_id = "account"
  databricks_client_id = ""
  databricks_client_secret = "secret"
  databricks_workspace_host = "https://example.invalid"
  sql_warehouse_id = "warehouse"
  masking_sql_file = "tests/fixtures/mask_email.sql"
  deploy_masking_script = "tests/fixtures/noop_deploy_masking.py"
  auth_file = "tests/fixtures/missing-auth.auto.tfvars"
  coverage_gate_file = "tests/fixtures/missing.json"
  coverage_ddl_file = "tests/fixtures/mask_email.sql"
  tag_assignments = [{
    entity_type = "columns", entity_name = "cat.sch.orders.email",
    tag_key = "gr_treatment", tag_value = "email_partial"
  }]
}

run "deterministic_uses_column_key" {
  command = plan
  providers = { databricks.account = databricks.account, databricks.workspace = databricks.workspace, time = time }
  variables { governance_mode = "deterministic" }
  assert {
    condition = keys(databricks_entity_tag_assignment.treatment) == ["cat.sch.orders.email"] && length(databricks_entity_tag_assignment.assignments) == 0
    error_message = "deterministic treatment tags must have one stable column-keyed resource"
  }
}

run "treatment_value_changes_at_same_address" {
  command = plan
  providers = { databricks.account = databricks.account, databricks.workspace = databricks.workspace, time = time }
  variables {
    governance_mode = "deterministic"
    tag_assignments = [{ entity_type = "columns", entity_name = "cat.sch.orders.email", tag_key = "gr_treatment", tag_value = "redact" }]
  }
  assert {
    condition = keys(databricks_entity_tag_assignment.treatment) == ["cat.sch.orders.email"] && databricks_entity_tag_assignment.treatment["cat.sch.orders.email"].tag_value == "redact"
    error_message = "a treatment change must retain the resource address and update only its value"
  }
}

run "legacy_plan_uses_unchanged_resource" {
  command = plan
  providers = { databricks.account = databricks.account, databricks.workspace = databricks.workspace, time = time }
  assert {
    condition = length(databricks_entity_tag_assignment.treatment) == 0 && keys(databricks_entity_tag_assignment.assignments) == ["columns|cat.sch.orders.email|gr_treatment|email_partial"]
    error_message = "legacy environments must keep their original tag resource plan"
  }
}
