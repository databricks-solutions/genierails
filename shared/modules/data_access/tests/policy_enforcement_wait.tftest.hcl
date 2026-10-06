# The policy-enforcement wait (time_sleep) is keyed on the masking-function
# deployment and the policies: re-applying unchanged inputs must not restart
# it or replace any grant, mask or policy; redeploying the masks restarts it.
# Applies the module with mock providers and a no-op mask deployer.

mock_provider "databricks" {
  alias = "account"
}
mock_provider "databricks" {
  alias = "workspace"
}
mock_provider "time" {
  mock_resource "time_sleep" {
    defaults = {
      id = "2026-01-01T00:00:00Z"
    }
  }
}

# Mocks don't compute replacements, so the re-plans use the real (local-only)
# time provider to show whether the wait would be recreated.
provider "time" {
  alias = "real"
}

variables {
  databricks_account_id     = "account"
  databricks_client_id      = "service-principal"
  databricks_client_secret  = "secret"
  databricks_workspace_host = "https://example.invalid"
  sql_warehouse_id          = "warehouse"
  masking_sql_file          = "tests/.tmp/wait/data_access/masking_functions.sql"
  deploy_masking_script     = "tests/fixtures/noop_deploy_masking.py"
  auth_file                 = "tests/.tmp/wait/data_access/auth.auto.tfvars"
  coverage_gate_file        = "tests/.tmp/wait/data_access/.coverage_gate.json"
  coverage_ddl_file         = "tests/.tmp/wait/ddl/_fetched.sql"
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
  business_access_enabled = true
}

run "setup_inputs" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/wait/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/wait/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
    }
  }
}

run "gate_inputs" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    business_access_enabled = false
  }
}

run "write_passing_gate" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/wait/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/wait/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/wait/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint })
    }
  }
}

run "first_apply" {
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  assert {
    condition     = time_sleep.wait_for_policy_enforcement.triggers["masking_functions_id"] == terraform_data.masking_functions.id
    error_message = "the policy-enforcement wait must be keyed on the masking-function deployment"
  }
  assert {
    condition     = time_sleep.wait_for_policy_enforcement.triggers["policy_hash"] == sha256(jsonencode({ for p in var.fgac_policies : p.name => p }))
    error_message = "the policy-enforcement wait must be keyed on the policies"
  }
  assert {
    condition     = length(databricks_grant.table_access) == 1
    error_message = "the passing gate must allow the business grant"
  }
}

# terraform test can't assert plan actions, so test_data_access_grants.py
# runs this file with -verbose and checks the plans of the next two runs.
run "replan_unchanged_inputs" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time.real
  }
}

run "replan_after_warehouse_change" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time.real
  }
  variables {
    sql_warehouse_id = "another-warehouse"
  }
}
