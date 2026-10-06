# The data_access module plans business SELECT only while the coverage-gate
# result records a pass for the exact inputs Terraform sees. These runs test
# the module directly so the precondition on table_access is expectable.

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
  masking_sql_file          = "tests/.tmp/gate/data_access/masking_functions.sql"
  deploy_masking_script     = "tests/fixtures/noop_deploy_masking.py"
  auth_file                 = "tests/.tmp/gate/data_access/auth.auto.tfvars"
  coverage_gate_file        = "tests/.tmp/gate/data_access/.coverage_gate.json"
  coverage_ddl_file         = "tests/.tmp/gate/ddl/_fetched.sql"
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
  # Results below record a fixed refresh time; a century-long max age keeps
  # them valid whatever the wall clock says (expiry has its own runs).
  coverage_gate_max_age = "876000h"
}

run "setup_inputs" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = null
    }
  }
}

run "closed_gate_plans_without_a_gate_result" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    business_access_enabled = false
  }
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "business_access_enabled = false must plan no business SELECT"
  }
  assert {
    condition     = toset(output.coverage_gate_inputs.grant_tables) == toset(["cat.sch.customers"])
    error_message = "the gate inputs must name the tables the open gate would grant"
  }
}

run "missing_gate_result_blocks_select" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  expect_failures = [databricks_grant.table_access]
}

run "write_passing_gate" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.closed_gate_plans_without_a_gate_result.coverage_gate_inputs.fingerprint, refreshed_at = "2026-01-01T00:00:00Z" })
    }
  }
}

run "current_passing_gate_allows_select" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  assert {
    condition     = toset(keys(databricks_grant.table_access)) == toset(["cat.sch.customers|analysts"])
    error_message = "a current pass must plan the business SELECT grant"
  }
  assert {
    condition     = output.coverage_gate.status == "pass" && output.coverage_gate.business_access_enabled
    error_message = "the applied gate status recorded in state must be pass"
  }
  assert {
    condition     = output.coverage_gate_inputs.fingerprint == run.closed_gate_plans_without_a_gate_result.coverage_gate_inputs.fingerprint
    error_message = "opening business_access_enabled must not change the gate fingerprint"
  }
}

run "changed_tags_make_the_gate_stale" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    tag_assignments = []
  }
  expect_failures = [databricks_grant.table_access]
}

run "changed_policies_make_the_gate_stale" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    fgac_policies = []
  }
  expect_failures = [databricks_grant.table_access]
}

run "new_grant_principal_makes_the_gate_stale" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    groups = { analysts = {}, auditors = {} }
  }
  expect_failures = [databricks_grant.table_access]
}

run "acknowledgement_makes_the_gate_stale" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    coverage_acknowledged_columns = ["cat.sch.customers.id"]
  }
  expect_failures = [databricks_grant.table_access]
}

run "changed_ddl_and_masks_make_the_gate_stale" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING,\n  phone STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.closed_gate_plans_without_a_gate_result.coverage_gate_inputs.fingerprint, refreshed_at = "2026-01-01T00:00:00Z" })
    }
  }
}

run "new_ddl_column_blocks_select_until_regated" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  expect_failures = [databricks_grant.table_access]
}

run "write_failed_gate" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "fail", fingerprint = run.closed_gate_plans_without_a_gate_result.coverage_gate_inputs.fingerprint, refreshed_at = "2026-01-01T00:00:00Z" })
    }
  }
}

run "failed_gate_blocks_select_even_for_current_inputs" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  expect_failures = [databricks_grant.table_access]
}

run "write_unreadable_gate" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = "{not json"
    }
  }
}

run "unreadable_gate_blocks_select" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  expect_failures = [databricks_grant.table_access]
}

# Terraform can't re-read Unity Catalog, so a pass must carry the time make
# last refreshed live tags and DDL, and that refresh must be recent.
run "write_pass_without_a_live_refresh" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.closed_gate_plans_without_a_gate_result.coverage_gate_inputs.fingerprint })
    }
  }
}

run "never_refreshed_pass_blocks_select" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  expect_failures = [databricks_grant.table_access]
}

run "write_pass_from_an_old_refresh" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.closed_gate_plans_without_a_gate_result.coverage_gate_inputs.fingerprint, refreshed_at = "2000-01-01T00:00:00Z" })
    }
  }
}

run "pass_older_than_the_max_age_blocks_select" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    coverage_gate_max_age = "6h"
  }
  expect_failures = [databricks_grant.table_access]
}

run "old_refresh_within_a_longer_max_age_allows_select" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  assert {
    condition     = length(databricks_grant.table_access) == 1 && output.coverage_gate.status == "pass"
    error_message = "the max age is the only bound on the refresh time"
  }
}

run "write_pass_with_a_future_refresh" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.closed_gate_plans_without_a_gate_result.coverage_gate_inputs.fingerprint, refreshed_at = "2999-01-01T00:00:00Z" })
    }
  }
}

run "future_refresh_time_blocks_select" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  expect_failures = [databricks_grant.table_access]
}

run "malformed_max_age_is_rejected" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    coverage_gate_max_age = "six hours"
  }
  expect_failures = [var.coverage_gate_max_age]
}
