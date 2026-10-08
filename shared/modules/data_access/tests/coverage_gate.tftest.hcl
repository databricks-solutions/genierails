# The data_access module plans business SELECT only while the coverage-gate
# result records a pass for the exact inputs Terraform sees. Without one, a
# new grant is withheld (left out of the plan, and named in
# withheld_table_grants). These runs test the module directly so that is expectable.

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

# There is no exposure switch: without a gate result, business SELECT is
# refused, and the gate inputs are still readable (they come from config).
run "missing_gate_result_blocks_select" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
  }
}

run "write_passing_gate" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.missing_gate_result_blocks_select.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
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
    condition     = output.coverage_gate.status == "pass"
    error_message = "the applied gate status recorded in state must be pass"
  }
  assert {
    condition     = toset(output.coverage_gate_inputs.grant_tables) == toset(["cat.sch.customers"])
    error_message = "the gate inputs must name the tables the gate would grant"
  }
  assert {
    condition     = output.coverage_gate_inputs.fingerprint == run.missing_gate_result_blocks_select.coverage_gate_inputs.fingerprint
    error_message = "the gate result must not change the gate fingerprint"
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
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
  }
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
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
  }
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
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
  }
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
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
  }
}

run "changed_ddl_and_masks_make_the_gate_stale" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING,\n  phone STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.missing_gate_result_blocks_select.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
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
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
  }
}

run "write_failed_gate" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "fail", fingerprint = run.missing_gate_result_blocks_select.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
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
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
  }
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
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
  }
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
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.missing_gate_result_blocks_select.coverage_gate_inputs.fingerprint })
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
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
  }
}

run "write_pass_from_an_old_refresh" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.missing_gate_result_blocks_select.coverage_gate_inputs.fingerprint, refreshed_at = "2000-01-01T00:00:00Z" })
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
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
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
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.missing_gate_result_blocks_select.coverage_gate_inputs.fingerprint, refreshed_at = "2999-01-01T00:00:00Z" })
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
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
  }
}

# The max age is a gate input with a hard 24h ceiling, so a raw -var or
# TF_VAR_ override can't revive an expired pass: raising it makes the result
# stale, and a re-gated result still expires from its refresh time.
run "write_fresh_pass_at_the_default_max_age" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.missing_gate_result_blocks_select.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
    }
  }
}

run "fresh_pass_allows_select" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  assert {
    condition     = length(databricks_grant.table_access) == 1 && output.coverage_gate.status == "pass"
    error_message = "a pass refreshed at plan time, at the default max age, must allow SELECT"
  }
}

run "raising_the_max_age_makes_the_pass_stale" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    coverage_gate_max_age = "12h"
  }
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
  }
}

run "gate_inputs_at_the_ceiling" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    coverage_gate_max_age = "24h"
  }
  # The recorded pass is for the 6h inputs, so this plan refuses SELECT.
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
  }
  assert {
    condition     = output.coverage_gate_inputs.fingerprint != run.missing_gate_result_blocks_select.coverage_gate_inputs.fingerprint
    error_message = "the max age must be part of the gate fingerprint"
  }
}

run "write_regated_pass_from_an_old_refresh" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/gate/data_access/masking_functions.sql" = "CREATE OR REPLACE FUNCTION cat.sch.mask_email(v STRING) RETURNS STRING RETURN '***';\n"
      "tests/.tmp/gate/ddl/_fetched.sql"                  = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/gate/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.gate_inputs_at_the_ceiling.coverage_gate_inputs.fingerprint, refreshed_at = "2000-01-01T00:00:00Z" })
    }
  }
}

run "ceiling_max_age_still_expires_an_old_refresh" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    coverage_gate_max_age = "24h"
  }
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "without a current pass the new grant must be withheld"
  }
}

run "max_age_above_the_ceiling_is_rejected" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    coverage_gate_max_age = "876000h"
  }
  expect_failures = [var.coverage_gate_max_age]
}

run "max_age_just_above_the_ceiling_is_rejected" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    coverage_gate_max_age = "24h1s"
  }
  expect_failures = [var.coverage_gate_max_age]
}

run "max_age_zero_is_rejected" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    coverage_gate_max_age = "0s"
  }
  expect_failures = [var.coverage_gate_max_age]
}

run "max_age_negative_is_rejected" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    coverage_gate_max_age = "-1h"
  }
  expect_failures = [var.coverage_gate_max_age]
}

run "max_age_malformed_is_rejected" {
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
