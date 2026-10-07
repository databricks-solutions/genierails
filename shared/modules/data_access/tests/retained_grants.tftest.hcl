# Keeping or revoking SELECT never needs the gate: after a gated apply, a grant
# that already exists stays plannable when the gate result later expires or
# fails, as long as its protection (tags, policies, masks, DDL, acks, max age)
# is unchanged. New grants, or a changed protection, still need a pass.
# The masking SQL is a committed fixture: the applied module needs it again
# at teardown, after the file writer has cleaned up.

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
  auth_file                 = "tests/.tmp/retain/data_access/auth.auto.tfvars"
  coverage_gate_file        = "tests/.tmp/retain/data_access/.coverage_gate.json"
  coverage_ddl_file         = "tests/.tmp/retain/ddl/_fetched.sql"
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
      "tests/.tmp/retain/ddl/_fetched.sql" = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
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
  # No gate result yet: business SELECT is refused, and the gate inputs are
  # still readable.
  expect_failures = [databricks_grant.table_access]
}

run "write_passing_gate" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/retain/ddl/_fetched.sql"                = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/retain/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
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
    condition     = length(databricks_grant.table_access) == 1
    error_message = "the gated apply must make the grant"
  }
}

run "gate_expires" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/retain/ddl/_fetched.sql"                = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/retain/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "2000-01-01T00:00:00Z" })
    }
  }
}

# test_data_access_grants.py checks this re-plan has no resource changes.
run "replan_unchanged_grant_after_expiry" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
  }
  assert {
    condition     = output.coverage_gate.status == "expired" && length(databricks_grant.table_access) == 1
    error_message = "an unchanged existing grant must stay plannable after the gate expires"
  }
  assert {
    condition     = !output.coverage_gate_inputs.needs_gate
    error_message = "keeping existing grants with unchanged protection needs no gate"
  }
}

run "widened_grant_after_expiry_blocks" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
    groups                         = { analysts = {}, auditors = {} }
  }
  expect_failures = [databricks_grant.table_access]
}

run "removed_mask_after_expiry_blocks_existing_grant" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
    fgac_policies                  = []
  }
  expect_failures = [databricks_grant.table_access]
}

run "unknown_applied_protection_gets_no_exemption" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts"]
    applied_protection_fingerprint = ""
  }
  expect_failures = [databricks_grant.table_access]
}

run "gate_fails" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/retain/ddl/_fetched.sql"                = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/retain/data_access/.coverage_gate.json" = jsonencode({ status = "fail", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
    }
  }
}

run "existing_grant_survives_a_failed_gate" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
  }
  assert {
    condition     = output.coverage_gate.status == "failed" && length(databricks_grant.table_access) == 1
    error_message = "a failing gate must not block keeping an existing grant whose protection is unchanged"
  }
}

run "revocation_plans_with_a_failed_gate" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
    uc_tables                      = []
    admin_uc_tables                = []
  }
  assert {
    condition     = length(databricks_grant.table_access) == 0
    error_message = "revoking SELECT must plan whatever the gate says"
  }
}

run "gate_missing" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/retain/ddl/_fetched.sql" = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
    }
  }
}

run "existing_grant_survives_a_missing_gate" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
  }
  assert {
    condition     = output.coverage_gate.status == "missing" && length(databricks_grant.table_access) == 1
    error_message = "a missing gate result must not block keeping an existing grant"
  }
}
