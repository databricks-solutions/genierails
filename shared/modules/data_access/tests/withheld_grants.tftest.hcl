# A failing coverage check never holds up withdrawing access. After a checked
# apply of two grants, with the check now failing:
#   - a revoke still applies after a re-derive changed the protection by
#     adding to it (a new tag, a re-read DDL);
#   - a change that adds one grant and removes another applies the removal
#     and withholds the addition;
#   - keeping a grant whose protection was weakened (a tag or policy removed,
#     a policy changed or no longer masking a kept grantee, a new
#     acknowledgement, a higher max age) still fails the plan.

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
  auth_file                 = "tests/.tmp/withheld/data_access/auth.auto.tfvars"
  coverage_gate_file        = "tests/.tmp/withheld/data_access/.coverage_gate.json"
  coverage_ddl_file         = "tests/.tmp/withheld/ddl/_fetched.sql"
  groups                    = { analysts = {}, auditors = {} }
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
    to_principals    = ["analysts", "auditors"]
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
      "tests/.tmp/withheld/ddl/_fetched.sql" = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
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
}

run "write_passing_gate" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/withheld/ddl/_fetched.sql"                = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n"
      "tests/.tmp/withheld/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
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
    condition     = toset(keys(databricks_grant.table_access)) == toset(["cat.sch.customers|analysts", "cat.sch.customers|auditors"])
    error_message = "the checked apply must make both grants"
  }
}

# A re-derive re-read the DDL (a new column) and the check now fails.
run "rederive_and_gate_fails" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/withheld/ddl/_fetched.sql"                = "CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING,\n  phone STRING\n);\n"
      "tests/.tmp/withheld/data_access/.coverage_gate.json" = jsonencode({ status = "fail", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
    }
  }
}

run "revoke_after_protection_change_applies" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
    applied_protection             = run.first_apply.coverage_gate.protection
    groups                         = { analysts = {} }
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
    tag_assignments = [
      { entity_type = "columns", entity_name = "cat.sch.customers.email", tag_key = "gr_treatment", tag_value = "mask_email" },
      { entity_type = "columns", entity_name = "cat.sch.customers.phone", tag_key = "gr_treatment", tag_value = "mask_email" },
    ]
  }
  assert {
    condition     = output.coverage_gate.protection_fingerprint != run.first_apply.coverage_gate.protection_fingerprint
    error_message = "the re-derive must have changed the protection"
  }
  assert {
    condition     = output.coverage_gate.status != "pass" && toset(keys(databricks_grant.table_access)) == toset(["cat.sch.customers|analysts"])
    error_message = "the revoke must apply and the kept grant stay while the check fails"
  }
  assert {
    condition     = !output.coverage_gate_inputs.needs_gate && length(output.coverage_gate_inputs.new_grants) == 0
    error_message = "a revoke after protection was only added to must not need the gate"
  }
}

run "mixed_add_and_remove_applies_the_removal_and_withholds_the_addition" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
    applied_protection             = run.first_apply.coverage_gate.protection
    groups                         = { analysts = {}, viewers = {} }
    fgac_policies = [{
      name             = "mask_email"
      policy_type      = "POLICY_TYPE_COLUMN_MASK"
      catalog          = "cat"
      to_principals    = ["analysts", "viewers"]
      match_condition  = "hasTagValue('gr_treatment', 'mask_email')"
      match_alias      = "email"
      function_name    = "mask_email"
      function_catalog = "cat"
      function_schema  = "sch"
    }]
  }
  assert {
    condition     = toset(keys(databricks_grant.table_access)) == toset(["cat.sch.customers|analysts"])
    error_message = "auditors must be revoked, analysts kept and viewers withheld"
  }
  assert {
    condition     = toset(output.withheld_table_grants.grants) == toset(["cat.sch.customers|viewers"]) && !output.coverage_gate_inputs.needs_gate
    error_message = "the gate inputs must name the addition and not block the plan"
  }
}

run "changed_mask_condition_still_needs_a_pass" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
    applied_protection             = run.first_apply.coverage_gate.protection
    fgac_policies = [{
      name             = "mask_email"
      policy_type      = "POLICY_TYPE_COLUMN_MASK"
      catalog          = "cat"
      to_principals    = ["analysts", "auditors"]
      match_condition  = "hasTagValue('gr_treatment', 'mask_other')"
      match_alias      = "email"
      function_name    = "mask_email"
      function_catalog = "cat"
      function_schema  = "sch"
    }]
  }
  expect_failures = [databricks_grant.table_access]
}

run "removed_policy_still_needs_a_pass" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
    applied_protection             = run.first_apply.coverage_gate.protection
    groups                         = { analysts = {} }
    fgac_policies                  = []
  }
  expect_failures = [databricks_grant.table_access]
}

# auditors keeps its grant but would no longer be masked.
run "unmasking_a_kept_grantee_still_needs_a_pass" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
    applied_protection             = run.first_apply.coverage_gate.protection
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
  expect_failures = [databricks_grant.table_access]
}

run "removed_tag_still_needs_a_pass" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
    applied_protection             = run.first_apply.coverage_gate.protection
    tag_assignments                = []
  }
  expect_failures = [databricks_grant.table_access]
}

run "new_acknowledgement_still_needs_a_pass" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
    applied_protection             = run.first_apply.coverage_gate.protection
    coverage_acknowledged_columns  = ["cat.sch.customers.phone"]
  }
  expect_failures = [databricks_grant.table_access]
}

run "raised_max_age_still_needs_a_pass" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
    applied_protection             = run.first_apply.coverage_gate.protection
    coverage_gate_max_age          = "12h"
  }
  expect_failures = [databricks_grant.table_access]
}

run "protection_from_another_deployment_exempts_nothing" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    time                 = time
  }
  variables {
    applied_table_grants           = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"]
    applied_protection_fingerprint = run.first_apply.coverage_gate.protection_fingerprint
    applied_protection             = run.first_apply.coverage_gate.protection
    deployment_binding             = "another-deployment"
  }
  expect_failures = [databricks_grant.table_access]
}
