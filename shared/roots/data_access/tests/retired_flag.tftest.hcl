# business_access_enabled is retired: still declared for one release so
# existing env.auto.tfvars files and -var flags keep working, but ignored.
# true, false and unset plan the same business SELECT through the coverage
# gate; false in particular never revokes it. (make prints the deprecation
# warning; see test_exposure_gating.py.)

mock_provider "databricks" {}
mock_provider "null" {}
mock_provider "time" {}

variables {
  env_dir                   = "tests/.tmp/retired/data_access"
  databricks_account_id     = "account"
  databricks_client_id      = "service-principal"
  databricks_client_secret  = "secret"
  databricks_workspace_host = "https://example.invalid"
  sql_warehouse_id          = "warehouse"
  uc_tables                 = ["cat.sch.customers"]
  groups                    = { analysts = {} }
}

run "setup" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/retired/data_access/masking_functions.sql" = "SELECT 1;\n"
      "tests/.tmp/retired/data_access/.coverage_gate.json"   = null
    }
  }
}

# Refresh-only: plans no grants, so the gate inputs read without a gate result.
run "gate_inputs" {
  command = plan
  plan_options {
    mode = refresh-only
  }
  expect_failures = [check.business_select_withheld]
}

run "gate_passes" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/retired/data_access/masking_functions.sql" = "SELECT 1;\n"
      "tests/.tmp/retired/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
    }
  }
}

run "unset_grants_through_the_gate" {
  command = plan
  assert {
    condition     = output.table_grant_resource_keys == ["cat.sch.customers|analysts"]
    error_message = "with a current pass, business SELECT must be planned without any flag"
  }
}

run "false_does_not_revoke" {
  command = plan
  variables {
    business_access_enabled = false
  }
  assert {
    condition     = output.table_grant_resource_keys == ["cat.sch.customers|analysts"]
    error_message = "business_access_enabled = false must not withhold or revoke business SELECT"
  }
  assert {
    condition     = output.coverage_gate_inputs.fingerprint == run.gate_inputs.coverage_gate_inputs.fingerprint
    error_message = "the retired flag must not change the gate fingerprint"
  }
}

run "true_changes_nothing" {
  command = plan
  variables {
    business_access_enabled = true
  }
  assert {
    condition     = output.table_grant_resource_keys == ["cat.sch.customers|analysts"]
    error_message = "business_access_enabled = true must plan exactly what unset plans"
  }
  assert {
    condition     = output.coverage_gate_inputs.fingerprint == run.gate_inputs.coverage_gate_inputs.fingerprint
    error_message = "the retired flag must not change the gate fingerprint"
  }
}
