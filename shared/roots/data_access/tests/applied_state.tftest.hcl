# The root's reading of its own state (what the last apply left in place),
# in the shape Terraform writes it. Only a current, successfully applied grant
# recorded for this deployment (workspace host + ID) is kept, and with
# unchanged protection is exempt from the gate (needs_gate = false); deposed,
# tainted, foreign or pre-binding records count as not applied (a new grant,
# in new_grants), and an applied grant without a recorded protection needs
# the gate. Runs plan with a passing gate so the plan succeeds and the gate
# inputs can be read.

mock_provider "databricks" {}
mock_provider "null" {}
mock_provider "time" {}

variables {
  env_dir                   = "tests/.tmp/applied/data_access"
  databricks_account_id     = "account"
  databricks_client_id      = "service-principal"
  databricks_client_secret  = "secret"
  databricks_workspace_host = "https://example.invalid"
  databricks_workspace_id   = "123"
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
      "tests/.tmp/applied/data_access/masking_functions.sql" = "SELECT 1;\n"
      "tests/.tmp/applied/data_access/terraform.tfstate"     = null
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

run "gate_inputs_elsewhere" {
  command = plan
  plan_options {
    mode = refresh-only
  }
  expect_failures = [check.business_select_withheld]
  variables {
    databricks_workspace_host = "https://other.invalid"
  }
}

run "current_grant_for_this_deployment_is_kept__state" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/applied/data_access/masking_functions.sql" = "SELECT 1;\n"
      "tests/.tmp/applied/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
      "tests/.tmp/applied/data_access/terraform.tfstate"     = jsonencode({ version = 4, outputs = { coverage_gate = { value = { status = "pass", fingerprint = "applied", max_age = "6h", table_grant_count = 1, protection_fingerprint = run.gate_inputs.coverage_gate_inputs.protection_fingerprint, deployment_binding = run.gate_inputs.coverage_gate_inputs.deployment_binding } } }, resources = [{ module = "module.data_access", mode = "managed", type = "databricks_grant", name = "table_access", provider = "provider[\"registry.terraform.io/databricks/databricks\"].workspace", instances = [{ index_key = "cat.sch.customers|analysts", schema_version = 0, attributes = { id = "cat.sch.customers|analysts" } }] }] })
    }
  }
}

run "current_grant_for_this_deployment_is_kept" {
  command = plan
  assert {
    condition     = output.coverage_gate_inputs.needs_gate == false && length(output.coverage_gate_inputs.new_grants) == 0
    error_message = "a current grant recorded for this deployment with unchanged protection needs no gate"
  }
}

run "deposed_grant_is_not_applied__state" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/applied/data_access/masking_functions.sql" = "SELECT 1;\n"
      "tests/.tmp/applied/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
      "tests/.tmp/applied/data_access/terraform.tfstate"     = jsonencode({ version = 4, outputs = { coverage_gate = { value = { status = "pass", fingerprint = "applied", max_age = "6h", table_grant_count = 1, protection_fingerprint = run.gate_inputs.coverage_gate_inputs.protection_fingerprint, deployment_binding = run.gate_inputs.coverage_gate_inputs.deployment_binding } } }, resources = [{ module = "module.data_access", mode = "managed", type = "databricks_grant", name = "table_access", provider = "provider[\"registry.terraform.io/databricks/databricks\"].workspace", instances = [{ index_key = "cat.sch.customers|analysts", deposed = "00000001", schema_version = 0, attributes = { id = "cat.sch.customers|analysts" } }] }] })
    }
  }
}

run "deposed_grant_is_not_applied" {
  command = plan
  assert {
    condition     = contains(output.coverage_gate_inputs.new_grants, "cat.sch.customers|analysts")
    error_message = "a deposed object left by a failed replacement is not an applied grant"
  }
}

run "current_and_deposed_together_still_count_once__state" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/applied/data_access/masking_functions.sql" = "SELECT 1;\n"
      "tests/.tmp/applied/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
      "tests/.tmp/applied/data_access/terraform.tfstate"     = jsonencode({ version = 4, outputs = { coverage_gate = { value = { status = "pass", fingerprint = "applied", max_age = "6h", table_grant_count = 1, protection_fingerprint = run.gate_inputs.coverage_gate_inputs.protection_fingerprint, deployment_binding = run.gate_inputs.coverage_gate_inputs.deployment_binding } } }, resources = [{ module = "module.data_access", mode = "managed", type = "databricks_grant", name = "table_access", provider = "provider[\"registry.terraform.io/databricks/databricks\"].workspace", instances = [{ index_key = "cat.sch.customers|analysts", schema_version = 0, attributes = { id = "cat.sch.customers|analysts" } }, { index_key = "cat.sch.customers|analysts", deposed = "00000001", schema_version = 0, attributes = { id = "cat.sch.customers|analysts" } }] }] })
    }
  }
}

run "current_and_deposed_together_still_count_once" {
  command = plan
  assert {
    condition     = output.coverage_gate_inputs.needs_gate == false && length(output.coverage_gate_inputs.new_grants) == 0
    error_message = "a current object next to a deposed one is still the applied grant (no duplicate-key error)"
  }
}

run "tainted_grant_is_not_applied__state" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/applied/data_access/masking_functions.sql" = "SELECT 1;\n"
      "tests/.tmp/applied/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
      "tests/.tmp/applied/data_access/terraform.tfstate"     = jsonencode({ version = 4, outputs = { coverage_gate = { value = { status = "pass", fingerprint = "applied", max_age = "6h", table_grant_count = 1, protection_fingerprint = run.gate_inputs.coverage_gate_inputs.protection_fingerprint, deployment_binding = run.gate_inputs.coverage_gate_inputs.deployment_binding } } }, resources = [{ module = "module.data_access", mode = "managed", type = "databricks_grant", name = "table_access", provider = "provider[\"registry.terraform.io/databricks/databricks\"].workspace", instances = [{ index_key = "cat.sch.customers|analysts", status = "tainted", schema_version = 0, attributes = { id = "cat.sch.customers|analysts" } }] }] })
    }
  }
}

run "tainted_grant_is_not_applied" {
  command = plan
  assert {
    condition     = contains(output.coverage_gate_inputs.new_grants, "cat.sch.customers|analysts")
    error_message = "a tainted grant is not an applied grant"
  }
}

run "state_copied_from_another_host_is_not_applied__state" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/applied/data_access/masking_functions.sql" = "SELECT 1;\n"
      "tests/.tmp/applied/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
      "tests/.tmp/applied/data_access/terraform.tfstate"     = jsonencode({ version = 4, outputs = { coverage_gate = { value = { status = "pass", fingerprint = "applied", max_age = "6h", table_grant_count = 1, protection_fingerprint = run.gate_inputs_elsewhere.coverage_gate_inputs.protection_fingerprint, deployment_binding = run.gate_inputs_elsewhere.coverage_gate_inputs.deployment_binding } } }, resources = [{ module = "module.data_access", mode = "managed", type = "databricks_grant", name = "table_access", provider = "provider[\"registry.terraform.io/databricks/databricks\"].workspace", instances = [{ index_key = "cat.sch.customers|analysts", schema_version = 0, attributes = { id = "cat.sch.customers|analysts" } }] }] })
    }
  }
}

run "state_copied_from_another_host_is_not_applied" {
  command = plan
  assert {
    condition     = contains(output.coverage_gate_inputs.new_grants, "cat.sch.customers|analysts")
    error_message = "a state recorded for another workspace host exempts nothing"
  }
}

run "foreign_binding_alone_exempts_nothing__state" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/applied/data_access/masking_functions.sql" = "SELECT 1;\n"
      "tests/.tmp/applied/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
      "tests/.tmp/applied/data_access/terraform.tfstate"     = jsonencode({ version = 4, outputs = { coverage_gate = { value = { status = "pass", fingerprint = "applied", max_age = "6h", table_grant_count = 1, protection_fingerprint = run.gate_inputs.coverage_gate_inputs.protection_fingerprint, deployment_binding = run.gate_inputs_elsewhere.coverage_gate_inputs.deployment_binding } } }, resources = [{ module = "module.data_access", mode = "managed", type = "databricks_grant", name = "table_access", provider = "provider[\"registry.terraform.io/databricks/databricks\"].workspace", instances = [{ index_key = "cat.sch.customers|analysts", schema_version = 0, attributes = { id = "cat.sch.customers|analysts" } }] }] })
    }
  }
}

run "foreign_binding_alone_exempts_nothing" {
  command = plan
  assert {
    condition     = contains(output.coverage_gate_inputs.new_grants, "cat.sch.customers|analysts")
    error_message = "the deployment binding is checked on its own, even if the protection matches"
  }
}

run "state_from_before_the_binding_is_not_applied__state" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/applied/data_access/masking_functions.sql" = "SELECT 1;\n"
      "tests/.tmp/applied/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
      "tests/.tmp/applied/data_access/terraform.tfstate"     = jsonencode({ version = 4, outputs = { coverage_gate = { value = { status = "pass", fingerprint = "applied", max_age = "6h", table_grant_count = 1, protection_fingerprint = run.gate_inputs.coverage_gate_inputs.protection_fingerprint } } }, resources = [{ module = "module.data_access", mode = "managed", type = "databricks_grant", name = "table_access", provider = "provider[\"registry.terraform.io/databricks/databricks\"].workspace", instances = [{ index_key = "cat.sch.customers|analysts", schema_version = 0, attributes = { id = "cat.sch.customers|analysts" } }] }] })
    }
  }
}

run "state_from_before_the_binding_is_not_applied" {
  command = plan
  assert {
    condition     = contains(output.coverage_gate_inputs.new_grants, "cat.sch.customers|analysts")
    error_message = "a state written before the deployment binding existed exempts nothing"
  }
}

run "state_without_protection_is_not_applied__state" {
  module {
    source = "./tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/applied/data_access/masking_functions.sql" = "SELECT 1;\n"
      "tests/.tmp/applied/data_access/.coverage_gate.json"   = jsonencode({ status = "pass", fingerprint = run.gate_inputs.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" })
      "tests/.tmp/applied/data_access/terraform.tfstate"     = jsonencode({ version = 4, outputs = { coverage_gate = { value = { status = "pass", fingerprint = "applied", max_age = "6h", table_grant_count = 1, deployment_binding = run.gate_inputs.coverage_gate_inputs.deployment_binding } } }, resources = [{ module = "module.data_access", mode = "managed", type = "databricks_grant", name = "table_access", provider = "provider[\"registry.terraform.io/databricks/databricks\"].workspace", instances = [{ index_key = "cat.sch.customers|analysts", schema_version = 0, attributes = { id = "cat.sch.customers|analysts" } }] }] })
    }
  }
}

run "state_without_protection_is_not_applied" {
  command = plan
  assert {
    condition     = output.coverage_gate_inputs.needs_gate == true
    error_message = "a state without a recorded protection exempts nothing"
  }
}
