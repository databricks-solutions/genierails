mock_provider "databricks" {
  alias = "account"
}
mock_provider "databricks" {
  alias = "workspace"
}
mock_provider "null" {}

# Non-empty CAN_RUN needs a data_access layer applied with a current passing
# coverage check (genie_exposure_gate.tftest.hcl); record one for these runs.
run "data_access_is_gated" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/acl/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "f1", status = "pass", max_age = "6h", table_grant_count = 1 } }, table_grant_resource_keys = { value = ["pay.agent.facts|pay_group"] } } })
      "tests/.tmp/acl/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "f1", refreshed_at = "@NOW@" })
    }
  }
}

run "explicit_empty_acl_clears_can_run" {
  command = plan

  override_data {
    target = module.workspace.data.databricks_group.existing
    values = {
      id = 123
    }
  }

  variables {
    env_dir                   = "tests/.tmp/acl"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_id   = "123"
    databricks_workspace_host = "https://example.invalid"
    sql_warehouse_id          = "warehouse"
    groups = {
      group_a = {}
      group_b = {}
    }
    genie_spaces = [
      { name = "Nobody", genie_space_id = "space-1", uc_tables = [] },
    ]
    genie_space_configs = {
      Nobody = { acl_groups = [] }
    }
  }

  assert {
    condition     = output.genie_space_acls_groups["nobody"] == ""
    error_message = "explicit acl_groups=[] must resolve to an empty CAN_RUN group list"
  }

  assert {
    condition     = output.genie_space_acls_applied
    error_message = "an empty ACL must actively apply an empty permission list to clear prior CAN_RUN grants"
  }
}

run "id_only_space_uses_canonical_title_for_can_run" {
  command = plan
  override_data {
    target = module.workspace.data.databricks_group.existing
    values = { id = 123 }
  }
  variables {
    env_dir                   = "tests/.tmp/acl"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_id   = "123"
    databricks_workspace_host = "https://example.invalid"
    sql_warehouse_id          = "warehouse"
    groups                    = { pay_group = {}, hr_group = {} }
    genie_spaces              = [{ genie_space_id = "space-1", uc_tables = [] }]
    genie_space_id_to_name    = { "space-1" = "Payments" }
    genie_space_configs       = { Payments = { acl_groups = ["pay_group"] } }
  }
  assert {
    condition     = output.genie_space_acls_groups["space-1"] == "pay_group"
    error_message = "id-only spaces must apply CAN_RUN from the canonical resolved title"
  }
}
