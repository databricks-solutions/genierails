mock_provider "databricks" {
  alias = "account"
}
mock_provider "databricks" {
  alias = "workspace"
}
mock_provider "null" {}

run "explicit_empty_acl_clears_can_run" {
  command = plan

  override_data {
    target = module.workspace.data.databricks_group.existing
    values = {
      id = 123
    }
  }

  variables {
    env_dir                   = "."
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_id   = "123"
    databricks_workspace_host = "https://example.invalid"
    sql_warehouse_id          = "warehouse"
    business_access_enabled   = true
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
