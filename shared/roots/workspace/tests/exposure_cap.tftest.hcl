mock_provider "databricks" {
  alias = "account"
}
mock_provider "databricks" {
  alias = "workspace"
}
mock_provider "null" {}

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
    group_a   = {}
    new_group = {}
  }
  genie_spaces = [
    { name = "Released", genie_space_id = "space-1", uc_tables = [] },
    { name = "Brand New", genie_space_id = "space-2", uc_tables = [] },
  ]
  genie_space_configs = {
    Released    = { acl_groups = ["group_a", "new_group"] }
    "Brand New" = { acl_groups = ["group_a"] }
  }
}

run "release_cap_withholds_new_agents_and_groups" {
  command = plan

  variables {
    released_genie_acls = {
      released = ["group_a"]
    }
  }

  assert {
    condition     = jsonencode(output.genie_space_acls_groups) == jsonencode({ released = "group_a" })
    error_message = "CAN_RUN must stay at what the last release opened: no new agent, no new group"
  }
}

run "uncapped_release_opens_every_agent" {
  command = plan

  assert {
    condition = jsonencode(output.genie_space_acls_groups) == jsonencode({
      released  = "group_a,new_group"
      brand_new = "group_a"
    })
    error_message = "without a cap every agent gets its full CAN_RUN groups"
  }
}

