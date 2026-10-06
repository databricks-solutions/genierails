mock_provider "databricks" {}
mock_provider "null" {}
mock_provider "time" {}

variables {
  env_dir                   = "../../examples/healthcare"
  databricks_account_id     = "account"
  databricks_client_id      = "service-principal"
  databricks_client_secret  = "secret"
  databricks_workspace_host = "https://example.invalid"
  genie_spaces = [
    { name = "Agent A", uc_tables = ["cap_catalog.space.released", "cap_catalog.space.new_table"] },
  ]
  genie_space_acl_groups = {
    "Agent A" = ["agent_a_group", "new_group"]
  }
  groups = {
    agent_a_group = {}
    new_group     = {}
  }
  business_access_enabled = true
  sql_warehouse_id        = "warehouse"
}

run "uncapped_release_grants_the_whole_footprint" {
  command = plan

  assert {
    condition = toset(output.table_grant_resource_keys) == toset([
      "cap_catalog.space.released|agent_a_group",
      "cap_catalog.space.released|new_group",
      "cap_catalog.space.new_table|agent_a_group",
      "cap_catalog.space.new_table|new_group",
    ])
    error_message = "without a release cap the open gate grants the full footprint"
  }
}

run "release_cap_keeps_released_grants_and_withholds_new_ones" {
  command = plan

  variables {
    released_table_grants = [
      "cap_catalog.space.released|agent_a_group",
      "cap_catalog.space.dropped|agent_a_group",
    ]
  }

  assert {
    condition     = toset(output.table_grant_resource_keys) == toset(["cap_catalog.space.released|agent_a_group"])
    error_message = "the cap must keep released grants still in the footprint and withhold new tables and principals"
  }
}

run "closed_gate_grants_nothing_even_with_a_cap" {
  command = plan

  variables {
    business_access_enabled = false
    released_table_grants   = ["cap_catalog.space.released|agent_a_group"]
  }

  assert {
    condition     = length(output.table_grant_resource_keys) == 0
    error_message = "the cap never opens access by itself"
  }
}
