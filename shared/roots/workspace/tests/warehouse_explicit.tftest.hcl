mock_provider "databricks" {
  alias = "account"
}
mock_provider "databricks" {
  alias = "workspace"
}
mock_provider "null" {}

run "top_level_warehouse_is_not_explicit_for_attached_agent" {
  command = plan

  variables {
    env_dir                   = "tests/.tmp/warehouse-explicit"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_id   = "123"
    databricks_workspace_host = "https://example.invalid"
    genie_only                = true
    sql_warehouse_id          = "shared-top-level-warehouse"
    groups                    = {}
    genie_spaces = [{
      name             = "Attached"
      genie_space_id   = "space-1"
      sql_warehouse_id = ""
      uc_tables        = ["cat.schema.table"]
    }]
    genie_space_configs = {
      Attached = { description = "make update-config observable" }
    }
  }

  assert {
    condition     = output.genie_existing_space_warehouse_intent["attached"].warehouse_id == ""
    error_message = "an attached agent must not inherit the top-level warehouse as a live update trigger"
  }

  assert {
    condition     = output.genie_existing_space_warehouse_intent["attached"].warehouse_explicit == "0"
    error_message = "only a per-space warehouse may be explicit for an attached agent"
  }
}
