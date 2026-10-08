mock_provider "databricks" {
  alias = "account"
}
mock_provider "databricks" {
  alias = "workspace"
}
mock_provider "null" {}

variables {
  databricks_account_id        = "account"
  databricks_client_id         = "client"
  databricks_client_secret     = "secret"
  databricks_workspace_id      = "123"
  databricks_workspace_host    = "https://example.invalid"
  groups                       = {}
  genie_exposure_blocker       = ""
  genie_space_missing_grants   = {}
  genie_space_can_run_widening = {}
  genie_id_file_prefix         = "/tmp/.genie_space_id"
  genie_script_path            = ""
  genie_destroy_script         = "bash tests/genie_destroy_stub.sh"
  genie_only                   = true
}

run "create_automatic_warehouse" {
  command = apply
  variables { sql_warehouse_id = "" }
  assert {
    condition     = length(databricks_sql_endpoint.warehouse) == 1
    error_message = "empty warehouse ID must create the managed warehouse"
  }
}

run "switch_to_explicit_warehouse_retains_automatic" {
  command = apply
  variables {
    sql_warehouse_id      = "explicit-warehouse"
    retain_auto_warehouse = true
  }
  assert {
    condition     = length(databricks_sql_endpoint.warehouse) == 1
    error_message = "the transition must retain the automatic warehouse so Terraform cannot destroy it"
  }
}
