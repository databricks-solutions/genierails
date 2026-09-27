mock_provider "databricks" {}
mock_provider "null" {}
mock_provider "time" {}

run "space_only_tables_are_classification_only" {
  command = plan

  variables {
    env_dir                   = "../../examples/healthcare"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_host = "https://example.invalid"
    uc_tables                 = ["grants_catalog.business.orders"]
    genie_spaces              = [{ uc_tables = ["classification_catalog.space_only.events"] }]
    groups                    = { analysts = {} }
    business_access_enabled   = true
    enable_classification     = true
    sql_warehouse_id          = "warehouse"
  }

  assert {
    condition     = toset(output.catalogs) == toset(["grants_catalog"])
    error_message = "space-only catalogs must not enter any service-principal or business catalog grants"
  }

  assert {
    condition     = toset(output.grant_uc_tables) == toset(["grants_catalog.business.orders"])
    error_message = "space-only tables must not enter the grant footprint"
  }

  assert {
    condition = toset(output.classification_uc_tables) == toset([
      "grants_catalog.business.orders",
      "classification_catalog.space_only.events",
    ])
    error_message = "classification must cover the union of normal and Genie-space-only tables"
  }
}
