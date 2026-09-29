mock_provider "databricks" {}
mock_provider "null" {}
mock_provider "time" {}

# Models env B planning after env A's schema is already present in the live
# catalog singleton. The prepare step supplies that live scope on every plan/apply.
run "second_environment_preserves_first_environment_scope" {
  command = plan

  variables {
    env_dir                   = "../../examples/healthcare"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_host = "https://example.invalid"
    uc_tables                 = ["shared_catalog.env_b.orders"]
    enable_classification     = true
    enable_auto_tagging       = true
    classification_existing_schemas = {
      shared_catalog = ["env_a"]
    }
    sql_warehouse_id = "warehouse"
  }

  assert {
    condition     = toset(output.classification_catalog_schemas["shared_catalog"]) == toset(["env_a", "env_b"])
    error_message = "env B must union its schema with env A's live classification scope"
  }
}
