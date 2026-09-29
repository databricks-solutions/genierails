mock_provider "databricks" {}
mock_provider "null" {}
mock_provider "time" {}

run "classification_scans_without_auto_tagging" {
  command = plan

  variables {
    env_dir                   = "../../examples/healthcare"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_host = "https://example.invalid"
    uc_tables                 = ["review_first.customers.records"]
    enable_classification     = true
    enable_auto_tagging       = false
    sql_warehouse_id          = "warehouse"
  }

  assert {
    condition     = length(output.classification_auto_tag_configs["review_first"]) == 0
    error_message = "classification must remain enabled with no auto-tag configs when enable_auto_tagging is false"
  }
}

run "auto_tagging_emits_all_champion_classifier_types" {
  command = plan

  variables {
    env_dir                   = "../../examples/healthcare"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_host = "https://example.invalid"
    uc_tables                 = ["review_first.customers.records"]
    enable_classification     = true
    enable_auto_tagging       = true
    sql_warehouse_id          = "warehouse"
  }

  assert {
    condition     = length(output.classification_auto_tag_configs["review_first"]) == 7
    error_message = "auto-tagging opt-in must emit one config for every champion classifier type"
  }

  assert {
    condition = alltrue([
      for config in output.classification_auto_tag_configs["review_first"] :
      config.auto_tagging_mode == "AUTO_TAGGING_ENABLED"
    ])
    error_message = "every emitted auto-tag config must enable auto-tagging"
  }
}
