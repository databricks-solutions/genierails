mock_provider "databricks" {}
mock_provider "null" {}
mock_provider "time" {}

run "effective_tables_reach_classification_and_grants" {
  command = plan

  variables {
    env_dir                   = "../../examples/healthcare"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_host = "https://example.invalid"
    uc_tables                 = ["grants_catalog.business.orders"]
    genie_spaces              = [{ uc_tables = ["classification_catalog.space_only.events"] }]
    discovered_uc_tables      = ["discovered_catalog.agent.facts"]
    groups                    = { analysts = {} }
    business_access_enabled   = true
    enable_classification     = true
    sql_warehouse_id          = "warehouse"
  }

  assert {
    condition     = toset(output.catalogs) == toset(["grants_catalog", "classification_catalog", "discovered_catalog"])
    error_message = "all effective catalogs must enter service-principal and business catalog grants"
  }

  assert {
    condition = toset(output.grant_uc_tables) == toset([
      "grants_catalog.business.orders",
      "classification_catalog.space_only.events",
      "discovered_catalog.agent.facts",
    ])
    error_message = "user, Genie-space, and discovered tables must enter the grant footprint"
  }

  assert {
    condition = toset(output.schema_grant_resource_keys) == toset([
      "grants_catalog.business|analysts",
      "classification_catalog.space_only|analysts",
      "discovered_catalog.agent|analysts",
    ])
    error_message = "schema grant resources must be sourced only from uc_tables"
  }

  assert {
    condition = toset(output.table_grant_resource_keys) == toset([
      "grants_catalog.business.orders|analysts",
      "classification_catalog.space_only.events|analysts",
      "discovered_catalog.agent.facts|analysts",
    ])
    error_message = "table grant resources must be sourced only from uc_tables"
  }

  assert {
    condition = toset(output.classification_uc_tables) == toset([
      "grants_catalog.business.orders",
      "classification_catalog.space_only.events",
      "discovered_catalog.agent.facts",
    ])
    error_message = "classification must cover the union of normal and Genie-space-only tables"
  }
}

run "absent_discovery_preserves_legacy_user_table_behavior" {
  command = plan

  variables {
    env_dir                   = "../../examples/healthcare"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_host = "https://example.invalid"
    uc_tables                 = ["legacy_catalog.business.orders"]
    groups                    = { analysts = {} }
    business_access_enabled   = true
    sql_warehouse_id          = "warehouse"
  }

  assert {
    condition     = toset(output.grant_uc_tables) == toset(["legacy_catalog.business.orders"])
    error_message = "the default empty discovered list must preserve the legacy grant footprint"
  }

  assert {
    condition     = toset(output.table_grant_resource_keys) == toset(["legacy_catalog.business.orders|analysts"])
    error_message = "the absent discovered file must not change table grants"
  }
}
