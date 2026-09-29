output "sql_warehouse_id" {
  description = "Effective SQL warehouse ID used for governance execution."
  value       = local.effective_warehouse_id
}

output "catalogs" {
  description = "Catalogs governed by this data_access layer."
  value       = local.all_catalogs
}
output "classification_catalog_schemas" {
  description = "Live remote scope unioned with this environment's classification footprint."
  value       = local.classification_catalog_schemas
}

output "classification_auto_tag_configs" {
  description = "Planned provider auto-tagging configs per classified catalog."
  value = {
    for catalog, config in databricks_data_classification_catalog_config.classification :
    catalog => config.auto_tag_configs
  }
}

output "schema_grant_resource_keys" {
  description = "Instantiated schema grant resource keys."
  value       = keys(databricks_grant.schema_access)
}

output "table_grant_resource_keys" {
  description = "Instantiated table grant resource keys."
  value       = keys(databricks_grant.table_access)
}
