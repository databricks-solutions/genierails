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
