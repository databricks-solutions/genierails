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

output "legacy_unattributed_discovered_tables" {
  description = "Discovered tables using the backward-compatible all-principals fallback because agent attribution is absent."
  value       = local.legacy_unattributed_discovered_tables
}

output "coverage_gate_inputs" {
  description = "What scripts/coverage_gate.py gates: the input fingerprint, whether business SELECT is requested, the tables it would grant, and the acknowledged columns. Computed from configuration only, so terraform console can read it before any apply."
  value = {
    fingerprint             = local.coverage_gate_fingerprint
    business_access_enabled = var.business_access_enabled
    grant_tables            = local.coverage_gate_grant_tables
    acknowledged_columns    = sort(distinct([for column in var.coverage_acknowledged_columns : lower(column)]))
    max_age                 = var.coverage_gate_max_age
  }
}

output "coverage_gate" {
  description = "Coverage-gate result this layer was applied with. The workspace layer reads it from state before it grants Genie CAN_RUN. It references the table grants, so a failed grant leaves the previous value in state."
  value = {
    business_access_enabled = var.business_access_enabled
    fingerprint             = local.coverage_gate_fingerprint
    status                  = local.coverage_gate_status
    max_age                 = var.coverage_gate_max_age
    table_grant_count       = length(databricks_grant.table_access)
  }
}
