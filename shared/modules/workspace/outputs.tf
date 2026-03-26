output "group_ids" {
  description = "Map of group names to their Databricks group IDs."
  value       = local.group_ids
}

output "group_names" {
  description = "List of group names looked up by the workspace module."
  value       = keys(local.group_ids)
}

output "workspace_assignments" {
  description = "Map of group names to their workspace assignment IDs."
  value = {
    for name, assignment in databricks_mws_permission_assignment.group_assignments : name => assignment.id
  }
}

output "group_entitlements" {
  description = "Summary of entitlements granted to each group."
  value = {
    for name, entitlement in databricks_entitlements.group_entitlements : name => {
      workspace_consume = entitlement.workspace_consume
    }
  }
}

output "sql_warehouse_id" {
  description = "Effective shared SQL warehouse ID (provided or auto-created)."
  value       = local.shared_warehouse_id
}

output "genie_space_acls_applied" {
  description = "Whether Genie Space ACLs were applied to any space."
  value       = length(null_resource.genie_space_acls) > 0 || length(null_resource.genie_space_acls_created) > 0
}

output "genie_space_acls_groups" {
  description = "Per-space groups granted CAN_RUN on each Genie Space."
  value = local.genie_space_groups
}

output "genie_spaces_created" {
  description = "Set of Genie Space keys that were auto-created (genie_space_id was empty)."
  value       = keys(null_resource.genie_space_create)
}

output "genie_groups_csv" {
  description = "Comma-separated group names for Genie ACL calls."
  value       = local.genie_groups_csv
}
