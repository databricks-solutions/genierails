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
  description = "Whether Genie agent ACLs were applied to any space."
  value       = length(null_resource.genie_space_acls) > 0 || length(null_resource.genie_space_acls_created) > 0
}

output "genie_space_acls_groups" {
  description = "Per-space groups the config grants CAN_RUN on each Genie agent (before any are withheld)."
  value       = local.genie_space_groups
}

output "genie_space_can_run_withheld" {
  description = "Per-space CAN_RUN groups this plan withholds because opening them is blocked (genie_exposure_blocker or missing SELECT grants). Everything else in the change applies."
  value       = { for key, groups in local.genie_space_can_run_withheld : key => groups if length(groups) > 0 }
}

output "genie_space_acl_removal_only" {
  description = "Space keys whose desired CAN_RUN is fully withheld but whose direct ACL is still synced empty to remove hand-added access."
  value = sort(concat(
    keys(null_resource.genie_space_acls_removal_only),
    keys(null_resource.genie_space_acls_created_removal_only),
  ))
}

output "genie_spaces_created" {
  description = "Set of Genie agent keys that were auto-created (genie_space_id was empty)."
  value       = keys(terraform_data.genie_space)
}

output "genie_existing_space_warehouse_intent" {
  description = "Per attached agent, the raw per-space warehouse trigger and whether it is explicit."
  value = {
    for key, resource in null_resource.genie_space_config_existing : key => {
      warehouse_id       = resource.triggers.warehouse_id
      warehouse_explicit = resource.triggers.warehouse_explicit
    }
  }
}

output "genie_groups_csv" {
  description = "Comma-separated group names for Genie ACL calls (all groups, for backward compat)."
  value       = join(",", keys(var.groups))
}
