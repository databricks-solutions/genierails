output "group_ids" {
  description = "Map of group names to their Databricks group IDs."
  value       = local.group_ids
}

output "group_names" {
  description = "List of group names managed by the account module."
  value       = keys(local.group_ids)
}

output "tag_policy_keys" {
  description = "List of tag policy keys created by the account module."
  value       = keys(databricks_tag_policy.policies)
}
