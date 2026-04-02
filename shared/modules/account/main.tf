terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      version               = "~> 1.91.0"
      configuration_aliases = [databricks.account, databricks.workspace]
    }
  }
}

locals {
  group_ids = {
    for name, group in databricks_group.groups : name => group.id
  }

  group_member_pairs = flatten([
    for group, members in var.group_members : [
      for member_id in members : {
        group     = group
        member_id = member_id
      }
    ]
  ])

  group_member_map = {
    for pair in local.group_member_pairs :
    "${pair.group}|${pair.member_id}" => pair
  }
}

resource "databricks_group" "groups" {
  for_each = var.groups

  provider     = databricks.account
  display_name = each.key
}

resource "databricks_group_member" "members" {
  for_each = local.group_member_map

  provider  = databricks.account
  group_id  = local.group_ids[each.value.group]
  member_id = each.value.member_id

  depends_on = [databricks_group.groups]
}

# Tag policies are account-scoped resources managed here once, shared across
# all workspace environments. The workspace provider is required to create them.
# Values are fully managed by the generated config — the autofix pipeline and
# _preserve_existing_tag_policy_values() ensure the values list is always a
# superset of what's already assigned to columns.
resource "databricks_tag_policy" "policies" {
  for_each = { for tp in var.tag_policies : tp.key => tp }

  provider    = databricks.workspace
  tag_key     = each.value.key
  description = each.value.description
  values      = [for v in each.value.values : { name = v }]

  # The provider can reorder values after apply. We manage value convergence
  # via sync_tag_policies.py and keep Terraform responsible for key ownership.
  lifecycle {
    ignore_changes = [values]
  }
}
