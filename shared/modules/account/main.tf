terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      version               = "~> 1.111.0"
      configuration_aliases = [databricks.account, databricks.workspace]
    }
  }
}

locals {
  # Consume-by-default: when manage_groups is false (the default), access-tier
  # groups are looked up by name from the IdP-synced account groups via the
  # data source below and GenieRails mints nothing. The opt-in create path
  # (manage_groups = true, demo/greenfield) creates them as resources instead.
  group_ids = var.manage_groups ? {
    for name, group in databricks_group.groups : name => group.id
    } : {
    for name, group in data.databricks_group.consumed : name => group.id
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

# Consume path (DEFAULT): look up existing IdP-synced groups by display name.
# A missing group makes this read fail, naming the group — the loud, actionable
# preflight for consume-by-default also runs in generate_abac.py before apply.
data "databricks_group" "consumed" {
  for_each = var.manage_groups ? {} : var.groups

  provider     = databricks.account
  display_name = each.key
}

# Create path (OPT-IN, demo/greenfield only): mint the groups when no IdP is
# syncing them yet. Off by default so GenieRails does not own group lifecycle.
resource "databricks_group" "groups" {
  for_each = var.manage_groups ? var.groups : {}

  provider     = databricks.account
  display_name = each.key
}

resource "databricks_group_member" "members" {
  # Membership is IdP-owned in the default consume path; only manage it when
  # GenieRails is minting the groups (manage_groups = true).
  for_each = var.manage_groups ? local.group_member_map : {}

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
