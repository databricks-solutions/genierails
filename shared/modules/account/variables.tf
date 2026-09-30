variable "manage_groups" {
  type        = bool
  default     = false
  description = <<-EOT
    Group ownership mode for the account layer.

    false (DEFAULT) — CONSUME existing IdP-synced groups. Each name in `groups`
    is looked up by display name via `data "databricks_group"`; GenieRails does
    NOT mint groups or manage their membership (the IdP owns both). This is the
    normal path: enable AIM (or SCIM where AIM is unavailable) so the identity
    provider syncs the access-tier groups into the Databricks account first.

    true (OPT-IN, demo/greenfield only) — CREATE the groups as `databricks_group`
    resources and manage `group_members` here. Use this only when no IdP is
    syncing the groups yet.
  EOT
}

variable "groups" {
  type = map(object({
    description = optional(string, "")
  }))
  description = <<-EOT
    Map of access-tier group name -> config. In the default consume path these
    are the existing IdP-synced group names each access tier maps to (looked up
    by display name); in the opt-in create path each key becomes a new
    account-level Databricks group.
  EOT
}

variable "group_members" {
  type        = map(list(string))
  default     = {}
  description = "Map of group name -> list of account-level user IDs. Only applied in the opt-in create path (manage_groups = true); in the default consume path the IdP owns membership."
}

variable "tag_policies" {
  type = list(object({
    key         = string
    description = optional(string, "")
    values      = list(string)
  }))
  default     = []
  description = "Account-scoped tag policy definitions, shared across all workspace environments."
}
