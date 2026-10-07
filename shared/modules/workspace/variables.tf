variable "databricks_account_id" {
  type        = string
  description = "The Databricks account ID."
}

variable "databricks_client_id" {
  type        = string
  description = "The Databricks service principal client ID."
}

variable "databricks_client_secret" {
  type        = string
  description = "The Databricks service principal client secret."
  sensitive   = true
}

variable "databricks_workspace_id" {
  type        = string
  description = "The Databricks workspace ID."
}

variable "databricks_workspace_host" {
  type        = string
  description = "The Databricks workspace URL."
}

variable "manage_groups" {
  type        = bool
  default     = false
  description = "Workspace state only supports lookup/onboarding for pre-existing groups."

  validation {
    condition     = !var.manage_groups
    error_message = "Workspace state requires manage_groups = false. Manage groups in envs/account instead."
  }
}

variable "groups" {
  type = map(object({
    description = optional(string, "")
  }))
  description = "Map of group name -> config. Workspace state looks these groups up by name."
}

variable "genie_exposure_blocker" {
  type        = string
  description = "Why the data_access layer isn't ready for business exposure, or \"\" when it is. A non-empty value blocks every non-empty Genie CAN_RUN grant."
}

variable "genie_space_missing_grants" {
  type        = map(list(string))
  description = "Per Genie agent key: <table>|<group> SELECT grants its CAN_RUN groups need that the data_access state lacks. A non-empty list (or a missing key) blocks that agent's non-empty CAN_RUN."
}

variable "genie_space_can_run_widening" {
  type        = map(list(string))
  description = "Per Genie agent key: CAN_RUN groups the desired ACL adds beyond what the last apply left in place. Empty (keep, shrink or clear) needs no coverage check; a missing key counts as widening."
}

variable "sql_warehouse_id" {
  type        = string
  default     = ""
  description = "Shared SQL warehouse ID. Per-space sql_warehouse_id in genie_spaces overrides this."
}

variable "warehouse_name" {
  type        = string
  default     = "ABAC Serverless Warehouse"
  description = "Name of the auto-created serverless warehouse."
}

# ── Multi-space Genie variables ───────────────────────────────────────────────

variable "genie_spaces" {
  type = map(object({
    name             = string
    genie_space_id   = string
    sql_warehouse_id = string
    uc_tables        = list(string)
    config = object({
      title            = string
      description      = string
      sample_questions = list(string)
      instructions     = string
      benchmarks = list(object({
        question = string
        sql      = string
      }))
      sql_filters = list(object({
        sql          = string
        display_name = string
        comment      = string
        instruction  = string
      }))
      sql_expressions = list(object({
        alias        = string
        sql          = string
        display_name = string
        comment      = string
        instruction  = string
      }))
      sql_measures = list(object({
        alias        = string
        sql          = string
        display_name = string
        comment      = string
        instruction  = string
      }))
      join_specs = list(object({
        left_table  = string
        left_alias  = string
        right_table = string
        right_alias = string
        sql         = string
        comment     = string
        instruction = string
      }))
      acl_groups = optional(list(string), [])
    })
  }))
  default     = {}
  description = "Map of Genie agent key to merged infra, resolved ACL, and semantic config produced by the workspace root. Durable ACL ownership remains in env genie_spaces[], not generated semantic drafts."
}

variable "genie_only" {
  type        = bool
  default     = false
  description = "When true, skip account-level operations (group lookup, workspace assignment, entitlements). SP only needs Workspace Admin."
}

variable "genie_id_file_prefix" {
  type        = string
  description = "Path prefix for per-space Genie ID files. Each space appends _{key} to this prefix."
}

variable "genie_script_path" {
  type        = string
  description = "Path to scripts/genie_space.sh."
}
