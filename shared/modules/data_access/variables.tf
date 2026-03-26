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

variable "databricks_workspace_host" {
  type        = string
  description = "The governance execution workspace URL."
}

variable "groups" {
  type = map(object({
    description = optional(string, "")
  }))
  default     = {}
  description = "Map of group names referenced by shared grants and policies."
}

variable "uc_tables" {
  type        = list(string)
  default     = []
  description = "Optional UC table list used to derive catalogs for grants."
}

variable "tag_assignments" {
  type = list(object({
    entity_type = string
    entity_name = string
    tag_key     = string
    tag_value   = string
  }))
  default     = []
  description = "Tag-to-entity mappings."
}

variable "fgac_policies" {
  type = list(object({
    name              = string
    policy_type       = string
    catalog           = string
    to_principals     = list(string)
    except_principals = optional(list(string), [])
    comment           = optional(string, "")
    match_condition   = optional(string)
    match_alias       = optional(string)
    function_name     = string
    function_catalog  = string
    function_schema   = string
    when_condition    = optional(string)
  }))
  default     = []
  description = "FGAC policies scoped to governed catalogs."
}

variable "sql_warehouse_id" {
  type        = string
  default     = ""
  description = "Existing SQL warehouse ID to reuse for governance execution."
}

variable "warehouse_name" {
  type        = string
  default     = "ABAC Governance Warehouse"
  description = "Name of the auto-created governance warehouse."
}

variable "masking_sql_file" {
  type        = string
  description = "Path to masking_functions.sql owned by the data_access layer."
}

variable "deploy_masking_script" {
  type        = string
  description = "Path to deploy_masking_functions.py."
}
