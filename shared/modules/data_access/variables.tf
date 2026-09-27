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

variable "business_access_enabled" {
  type        = bool
  default     = false
  description = "Fail-closed exposure gate. Set true only after the coverage gate and schema drift check pass; controls business-group SELECT grants."
}

variable "enable_classification" {
  type        = bool
  default     = false
  description = "Opt-in to enable UC Data Classification with auto-tagging, scoped to schemas in uc_tables."
}

variable "classification_existing_schemas" {
  type        = map(list(string))
  default     = {}
  description = "Existing schemas to preserve when a catalog classification config is shared across environments."
}

variable "tag_assignments" {
  type = list(object({
    entity_type = string
    entity_name = string
    tag_key     = string
    tag_value   = string
  }))
  default     = []
  description = "Classifier-owned tag-to-entity facts. Promotion leaves this empty so each environment derives assignments from its own classification scan."
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
  default     = "ABAC Serverless Warehouse"
  description = "Name of the auto-created governance warehouse."
}

variable "warehouse_cluster_size" {
  type        = string
  default     = "Small"
  description = "Cluster size for the auto-created governance warehouse (2X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large, 4X-Large)."
}

variable "masking_sql_file" {
  type        = string
  description = "Path to masking_functions.sql owned by the data_access layer."
}

variable "deploy_masking_script" {
  type        = string
  description = "Path to deploy_masking_functions.py."
}
