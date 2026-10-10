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

variable "admin_uc_tables" {
  type        = list(string)
  default     = []
  description = "Top-level administrator-authored tables that intentionally grant SELECT to every access principal."
}

variable "discovered_uc_tables" {
  type        = list(string)
  default     = []
  description = "Tool-owned per-environment table facts discovered from Genie agents."
}


variable "table_agents" {
  type        = map(list(string))
  default     = {}
  description = "Mapping from table FQN to Genie agents that expose the table."
}

variable "genie_space_acl_groups" {
  type        = map(list(string))
  default     = {}
  description = "Tool-owned resolved mapping from Genie agent name to CAN_RUN groups; explicit [] remains nobody, while omitted/null user ACLs are derived upstream from policy to_principals plus except_principals."
}

variable "classification_uc_tables" {
  type        = list(string)
  default     = []
  description = "Classification-only UC table footprint; never used to derive grants."
}

variable "coverage_gate_file" {
  type        = string
  description = "Path to the coverage check result scripts/coverage_gate.py writes for this layer. Business SELECT grants are planned only while it records a pass for the current inputs."
}

variable "coverage_ddl_file" {
  type        = string
  description = "Path to the fetched DDL the coverage check reads; its content is part of the gate fingerprint."
}

variable "coverage_gate_max_age" {
  type        = string
  default     = "6h"
  description = "Oldest live refresh (derive-assignments re-reading class.* tags and DDL from Unity Catalog) a passing coverage check may rest on, as a Terraform duration of at most 24h. make refreshes right before every checked plan/apply; this bounds what a raw terraform run can rely on. It is part of the gate fingerprint, so changing it requires a new check run."

  # Same bounds as modules/coverage_gate_check (the authority, whose 24h
  # ceiling no variable can raise); repeated here only to fail early.
  validation {
    condition = try(
      timecmp(timeadd("2000-01-01T00:00:00Z", var.coverage_gate_max_age), "2000-01-01T00:00:00Z") > 0
      && timecmp(timeadd("2000-01-01T00:00:00Z", var.coverage_gate_max_age), timeadd("2000-01-01T00:00:00Z", "24h")) <= 0,
      false
    )
    error_message = "coverage_gate_max_age must be a positive Terraform duration of at most 24h, such as \"6h\" or \"90m\"."
  }
}

variable "applied_table_grants" {
  type        = list(string)
  default     = []
  description = "table_access keys (\"<table>|<principal>\") the last apply made, from this layer's state. With an unchanged protection fingerprint they stay plannable without a current coverage check pass."
}

variable "deployment_binding" {
  type        = string
  default     = ""
  description = "Identity of this deployment (hash of workspace host and ID), recorded with the applied protection so a state from another deployment exempts nothing."
}

variable "applied_protection" {
  type        = any
  default     = null
  description = "coverage_gate.protection the last apply recorded in this layer's state (what protected its grants, part by part); null when unknown. A kept grant needs no pass unless this change removes or changes part of it."
}

variable "applied_protection_fingerprint" {
  type        = string
  default     = ""
  description = "coverage_gate.protection_fingerprint the last apply recorded in this layer's state; \"\" when unknown (no exemption)."
}

variable "coverage_acknowledged_columns" {
  type        = list(string)
  default     = []
  description = "Fully qualified catalog.schema.table.column names reviewed as not sensitive. The coverage check does not block first exposure on them."
}

variable "enable_classification" {
  type        = bool
  default     = false
  description = "Opt-in to enable UC Data Classification scanning, scoped to schemas in classification_uc_tables."
}

variable "enable_auto_tagging" {
  type        = bool
  default     = null
  description = "Optional scripted auto-tagging control. Null preserves the catalog's existing UI-managed auto-tag configuration; true replaces UI per-tag choices with the module's supported class.* tag list; false explicitly disables them."
}

variable "classification_existing_schemas" {
  type        = map(list(string))
  default     = {}
  description = "Existing schemas to preserve when a catalog classification config is shared across environments."
}

variable "classification_all_schemas" {
  type        = set(string)
  default     = []
  description = "Catalogs whose classification config intentionally covers all schemas (unset included_schemas)."
}

variable "classification_existing_auto_tag_configs" {
  type = map(list(object({
    classification_tag = string
    auto_tagging_mode  = string
  })))
  default     = {}
  description = "Existing UI-managed auto-tag configuration preserved when enable_auto_tagging is null."
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

variable "retain_auto_warehouse" {
  type        = bool
  default     = false
  description = "Keep a previously auto-created warehouse managed after selecting an explicit warehouse."
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

variable "auth_file" {
  type        = string
  description = "Path to the layer's auth.auto.tfvars; deploy_masking_functions.py reads the current SP credentials from it."
}
