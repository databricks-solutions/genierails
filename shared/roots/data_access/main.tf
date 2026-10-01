terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.111.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }
  required_version = ">= 1.0"

  backend "local" {}
}

provider "databricks" {
  alias         = "account"
  host          = var.databricks_account_host
  account_id    = var.databricks_account_id
  client_id     = var.databricks_client_id
  client_secret = var.databricks_client_secret
}

provider "databricks" {
  alias         = "workspace"
  host          = var.databricks_workspace_host
  client_id     = var.databricks_client_id
  client_secret = var.databricks_client_secret
}

locals {
  project_root = abspath("${path.root}/../..")
  full_admin_uc_tables = [for t in var.uc_tables :
    length(split(".", t)) >= 3 ? t : (var.uc_catalog != "" ? "${var.uc_catalog}.${t}" : t)
  ]
  configured_uc_tables = distinct(concat(
    var.uc_tables,
    flatten([for space in var.genie_spaces : space.uc_tables]),
  ))
  # 3-part entries (catalog.schema.table) are already fully qualified and passed through as-is.
  # 2-part entries (schema.table) are prefixed with uc_catalog (legacy schema-relative support).
  full_uc_tables = [for t in local.configured_uc_tables :
    length(split(".", t)) >= 3 ? t : (var.uc_catalog != "" ? "${var.uc_catalog}.${t}" : t)
  ]
  full_discovered_uc_tables = [for t in var.discovered_uc_tables :
    length(split(".", t)) >= 3 ? t : (var.uc_catalog != "" ? "${var.uc_catalog}.${t}" : t)
  ]
  full_discovered_table_agents = {
    for table, agents in var.discovered_table_agents :
    length(split(".", table)) >= 3 ? table : (var.uc_catalog != "" ? "${var.uc_catalog}.${table}" : table) => agents
  }
  _explicit_table_agent_pairs = flatten([
    for space in var.genie_spaces : [
      for table in space.uc_tables : {
        table = length(split(".", table)) >= 3 ? table : (var.uc_catalog != "" ? "${var.uc_catalog}.${table}" : table)
        agent = space.name != "" ? space.name : lookup(var.genie_space_id_to_name, space.genie_space_id, space.genie_space_id)
      }
    ]
  ])
  explicit_table_agents = {
    for pair in local._explicit_table_agent_pairs : pair.table => pair.agent...
  }
  table_agents = {
    for table in distinct(concat(keys(local.explicit_table_agents), keys(local.full_discovered_table_agents))) :
    table => distinct(concat(
      lookup(local.explicit_table_agents, table, []),
      lookup(local.full_discovered_table_agents, table, []),
    ))
  }
  full_effective_uc_tables = distinct(concat(local.full_uc_tables, local.full_discovered_uc_tables))
}

variable "env_dir" {
  type = string
}

variable "databricks_account_host" {
  type    = string
  default = "https://accounts.cloud.databricks.com"
}

variable "databricks_account_id" {
  type = string
}

variable "databricks_client_id" {
  type = string
}

variable "databricks_client_secret" {
  type      = string
  sensitive = true
}

variable "databricks_workspace_id" {
  type    = string
  default = ""
}

variable "serverless_usage_policy_id" {
  type        = string
  default     = ""
  description = "AWS test automation workaround for provider issue #5985; empty for normal and Azure environments."
}

variable "databricks_workspace_host" {
  type = string
}

variable "uc_catalog" {
  type    = string
  default = ""
}

variable "uc_tables" {
  type    = list(string)
  default = []
}

variable "discovered_uc_tables" {
  type        = list(string)
  default     = []
  description = "Tool-owned per-environment table facts discovered from Genie agents."
}

variable "discovered_table_agents" {
  type        = map(list(string))
  default     = {}
  description = "Tool-owned per-environment mapping from discovered UC table FQN to exposing Genie agent names."
}

variable "genie_space_id_to_name" {
  type        = map(string)
  default     = {}
  description = "Tool-owned mapping from imported Genie space IDs to their canonical names."
}

variable "genie_spaces" {
  type = list(object({
    name             = optional(string, "")
    genie_space_id   = optional(string, "")
    sql_warehouse_id = optional(string, "")
    uc_tables        = optional(list(string), [])
  }))
  default     = []
  description = "Workspace definitions whose UC tables also form the classification footprint."
}

variable "genie_space_acl_groups" {
  type        = map(list(string))
  default     = {}
  description = "Generated mapping from Genie agent name to groups authorized for CAN_RUN."
}

variable "business_access_enabled" {
  type        = bool
  default     = false
  description = "Fail-closed exposure gate. Enable only after coverage validation and the schema drift check pass."
}

variable "enable_classification" {
  type        = bool
  default     = false
  description = "Opt-in to enable UC Data Classification scanning, scoped to schemas in the combined classification footprint."
}

variable "enable_auto_tagging" {
  type        = bool
  default     = false
  description = "Opt-in to automatically apply class.* tags for classification detections."
}

variable "classification_existing_schemas" {
  type        = map(list(string))
  default     = {}
  description = "Existing catalog classification scope preserved when adopting a singleton catalog config."
}

variable "classification_all_schemas" {
  type        = set(string)
  default     = []
  description = "Catalog classification configs whose remote included_schemas is unset (all schemas)."
}

variable "manage_groups" {
  type    = bool
  default = false
}
variable "groups" {
  type = map(object({
    description = optional(string, "")
  }))
  default = {}
}
variable "group_members" {
  type    = map(list(string))
  default = {}
}
variable "tag_assignments" {
  type = list(object({
    entity_type = string
    entity_name = string
    tag_key     = string
    tag_value   = string
  }))
  default = []
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
  default = []
}
variable "sql_warehouse_id" {
  type    = string
  default = ""
}

variable "warehouse_name" {
  type    = string
  default = "ABAC Serverless Warehouse"
}

variable "genie_space_id" {
  type    = string
  default = ""
}

variable "genie_space_title" {
  type    = string
  default = "Genie Space"
}

variable "genie_space_description" {
  type    = string
  default = ""
}

variable "genie_sample_questions" {
  type    = list(string)
  default = []
}

variable "genie_instructions" {
  type    = string
  default = ""
}
variable "genie_benchmarks" {
  type = list(object({
    question = string
    sql      = string
  }))
  default = []
}
variable "genie_sql_filters" {
  type = list(object({
    sql          = string
    display_name = string
    comment      = string
    instruction  = string
  }))
  default = []
}
variable "genie_sql_expressions" {
  type = list(object({
    alias        = string
    sql          = string
    display_name = string
    comment      = string
    instruction  = string
  }))
  default = []
}
variable "genie_sql_measures" {
  type = list(object({
    alias        = string
    sql          = string
    display_name = string
    comment      = string
    instruction  = string
  }))
  default = []
}
variable "genie_join_specs" {
  type = list(object({
    left_table  = string
    left_alias  = string
    right_table = string
    right_alias = string
    sql         = string
    comment     = string
    instruction = string
  }))
  default = []
}

module "data_access" {
  source = "../../modules/data_access"

  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
  }

  databricks_account_id           = var.databricks_account_id
  databricks_client_id            = var.databricks_client_id
  databricks_client_secret        = var.databricks_client_secret
  databricks_workspace_host       = var.databricks_workspace_host
  groups                          = var.groups
  uc_tables                       = local.full_uc_tables
  admin_uc_tables                 = local.full_admin_uc_tables
  discovered_uc_tables            = local.full_discovered_uc_tables
  table_agents                    = local.table_agents
  genie_space_acl_groups          = var.genie_space_acl_groups
  classification_uc_tables        = local.full_effective_uc_tables
  business_access_enabled         = var.business_access_enabled
  enable_classification           = var.enable_classification
  enable_auto_tagging             = var.enable_auto_tagging
  classification_existing_schemas = var.classification_existing_schemas
  classification_all_schemas      = var.classification_all_schemas
  tag_assignments                 = var.tag_assignments
  fgac_policies                   = var.fgac_policies
  sql_warehouse_id                = var.sql_warehouse_id
  warehouse_name                  = var.warehouse_name
  masking_sql_file                = "${var.env_dir}/masking_functions.sql"
  deploy_masking_script           = "${local.project_root}/deploy_masking_functions.py"
}

output "sql_warehouse_id" {
  value = module.data_access.sql_warehouse_id
}

output "catalogs" {
  value = module.data_access.catalogs
}

output "grant_uc_tables" {
  description = "Fully qualified table footprint used for grants."
  value       = local.full_effective_uc_tables
}

output "classification_uc_tables" {
  description = "Fully qualified table footprint used for classification and grant coverage."
  value       = local.full_effective_uc_tables
}

output "classification_catalog_schemas" {
  value = module.data_access.classification_catalog_schemas
}

output "classification_auto_tag_configs" {
  value = module.data_access.classification_auto_tag_configs
}

output "schema_grant_resource_keys" {
  value = module.data_access.schema_grant_resource_keys
}

output "table_grant_resource_keys" {
  value = module.data_access.table_grant_resource_keys
}

output "legacy_unattributed_discovered_tables" {
  description = "Legacy discovered tables falling back to all access principals until make generate re-derives agent attribution."
  value       = module.data_access.legacy_unattributed_discovered_tables
}
