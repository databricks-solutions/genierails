terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.91.0"
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
  # 3-part entries (catalog.schema.table) are already fully qualified and passed through as-is.
  # 2-part entries (schema.table) are prefixed with uc_catalog (legacy schema-relative support).
  full_uc_tables = [for t in var.uc_tables :
    length(split(".", t)) >= 3 ? t : (var.uc_catalog != "" ? "${var.uc_catalog}.${t}" : t)
  ]
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

  databricks_account_id     = var.databricks_account_id
  databricks_client_id      = var.databricks_client_id
  databricks_client_secret  = var.databricks_client_secret
  databricks_workspace_host = var.databricks_workspace_host
  groups                   = var.groups
  uc_tables                = local.full_uc_tables
  tag_assignments          = var.tag_assignments
  fgac_policies            = var.fgac_policies
  sql_warehouse_id         = var.sql_warehouse_id
  warehouse_name           = var.warehouse_name
  masking_sql_file         = "${var.env_dir}/masking_functions.sql"
  deploy_masking_script    = "${local.project_root}/deploy_masking_functions.py"
}

output "sql_warehouse_id" {
  value = module.data_access.sql_warehouse_id
}

output "catalogs" {
  value = module.data_access.catalogs
}
