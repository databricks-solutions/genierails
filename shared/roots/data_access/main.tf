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
    external = {
      source  = "hashicorp/external"
      version = "~> 2.3"
    }
  }
  required_version = ">= 1.7"

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

  # What the last apply of this layer left in place, read from its own state
  # (the layer runner's local backend), so keeping or revoking SELECT never
  # needs a current coverage check (see modules/data_access). Only current,
  # successfully applied objects count: tainted instances and deposed ones
  # (left by a failed or partial replacement) don't. The record must also be
  # for this deployment (workspace host and ID); a state copied from another
  # environment, or written before the binding existed, counts as nothing
  # applied. Unreadable state means nothing is exempt.
  deployment_binding = sha256(jsonencode({
    workspace_host = lower(trimsuffix(trimspace(var.databricks_workspace_host), "/"))
    workspace_id   = trimspace(var.databricks_workspace_id)
  }))
  _own_state = fileexists("${var.env_dir}/terraform.tfstate") ? try(jsondecode(file("${var.env_dir}/terraform.tfstate")), null) : null
  retain_auto_warehouse = anytrue([
    for resource in try(local._own_state.resources, []) :
    try(resource.module, "") == "module.data_access" &&
    try(resource.type, "") == "databricks_sql_endpoint" &&
    try(resource.name, "") == "warehouse" && length(try(resource.instances, [])) > 0
  ])
  _applied_record  = try(local._own_state.outputs.coverage_gate.value, null)
  _state_is_for_us = try(tostring(local._applied_record.deployment_binding), "") == local.deployment_binding
  applied_table_grants = local._state_is_for_us ? flatten([
    for resource in try(local._own_state.resources, []) : [
      for instance in try(resource.instances, []) : tostring(instance.index_key)
      if try(instance.status, "") != "tainted" && try(instance.deposed, "") == "" && try(instance.index_key, null) != null
    ]
    if try(resource.module, "") == "module.data_access" && try(resource.mode, "") == "managed"
    && try(resource.type, "") == "databricks_grant" && try(resource.name, "") == "table_access"
  ]) : []
  applied_protection_fingerprint = local._state_is_for_us ? try(tostring(local._applied_record.protection_fingerprint), "") : ""
  applied_protection             = local._state_is_for_us ? try(local._applied_record.protection, null) : null
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
    acl_groups       = optional(list(string), null)
    delete           = optional(bool, false)
  }))
  default     = []
  description = "User-owned workspace definitions and classification footprint. acl_groups omitted/null derives fresh from policy to_principals plus except_principals; [] explicitly grants nobody; a non-empty list is the durable override."
}

variable "genie_space_acl_groups" {
  type        = map(list(string))
  default     = {}
  description = "Tool-owned resolved ACL mapping. Inputs come only from genie_spaces: explicit lists win (including []); omitted/null entries are freshly derived from policy to_principals plus except_principals."
}

variable "business_access_enabled" {
  type        = bool
  default     = null
  description = "DEPRECATED and ignored; removed in the next release. The old access switch is retired: Business SELECT and Genie CAN_RUN are granted whenever the coverage check passes, so true and false both do nothing (false does NOT revoke access: remove the groups or acl_groups entries instead). Still declared so existing env.auto.tfvars files and -var flags keep working; make warns while it is set."
}

variable "enable_classification" {
  type        = bool
  default     = false
  description = "Opt-in to enable UC Data Classification scanning, scoped to schemas in the combined classification footprint."
}

variable "coverage_gate_max_age" {
  type        = string
  default     = "6h"
  description = "Oldest live refresh of tags and DDL a passing coverage check may rest on (Terraform duration). Set in env.auto.tfvars."
}

variable "coverage_acknowledged_columns" {
  type        = list(string)
  default     = []
  description = "Fully qualified catalog.schema.table.column names reviewed as not sensitive; the coverage check does not block first exposure on them. Set in env.auto.tfvars."
}

variable "verify_key_column" {
  type        = string
  default     = ""
  description = "Non-sensitive stable row identifier used only by effective-access verification tooling."
}

variable "verify_key_columns" {
  type        = map(string)
  default     = {}
  description = "Per-table row-pairing key (\"catalog.schema.table\" = \"column\") used only by effective-access verification tooling; saved after a passing verify-access."
}

variable "enable_auto_tagging" {
  type        = bool
  default     = null
  description = "Optional scripted auto-tagging control. Null preserves the catalog's existing UI-managed setting; true replaces UI per-tag choices with the module's supported class.* tag list; false explicitly disables them."
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

variable "classification_existing_auto_tag_configs" {
  type = map(list(object({
    classification_tag = string
    auto_tagging_mode  = string
  })))
  default     = {}
  description = "Existing catalog auto-tag configuration preserved when enable_auto_tagging is null."
}

variable "access_tier_groups" {
  type        = list(string)
  default     = []
  description = "Deterministic-governance tiers ordered raw, optional partial tier(s), then full. Unused until rollout step 4."
  validation {
    condition     = length(var.access_tier_groups) == length(distinct(var.access_tier_groups)) && alltrue([for group in var.access_tier_groups : trimspace(group) != ""])
    error_message = "access_tier_groups must contain unique, non-empty group names."
  }
}

variable "governance_mode" {
  type        = string
  default     = "legacy"
  description = "Governance implementation selector."
  validation {
    condition     = contains(["legacy", "deterministic"], var.governance_mode)
    error_message = "governance_mode must be legacy or deterministic."
  }
}
variable "raw_exempt_principals" {
  type        = list(string)
  default     = []
  description = "Used from rollout step 5."
  validation {
    condition = alltrue([for principal in var.raw_exempt_principals :
      trimspace(principal) != "" && length(regexall("@", principal)) == 0
    ])
    error_message = "raw_exempt_principals must contain non-empty account group names, not user emails. UUID/hex-shaped group display names are allowed and resolved exactly during live verification."
  }
}

variable "treatment_versions" {
  type        = map(object({ partial = string }))
  default     = {}
  description = "Used from rollout step 3."
}
variable "tier_access_overrides" {
  type        = map(map(string))
  default     = {}
  description = "Used from rollout step 4."
  validation {
    condition     = alltrue(flatten([for rules in values(var.tier_access_overrides) : [for access in values(rules) : contains(["raw", "partial", "full"], access)]]))
    error_message = "tier_access_overrides values must be raw, partial, or full."
  }
}
variable "column_overrides" {
  type        = any
  default     = {}
  description = "Used from rollout step 3."
}
variable "row_filters" {
  type        = list(object({ table = string, column = string, values_by_group = map(list(string)) }))
  default     = []
  description = "Used from rollout step 6."
}
variable "require_acl_groups" {
  type        = bool
  default     = false
  description = "Rollout step 5 changes the default to true."
}

variable "promote_from" {
  type        = string
  default     = ""
  description = "make promote-to input only (saved in the destination env): the env rules are promoted from. Declared so env.auto.tfvars loads cleanly; no resource reads it."
}

variable "catalog_map" {
  type        = any
  default     = {}
  description = "make promote-to input only (saved in the destination env): source-to-target catalog renames. Declared so env.auto.tfvars loads cleanly; no resource reads it."
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

  databricks_account_id                    = var.databricks_account_id
  databricks_client_id                     = var.databricks_client_id
  databricks_client_secret                 = var.databricks_client_secret
  databricks_workspace_host                = var.databricks_workspace_host
  groups                                   = var.groups
  uc_tables                                = local.full_uc_tables
  admin_uc_tables                          = local.full_admin_uc_tables
  discovered_uc_tables                     = local.full_discovered_uc_tables
  table_agents                             = local.table_agents
  genie_space_acl_groups                   = var.genie_space_acl_groups
  classification_uc_tables                 = local.full_effective_uc_tables
  coverage_gate_file                       = "${var.env_dir}/.coverage_gate.json"
  coverage_ddl_file                        = "${var.env_dir}/../ddl/_fetched.sql"
  coverage_acknowledged_columns            = var.coverage_acknowledged_columns
  coverage_gate_max_age                    = var.coverage_gate_max_age
  applied_table_grants                     = local.applied_table_grants
  applied_protection_fingerprint           = local.applied_protection_fingerprint
  applied_protection                       = local.applied_protection
  deployment_binding                       = local.deployment_binding
  enable_classification                    = var.enable_classification
  enable_auto_tagging                      = var.enable_auto_tagging
  classification_existing_schemas          = var.classification_existing_schemas
  classification_all_schemas               = var.classification_all_schemas
  classification_existing_auto_tag_configs = var.classification_existing_auto_tag_configs
  tag_assignments                          = var.tag_assignments
  fgac_policies                            = var.fgac_policies
  sql_warehouse_id                         = var.sql_warehouse_id
  warehouse_name                           = var.warehouse_name
  retain_auto_warehouse                    = local.retain_auto_warehouse
  masking_sql_file                         = "${var.env_dir}/masking_functions.sql"
  deploy_masking_script                    = "${local.project_root}/deploy_masking_functions.py"
  auth_file                                = "${var.env_dir}/auth.auto.tfvars"
}

# A raw terraform run that withholds new grants still applies; say so.
check "business_select_withheld" {
  assert {
    condition     = length(module.data_access.withheld_table_grants.grants) == 0
    error_message = "Coverage check ${module.data_access.withheld_table_grants.status}: ${module.data_access.withheld_table_grants.problem}. New business SELECT withheld: ${join(", ", module.data_access.withheld_table_grants.grants)}. Removals and existing grants still apply. Run this layer through make (make apply ENV=${basename(dirname(var.env_dir))}), which refreshes live tags and DDL and runs the coverage check."
  }
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

output "coverage_gate_inputs" {
  description = "Read by scripts/coverage_gate.py through terraform console."
  value       = module.data_access.coverage_gate_inputs
}

output "coverage_gate" {
  description = "Coverage check result this layer was applied with; the workspace layer reads it from state."
  value       = module.data_access.coverage_gate
}

output "legacy_unattributed_discovered_tables" {
  description = "Legacy discovered tables falling back to all access principals until make generate re-derives agent attribution."
  value       = module.data_access.legacy_unattributed_discovered_tables
}
