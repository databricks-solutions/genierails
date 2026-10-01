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
  }
  required_version = ">= 1.0"

  backend "local" {}
}

provider "databricks" {
  alias         = "account"
  host          = var.genie_only ? var.databricks_workspace_host : var.databricks_account_host
  account_id    = var.genie_only ? null : var.databricks_account_id
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

  # ── Backward-compat shim ──────────────────────────────────────────────────
  # If genie_spaces is not set (old single-space config), synthesize a single
  # space from the legacy flat variables so existing configs work without changes.
  # Table names in the legacy path are expanded from schema-relative to
  # fully-qualified using the legacy uc_catalog prefix.
  legacy_full_uc_tables = [for t in var.uc_tables :
    length(split(".", t)) >= 3 ? t : (var.uc_catalog != "" ? "${var.uc_catalog}.${t}" : t)
  ]

  legacy_genie_config = {
    title            = var.genie_space_title
    description      = var.genie_space_description
    sample_questions = var.genie_sample_questions
    instructions     = var.genie_instructions
    benchmarks       = var.genie_benchmarks
    sql_filters      = var.genie_sql_filters
    sql_expressions  = var.genie_sql_expressions
    sql_measures     = var.genie_sql_measures
    join_specs       = var.genie_join_specs
    acl_groups       = var.genie_acl_groups
  }

  legacy_space_name = var.genie_space_title != "" ? var.genie_space_title : "Genie Space"

  # The legacy single-space path is only activated when genie_space_title is
  # explicitly set (non-empty).  Having uc_tables in env.auto.tfvars for ABAC
  # policy generation must NOT cause a Genie agent to be created.
  effective_spaces = length(var.genie_spaces) > 0 ? var.genie_spaces : (
    var.genie_space_title != "" || var.genie_space_id != "" ? [{
      name             = local.legacy_space_name
      genie_space_id   = var.genie_space_id
      sql_warehouse_id = var.sql_warehouse_id
      uc_tables        = local.legacy_full_uc_tables
      acl_groups       = null
    }] : []
  )

  effective_genie_space_configs = length(var.genie_space_configs) > 0 ? var.genie_space_configs : (
    var.genie_space_title != "" ? { (local.legacy_space_name) = local.legacy_genie_config } : {}
  )

  canonical_space_names = {
    for idx, s in local.effective_spaces : idx => (
      s.name != "" ? s.name : lookup(var.genie_space_id_to_name, s.genie_space_id, s.genie_space_id)
    )
  }

  # Empty config used as fallback when a space has no abac config entry.
  empty_genie_config = {
    title            = ""
    description      = ""
    sample_questions = []
    instructions     = ""
    benchmarks       = []
    sql_filters      = []
    sql_expressions  = []
    sql_measures     = []
    join_specs       = []
    acl_groups       = []
  }

  # ── Merged space map passed to the workspace module ───────────────────────
  # The internal Terraform for_each key is derived by sanitizing the human-
  # readable name: lowercase, collapse any run of non-alphanumeric characters
  # into a single underscore, strip leading/trailing underscores.
  # e.g. "Finance & HR Analytics" -> "finance_hr_analytics"
  #
  # When name is omitted (empty string), genie_space_id is used as the key
  # directly — this is the common case when attaching to an existing space.
  # Preserve that legacy key for the first occurrence.  If another space has
  # the same sanitized key, disambiguate it with its stable existing-space ID,
  # or with its list index when it has not been created yet.  The "--" separator
  # cannot occur in a sanitized name.  Thus ordinary deployments keep their
  # current resource addresses while collisions cannot overwrite an entry in
  # this map.
  #
  # The name is also used as the default Genie agent title when genie_space_configs
  # does not set an explicit title.
  merged_spaces = {
    for idx, s in local.effective_spaces :
    (length([
      for prior_idx, prior in local.effective_spaces : prior
      if prior_idx < idx && (
        prior.name != ""
        ? trim(replace(lower(prior.name), "/[^a-z0-9]+/", "_"), "_")
        : prior.genie_space_id
        ) == (
        s.name != ""
        ? trim(replace(lower(s.name), "/[^a-z0-9]+/", "_"), "_")
        : s.genie_space_id
      )
      ]) == 0
      ? (s.name != ""
        ? trim(replace(lower(s.name), "/[^a-z0-9]+/", "_"), "_")
      : s.genie_space_id)
      : "${s.name != "" ? trim(replace(lower(s.name), "/[^a-z0-9]+/", "_"), "_") : s.genie_space_id}--${s.genie_space_id != "" ? s.genie_space_id : idx}"
      ) => {
      name             = local.canonical_space_names[idx]
      genie_space_id   = s.genie_space_id
      sql_warehouse_id = s.sql_warehouse_id != "" ? s.sql_warehouse_id : var.sql_warehouse_id
      uc_tables        = s.uc_tables
      config           = try(local.effective_genie_space_configs[local.canonical_space_names[idx]], local.empty_genie_config)
    }
  }
}

# ── Variables ─────────────────────────────────────────────────────────────────

variable "env_dir" {
  type = string
}

variable "databricks_account_host" {
  type    = string
  default = "https://accounts.cloud.databricks.com"
}

variable "databricks_account_id" {
  type    = string
  default = ""
}

variable "genie_only" {
  type        = bool
  default     = false
  description = "When true, skip account-level operations. The SP only needs Workspace Admin."
}

variable "databricks_client_id" {
  type = string
}

variable "databricks_client_secret" {
  type      = string
  sensitive = true
}

variable "databricks_workspace_id" {
  type = string
}

variable "serverless_usage_policy_id" {
  type    = string
  default = ""
}

variable "databricks_workspace_host" {
  type = string
}

# ── New multi-space variables ─────────────────────────────────────────────────

variable "genie_spaces" {
  type = list(object({
    name             = optional(string, "")
    genie_space_id   = optional(string, "")
    sql_warehouse_id = optional(string, "")
    uc_tables        = optional(list(string), [])
    acl_groups       = optional(list(string), null)
  }))
  default     = []
  description = "List of Genie agent definitions. 'name' is the human-readable agent title and the lookup key for genie_space_configs. An internal Terraform key is derived automatically by sanitizing the name."
}

variable "genie_space_id_to_name" {
  type        = map(string)
  default     = {}
  description = "Tool-owned mapping from imported Genie space IDs to their canonical names."
}

variable "genie_space_configs" {
  type = map(object({
    title            = optional(string, "")
    description      = optional(string, "")
    sample_questions = optional(list(string), [])
    instructions     = optional(string, "")
    benchmarks = optional(list(object({
      question = string
      sql      = string
    })), [])
    sql_filters = optional(list(object({
      sql          = string
      display_name = string
      comment      = string
      instruction  = string
    })), [])
    sql_expressions = optional(list(object({
      alias        = string
      sql          = string
      display_name = string
      comment      = string
      instruction  = string
    })), [])
    sql_measures = optional(list(object({
      alias        = string
      sql          = string
      display_name = string
      comment      = string
      instruction  = string
    })), [])
    join_specs = optional(list(object({
      left_table  = string
      left_alias  = string
      right_table = string
      right_alias = string
      sql         = string
      comment     = string
      instruction = string
    })), [])
    acl_groups = optional(list(string), [])
  }))
  default     = {}
  description = "Map of space key to Genie semantic config (title, benchmarks, join specs, etc.). Keys must match genie_spaces[*].key."
}

# ── Shared warehouse variable ─────────────────────────────────────────────────

variable "sql_warehouse_id" {
  type        = string
  default     = ""
  description = "Shared SQL warehouse ID for all spaces. Per-space sql_warehouse_id in genie_spaces overrides this."
}

variable "warehouse_name" {
  type    = string
  default = "ABAC Serverless Warehouse"
}

# ── Legacy single-space variables (kept for backward compatibility) ───────────

variable "uc_catalog" {
  type    = string
  default = ""
}

variable "uc_tables" {
  type    = list(string)
  default = []
}

variable "genie_space_id" {
  type    = string
  default = ""
}

variable "genie_space_title" {
  type    = string
  default = ""
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

variable "genie_acl_groups" {
  type        = list(string)
  default     = []
  description = "Groups that should have CAN_RUN access to this Genie agent. Explicit empty means no business access."
}

# ── Group variables ───────────────────────────────────────────────────────────

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

variable "business_access_enabled" {
  type        = bool
  default     = false
  description = "Fail-closed exposure gate. Enable only after coverage validation and the schema drift check pass."
}

# Shared env.auto.tfvars is consumed by both workspace and data-access roots.
# Classification is implemented only in data_access, but declaring the switch
# here avoids a misleading undeclared-variable warning during full apply.
variable "enable_classification" {
  type    = bool
  default = false
}

# Shared env.auto.tfvars is consumed by both workspace and data-access roots.
# Auto-tagging is implemented only in data_access; declare it here to avoid an
# undeclared-variable warning during a full apply.
variable "enable_auto_tagging" {
  type    = bool
  default = false
}

variable "group_members" {
  type    = map(list(string))
  default = {}
}

variable "tag_policies" {
  type = list(object({
    key         = string
    description = optional(string, "")
    values      = list(string)
  }))
  default = []
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

# ── Module call ───────────────────────────────────────────────────────────────

module "workspace" {
  source = "../../modules/workspace"

  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
  }

  databricks_account_id     = var.databricks_account_id
  databricks_client_id      = var.databricks_client_id
  databricks_client_secret  = var.databricks_client_secret
  databricks_workspace_id   = var.databricks_workspace_id
  databricks_workspace_host = var.databricks_workspace_host
  genie_only                = var.genie_only
  manage_groups             = var.manage_groups
  groups                    = var.groups
  business_access_enabled   = var.business_access_enabled
  sql_warehouse_id          = var.sql_warehouse_id
  warehouse_name            = var.warehouse_name
  genie_spaces              = local.merged_spaces
  genie_id_file_prefix      = "${var.env_dir}/.genie_space_id"
  genie_script_path         = "${local.project_root}/scripts/genie_space.sh"
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "group_ids" {
  value = module.workspace.group_ids
}

output "group_names" {
  value = module.workspace.group_names
}

output "workspace_assignments" {
  value = module.workspace.workspace_assignments
}

output "group_entitlements" {
  value = module.workspace.group_entitlements
}

output "sql_warehouse_id" {
  value = module.workspace.sql_warehouse_id
}

output "genie_space_acls_applied" {
  value = module.workspace.genie_space_acls_applied
}

output "genie_space_acls_groups" {
  value = module.workspace.genie_space_acls_groups
}

output "genie_spaces_created" {
  value = module.workspace.genie_spaces_created
}

output "genie_groups_csv" {
  value = module.workspace.genie_groups_csv
}
