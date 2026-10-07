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
  required_version = ">= 1.7"

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

  # ── Cross-layer exposure check for Genie CAN_RUN ──────────────────────────
  # CAN_RUN is granted only after the data_access layer was applied with a
  # passing coverage gate and business grants in place, and only while the gate
  # result on disk is still for the inputs that apply used (otherwise the
  # governance config moved on and hasn't been applied) and rests on a live
  # refresh no older than the max age that apply recorded. The result is
  # judged by modules/coverage_gate_check, the same check data_access uses.
  # Read from the local state the layer runner writes; anything missing or
  # unreadable blocks.
  data_access_dir        = "${var.env_dir}/data_access"
  _data_access_state     = fileexists("${local.data_access_dir}/terraform.tfstate") ? try(jsondecode(file("${local.data_access_dir}/terraform.tfstate")), null) : null
  _applied_coverage_gate = try(local._data_access_state.outputs.coverage_gate.value, null)
  genie_exposure_blocker = (
    local._data_access_state == null ? "the data_access layer has no readable state (${local.data_access_dir}/terraform.tfstate)" :
    local._applied_coverage_gate == null ? "the data_access state predates the coverage gate; re-apply the data_access layer" :
    # Legacy state from before business_access_enabled was retired: an apply
    # made with it false granted nothing. Current state no longer records it.
    try(local._applied_coverage_gate.business_access_enabled, true) != true ? "the data_access layer was last applied with business access closed (before business_access_enabled was retired); re-apply the data_access layer" :
    try(local._applied_coverage_gate.status, "") != "pass" ? "the data_access layer was last applied without a passing coverage gate" :
    try(local._applied_coverage_gate.table_grant_count, 0) < 1 ? "the data_access layer has no business table grants in place" :
    try(local._applied_coverage_gate.max_age, null) == null ? "the data_access state predates the coverage-gate max age; re-apply the data_access layer" :
    module.coverage_gate_check.status == "stale" ? "the data_access config changed after its last gated apply" :
    module.coverage_gate_check.problem
  )

  # What CAN_RUN the last apply left in place, per space, from this layer's
  # own state (the layer runner's local backend): the groups recorded in the
  # ACL resource's triggers, for the same Genie agent (same space ID, or the
  # same created agent on the same host). The blocker and grant checks only
  # bite for groups beyond that, so keeping, shrinking or clearing an ACL
  # always plans, and an unchanged ACL never errors once the gate expires.
  # Unreadable state means nothing is on record (fail closed).
  _own_state = fileexists("${var.env_dir}/terraform.tfstate") ? try(jsondecode(file("${var.env_dir}/terraform.tfstate")), null) : null
  _state_instances = flatten([
    for resource in try(local._own_state.resources, []) : [
      for instance in try(resource.instances, []) : {
        name  = resource.name
        key   = try(tostring(instance.index_key), "")
        attrs = try(instance.attributes, {})
      }
      # Tainted and deposed (failed or partial replacement) objects aren't
      # successfully applied CAN_RUN.
      if try(instance.status, "") != "tainted" && try(instance.deposed, "") == ""
    ]
    if try(resource.module, "") == "module.workspace" && try(resource.mode, "") == "managed"
  ])
  # Grouped so an unexpected duplicate can't fail the plan; anything but
  # exactly one current object counts as nothing applied.
  _applied_candidates = { for instance in local._state_instances : "${instance.name}|${instance.key}" => instance.attrs... }
  _applied            = { for key, attrs in local._applied_candidates : key => attrs[0] if length(attrs) == 1 }
  applied_can_run_groups = {
    for key, space in local.merged_spaces : key => [
      for group in split(",", (
        space.genie_space_id != ""
        ? (
          try(local._applied["genie_space_acls|${key}"].triggers.space_id, "") == space.genie_space_id
          ? try(local._applied["genie_space_acls|${key}"].triggers.groups, "")
          : ""
        )
        : (
          try(local._applied["genie_space_acls_created|${key}"].triggers.space_create_id, "") != ""
          && try(local._applied["genie_space_acls_created|${key}"].triggers.space_create_id, "") == try(local._applied["genie_space|${key}"].id, "-")
          && try(local._applied["genie_space|${key}"].triggers_replace.value.host, local._applied["genie_space|${key}"].triggers_replace.host, "") == var.databricks_workspace_host
          ? try(local._applied["genie_space_acls_created|${key}"].triggers.groups, "")
          : ""
        )
      )) : group if group != ""
    ]
  }
  # Groups each space's desired CAN_RUN adds beyond what is applied.
  genie_space_can_run_widening = {
    for key, space in local.merged_spaces : key => (
      length(var.groups) > 0
      ? sort(tolist(setsubtract(toset(space.config.acl_groups), toset(local.applied_can_run_groups[key]))))
      : []
    )
  }

  # Per space: the SELECT grants its CAN_RUN groups need must be in the
  # data_access state (table_grant_resource_keys, "<table>|<group>"). Spaces
  # that list tables need every table x group pair (a catalog.schema.* entry
  # needs a grant in that schema); spaces known only by ID, whose tables were
  # discovered on the data_access side, need at least one grant per group.
  _applied_table_grants = toset(try(local._data_access_state.outputs.table_grant_resource_keys.value, []))
  _space_tables = {
    for key, space in local.merged_spaces : key => [
      for t in space.uc_tables :
      length(split(".", t)) >= 3 ? t : (var.uc_catalog != "" ? "${var.uc_catalog}.${t}" : t)
    ]
  }
  genie_space_missing_grants = {
    for key, space in local.merged_spaces : key => (
      length(local._space_tables[key]) > 0
      ? [
        for pair in setproduct(local._space_tables[key], space.config.acl_groups) : "${pair[0]}|${pair[1]}"
        if length([
          for grant in local._applied_table_grants : grant
          if endswith(pair[0], "*")
          ? (startswith(grant, trimsuffix(pair[0], "*")) && endswith(grant, "|${pair[1]}"))
          : grant == "${pair[0]}|${pair[1]}"
        ]) == 0
      ]
      : [
        for group in space.config.acl_groups : "<any table>|${group}"
        if length([for grant in local._applied_table_grants : grant if endswith(grant, "|${group}")]) == 0
      ]
    )
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
  description = "User-owned Genie agent definitions. 'name' is the semantic-config lookup key. acl_groups omitted/null derives fresh from policy principals, [] explicitly grants nobody, and a non-empty list is the durable override; generated semantic drafts do not own ACLs."
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
  description = "Tool-owned semantic config (title, benchmarks, joins, etc.). Keys match genie_spaces names. Any nested acl_groups is resolved input for compatibility, not durable ACL ownership; durable intent belongs on genie_spaces[]."
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
  description = "Legacy single-agent CAN_RUN groups. Explicit empty means no business access. Multi-agent durable ACL intent belongs on genie_spaces[].acl_groups instead."
}

# ── Group variables ───────────────────────────────────────────────────────────

variable "access_tier_groups" {
  type        = list(string)
  default     = []
  description = "Generate-time input only (read by make generate, carried by make promote): existing IdP-synced access-tier groups, most to least privileged. Declared so env.auto.tfvars loads cleanly; no resource reads it."
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

variable "business_access_enabled" {
  type        = bool
  default     = null
  description = "DEPRECATED and ignored; removed in the next release. Business SELECT and Genie CAN_RUN are granted whenever the coverage gate allows it, so true and false both do nothing (false does NOT revoke access: remove the groups or acl_groups entries instead). Still declared so existing env.auto.tfvars files and -var flags keep working; make warns while it is set."
}

# Shared env.auto.tfvars is consumed by both workspace and data-access roots.
# Classification is implemented only in data_access, but declaring the switch
# here avoids a misleading undeclared-variable warning during full apply.
variable "enable_classification" {
  type    = bool
  default = false
}

variable "verify_key_column" {
  type        = string
  default     = ""
  description = "Non-sensitive stable row identifier used only by effective-access verification tooling."
}

# Shared env.auto.tfvars is consumed by both workspace and data-access roots.
# The coverage gate reads acknowledgements only in data_access; declare it
# here to avoid an undeclared-variable warning during a full apply.
variable "coverage_acknowledged_columns" {
  type    = list(string)
  default = []
}

# Read by data_access, which binds it into the gate and records it in state;
# the CAN_RUN check uses that recorded value, so it can't be overridden here.
variable "coverage_gate_max_age" {
  type    = string
  default = "6h"
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

# The current gate result must still be for the inputs (fingerprint, max age)
# the last data_access apply used, and its live refresh must be recent.
module "coverage_gate_check" {
  source = "../../modules/coverage_gate_check"

  gate_file            = "${local.data_access_dir}/.coverage_gate.json"
  expected_fingerprint = try(local._applied_coverage_gate.fingerprint, "")
  max_age              = try(local._applied_coverage_gate.max_age, "")
}

module "workspace" {
  source = "../../modules/workspace"

  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
  }

  databricks_account_id        = var.databricks_account_id
  databricks_client_id         = var.databricks_client_id
  databricks_client_secret     = var.databricks_client_secret
  databricks_workspace_id      = var.databricks_workspace_id
  databricks_workspace_host    = var.databricks_workspace_host
  genie_only                   = var.genie_only
  manage_groups                = var.manage_groups
  groups                       = var.groups
  genie_exposure_blocker       = local.genie_exposure_blocker
  genie_space_missing_grants   = local.genie_space_missing_grants
  genie_space_can_run_widening = local.genie_space_can_run_widening
  sql_warehouse_id             = var.sql_warehouse_id
  warehouse_name               = var.warehouse_name
  genie_spaces                 = local.merged_spaces
  genie_id_file_prefix         = "${var.env_dir}/.genie_space_id"
  genie_script_path            = "${local.project_root}/scripts/genie_space.sh"
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

output "genie_space_missing_grants" {
  description = "Per Genie agent: <table>|<group> SELECT grants its CAN_RUN groups need that the data_access state doesn't have. Any entry blocks that agent's non-empty CAN_RUN."
  value       = local.genie_space_missing_grants
}

output "genie_space_can_run_widening" {
  description = "Per Genie agent: CAN_RUN groups its ACL adds beyond what the last apply left in place. Only these need the coverage gate and the agent's grants."
  value       = local.genie_space_can_run_widening
}

output "genie_exposure_blocker" {
  description = "Why Genie CAN_RUN grants are blocked (data_access layer not applied with a current passing coverage gate), or \"\" when they may be granted."
  value       = local.genie_exposure_blocker
}

output "genie_spaces_created" {
  value = module.workspace.genie_spaces_created
}

output "genie_groups_csv" {
  value = module.workspace.genie_groups_csv
}
