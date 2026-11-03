terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      version               = "~> 1.111.0"
      configuration_aliases = [databricks.account, databricks.workspace]
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
}

locals {
  effective_uc_tables = distinct(concat(var.uc_tables, var.discovered_uc_tables))
  effective_warehouse_id = (
    var.sql_warehouse_id != ""
    ? var.sql_warehouse_id
    : databricks_sql_endpoint.warehouse[0].id
  )

  # Keyed without the value, so a treatment change updates the tag in place.
  treatment_tag_assignments = var.governance_mode == "deterministic" ? {
    for ta in var.tag_assignments : "${ta.entity_type}|${ta.entity_name}|${ta.tag_key}" => ta
    if ta.entity_type == "columns" && ta.tag_key == "gr_treatment"
  } : {}
  legacy_tag_assignments = [for ta in var.tag_assignments : ta if !(
    var.governance_mode == "deterministic" && ta.entity_type == "columns" && ta.tag_key == "gr_treatment"
  )]
  _grouped_tag_assignments = {
    for ta in local.legacy_tag_assignments :
    "${ta.entity_type}|${ta.entity_name}|${ta.tag_key}|${ta.tag_value}" => ta...
  }

  tag_assignment_map = {
    for key, grouped in local._grouped_tag_assignments :
    key => grouped[0]
  }

  fgac_policy_map = { for p in var.fgac_policies : p.name => p }

  # Built-in principals such as "account users" are intentionally absent from
  # the managed groups map, but still need catalog/schema/table grants. Derive
  # the access set from both managed groups and policy targets.
  # Legacy only. Deterministic access comes from table_readers alone (below).
  access_principals = var.governance_mode == "deterministic" ? [] : distinct(concat(
    keys(var.groups),
    flatten([
      for p in var.fgac_policies : p.to_principals
      if !startswith(p.comment, "GenieRails treatment fallback; principals are masking-only")
    ]),
    flatten(values(var.genie_space_acl_groups)),
  ))

  legacy_unattributed_discovered_tables = setsubtract(
    toset(var.discovered_uc_tables),
    toset(keys(var.table_agents)),
  )

  scoped_table_access_principals = {
    for table in local.effective_uc_tables : table => distinct(flatten([
      for agent in lookup(var.table_agents, table, []) :
      lookup(var.genie_space_acl_groups, agent, [])
    ]))
  }

  table_access_principals = {
    for table in local.effective_uc_tables : table => (
      contains(var.admin_uc_tables, table)
      || contains(local.legacy_unattributed_discovered_tables, table)
      ? local.access_principals
      : local.scoped_table_access_principals[table]
    )
  }

  table_access_pairs = var.governance_mode == "deterministic" ? local.reader_access_pairs : flatten([
    for table, principals in local.table_access_principals : [
      for principal in principals : { table = table, principal = principal }
    ]
  ])

  # Deterministic envs: SELECT only from table_readers (design decision 7).
  # A table's readers are the union of every entry matching it: the exact
  # table and its catalog.schema.*. Wildcards resolve over the tables this
  # layer knows are governed (uc_tables, agent tables, treatment-tagged
  # tables, table_readers keys and var.governed_tables). Agents' acl_groups
  # never grant SELECT here.
  grants_from_readers = var.governance_mode == "deterministic"
  _reader_map         = var.table_readers == null ? {} : var.table_readers
  _reader_groups      = { for key, readers in local._reader_map : lower(trimspace(key)) => readers... }
  reader_entries      = { for key, readers in local._reader_groups : key => distinct(flatten(readers)) }
  _known_tables = distinct([
    for table in concat(
      local.effective_uc_tables,
      [for ta in var.tag_assignments : join(".", slice(split(".", ta.entity_name), 0, 3)) if ta.entity_type == "columns" && length(split(".", ta.entity_name)) == 4],
      keys(local._reader_map),
      var.governed_tables,
    ) : lower(trimspace(table)) if length(split(".", table)) == 3 && !strcontains(table, "*")
  ])
  table_readers_resolved = {
    for table in local._known_tables : table => sort(distinct(flatten([
      for key, readers in local.reader_entries : readers
      if key == table || (endswith(key, ".*") && trimsuffix(key, ".*") == join(".", slice(split(".", table), 0, 2)))
    ])))
  }
  reader_access_pairs = local.grants_from_readers ? flatten([
    for table, readers in local.table_readers_resolved : [
      for principal in readers : { table = table, principal = principal }
    ]
  ]) : []
  # USE_CATALOG / USE_SCHEMA only where a reader is granted SELECT.
  reader_catalog_pairs = distinct([for pair in local.reader_access_pairs : { catalog = split(".", pair.table)[0], group = pair.principal }])
  reader_schema_pairs  = distinct([for pair in local.reader_access_pairs : { schema = join(".", slice(split(".", pair.table), 0, 2)), group = pair.principal }])

  # Refusals (output.access_config preconditions), so a raw terraform run
  # fails the same way make does.
  _allowed_readers = toset(concat(var.access_tier_groups, var.raw_exempt_principals))
  table_readers_refused = local.grants_from_readers ? flatten([
    for key, readers in local.reader_entries : [
      for reader in readers : "${key} = ${reader}"
      if !contains(local._allowed_readers, reader) || contains(["account users", "users"], lower(trimspace(reader)))
    ]
  ]) : []
  table_readers_bad_keys = [
    for key in keys(local.reader_entries) : key
    if length(split(".", key)) != 3 || strcontains(join(".", slice(split(".", key), 0, min(2, length(split(".", key))))), "*")
    || (strcontains(element(split(".", key), length(split(".", key)) - 1), "*") && element(split(".", key), length(split(".", key)) - 1) != "*")
  ]

  _ta_catalogs = [
    for ta in var.tag_assignments :
    split(".", ta.entity_name)[0]
  ]

  _fgac_catalogs = [
    for p in var.fgac_policies :
    p.catalog
  ]

  _uc_catalogs = [
    for t in local.effective_uc_tables :
    split(".", t)[0]
  ]

  _classification_catalogs = [
    for t in var.classification_uc_tables :
    split(".", t)[0]
  ]

  uc_schemas = distinct([
    for t in local.effective_uc_tables :
    join(".", slice(split(".", t), 0, 2))
  ])

  classification_uc_schemas = distinct([
    for t in var.classification_uc_tables :
    join(".", slice(split(".", t), 0, 2))
  ])

  classification_catalog_schemas = {
    for catalog in distinct(local._classification_catalogs) : catalog => distinct(concat([
      for schema in local.classification_uc_schemas : split(".", schema)[1]
      if split(".", schema)[0] == catalog
    ], lookup(var.classification_existing_schemas, catalog, [])))
  }

  # Native classifier types exercised by the dev-to-prod footprint. Auto-tagging
  # must be enabled per type; catalog classification alone does not land tags.
  classification_auto_tags = toset([
    "class.card_security_code",
    "class.credit_card",
    "class.date_of_birth",
    "class.email_address",
    "class.name",
    "class.phone_number",
    "class.us_ssn",
  ])

  all_catalogs = distinct(concat(
    local._ta_catalogs,
    local._fgac_catalogs,
    local._uc_catalogs,
  ))

  # Coverage check. scripts/coverage_gate.py reads this fingerprint (terraform
  # console), runs the gate, and records the result in var.coverage_gate_file.
  # Business SELECT is planned only while that file records a pass for exactly
  # these inputs, so editing the config, the masks, the DDL or the grants after
  # the gate (or passing -var overrides to a raw terraform run) fails the plan.
  #
  # The inputs are local snapshots of live UC tags and DDL. Terraform can't
  # re-read UC, so the pass must also carry the time make last refreshed them
  # (refreshed_at, from derive-assignments) and that refresh must be no older
  # than var.coverage_gate_max_age when this plan is made. The max age is a
  # gate input too: raising it after the gate ran makes the result stale, and
  # modules/coverage_gate_check caps it at 24h whatever it is set to.
  coverage_gate_grant_tables = sort(distinct([for pair in local.table_access_pairs : pair.table]))
  coverage_gate_fingerprint = sha256(jsonencode({
    version         = 1
    tag_assignments = sort(keys(local.tag_assignment_map))
    fgac_policies   = local.fgac_policy_map
    table_grants    = sort([for pair in local.table_access_pairs : "${pair.table}|${pair.principal}"])
    masking_sql     = filesha256(var.masking_sql_file)
    ddl             = fileexists(var.coverage_ddl_file) ? filesha256(var.coverage_ddl_file) : ""
    acknowledged    = sort(distinct([for column in var.coverage_acknowledged_columns : lower(column)]))
    max_age         = var.coverage_gate_max_age
  }))
  coverage_gate_status  = module.coverage_gate.status
  coverage_gate_problem = module.coverage_gate.problem

  # Keeping or revoking SELECT never needs the gate. A grant that the last
  # apply already made (var.applied_table_grants, from this layer's state)
  # stays plannable whatever the gate says, as long as nothing that protects
  # it was weakened since that apply (var.applied_protection): no tag
  # assignment or ABAC policy removed or changed (beyond dropping principals
  # that no longer hold a grant), the masking SQL unchanged, no new
  # acknowledgement and no higher max age. Added tags or policies and a
  # re-read DDL only add protection (or record what is already live), so they
  # don't hold up a revoke. A kept grant whose protection was weakened still
  # needs a current pass, and fails the plan without one.
  #
  # New grants are never planned without a pass: they are withheld (left out
  # of table_access) rather than failing the plan, so the removals and kept
  # grants in the same change still apply. make reports what was withheld and
  # exits non-zero; a raw terraform run of the root gets the
  # business_select_withheld warning.
  coverage_gate_protection = sha256(jsonencode({
    version         = 1
    deployment      = var.deployment_binding
    tag_assignments = sort(keys(local.tag_assignment_map))
    fgac_policies   = local.fgac_policy_map
    masking_sql     = filesha256(var.masking_sql_file)
    ddl             = fileexists(var.coverage_ddl_file) ? filesha256(var.coverage_ddl_file) : ""
    acknowledged    = sort(distinct([for column in var.coverage_acknowledged_columns : lower(column)]))
    max_age         = var.coverage_gate_max_age
  }))
  coverage_gate_protection_parts = {
    version         = 1
    deployment      = var.deployment_binding
    tag_assignments = sort(keys(local.tag_assignment_map))
    # Principals apart: dropping one from a policy weakens nothing once it
    # holds no grant (the usual way a group's access is withdrawn).
    fgac_policies = {
      for name, policy in local.fgac_policy_map : name => {
        rest   = sha256(jsonencode(merge(policy, { to_principals = [], except_principals = [] })))
        to     = sort(distinct(policy.to_principals))
        except = sort(distinct(policy.except_principals))
      }
    }
    masking_sql  = filesha256(var.masking_sql_file)
    acknowledged = sort(distinct([for column in var.coverage_acknowledged_columns : lower(column)]))
    max_age      = var.coverage_gate_max_age
  }
  _protection_unchanged = (
    var.applied_protection_fingerprint != ""
    && var.applied_protection_fingerprint == local.coverage_gate_protection
  )
  # Anything malformed or missing in the applied record counts as weakened.
  _granted_principals = toset([for pair in local.table_access_pairs : pair.principal])
  _protection_not_weakened = try(
    var.applied_protection.version == local.coverage_gate_protection_parts.version
    && var.applied_protection.deployment == var.deployment_binding
    && length(setsubtract(toset(var.applied_protection.tag_assignments), toset(local.coverage_gate_protection_parts.tag_assignments))) == 0
    && alltrue([
      for name, applied in var.applied_protection.fgac_policies :
      try(local.coverage_gate_protection_parts.fgac_policies[name].rest, "") == applied.rest
      && length(setintersection(local._granted_principals, setsubtract(toset(applied.to), toset(try(local.coverage_gate_protection_parts.fgac_policies[name].to, []))))) == 0
      && length(setintersection(local._granted_principals, setsubtract(toset(try(local.coverage_gate_protection_parts.fgac_policies[name].except, [])), toset(applied.except)))) == 0
    ])
    && var.applied_protection.masking_sql == local.coverage_gate_protection_parts.masking_sql
    && length(setsubtract(toset(local.coverage_gate_protection_parts.acknowledged), toset(var.applied_protection.acknowledged))) == 0
    && timecmp(timeadd("2000-01-01T00:00:00Z", var.coverage_gate_max_age), timeadd("2000-01-01T00:00:00Z", var.applied_protection.max_age)) <= 0,
    false
  )
  _protection_kept = local._protection_unchanged || local._protection_not_weakened

  table_grant_keys = sort([for pair in local.table_access_pairs : "${pair.table}|${pair.principal}"])
  table_grants_new = [for key in local.table_grant_keys : key if !contains(var.applied_table_grants, key)]
  table_grants_needing_gate = [
    for key in local.table_grant_keys : key
    if !(local._protection_kept && contains(var.applied_table_grants, key))
  ]
  # Kept grants that need a pass: the plan fails without one.
  table_grants_blocking = [for key in local.table_grants_needing_gate : key if contains(var.applied_table_grants, key)]
  table_grants_withheld = local.coverage_gate_status == "pass" && !var.withhold_new_table_grants ? [] : local.table_grants_new
}

# Shared with the workspace layer's CAN_RUN check; no resources.
module "coverage_gate" {
  source = "../coverage_gate_check"

  gate_file            = var.coverage_gate_file
  expected_fingerprint = local.coverage_gate_fingerprint
  max_age              = var.coverage_gate_max_age
  import_walk          = var.import_walk
}

# Data Classification is opt-in because deleting this resource disables scans
# for the catalog. When enabled, scope scans to only the schemas represented by
# the governed UC table footprint. Auto-tagging stays UI-managed by default;
# explicit true/false remains available for scripted environments such as CI.
resource "databricks_data_classification_catalog_config" "classification" {
  for_each = var.enable_classification ? local.classification_catalog_schemas : {}

  provider = databricks.workspace
  parent   = "catalogs/${each.key}"

  included_schemas = contains(var.classification_all_schemas, each.key) ? null : {
    names = each.value
  }

  auto_tag_configs = var.enable_auto_tagging == null ? lookup(
    var.classification_existing_auto_tag_configs, each.key, []
    ) : var.enable_auto_tagging ? [
    for classification_tag in local.classification_auto_tags : {
      classification_tag = classification_tag
      auto_tagging_mode  = "AUTO_TAGGING_ENABLED"
    }
  ] : []

  # Provider imports omit `parent`, although it is required and ForceNew in
  # configuration. Ignoring that import-only mismatch lets an existing
  # singleton catalog config be adopted and updated instead of deleted first.
  lifecycle {
    ignore_changes  = [parent]
    prevent_destroy = true
  }
}

resource "databricks_entity_tag_assignment" "assignments" {
  for_each = local.tag_assignment_map

  provider    = databricks.workspace
  entity_type = each.value.entity_type
  entity_name = each.value.entity_name
  tag_key     = each.value.tag_key
  tag_value   = each.value.tag_value

  depends_on = [databricks_grant.terraform_sp_manage_catalog]

  # Classification facts are owned by the environment's classifier. Do not
  # reconcile classifier updates back to a promoted Terraform snapshot.
  lifecycle {
    ignore_changes = all
  }
}

# Retagged only after the masking functions and every policy exist (the
# account layer has already added the allowed value), so a column never
# carries a treatment that no policy masks yet.
resource "databricks_entity_tag_assignment" "treatment" {
  for_each    = local.treatment_tag_assignments
  provider    = databricks.workspace
  entity_type = each.value.entity_type
  entity_name = each.value.entity_name
  tag_key     = each.value.tag_key
  tag_value   = each.value.tag_value
  depends_on = [
    databricks_grant.terraform_sp_manage_catalog,
    terraform_data.masking_functions,
    databricks_policy_info.policies,
  ]
}

resource "time_sleep" "wait_for_tag_propagation" {
  depends_on      = [databricks_entity_tag_assignment.assignments]
  create_duration = "30s"
}

resource "databricks_grant" "terraform_sp_manage_catalog" {
  # User/CLI-profile authentication has no service-principal client ID and the
  # active user already carries their own privileges. Avoid an invalid grant to
  # the empty-string principal in that supported path.
  for_each = var.databricks_client_id != "" ? toset([
    for catalog in local.all_catalogs : catalog if !contains(var.held_access_grants, "terraform_sp_manage_catalog|${catalog}")
  ]) : toset([])

  # SELECT lets verify-access prove row-pairing keys as this SP. This grant is
  # authoritative for the SP, so leaving SELECT out would revoke bootstrap's.
  provider   = databricks.workspace
  catalog    = each.value
  principal  = var.databricks_client_id
  privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "EXECUTE", "MANAGE", "CREATE_FUNCTION", "APPLY_TAG"]
}

# New USE_CATALOG and USE_SCHEMA wait with new SELECT: a staged phase before
# grant plans only those already applied (withhold_new_use_grants), so a
# business group gets nothing on a catalog before its masks are live.
resource "databricks_grant" "catalog_access" {
  for_each = {
    for key, pair in(var.governance_mode == "deterministic" ? tomap({
      for pair in local.reader_catalog_pairs : "${pair.catalog}|${pair.group}" => pair
      }) : tomap({
      for pair in setproduct(local.all_catalogs, local.access_principals) :
      "${pair[0]}|${pair[1]}" => { catalog = pair[0], group = pair[1] }
    })) : key => pair if !contains(var.held_access_grants, "catalog_access|${key}") && (!var.withhold_new_use_grants || contains(var.applied_use_grants, "catalog_access|${key}"))
  }

  provider   = databricks.workspace
  catalog    = each.value.catalog
  principal  = each.value.group
  privileges = ["USE_CATALOG"]

  # Order group grants after the deployment SP grant to avoid the SP-vs-group
  # read/modify/write race on catalog permissions.
  depends_on = [databricks_grant.terraform_sp_manage_catalog]
}

resource "databricks_grant" "schema_access" {
  for_each = {
    for key, pair in(var.governance_mode == "deterministic" ? tomap({
      for pair in local.reader_schema_pairs : "${pair.schema}|${pair.group}" => pair
      }) : tomap({
      for pair in setproduct(local.uc_schemas, local.access_principals) :
      "${pair[0]}|${pair[1]}" => { schema = pair[0], group = pair[1] }
    })) : key => pair if !contains(var.held_access_grants, "schema_access|${key}") && (!var.withhold_new_use_grants || contains(var.applied_use_grants, "schema_access|${key}"))
  }

  provider   = databricks.workspace
  schema     = each.value.schema
  principal  = each.value.group
  privileges = ["USE_SCHEMA"]
}

resource "databricks_grant" "table_access" {
  # Same addresses and keys as when this was held behind the retired
  # business_access_enabled flag, so already-released grants are never rebuilt.
  # New grants only with a pass (see table_grants_withheld).
  for_each = {
    for pair in local.table_access_pairs :
    "${pair.table}|${pair.principal}" => { table = pair.table, group = pair.principal }
    if((
      (local.coverage_gate_status == "pass" && !var.withhold_new_table_grants) || contains(var.applied_table_grants, "${pair.table}|${pair.principal}")
    ) && !contains(var.held_access_grants, "table_access|${pair.table}|${pair.principal}"))
  }

  provider   = databricks.workspace
  table      = each.value.table
  principal  = each.value.group
  privileges = ["SELECT"]

  # Business SELECT must not become reachable until tags, masking functions,
  # and every ABAC policy have been created and allowed to propagate.
  depends_on = [
    time_sleep.wait_for_tag_propagation,
    terraform_data.masking_functions,
    databricks_policy_info.policies,
    databricks_entity_tag_assignment.treatment,
    time_sleep.wait_for_policy_enforcement,
  ]

  # New grants never reach this without a pass (for_each above). Checked for
  # every planned instance, so neither a raw terraform run nor
  # terraform_layer.sh can keep a grant whose protection was weakened without
  # a current pass. Removed grants are never checked.
  lifecycle {
    precondition {
      condition     = local.coverage_gate_status == "pass" || !contains(local.table_grants_needing_gate, each.key)
      error_message = "Coverage check ${local.coverage_gate_status}: ${local.coverage_gate_problem}. Business SELECT grants already in place would be kept with weaker protection than their last checked apply (a tag assignment, ABAC policy or mask removed or changed, an acknowledgement added or coverage_gate_max_age raised), or with protection this layer's state doesn't record, which needs a passing check. Terraform can't re-read Unity Catalog, so run this layer through make (make apply, make plan, make release or make maintain ENV=${basename(dirname(dirname(var.coverage_gate_file)))}), which refreshes live tags and DDL, then runs the coverage check."
    }
  }
}

resource "databricks_sql_endpoint" "warehouse" {
  count = var.sql_warehouse_id == "" || var.retain_auto_warehouse ? 1 : 0

  provider         = databricks.workspace
  name             = var.warehouse_name
  cluster_size     = var.warehouse_cluster_size
  max_num_clusters = 1

  enable_serverless_compute = true
  warehouse_type            = "PRO"

  auto_stop_mins = 15
}

# No secret goes into these triggers: state keeps them, and a destroy-time
# provisioner can only read state, so a rotated secret would linger there and
# break --drop. The script loads the current SP credentials from auth_file.
# Only what changes the deployed functions forces a replacement, which only
# re-runs CREATE OR REPLACE: the drop lives in masking_functions_drop, so
# changing the SQL never drops a function the live policies use.
data "external" "normalized_masking_sql" {
  program = ["python3", "${path.module}/normalize_masking_sql.py"]
  query = {
    sql_file = var.masking_sql_file
  }
}

locals {
  # Checkout paths in state, relative where they can be: the script to the
  # project root (path.module/../.., the shared/ the run uses, a prepared
  # root's copy included), the SQL and auth files to masking_paths_base (the
  # env dir). A path outside its anchor, or with no base, stays as given.
  masking_project_root = "${abspath("${path.module}/../..")}/"
  masking_paths_base   = var.masking_paths_base == "" ? "" : "${abspath(var.masking_paths_base)}/"
  masking_drop_script = (startswith(abspath(var.deploy_masking_script), local.masking_project_root)
  ? trimprefix(abspath(var.deploy_masking_script), local.masking_project_root) : var.deploy_masking_script)
  masking_drop_files = {
    for key, path in { sql_file = var.masking_sql_file, auth_file = var.auth_file } :
    key => (local.masking_paths_base != "" && startswith(abspath(path), local.masking_paths_base)
    ? trimprefix(abspath(path), local.masking_paths_base) : path)
  }
}

resource "terraform_data" "masking_functions" {
  triggers_replace = {
    # Format 3 hashes the exact execution context and blocks used by the shared
    # deployer parser. Only the deployed definitions (sql_hash) and where they go
    # replace this resource: never the checkout's paths, so a plan from another
    # clone, CI runner or moved repo changes nothing. Upgrading state that still
    # records sql_file, script and auth_file here runs one drop-free CREATE OR
    # REPLACE of the same functions (the guards and the plan note call it a
    # re-key); safe function reordering never replaces it.
    sql_hash     = data.external.normalized_masking_sql.result.hash
    warehouse_id = local.effective_warehouse_id
    host         = var.databricks_workspace_host
    client_id    = var.databricks_client_id
  }

  # The arguments go in as environment variables the shell expands quoted, so
  # a checkout path with spaces or shell characters reaches the script intact.
  provisioner "local-exec" {
    command = "python3 \"$MASKING_SCRIPT\" --sql-file \"$MASKING_SQL_FILE\" --warehouse-id \"$MASKING_WAREHOUSE_ID\" --auth-file \"$MASKING_AUTH_FILE\" --host \"$MASKING_HOST\""
    environment = {
      MASKING_SCRIPT       = var.deploy_masking_script
      MASKING_SQL_FILE     = var.masking_sql_file
      MASKING_WAREHOUSE_ID = self.triggers_replace.warehouse_id
      MASKING_AUTH_FILE    = var.auth_file
      MASKING_HOST         = self.triggers_replace.host
    }
  }

  depends_on = [
    time_sleep.wait_for_tag_propagation,
    databricks_grant.terraform_sp_manage_catalog,
    databricks_sql_endpoint.warehouse,
    terraform_data.masking_functions_drop,
  ]
}

# Drops the functions when the layer is destroyed, after the policies and
# masking_functions are gone. Its settings are input, not triggers, so a
# change updates it in place: nothing but a real destroy runs the drop.
# A destroy-time provisioner reads only state, so the paths it records are
# relative (local.masking_drop_*) and resolved when the destroy runs: the
# script under this run's path.module/../.., the SQL and auth files under
# GENIERAILS_MASKING_BASE_DIR, which terraform_layer.sh exports as the env
# dir. A destroy from another checkout drops the functions with that
# checkout's files; without the variable the drop refuses rather than guess.
# sql_hash records which functions these are: the drop hashes the SQL it
# resolved and refuses, before it connects, when that is another env's (a
# wrong GENIERAILS_MASKING_BASE_DIR) or the file changed since the last apply.
# All state from before this (absolute paths, no hash) refuses to drop,
# whether its paths still exist or not, until an apply from this checkout
# re-keys the drop (relative paths and sql_hash): a reused or replaced
# checkout may hold another env's SQL at those paths. An absolute path
# recorded with sql_hash that is gone is found by its place under the old
# shared/ (the old script's dir) or the env dir, and the hash then checked.
resource "terraform_data" "masking_functions_drop" {
  input = {
    sql_file     = local.masking_drop_files.sql_file
    script       = local.masking_drop_script
    auth_file    = local.masking_drop_files.auth_file
    sql_hash     = data.external.normalized_masking_sql.result.hash
    warehouse_id = local.effective_warehouse_id
    host         = var.databricks_workspace_host
  }

  provisioner "local-exec" {
    when    = destroy
    command = "python3 \"$MASKING_SCRIPT\" --sql-file \"$MASKING_SQL_FILE\" --warehouse-id \"$MASKING_WAREHOUSE_ID\" --auth-file \"$MASKING_AUTH_FILE\" --host \"$MASKING_HOST\" --drop --expect-sql-hash \"$MASKING_SQL_HASH\" --relocate-from \"$MASKING_RELOCATE_FROM\" --base-dir \"$${GENIERAILS_MASKING_BASE_DIR:-}\""
    environment = {
      # A recorded deploy_masking_functions.py, relative or an older state's
      # absolute path, runs from this checkout; it understands these flags.
      MASKING_SCRIPT = (!startswith(self.input.script, "/") ? "${abspath("${path.module}/../..")}/${self.input.script}"
        : basename(self.input.script) == "deploy_masking_functions.py"
      ? "${abspath("${path.module}/../..")}/deploy_masking_functions.py" : self.input.script)
      MASKING_SQL_FILE     = self.input.sql_file
      MASKING_WAREHOUSE_ID = self.input.warehouse_id
      MASKING_AUTH_FILE    = self.input.auth_file
      MASKING_HOST         = self.input.host
      MASKING_SQL_HASH     = try(self.input.sql_hash, "")
      # The shared/ an absolute path was recorded under, for the deployer to
      # find a moved file at the same place under this checkout's.
      MASKING_RELOCATE_FROM = (startswith(self.input.script, "/") && basename(self.input.script) == "deploy_masking_functions.py"
      ? dirname(self.input.script) : "")
    }
  }

  depends_on = [
    time_sleep.wait_for_tag_propagation,
    # Keep the SP's catalog privileges in place until function drops finish.
    databricks_grant.terraform_sp_manage_catalog,
    databricks_sql_endpoint.warehouse,
  ]
}

# Earlier versions managed the functions as this null_resource, with the SP
# secret in its triggers. Forget it without running its destroy-time --drop;
# terraform_data.masking_functions takes over with CREATE OR REPLACE, so no
# function is dropped and the stale secret leaves state.
removed {
  from = null_resource.deploy_masking_functions

  lifecycle {
    destroy = false
  }
}

resource "databricks_policy_info" "policies" {
  for_each = local.fgac_policy_map

  provider = databricks.workspace

  name                  = "${each.value.catalog}_${each.key}"
  on_securable_type     = "CATALOG"
  on_securable_fullname = each.value.catalog
  policy_type           = each.value.policy_type
  for_securable_type    = "TABLE"
  to_principals         = each.value.to_principals
  except_principals     = length(each.value.except_principals) > 0 ? each.value.except_principals : null
  comment               = each.value.comment

  when_condition = each.value.when_condition

  # Column masks and column-aware row filters both bind matched column aliases.
  # For a row filter, match_alias is passed to the function through `using`.
  match_columns = (
    contains(["POLICY_TYPE_COLUMN_MASK", "POLICY_TYPE_ROW_FILTER"], each.value.policy_type)
    && each.value.match_condition != null
    ) ? [{
      condition = each.value.match_condition
      alias     = each.value.match_alias
  }] : null

  column_mask = each.value.policy_type == "POLICY_TYPE_COLUMN_MASK" ? {
    function_name = "${each.value.function_catalog}.${each.value.function_schema}.${each.value.function_name}"
    on_column     = each.value.match_alias
    using         = null
  } : null

  row_filter = each.value.policy_type == "POLICY_TYPE_ROW_FILTER" ? {
    function_name = "${each.value.function_catalog}.${each.value.function_schema}.${each.value.function_name}"
    using = each.value.match_alias != null ? [{
      alias = each.value.match_alias
    }] : []
  } : null

  depends_on = [
    time_sleep.wait_for_tag_propagation,
    databricks_grant.catalog_access,
    databricks_grant.schema_access,
    databricks_grant.terraform_sp_manage_catalog,
    terraform_data.masking_functions,
  ]

}

# Unity Catalog policy creation and treatment retags can return before
# enforcement is observable. New table grants wait out that window. The wait
# restarts when the policies change or the masking functions are redeployed
# (any terraform_data replacement: SQL, warehouse, host, client ID), which
# delays grants created in the same apply; SELECT grants that
# already exist stay in place throughout.
resource "time_sleep" "wait_for_policy_enforcement" {
  depends_on      = [databricks_policy_info.policies, databricks_entity_tag_assignment.treatment]
  create_duration = "30s"

  triggers = {
    policy_hash          = sha256(jsonencode(local.fgac_policy_map))
    masking_functions_id = terraform_data.masking_functions.id
  }
}
