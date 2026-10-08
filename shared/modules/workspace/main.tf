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
  }
}

data "databricks_group" "existing" {
  for_each = var.genie_only ? {} : var.groups

  provider     = databricks.account
  display_name = each.key
}

locals {
  group_ids = {
    for name, group in data.databricks_group.existing : name => group.id
  }

  shared_warehouse_id = (
    var.sql_warehouse_id != ""
    ? var.sql_warehouse_id
    : databricks_sql_endpoint.warehouse[0].id
  )

  # ACL resolution is fail-closed before this module. Explicit [] means nobody.
  # When var.groups is empty (genie-only mode), ACLs are skipped entirely.
  genie_space_groups = length(var.groups) > 0 ? {
    for key, space in var.genie_spaces : key => join(",", space.config.acl_groups)
  } : {}

  # Opening or widening CAN_RUN requires the data_access layer's gated grants
  # for this space's tables and groups (genie_exposure_blocker and
  # genie_space_missing_grants in the root). Without them the groups beyond
  # what the last apply left in place (genie_space_can_run_widening) are
  # withheld: the ACL is set to the desired groups minus those, so keeping,
  # shrinking or clearing it (and every other space's change) still applies.
  # An unknown widening withholds every desired group.
  genie_space_can_run_withheld = {
    for key, csv in local.genie_space_groups : key => (
      csv == "" || length(try(var.genie_space_can_run_widening[key], ["unknown"])) == 0
      || (var.genie_exposure_blocker == "" && length(try(var.genie_space_missing_grants[key], ["unknown"])) == 0)
      ? []
      : try(var.genie_space_can_run_widening[key], split(",", csv))
    )
  }
  genie_space_acl_groups = {
    for key, csv in local.genie_space_groups : key => join(",", [
      for group in split(",", csv) : group
      if group != "" && !contains(local.genie_space_can_run_withheld[key], group)
    ])
  }
  # A space whose every desired group is withheld gets no ACL resource rather
  # than an empty one (set-acls with no groups would clear its direct ACL).
  genie_space_acl_keys = [
    for key, csv in local.genie_space_groups : key
    if csv == "" || local.genie_space_acl_groups[key] != ""
  ]

  # Spaces that already have an ID — apply ACLs, and config if defined.
  existing_spaces = { for k, v in var.genie_spaces : k => v if v.genie_space_id != "" }

  # Existing spaces that have non-trivial config — also run update-config.
  existing_spaces_with_config = {
    for k, v in local.existing_spaces : k => v
    if(
      length(v.config.benchmarks) > 0 ||
      v.config.instructions != "" ||
      v.config.description != "" ||
      length(v.config.sample_questions) > 0
    )
  }

  # Spaces that need to be created — genie_space_id is empty and uc_tables is non-empty.
  new_spaces = {
    for k, v in var.genie_spaces : k => v
    if v.genie_space_id == "" && length(v.uc_tables) > 0
  }
}

resource "databricks_mws_permission_assignment" "group_assignments" {
  for_each = var.genie_only ? {} : local.group_ids

  provider     = databricks.account
  workspace_id = var.databricks_workspace_id
  principal_id = each.value
  permissions  = ["USER"]
}

resource "databricks_entitlements" "group_entitlements" {
  for_each = var.genie_only ? {} : local.group_ids

  provider = databricks.workspace
  group_id = each.value

  workspace_consume = true

  depends_on = [databricks_mws_permission_assignment.group_assignments]
}

resource "databricks_sql_endpoint" "warehouse" {
  count = var.sql_warehouse_id == "" || var.retain_auto_warehouse ? 1 : 0

  provider         = databricks.workspace
  name             = var.warehouse_name
  cluster_size     = "Small"
  max_num_clusters = 1

  enable_serverless_compute = true
  warehouse_type            = "PRO"

  auto_stop_mins = 15
}

# ── Existing spaces: apply ACLs + config (when config is defined) ─────────────

resource "null_resource" "genie_space_acls" {
  for_each = {
    for k, v in local.existing_spaces : k => v
    if contains(local.genie_space_acl_keys, k)
  }

  triggers = {
    space_id = each.value.genie_space_id
    groups   = local.genie_space_acl_groups[each.key]
  }

  provisioner "local-exec" {
    command = var.genie_script_path == "" ? "true" : "${var.genie_script_path} set-acls"

    environment = {
      DATABRICKS_HOST          = var.databricks_workspace_host
      DATABRICKS_CLIENT_ID     = var.databricks_client_id
      DATABRICKS_CLIENT_SECRET = var.databricks_client_secret
      GENIE_SPACE_OBJECT_ID    = each.value.genie_space_id
      GENIE_GROUPS_CSV         = local.genie_space_acl_groups[each.key]
      GENIE_ALLOW_EMPTY_ACL    = "1"
    }
  }

  # Removing the space or its groups from config (or all groups) destroys
  # this resource: take back the CAN_RUN it granted, only for those groups.
  # A change of groups replaces it, so the old groups are revoked just before
  # set-acls grants the new list. terraform_layer.sh always runs from
  # shared/roots/workspace; the script reads current credentials from
  # $LAYER_ENV_DIR/auth.auto.tfvars, so none is kept in state.
  provisioner "local-exec" {
    when    = destroy
    command = "bash ../../scripts/genie_space.sh revoke-acls"

    environment = {
      GENIE_SPACE_OBJECT_ID   = self.triggers.space_id
      GENIE_REVOKE_GROUPS_CSV = self.triggers.groups
    }
  }

  depends_on = [databricks_mws_permission_assignment.group_assignments]
}

# If every desired group is withheld, there is deliberately no grant-bearing
# ACL resource. Still authoritative-sync an empty direct ACL so an adopted
# agent cannot retain hand-added CAN_RUN. set-acls audits and prints every
# removal and fails closed when it cannot read the current ACL.
resource "null_resource" "genie_space_acls_removal_only" {
  for_each = {
    for k, v in local.existing_spaces : k => v
    if lookup(local.genie_space_groups, k, "") != ""
    && lookup(local.genie_space_acl_groups, k, "") == ""
  }

  triggers = {
    space_id = each.value.genie_space_id
    groups   = ""
    withheld = join(",", local.genie_space_can_run_withheld[each.key])
  }

  provisioner "local-exec" {
    command = var.genie_script_path == "" ? "true" : "${var.genie_script_path} set-acls"

    environment = {
      DATABRICKS_HOST          = var.databricks_workspace_host
      DATABRICKS_CLIENT_ID     = var.databricks_client_id
      DATABRICKS_CLIENT_SECRET = var.databricks_client_secret
      GENIE_SPACE_OBJECT_ID    = each.value.genie_space_id
      GENIE_GROUPS_CSV         = ""
      GENIE_ALLOW_EMPTY_ACL    = "1"
    }
  }

  depends_on = [databricks_mws_permission_assignment.group_assignments]
}

# ── Existing spaces: apply config (when genie_space_configs is defined) ───────

resource "null_resource" "genie_space_config_existing" {
  for_each = local.existing_spaces_with_config

  triggers = {
    space_id           = each.value.genie_space_id
    description        = each.value.config.description
    questions          = jsonencode(each.value.config.sample_questions)
    instructions       = each.value.config.instructions
    benchmarks         = jsonencode(each.value.config.benchmarks)
    sql_filters        = jsonencode(each.value.config.sql_filters)
    sql_measures       = jsonencode(each.value.config.sql_measures)
    sql_expressions    = jsonencode(each.value.config.sql_expressions)
    join_specs         = jsonencode(each.value.config.join_specs)
    warehouse_id       = each.value.configured_sql_warehouse_id == null ? "" : each.value.configured_sql_warehouse_id
    warehouse_explicit = (each.value.configured_sql_warehouse_id == null ? "" : each.value.configured_sql_warehouse_id) != "" ? "1" : "0"
  }

  provisioner "local-exec" {
    command = "${var.genie_script_path} update-config"

    environment = {
      DATABRICKS_HOST          = var.databricks_workspace_host
      DATABRICKS_CLIENT_ID     = var.databricks_client_id
      DATABRICKS_CLIENT_SECRET = var.databricks_client_secret
      GENIE_SPACE_OBJECT_ID    = each.value.genie_space_id
      GENIE_TABLES_CSV         = join(",", each.value.uc_tables)
      GENIE_TITLE              = each.value.config.title != "" ? each.value.config.title : each.value.name
      GENIE_DESCRIPTION        = each.value.config.description
      GENIE_SAMPLE_QUESTIONS   = jsonencode(each.value.config.sample_questions)
      GENIE_INSTRUCTIONS       = each.value.config.instructions
      GENIE_BENCHMARKS         = jsonencode(each.value.config.benchmarks)
      GENIE_SQL_FILTERS        = jsonencode(each.value.config.sql_filters)
      GENIE_SQL_EXPRESSIONS    = jsonencode(each.value.config.sql_expressions)
      GENIE_SQL_MEASURES       = jsonencode(each.value.config.sql_measures)
      GENIE_JOIN_SPECS         = jsonencode(each.value.config.join_specs)
      GENIE_WAREHOUSE_ID       = self.triggers.warehouse_id
      GENIE_WAREHOUSE_EXPLICIT = self.triggers.warehouse_explicit
    }
  }

  depends_on = [databricks_mws_permission_assignment.group_assignments]
}

# ── New spaces: create ────────────────────────────────────────────────────────

# Only the host forces a replacement: moving a space to another workspace must
# trash it there before creating its replacement. No credential is kept here:
# create gets them from variables, and trash reads the layer's auth file.
resource "terraform_data" "genie_space" {
  for_each = local.new_spaces

  triggers_replace = {
    host = var.databricks_workspace_host
  }

  input = {
    id_file = "${var.genie_id_file_prefix}_${each.key}"
  }

  provisioner "local-exec" {
    command = "${var.genie_script_path} create"

    environment = {
      DATABRICKS_HOST          = var.databricks_workspace_host
      DATABRICKS_CLIENT_ID     = var.databricks_client_id
      DATABRICKS_CLIENT_SECRET = var.databricks_client_secret
      GENIE_ID_FILE            = "${var.genie_id_file_prefix}_${each.key}"
      GENIE_TABLES_CSV         = join(",", each.value.uc_tables)
      GENIE_WAREHOUSE_ID = (
        each.value.sql_warehouse_id != ""
        ? each.value.sql_warehouse_id
        : local.shared_warehouse_id
      )
      GENIE_TITLE = each.value.config.title != "" ? each.value.config.title : each.value.name
    }
  }

  provisioner "local-exec" {
    when = destroy
    # terraform_layer.sh always executes from shared/roots/workspace. Keep this
    # command project-relative so state remains portable across worktrees.
    command = "bash ../../scripts/genie_space.sh trash"

    environment = {
      GENIE_ID_BASENAME   = basename(self.input.id_file)
      GENIE_EXPECTED_HOST = self.triggers_replace.host
    }
  }

  depends_on = [
    databricks_mws_permission_assignment.group_assignments,
    databricks_sql_endpoint.warehouse,
  ]
}

# Earlier versions created spaces with this null_resource, whose triggers kept
# the SP secret in state. Forget it without running its destroy-time trash;
# terraform_data.genie_space adopts the agent named in its ID file, so the
# agent and its ID are unchanged.
removed {
  from = null_resource.genie_space_create

  lifecycle {
    destroy = false
  }
}

# ── New spaces: apply config ──────────────────────────────────────────────────

resource "null_resource" "genie_space_config" {
  for_each = local.new_spaces

  triggers = {
    tables          = join(",", each.value.uc_tables)
    title           = each.value.config.title
    description     = each.value.config.description
    questions       = jsonencode(each.value.config.sample_questions)
    instructions    = each.value.config.instructions
    benchmarks      = jsonencode(each.value.config.benchmarks)
    sql_filters     = jsonencode(each.value.config.sql_filters)
    sql_measures    = jsonencode(each.value.config.sql_measures)
    sql_expressions = jsonencode(each.value.config.sql_expressions)
    join_specs      = jsonencode(each.value.config.join_specs)
    warehouse_id    = each.value.sql_warehouse_id != "" ? each.value.sql_warehouse_id : local.shared_warehouse_id
    space_create_id = terraform_data.genie_space[each.key].id
  }

  provisioner "local-exec" {
    command = "${var.genie_script_path} update-config"

    environment = {
      DATABRICKS_HOST          = var.databricks_workspace_host
      DATABRICKS_CLIENT_ID     = var.databricks_client_id
      DATABRICKS_CLIENT_SECRET = var.databricks_client_secret
      GENIE_ID_FILE            = "${var.genie_id_file_prefix}_${each.key}"
      GENIE_TABLES_CSV         = join(",", each.value.uc_tables)
      GENIE_WAREHOUSE_ID = (
        each.value.sql_warehouse_id != ""
        ? each.value.sql_warehouse_id
        : local.shared_warehouse_id
      )
      GENIE_WAREHOUSE_EXPLICIT        = (each.value.configured_sql_warehouse_id == null ? "" : each.value.configured_sql_warehouse_id) != "" ? "1" : "0"
      GENIE_WAREHOUSE_CREATED_DEFAULT = "1"
      GENIE_TITLE                     = each.value.config.title != "" ? each.value.config.title : each.value.name
      GENIE_DESCRIPTION               = each.value.config.description
      GENIE_SAMPLE_QUESTIONS          = jsonencode(each.value.config.sample_questions)
      GENIE_INSTRUCTIONS              = each.value.config.instructions
      GENIE_BENCHMARKS                = jsonencode(each.value.config.benchmarks)
      GENIE_SQL_FILTERS               = jsonencode(each.value.config.sql_filters)
      GENIE_SQL_EXPRESSIONS           = jsonencode(each.value.config.sql_expressions)
      GENIE_SQL_MEASURES              = jsonencode(each.value.config.sql_measures)
      GENIE_JOIN_SPECS                = jsonencode(each.value.config.join_specs)
    }
  }

  depends_on = [terraform_data.genie_space, databricks_sql_endpoint.warehouse]
}

# ── New spaces: apply ACLs ────────────────────────────────────────────────────

resource "null_resource" "genie_space_acls_created" {
  # Skip ACL setup when no groups are configured (e.g. self-service genie-only mode
  # where groups are managed by the governance team in a separate environment).
  for_each = {
    for k, v in local.new_spaces : k => v
    if contains(local.genie_space_acl_keys, k)
  }

  triggers = {
    groups          = local.genie_space_acl_groups[each.key]
    space_create_id = terraform_data.genie_space[each.key].id
  }

  provisioner "local-exec" {
    command = "${var.genie_script_path} set-acls"

    environment = {
      DATABRICKS_HOST          = var.databricks_workspace_host
      DATABRICKS_CLIENT_ID     = var.databricks_client_id
      DATABRICKS_CLIENT_SECRET = var.databricks_client_secret
      GENIE_ID_FILE            = "${var.genie_id_file_prefix}_${each.key}"
      GENIE_GROUPS_CSV         = local.genie_space_acl_groups[each.key]
      GENIE_ALLOW_EMPTY_ACL    = "1"
    }
  }

  # As for genie_space_acls. Destroyed before the agent it belongs to, so the
  # ID file still names it. The root sets genie_id_file_prefix to
  # $LAYER_ENV_DIR/.genie_space_id.
  provisioner "local-exec" {
    when    = destroy
    command = "bash ../../scripts/genie_space.sh revoke-acls"

    environment = {
      GENIE_ID_BASENAME       = ".genie_space_id_${each.key}"
      GENIE_REVOKE_GROUPS_CSV = self.triggers.groups
    }
  }

  depends_on = [terraform_data.genie_space]
}

# Same removal-only sync for spaces reached through the create path. If create
# title-adopts an existing agent, this clears and reports its hand-added direct
# ACL without ever granting the groups withheld above. A genuinely new agent
# simply has an already-empty direct ACL.
resource "null_resource" "genie_space_acls_created_removal_only" {
  for_each = {
    for k, v in local.new_spaces : k => v
    if lookup(local.genie_space_groups, k, "") != ""
    && lookup(local.genie_space_acl_groups, k, "") == ""
  }

  triggers = {
    groups          = ""
    withheld        = join(",", local.genie_space_can_run_withheld[each.key])
    space_create_id = terraform_data.genie_space[each.key].id
  }

  provisioner "local-exec" {
    command = "${var.genie_script_path} set-acls"

    environment = {
      DATABRICKS_HOST          = var.databricks_workspace_host
      DATABRICKS_CLIENT_ID     = var.databricks_client_id
      DATABRICKS_CLIENT_SECRET = var.databricks_client_secret
      GENIE_ID_FILE            = "${var.genie_id_file_prefix}_${each.key}"
      GENIE_GROUPS_CSV         = ""
      GENIE_ALLOW_EMPTY_ACL    = "1"
    }
  }

  depends_on = [terraform_data.genie_space]
}
