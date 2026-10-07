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
  count = var.sql_warehouse_id != "" ? 0 : 1

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
    if contains(keys(local.genie_space_groups), k)
  }

  triggers = {
    space_id = each.value.genie_space_id
    groups   = local.genie_space_groups[each.key]
  }

  provisioner "local-exec" {
    command = var.genie_script_path == "" ? "true" : "${var.genie_script_path} set-acls"

    environment = {
      DATABRICKS_HOST          = var.databricks_workspace_host
      DATABRICKS_CLIENT_ID     = var.databricks_client_id
      DATABRICKS_CLIENT_SECRET = var.databricks_client_secret
      GENIE_SPACE_OBJECT_ID    = each.value.genie_space_id
      GENIE_GROUPS_CSV         = local.genie_space_groups[each.key]
      GENIE_ALLOW_EMPTY_ACL    = "1"
    }
  }

  depends_on = [databricks_mws_permission_assignment.group_assignments]

  # Opening or widening CAN_RUN requires the data_access layer's gated grants
  # for this space's tables and groups (genie_exposure_blocker and
  # genie_space_missing_grants in the root). Keeping, shrinking or clearing
  # the ACL the last apply left in place (genie_space_can_run_widening empty)
  # never needs them, so revocation always plans and an unchanged ACL never
  # errors after the gate expires.
  lifecycle {
    precondition {
      condition     = local.genie_space_groups[each.key] == "" || length(try(var.genie_space_can_run_widening[each.key], ["unknown"])) == 0 || (var.genie_exposure_blocker == "" && length(try(var.genie_space_missing_grants[each.key], ["unknown"])) == 0)
      error_message = "Opening Genie CAN_RUN for ${each.value.name} to ${join(", ", try(var.genie_space_can_run_widening[each.key], ["unknown"]))} is blocked: ${var.genie_exposure_blocker != "" ? var.genie_exposure_blocker : "the data_access state lacks the SELECT grants its CAN_RUN groups need (${join(", ", try(var.genie_space_missing_grants[each.key], ["unknown"]))})"}. Apply governance first through make (make apply, make release or make apply-governance), which runs the coverage check and applies data_access before Genie ACLs."
    }
  }
}

# ── Existing spaces: apply config (when genie_space_configs is defined) ───────

resource "null_resource" "genie_space_config_existing" {
  for_each = local.existing_spaces_with_config

  triggers = {
    space_id        = each.value.genie_space_id
    description     = each.value.config.description
    questions       = jsonencode(each.value.config.sample_questions)
    instructions    = each.value.config.instructions
    benchmarks      = jsonencode(each.value.config.benchmarks)
    sql_filters     = jsonencode(each.value.config.sql_filters)
    sql_measures    = jsonencode(each.value.config.sql_measures)
    sql_expressions = jsonencode(each.value.config.sql_expressions)
    join_specs      = jsonencode(each.value.config.join_specs)
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
      GENIE_TITLE            = each.value.config.title != "" ? each.value.config.title : each.value.name
      GENIE_DESCRIPTION      = each.value.config.description
      GENIE_SAMPLE_QUESTIONS = jsonencode(each.value.config.sample_questions)
      GENIE_INSTRUCTIONS     = each.value.config.instructions
      GENIE_BENCHMARKS       = jsonencode(each.value.config.benchmarks)
      GENIE_SQL_FILTERS      = jsonencode(each.value.config.sql_filters)
      GENIE_SQL_EXPRESSIONS  = jsonencode(each.value.config.sql_expressions)
      GENIE_SQL_MEASURES     = jsonencode(each.value.config.sql_measures)
      GENIE_JOIN_SPECS       = jsonencode(each.value.config.join_specs)
    }
  }

  depends_on = [terraform_data.genie_space]
}

# ── New spaces: apply ACLs ────────────────────────────────────────────────────

resource "null_resource" "genie_space_acls_created" {
  # Skip ACL setup when no groups are configured (e.g. self-service genie-only mode
  # where groups are managed by the governance team in a separate environment).
  for_each = {
    for k, v in local.new_spaces : k => v
    if contains(keys(local.genie_space_groups), k)
  }

  triggers = {
    groups          = local.genie_space_groups[each.key]
    space_create_id = terraform_data.genie_space[each.key].id
  }

  provisioner "local-exec" {
    command = "${var.genie_script_path} set-acls"

    environment = {
      DATABRICKS_HOST          = var.databricks_workspace_host
      DATABRICKS_CLIENT_ID     = var.databricks_client_id
      DATABRICKS_CLIENT_SECRET = var.databricks_client_secret
      GENIE_ID_FILE            = "${var.genie_id_file_prefix}_${each.key}"
      GENIE_GROUPS_CSV         = local.genie_space_groups[each.key]
      GENIE_ALLOW_EMPTY_ACL    = "1"
    }
  }

  depends_on = [terraform_data.genie_space]

  # Opening or widening CAN_RUN requires the data_access layer's gated grants
  # for this space's tables and groups (genie_exposure_blocker and
  # genie_space_missing_grants in the root). Keeping, shrinking or clearing
  # the ACL the last apply left in place (genie_space_can_run_widening empty)
  # never needs them, so revocation always plans and an unchanged ACL never
  # errors after the gate expires.
  lifecycle {
    precondition {
      condition     = local.genie_space_groups[each.key] == "" || length(try(var.genie_space_can_run_widening[each.key], ["unknown"])) == 0 || (var.genie_exposure_blocker == "" && length(try(var.genie_space_missing_grants[each.key], ["unknown"])) == 0)
      error_message = "Opening Genie CAN_RUN for ${each.value.name} to ${join(", ", try(var.genie_space_can_run_widening[each.key], ["unknown"]))} is blocked: ${var.genie_exposure_blocker != "" ? var.genie_exposure_blocker : "the data_access state lacks the SELECT grants its CAN_RUN groups need (${join(", ", try(var.genie_space_missing_grants[each.key], ["unknown"]))})"}. Apply governance first through make (make apply, make release or make apply-governance), which runs the coverage check and applies data_access before Genie ACLs."
    }
  }
}
