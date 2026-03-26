terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      version               = "~> 1.91.0"
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

  # Per-space group list: use acl_groups if set, otherwise all groups (backward compat)
  genie_space_groups = {
    for key, space in var.genie_spaces : key => (
      length(try(space.config.acl_groups, [])) > 0
        ? join(",", space.config.acl_groups)
        : join(",", keys(var.groups))
    )
  }

  # Spaces that already have an ID — apply ACLs, and config if defined.
  existing_spaces = { for k, v in var.genie_spaces : k => v if v.genie_space_id != "" }

  # Existing spaces that have non-trivial config — also run update-config.
  existing_spaces_with_config = {
    for k, v in local.existing_spaces : k => v
    if (
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
    if lookup(local.genie_space_groups, k, "") != ""
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
    }
  }

  depends_on = [databricks_mws_permission_assignment.group_assignments]
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

resource "null_resource" "genie_space_create" {
  for_each = local.new_spaces

  triggers = {
    id_file       = "${var.genie_id_file_prefix}_${each.key}"
    script        = var.genie_script_path
    host          = var.databricks_workspace_host
    client_id     = var.databricks_client_id
    client_secret = var.databricks_client_secret
  }

  provisioner "local-exec" {
    command = "${self.triggers.script} create"

    environment = {
      DATABRICKS_HOST          = self.triggers.host
      DATABRICKS_CLIENT_ID     = self.triggers.client_id
      DATABRICKS_CLIENT_SECRET = self.triggers.client_secret
      GENIE_ID_FILE            = self.triggers.id_file
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
    when    = destroy
    command = "${self.triggers.script} trash"

    environment = {
      DATABRICKS_HOST          = self.triggers.host
      DATABRICKS_CLIENT_ID     = self.triggers.client_id
      DATABRICKS_CLIENT_SECRET = self.triggers.client_secret
      GENIE_ID_FILE            = self.triggers.id_file
    }
  }

  depends_on = [
    databricks_mws_permission_assignment.group_assignments,
    databricks_sql_endpoint.warehouse,
  ]
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

  depends_on = [null_resource.genie_space_create]
}

# ── New spaces: apply ACLs ────────────────────────────────────────────────────

resource "null_resource" "genie_space_acls_created" {
  # Skip ACL setup when no groups are configured (e.g. self-service genie-only mode
  # where groups are managed by the governance team in a separate environment).
  for_each = {
    for k, v in local.new_spaces : k => v
    if lookup(local.genie_space_groups, k, "") != ""
  }

  triggers = {
    groups = local.genie_space_groups[each.key]
  }

  provisioner "local-exec" {
    command = "${var.genie_script_path} set-acls"

    environment = {
      DATABRICKS_HOST          = var.databricks_workspace_host
      DATABRICKS_CLIENT_ID     = var.databricks_client_id
      DATABRICKS_CLIENT_SECRET = var.databricks_client_secret
      GENIE_ID_FILE            = "${var.genie_id_file_prefix}_${each.key}"
      GENIE_GROUPS_CSV         = local.genie_space_groups[each.key]
    }
  }

  depends_on = [null_resource.genie_space_create]
}
