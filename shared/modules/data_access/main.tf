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
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }
}

data "databricks_group" "existing" {
  for_each = var.groups

  provider     = databricks.account
  display_name = each.key
}

locals {
  effective_warehouse_id = (
    var.sql_warehouse_id != ""
    ? var.sql_warehouse_id
    : databricks_sql_endpoint.warehouse[0].id
  )

  _grouped_tag_assignments = {
    for ta in var.tag_assignments :
    "${ta.entity_type}|${ta.entity_name}|${ta.tag_key}|${ta.tag_value}" => ta...
  }

  tag_assignment_map = {
    for key, grouped in local._grouped_tag_assignments :
    key => grouped[0]
  }

  fgac_policy_map = { for p in var.fgac_policies : p.name => p }

  _ta_catalogs = [
    for ta in var.tag_assignments :
    split(".", ta.entity_name)[0]
  ]

  _fgac_catalogs = [
    for p in var.fgac_policies :
    p.catalog
  ]

  _uc_catalogs = [
    for t in var.uc_tables :
    split(".", t)[0]
  ]

  all_catalogs = distinct(concat(
    local._ta_catalogs,
    local._fgac_catalogs,
    local._uc_catalogs,
  ))
}

resource "databricks_entity_tag_assignment" "assignments" {
  for_each = local.tag_assignment_map

  provider    = databricks.workspace
  entity_type = each.value.entity_type
  entity_name = each.value.entity_name
  tag_key     = each.value.tag_key
  tag_value   = each.value.tag_value
}

resource "time_sleep" "wait_for_tag_propagation" {
  depends_on      = [databricks_entity_tag_assignment.assignments]
  create_duration = "30s"
}

resource "databricks_grant" "terraform_sp_manage_catalog" {
  for_each = toset(local.all_catalogs)

  provider   = databricks.workspace
  catalog    = each.value
  principal  = var.databricks_client_id
  privileges = ["USE_CATALOG", "USE_SCHEMA", "EXECUTE", "MANAGE", "CREATE_FUNCTION"]
}

resource "databricks_grant" "catalog_access" {
  for_each = {
    for pair in setproduct(local.all_catalogs, keys(var.groups)) :
    "${pair[0]}|${pair[1]}" => { catalog = pair[0], group = pair[1] }
  }

  provider   = databricks.workspace
  catalog    = each.value.catalog
  principal  = each.value.group
  privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
}

resource "databricks_sql_endpoint" "warehouse" {
  count = var.sql_warehouse_id != "" ? 0 : 1

  provider         = databricks.workspace
  name             = var.warehouse_name
  cluster_size     = var.warehouse_cluster_size
  max_num_clusters = 1

  enable_serverless_compute = true
  warehouse_type            = "PRO"

  auto_stop_mins = 15
}

resource "null_resource" "deploy_masking_functions" {
  # NOTE: Only non-sensitive values belong in triggers (stored in tfstate plaintext).
  # Credentials are passed via environment blocks using var.* references.
  # The destroy provisioner uses self.triggers for host/client_id (non-secret)
  # and nonsensitive() for the secret to avoid tfstate exposure.
  triggers = {
    sql_hash     = filemd5(var.masking_sql_file)
    sql_file     = var.masking_sql_file
    script       = var.deploy_masking_script
    warehouse_id = local.effective_warehouse_id
    host         = var.databricks_workspace_host
    client_id    = var.databricks_client_id
  }

  provisioner "local-exec" {
    command = "python3 ${self.triggers.script} --sql-file ${self.triggers.sql_file} --warehouse-id ${self.triggers.warehouse_id}"

    environment = {
      DATABRICKS_HOST          = self.triggers.host
      DATABRICKS_CLIENT_ID     = self.triggers.client_id
      DATABRICKS_CLIENT_SECRET = var.databricks_client_secret
    }
  }

  provisioner "local-exec" {
    when    = destroy
    command = "python3 ${self.triggers.script} --sql-file ${self.triggers.sql_file} --warehouse-id ${self.triggers.warehouse_id} --drop"

    environment = {
      DATABRICKS_HOST          = self.triggers.host
      DATABRICKS_CLIENT_ID     = self.triggers.client_id
      DATABRICKS_CLIENT_SECRET = nonsensitive(var.databricks_client_secret)
    }
  }

  depends_on = [
    time_sleep.wait_for_tag_propagation,
    # Keep the SP's catalog privileges in place until function drops finish.
    databricks_grant.terraform_sp_manage_catalog,
    databricks_sql_endpoint.warehouse,
  ]
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

  match_columns = each.value.policy_type == "POLICY_TYPE_COLUMN_MASK" ? [{
    condition = each.value.match_condition
    alias     = each.value.match_alias
  }] : null

  column_mask = each.value.policy_type == "POLICY_TYPE_COLUMN_MASK" ? {
    function_name = "${each.value.function_catalog}.${each.value.function_schema}.${each.value.function_name}"
    on_column     = each.value.match_alias
    using         = []
  } : null

  row_filter = each.value.policy_type == "POLICY_TYPE_ROW_FILTER" ? {
    function_name = "${each.value.function_catalog}.${each.value.function_schema}.${each.value.function_name}"
    using         = []
  } : null

  depends_on = [
    time_sleep.wait_for_tag_propagation,
    databricks_grant.catalog_access,
    databricks_grant.terraform_sp_manage_catalog,
    null_resource.deploy_masking_functions,
  ]
}
