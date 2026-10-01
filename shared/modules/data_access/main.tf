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
  }
}

locals {
  effective_uc_tables = distinct(concat(var.uc_tables, var.discovered_uc_tables))
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

  # Built-in principals such as "account users" are intentionally absent from
  # the managed groups map, but still need catalog/schema/table grants. Derive
  # the access set from both managed groups and policy targets.
  access_principals = distinct(concat(
    keys(var.groups),
    flatten([for p in var.fgac_policies : p.to_principals]),
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
      || length(local.scoped_table_access_principals[table]) == 0
      ? local.access_principals
      : local.scoped_table_access_principals[table]
    )
  }

  table_access_pairs = flatten([
    for table, principals in local.table_access_principals : [
      for principal in principals : { table = table, principal = principal }
    ]
  ])

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
}

# Data Classification is opt-in because deleting this resource disables scans
# for the catalog. When enabled, scope scans to only the schemas represented by
# the governed UC table footprint. Auto-tagging is a separate opt-in so operators
# can review detections before allowing class.* tags to land on columns.
resource "databricks_data_classification_catalog_config" "classification" {
  for_each = var.enable_classification ? local.classification_catalog_schemas : {}

  provider = databricks.workspace
  parent   = "catalogs/${each.key}"

  included_schemas = contains(var.classification_all_schemas, each.key) ? null : {
    names = each.value
  }

  auto_tag_configs = var.enable_auto_tagging ? [
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

resource "time_sleep" "wait_for_tag_propagation" {
  depends_on      = [databricks_entity_tag_assignment.assignments]
  create_duration = "30s"
}

resource "databricks_grant" "terraform_sp_manage_catalog" {
  # User/CLI-profile authentication has no service-principal client ID and the
  # active user already carries their own privileges. Avoid an invalid grant to
  # the empty-string principal in that supported path.
  for_each = var.databricks_client_id != "" ? toset(local.all_catalogs) : toset([])

  provider   = databricks.workspace
  catalog    = each.value
  principal  = var.databricks_client_id
  privileges = ["USE_CATALOG", "USE_SCHEMA", "EXECUTE", "MANAGE", "CREATE_FUNCTION", "APPLY_TAG"]
}

resource "databricks_grant" "catalog_access" {
  for_each = {
    for pair in setproduct(local.all_catalogs, local.access_principals) :
    "${pair[0]}|${pair[1]}" => { catalog = pair[0], group = pair[1] }
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
    for pair in setproduct(local.uc_schemas, local.access_principals) :
    "${pair[0]}|${pair[1]}" => { schema = pair[0], group = pair[1] }
  }

  provider   = databricks.workspace
  schema     = each.value.schema
  principal  = each.value.group
  privileges = ["USE_SCHEMA"]
}

resource "databricks_grant" "table_access" {
  for_each = var.business_access_enabled ? {
    for pair in local.table_access_pairs :
    "${pair.table}|${pair.principal}" => { table = pair.table, group = pair.principal }
  } : {}

  provider   = databricks.workspace
  table      = each.value.table
  principal  = each.value.group
  privileges = ["SELECT"]
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
  triggers = {
    sql_hash      = filemd5(var.masking_sql_file)
    sql_file      = var.masking_sql_file
    script        = var.deploy_masking_script
    warehouse_id  = local.effective_warehouse_id
    host          = var.databricks_workspace_host
    client_id     = var.databricks_client_id
    client_secret = var.databricks_client_secret
  }

  provisioner "local-exec" {
    command = "python3 ${self.triggers.script} --sql-file ${self.triggers.sql_file} --warehouse-id ${self.triggers.warehouse_id}"

    environment = {
      DATABRICKS_HOST          = self.triggers.host
      DATABRICKS_CLIENT_ID     = self.triggers.client_id
      DATABRICKS_CLIENT_SECRET = self.triggers.client_secret
    }
  }

  provisioner "local-exec" {
    when    = destroy
    command = "python3 ${self.triggers.script} --sql-file ${self.triggers.sql_file} --warehouse-id ${self.triggers.warehouse_id} --drop"

    environment = {
      DATABRICKS_HOST          = self.triggers.host
      DATABRICKS_CLIENT_ID     = self.triggers.client_id
      DATABRICKS_CLIENT_SECRET = self.triggers.client_secret
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
    using         = []
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
    databricks_grant.table_access,
    databricks_grant.terraform_sp_manage_catalog,
    null_resource.deploy_masking_functions,
  ]
}
