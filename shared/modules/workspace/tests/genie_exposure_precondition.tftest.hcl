# While the root reports a genie_exposure_blocker, the module refuses every
# non-empty Genie CAN_RUN ACL (new or existing space) but still applies an
# empty ACL, which only clears access.

mock_provider "databricks" {
  alias = "account"
}
mock_provider "databricks" {
  alias = "workspace"
}
mock_provider "null" {}

variables {
  databricks_account_id     = "account"
  databricks_client_id      = "service-principal"
  databricks_client_secret  = "secret"
  databricks_workspace_id   = "123"
  databricks_workspace_host = "https://example.invalid"
  sql_warehouse_id          = "warehouse"
  business_access_enabled   = true
  groups                    = { analysts = {} }
}

run "blocked_exposure_refuses_can_run_on_existing_space" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix   = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path      = "true"
    genie_exposure_blocker = "the data_access layer has no readable state"
    genie_spaces           = { sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts"] } } }
  }
  expect_failures = [null_resource.genie_space_acls]
}

run "blocked_exposure_refuses_can_run_on_new_space" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix   = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path      = "true"
    genie_exposure_blocker = "the data_access layer has no readable state"
    genie_spaces           = { sales = { name = "Sales", genie_space_id = "", sql_warehouse_id = "warehouse", uc_tables = ["cat.sch.customers"], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts"] } } }
  }
  expect_failures = [null_resource.genie_space_acls_created]
}

run "blocked_exposure_still_clears_can_run" {
  command = plan
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
    null                 = null
  }
  override_data {
    target = data.databricks_group.existing
    values = {
      id = 123
    }
  }
  variables {
    genie_id_file_prefix   = "tests/.tmp/exposure/.genie_space_id"
    genie_script_path      = "true"
    genie_exposure_blocker = "the data_access layer has no readable state"
    genie_spaces           = { sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = [], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = [] } } }
  }
  assert {
    condition     = output.genie_space_acls_applied
    error_message = "an empty ACL must still be applied to clear CAN_RUN while exposure is blocked"
  }
}
