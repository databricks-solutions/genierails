mock_provider "databricks" {
  alias = "account"
}
mock_provider "databricks" {
  alias = "workspace"
}
mock_provider "null" {}

variables {
  databricks_account_id      = "account"
  databricks_client_id       = "client"
  databricks_client_secret   = "secret"
  databricks_workspace_id    = "123"
  databricks_workspace_host  = "https://example.invalid"
  sql_warehouse_id           = "warehouse"
  groups                     = { analysts = {}, auditors = {} }
  genie_only                 = true
  genie_exposure_blocker     = "the coverage check is failing"
  genie_space_missing_grants = { sales = [] }
  genie_id_file_prefix       = "tests/.tmp/handoff/.genie_space_id"
  genie_script_path          = "true"
}

run "create_path" {
  command = apply
  variables {
    genie_space_can_run_widening = { sales = [] }
    genie_spaces = {
      sales = { name = "Sales", genie_space_id = "", sql_warehouse_id = "warehouse", uc_tables = ["cat.sch.customers"], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts"] } }
    }
  }
}

run "move_to_id_path_while_gate_fails" {
  command = apply
  variables {
    genie_space_can_run_widening = { sales = ["auditors"] }
    genie_space_acl_created_handoffs = {
      sales = { space_create_id = run.create_path.genie_space_create_bindings["sales"], groups = "analysts" }
    }
    genie_spaces = {
      sales = { name = "Sales", genie_space_id = "space-1", sql_warehouse_id = "warehouse", uc_tables = ["cat.sch.customers"], config = { title = "", description = "", sample_questions = [], instructions = "", benchmarks = [], sql_filters = [], sql_expressions = [], sql_measures = [], join_specs = [], acl_groups = ["analysts", "auditors"] } }
    }
  }
  assert {
    condition     = output.genie_space_acls_created_groups["sales"] == "analysts" && output.genie_space_can_run_withheld["sales"] == tolist(["auditors"])
    error_message = "the create-path resource must keep the recorded group and withhold only the new group"
  }
  assert {
    condition     = length(output.genie_space_acls_created_groups) > 0 && !fileexists("handoff-revoke.log")
    error_message = "moving the same agent from the create path to the ID path must not run revoke-acls"
  }
}
