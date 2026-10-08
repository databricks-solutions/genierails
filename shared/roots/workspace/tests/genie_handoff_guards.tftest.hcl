# Every create-to-ID handoff guard fails closed. Each malformed state below
# must receive no credit for its recorded ACL groups.
mock_provider "databricks" {
  alias = "account"
}
mock_provider "databricks" {
  alias = "workspace"
}
mock_provider "null" {}

override_data {
  target = module.workspace.data.databricks_group.existing
  values = { id = 123 }
}

variables {
  env_dir                   = "tests/.tmp/handoff-guards"
  databricks_account_id     = "account"
  databricks_client_id      = "service-principal"
  databricks_client_secret  = "secret"
  databricks_workspace_id   = "123"
  databricks_workspace_host = "https://example.invalid"
  sql_warehouse_id          = "warehouse"
  groups                    = { analysts = {}, auditors = {} }
  genie_spaces              = [{ name = "Sales", genie_space_id = "space-1", uc_tables = ["cat.sch.customers"] }]
  genie_space_configs       = { Sales = { acl_groups = ["analysts", "auditors"] } }
}

run "id_contents_mismatch_state" {
  module { source = "../data_access/tests/file_writer" }
  variables {
    files = {
      "tests/.tmp/handoff-guards/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { status = "fail", fingerprint = "applied", max_age = "6h", table_grant_count = 2 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"] } } })
      "tests/.tmp/handoff-guards/data_access/.coverage_gate.json" = jsonencode({ status = "fail", fingerprint = "applied", refreshed_at = "@NOW@" })
      "tests/.tmp/handoff-guards/.genie_space_id_sales"           = "space-other\n"
      "tests/.tmp/handoff-guards/terraform.tfstate"               = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "terraform_data", name = "genie_space", instances = [{ index_key = "sales", attributes = { id = "created-1", input = { value = { id_file = "tests/.tmp/handoff-guards/.genie_space_id_sales" }, type = ["object", { id_file = "string" }] }, triggers_replace = { value = { host = "https://example.invalid" }, type = ["object", { host = "string" }] } } }] }, { module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls_created", instances = [{ index_key = "sales", attributes = { id = "2", triggers = { space_create_id = "created-1", groups = "analysts" } } }] }] })
    }
  }
}

run "id_contents_mismatch_gets_no_handoff" {
  command         = plan
  expect_failures = [check.genie_can_run_withheld]
  assert {
    condition     = toset(output.genie_space_can_run_widening["sales"]) == toset(["analysts", "auditors"]) && !contains(keys(output.genie_space_acl_created_handoffs), "sales")
    error_message = "an ID file naming another agent must not authorize a handoff"
  }
}

run "missing_id_file_state" {
  module { source = "../data_access/tests/file_writer" }
  variables {
    files = {
      "tests/.tmp/handoff-guards/.genie_space_id_sales" = null
      "tests/.tmp/handoff-guards/terraform.tfstate"     = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "terraform_data", name = "genie_space", instances = [{ index_key = "sales", attributes = { id = "created-1", input = { value = { id_file = "tests/.tmp/handoff-guards/.genie_space_id_sales" }, type = ["object", { id_file = "string" }] }, triggers_replace = { value = { host = "https://example.invalid" }, type = ["object", { host = "string" }] } } }] }, { module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls_created", instances = [{ index_key = "sales", attributes = { id = "2", triggers = { space_create_id = "created-1", groups = "analysts" } } }] }] })
    }
  }
}

run "missing_id_file_gets_no_handoff" {
  command         = plan
  expect_failures = [check.genie_can_run_withheld]
  assert {
    condition     = toset(output.genie_space_can_run_widening["sales"]) == toset(["analysts", "auditors"]) && !contains(keys(output.genie_space_acl_created_handoffs), "sales")
    error_message = "a missing ID file must not authorize a handoff"
  }
}

run "different_host_state" {
  module { source = "../data_access/tests/file_writer" }
  variables {
    files = {
      "tests/.tmp/handoff-guards/.genie_space_id_sales" = "space-1\n"
      "tests/.tmp/handoff-guards/terraform.tfstate"     = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "terraform_data", name = "genie_space", instances = [{ index_key = "sales", attributes = { id = "created-1", input = { value = { id_file = "tests/.tmp/handoff-guards/.genie_space_id_sales" }, type = ["object", { id_file = "string" }] }, triggers_replace = { value = { host = "https://other.invalid" }, type = ["object", { host = "string" }] } } }] }, { module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls_created", instances = [{ index_key = "sales", attributes = { id = "2", triggers = { space_create_id = "created-1", groups = "analysts" } } }] }] })
    }
  }
}

run "different_host_gets_no_handoff" {
  command         = plan
  expect_failures = [check.genie_can_run_withheld]
  assert {
    condition     = toset(output.genie_space_can_run_widening["sales"]) == toset(["analysts", "auditors"]) && !contains(keys(output.genie_space_acl_created_handoffs), "sales")
    error_message = "create state from another host must not authorize a handoff"
  }
}

run "create_id_mismatch_state" {
  module { source = "../data_access/tests/file_writer" }
  variables {
    files = {
      "tests/.tmp/handoff-guards/.genie_space_id_sales" = "space-1\n"
      "tests/.tmp/handoff-guards/terraform.tfstate"     = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "terraform_data", name = "genie_space", instances = [{ index_key = "sales", attributes = { id = "created-1", input = { value = { id_file = "tests/.tmp/handoff-guards/.genie_space_id_sales" }, type = ["object", { id_file = "string" }] }, triggers_replace = { value = { host = "https://example.invalid" }, type = ["object", { host = "string" }] } } }] }, { module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls_created", instances = [{ index_key = "sales", attributes = { id = "2", triggers = { space_create_id = "created-0", groups = "analysts" } } }] }] })
    }
  }
}

run "create_id_mismatch_gets_no_handoff" {
  command         = plan
  expect_failures = [check.genie_can_run_withheld]
  assert {
    condition     = toset(output.genie_space_can_run_widening["sales"]) == toset(["analysts", "auditors"]) && !contains(keys(output.genie_space_acl_created_handoffs), "sales")
    error_message = "ACL state for another create resource must not authorize a handoff"
  }
}

run "different_recorded_id_path_state" {
  module { source = "../data_access/tests/file_writer" }
  variables {
    files = {
      "tests/.tmp/handoff-guards/.genie_space_id_sales" = "space-1\n"
      "tests/.tmp/handoff-guards/terraform.tfstate"     = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "terraform_data", name = "genie_space", instances = [{ index_key = "sales", attributes = { id = "created-1", input = { value = { id_file = "tests/.tmp/another-deployment/.genie_space_id_sales" }, type = ["object", { id_file = "string" }] }, triggers_replace = { value = { host = "https://example.invalid" }, type = ["object", { host = "string" }] } } }] }, { module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls_created", instances = [{ index_key = "sales", attributes = { id = "2", triggers = { space_create_id = "created-1", groups = "analysts" } } }] }] })
    }
  }
}

run "different_recorded_id_path_gets_no_handoff" {
  command         = plan
  expect_failures = [check.genie_can_run_withheld]
  assert {
    condition     = toset(output.genie_space_can_run_widening["sales"]) == toset(["analysts", "auditors"]) && !contains(keys(output.genie_space_acl_created_handoffs), "sales")
    error_message = "create state from another deployment path must not authorize a handoff"
  }
}

run "tainted_acl_state" {
  module { source = "../data_access/tests/file_writer" }
  variables {
    files = {
      "tests/.tmp/handoff-guards/.genie_space_id_sales" = "space-1\n"
      "tests/.tmp/handoff-guards/terraform.tfstate"     = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "terraform_data", name = "genie_space", instances = [{ index_key = "sales", attributes = { id = "created-1", input = { value = { id_file = "tests/.tmp/handoff-guards/.genie_space_id_sales" }, type = ["object", { id_file = "string" }] }, triggers_replace = { value = { host = "https://example.invalid" }, type = ["object", { host = "string" }] } } }] }, { module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls_created", instances = [{ index_key = "sales", status = "tainted", attributes = { id = "2", triggers = { space_create_id = "created-1", groups = "analysts" } } }] }] })
    }
  }
}

run "tainted_acl_gets_no_handoff" {
  command         = plan
  expect_failures = [check.genie_can_run_withheld]
  assert {
    condition     = toset(output.genie_space_can_run_widening["sales"]) == toset(["analysts", "auditors"]) && !contains(keys(output.genie_space_acl_created_handoffs), "sales")
    error_message = "a tainted create-path ACL must not authorize a handoff"
  }
}

run "deposed_acl_state" {
  module { source = "../data_access/tests/file_writer" }
  variables {
    files = {
      "tests/.tmp/handoff-guards/.genie_space_id_sales" = "space-1\n"
      "tests/.tmp/handoff-guards/terraform.tfstate"     = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "terraform_data", name = "genie_space", instances = [{ index_key = "sales", attributes = { id = "created-1", input = { value = { id_file = "tests/.tmp/handoff-guards/.genie_space_id_sales" }, type = ["object", { id_file = "string" }] }, triggers_replace = { value = { host = "https://example.invalid" }, type = ["object", { host = "string" }] } } }] }, { module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls_created", instances = [{ index_key = "sales", deposed = "deadbeef", attributes = { id = "2", triggers = { space_create_id = "created-1", groups = "analysts" } } }] }] })
    }
  }
}

run "deposed_acl_gets_no_handoff" {
  command         = plan
  expect_failures = [check.genie_can_run_withheld]
  assert {
    condition     = toset(output.genie_space_can_run_widening["sales"]) == toset(["analysts", "auditors"]) && !contains(keys(output.genie_space_acl_created_handoffs), "sales")
    error_message = "a deposed create-path ACL must not authorize a handoff"
  }
}
