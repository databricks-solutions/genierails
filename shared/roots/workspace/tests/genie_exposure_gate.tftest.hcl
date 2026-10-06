# Genie CAN_RUN opens only after the data_access layer was applied with
# business access open and a passing coverage gate, while the gate result on
# disk is still the one that apply used. The root computes why exposure is
# blocked (genie_exposure_blocker) from the data_access state and gate file;
# modules/workspace/tests checks that the module then refuses every non-empty
# ACL. These runs use an empty ACL so the plan succeeds and the computed
# reason can be asserted.

mock_provider "databricks" {
  alias = "account"
}
mock_provider "databricks" {
  alias = "workspace"
}
mock_provider "null" {}

override_data {
  target = module.workspace.data.databricks_group.existing
  values = {
    id = 123
  }
}

variables {
  env_dir                   = "tests/.tmp/exposure"
  databricks_account_id     = "account"
  databricks_client_id      = "service-principal"
  databricks_client_secret  = "secret"
  databricks_workspace_id   = "123"
  databricks_workspace_host = "https://example.invalid"
  sql_warehouse_id          = "warehouse"
  business_access_enabled   = true
  groups                    = { analysts = {} }
  genie_spaces              = [{ name = "Sales", genie_space_id = "space-1", uc_tables = [] }]
  genie_space_configs       = { Sales = { acl_groups = [] } }
}


run "no_data_access_state" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = null
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = null
    }
  }
}

run "missing_state_blocks" {
  command = plan
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "no readable state")
    error_message = "a missing data_access state must block CAN_RUN"
  }
}

run "state_predating_the_gate" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = {} })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
    }
  }
}

run "pre_gate_state_blocks" {
  command = plan
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "predates the coverage gate")
    error_message = "a data_access state without the gate output must block CAN_RUN"
  }
}

run "closed_data_access" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = false, fingerprint = "applied", status = "missing", max_age = "6h", table_grant_count = 0 } } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
    }
  }
}

run "closed_data_access_blocks" {
  command = plan
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "business_access_enabled = false")
    error_message = "a data_access layer applied closed must block CAN_RUN"
  }
}

run "ungated_data_access" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "stale", max_age = "6h", table_grant_count = 0 } } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
    }
  }
}

run "ungated_apply_blocks" {
  command = plan
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "without a passing coverage gate")
    error_message = "an apply without a passing gate must block CAN_RUN"
  }
}

run "gate_result_missing" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 1 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = null
    }
  }
}

run "missing_gate_result_blocks" {
  command = plan
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "no coverage-gate result exists")
    error_message = "a missing gate result must block CAN_RUN"
  }
}

run "gate_failed_since" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 1 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "fail", fingerprint = "applied", refreshed_at = "@NOW@" })
    }
  }
}

run "failed_gate_blocks" {
  command = plan
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "FAILED")
    error_message = "a failed gate must block CAN_RUN"
  }
}

run "config_moved_on" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 1 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "newer", refreshed_at = "@NOW@" })
    }
  }
}

run "unapplied_config_blocks" {
  command = plan
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "changed after its last gated apply")
    error_message = "a gate for config data_access hasn't applied must block CAN_RUN"
  }
}

run "data_access_ready" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 1 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
    }
  }
}

run "ready_data_access_allows_can_run" {
  command = plan
  variables {
    genie_space_configs = { Sales = { acl_groups = ["analysts"] } }
  }
  assert {
    condition     = output.genie_exposure_blocker == ""
    error_message = "a current gated data_access apply must allow CAN_RUN"
  }
  assert {
    condition     = output.genie_space_acls_groups["sales"] == "analysts" && output.genie_space_acls_applied
    error_message = "with data_access ready, the CAN_RUN ACL must be planned"
  }
}

run "matching_pass_but_no_grants" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 0 } }, table_grant_resource_keys = { value = [] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
    }
  }
}

run "zero_grants_block_can_run_but_not_an_empty_acl" {
  command = plan
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "no business table grants")
    error_message = "a gated apply that left zero table grants must block CAN_RUN"
  }
  assert {
    condition     = output.genie_space_acls_groups["sales"] == ""
    error_message = "the explicit empty ACL must still be planned (it only clears access)"
  }
}

run "grants_for_other_groups_and_tables" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 2 } }, table_grant_resource_keys = { value = ["cat.sch.customers|auditors", "cat.sch.orders|analysts"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
    }
  }
}

# The gap runs plan with groups = {} (no ACL resources, so no precondition)
# to read the computed gaps; test_coverage_gate_script.py shows the same
# states refuse a real non-empty ACL end to end.
run "listed_tables_need_every_table_and_group_grant" {
  command = plan
  variables {
    groups              = {}
    genie_spaces        = [{ name = "Sales", genie_space_id = "space-1", uc_tables = ["cat.sch.customers", "cat.sch.orders"] }]
    genie_space_configs = { Sales = { acl_groups = ["analysts"] } }
  }
  assert {
    condition     = output.genie_exposure_blocker == ""
    error_message = "the layer-wide checks pass here; only the per-space grant check applies"
  }
  assert {
    condition     = toset(output.genie_space_missing_grants["sales"]) == toset(["cat.sch.customers|analysts"])
    error_message = "an unrelated grant (other group, other table) must not satisfy the space's CAN_RUN"
  }
}

run "schema_wildcards_need_a_grant_in_that_schema" {
  command = plan
  variables {
    groups              = {}
    genie_spaces        = [{ name = "Sales", genie_space_id = "space-1", uc_tables = ["cat.sch.*", "cat.other.*"] }]
    genie_space_configs = { Sales = { acl_groups = ["analysts"] } }
  }
  assert {
    condition     = toset(output.genie_space_missing_grants["sales"]) == toset(["cat.other.*|analysts"])
    error_message = "a catalog.schema.* space entry needs a grant for the group in that schema"
  }
}

run "id_only_space_needs_a_grant_for_each_group" {
  command = plan
  variables {
    genie_space_configs = { Sales = { acl_groups = ["analysts", "auditors", "viewers"] } }
    groups              = {}
  }
  assert {
    condition     = toset(output.genie_space_missing_grants["sales"]) == toset(["<any table>|viewers"])
    error_message = "a space known only by ID needs at least one grant per CAN_RUN group"
  }
}

# The same freshness rules as data_access (modules/coverage_gate_check),
# with the max age the gated data_access apply recorded in state.
run "pass_from_an_old_refresh" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 1 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "2000-01-01T00:00:00Z" })
    }
  }
}

run "expired_refresh_blocks_can_run" {
  command = plan
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "older than coverage_gate_max_age")
    error_message = "a pass whose live refresh is older than the recorded max age must block CAN_RUN"
  }
  assert {
    condition     = output.genie_space_acls_groups["sales"] == ""
    error_message = "the explicit empty ACL must still be planned while CAN_RUN is blocked"
  }
}

run "pass_from_the_future" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 1 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "2999-01-01T00:00:00Z" })
    }
  }
}

run "future_refresh_blocks_can_run" {
  command = plan
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "no live refresh")
    error_message = "a future refresh time is not a live refresh"
  }
  assert {
    condition     = output.genie_space_acls_groups["sales"] == ""
    error_message = "the explicit empty ACL must still be planned while CAN_RUN is blocked"
  }
}

run "pass_without_a_refresh" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 1 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied" })
    }
  }
}

run "unrefreshed_pass_blocks_can_run" {
  command = plan
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "no live refresh")
    error_message = "a pass with no refresh time must block CAN_RUN"
  }
  assert {
    condition     = output.genie_space_acls_groups["sales"] == ""
    error_message = "the explicit empty ACL must still be planned while CAN_RUN is blocked"
  }
}

run "state_with_a_raised_max_age" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "876000h", table_grant_count = 1 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "2000-01-01T00:00:00Z" })
    }
  }
}

run "recorded_max_age_above_the_ceiling_blocks_can_run" {
  command = plan
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "at most 24h")
    error_message = "a recorded max age above the ceiling must not be honoured"
  }
  assert {
    condition     = output.genie_space_acls_groups["sales"] == ""
    error_message = "the explicit empty ACL must still be planned while CAN_RUN is blocked"
  }
}

run "state_predating_the_max_age" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", table_grant_count = 1 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
    }
  }
}

run "state_without_a_recorded_max_age_blocks_can_run" {
  command = plan
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "predates the coverage-gate max age")
    error_message = "a data_access state without a recorded max age must block CAN_RUN"
  }
  assert {
    condition     = output.genie_space_acls_groups["sales"] == ""
    error_message = "the explicit empty ACL must still be planned while CAN_RUN is blocked"
  }
}

# Only CAN_RUN beyond what the last apply left in place needs the gate. The
# root reads that from its own state (envs/<env>/terraform.tfstate), in the
# shape Terraform writes it; these runs check the reading and that an
# unchanged non-empty ACL plans after the gate expires.
run "existing_space_acl_on_record" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 2 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
      "tests/.tmp/exposure/terraform.tfstate"               = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls", instances = [{ index_key = "sales", attributes = { id = "1", triggers = { space_id = "space-1", groups = "analysts" } } }] }] })
    }
  }
}

run "unchanged_acl_adds_nothing" {
  command = plan
  variables {
    genie_spaces        = [{ name = "Sales", genie_space_id = "space-1", uc_tables = [] }]
    groups              = { analysts = {}, auditors = {} }
    genie_space_configs = { Sales = { acl_groups = ["analysts"] } }
  }
  assert {
    condition     = length(output.genie_space_can_run_widening["sales"]) == 0
    error_message = "an unchanged ACL adds no CAN_RUN"
  }
}

run "widened_acl_adds_only_the_new_group" {
  command = plan
  variables {
    genie_spaces        = [{ name = "Sales", genie_space_id = "space-1", uc_tables = [] }]
    groups              = { analysts = {}, auditors = {} }
    genie_space_configs = { Sales = { acl_groups = ["analysts", "auditors"] } }
  }
  assert {
    condition     = toset(output.genie_space_can_run_widening["sales"]) == toset(["auditors"])
    error_message = "only the added group needs the gate"
  }
}

run "shrunk_acl_adds_nothing" {
  command = plan
  variables {
    genie_spaces        = [{ name = "Sales", genie_space_id = "space-1", uc_tables = [] }]
    groups              = { analysts = {}, auditors = {} }
    genie_space_configs = { Sales = { acl_groups = [] } }
  }
  assert {
    condition     = length(output.genie_space_can_run_widening["sales"]) == 0
    error_message = "clearing an ACL never needs the gate"
  }
}

run "acl_on_record_for_another_space" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 2 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
      "tests/.tmp/exposure/terraform.tfstate"               = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls", instances = [{ index_key = "sales", attributes = { id = "1", triggers = { space_id = "space-0", groups = "analysts" } } }] }] })
    }
  }
}

run "other_space_id_gets_no_credit" {
  command = plan
  variables {
    genie_spaces        = [{ name = "Sales", genie_space_id = "space-1", uc_tables = [] }]
    groups              = { analysts = {}, auditors = {} }
    genie_space_configs = { Sales = { acl_groups = ["analysts"] } }
  }
  assert {
    condition     = toset(output.genie_space_can_run_widening["sales"]) == toset(["analysts"])
    error_message = "an ACL applied to another agent is not this agent's"
  }
}

run "tainted_acl_on_record" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 2 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
      "tests/.tmp/exposure/terraform.tfstate"               = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls", instances = [{ index_key = "sales", status = "tainted", attributes = { id = "1", triggers = { space_id = "space-1", groups = "analysts" } } }] }] })
    }
  }
}

run "tainted_acl_gets_no_credit" {
  command = plan
  variables {
    genie_spaces        = [{ name = "Sales", genie_space_id = "space-1", uc_tables = [] }]
    groups              = { analysts = {}, auditors = {} }
    genie_space_configs = { Sales = { acl_groups = ["analysts"] } }
  }
  assert {
    condition     = toset(output.genie_space_can_run_widening["sales"]) == toset(["analysts"])
    error_message = "a tainted (failed) ACL apply is not on record"
  }
}

run "unreadable_own_state" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 2 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
      "tests/.tmp/exposure/terraform.tfstate"               = "{truncated"
    }
  }
}

run "unreadable_own_state_gets_no_credit" {
  command = plan
  variables {
    genie_spaces        = [{ name = "Sales", genie_space_id = "space-1", uc_tables = [] }]
    groups              = { analysts = {}, auditors = {} }
    genie_space_configs = { Sales = { acl_groups = ["analysts"] } }
  }
  assert {
    condition     = toset(output.genie_space_can_run_widening["sales"]) == toset(["analysts"])
    error_message = "unreadable state means nothing is on record"
  }
}

run "created_space_acl_on_record" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 2 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
      "tests/.tmp/exposure/terraform.tfstate"               = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "terraform_data", name = "genie_space", instances = [{ index_key = "sales", attributes = { id = "created-1", triggers_replace = { value = { host = "https://example.invalid" }, type = ["object", { host = "string" }] } } }] }, { module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls_created", instances = [{ index_key = "sales", attributes = { id = "2", triggers = { space_create_id = "created-1", groups = "analysts" } } }] }] })
    }
  }
}

run "unchanged_created_space_acl_adds_nothing" {
  command = plan
  variables {
    genie_spaces        = [{ name = "Sales", uc_tables = ["cat.sch.customers"] }]
    groups              = { analysts = {}, auditors = {} }
    genie_space_configs = { Sales = { acl_groups = ["analysts"] } }
  }
  assert {
    condition     = length(output.genie_space_can_run_widening["sales"]) == 0
    error_message = "an unchanged ACL on a created agent adds no CAN_RUN"
  }
}

run "created_space_on_another_host" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 2 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
      "tests/.tmp/exposure/terraform.tfstate"               = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "terraform_data", name = "genie_space", instances = [{ index_key = "sales", attributes = { id = "created-1", triggers_replace = { value = { host = "https://old.invalid" }, type = ["object", { host = "string" }] } } }] }, { module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls_created", instances = [{ index_key = "sales", attributes = { id = "2", triggers = { space_create_id = "created-1", groups = "analysts" } } }] }] })
    }
  }
}

run "moved_created_space_gets_no_credit" {
  command = plan
  variables {
    genie_spaces        = [{ name = "Sales", uc_tables = ["cat.sch.customers"] }]
    groups              = { analysts = {}, auditors = {} }
    genie_space_configs = { Sales = { acl_groups = ["analysts"] } }
  }
  assert {
    condition     = toset(output.genie_space_can_run_widening["sales"]) == toset(["analysts"])
    error_message = "a created agent being moved to another host starts with no CAN_RUN"
  }
}

run "created_space_acl_for_an_old_agent" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 2 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "@NOW@" })
      "tests/.tmp/exposure/terraform.tfstate"               = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "terraform_data", name = "genie_space", instances = [{ index_key = "sales", attributes = { id = "created-1", triggers_replace = { value = { host = "https://example.invalid" }, type = ["object", { host = "string" }] } } }] }, { module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls_created", instances = [{ index_key = "sales", attributes = { id = "2", triggers = { space_create_id = "created-0", groups = "analysts" } } }] }] })
    }
  }
}

run "recreated_space_gets_no_credit" {
  command = plan
  variables {
    genie_spaces        = [{ name = "Sales", uc_tables = ["cat.sch.customers"] }]
    groups              = { analysts = {}, auditors = {} }
    genie_space_configs = { Sales = { acl_groups = ["analysts"] } }
  }
  assert {
    condition     = toset(output.genie_space_can_run_widening["sales"]) == toset(["analysts"])
    error_message = "an ACL recorded for an earlier created agent is not this agent's"
  }
}

run "gate_expires_with_acls_on_record" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 2 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "2000-01-01T00:00:00Z" })
      "tests/.tmp/exposure/terraform.tfstate"               = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls", instances = [{ index_key = "sales", attributes = { id = "1", triggers = { space_id = "space-1", groups = "analysts" } } }] }] })
    }
  }
}

run "unchanged_acl_plans_after_the_gate_expires" {
  command = plan
  variables {
    genie_spaces        = [{ name = "Sales", genie_space_id = "space-1", uc_tables = [] }]
    groups              = { analysts = {}, auditors = {} }
    genie_space_configs = { Sales = { acl_groups = ["analysts"] } }
  }
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "older than coverage_gate_max_age")
    error_message = "the gate has expired"
  }
  assert {
    condition     = output.genie_space_acls_groups["sales"] == "analysts" && output.genie_space_acls_applied
    error_message = "the unchanged ACL must still plan"
  }
}

run "shrunk_acl_plans_after_the_gate_expires" {
  command = plan
  variables {
    genie_spaces        = [{ name = "Sales", genie_space_id = "space-1", uc_tables = [] }]
    groups              = { analysts = {}, auditors = {} }
    genie_space_configs = { Sales = { acl_groups = [] } }
  }
  assert {
    condition     = output.genie_space_acls_groups["sales"] == ""
    error_message = "clearing CAN_RUN must plan while exposure is blocked"
  }
}

run "gate_expires_with_created_acl_on_record" {
  module {
    source = "../data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/exposure/data_access/terraform.tfstate"   = jsonencode({ version = 4, outputs = { coverage_gate = { value = { business_access_enabled = true, fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = 2 } }, table_grant_resource_keys = { value = ["cat.sch.customers|analysts", "cat.sch.customers|auditors"] } } })
      "tests/.tmp/exposure/data_access/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "applied", refreshed_at = "2000-01-01T00:00:00Z" })
      "tests/.tmp/exposure/terraform.tfstate"               = jsonencode({ version = 4, outputs = {}, resources = [{ module = "module.workspace", mode = "managed", type = "terraform_data", name = "genie_space", instances = [{ index_key = "sales", attributes = { id = "created-1", triggers_replace = { value = { host = "https://example.invalid" }, type = ["object", { host = "string" }] } } }] }, { module = "module.workspace", mode = "managed", type = "null_resource", name = "genie_space_acls_created", instances = [{ index_key = "sales", attributes = { id = "2", triggers = { space_create_id = "created-1", groups = "analysts" } } }] }] })
    }
  }
}

run "unchanged_created_acl_plans_after_the_gate_expires" {
  command = plan
  variables {
    genie_spaces        = [{ name = "Sales", uc_tables = ["cat.sch.customers"] }]
    groups              = { analysts = {}, auditors = {} }
    genie_space_configs = { Sales = { acl_groups = ["analysts"] } }
  }
  assert {
    condition     = strcontains(output.genie_exposure_blocker, "older than coverage_gate_max_age")
    error_message = "the gate has expired"
  }
  assert {
    condition     = output.genie_space_acls_groups["sales"] == "analysts"
    error_message = "the unchanged ACL on a created agent must still plan"
  }
}
