mock_provider "databricks" {}
mock_provider "null" {}
mock_provider "time" {}

run "per_agent_select_grants_are_isolated_and_shared_tables_union_acls" {
  command = plan

  variables {
    env_dir                   = "../../examples/healthcare"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_host = "https://example.invalid"
    uc_tables                 = ["grants_catalog.business.orders"]
    genie_spaces = [
      { name = "Agent A", uc_tables = ["agent_catalog.space.a_only", "agent_catalog.space.shared"] },
      { name = "Agent B", uc_tables = ["agent_catalog.space.b_only", "agent_catalog.space.shared"] },
    ]
    genie_space_acl_groups = {
      "Agent A" = ["agent_a_group"]
      "Agent B" = ["agent_b_group"]
    }
    discovered_uc_tables = ["discovered_catalog.agent.facts"]
    discovered_table_agents = {
      "discovered_catalog.agent.facts" = ["Agent A"]
    }
    groups = {
      agent_a_group = {}
      agent_b_group = {}
    }
    business_access_enabled = true
    enable_classification   = true
    sql_warehouse_id        = "warehouse"
  }

  assert {
    condition     = toset(output.catalogs) == toset(["grants_catalog", "agent_catalog", "discovered_catalog"])
    error_message = "all effective catalogs must enter service-principal and business catalog grants"
  }

  assert {
    condition = toset(output.grant_uc_tables) == toset([
      "grants_catalog.business.orders",
      "agent_catalog.space.a_only",
      "agent_catalog.space.b_only",
      "agent_catalog.space.shared",
      "discovered_catalog.agent.facts",
    ])
    error_message = "user, Genie-space, and discovered tables must enter the grant footprint"
  }

  assert {
    condition = toset(output.schema_grant_resource_keys) == toset([
      "grants_catalog.business|agent_a_group",
      "grants_catalog.business|agent_b_group",
      "agent_catalog.space|agent_a_group",
      "agent_catalog.space|agent_b_group",
      "discovered_catalog.agent|agent_a_group",
      "discovered_catalog.agent|agent_b_group",
    ])
    error_message = "schema grant resources must be sourced from the effective governed table footprint"
  }

  assert {
    condition = toset(output.table_grant_resource_keys) == toset([
      "grants_catalog.business.orders|agent_a_group",
      "grants_catalog.business.orders|agent_b_group",
      "agent_catalog.space.a_only|agent_a_group",
      "agent_catalog.space.b_only|agent_b_group",
      "agent_catalog.space.shared|agent_a_group",
      "agent_catalog.space.shared|agent_b_group",
      "discovered_catalog.agent.facts|agent_a_group",
    ])
    error_message = "table grant resources must be sourced from the effective governed table footprint"
  }

  assert {
    condition = toset(output.classification_uc_tables) == toset([
      "grants_catalog.business.orders",
      "agent_catalog.space.a_only",
      "agent_catalog.space.b_only",
      "agent_catalog.space.shared",
      "discovered_catalog.agent.facts",
    ])
    error_message = "classification must cover the union of normal and Genie-space-only tables"
  }
}

run "legacy_unattributed_discovery_falls_back_to_all_principals" {
  command = plan

  variables {
    env_dir                   = "../../examples/healthcare"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_host = "https://example.invalid"
    discovered_uc_tables      = ["legacy_catalog.agent.facts"]
    groups = {
      agent_a_group = {}
      agent_b_group = {}
    }
    business_access_enabled = true
    sql_warehouse_id        = "warehouse"
  }

  assert {
    condition = toset(output.table_grant_resource_keys) == toset([
      "legacy_catalog.agent.facts|agent_a_group",
      "legacy_catalog.agent.facts|agent_b_group",
    ])
    error_message = "legacy unattributed discovered tables must retain all-tier SELECT access"
  }

  assert {
    condition     = toset(output.legacy_unattributed_discovered_tables) == toset(["legacy_catalog.agent.facts"])
    error_message = "legacy fallback must be visible as a warning-oriented output"
  }
}

run "empty_agent_list_falls_back_to_all_principals" {
  command = plan

  variables {
    env_dir                   = "../../examples/healthcare"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_host = "https://example.invalid"
    discovered_uc_tables      = ["orphan_catalog.agent.facts"]
    discovered_table_agents = {
      "orphan_catalog.agent.facts" = []
    }
    groups = {
      agent_a_group = {}
      agent_b_group = {}
    }
    business_access_enabled = true
    sql_warehouse_id        = "warehouse"
  }

  assert {
    condition = toset(output.table_grant_resource_keys) == toset([
      "orphan_catalog.agent.facts|agent_a_group",
      "orphan_catalog.agent.facts|agent_b_group",
    ])
    error_message = "an explicitly empty owner list must fail safe to all access principals"
  }
}

run "unknown_agent_does_not_widen_known_agent_scope" {
  command = plan

  variables {
    env_dir                   = "../../examples/healthcare"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_host = "https://example.invalid"
    discovered_uc_tables      = ["scoped_catalog.agent.facts"]
    discovered_table_agents = {
      "scoped_catalog.agent.facts" = ["Agent A", "Old A"]
    }
    genie_space_acl_groups = {
      "Agent A" = ["agent_a_group"]
    }
    groups = {
      agent_a_group = {}
      agent_b_group = {}
    }
    business_access_enabled = true
    sql_warehouse_id        = "warehouse"
  }

  assert {
    condition     = toset(output.table_grant_resource_keys) == toset(["scoped_catalog.agent.facts|agent_a_group"])
    error_message = "an unknown stale agent must contribute nothing and must not widen a known agent's ACL scope"
  }
}

run "top_level_admin_table_wins_over_agent_scope" {
  command = plan

  variables {
    env_dir                   = "../../examples/healthcare"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_host = "https://example.invalid"
    uc_tables                 = ["admin_catalog.shared.facts"]
    genie_spaces = [
      { name = "Agent A", uc_tables = ["admin_catalog.shared.facts"] },
    ]
    genie_space_acl_groups = {
      "Agent A" = ["agent_a_group"]
    }
    groups = {
      agent_a_group = {}
      agent_b_group = {}
    }
    business_access_enabled = true
    sql_warehouse_id        = "warehouse"
  }

  assert {
    condition = toset(output.table_grant_resource_keys) == toset([
      "admin_catalog.shared.facts|agent_a_group",
      "admin_catalog.shared.facts|agent_b_group",
    ])
    error_message = "top-level administrator intent must grant all tiers even when the table is also agent-scoped"
  }
}

run "absent_discovery_preserves_legacy_user_table_behavior" {
  command = plan

  variables {
    env_dir                   = "../../examples/healthcare"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_host = "https://example.invalid"
    uc_tables                 = ["legacy_catalog.business.orders"]
    groups                    = { analysts = {} }
    business_access_enabled   = true
    sql_warehouse_id          = "warehouse"
  }

  assert {
    condition     = toset(output.grant_uc_tables) == toset(["legacy_catalog.business.orders"])
    error_message = "the default empty discovered list must preserve the legacy grant footprint"
  }

  assert {
    condition     = toset(output.table_grant_resource_keys) == toset(["legacy_catalog.business.orders|analysts"])
    error_message = "the absent discovered file must not change table grants"
  }
}
