import re
from pathlib import Path


MAIN_TF = Path(__file__).parents[1] / "modules" / "data_access" / "main.tf"
WORKSPACE_MAIN_TF = Path(__file__).parents[1] / "modules" / "workspace" / "main.tf"


def _resource_body(source: str, name: str) -> str:
    marker = f'resource "databricks_grant" "{name}" {{'
    start = source.index(marker) + len(marker)
    depth = 1
    for index in range(start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start:index]
    raise AssertionError(f"unterminated resource {name}")


def _typed_resource_body(source: str, resource_type: str, name: str) -> str:
    marker = f'resource "{resource_type}" "{name}" {{'
    start = source.index(marker) + len(marker)
    depth = 1
    for index in range(start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start:index]
    raise AssertionError(f"unterminated resource {resource_type}.{name}")


def test_group_grants_follow_catalog_schema_table_chain():
    source = MAIN_TF.read_text()
    catalog = _resource_body(source, "catalog_access")
    schema = _resource_body(source, "schema_access")
    table = _resource_body(source, "table_access")

    assert "setproduct(local.all_catalogs, local.access_principals)" in catalog
    assert 'privileges = ["USE_CATALOG"]' in catalog

    assert "setproduct(local.uc_schemas, local.access_principals)" in schema
    assert "schema     = each.value.schema" in schema
    assert 'privileges = ["USE_SCHEMA"]' in schema

    assert "for pair in local.table_access_pairs" in table
    assert "table      = each.value.table" in table
    assert 'privileges = ["SELECT"]' in table


def test_select_is_not_granted_at_namespace_level():
    source = MAIN_TF.read_text()

    assert '"SELECT"' not in _resource_body(source, "catalog_access")
    assert '"SELECT"' not in _resource_body(source, "schema_access")
    assert '"USE_CATALOG"' not in _resource_body(source, "table_access")
    assert '"USE_SCHEMA"' not in _resource_body(source, "table_access")


def test_deployment_sp_self_grant_includes_apply_tag():
    source = MAIN_TF.read_text()
    deployer = _resource_body(source, "terraform_sp_manage_catalog")

    match = re.search(r"privileges\s*=\s*\[([^]]+)\]", deployer)
    assert match is not None
    assert set(re.findall(r'"([A-Z_]+)"', match.group(1))) == {
        "USE_CATALOG",
        "USE_SCHEMA",
        "EXECUTE",
        "MANAGE",
        "CREATE_FUNCTION",
        "APPLY_TAG",
    }


def test_tag_assignments_wait_for_deployment_sp_grant():
    source = MAIN_TF.read_text()
    start = source.index('resource "databricks_entity_tag_assignment" "assignments" {')
    end = source.index('resource "time_sleep" "wait_for_tag_propagation"', start)

    assert "depends_on = [databricks_grant.terraform_sp_manage_catalog]" in source[start:end]


def test_masking_functions_and_policies_wait_for_deployment_sp_grant():
    source = MAIN_TF.read_text()
    for start_marker, end_marker in (
        ('resource "terraform_data" "masking_functions" {',
         'resource "databricks_policy_info" "policies" {'),
        ('resource "databricks_policy_info" "policies" {', None),
    ):
        start = source.index(start_marker)
        end = source.index(end_marker, start) if end_marker else len(source)
        assert "databricks_grant.terraform_sp_manage_catalog" in source[start:end]


def test_table_select_waits_for_complete_policy_enforcement_chain():
    source = MAIN_TF.read_text()
    table = _resource_body(source, "table_access")
    policies = _typed_resource_body(source, "databricks_policy_info", "policies")
    enforcement_wait = _typed_resource_body(
        source, "time_sleep", "wait_for_policy_enforcement"
    )

    # Whole-resource dependencies make any failed mask or policy instance block
    # every table grant, rather than only a matching for_each instance.
    for prerequisite in (
        "time_sleep.wait_for_tag_propagation",
        "terraform_data.masking_functions",
        "databricks_policy_info.policies",
        "time_sleep.wait_for_policy_enforcement",
    ):
        assert prerequisite in table

    assert "databricks_grant.table_access" not in policies
    assert "depends_on      = [databricks_policy_info.policies]" in enforcement_wait
    assert 'create_duration = "30s"' in enforcement_wait


def test_policy_grant_dependency_graph_is_acyclic_and_fail_closed():
    source = MAIN_TF.read_text()
    bodies = {
        "table": _resource_body(source, "table_access"),
        "policies": _typed_resource_body(source, "databricks_policy_info", "policies"),
        "policy_wait": _typed_resource_body(
            source, "time_sleep", "wait_for_policy_enforcement"
        ),
    }
    refs = {
        node: set(re.findall(
            r"(?:databricks_grant|databricks_policy_info|terraform_data|time_sleep)\.[A-Za-z0-9_]+",
            body,
        ))
        for node, body in bodies.items()
    }

    # This is the relevant Terraform plan graph: grants have both failed-policy
    # and failed-mask nodes as ancestors. Terraform reverses these edges during
    # destroy, so table grants are removed before the wait and policies.
    assert "databricks_policy_info.policies" in refs["table"]
    assert "terraform_data.masking_functions" in refs["table"]
    assert "databricks_policy_info.policies" in refs["policy_wait"]
    assert "databricks_grant.table_access" not in refs["policies"]


def test_existing_grant_and_policy_resource_addresses_and_keys_are_unchanged():
    source = MAIN_TF.read_text()
    table = _resource_body(source, "table_access")
    policies = _typed_resource_body(source, "databricks_policy_info", "policies")

    assert "for pair in local.table_access_pairs" in table
    assert '"${pair.table}|${pair.principal}"' in table
    assert "for_each = local.fgac_policy_map" in policies
    assert 'name                  = "${each.value.catalog}_${each.key}"' in policies


def test_business_select_is_fail_closed_while_structural_grants_remain():
    source = MAIN_TF.read_text()

    assert "for_each = var.business_access_enabled ? {" in _resource_body(source, "table_access")
    assert "var.business_access_enabled" not in _resource_body(source, "catalog_access")
    assert "var.business_access_enabled" not in _resource_body(source, "schema_access")


def test_explicit_empty_agent_acl_is_fail_closed_in_both_layers():
    data_access = MAIN_TF.read_text()
    workspace = WORKSPACE_MAIN_TF.read_text()

    assert "length(local.scoped_table_access_principals[table]) == 0" not in data_access
    assert 'join(",", space.config.acl_groups)' in workspace
    assert 'join(",", keys(var.groups))' not in workspace.split(
        "genie_space_groups =", 1
    )[1].split("existing_spaces =", 1)[0]
    assert 'GENIE_ALLOW_EMPTY_ACL    = "1"' in workspace


def test_catalog_grants_are_serialized_without_authoritative_replacement():
    source = MAIN_TF.read_text()
    catalog_access = _resource_body(source, "catalog_access")
    assert "depends_on = [databricks_grant.terraform_sp_manage_catalog]" in catalog_access
    assert 'resource "databricks_grants"' not in source


def test_builtin_policy_targets_are_included_in_access_principals():
    source = MAIN_TF.read_text()
    normalized = " ".join(source.split())
    assert (
        "access_principals = distinct(concat( keys(var.groups), "
        "flatten([ for p in var.fgac_policies : p.to_principals "
        "if !startswith(p.comment, \"GenieRails treatment fallback; "
        "principals are masking-only\") ]), "
        "flatten(values(var.genie_space_acl_groups)), ))"
    ) in normalized
    for resource in ("catalog_access", "schema_access"):
        body = _resource_body(source, resource)
        assert "local.access_principals" in body
        assert "keys(var.groups)" not in body
    assert "local.table_access_pairs" in _resource_body(source, "table_access")


def test_table_select_principals_are_derived_per_table():
    source = MAIN_TF.read_text()

    assert "table_access_principals = {" in source
    assert "lookup(var.table_agents, table, [])" in source
    assert "lookup(var.genie_space_acl_groups, agent, [])" in source
    assert "contains(var.admin_uc_tables, table)" in source
    assert "contains(local.legacy_unattributed_discovered_tables, table)" in source
    assert "setproduct(local.effective_uc_tables, local.access_principals)" not in source


def test_masking_deployer_does_not_declassify_oauth_secret():
    source = MAIN_TF.read_text()

    masking = source[source.index('resource "terraform_data" "masking_functions" {'):
                     source.index('resource "databricks_policy_info" "policies" {')]
    # The deployer reads the secret from auth.auto.tfvars; it never enters state.
    assert "databricks_client_secret" not in masking
    assert "nonsensitive(var.databricks_client_secret)" not in source
