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
        ('resource "null_resource" "deploy_masking_functions" {',
         'resource "databricks_policy_info" "policies" {'),
        ('resource "databricks_policy_info" "policies" {', None),
    ):
        start = source.index(start_marker)
        end = source.index(end_marker, start) if end_marker else len(source)
        assert "databricks_grant.terraform_sp_manage_catalog" in source[start:end]


def test_business_select_is_fail_closed_while_structural_grants_remain():
    source = MAIN_TF.read_text()

    assert "for_each = var.business_access_enabled ? {" in _resource_body(source, "table_access")
    assert "var.business_access_enabled" not in _resource_body(source, "catalog_access")
    assert "var.business_access_enabled" not in _resource_body(source, "schema_access")


def test_explicit_empty_agent_acl_is_fail_closed_in_both_layers():
    data_access = MAIN_TF.read_text()
    workspace = WORKSPACE_MAIN_TF.read_text()

    assert "length(local.scoped_table_access_principals[table]) == 0" not in data_access
    assert 'var.released_genie_acls == null ? space.config.acl_groups' in workspace
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

    assert "client_secret = var.databricks_client_secret" in source
    assert "nonsensitive(var.databricks_client_secret)" not in source
