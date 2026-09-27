from pathlib import Path


MAIN_TF = Path(__file__).parents[1] / "modules" / "data_access" / "main.tf"


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

    assert "setproduct(var.uc_tables, local.access_principals)" in table
    assert "table      = each.value.table" in table
    assert 'privileges = ["SELECT"]' in table


def test_select_is_not_granted_at_namespace_level():
    source = MAIN_TF.read_text()

    assert '"SELECT"' not in _resource_body(source, "catalog_access")
    assert '"SELECT"' not in _resource_body(source, "schema_access")
    assert '"USE_CATALOG"' not in _resource_body(source, "table_access")
    assert '"USE_SCHEMA"' not in _resource_body(source, "table_access")


def test_business_select_is_fail_closed_while_structural_grants_remain():
    source = MAIN_TF.read_text()

    assert "for_each = var.business_access_enabled ? {" in _resource_body(source, "table_access")
    assert "var.business_access_enabled" not in _resource_body(source, "catalog_access")
    assert "var.business_access_enabled" not in _resource_body(source, "schema_access")


def test_builtin_policy_targets_are_included_in_access_principals():
    source = MAIN_TF.read_text()

    assert "keys(var.groups)" in source
    assert "flatten([for p in var.fgac_policies : p.to_principals])" in source
