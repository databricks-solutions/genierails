import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import reader_inventory as ri


def _principals(items):
    return {item.principal for item in items}


def test_table_schema_catalog_select_and_all_privileges_are_readers():
    source = ri.FakeSource(grant_rows=[
        ri.Grant("TABLE", "c.s.t", "table-reader", "SELECT"),
        ri.Grant("SCHEMA", "c.s", "schema-reader", "ALL PRIVILEGES"),
        ri.Grant("CATALOG", "c", "catalog-reader", "SELECT"),
        ri.Grant("TABLE", "c.s.t", "ignored", "MODIFY"),
    ])
    report = ri.inventory_readers(source, "c.s.t")
    assert _principals(report.readers) == {"table-reader", "schema-reader", "catalog-reader"}


def test_nested_groups_expand_transitively_and_cycles_terminate():
    source = ri.FakeSource(
        grant_rows=[ri.Grant("TABLE", "c.s.t", "outer", "SELECT")],
        groups={"outer": {"inner", "direct-user"}, "inner": {"leaf", "outer"}},
    )
    assert _principals(ri.inventory_readers(source, "c.s.t").readers) == {
        "outer", "inner", "direct-user", "leaf"
    }


def test_views_of_views_and_inherited_view_grants_are_followed():
    source = ri.FakeSource(
        grant_rows=[
            ri.Grant("SCHEMA", "vcat.vs", "inherited", "SELECT"),
            ri.Grant("TABLE", "vcat.vs.v2", "direct", "ALL PRIVILEGES"),
        ],
        dependencies={"c.s.t": {"vcat.vs.v1"}, "vcat.vs.v1": {"vcat.vs.v2"}},
    )
    report = ri.inventory_readers(source, "c.s.t")
    assert report.dependent_views == ("vcat.vs.v1", "vcat.vs.v2")
    assert _principals(report.readers) == {"inherited", "direct"}


def test_genierails_exact_grant_is_excluded_but_other_path_remains():
    table_grant = ri.Grant("TABLE", "c.s.t", "p", "SELECT")
    source = ri.FakeSource(grant_rows=[table_grant, ri.Grant("SCHEMA", "c.s", "p", "SELECT")])
    report = ri.inventory_readers(source, "c.s.t", [table_grant])
    evidence = next(item for item in report.readers if item.principal == "p")
    assert evidence.reasons == ("SELECT on SCHEMA c.s",)


def test_base_owners_are_readers_and_granters_but_view_owner_is_granter_only():
    source = ri.FakeSource(
        owners={
            ("CATALOG", "c"): "catalog-owner", ("SCHEMA", "c.s"): "schema-owner",
            ("TABLE", "c.s.t"): "table-owner", ("TABLE", "v.s.v"): "view-owner",
        },
        dependencies={"c.s.t": {"v.s.v"}},
    )
    report = ri.inventory_readers(source, "c.s.t")
    assert _principals(report.readers) == {"catalog-owner", "schema-owner", "table-owner"}
    assert _principals(report.granters) == {
        "catalog-owner", "schema-owner", "table-owner", "view-owner"
    }


def test_manage_is_granter_not_reader_and_metastore_admin_is_both():
    source = ri.FakeSource(
        grant_rows=[ri.Grant("TABLE", "c.s.t", "manager", "MANAGE")], admins={"admin"}
    )
    report = ri.inventory_readers(source, "c.s.t")
    assert _principals(report.readers) == {"admin"}
    assert _principals(report.granters) == {"manager", "admin"}


def test_granter_group_members_are_expanded():
    source = ri.FakeSource(
        grant_rows=[ri.Grant("TABLE", "c.s.t", "managers", "MANAGE")],
        groups={"managers": {"alice"}},
    )
    assert _principals(ri.inventory_readers(source, "c.s.t").granters) == {"managers", "alice"}


def test_duplicate_inherited_paths_merge_evidence():
    source = ri.FakeSource(grant_rows=[
        ri.Grant("TABLE", "c.s.t", "p", "SELECT"), ri.Grant("SCHEMA", "c.s", "p", "SELECT")
    ])
    evidence = ri.inventory_readers(source, "c.s.t").readers[0]
    assert evidence.reasons == ("SELECT on SCHEMA c.s", "SELECT on TABLE c.s.t")


def test_databricks_sdk_is_not_imported_for_pure_inventory(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.sdk", None)
    assert ri.inventory_readers(ri.FakeSource(), "c.s.t").table == "c.s.t"


def test_databricks_source_uses_statement_api_and_pages_results():
    first = SimpleNamespace(
        status=SimpleNamespace(state="SUCCEEDED"), statement_id="id",
        result=SimpleNamespace(data_array=[["p", "SELECT"]]),
        manifest=SimpleNamespace(total_chunk_count=2),
    )
    execution = SimpleNamespace(
        execute_statement=lambda **kwargs: first,
        get_statement_result_chunk_n=lambda *args: SimpleNamespace(data_array=[["q", "ALL PRIVILEGES"]]),
    )
    source = ri.DatabricksSource("wh", SimpleNamespace(statement_execution=execution))
    grants = list(source.grants("TABLE", "c.s.t"))
    assert [(g.principal, g.privilege) for g in grants] == [("p", "SELECT"), ("q", "ALL PRIVILEGES")]
