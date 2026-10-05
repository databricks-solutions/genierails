from scripts.footprint import main, resolve_footprint


def test_resolver_unions_all_sources_with_stable_deduplication(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text(
        'uc_tables = ["cat.top.one", "cat.shared.table"]\n'
        'genie_spaces = [{ uc_tables = ["cat.space.two", "cat.shared.table"] }]\n'
    )
    (tmp_path / "data_access").mkdir()
    (tmp_path / "data_access/discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["cat.discovered.three", "cat.top.one"]\n'
    )
    assert resolve_footprint(tmp_path) == [
        "cat.top.one",
        "cat.shared.table",
        "cat.space.two",
        "cat.discovered.three",
    ]


def test_promote_catalog_detection_reads_discovered_file(tmp_path, capsys):
    (tmp_path / "env.auto.tfvars").write_text("uc_tables = []\n")
    (tmp_path / "data_access").mkdir()
    (tmp_path / "data_access/discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["discovered_catalog.schema.table"]\n'
    )
    assert main([str(tmp_path), "--catalogs"]) == 0
    assert capsys.readouterr().out.strip() == "discovered_catalog"
