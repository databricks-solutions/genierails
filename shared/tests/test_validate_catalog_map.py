import sys

import pytest

from scripts import validate_catalog_map


def run_validator(monkeypatch, env_dir, mapping):
    monkeypatch.setattr(
        sys, "argv", ["validate_catalog_map.py", str(env_dir), mapping]
    )
    validate_catalog_map.main()


def test_catalog_found_only_in_discovered_file(tmp_path, monkeypatch):
    (tmp_path / "env.auto.tfvars").write_text("uc_tables = []\n")
    (tmp_path / "data_access").mkdir()
    (tmp_path / "data_access/discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["dev_catalog.schema.table"]\n'
    )
    run_validator(monkeypatch, tmp_path, "dev_catalog=prod_catalog")


def test_empty_footprint_has_generate_guidance(tmp_path, monkeypatch, capsys):
    (tmp_path / "env.auto.tfvars").write_text("uc_tables = []\n")
    with pytest.raises(SystemExit) as exc:
        run_validator(monkeypatch, tmp_path, "dev=prod")
    assert exc.value.code == 1
    assert f"make generate ENV={tmp_path.name} MODE=genie" in capsys.readouterr().out
