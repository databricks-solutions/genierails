import sys
from pathlib import Path

import hcl2

from scripts import remap_env_config


def test_table_only_promotion_preserves_and_remaps_top_level_uc_tables(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    source.mkdir()
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = []\n'
        'uc_tables = ["dev_catalog.genierails_e2e.customers"]\n'
    )

    monkeypatch.setattr(
        sys,
        "argv",
        ["remap_env_config.py", str(source), str(dest), "dev_catalog=prod_catalog"],
    )
    remap_env_config.main()

    with (dest / "env.auto.tfvars").open() as handle:
        config = hcl2.load(handle)
    assert config["genie_spaces"] == []
    assert config["uc_tables"] == ["prod_catalog.genierails_e2e.customers"]
