import sys
from pathlib import Path

import hcl2
import pytest

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
    assert config["enable_classification"] is True
    assert config["enable_auto_tagging"] is False
    assert config["business_access_enabled"] is False


def test_promotion_does_not_copy_or_overwrite_environment_discovery(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    (source / "data_access").mkdir(parents=True)
    (dest / "data_access").mkdir(parents=True)
    (source / "env.auto.tfvars").write_text("genie_spaces = []\nuc_tables = []\n")
    (source / "data_access" / "discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["dev_catalog.agent.orders"]\n'
        'discovered_table_agents = { "dev_catalog.agent.orders" = ["Dev agent"] }\n'
    )
    prod_fact = dest / "data_access" / "discovered_uc_tables.auto.tfvars"
    prod_fact.write_text(
        'discovered_uc_tables = ["prod_catalog.agent.orders"]\n'
        'discovered_table_agents = { "prod_catalog.agent.orders" = ["Prod agent"] }\n'
    )

    monkeypatch.setattr(
        sys,
        "argv",
        ["remap_env_config.py", str(source), str(dest), "dev_catalog=prod_catalog"],
    )
    remap_env_config.main()

    assert prod_fact.read_text() == (
        'discovered_uc_tables = ["prod_catalog.agent.orders"]\n'
        'discovered_table_agents = { "prod_catalog.agent.orders" = ["Prod agent"] }\n'
    )


def test_id_only_spaces_promote_with_distinct_canonical_titles(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    (source / "generated").mkdir(parents=True)
    (source / "env.auto.tfvars").write_text('''genie_spaces = [
  { genie_space_id = "s1", uc_tables = ["paycat.s.t"] },
  { genie_space_id = "s2", uc_tables = ["hrcat.s.t"] },
  { name = "Named", genie_space_id = "s3", uc_tables = ["paycat.s.n"] },
]
''')
    (source / "generated" / "abac.auto.tfvars").write_text(
        'genie_space_id_to_name = { s1 = "Payments", s2 = "HR", s3 = "Ignored Title" }\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "paycat=ppay,hrcat=phr"
    ])
    remap_env_config.main()
    with (dest / "env.auto.tfvars").open() as handle:
        spaces = hcl2.load(handle)["genie_spaces"]
    assert [(s["name"], s["genie_space_id"]) for s in spaces] == [
        ("Payments", ""), ("HR", ""), ("Named", "")
    ]


def test_duplicate_resolved_titles_fail_loud(tmp_path, monkeypatch, capsys):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    (source / "generated").mkdir(parents=True)
    (source / "env.auto.tfvars").write_text('''genie_spaces = [
  { genie_space_id = "s1", uc_tables = ["a.s.t"] },
  { genie_space_id = "s2", uc_tables = ["b.s.t"] },
]
''')
    (source / "generated" / "abac.auto.tfvars").write_text(
        'genie_space_id_to_name = { s1 = "Same", s2 = "Same" }\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "a=pa,b=pb"
    ])
    with pytest.raises(SystemExit) as exc:
        remap_env_config.main()
    assert exc.value.code == 1
    assert "same canonical name" in capsys.readouterr().out
    assert not (dest / "env.auto.tfvars").exists()


def test_canonical_title_is_hcl_escaped(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    (source / "generated").mkdir(parents=True)
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = [{ genie_space_id = "s1", uc_tables = ["a.s.t"] }]\n'
    )
    title = 'Finance "Quoted" \\ Analytics'
    (source / "generated" / "abac.auto.tfvars").write_text(
        "genie_space_id_to_name = { s1 = " + __import__("json").dumps(title) + " }\n"
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "a=pa"
    ])
    remap_env_config.main()
    with (dest / "env.auto.tfvars").open() as handle:
        assert hcl2.load(handle)["genie_spaces"][0]["name"] == title
