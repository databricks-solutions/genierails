import json
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
    assert config["sql_warehouse_id"] == ""


def test_promotion_carries_verify_key_column_from_source(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    source.mkdir()
    (source / "env.auto.tfvars").write_text(
        'uc_tables = ["dev.sales.customers"]\nverify_key_column = "customer_id"\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    remap_env_config.main()
    config = hcl2.load((dest / "env.auto.tfvars").open())
    assert config["verify_key_column"] == "customer_id"


def test_promotion_preserves_destination_key_when_source_has_none(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    source.mkdir()
    dest.mkdir()
    (source / "env.auto.tfvars").write_text('uc_tables = ["dev.sales.customers"]\n')
    (dest / "env.auto.tfvars").write_text('verify_key_column = "prod_customer_id"\n')
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    remap_env_config.main()
    config = hcl2.load((dest / "env.auto.tfvars").open())
    assert config["verify_key_column"] == "prod_customer_id"


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


def test_promotion_carries_user_acl_overrides_including_explicit_empty(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    (source / "generated").mkdir(parents=True)
    (source / "env.auto.tfvars").write_text('''genie_spaces = [
  { name = "Payments", uc_tables = ["dev.pay.t"], acl_groups = ["pay_group"] },
  { name = "Private", uc_tables = ["dev.private.t"], acl_groups = [] },
  { name = "Derived", uc_tables = ["dev.derived.t"] },
]
''')
    (source / "generated" / "abac.auto.tfvars").write_text("")
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    remap_env_config.main()
    with (dest / "env.auto.tfvars").open() as handle:
        spaces = hcl2.load(handle)["genie_spaces"]
    assert spaces[0]["acl_groups"] == ["pay_group"]
    assert spaces[1]["acl_groups"] == []
    assert "acl_groups" not in spaces[2]


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


def test_discovered_only_promotion_remaps_union(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    (source / "data_access").mkdir(parents=True)
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "Orders", genie_space_id = "s1" }]\n'
    )
    (source / "data_access/discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["dev.sales.orders"]\n'
        'discovered_table_agents = { "dev.sales.orders" = ["Orders"] }\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    remap_env_config.main()
    config = hcl2.load((dest / "env.auto.tfvars").open())
    assert config["uc_tables"] == ["prod.sales.orders"]
    assert config["genie_spaces"][0]["uc_tables"] == ["prod.sales.orders"]


def test_promotion_fails_closed_without_tables(tmp_path, monkeypatch, capsys):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    source.mkdir()
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "Empty", genie_space_id = "s1" }]\n'
    )
    monkeypatch.setattr(remap_env_config, "_discover_from_genie_api", lambda *_: ("", []))
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    with pytest.raises(SystemExit) as exc:
        remap_env_config.main()
    assert exc.value.code == 1
    assert "no tables found for agent s1" in capsys.readouterr().out
    assert not (dest / "env.auto.tfvars").exists()


def test_empty_environment_fails_closed(tmp_path, monkeypatch, capsys):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    source.mkdir()
    (source / "env.auto.tfvars").write_text("genie_spaces = []\nuc_tables = []\n")
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    with pytest.raises(SystemExit) as exc:
        remap_env_config.main()
    assert exc.value.code == 1
    assert "no tables found for source environment" in capsys.readouterr().out
    assert not (dest / "env.auto.tfvars").exists()


def test_repromotion_preserves_destination_settings_and_closes_gate(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    source.mkdir()
    dest.mkdir()
    (source / "env.auto.tfvars").write_text('uc_tables = ["dev.s.t"]\n')
    (dest / "env.auto.tfvars").write_text(
        'sql_warehouse_id = "warehouse-prod"\n'
        'enable_auto_tagging = true\n'
        'business_access_enabled = true\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    remap_env_config.main()
    config = hcl2.load((dest / "env.auto.tfvars").open())
    assert config["sql_warehouse_id"] == "warehouse-prod"
    assert config["enable_auto_tagging"] is True
    assert config["business_access_enabled"] is False


def test_repromotion_keeps_destination_coverage_acknowledgements_only(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    source.mkdir()
    dest.mkdir()
    (source / "env.auto.tfvars").write_text(
        'uc_tables = ["dev.s.t"]\n'
        'coverage_acknowledged_columns = ["dev.s.t.product_name"]\n'
    )
    (dest / "env.auto.tfvars").write_text(
        'coverage_acknowledged_columns = ["prod.s.t.merchant_name"]\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    remap_env_config.main()
    config = hcl2.load((dest / "env.auto.tfvars").open())
    # A destination's reviewed false positives survive re-promotion; the
    # source's are its own review and never become the destination's.
    assert config["coverage_acknowledged_columns"] == ["prod.s.t.merchant_name"]


def test_first_promotion_writes_no_coverage_acknowledgements(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    source.mkdir()
    (source / "env.auto.tfvars").write_text(
        'uc_tables = ["dev.s.t"]\n'
        'coverage_acknowledged_columns = ["dev.s.t.product_name"]\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    remap_env_config.main()
    assert "coverage_acknowledged_columns" not in hcl2.load((dest / "env.auto.tfvars").open())


def test_legacy_single_space_fallback_uses_only_discovered_tables(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    (source / "data_access").mkdir(parents=True)
    (source / "env.auto.tfvars").write_text(
        'uc_tables = ["dev.admin.audit_log"]\n'
        'genie_spaces = [{ name = "A", genie_space_id = "a" }]\n'
    )
    (source / "data_access/discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["dev.s.t1"]\n'
    )
    monkeypatch.setattr(
        remap_env_config, "_discover_from_genie_api",
        lambda *_: pytest.fail("legacy discovered fallback should avoid the API"),
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    remap_env_config.main()
    config = hcl2.load((dest / "env.auto.tfvars").open())
    assert config["genie_spaces"][0]["uc_tables"] == ["prod.s.t1"]
    assert config["uc_tables"] == ["prod.admin.audit_log", "prod.s.t1"]


def test_renamed_attribution_uses_api_instead_of_widening(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    (source / "data_access").mkdir(parents=True)
    (source / "env.auto.tfvars").write_text(
        'uc_tables = ["dev.admin.audit_log"]\n'
        'genie_spaces = [{ name = "New Name", genie_space_id = "a" }]\n'
    )
    (source / "data_access/discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["dev.s.t1"]\n'
        'discovered_table_agents = { "dev.s.t1" = ["Old Name"] }\n'
    )
    calls = []
    monkeypatch.setattr(
        remap_env_config, "_discover_from_genie_api",
        lambda space_id, _auth: (calls.append(space_id) or ("New Name", ["dev.s.current"])),
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    remap_env_config.main()
    config = hcl2.load((dest / "env.auto.tfvars").open())
    assert calls == ["a"]
    assert config["genie_spaces"][0]["uc_tables"] == ["prod.s.current"]


def test_api_discovered_catalog_must_be_mapped(tmp_path, monkeypatch, capsys):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    source.mkdir()
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "A", genie_space_id = "a" }]\n'
    )
    monkeypatch.setattr(
        remap_env_config, "_discover_from_genie_api",
        lambda *_: ("A", ["other.s.t"]),
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    with pytest.raises(SystemExit) as exc:
        remap_env_config.main()
    assert exc.value.code == 1
    output = capsys.readouterr().out
    assert "missing mappings for resolved catalog(s): other" in output
    assert "make generate ENV=dev MODE=genie" in output
    assert not (dest / "env.auto.tfvars").exists()


def test_attribution_key_missing_from_discovered_union_fails_closed(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    (source / "data_access").mkdir(parents=True)
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "A", genie_space_id = "a" }]\n'
    )
    (source / "data_access/discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["dev.s.t1"]\n'
        'discovered_table_agents = { "other.s.x" = ["A"] }\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod,other=prod_other"
    ])
    with pytest.raises(SystemExit) as exc:
        remap_env_config.main()
    assert exc.value.code == 1
    output = capsys.readouterr().out
    assert "table keys absent from discovered_uc_tables: other.s.x" in output
    assert "make generate" in output
    assert not (dest / "env.auto.tfvars").exists()


def test_unmapped_three_part_inline_table_fails(tmp_path, monkeypatch, capsys):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    source.mkdir()
    (source / "env.auto.tfvars").write_text(
        'uc_tables = ["s.t", "other.s.t"]\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    with pytest.raises(SystemExit):
        remap_env_config.main()
    assert "other" in capsys.readouterr().out


def test_two_spaces_use_discovered_agent_attribution(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    (source / "data_access").mkdir(parents=True)
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "A" }, { name = "B" }]\n'
    )
    (source / "data_access/discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["dev.s.t1", "dev.s.t2"]\n'
        'discovered_table_agents = {\n'
        '  "dev.s.t1" = ["A"]\n'
        '  "dev.s.t2" = ["A", "B"]\n'
        '}\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    remap_env_config.main()
    config = hcl2.load((dest / "env.auto.tfvars").open())
    assert config["genie_spaces"][0]["uc_tables"] == ["prod.s.t1", "prod.s.t2"]
    assert config["genie_spaces"][1]["uc_tables"] == ["prod.s.t2"]


def test_stale_destination_discovery_fails_before_write(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    source.mkdir()
    (dest / "data_access").mkdir(parents=True)
    (source / "env.auto.tfvars").write_text('uc_tables = ["dev.s.current"]\n')
    original = 'sql_warehouse_id = "prod-wh"\n'
    (dest / "env.auto.tfvars").write_text(original)
    (dest / "data_access/discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["prod.s.removed"]\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    with pytest.raises(SystemExit):
        remap_env_config.main()
    assert (dest / "env.auto.tfvars").read_text() == original
    output = capsys.readouterr().out
    stale_path = dest / "data_access/discovered_uc_tables.auto.tfvars"
    assert f"Remove the stale tool-owned discovery file before promoting: {stale_path}" in output
    assert "Preserved destination" not in output
    assert "Reset destination" not in output


def test_preservation_messages_and_per_space_warehouse(tmp_path, monkeypatch, capsys):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    source.mkdir()
    dest.mkdir()
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "A", uc_tables = ["dev.s.t"] }]\n'
    )
    (dest / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "A", sql_warehouse_id = "space-wh" }]\n'
        'business_access_enabled = true\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    remap_env_config.main()
    output = capsys.readouterr().out
    assert "Preserved destination sql_warehouse_id" not in output
    assert "Preserved destination enable_auto_tagging" not in output
    assert "Reset destination business_access_enabled=true to false" in output
    config = hcl2.load((dest / "env.auto.tfvars").open())
    assert config["genie_spaces"][0]["sql_warehouse_id"] == "space-wh"


def test_space_title_naming_the_catalog_is_renamed_like_the_generated_config(
    tmp_path, monkeypatch
):
    """The env name must match the genie_space_configs key remap_hcl rewrites."""
    from scripts.remap_generated_config import remap_hcl

    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    (source / "generated").mkdir(parents=True)
    title = "Walkthrough (dev_cat.demo)"
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = [\n  { genie_space_id = "s1", uc_tables = ["dev_cat.demo.t"] },\n]\n'
    )
    (source / "generated" / "abac.auto.tfvars").write_text(
        f'genie_space_id_to_name = {{ s1 = "{title}" }}\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev_cat=prod_cat"
    ])
    remap_env_config.main()

    with (dest / "env.auto.tfvars").open() as handle:
        name = hcl2.load(handle)["genie_spaces"][0]["name"]
    generated_key = remap_hcl(f'"{title}" = {{}}', [("dev_cat", "prod_cat")])
    assert name == "Walkthrough (prod_cat.demo)"
    assert f'"{name}"' in generated_key


def _two_space_source(tmp_path, titles, catalog_map_src="dev_cat"):
    source = tmp_path / "dev"
    (source / "generated").mkdir(parents=True)
    entries = "".join(
        f'  {{ genie_space_id = "s{i}", uc_tables = ["{catalog_map_src}.demo.t{i}"] }},\n'
        for i in range(len(titles))
    )
    (source / "env.auto.tfvars").write_text(f"genie_spaces = [\n{entries}]\n")
    id_to_name = " ".join(f's{i} = "{t}"' for i, t in enumerate(titles))
    (source / "generated" / "abac.auto.tfvars").write_text(
        f"genie_space_id_to_name = {{ {id_to_name} }}\n"
    )
    return source


def test_space_names_that_collide_after_remap_fail_with_both_sources(
    tmp_path, monkeypatch, capsys
):
    source = _two_space_source(tmp_path, ["Agent (dev_cat.demo)", "Agent (prod_cat.demo)"])
    dest = tmp_path / "prod"
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev_cat=prod_cat"
    ])

    with pytest.raises(SystemExit):
        remap_env_config.main()

    out = capsys.readouterr().out
    assert "'Agent (dev_cat.demo)', 'Agent (prod_cat.demo)'" in out
    assert "'Agent (prod_cat.demo)'" in out
    assert not (dest / "env.auto.tfvars").exists()


def _deployed_state(dest, key):
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "terraform.tfstate").write_text(json.dumps({"resources": [
        {"module": "module.workspace", "type": "null_resource", "name": name,
         "instances": [{"index_key": key}]}
        for name in ("genie_space_create", "genie_space_config")
    ]}))
    (dest / f".genie_space_id_{key}").write_text("01live\n")


def test_rename_of_an_already_deployed_space_refuses_with_state_mv_guidance(
    tmp_path, monkeypatch, capsys
):
    """A changed key would destroy genie_space_create, which trashes the live agent."""
    title = "Walkthrough (dev_cat.demo)"
    source = _two_space_source(tmp_path, [title])
    dest = tmp_path / "prod"
    old_key = "walkthrough_dev_cat_demo"
    _deployed_state(dest, old_key)
    (dest / "env.auto.tfvars").write_text("business_access_enabled = true\n")
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev_cat=prod_cat"
    ])

    with pytest.raises(SystemExit):
        remap_env_config.main()

    out = capsys.readouterr().out
    assert "would trash the deployed agent" in out
    for name in ("genie_space_create", "genie_space_config"):
        assert (
            f"state-mv 'module.workspace.null_resource.{name}[\"{old_key}\"]' "
            f"'module.workspace.null_resource.{name}[\"walkthrough_prod_cat_demo\"]'"
        ) in out
    assert ".genie_space_id_walkthrough_prod_cat_demo" in out
    assert (dest / "env.auto.tfvars").read_text() == "business_access_enabled = true\n"


def test_deployed_space_whose_key_is_unchanged_promotes_normally(tmp_path, monkeypatch):
    title = "Walkthrough (dev_cat.demo)"
    source = _two_space_source(tmp_path, [title])
    dest = tmp_path / "prod"
    _deployed_state(dest, "walkthrough_prod_cat_demo")  # earlier promote already renamed it
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev_cat=prod_cat"
    ])

    remap_env_config.main()

    with (dest / "env.auto.tfvars").open() as handle:
        assert hcl2.load(handle)["genie_spaces"][0]["name"] == "Walkthrough (prod_cat.demo)"


def test_names_that_normalize_to_one_terraform_key_after_remap_fail(tmp_path, monkeypatch, capsys):
    """Distinct names can share a for_each key once lowercased and punctuation-collapsed."""
    source = _two_space_source(tmp_path, ["Agent (dev_cat.demo)", "agent prod_cat demo"])
    dest = tmp_path / "prod"
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev_cat=prod_cat"
    ])

    with pytest.raises(SystemExit):
        remap_env_config.main()

    out = capsys.readouterr().out
    assert "'Agent (dev_cat.demo)', 'agent prod_cat demo'" in out
    assert "Terraform key 'agent_prod_cat_demo'" in out
    assert not (dest / "env.auto.tfvars").exists()


def test_same_key_names_that_the_rename_does_not_merge_still_promote(tmp_path, monkeypatch):
    """Pre-existing same-key names are disambiguated by roots/workspace, as before."""
    source = _two_space_source(tmp_path, ["Pay Ops", "pay-ops"])
    dest = tmp_path / "prod"
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev_cat=prod_cat"
    ])

    remap_env_config.main()

    with (dest / "env.auto.tfvars").open() as handle:
        assert [s["name"] for s in hcl2.load(handle)["genie_spaces"]] == ["Pay Ops", "pay-ops"]
