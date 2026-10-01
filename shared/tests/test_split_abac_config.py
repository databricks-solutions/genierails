"""Tests for splitting generated ABAC configuration into ownership layers."""

import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from split_abac_config import (
    build_account_config,
    build_data_access_config,
    build_workspace_config,
    load_generated_config,
)


def test_same_environment_split_keeps_tag_assignments():
    generated = {
        "groups": {"analysts": {"description": "Data analysts"}},
        "tag_policies": [{"key": "sensitivity", "values": ["pii"]}],
        "tag_assignments": [
            {
                "entity_type": "columns",
                "entity_name": "dev_fin.finance.accounts.ssn",
                "tag_key": "sensitivity",
                "tag_value": "pii",
            }
        ],
        "fgac_policies": [{"name": "mask_pii", "catalog": "dev_fin"}],
    }

    account = build_account_config(generated, None)
    data_access = build_data_access_config(generated)

    assert account["tag_policies"][0]["key"] == "sensitivity"
    assert account["tag_policies"][0]["values"] == ["pii"]
    assert data_access["fgac_policies"] == generated["fgac_policies"]
    assert data_access["groups"] == generated["groups"]
    assert data_access["tag_assignments"] == generated["tag_assignments"]


def test_data_access_split_derives_only_agent_acl_map_from_workspace_config():
    generated = {
        "genie_space_configs": {
            "Agent A": {"title": "A", "acl_groups": ["a_group"]},
            "Agent B": {"title": "B", "acl_groups": ["b_group", "shared_group"]},
        },
        "genie_space_derived_acl_groups": {
            "Agent A": ["derived_a"],
            "Agent B": ["derived_b"],
        },
        "discovered_uc_tables": ["cat.schema.table"],
        "discovered_table_agents": {"cat.schema.table": ["Agent A"]},
    }

    data_access = build_data_access_config(generated)

    assert data_access == {
        "genie_space_acl_groups": {
            "Agent A": ["derived_a"],
            "Agent B": ["derived_b"],
        }
    }


def test_multi_space_model_legacy_acl_is_not_authoritative():
    generated = {
        "genie_space_configs": {
            "payments": {"genie_acl_groups": ["payments_group"]},
            "hr": {"genie_acl_groups": ["hr_group"]},
        }
    }
    with pytest.raises(ValueError, match="no resolved ACL"):
        build_data_access_config(generated)


def test_legacy_single_space_conversion_preserves_authored_acl():
    generated = {
        "genie_space_title": "Payments",
        "genie_acl_groups": ["shared_group"],
    }
    workspace = build_workspace_config(generated)
    assert workspace["genie_space_configs"]["Payments"]["acl_groups"] == [
        "shared_group"
    ]
    assert build_data_access_config(generated)["genie_space_acl_groups"] == {
        "Payments": ["shared_group"],
    }


def test_legacy_single_space_conversion_preserves_explicit_empty_acl():
    generated = {"genie_space_title": "Nobody", "genie_acl_groups": []}
    assert build_workspace_config(generated)["genie_space_configs"]["Nobody"][
        "acl_groups"
    ] == []
    assert build_data_access_config(generated)["genie_space_acl_groups"] == {
        "Nobody": []
    }


def test_workspace_and_data_access_consumers_resolve_identical_acl_precedence():
    generated = {
        "genie_space_configs": {
            "canonical": {"acl_groups": []},
            "legacy": {"genie_acl_groups": ["legacy_group"]},
            "derived": {"title": "Derived"},
        },
        "genie_space_derived_acl_groups": {
            "canonical": ["must_not_widen"],
            "legacy": ["must_not_override"],
            "derived": ["derived_group"],
        },
    }

    data_access = build_data_access_config(generated)["genie_space_acl_groups"]
    workspace = build_workspace_config(generated)

    assert data_access == {
        "canonical": ["must_not_widen"],
        "legacy": ["must_not_override"],
        "derived": ["derived_group"],
    }
    assert {
        name: config["acl_groups"]
        for name, config in workspace["genie_space_configs"].items()
    } == data_access


def test_split_loads_tool_owned_acl_sidecar_for_both_consumers(tmp_path):
    source = tmp_path / "abac.auto.tfvars"
    source.write_text('genie_space_configs = { Pay = { title = "Pay" } }\n')
    (tmp_path / "genie_space_derived_acl_groups.auto.tfvars").write_text(
        'genie_space_derived_acl_groups = { Pay = ["pay_group"] }\n'
    )

    loaded = load_generated_config(source)
    data_access = build_data_access_config(loaded)["genie_space_acl_groups"]
    workspace = build_workspace_config(loaded)["genie_space_configs"]

    assert data_access == {"Pay": ["pay_group"]}
    assert workspace["Pay"]["acl_groups"] == data_access["Pay"]


def test_missing_sidecar_entry_fails_closed_for_both_consumers():
    generated = {"genie_space_configs": {"Pay": {"title": "Pay"}}}

    with pytest.raises(ValueError, match="Run `make generate`.*set acl_groups"):
        build_data_access_config(generated)
    with pytest.raises(ValueError, match="Run `make generate`.*set acl_groups"):
        build_workspace_config(generated)


@pytest.mark.parametrize("model_acl", [None, "${var.groups}", ["ok", 7], []])
def test_model_acl_is_ignored_and_missing_sidecar_fails_closed(model_acl):
    generated = {
        "genie_space_configs": {"Pay": {"acl_groups": model_acl}}
    }

    with pytest.raises(ValueError, match="no resolved ACL"):
        build_data_access_config(generated)
    with pytest.raises(ValueError, match="no resolved ACL"):
        build_workspace_config(generated)
