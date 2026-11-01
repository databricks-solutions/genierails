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
        "genie_space_legacy_mode": True,
    }
    workspace = build_workspace_config(generated)
    assert workspace["genie_space_configs"]["Payments"]["acl_groups"] == [
        "shared_group"
    ]
    assert build_data_access_config(generated)["genie_space_acl_groups"] == {
        "Payments": ["shared_group"],
    }


def test_legacy_single_space_conversion_preserves_explicit_empty_acl():
    generated = {
        "genie_space_title": "Nobody",
        "genie_acl_groups": [],
        "genie_space_legacy_mode": True,
    }
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


# ── deterministic gr_treatment values (live AWS Phase A, N3) ────────────────

LEGACY_ACCOUNT = {"tag_policies": [{"key": "gr_treatment", "values": ["account_last4", "redact"]}]}
DETERMINISTIC = {"tag_policies": [{"key": "gr_treatment", "values": [
    "card_last4__string__951e00ac", "unmapped_redact__string__0123abcd"]}]}


@pytest.mark.parametrize("existing", [None, LEGACY_ACCOUNT, DETERMINISTIC])
def test_split_merges_deterministic_treatment_values_into_the_account_tag_policy(existing):
    account = build_account_config(DETERMINISTIC, existing)

    values = account["tag_policies"][0]["values"]
    legacy = ["account_last4", "redact"] if existing is LEGACY_ACCOUNT else []
    assert values == legacy + ["card_last4__string__951e00ac", "unmapped_redact__string__0123abcd"]


def test_split_keeps_legacy_values_from_an_existing_account_policy_unchanged():
    account = build_account_config(LEGACY_ACCOUNT, DETERMINISTIC)

    assert account["tag_policies"][0]["values"] == [
        "card_last4__string__951e00ac", "unmapped_redact__string__0123abcd", "account_last4", "redact"]


@pytest.mark.parametrize("value", [
    "not_a_treatment",                   # legacy-shaped, not in the registry
    "not_a_treatment__string__951e00ac", # deterministic-shaped, no such library treatment
    "card_last4__string__951e00a",       # malformed generation
])
@pytest.mark.parametrize("side", ["new", "existing"])
def test_split_still_refuses_an_unknown_treatment_value(value, side):
    bad = {"tag_policies": [{"key": "gr_treatment", "values": [value]}]}
    new, existing = (bad, LEGACY_ACCOUNT) if side == "new" else (LEGACY_ACCOUNT, bad)

    with pytest.raises(ValueError, match="unknown canonical value"):
        build_account_config(new, existing)


def test_split_refuses_a_deterministic_shaped_value_on_another_registry_key():
    bad = {"tag_policies": [{"key": "pii_level", "values": ["card_last4__string__951e00ac"]}]}

    with pytest.raises(ValueError, match="unknown canonical value"):
        build_account_config(bad, None)


def test_account_only_writes_exactly_the_full_splits_account_file_and_nothing_else(tmp_path):
    """rehearse's and release's account step plans from it before the env's own split runs."""
    import subprocess

    script = Path(__file__).resolve().parent.parent / "scripts" / "split_abac_config.py"
    draft = tmp_path / "abac.auto.tfvars"
    draft.write_text('groups = {\n  analysts = { description = "A" }\n}\n'
                     'tag_policies = [\n  { key = "gr_treatment", description = "", values = ["card_last4__string__951e00ac"] },\n]\n'
                     'tag_assignments = [\n]\nfgac_policies = [\n]\n')
    existing = '# kept\ngroups = {\n  viewers = { description = "" }\n}\n'
    for name in ("only", "full"):
        (tmp_path / name).mkdir()
        (tmp_path / name / "account.tfvars").write_text(existing)
    only = subprocess.run([sys.executable, str(script), "--account-only", str(draft),
                           str(tmp_path / "only" / "account.tfvars")], text=True, capture_output=True)
    assert only.returncode == 0, only.stdout + only.stderr
    assert only.stdout == f"  Wrote shared account config: {tmp_path / 'only' / 'account.tfvars'}\n"
    full = subprocess.run([sys.executable, str(script), str(draft), str(tmp_path / "full" / "account.tfvars"),
                           str(tmp_path / "full" / "da.tfvars"), str(tmp_path / "full" / "ws.tfvars")],
                          text=True, capture_output=True)
    assert full.returncode == 0, full.stdout + full.stderr
    assert (tmp_path / "only" / "account.tfvars").read_bytes() == (tmp_path / "full" / "account.tfvars").read_bytes()
    assert sorted(p.name for p in (tmp_path / "only").iterdir()) == ["account.tfvars"]
    # Re-running it (the env's split under its lock, after the step) changes no byte.
    before = (tmp_path / "only" / "account.tfvars").read_bytes()
    subprocess.run([sys.executable, str(script), str(draft), str(tmp_path / "only" / "account.tfvars"),
                    str(tmp_path / "only" / "da.tfvars"), str(tmp_path / "only" / "ws.tfvars")], check=True,
                   capture_output=True)
    assert (tmp_path / "only" / "account.tfvars").read_bytes() == before
