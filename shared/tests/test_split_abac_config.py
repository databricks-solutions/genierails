"""Tests for splitting generated ABAC configuration into ownership layers."""

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from split_abac_config import build_account_config, build_data_access_config


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
