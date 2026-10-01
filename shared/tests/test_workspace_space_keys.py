"""Regression tests for collision-free workspace module for_each keys."""

import re
from pathlib import Path


WORKSPACE_MAIN = Path(__file__).parents[1] / "roots" / "workspace" / "main.tf"


def _terraform_key(spaces, index):
    """Model the merged_spaces key expression for concrete test inputs."""
    def base(space):
        return re.sub(r"[^a-z0-9]+", "_", space["name"].lower()).strip("_") \
            if space["name"] else space["genie_space_id"]

    space = spaces[index]
    key = base(space)
    if any(base(prior) == key for prior in spaces[:index]):
        return f'{key}--{space["genie_space_id"] or index}'
    return key


def test_same_sanitized_names_produce_distinct_stable_keys():
    spaces = [
        {"name": "Finance & HR", "genie_space_id": "space-a"},
        {"name": "Finance---HR", "genie_space_id": "space-b"},
        {"name": "Finance HR", "genie_space_id": ""},
    ]

    keys = [_terraform_key(spaces, index) for index in range(len(spaces))]

    assert keys == ["finance_hr", "finance_hr--space-b", "finance_hr--2"]
    assert len(keys) == len(set(keys))

    source = WORKSPACE_MAIN.read_text()
    assert "for idx, s in local.effective_spaces" in source
    assert "prior_idx < idx" in source
    assert 's.genie_space_id != "" ? s.genie_space_id : idx' in source
