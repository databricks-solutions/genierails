from pathlib import Path
import re


SHARED = Path(__file__).parents[1]


def test_gate_defaults_false_in_both_roots_and_modules():
    for path in (
        SHARED / "modules/data_access/variables.tf",
        SHARED / "modules/workspace/variables.tf",
        SHARED / "roots/data_access/main.tf",
        SHARED / "roots/workspace/main.tf",
    ):
        source = path.read_text()
        start = source.index('variable "business_access_enabled"')
        body = source[start : source.index("}\n", start) + 2]
        assert "default     = false" in body


def test_workspace_business_acls_are_gated_but_creation_is_not():
    source = (SHARED / "modules/workspace/main.tf").read_text()

    assert source.count(
        'if var.business_access_enabled && lookup(local.genie_space_groups, k, "") != ""'
    ) == 2

    create_start = source.index('resource "null_resource" "genie_space_create"')
    create_end = source.index(
        'resource "null_resource" "genie_space_config"', create_start
    )
    assert "business_access_enabled" not in source[create_start:create_end]


def test_roots_forward_the_same_gate_to_both_modules():
    assignment = r"business_access_enabled\s*=\s*var\.business_access_enabled"
    assert re.search(assignment, (SHARED / "roots/data_access/main.tf").read_text())
    assert re.search(assignment, (SHARED / "roots/workspace/main.tf").read_text())
