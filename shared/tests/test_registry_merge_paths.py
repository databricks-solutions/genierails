import sys
from pathlib import Path

import hcl2
import pytest


SHARED_DIR = Path(__file__).parent.parent
SCRIPTS_DIR = SHARED_DIR / "scripts"
sys.path.insert(0, str(SHARED_DIR))
sys.path.insert(0, str(SCRIPTS_DIR))

from split_abac_config import merge_tag_policies  # noqa: E402
from merge_space_configs import merge_into_assembled  # noqa: E402


class TestSplitTagPolicies:
    def test_merge_tag_policies_normalizes_registry_aliases(self):
        merged = merge_tag_policies(
            [{"key": "aml_scope_deadbe", "values": ["public"]}],
            [{"key": "compliance_scope_deadbe", "values": ["compliance_restricted"]}],
        )

        assert merged == [
            {
                "key": "compliance_scope_deadbe",
                "description": "",
                "values": ["standard", "aml_restricted"],
            }
        ]


class TestMergeSpaceConfigs:
    def test_merge_into_assembled_raises_on_conflicting_canonical_assignment(
        self,
        tmp_path,
    ):
        generated_dir = tmp_path / "generated"
        generated_dir.mkdir()
        (generated_dir / "spaces" / "finance").mkdir(parents=True)
        (generated_dir / "masking_functions.sql").write_text("")
        (generated_dir / "spaces" / "finance" / "masking_functions.sql").write_text("")

        (generated_dir / "abac.auto.tfvars").write_text(
            """\
tag_policies = [
  {
    key    = "pci_level_deadbe"
    values = ["public", "masked_card_last4", "redacted_card_full"]
  },
]

tag_assignments = [
  {
    entity_type = "columns"
    entity_name = "main.fin.cards.card_number"
    tag_key     = "pci_level_deadbe"
    tag_value   = "masked_card_last4"
  },
]

fgac_policies = []
"""
        )
        (generated_dir / "spaces" / "finance" / "abac.auto.tfvars").write_text(
            """\
tag_policies = [
  {
    key    = "pci_level_deadbe"
    values = ["restricted_card"]
  },
]

tag_assignments = [
  {
    entity_type = "columns"
    entity_name = "main.fin.cards.card_number"
    tag_key     = "pci_level_deadbe"
    tag_value   = "restricted_card"
  },
]

fgac_policies = []
"""
        )

        # Per-space generate takes precedence — conflict is resolved, not raised
        merge_into_assembled(generated_dir, "finance")
        import hcl2, io
        result = hcl2.load(io.StringIO((generated_dir / "abac.auto.tfvars").read_text()))
        ta_list = result.get("tag_assignments", [])
        # The per-space value should win
        card_ta = [t for t in ta_list if "card_number" in t.get("entity_name", "")]
        assert len(card_ta) == 1
        assert card_ta[0]["tag_value"] == "redacted_card_full"  # canonical of "restricted_card"

    def test_merge_into_assembled_normalizes_alias_values(self, tmp_path):
        generated_dir = tmp_path / "generated"
        generated_dir.mkdir()
        (generated_dir / "spaces" / "finance").mkdir(parents=True)
        (generated_dir / "masking_functions.sql").write_text("")
        (generated_dir / "spaces" / "finance" / "masking_functions.sql").write_text("")

        (generated_dir / "abac.auto.tfvars").write_text(
            """\
tag_policies = [
  {
    key    = "compliance_scope_deadbe"
    values = ["standard"]
  },
]

tag_assignments = []
fgac_policies = []
"""
        )
        (generated_dir / "spaces" / "finance" / "abac.auto.tfvars").write_text(
            """\
tag_policies = [
  {
    key    = "aml_scope_deadbe"
    values = ["compliance_restricted"]
  },
]

tag_assignments = []
fgac_policies = []
"""
        )

        merge_into_assembled(generated_dir, "finance")

        with open(generated_dir / "abac.auto.tfvars") as f:
            merged_cfg = hcl2.load(f)

        assert merged_cfg["tag_policies"] == [
            {
                "key": "compliance_scope_deadbe",
                "values": ["aml_restricted", "standard"],
            }
        ]
