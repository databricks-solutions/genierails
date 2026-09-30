"""Unit tests for the autofix functions in generate_abac.py.

All tests run without any Databricks, LLM, or Terraform dependency.
Each test writes a minimal .tfvars snippet to a temp file, calls the
relevant autofix function, and asserts the expected outcome.
"""
import re
import sys
import inspect
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

# ---------------------------------------------------------------------------
# Make sure the project root is importable regardless of how pytest is invoked
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

import generate_abac

from generate_abac import (
    fix_hcl_syntax,
    autofix_canonical_tag_vocabulary,
    autofix_tag_policies,
    autofix_invalid_tag_values,
    autofix_undefined_tag_refs,
    autofix_missing_fgac_policies,
    autofix_fgac_policy_count,
    autofix_remove_bodyless_functions,
    bootstrap_per_space_dirs,
    extract_code_blocks,
    persist_discovered_uc_tables,
)
from tests.conftest import assert_valid_hcl


def test_discovered_table_writeback_aggregates_and_is_idempotent(tmp_path, capsys):
    path = tmp_path / "data_access" / "discovered_uc_tables.auto.tfvars"
    tables = ["main.sales.orders", "main.hr.people", "main.sales.orders"]

    added, present, disappeared = persist_discovered_uc_tables(path, tables)
    first = path.read_bytes()
    first_mtime = path.stat().st_mtime_ns
    assert added == ["main.sales.orders", "main.hr.people"]
    assert present == []
    assert disappeared == []
    assert assert_valid_hcl(path)["discovered_uc_tables"] == [
        "main.sales.orders", "main.hr.people"
    ]

    added, present, disappeared = persist_discovered_uc_tables(path, tables)
    assert path.read_bytes() == first
    assert path.stat().st_mtime_ns == first_mtime
    assert added == []
    assert present == ["main.sales.orders", "main.hr.people"]
    assert disappeared == []
    assert "unchanged:" in capsys.readouterr().out


def test_per_space_discovery_merges_without_wiping_other_agents(tmp_path):
    path = tmp_path / "discovered_uc_tables.auto.tfvars"
    persist_discovered_uc_tables(path, ["main.finance.transactions"])

    persist_discovered_uc_tables(
        path, ["main.support.tickets"], merge_existing=True
    )

    assert assert_valid_hcl(path)["discovered_uc_tables"] == [
        "main.finance.transactions", "main.support.tickets"
    ]


def test_full_discovery_reflects_current_state_and_reports_disappeared(tmp_path, capsys):
    path = tmp_path / "discovered_uc_tables.auto.tfvars"
    persist_discovered_uc_tables(path, ["main.old.table", "main.kept.table"])
    capsys.readouterr()

    _, present, disappeared = persist_discovered_uc_tables(path, ["main.kept.table"])

    assert present == ["main.kept.table"]
    assert disappeared == ["main.old.table"]
    assert "disappeared: main.old.table" in capsys.readouterr().out


def test_persist_parse_failure_aborts_without_overwriting_with_empty(tmp_path):
    path = tmp_path / "discovered_uc_tables.auto.tfvars"
    corrupt = 'discovered_uc_tables = ["main.kept.table"\n'
    path.write_text(corrupt)

    with pytest.raises(ValueError, match="Failed to parse previously discovered"):
        persist_discovered_uc_tables(path, [])

    assert path.read_text() == corrupt


def test_strict_environment_parse_failure_aborts(tmp_path):
    auth = tmp_path / "auth.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    auth.write_text("")
    env.write_text('genie_spaces = [\n')

    with pytest.raises(ValueError, match="Failed to parse environment"):
        generate_abac.load_auth_config(auth, env, strict_env=True)


def test_incomplete_discovery_folds_persisted_tables_into_masking_footprint(tmp_path):
    path = tmp_path / "discovered_uc_tables.auto.tfvars"
    persist_discovered_uc_tables(path, ["main.persisted.customers"])
    effective_tables = ["main.live.orders"]
    footprint = ["main.live.orders"]

    preserved = generate_abac.preserve_discovered_tables_for_incomplete_run(
        path, effective_tables, footprint
    )

    assert preserved == ["main.persisted.customers"]
    assert effective_tables == ["main.live.orders", "main.persisted.customers"]
    assert footprint == ["main.live.orders", "main.persisted.customers"]


def test_discovered_file_is_world_readable(tmp_path):
    path = tmp_path / "discovered_uc_tables.auto.tfvars"
    persist_discovered_uc_tables(path, ["main.sales.orders"])
    assert path.stat().st_mode & 0o777 == 0o644

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MINIMAL_VALID_HCL = """\
groups = {
  analysts = { description = "Analyst group" }
}

tag_policies = [
  {
    key    = "pii_level"
    values = ["public", "Limited_PII", "Full_PII"]
  },
]

tag_assignments = [
  {
    entity_type = "tables"
    entity_name = "main.hr.employees"
    tag_key     = "pii_level"
    tag_value   = "public"
  },
]

fgac_policies = []
"""


# ===========================================================================
#  fix_hcl_syntax
# ===========================================================================

class TestFixHclSyntax:

    def test_no_change_on_valid_hcl(self, tmp_tfvars):
        """Already-valid HCL should come back untouched (returns 0)."""
        path = tmp_tfvars(MINIMAL_VALID_HCL)
        repairs = fix_hcl_syntax(path)
        assert repairs == 0
        assert_valid_hcl(path)

    def test_adds_missing_comma_between_adjacent_objects(self, tmp_tfvars):
        """A missing comma between two list objects must be inserted."""
        bad = """\
tag_policies = [
  {
    key    = "pii_level"
    values = ["public", "Limited_PII"]
  }
  {
    key    = "phi_level"
    values = ["high"]
  }
]
"""
        path = tmp_tfvars(bad)
        repairs = fix_hcl_syntax(path)
        assert repairs >= 1
        assert_valid_hcl(path)
        # The comma must appear after the first closing brace
        text = path.read_text()
        assert "},\n  {" in text or "},\n{" in text

    def test_adds_missing_comma_with_blank_lines_between_objects(self, tmp_tfvars):
        """Blank lines between } and { should still trigger the comma fix."""
        bad = """\
tag_policies = [
  {
    key    = "pii_level"
    values = ["public"]
  }

  {
    key    = "phi_level"
    values = ["high"]
  }
]
"""
        path = tmp_tfvars(bad)
        repairs = fix_hcl_syntax(path)
        assert repairs >= 1
        assert_valid_hcl(path)

    def test_adds_missing_comma_with_comment_between_objects(self, tmp_tfvars):
        """Comment lines between } and { should still trigger the comma fix."""
        bad = """\
tag_policies = [
  {
    key    = "pii_level"
    values = ["public"]
  }
  # next policy
  {
    key    = "phi_level"
    values = ["high"]
  }
]
"""
        path = tmp_tfvars(bad)
        repairs = fix_hcl_syntax(path)
        assert repairs >= 1
        assert_valid_hcl(path)

    def test_does_not_duplicate_existing_comma(self, tmp_tfvars):
        """Objects that already have a trailing comma must not get a double comma."""
        good = """\
tag_policies = [
  {
    key    = "pii_level"
    values = ["public"]
  },
  {
    key    = "phi_level"
    values = ["high"]
  },
]
"""
        path = tmp_tfvars(good)
        repairs = fix_hcl_syntax(path)
        assert repairs == 0
        text = path.read_text()
        assert "}," in text
        assert "},," not in text

    def test_converts_object_style_values_to_strings(self, tmp_tfvars):
        """values = [{name = "v"}] → values = ["v"]."""
        bad = """\
tag_policies = [
  {
    key    = "pii_level"
    values = [{name = "public"}, {name = "Limited_PII"}]
  },
]
"""
        path = tmp_tfvars(bad)
        repairs = fix_hcl_syntax(path)
        assert repairs >= 1
        text = path.read_text()
        assert 'values = ["public", "Limited_PII"]' in text

    def test_multiple_fixes_in_one_pass(self, tmp_tfvars):
        """Both a missing comma and object-style values can be fixed together."""
        bad = """\
tag_policies = [
  {
    key    = "pii_level"
    values = [{name = "public"}]
  }
  {
    key    = "phi_level"
    values = ["high"]
  }
]
"""
        path = tmp_tfvars(bad)
        repairs = fix_hcl_syntax(path)
        assert repairs >= 2
        assert_valid_hcl(path)


# ===========================================================================
#  autofix_tag_policies
# ===========================================================================

class TestAutofixTagPolicies:

    def _base_hcl(self, allowed_values: str, used_value: str) -> str:
        return f"""\
tag_policies = [
  {{
    key    = "pii_level"
    values = [{allowed_values}]
  }},
]

tag_assignments = [
  {{
    entity_type = "tables"
    entity_name = "main.hr.employees"
    tag_key     = "pii_level"
    tag_value   = "{used_value}"
  }},
]

fgac_policies = []
"""

    def test_no_change_when_values_already_allowed(self, tmp_tfvars):
        hcl = self._base_hcl('"public", "Limited_PII"', "Limited_PII")
        path = tmp_tfvars(hcl)
        count = autofix_tag_policies(path)
        assert count == 0

    def test_adds_missing_value_simple(self, tmp_tfvars):
        hcl = self._base_hcl('"public"', "Limited_PII")
        path = tmp_tfvars(hcl)
        count = autofix_tag_policies(path)
        assert count == 1
        text = path.read_text()
        assert '"Limited_PII"' in text

    def test_adds_missing_value_with_tight_spacing(self, tmp_tfvars):
        """Works even when the original values list has no spaces: ["A","B"]."""
        hcl = self._base_hcl('"public","Limited_PII"', "Full_PII")
        path = tmp_tfvars(hcl)
        count = autofix_tag_policies(path)
        assert count == 1
        text = path.read_text()
        assert '"Full_PII"' in text

    def test_does_not_add_value_from_fgac_condition(self, tmp_tfvars):
        """autofix_tag_policies should NOT promote values from FGAC conditions."""
        hcl = """\
tag_policies = [
  {
    key    = "pii_level"
    values = ["public"]
  },
]

tag_assignments = []

fgac_policies = [
  {
    name            = "mask_pii"
    policy_type     = "POLICY_TYPE_COLUMN_MASK"
    catalog         = "main"
    to_principals   = ["account users"]
    match_condition = "hasTagValue('pii_level', 'Full_PII')"
    match_alias     = "mask_pii"
    function_name   = "mask_pii_partial"
    function_catalog = "main"
    function_schema  = "governance"
  },
]
"""
        path = tmp_tfvars(hcl)
        count = autofix_tag_policies(path)
        assert count == 0
        assert '"Full_PII"' not in path.read_text()

    def test_adds_multiple_missing_values(self, tmp_tfvars):
        hcl = self._base_hcl('"public"', "Full_PII")
        # Inject a second assignment with another missing value
        hcl += """\
# extra assignment outside base block — same key, different missing value
"""
        path = tmp_tfvars(hcl)
        # patch the file to add a second assignment with a different missing value
        text = path.read_text().replace(
            'fgac_policies = []',
            'fgac_policies = []\n# marker'
        )
        # Add second assignment inline
        text = text.replace(
            'fgac_policies = []\n# marker',
            """\
fgac_policies = []
""",
        )
        # Simpler: just put two tag_assignments with two different missing values
        hcl2_content = """\
tag_policies = [
  {
    key    = "pii_level"
    values = ["public"]
  },
]

tag_assignments = [
  {
    entity_type = "tables"
    entity_name = "main.hr.employees"
    tag_key     = "pii_level"
    tag_value   = "Limited_PII"
  },
  {
    entity_type = "tables"
    entity_name = "main.hr.salaries"
    tag_key     = "pii_level"
    tag_value   = "Full_PII"
  },
]

fgac_policies = []
"""
        path2 = tmp_tfvars(hcl2_content)
        count = autofix_tag_policies(path2)
        assert count == 2
        text2 = path2.read_text()
        assert '"Limited_PII"' in text2
        assert '"Full_PII"' in text2


class TestAutofixCanonicalTagVocabulary:

    def test_normalizes_and_merges_duplicate_tag_policies(self, tmp_tfvars):
        hcl = """\
tag_policies = [
  {
    key    = "pci_level_deadbe"
    values = ["public", "masked_card", "restricted_card", "restricted_cvv"]
  },
  {
    key    = "pci_level_deadbe"
    values = ["public", "masked_card_last4", "redacted_card_full", "redacted_cvv"]
  },
  {
    key    = "aml_scope_deadbe"
    values = ["public", "aml_restricted"]
  },
  {
    key    = "financial_level_deadbe"
    values = ["public", "masked_amount"]
  },
]

tag_assignments = [
  {
    entity_type = "columns"
    entity_name = "main.fin.credit_cards.card_number"
    tag_key     = "pci_level_deadbe"
    tag_value   = "restricted_card"
  },
  {
    entity_type = "tables"
    entity_name = "main.fin.transactions"
    tag_key     = "aml_scope_deadbe"
    tag_value   = "aml_restricted"
  },
]

fgac_policies = [
  {
    name            = "mask_pci"
    policy_type     = "POLICY_TYPE_COLUMN_MASK"
    catalog         = "main"
    to_principals   = ["account users"]
    match_condition = "hasTagValue('pci_level_deadbe', 'masked_card_full')"
    match_alias     = "mask_pci"
    function_name   = "mask_credit_card_full"
    function_catalog = "main"
    function_schema  = "governance"
  },
  {
    name           = "filter_aml"
    policy_type    = "POLICY_TYPE_ROW_FILTER"
    catalog        = "main"
    to_principals  = ["account users"]
    when_condition = "hasTagValue('aml_scope_deadbe', 'aml_restricted')"
    function_name  = "filter_compliance_only"
    function_catalog = "main"
    function_schema  = "governance"
  },
]
"""
        path = tmp_tfvars(hcl)
        count = autofix_canonical_tag_vocabulary(path)
        assert count > 0
        cfg = assert_valid_hcl(path)

        tag_policies = cfg.get("tag_policies", [])
        pci_policies = [tp for tp in tag_policies if tp.get("key") == "pci_level_deadbe"]
        assert len(pci_policies) == 1
        assert pci_policies[0]["values"] == [
            "public",
            "masked_card_last4",
            "redacted_card_full",
            "redacted_cvv",
        ]

        compliance_policies = [
            tp for tp in tag_policies if tp.get("key") == "compliance_scope_deadbe"
        ]
        assert len(compliance_policies) == 1
        assert compliance_policies[0]["values"] == ["standard", "aml_restricted"]

        financial_policies = [
            tp for tp in tag_policies
            if tp.get("key") == "financial_sensitivity_deadbe"
        ]
        assert len(financial_policies) == 1
        assert financial_policies[0]["values"] == ["public", "rounded_amounts"]

        assignments = cfg.get("tag_assignments", [])
        assert assignments[0]["tag_value"] == "redacted_card_full"
        assert assignments[1]["tag_key"] == "compliance_scope_deadbe"

    def test_normalizes_fgac_conditions(self, tmp_tfvars):
        hcl = """\
tag_policies = [
  {
    key    = "pci_level_deadbe"
    values = ["public", "redacted_card_full", "redacted_cvv"]
  },
  {
    key    = "compliance_scope_deadbe"
    values = ["standard", "aml_restricted"]
  },
]

tag_assignments = []

fgac_policies = [
  {
    name            = "mask_pci"
    policy_type     = "POLICY_TYPE_COLUMN_MASK"
    catalog         = "main"
    to_principals   = ["account users"]
    match_condition = "hasTagValue('pci_level_deadbe', 'pci_full_mask')"
    when_condition  = "hasTag('aml_scope_deadbe')"
    match_alias     = "mask_pci"
    function_name   = "mask_credit_card_full"
    function_catalog = "main"
    function_schema  = "governance"
  },
]
"""
        path = tmp_tfvars(hcl)
        count = autofix_canonical_tag_vocabulary(path)
        assert count > 0
        text = path.read_text()
        assert "pci_full_mask" not in text
        assert "aml_scope_deadbe" not in text
        assert "hasTagValue('pci_level_deadbe', 'redacted_card_full')" in text
        assert "hasTag('compliance_scope_deadbe')" in text

    def test_dedupes_identical_tag_assignments_after_normalization(self, tmp_tfvars):
        hcl = """\
tag_policies = [
  {
    key    = "aml_scope_deadbe"
    values = ["public", "aml_restricted"]
  },
]

tag_assignments = [
  {
    entity_type = "tables"
    entity_name = "main.fin.transactions"
    tag_key     = "aml_scope_deadbe"
    tag_value   = "aml_restricted"
  },
  {
    entity_type = "tables"
    entity_name = "main.fin.transactions"
    tag_key     = "compliance_scope_deadbe"
    tag_value   = "aml_restricted"
  },
]

fgac_policies = []
"""
        path = tmp_tfvars(hcl)
        count = autofix_canonical_tag_vocabulary(path)
        assert count > 0
        cfg = assert_valid_hcl(path)
        assert cfg["tag_assignments"] == [
            {
                "entity_type": "tables",
                "entity_name": "main.fin.transactions",
                "tag_key": "compliance_scope_deadbe",
                "tag_value": "aml_restricted",
            }
        ]

    def test_removes_unknown_canonical_tag_policy_values(self, tmp_tfvars):
        hcl = """\
tag_policies = [
  {
    key    = "pii_level_deadbe"
    values = ["public", "masked_email", "masked_loyalty", "masked_member_id"]
  },
]

tag_assignments = []

fgac_policies = []
"""
        path = tmp_tfvars(hcl)
        count = autofix_canonical_tag_vocabulary(path)
        assert count > 0
        cfg = assert_valid_hcl(path)
        assert cfg["tag_policies"] == [{"key": "pii_level_deadbe", "values": ["public", "masked_email"]}]


class TestBootstrapPerSpaceDirs:

    def test_bootstraps_all_configured_spaces_when_parser_shape_is_partial(self, tmp_path):
        out_dir = tmp_path / "generated"
        out_dir.mkdir()
        (out_dir / "abac.auto.tfvars").write_text(
            """\
genie_space_configs = {
  "Finance Analytics" = {
    title = "Finance Analytics"
  }
}
"""
        )
        auth_cfg = {
            "genie_spaces": [
                {"name": "Finance Analytics", "config": {"title": "Finance Analytics"}},
                {"name": "Clinical Analytics", "config": {"title": "Clinical Analytics"}},
            ]
        }

        bootstrap_per_space_dirs(out_dir, auth_cfg, "")

        assert (out_dir / "spaces" / "finance_analytics" / "abac.auto.tfvars").exists()
        assert (out_dir / "spaces" / "clinical_analytics" / "abac.auto.tfvars").exists()


class TestGenieFetchFailureIsolation:

    def test_patch_fallback_failure_does_not_block_later_spaces(self, monkeypatch, capsys):
        """One broken fallback is warning-only and the next agent is still fetched."""
        calls = []

        class FakeApiClient:
            def do(self, method, path, **kwargs):
                space_id = path.rsplit("/", 1)[-1]
                calls.append((method, space_id))
                if space_id == "broken":
                    if method == "GET":
                        raise RuntimeError("Partner Powered AI is unavailable")
                    raise RuntimeError("PATCH endpoint failed")
                return {
                    "title": "Healthy agent",
                    "serialized_space": json.dumps({}),
                }

        class FakeWorkspaceClient:
            def __init__(self, **kwargs):
                self.api_client = FakeApiClient()

        monkeypatch.setattr(generate_abac, "configure_databricks_env", lambda _: None)
        monkeypatch.setattr(
            sys.modules["databricks.sdk"], "WorkspaceClient", FakeWorkspaceClient
        )

        spaces = ["broken", "healthy"]
        governed = []
        for space_id in spaces:
            tables, config, title, complete = generate_abac.fetch_tables_from_genie_space(
                space_id, {}, quick_check_only=True
            )
            if complete and title:
                governed.append(space_id)

        assert governed == ["healthy"]
        assert ("PATCH", "broken") in calls
        assert ("GET", "healthy") in calls
        assert "WARNING: Could not reach Genie agent broken via PATCH fallback" in capsys.readouterr().out

    def test_title_without_serialized_space_is_incomplete(self, monkeypatch):
        class FakeApiClient:
            def do(self, method, path, **kwargs):
                return {"title": "Still provisioning", "serialized_space": ""}

        class FakeWorkspaceClient:
            def __init__(self, **kwargs):
                self.api_client = FakeApiClient()

        monkeypatch.setattr(generate_abac, "configure_databricks_env", lambda _: None)
        monkeypatch.setattr("time.sleep", lambda _: None)
        monkeypatch.setattr(
            sys.modules["databricks.sdk"], "WorkspaceClient", FakeWorkspaceClient
        )

        tables, config, title, complete = generate_abac.fetch_tables_from_genie_space(
            "provisioning", {}
        )

        assert (tables, config, title) == ([], {}, "Still provisioning")
        assert complete is False


class TestDatabricksModelCompatibility:

    @staticmethod
    def _install_fake_client(monkeypatch, content):
        calls = []

        class FakeServingEndpoints:
            def query(self, **kwargs):
                calls.append(kwargs)
                message = SimpleNamespace(content=content)
                return SimpleNamespace(choices=[SimpleNamespace(message=message)])

        class FakeWorkspaceClient:
            def __init__(self, **kwargs):
                self.serving_endpoints = FakeServingEndpoints()

        monkeypatch.setattr("databricks.sdk.WorkspaceClient", FakeWorkspaceClient)
        monkeypatch.setattr("databricks.sdk.config.Config", lambda **kwargs: object())
        return calls

    def test_claude_5_omits_temperature_and_normalizes_content_blocks(self, monkeypatch):
        calls = self._install_fake_client(
            monkeypatch,
            [
                {"type": "reasoning", "summary": []},
                {"type": "text", "text": "```sql\nSELECT 1;\n```"},
                SimpleNamespace(type="text", text="```hcl\ngroups = {}\n```"),
            ],
        )

        result = generate_abac.call_databricks("prompt", "databricks-claude-sonnet-5-5")

        assert "temperature" not in calls[0]
        assert result == "```sql\nSELECT 1;\n```\n```hcl\ngroups = {}\n```"

    def test_claude_4_keeps_deterministic_temperature_and_string_content(self, monkeypatch):
        calls = self._install_fake_client(monkeypatch, "plain response")

        result = generate_abac.call_databricks("prompt", "databricks-claude-sonnet-4-6")

        assert calls[0]["temperature"] == 0
        assert result == "plain response"

    def test_structured_response_without_text_fails_clearly(self, monkeypatch):
        self._install_fake_client(
            monkeypatch, [{"type": "reasoning", "summary": []}]
        )

        with pytest.raises(ValueError, match="returned no text content"):
            generate_abac.call_databricks("prompt", "databricks-claude-sonnet-5")


class TestExtractCodeBlocks:

    def test_extracts_hcl_from_nonstandard_label(self):
        sql, hcl = extract_code_blocks(
            """\
Here is the output.
```sql
CREATE OR REPLACE FUNCTION mask_x(input STRING) RETURNS STRING RETURN input;
```
```tfvars
groups = {}
tag_policies = []
tag_assignments = []
```
"""
        )
        assert sql is not None
        assert hcl is not None
        assert "tag_policies" in hcl

    def test_falls_back_to_plain_hcl_when_fence_missing(self):
        sql, hcl = extract_code_blocks(
            """\
```sql
CREATE OR REPLACE FUNCTION mask_x(input STRING) RETURNS STRING RETURN input;
```

groups = {}
tag_policies = []
tag_assignments = []
fgac_policies = []
"""
        )
        assert sql is not None
        assert hcl is not None
        assert "fgac_policies" in hcl

    def test_uses_rest_of_response_for_unclosed_hcl_fence(self):
        sql, hcl = extract_code_blocks(
            """\
```sql
CREATE OR REPLACE FUNCTION mask_x(input STRING) RETURNS STRING RETURN input;
```
```hcl
groups = {}
tag_policies = []
genie_instructions = "When asked about 'transactions', default to completed."
"""
        )
        assert sql is not None
        assert hcl is not None
        assert "genie_instructions" in hcl


# ===========================================================================
#  autofix_invalid_tag_values
# ===========================================================================

class TestAutofixInvalidTagValues:

    def _hcl_with_bad_assignment(self, bad_value: str) -> str:
        return f"""\
tag_policies = [
  {{
    key    = "pii_level"
    values = ["public", "Limited_PII", "Full_PII"]
  }},
]

tag_assignments = [
  {{
    entity_type = "tables"
    entity_name = "main.hr.employees"
    tag_key     = "pii_level"
    tag_value   = "{bad_value}"
  }},
]

fgac_policies = []
"""

    def test_no_change_when_value_is_valid(self, tmp_tfvars):
        path = tmp_tfvars(self._hcl_with_bad_assignment("Limited_PII"))
        count = autofix_invalid_tag_values(path)
        assert count == 0

    def test_removes_assignment_with_invalid_value(self, tmp_tfvars):
        path = tmp_tfvars(self._hcl_with_bad_assignment("masked_phone"))
        count = autofix_invalid_tag_values(path)
        assert count == 1
        cfg = assert_valid_hcl(path)
        assignments = cfg.get("tag_assignments", [])
        assert all(a.get("tag_value") != "masked_phone" for a in assignments)

    def test_result_is_valid_hcl(self, tmp_tfvars):
        path = tmp_tfvars(self._hcl_with_bad_assignment("not_a_real_value"))
        autofix_invalid_tag_values(path)
        assert_valid_hcl(path)

    def test_removes_only_the_bad_assignment_keeps_good(self, tmp_tfvars):
        """Bad assignment is removed; a good assignment for a different key is kept."""
        hcl = """\
tag_policies = [
  {
    key    = "pii_level"
    values = ["public", "Limited_PII"]
  },
  {
    key    = "phi_level"
    values = ["high"]
  },
]

tag_assignments = [
  {
    entity_type = "tables"
    entity_name = "main.hr.salaries"
    tag_key     = "phi_level"
    tag_value   = "high"
  },
  {
    entity_type = "tables"
    entity_name = "main.hr.employees"
    tag_key     = "pii_level"
    tag_value   = "bad_value"
  },
]

fgac_policies = []
"""
        path = tmp_tfvars(hcl)
        count = autofix_invalid_tag_values(path)
        assert count == 1
        cfg = assert_valid_hcl(path)
        assignments = cfg.get("tag_assignments", [])
        assert len(assignments) == 1
        assert assignments[0]["tag_value"] == "high"


# ===========================================================================
#  autofix_undefined_tag_refs
# ===========================================================================

class TestAutofixUndefinedTagRefs:

    def test_no_change_when_all_refs_valid(self, tmp_tfvars):
        path = tmp_tfvars(MINIMAL_VALID_HCL)
        count = autofix_undefined_tag_refs(path)
        assert count == 0

    def test_removes_assignment_with_undefined_tag_key(self, tmp_tfvars):
        hcl = """\
tag_policies = [
  {
    key    = "pii_level"
    values = ["public"]
  },
]

tag_assignments = [
  {
    entity_type = "tables"
    entity_name = "main.hr.employees"
    tag_key     = "pii_level"
    tag_value   = "public"
  },
  {
    entity_type = "tables"
    entity_name = "main.hr.salaries"
    tag_key     = "undefined_key"
    tag_value   = "some_value"
  },
]

fgac_policies = []
"""
        path = tmp_tfvars(hcl)
        count = autofix_undefined_tag_refs(path)
        assert count >= 1
        cfg = assert_valid_hcl(path)
        assignments = cfg.get("tag_assignments", [])
        assert all(a.get("tag_key") != "undefined_key" for a in assignments)

    def test_removes_fgac_policy_with_undefined_tag_key(self, tmp_tfvars):
        hcl = """\
tag_policies = [
  {
    key    = "pii_level"
    values = ["public", "Limited_PII"]
  },
]

tag_assignments = []

fgac_policies = [
  {
    name            = "mask_with_bad_key"
    policy_type     = "POLICY_TYPE_COLUMN_MASK"
    catalog         = "main"
    to_principals   = ["account users"]
    match_condition = "hasTagValue('ghost_key', 'some_val')"
    match_alias     = "mask"
    function_name   = "mask_pii_partial"
    function_catalog = "main"
    function_schema  = "governance"
  },
]
"""
        path = tmp_tfvars(hcl)
        count = autofix_undefined_tag_refs(path)
        assert count >= 1
        cfg = assert_valid_hcl(path)
        policies = cfg.get("fgac_policies", [])
        assert all(p.get("name") != "mask_with_bad_key" for p in policies)

    def test_result_is_valid_hcl_after_removal(self, tmp_tfvars):
        hcl = """\
tag_policies = [
  {
    key    = "pii_level"
    values = ["public"]
  },
]

tag_assignments = [
  {
    entity_type = "tables"
    entity_name = "main.hr.t1"
    tag_key     = "undefined_key"
    tag_value   = "x"
  },
  {
    entity_type = "tables"
    entity_name = "main.hr.t2"
    tag_key     = "pii_level"
    tag_value   = "public"
  },
]

fgac_policies = []
"""
        path = tmp_tfvars(hcl)
        autofix_undefined_tag_refs(path)
        assert_valid_hcl(path)


# ===========================================================================
#  autofix_missing_fgac_policies
# ===========================================================================

class TestAutofixMissingFgacPolicies:

    def test_no_op_when_assignments_covered(self, tmp_tfvars):
        """If every non-public assignment is already covered, nothing changes."""
        hcl = """\
tag_policies = [
  {
    key    = "pii_level"
    values = ["public", "Limited_PII"]
  },
]

tag_assignments = [
  {
    entity_type = "columns"
    entity_name = "main.hr.employees.email"
    tag_key     = "pii_level"
    tag_value   = "public"
  },
]

fgac_policies = []
"""
        path = tmp_tfvars(hcl)
        count = autofix_missing_fgac_policies(path)
        # public values don't need coverage → no policies added
        assert count == 0

    def test_adds_policy_for_uncovered_column(self, tmp_tfvars):
        """A Limited_PII column assignment with no covering policy should get one."""
        hcl = """\
tag_policies = [
  {
    key    = "pii_level"
    values = ["public", "Limited_PII"]
  },
]

tag_assignments = [
  {
    entity_type = "columns"
    entity_name = "main.hr.employees.email"
    tag_key     = "pii_level"
    tag_value   = "Limited_PII"
  },
]

fgac_policies = []
"""
        path = tmp_tfvars(hcl)
        count = autofix_missing_fgac_policies(path)
        assert count >= 1
        assert_valid_hcl(path)
        text = path.read_text()
        # A COLUMN_MASK policy referencing the email column's catalog should appear
        assert "POLICY_TYPE_COLUMN_MASK" in text


# ===========================================================================
#  autofix_fgac_policy_count  (_remove_block correctness)
# ===========================================================================

class TestAutofixFgacPolicyCount:

    def _make_policy(self, name: str, condition: str = "hasTagValue('pii_level','Full_PII')") -> str:
        return f"""\
  {{
    name            = "{name}"
    policy_type     = "POLICY_TYPE_COLUMN_MASK"
    catalog         = "main"
    to_principals   = ["account users"]
    match_condition = "{condition}"
    match_alias     = "{name}"
    function_name   = "mask_pii_partial"
    function_catalog = "main"
    function_schema  = "governance"
  }},"""

    def _make_hcl(self, policy_names: list[str], limit: int = 5) -> str:
        policies_str = "\n".join(self._make_policy(n) for n in policy_names)
        return f"""\
tag_policies = [
  {{
    key    = "pii_level"
    values = ["public", "Limited_PII", "Full_PII"]
  }},
]

tag_assignments = []

fgac_policies = [
{policies_str}
]
"""

    def test_no_change_when_under_limit(self, tmp_tfvars, monkeypatch):
        monkeypatch.setattr(generate_abac, "_FGAC_PER_CATALOG_LIMIT", 5)
        path = tmp_tfvars(self._make_hcl(["p1", "p2", "p3"]))
        count = autofix_fgac_policy_count(path)
        assert count == 0
        assert_valid_hcl(path)

    def test_errors_without_removing_excess_policies(self, tmp_tfvars, monkeypatch):
        import generate_abac
        monkeypatch.setattr(generate_abac, "_FGAC_PER_CATALOG_LIMIT", 2)
        path = tmp_tfvars(self._make_hcl(["p1", "p2", "p3", "p4"]))
        original = path.read_text()
        with pytest.raises(ValueError, match="no policies were dropped") as exc:
            autofix_fgac_policy_count(path)
        assert "p1, p2, p3, p4" in str(exc.value)
        assert path.read_text() == original
        cfg = assert_valid_hcl(path)
        assert len(cfg.get("fgac_policies", [])) == 4

    def test_cap_error_leaves_valid_hcl(self, tmp_tfvars, monkeypatch):
        import generate_abac
        monkeypatch.setattr(generate_abac, "_FGAC_PER_CATALOG_LIMIT", 1)
        path = tmp_tfvars(self._make_hcl(["alpha", "beta", "gamma"]))
        with pytest.raises(ValueError):
            autofix_fgac_policy_count(path)
        assert_valid_hcl(path)

    def test_cap_error_does_not_rewrite_content(self, tmp_tfvars, monkeypatch):
        import generate_abac
        monkeypatch.setattr(generate_abac, "_FGAC_PER_CATALOG_LIMIT", 1)
        path = tmp_tfvars(self._make_hcl(["x", "y"]))
        original = path.read_text()
        with pytest.raises(ValueError):
            autofix_fgac_policy_count(path)
        text = path.read_text()
        assert text == original
        assert_valid_hcl(path)

    def test_multiple_catalogs_each_respect_limit(self, tmp_tfvars, monkeypatch):
        """Policies for different catalogs are counted separately."""
        import generate_abac
        monkeypatch.setattr(generate_abac, "_FGAC_PER_CATALOG_LIMIT", 2)

        def _pol(name: str, catalog: str) -> str:
            return f"""\
  {{
    name            = "{name}"
    policy_type     = "POLICY_TYPE_COLUMN_MASK"
    catalog         = "{catalog}"
    to_principals   = ["account users"]
    match_condition = "hasTagValue('pii_level','Full_PII')"
    match_alias     = "{name}"
    function_name   = "mask_pii_partial"
    function_catalog = "{catalog}"
    function_schema  = "governance"
  }},"""

        hcl = """\
tag_policies = [
  {
    key    = "pii_level"
    values = ["public", "Full_PII"]
  },
]

tag_assignments = []

fgac_policies = [
"""
        for i in range(3):
            hcl += _pol(f"cat_a_p{i}", "cat_a")
        for i in range(3):
            hcl += _pol(f"cat_b_p{i}", "cat_b")
        hcl += "]\n"

        path = tmp_tfvars(hcl)
        with pytest.raises(ValueError) as exc:
            autofix_fgac_policy_count(path)
        assert "catalog 'cat_a'" in str(exc.value)
        assert "catalog 'cat_b'" in str(exc.value)
        assert_valid_hcl(path)


# ===========================================================================
#  autofix_remove_bodyless_functions
# ===========================================================================

class TestAutofixRemoveBodylessFunctions:

    def test_removes_function_with_no_body(self, tmp_sql):
        sql = """\
USE CATALOG dev_fin;
USE SCHEMA finance;

CREATE OR REPLACE FUNCTION mask_email(val STRING) RETURNS STRING
RETURN CONCAT('***@', SPLIT(val, '@')[1]);

CREATE OR REPLACE FUNCTION filter_aml_compliance(aml_flag BOOLEAN);

CREATE OR REPLACE FUNCTION filter_aml_compliance_stub() RETURNS BOOLEAN
RETURN TRUE;
"""
        path = tmp_sql(sql)
        n = autofix_remove_bodyless_functions(path)
        assert n == 1
        out = path.read_text()
        assert "filter_aml_compliance(aml_flag BOOLEAN)" not in out
        assert "mask_email" in out
        assert "filter_aml_compliance_stub" in out

    def test_returns_zero_when_all_functions_have_bodies(self, tmp_sql):
        sql = """\
CREATE OR REPLACE FUNCTION mask_email(val STRING) RETURNS STRING
RETURN CONCAT('***@', SPLIT(val, '@')[1]);

CREATE OR REPLACE FUNCTION filter_admins() RETURNS BOOLEAN
RETURN is_account_group_member('admins');
"""
        path = tmp_sql(sql)
        before = path.read_text()
        n = autofix_remove_bodyless_functions(path)
        assert n == 0
        assert path.read_text() == before

    def test_does_not_confuse_returns_with_return(self, tmp_sql):
        # `RETURNS BOOLEAN` (type declaration) must not count as a body —
        # only standalone `RETURN` does.
        sql = """\
CREATE OR REPLACE FUNCTION foo(x INT) RETURNS BOOLEAN;
"""
        path = tmp_sql(sql)
        n = autofix_remove_bodyless_functions(path)
        assert n == 1
        assert "foo" not in path.read_text()

    def test_ignores_return_inside_comment(self, tmp_sql):
        # Stripping comments before the body check ensures a `RETURN` token
        # buried in a comment doesn't keep an actually-bodyless function.
        sql = """\
CREATE OR REPLACE FUNCTION foo(x INT)
-- TODO: add RETURN clause here
;

CREATE OR REPLACE FUNCTION bar(y INT) RETURNS INT
RETURN y * 2;
"""
        path = tmp_sql(sql)
        n = autofix_remove_bodyless_functions(path)
        assert n == 1
        assert "FUNCTION foo" not in path.read_text()
        assert "FUNCTION bar" in path.read_text()

    def test_no_change_when_sql_path_missing(self, tmp_path):
        n = autofix_remove_bodyless_functions(tmp_path / "does_not_exist.sql")
        assert n == 0

    def test_handles_multiple_bodyless_in_one_file(self, tmp_sql):
        sql = """\
CREATE OR REPLACE FUNCTION a(x INT);
CREATE OR REPLACE FUNCTION b(y INT) RETURNS INT RETURN y;
CREATE OR REPLACE FUNCTION c(z INT);
"""
        path = tmp_sql(sql)
        n = autofix_remove_bodyless_functions(path)
        assert n == 2
        out = path.read_text()
        assert "FUNCTION a(" not in out
        assert "FUNCTION c(" not in out
        assert "FUNCTION b(" in out

    def test_preserves_use_statements(self, tmp_sql):
        sql = """\
USE CATALOG dev_fin;
USE SCHEMA finance;

CREATE OR REPLACE FUNCTION orphan(x INT);
"""
        path = tmp_sql(sql)
        n = autofix_remove_bodyless_functions(path)
        assert n == 1
        out = path.read_text()
        assert "USE CATALOG dev_fin;" in out
        assert "USE SCHEMA finance;" in out


def test_strip_native_source_assignments_keeps_single_treatment(tmp_path):
    path = tmp_path / "abac.auto.tfvars"
    path.write_text('''tag_assignments = [
  { entity_type = "columns", entity_name = "cat.sch.tbl.email", tag_key = "pii_level", tag_value = "masked_email" },
  { entity_type = "columns", entity_name = "cat.sch.tbl.email", tag_key = "gr_treatment", tag_value = "email_partial" },
  { entity_type = "columns", entity_name = "cat.sch.tbl.region", tag_key = "gr_row_scope", tag_value = "region_code" }
]
''')

    assert generate_abac.strip_native_source_assignments(path) == 1
    cfg = assert_valid_hcl(path)
    assert [(a["tag_key"], a["tag_value"]) for a in cfg["tag_assignments"]] == [
        ("gr_treatment", "email_partial"),
        ("gr_row_scope", "region_code"),
    ]


def test_initial_and_retry_paths_share_authoritative_pipeline_wiring():
    source = inspect.getsource(generate_abac.main)
    assert source.count("authoritative_classification=classification_source is not None") == 2
    assert source.count("derive_and_finalize_treatments(") == 2


def test_native_finalizer_is_idempotent_and_strips_source_families(tmp_path):
    path = tmp_path / "abac.auto.tfvars"
    path.write_text('''tag_policies = [
  { key = "pii_level", values = ["masked_email"] },
  { key = "gr_treatment", values = ["email_partial"] }
]
tag_assignments = [
  { entity_type = "columns", entity_name = "cat.sch.tbl.email", tag_key = "pii_level", tag_value = "masked_email" }
]
fgac_policies = []
''')
    generate_abac.derive_and_finalize_treatments(path, native_authoritative=True)
    first = path.read_text()
    generate_abac.derive_and_finalize_treatments(path, native_authoritative=True)
    second = path.read_text()
    cfg = assert_valid_hcl(path)
    assert second == first
    assert [(a["tag_key"], a["tag_value"]) for a in cfg["tag_assignments"]] == [
        ("gr_treatment", "email_partial")
    ]
    assert [p["key"] for p in cfg["tag_policies"]] == ["gr_treatment"]


def test_derived_treatments_restore_configured_functions_before_ref_repair(tmp_path):
    tfvars = tmp_path / "abac.auto.tfvars"
    tfvars.write_text('''tag_policies = []
tag_assignments = [
  { entity_type = "columns", entity_name = "cat.sch.payments.credit_card_number", tag_key = "pci_level", tag_value = "masked_card_last4" },
  { entity_type = "columns", entity_name = "cat.sch.payments.amount", tag_key = "financial_sensitivity", tag_value = "rounded_amounts" }
]
fgac_policies = [
  { name = "template", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "cat", to_principals = ["users"], function_schema = "sch", match_condition = "hasTagValue('pii_level', 'masked')", function_name = "mask_redact" }
]
''')
    sql = tmp_path / "masking_functions.sql"
    sql.write_text('''USE CATALOG cat;
USE SCHEMA sch;
CREATE FUNCTION mask_redact(input STRING) RETURNS STRING RETURN '***';
CREATE FUNCTION mask_amount_rounded(amount DECIMAL(18,2)) RETURNS DECIMAL(18,2);
''')

    generate_abac.autofix_remove_bodyless_functions(sql)
    generate_abac.derive_and_finalize_treatments(tfvars, native_authoritative=True)
    assert generate_abac.ensure_derived_treatment_functions(tfvars, sql) == 2
    generate_abac.autofix_invalid_function_refs(tfvars, sql)

    cfg = assert_valid_hcl(tfvars)
    functions = {
        p["match_condition"]: p["function_name"] for p in cfg["fgac_policies"]
    }
    assert functions["hasTagValue('gr_treatment', 'card_last4')"] == "mask_credit_card_last4"
    assert functions["hasTagValue('gr_treatment', 'round_amount')"] == "mask_amount_rounded"
    sql_text = sql.read_text()
    assert "FUNCTION mask_credit_card_last4" in sql_text
    assert "FUNCTION mask_amount_rounded" in sql_text


def test_category_mismatch_autofix_preserves_canonical_treatment_function(tmp_path):
    tfvars = tmp_path / "abac.auto.tfvars"
    tfvars.write_text('''tag_assignments = [
  { entity_type = "columns", entity_name = "cat.sch.tbl.phone_number", tag_key = "gr_treatment", tag_value = "email_partial" }
]
fgac_policies = [
  {
    name = "mask_email"
    policy_type = "POLICY_TYPE_COLUMN_MASK"
    catalog = "cat"
    to_principals = ["users"]
    function_schema = "sch"
    match_condition = "hasTagValue('gr_treatment', 'email_partial')"
    function_name = "mask_email"
  }
]
''')
    sql = tmp_path / "masking_functions.sql"
    sql.write_text(
        "CREATE FUNCTION mask_email(input STRING) RETURNS STRING RETURN input;\n"
        "CREATE FUNCTION mask_redact(input STRING) RETURNS STRING RETURN '***';\n"
    )

    before = tfvars.read_text()
    assert generate_abac.autofix_function_category_mismatch(tfvars, sql) == 0
    assert tfvars.read_text() == before
    assert assert_valid_hcl(tfvars)["fgac_policies"][0]["function_name"] == "mask_email"


def test_required_native_classification_fails_without_warehouse(monkeypatch):
    import databricks.sdk

    class FakeWarehouses:
        @staticmethod
        def list():
            return []

    class FakeClient:
        warehouses = FakeWarehouses()

    monkeypatch.setattr(generate_abac, "configure_databricks_env", lambda _: None)
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", lambda **_: FakeClient())

    with pytest.raises(generate_abac.NativeClassificationRequiredError, match="No SQL warehouse"):
        generate_abac._fetch_live_classification_source(
            ["cat.sch.*"], {}, require_native=True,
        )
    assert generate_abac._fetch_live_classification_source(
        ["cat.sch.*"], {}, require_native=False,
    ) is None


def test_required_native_classification_fails_for_unresolvable_footprint():
    with pytest.raises(
        generate_abac.NativeClassificationRequiredError,
        match="footprint contains unresolvable table references",
    ):
        generate_abac._fetch_live_classification_source(
            ["schema_table"], {}, require_native=True,
        )


def test_required_native_classification_qualifies_schema_table(monkeypatch):
    import databricks.sdk

    class FakeWarehouses:
        @staticmethod
        def list():
            return []

    class FakeClient:
        warehouses = FakeWarehouses()

    monkeypatch.setattr(generate_abac, "configure_databricks_env", lambda _: None)
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", lambda **_: FakeClient())

    with pytest.raises(generate_abac.NativeClassificationRequiredError, match="No SQL warehouse"):
        generate_abac._fetch_live_classification_source(
            ["sch.table"], {"uc_catalog": "cat"}, require_native=True,
        )


def test_required_native_classification_fails_on_successful_empty_scan(monkeypatch):
    import databricks.sdk
    from databricks.sdk.service.sql import StatementState

    class FakeStatements:
        @staticmethod
        def execute_statement(**_):
            return SimpleNamespace(
                status=SimpleNamespace(state=StatementState.SUCCEEDED),
                result=SimpleNamespace(data_array=[]),
            )

    class FakeClient:
        statement_execution = FakeStatements()

    monkeypatch.setattr(generate_abac, "configure_databricks_env", lambda _: None)
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", lambda **_: FakeClient())

    with pytest.raises(
        generate_abac.NativeClassificationRequiredError,
        match=r"returned 0 class\.\* findings",
    ) as exc_info:
        generate_abac._fetch_live_classification_source(
            ["cat.sch.*"], {"sql_warehouse_id": "warehouse"}, require_native=True,
        )
    assert "review detections" in str(exc_info.value)
    assert "enable_auto_tagging = true" in str(exc_info.value)
    assert "re-apply enable-classification" in str(exc_info.value)
