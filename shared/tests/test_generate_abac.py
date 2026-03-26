"""Unit tests for the autofix functions in generate_abac.py.

All tests run without any Databricks, LLM, or Terraform dependency.
Each test writes a minimal .tfvars snippet to a temp file, calls the
relevant autofix function, and asserts the expected outcome.
"""
import re
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Make sure the project root is importable regardless of how pytest is invoked
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))

from generate_abac import (
    fix_hcl_syntax,
    autofix_tag_policies,
    autofix_invalid_tag_values,
    autofix_undefined_tag_refs,
    autofix_missing_fgac_policies,
    autofix_fgac_policy_count,
)
from tests.conftest import assert_valid_hcl

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

    def test_adds_value_referenced_in_fgac_condition(self, tmp_tfvars):
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
        assert count == 1
        assert '"Full_PII"' in path.read_text()

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
        import generate_abac
        monkeypatch.setattr(generate_abac, "_FGAC_PER_CATALOG_LIMIT", 5)
        path = tmp_tfvars(self._make_hcl(["p1", "p2", "p3"]))
        count = autofix_fgac_policy_count(path)
        assert count == 0
        assert_valid_hcl(path)

    def test_removes_excess_policies_when_over_limit(self, tmp_tfvars, monkeypatch):
        import generate_abac
        monkeypatch.setattr(generate_abac, "_FGAC_PER_CATALOG_LIMIT", 2)
        path = tmp_tfvars(self._make_hcl(["p1", "p2", "p3", "p4"]))
        count = autofix_fgac_policy_count(path)
        assert count == 2
        cfg = assert_valid_hcl(path)
        remaining = cfg.get("fgac_policies", [])
        assert len(remaining) == 2

    def test_remove_block_leaves_valid_hcl(self, tmp_tfvars, monkeypatch):
        """After _remove_block runs, the file must still parse as valid HCL."""
        import generate_abac
        monkeypatch.setattr(generate_abac, "_FGAC_PER_CATALOG_LIMIT", 1)
        path = tmp_tfvars(self._make_hcl(["alpha", "beta", "gamma"]))
        autofix_fgac_policy_count(path)
        assert_valid_hcl(path)

    def test_remove_block_no_stray_commas(self, tmp_tfvars, monkeypatch):
        """Ensure no double-commas after block removal and the result is valid HCL."""
        import generate_abac
        monkeypatch.setattr(generate_abac, "_FGAC_PER_CATALOG_LIMIT", 1)
        path = tmp_tfvars(self._make_hcl(["x", "y"]))
        autofix_fgac_policy_count(path)
        text = path.read_text()
        # HCL allows trailing commas before ] — only double commas are invalid
        assert ",,\n" not in text
        # The remaining HCL must still parse
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
        # 3 per catalog, limit 2 → 1 removal per catalog = 2 total
        count = autofix_fgac_policy_count(path)
        assert count == 2
        assert_valid_hcl(path)
