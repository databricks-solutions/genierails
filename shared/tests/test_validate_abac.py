"""Unit tests for validate_abac.py validation functions.

Tests exercise the individual validate_* functions with synthetic config dicts
so no file I/O, Databricks, or LLM access is needed.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from validate_abac import (
    ValidationResult,
    validate_groups,
    validate_tag_policies,
    validate_tag_assignments,
    validate_fgac_policies,
    parse_sql_functions,
    parse_sql_function_arg_counts,
    _condition_matches_tags,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _result() -> ValidationResult:
    return ValidationResult()


def _ok_cfg() -> dict:
    """Minimal valid config dict (already parsed from HCL)."""
    return {
        "groups": {"analysts": {"description": "Analyst group"}},
        "tag_policies": [
            {"key": "pii_level", "values": ["public", "Limited_PII", "Full_PII"]},
        ],
        "tag_assignments": [
            {
                "entity_type": "tables",
                "entity_name": "main.hr.employees",
                "tag_key": "pii_level",
                "tag_value": "public",
            }
        ],
        "fgac_policies": [],
    }


# ===========================================================================
#  validate_groups
# ===========================================================================

class TestValidateGroups:

    def test_valid_group_passes(self):
        r = _result()
        names = validate_groups({"groups": {"team_a": {"description": "Team A"}}}, r)
        assert r.passed
        assert "team_a" in names

    def test_missing_groups_key_fails(self):
        r = _result()
        validate_groups({}, r)
        assert not r.passed

    def test_empty_groups_fails(self):
        r = _result()
        validate_groups({"groups": {}}, r)
        assert not r.passed

    def test_multiple_groups_all_returned(self):
        r = _result()
        names = validate_groups(
            {"groups": {"g1": {"description": "G1"}, "g2": {"description": "G2"}}}, r
        )
        assert names == {"g1", "g2"}
        assert r.passed


# ===========================================================================
#  validate_tag_policies
# ===========================================================================

class TestValidateTagPolicies:

    def test_valid_policies_pass(self):
        r = _result()
        tag_map = validate_tag_policies(
            {"tag_policies": [{"key": "pii_level", "values": ["public", "Limited_PII"]}]}, r
        )
        assert r.passed
        assert "pii_level" in tag_map
        assert "public" in tag_map["pii_level"]

    def test_duplicate_key_fails(self):
        r = _result()
        validate_tag_policies(
            {
                "tag_policies": [
                    {"key": "pii_level", "values": ["public"]},
                    {"key": "pii_level", "values": ["limited"]},
                ]
            },
            r,
        )
        assert not r.passed
        assert any("duplicate" in e for e in r.errors)

    def test_empty_values_fails(self):
        r = _result()
        validate_tag_policies({"tag_policies": [{"key": "pii_level", "values": []}]}, r)
        assert not r.passed

    def test_missing_key_field_fails(self):
        r = _result()
        validate_tag_policies({"tag_policies": [{"values": ["public"]}]}, r)
        assert not r.passed


# ===========================================================================
#  validate_tag_assignments
# ===========================================================================

class TestValidateTagAssignments:

    def _tag_map(self) -> dict:
        return {"pii_level": {"public", "Limited_PII", "Full_PII"}}

    def test_valid_table_assignment_passes(self):
        cfg = {
            "tag_assignments": [
                {
                    "entity_type": "tables",
                    "entity_name": "cat.schema.table",
                    "tag_key": "pii_level",
                    "tag_value": "public",
                }
            ]
        }
        r = _result()
        validate_tag_assignments(cfg, self._tag_map(), r)
        assert r.passed

    def test_valid_column_assignment_passes(self):
        cfg = {
            "tag_assignments": [
                {
                    "entity_type": "columns",
                    "entity_name": "cat.schema.table.col",
                    "tag_key": "pii_level",
                    "tag_value": "Limited_PII",
                }
            ]
        }
        r = _result()
        validate_tag_assignments(cfg, self._tag_map(), r)
        assert r.passed

    def test_invalid_entity_type_fails(self):
        cfg = {
            "tag_assignments": [
                {
                    "entity_type": "views",
                    "entity_name": "cat.schema.v",
                    "tag_key": "pii_level",
                    "tag_value": "public",
                }
            ]
        }
        r = _result()
        validate_tag_assignments(cfg, self._tag_map(), r)
        assert not r.passed

    def test_undefined_tag_key_fails(self):
        cfg = {
            "tag_assignments": [
                {
                    "entity_type": "tables",
                    "entity_name": "cat.schema.tbl",
                    "tag_key": "nonexistent_key",
                    "tag_value": "public",
                }
            ]
        }
        r = _result()
        validate_tag_assignments(cfg, self._tag_map(), r)
        assert not r.passed
        assert any("not defined in tag_policies" in e for e in r.errors)

    def test_invalid_tag_value_fails(self):
        cfg = {
            "tag_assignments": [
                {
                    "entity_type": "tables",
                    "entity_name": "cat.schema.tbl",
                    "tag_key": "pii_level",
                    "tag_value": "bad_value",
                }
            ]
        }
        r = _result()
        validate_tag_assignments(cfg, self._tag_map(), r)
        assert not r.passed
        assert any("not an allowed value" in e for e in r.errors)

    def test_table_entity_wrong_dot_count_fails(self):
        cfg = {
            "tag_assignments": [
                {
                    "entity_type": "tables",
                    "entity_name": "just_a_table",
                    "tag_key": "pii_level",
                    "tag_value": "public",
                }
            ]
        }
        r = _result()
        validate_tag_assignments(cfg, self._tag_map(), r)
        assert not r.passed

    def test_duplicate_assignment_warns(self):
        assignment = {
            "entity_type": "tables",
            "entity_name": "cat.schema.tbl",
            "tag_key": "pii_level",
            "tag_value": "public",
        }
        cfg = {"tag_assignments": [assignment, assignment]}
        r = _result()
        validate_tag_assignments(cfg, self._tag_map(), r)
        assert any("duplicate" in w for w in r.warnings)


# ===========================================================================
#  validate_fgac_policies
# ===========================================================================

class TestValidateFgacPolicies:

    def _groups(self) -> set:
        return {"analysts"}

    def _tag_map(self) -> dict:
        return {"pii_level": {"public", "Limited_PII", "Full_PII"}}

    def _base_policy(self, **overrides) -> dict:
        p = {
            "name": "mask_pii",
            "policy_type": "POLICY_TYPE_COLUMN_MASK",
            "catalog": "main",
            "to_principals": ["account users"],
            "match_condition": "hasTagValue('pii_level', 'Full_PII')",
            "match_alias": "mask_pii",
            "function_name": "mask_pii_partial",
            "function_catalog": "main",
            "function_schema": "governance",
        }
        p.update(overrides)
        return p

    def test_valid_column_mask_passes(self):
        cfg = {"tag_assignments": [], "fgac_policies": [self._base_policy()]}
        r = _result()
        validate_fgac_policies(cfg, self._groups(), self._tag_map(), None, r)
        assert r.passed

    def test_invalid_policy_type_fails(self):
        cfg = {"tag_assignments": [], "fgac_policies": [self._base_policy(policy_type="BAD_TYPE")]}
        r = _result()
        validate_fgac_policies(cfg, self._groups(), self._tag_map(), None, r)
        assert not r.passed

    def test_missing_policy_name_fails(self):
        p = self._base_policy()
        del p["name"]
        cfg = {"tag_assignments": [], "fgac_policies": [p]}
        r = _result()
        validate_fgac_policies(cfg, self._groups(), self._tag_map(), None, r)
        assert not r.passed

    def test_undefined_group_in_principals_fails(self):
        p = self._base_policy(to_principals=["ghost_group"])
        cfg = {"tag_assignments": [], "fgac_policies": [p]}
        r = _result()
        validate_fgac_policies(cfg, self._groups(), self._tag_map(), None, r)
        assert not r.passed
        assert any("ghost_group" in e for e in r.errors)

    def test_account_users_builtin_passes(self):
        p = self._base_policy(to_principals=["account users"])
        cfg = {"tag_assignments": [], "fgac_policies": [p]}
        r = _result()
        validate_fgac_policies(cfg, self._groups(), self._tag_map(), None, r)
        assert r.passed

    def test_undefined_tag_key_in_condition_fails(self):
        p = self._base_policy(match_condition="hasTagValue('ghost_key', 'v')")
        cfg = {"tag_assignments": [], "fgac_policies": [p]}
        r = _result()
        validate_fgac_policies(cfg, self._groups(), self._tag_map(), None, r)
        assert not r.passed
        assert any("ghost_key" in e for e in r.errors)

    def test_undefined_tag_value_in_condition_fails(self):
        p = self._base_policy(match_condition="hasTagValue('pii_level', 'not_a_value')")
        cfg = {"tag_assignments": [], "fgac_policies": [p]}
        r = _result()
        validate_fgac_policies(cfg, self._groups(), self._tag_map(), None, r)
        assert not r.passed

    def test_sql_function_not_in_file_fails(self):
        p = self._base_policy(function_name="missing_fn")
        cfg = {"tag_assignments": [], "fgac_policies": [p]}
        r = _result()
        # Pass an empty set for sql_functions — the function won't be found
        validate_fgac_policies(cfg, self._groups(), self._tag_map(), set(), r)
        assert not r.passed
        assert any("missing_fn" in e for e in r.errors)

    def test_sql_function_present_passes(self):
        p = self._base_policy(function_name="mask_pii_partial")
        cfg = {"tag_assignments": [], "fgac_policies": [p]}
        r = _result()
        validate_fgac_policies(
            cfg, self._groups(), self._tag_map(), {"mask_pii_partial"}, r
        )
        assert r.passed


# ===========================================================================
#  parse_sql_functions / parse_sql_function_arg_counts
# ===========================================================================

class TestParseSqlFunctions:

    def test_extracts_simple_function(self, tmp_sql):
        # The regex expects either no prefix or a catalog.schema. (two-part) prefix.
        # Use the unqualified form here to test the simple case.
        sql = "CREATE FUNCTION mask_email(col STRING) RETURNS STRING RETURN col;"
        path = tmp_sql(sql)
        fns = parse_sql_functions(path)
        assert "mask_email" in fns

    def test_extracts_or_replace_function(self, tmp_sql):
        sql = "CREATE OR REPLACE FUNCTION mask_pii_partial(col STRING) RETURNS STRING RETURN col;"
        path = tmp_sql(sql)
        fns = parse_sql_functions(path)
        assert "mask_pii_partial" in fns

    def test_extracts_multiple_functions(self, tmp_sql):
        sql = """\
CREATE FUNCTION mask_email(col STRING) RETURNS STRING RETURN col;
CREATE OR REPLACE FUNCTION mask_phone(col STRING) RETURNS STRING RETURN col;
"""
        path = tmp_sql(sql)
        fns = parse_sql_functions(path)
        assert "mask_email" in fns
        assert "mask_phone" in fns

    def test_arg_count_single_arg(self, tmp_sql):
        sql = "CREATE FUNCTION mask_email(col STRING) RETURNS STRING RETURN col;"
        path = tmp_sql(sql)
        counts = parse_sql_function_arg_counts(path)
        assert counts.get("mask_email") == 1

    def test_arg_count_no_args(self, tmp_sql):
        sql = "CREATE FUNCTION filter_sensitive() RETURNS BOOLEAN RETURN TRUE;"
        path = tmp_sql(sql)
        counts = parse_sql_function_arg_counts(path)
        assert counts.get("filter_sensitive") == 0


# ===========================================================================
#  _condition_matches_tags
# ===========================================================================

class TestConditionMatchesTags:

    def test_empty_condition_always_matches(self):
        assert _condition_matches_tags("", {})

    def test_has_tag_value_match(self):
        assert _condition_matches_tags(
            "hasTagValue('pii_level', 'Full_PII')",
            {"pii_level": {"Full_PII"}},
        )

    def test_has_tag_value_no_match(self):
        assert not _condition_matches_tags(
            "hasTagValue('pii_level', 'Full_PII')",
            {"pii_level": {"public"}},
        )

    def test_and_condition(self):
        tags = {"pii_level": {"Full_PII"}, "phi_level": {"high"}}
        assert _condition_matches_tags(
            "hasTagValue('pii_level', 'Full_PII') AND hasTagValue('phi_level', 'high')",
            tags,
        )
        assert not _condition_matches_tags(
            "hasTagValue('pii_level', 'Full_PII') AND hasTagValue('phi_level', 'low')",
            tags,
        )

    def test_or_condition(self):
        tags = {"pii_level": {"Limited_PII"}}
        assert _condition_matches_tags(
            "hasTagValue('pii_level', 'Full_PII') OR hasTagValue('pii_level', 'Limited_PII')",
            tags,
        )
