"""Unit tests for schema drift detection and delta generation.

All tests run without any Databricks, LLM, or Terraform dependency.
"""
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from scripts.audit_schema_drift import (
    PII_COLUMN_PATTERN,
    extract_managed_tables,
    resolve_governed_keys,
    extract_config_tag_assignments,
)


# ---------------------------------------------------------------------------
# PII pattern regex
# ---------------------------------------------------------------------------

class TestPIIPattern:
    @pytest.mark.parametrize("col", [
        "ssn", "patient_ssn", "social_sec_num", "passport_number",
        "dob", "birth_date", "birthdate", "email", "email_address",
        "phone", "phone_number", "home_address", "mailing_address",
        "credit_card", "creditcard", "cvv", "account_num",
        "diagnosis", "diagnosis_code", "medication", "medication_name",
        "patient_id", "patient_name", "mrn", "npi", "insurance_id",
    ])
    def test_matches_pii_columns(self, col):
        assert PII_COLUMN_PATTERN.search(col), f"{col} should match PII pattern"

    @pytest.mark.parametrize("col", [
        "id", "created_at", "updated_at", "amount", "quantity",
        "status", "type", "name", "description", "category",
        "region", "country", "currency", "risk_tier", "enrolled_at",
    ])
    def test_rejects_non_pii_columns(self, col):
        assert not PII_COLUMN_PATTERN.search(col), f"{col} should NOT match PII pattern"


# ---------------------------------------------------------------------------
# Env parsing — both shapes
# ---------------------------------------------------------------------------

class TestExtractManagedTables:
    def test_top_level_uc_tables(self, tmp_path):
        (tmp_path / "env.auto.tfvars").write_text("""\
uc_tables = [
  "cat1.schema1.table1",
  "cat2.schema2.table2",
]
sql_warehouse_id = ""
""")
        tables = extract_managed_tables(tmp_path)
        assert tables == ["cat1.schema1.table1", "cat2.schema2.table2"]

    def test_genie_spaces_uc_tables(self, tmp_path):
        (tmp_path / "env.auto.tfvars").write_text("""\
genie_spaces = [
  {
    name = "Space A"
    uc_tables = [
      "cat1.schema1.table1",
      "cat1.schema1.table2",
    ]
  },
  {
    name = "Space B"
    uc_tables = [
      "cat2.schema2.table3",
    ]
  },
]
sql_warehouse_id = ""
""")
        tables = extract_managed_tables(tmp_path)
        assert tables == [
            "cat1.schema1.table1",
            "cat1.schema1.table2",
            "cat2.schema2.table3",
        ]

    def test_both_shapes_union(self, tmp_path):
        (tmp_path / "env.auto.tfvars").write_text("""\
uc_tables = [
  "cat1.schema1.shared_table",
]
genie_spaces = [
  {
    name = "Space A"
    uc_tables = [
      "cat2.schema2.space_table",
    ]
  },
]
sql_warehouse_id = ""
""")
        tables = extract_managed_tables(tmp_path)
        assert "cat1.schema1.shared_table" in tables
        assert "cat2.schema2.space_table" in tables

    def test_deduplication(self, tmp_path):
        (tmp_path / "env.auto.tfvars").write_text("""\
uc_tables = [
  "cat1.schema1.table1",
]
genie_spaces = [
  {
    name = "Space A"
    uc_tables = [
      "cat1.schema1.table1",
    ]
  },
]
sql_warehouse_id = ""
""")
        tables = extract_managed_tables(tmp_path)
        assert tables.count("cat1.schema1.table1") == 1

    def test_missing_file(self, tmp_path):
        tables = extract_managed_tables(tmp_path)
        assert tables == []


# ---------------------------------------------------------------------------
# Governed key resolution (4-level fallback)
# ---------------------------------------------------------------------------

class TestResolveGovernedKeys:
    def test_from_account_tag_policies(self, tmp_path):
        account_dir = tmp_path / "account"
        account_dir.mkdir()
        (account_dir / "abac.auto.tfvars").write_text("""\
tag_policies = [
  { key = "pii_level", values = ["masked", "full"], description = "" },
  { key = "phi_level", values = ["redacted"], description = "" },
]
""")
        env_dir = tmp_path / "dev"
        env_dir.mkdir()
        keys = resolve_governed_keys(env_dir)
        assert keys == ["pii_level", "phi_level"]

    def test_from_data_access_tag_assignments(self, tmp_path):
        env_dir = tmp_path / "dev"
        da_dir = env_dir / "data_access"
        da_dir.mkdir(parents=True)
        (da_dir / "abac.auto.tfvars").write_text("""\
tag_assignments = [
  { entity_type = "columns", entity_name = "c.s.t.col1", tag_key = "pii_level", tag_value = "masked" },
  { entity_type = "columns", entity_name = "c.s.t.col2", tag_key = "phi_level", tag_value = "full" },
  { entity_type = "columns", entity_name = "c.s.t.col3", tag_key = "pii_level", tag_value = "full" },
]
""")
        keys = resolve_governed_keys(env_dir)
        assert sorted(keys) == ["phi_level", "pii_level"]

    def test_from_generated(self, tmp_path):
        env_dir = tmp_path / "dev"
        gen_dir = env_dir / "generated"
        gen_dir.mkdir(parents=True)
        (gen_dir / "abac.auto.tfvars").write_text("""\
tag_policies = [
  { key = "financial_sensitivity", values = ["high"], description = "" },
]
""")
        keys = resolve_governed_keys(env_dir)
        assert keys == ["financial_sensitivity"]

    def test_hardcoded_fallback(self, tmp_path):
        env_dir = tmp_path / "dev"
        env_dir.mkdir()
        keys = resolve_governed_keys(env_dir)
        assert "pii_level" in keys
        assert "phi_level" in keys

    def test_priority_order(self, tmp_path):
        """Account config wins over data_access and generated."""
        account_dir = tmp_path / "account"
        account_dir.mkdir()
        (account_dir / "abac.auto.tfvars").write_text("""\
tag_policies = [
  { key = "account_key", values = ["v1"], description = "" },
]
""")
        env_dir = tmp_path / "dev"
        da_dir = env_dir / "data_access"
        da_dir.mkdir(parents=True)
        (da_dir / "abac.auto.tfvars").write_text("""\
tag_assignments = [
  { entity_type = "columns", entity_name = "c.s.t.col", tag_key = "da_key", tag_value = "v" },
]
""")
        keys = resolve_governed_keys(env_dir)
        assert keys == ["account_key"]


# ---------------------------------------------------------------------------
# Config tag assignment extraction
# ---------------------------------------------------------------------------

class TestExtractConfigTagAssignments:
    def test_extracts_assignments(self, tmp_path):
        da_dir = tmp_path / "data_access"
        da_dir.mkdir()
        (da_dir / "abac.auto.tfvars").write_text("""\
tag_assignments = [
  { entity_type = "columns", entity_name = "c.s.t.col1", tag_key = "pii_level", tag_value = "masked" },
  { entity_type = "tables", entity_name = "c.s.t", tag_key = "scope", tag_value = "aml" },
]
""")
        assignments = extract_config_tag_assignments(tmp_path)
        assert len(assignments) == 2
        assert assignments[0]["entity_name"] == "c.s.t.col1"

    def test_missing_file(self, tmp_path):
        assert extract_config_tag_assignments(tmp_path) == []


# ---------------------------------------------------------------------------
# Delta merge logic (pure function tests)
# ---------------------------------------------------------------------------

class TestDeltaMerge:
    """Tests for the merge_delta_assignments function in generate_abac.py."""

    def test_appends_new_assignments(self, tmp_path):
        from generate_abac import merge_delta_assignments
        existing = tmp_path / "abac.auto.tfvars"
        existing.write_text("""\
groups = {}

tag_assignments = [
  {
    entity_type = "columns"
    entity_name = "c.s.t.col1"
    tag_key     = "pii_level"
    tag_value   = "masked"
  },
]
""")
        new_assignments = [
            {"entity_type": "columns", "entity_name": "c.s.t.col2",
             "tag_key": "pii_level", "tag_value": "full"},
        ]
        merge_delta_assignments(existing, new_assignments)
        text = existing.read_text()
        assert "c.s.t.col2" in text
        assert "c.s.t.col1" in text  # existing preserved

    def test_deduplicates(self, tmp_path):
        from generate_abac import merge_delta_assignments
        existing = tmp_path / "abac.auto.tfvars"
        existing.write_text("""\
tag_assignments = [
  {
    entity_type = "columns"
    entity_name = "c.s.t.col1"
    tag_key     = "pii_level"
    tag_value   = "masked"
  },
]
""")
        new_assignments = [
            {"entity_type": "columns", "entity_name": "c.s.t.col1",
             "tag_key": "pii_level", "tag_value": "full"},
        ]
        merge_delta_assignments(existing, new_assignments)
        text = existing.read_text()
        assert text.count("c.s.t.col1") == 1  # not duplicated


class TestDeltaValidation:
    """Tests for validate_delta_assignments in generate_abac.py."""

    def test_rejects_unknown_key(self):
        from generate_abac import validate_delta_assignments
        governed = {"pii_level": ["masked", "full"]}
        drifted_columns = {"c.s.t.col1"}
        assignments = [
            {"entity_type": "columns", "entity_name": "c.s.t.col1",
             "tag_key": "invented_key", "tag_value": "whatever"},
        ]
        errors = validate_delta_assignments(assignments, governed, drifted_columns)
        assert any("invented_key" in e for e in errors)

    def test_rejects_unknown_value(self):
        from generate_abac import validate_delta_assignments
        governed = {"pii_level": ["masked", "full"]}
        drifted_columns = {"c.s.t.col1"}
        assignments = [
            {"entity_type": "columns", "entity_name": "c.s.t.col1",
             "tag_key": "pii_level", "tag_value": "invented_value"},
        ]
        errors = validate_delta_assignments(assignments, governed, drifted_columns)
        assert any("invented_value" in e for e in errors)

    def test_rejects_unknown_entity(self):
        from generate_abac import validate_delta_assignments
        governed = {"pii_level": ["masked", "full"]}
        drifted_columns = {"c.s.t.col1"}
        assignments = [
            {"entity_type": "columns", "entity_name": "c.s.t.col_unknown",
             "tag_key": "pii_level", "tag_value": "masked"},
        ]
        errors = validate_delta_assignments(assignments, governed, drifted_columns)
        assert any("col_unknown" in e for e in errors)

    def test_accepts_valid(self):
        from generate_abac import validate_delta_assignments
        governed = {"pii_level": ["masked", "full"]}
        drifted_columns = {"c.s.t.col1"}
        assignments = [
            {"entity_type": "columns", "entity_name": "c.s.t.col1",
             "tag_key": "pii_level", "tag_value": "masked"},
        ]
        errors = validate_delta_assignments(assignments, governed, drifted_columns)
        assert errors == []


class TestRemoveStaleAssignments:
    """Tests for remove_stale_assignments in generate_abac.py."""

    def test_removes_stale(self, tmp_path):
        from generate_abac import remove_stale_assignments
        abac = tmp_path / "abac.auto.tfvars"
        abac.write_text("""\
tag_assignments = [
  {
    entity_type = "columns"
    entity_name = "c.s.t.live_col"
    tag_key     = "pii_level"
    tag_value   = "masked"
  },
  {
    entity_type = "columns"
    entity_name = "c.s.t.dead_col"
    tag_key     = "pii_level"
    tag_value   = "full"
  },
]
""")
        removed = remove_stale_assignments(abac, ["c.s.t.dead_col"])
        assert removed == 1
        text = abac.read_text()
        assert "dead_col" not in text
        assert "live_col" in text

    def test_no_op_when_nothing_stale(self, tmp_path):
        from generate_abac import remove_stale_assignments
        abac = tmp_path / "abac.auto.tfvars"
        original = """\
tag_assignments = [
  {
    entity_type = "columns"
    entity_name = "c.s.t.live_col"
    tag_key     = "pii_level"
    tag_value   = "masked"
  },
]
"""
        abac.write_text(original)
        removed = remove_stale_assignments(abac, [])
        assert removed == 0
        assert abac.read_text() == original
