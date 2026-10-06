"""Unit tests for schema drift detection and delta generation.

All tests run without any Databricks, LLM, or Terraform dependency.
"""
import os
import re
import subprocess
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
    extract_tag_policies,
    extract_fgac_policies,
    build_rulebook,
    is_tag_covered,
    find_uncovered_tags,
    _parse_condition_tag_refs,
    _applied_tags_sql,
)
import scripts.audit_schema_drift as audit_mod


def _clean_subprocess_env(home: Path) -> dict[str, str]:
    return {
        "HOME": str(home),
        "PATH": os.environ.get("PATH", ""),
    }


def test_missing_python_hcl2_exits_2_in_subprocess(tmp_path):
    script = Path(audit_mod.__file__)
    code = (
        "import runpy, sys; "
        "sys.modules['hcl2'] = None; "
        f"runpy.run_path({str(script)!r}, run_name='__main__')"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        env=_clean_subprocess_env(tmp_path),
    )
    assert result.returncode == 2
    assert "ERROR: audit could not complete" in result.stderr
    assert "python-hcl2 is not installed" in result.stderr


@pytest.mark.parametrize(
    "relative_path",
    ["data_access/abac.auto.tfvars", "generated/abac.auto.tfvars"],
)
def test_unparseable_existing_tfvars_exits_2_in_subprocess(tmp_path, relative_path):
    clean_env = _clean_subprocess_env(tmp_path)
    hcl2_probe = subprocess.run(
        [sys.executable, "-c", "import hcl2"],
        text=True,
        capture_output=True,
        env=clean_env,
    )
    if hcl2_probe.returncode != 0:
        pytest.skip("python-hcl2 is not importable in the audit subprocess environment")
    (tmp_path / "env.auto.tfvars").write_text(
        'uc_tables = ["prod.finance.customers"]\nsql_warehouse_id = "warehouse"\n'
    )
    target = tmp_path / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("this is not valid hcl = [\n")
    script = Path(audit_mod.__file__)
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        env=clean_env,
    )
    assert result.returncode == 2
    assert relative_path in result.stderr
    assert "cannot parse" in result.stderr


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

    def test_discovered_only(self, tmp_path):
        (tmp_path / "env.auto.tfvars").write_text("genie_spaces = []\n")
        (tmp_path / "data_access").mkdir()
        (tmp_path / "data_access/discovered_uc_tables.auto.tfvars").write_text(
            'discovered_uc_tables = ["cat.discovered.table"]\n'
        )
        assert extract_managed_tables(tmp_path) == ["cat.discovered.table"]

    def test_malformed_discovered_file_reports_actionable_error(
        self, tmp_path, monkeypatch, capsys
    ):
        (tmp_path / "env.auto.tfvars").write_text("uc_tables = []\n")
        (tmp_path / "data_access").mkdir()
        (tmp_path / "data_access/discovered_uc_tables.auto.tfvars").write_text(
            'discovered_uc_tables = ["cat.bad.table"\n'
        )
        monkeypatch.chdir(tmp_path)
        assert audit_mod.main([]) == 2
        output = capsys.readouterr().out
        assert "invalid discovered footprint" in output
        assert "make generate" in output


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


# ---------------------------------------------------------------------------
# Rulebook drift — prod-applied tags with no covering policy or mask
# ---------------------------------------------------------------------------

class TestParseConditionTagRefs:
    def test_has_tag_value(self):
        vrefs, krefs = _parse_condition_tag_refs("hasTagValue('pii_level', 'masked_ssn')")
        assert vrefs == {("pii_level", "masked_ssn")}
        assert krefs == set()

    def test_has_tag_key_only(self):
        vrefs, krefs = _parse_condition_tag_refs("hasTag('compliance_scope')")
        assert vrefs == set()
        assert krefs == {"compliance_scope"}

    def test_compound_condition(self):
        cond = "hasTagValue('pii_level', 'masked_ssn') OR hasTag('compliance_scope')"
        vrefs, krefs = _parse_condition_tag_refs(cond)
        assert vrefs == {("pii_level", "masked_ssn")}
        assert krefs == {"compliance_scope"}

    def test_empty_and_none(self):
        assert _parse_condition_tag_refs("") == (set(), set())
        assert _parse_condition_tag_refs(None) == (set(), set())


class TestBuildRulebook:
    def test_policy_vocab_from_tag_policies(self):
        rb = build_rulebook(
            [{"key": "pii_level", "values": ["masked_ssn", "masked_name"]}],
            [],
        )
        assert rb["policy_vocab"] == {"pii_level": {"masked_ssn", "masked_name"}}

    def test_merges_duplicate_keys_across_layers(self):
        rb = build_rulebook(
            [
                {"key": "pii_level", "values": ["masked_ssn"]},
                {"key": "pii_level", "values": ["masked_name"]},
            ],
            [],
        )
        assert rb["policy_vocab"]["pii_level"] == {"masked_ssn", "masked_name"}

    def test_column_mask_refs_scoped_by_catalog(self):
        rb = build_rulebook(
            [],
            [
                {"policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "cat_a",
                 "match_condition": "hasTagValue('pci_level', 'redacted_cvv')"},
                {"policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "cat_b",
                 "match_condition": "hasTag('other_key')"},
            ],
        )
        assert rb["mask_value_refs"] == {"cat_a": {("pci_level", "redacted_cvv")}}
        assert rb["mask_key_refs"] == {"cat_b": {"other_key"}}

    def test_row_filter_when_condition_excluded(self):
        """A row-filter's when_condition targets TABLE tags — must not enter the
        column-tag rulebook at all."""
        rb = build_rulebook(
            [],
            [{"policy_type": "POLICY_TYPE_ROW_FILTER", "catalog": "cat_a",
              "when_condition": "hasTagValue('compliance_scope', 'aml_restricted')"}],
        )
        assert rb["mask_value_refs"] == {}
        assert rb["mask_key_refs"] == {}

    def test_unknown_policy_type_contributes_no_coverage(self):
        """Allowlist: only POLICY_TYPE_COLUMN_MASK contributes. An unknown type
        with a match_condition must NOT provide coverage."""
        rb = build_rulebook(
            [],
            [{"policy_type": "POLICY_TYPE_FUTURE_THING", "catalog": "cat_a",
              "match_condition": "hasTagValue('pii_level', 'masked_ssn')"}],
        )
        assert rb["mask_value_refs"] == {}
        assert rb["mask_key_refs"] == {}

    def test_missing_policy_type_contributes_no_coverage(self):
        """Allowlist: a policy with a match_condition but NO policy_type must NOT
        provide coverage."""
        rb = build_rulebook(
            [],
            [{"catalog": "cat_a",
              "match_condition": "hasTagValue('pii_level', 'masked_ssn')"}],
        )
        assert rb["mask_value_refs"] == {}
        assert rb["mask_key_refs"] == {}

    def test_handles_empty_inputs(self):
        rb = build_rulebook([], [])
        assert rb["policy_vocab"] == {}
        assert rb["mask_value_refs"] == {}
        assert rb["mask_key_refs"] == {}


class TestIsTagCovered:
    def setup_method(self):
        self.rb = build_rulebook(
            [{"key": "pii_level", "values": ["masked_ssn", "masked_name"]}],
            [
                {"policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "fin_catalog",
                 "match_condition": "hasTagValue('pci_level', 'redacted_cvv')"},
                {"policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "fin_catalog",
                 "match_condition": "hasTag('any_val_key')"},
            ],
        )

    def test_covered_by_tag_policy_value_any_catalog(self):
        # tag_policies are metastore-level — covered regardless of catalog
        assert is_tag_covered("fin_catalog", "pii_level", "masked_ssn", self.rb)
        assert is_tag_covered("other_catalog", "pii_level", "masked_ssn", self.rb)

    def test_covered_by_in_catalog_mask_value_ref(self):
        assert is_tag_covered("fin_catalog", "pci_level", "redacted_cvv", self.rb)

    def test_mask_does_not_cover_other_catalog(self):
        # same tag, different catalog — the mask in fin_catalog must NOT cover it
        assert not is_tag_covered("other_catalog", "pci_level", "redacted_cvv", self.rb)

    def test_covered_by_hastag_any_value_in_catalog(self):
        assert is_tag_covered("fin_catalog", "any_val_key", "whatever", self.rb)

    def test_uncovered_unknown_key(self):
        assert not is_tag_covered("fin_catalog", "class.pii", "ssn", self.rb)

    def test_uncovered_known_key_unknown_value(self):
        assert not is_tag_covered("fin_catalog", "pii_level", "some_new_value", self.rb)


class TestFindUncoveredTags:
    def _rb(self):
        return build_rulebook(
            [{"key": "pii_level", "values": ["masked_ssn", "masked_name"]}],
            [{"policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "c",
              "match_condition": "hasTagValue('pii_level', 'masked_ssn')"}],
        )

    def test_fully_covered_set_passes(self):
        """A set of applied tags all covered by policy/mask reports nothing."""
        applied = [
            {"catalog": "c", "schema": "s", "table": "customers",
             "column": "ssn", "tag_key": "pii_level", "tag_value": "masked_ssn"},
            {"catalog": "c", "schema": "s", "table": "customers",
             "column": "first_name", "tag_key": "pii_level", "tag_value": "masked_name"},
        ]
        assert find_uncovered_tags(applied, self._rb()) == []

    def test_detected_tag_with_no_covering_policy_is_flagged(self):
        """A class.* classification landed in prod with no rule — must be flagged."""
        applied = [
            {"catalog": "c", "schema": "s", "table": "customers",
             "column": "ssn", "tag_key": "pii_level", "tag_value": "masked_ssn"},  # covered
            {"catalog": "prod_cat", "schema": "finance", "table": "customers",
             "column": "passport_no", "tag_key": "class.pii", "tag_value": "ssn"},  # NOT covered
        ]
        uncovered = find_uncovered_tags(applied, self._rb())
        assert len(uncovered) == 1
        flagged = uncovered[0]
        assert flagged["tag_key"] == "class.pii"
        assert flagged["tag_value"] == "ssn"
        assert flagged["column"] == "passport_no"
        assert "unknown tag key" in flagged["reason"]

    def test_native_class_tag_is_covered_via_derived_treatment(self):
        rb = build_rulebook(
            [{"key": "gr_treatment", "values": ["card_last4"]}],
            [],
        )
        applied = [
            {"catalog": "prod", "schema": "finance", "table": "payments",
             "column": "card_number", "tag_key": "class.credit_card", "tag_value": ""},
        ]

        assert find_uncovered_tags(applied, rb) == []

    def test_native_class_tag_stays_uncovered_when_treatment_not_promoted(self):
        rb = build_rulebook(
            [{"key": "gr_treatment", "values": ["redact"]}],
            [],
        )
        applied = [
            {"catalog": "prod", "schema": "finance", "table": "payments",
             "column": "card_number", "tag_key": "class.credit_card", "tag_value": ""},
        ]

        assert len(find_uncovered_tags(applied, rb)) == 1

    def test_governed_key_unexpected_value_is_flagged(self):
        """Known key, but a value neither declared nor masked — flagged as value gap."""
        applied = [
            {"catalog": "c", "schema": "s", "table": "t",
             "column": "col", "tag_key": "pii_level", "tag_value": "brand_new_level"},
        ]
        uncovered = find_uncovered_tags(applied, self._rb())
        assert len(uncovered) == 1
        assert "value not covered" in uncovered[0]["reason"]

    def test_cross_catalog_mask_does_not_cover(self):
        """A custom tag masked only in catalog A, applied in catalog B, is drift."""
        rb = build_rulebook(
            [],  # no tag_policy declares this key — coverage can only come from a mask
            [{"policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "cat_a",
              "match_condition": "hasTagValue('team_tag', 'secret')"}],
        )
        applied = [
            {"catalog": "cat_a", "schema": "s", "table": "t",
             "column": "col", "tag_key": "team_tag", "tag_value": "secret"},  # covered
            {"catalog": "cat_b", "schema": "s", "table": "t",
             "column": "col", "tag_key": "team_tag", "tag_value": "secret"},  # NOT covered
        ]
        uncovered = find_uncovered_tags(applied, rb)
        assert len(uncovered) == 1
        assert uncovered[0]["catalog"] == "cat_b"

    def test_row_filter_does_not_cover_column_tag(self):
        """A column tag referenced only by a row-filter when_condition is NOT covered."""
        rb = build_rulebook(
            [],
            [{"policy_type": "POLICY_TYPE_ROW_FILTER", "catalog": "c",
              "when_condition": "hasTagValue('compliance_scope', 'aml_restricted')"}],
        )
        applied = [
            {"catalog": "c", "schema": "s", "table": "t", "column": "region",
             "tag_key": "compliance_scope", "tag_value": "aml_restricted"},
        ]
        uncovered = find_uncovered_tags(applied, rb)
        assert len(uncovered) == 1
        assert uncovered[0]["tag_key"] == "compliance_scope"

    def test_unknown_policy_type_does_not_suppress_flag(self):
        """A match_condition on a non-column-mask (unknown) type must NOT cover a
        tag — the genuinely uncovered tag is still flagged, not suppressed."""
        rb = build_rulebook(
            [],
            [{"policy_type": "SOMETHING_ELSE", "catalog": "c",
              "match_condition": "hasTagValue('pii_level', 'masked_ssn')"}],
        )
        applied = [
            {"catalog": "c", "schema": "s", "table": "t", "column": "ssn",
             "tag_key": "pii_level", "tag_value": "masked_ssn"},
        ]
        uncovered = find_uncovered_tags(applied, rb)
        assert len(uncovered) == 1
        assert uncovered[0]["tag_key"] == "pii_level"

    def test_empty_applied_tags(self):
        assert find_uncovered_tags([], self._rb()) == []


class TestExtractRulebookConfig:
    def test_extract_tag_policies_unions_account_and_data_access(self, tmp_path):
        account_dir = tmp_path / "account"
        account_dir.mkdir()
        (account_dir / "abac.auto.tfvars").write_text("""\
tag_policies = [
  { key = "pii_level", values = ["masked_ssn"], description = "" },
]
""")
        env_dir = tmp_path / "dev"
        da_dir = env_dir / "data_access"
        da_dir.mkdir(parents=True)
        (da_dir / "abac.auto.tfvars").write_text("""\
tag_policies = [
  { key = "pci_level", values = ["redacted_cvv"], description = "" },
]
""")
        policies = extract_tag_policies(env_dir)
        keys = {p["key"] for p in policies}
        assert keys == {"pii_level", "pci_level"}

    def test_extract_ignores_generated_draft(self, tmp_path):
        """Coverage is derived from PROMOTED config only — the local generated/
        draft must be ignored so a dropped rule cannot be masked by the draft."""
        env_dir = tmp_path / "dev"
        gen_dir = env_dir / "generated"
        gen_dir.mkdir(parents=True)
        (gen_dir / "abac.auto.tfvars").write_text("""\
tag_policies = [
  { key = "draft_only_key", values = ["x"], description = "" },
]
fgac_policies = [
  {
    name            = "draft_mask"
    policy_type     = "POLICY_TYPE_COLUMN_MASK"
    catalog         = "c"
    to_principals   = ["G"]
    match_condition = "hasTagValue('draft_only_key', 'x')"
    match_alias     = "cols"
    function_name   = "m"
    function_catalog = "c"
    function_schema  = "s"
  },
]
""")
        assert extract_tag_policies(env_dir) == []
        assert extract_fgac_policies(env_dir) == []

    def test_extract_fgac_policies_from_data_access(self, tmp_path):
        env_dir = tmp_path / "dev"
        da_dir = env_dir / "data_access"
        da_dir.mkdir(parents=True)
        (da_dir / "abac.auto.tfvars").write_text("""\
fgac_policies = [
  {
    name            = "mask_ssn"
    policy_type     = "POLICY_TYPE_COLUMN_MASK"
    catalog         = "c"
    to_principals   = ["Junior_Analyst"]
    match_condition = "hasTagValue('pii_level', 'masked_ssn')"
    match_alias     = "cols"
    function_name   = "mask_ssn"
    function_catalog = "c"
    function_schema  = "s"
  },
]
""")
        policies = extract_fgac_policies(env_dir)
        assert len(policies) == 1
        assert policies[0]["match_condition"] == "hasTagValue('pii_level', 'masked_ssn')"

    def test_extract_missing_files(self, tmp_path):
        env_dir = tmp_path / "dev"
        env_dir.mkdir()
        assert extract_tag_policies(env_dir) == []
        assert extract_fgac_policies(env_dir) == []

    def test_end_to_end_from_finance_shaped_config(self, tmp_path):
        """Build the rulebook from config on disk, then classify applied tags."""
        account_dir = tmp_path / "account"
        account_dir.mkdir()
        (account_dir / "abac.auto.tfvars").write_text("""\
tag_policies = [
  { key = "pii_level", values = ["masked_ssn", "masked_name"], description = "" },
]
""")
        env_dir = tmp_path / "dev"
        da_dir = env_dir / "data_access"
        da_dir.mkdir(parents=True)
        (da_dir / "abac.auto.tfvars").write_text("""\
fgac_policies = [
  {
    name            = "mask_ssn"
    policy_type     = "POLICY_TYPE_COLUMN_MASK"
    catalog         = "fin_catalog"
    to_principals   = ["Junior_Analyst"]
    match_condition = "hasTagValue('pii_level', 'masked_ssn')"
    match_alias     = "cols"
    function_name   = "mask_ssn"
    function_catalog = "fin_catalog"
    function_schema  = "finance"
  },
]
""")
        rb = build_rulebook(extract_tag_policies(env_dir), extract_fgac_policies(env_dir))
        applied = [
            {"catalog": "fin_catalog", "schema": "finance", "table": "customers",
             "column": "ssn", "tag_key": "pii_level", "tag_value": "masked_ssn"},
            {"catalog": "fin_catalog", "schema": "finance", "table": "customers",
             "column": "dob", "tag_key": "class.pii", "tag_value": "date_of_birth"},
        ]
        uncovered = find_uncovered_tags(applied, rb)
        assert len(uncovered) == 1
        assert uncovered[0]["tag_key"] == "class.pii"


class TestAppliedTagsQuery:
    def test_query_is_not_restricted_to_known_keys(self):
        """The applied-tags query must NOT filter tag_name — otherwise a tag whose
        key was dropped from config could never be fetched (issue 1)."""
        sql = _applied_tags_sql(["cat.sch.tbl"])
        assert "system.information_schema.column_tags" in sql
        # No tag_name restriction of any kind
        assert "tag_name LIKE" not in sql
        assert "tag_name IN" not in sql
        # Still scoped to the managed tables
        assert "cat.sch.tbl" in sql


class TestMainRulebookExit:
    """main() end-to-end with the Databricks seams mocked."""

    def _write_env(self, tmp_path):
        env_dir = tmp_path / "dev"
        da_dir = env_dir / "data_access"
        da_dir.mkdir(parents=True)
        (env_dir / "env.auto.tfvars").write_text("""\
uc_tables = ["prod_cat.finance.customers"]
sql_warehouse_id = "wh-123"
""")
        (da_dir / "abac.auto.tfvars").write_text("""\
tag_policies = [
  { key = "pii_level", values = ["masked_ssn"], description = "" },
]
fgac_policies = [
  {
    name            = "mask_ssn"
    policy_type     = "POLICY_TYPE_COLUMN_MASK"
    catalog         = "prod_cat"
    to_principals   = ["Junior_Analyst"]
    match_condition = "hasTagValue('pii_level', 'masked_ssn')"
    match_alias     = "cols"
    function_name   = "mask_ssn"
    function_catalog = "prod_cat"
    function_schema  = "finance"
  },
]
""")
        return env_dir

    def _mock_seams(self, monkeypatch, applied):
        monkeypatch.setattr(audit_mod, "_get_sdk_client", lambda env_dir: object())
        monkeypatch.setattr(audit_mod, "_get_warehouse_id", lambda env_dir, w: "wh-123")
        monkeypatch.setattr(
            audit_mod, "_query_applied_tags",
            lambda w, wh, tables: applied,
        )

    def test_rulebook_drift_exits_1(self, tmp_path, monkeypatch):
        env_dir = self._write_env(tmp_path)
        monkeypatch.chdir(env_dir)
        self._mock_seams(monkeypatch, [
            {"catalog": "prod_cat", "schema": "finance", "table": "customers",
             "column": "dob", "tag_key": "class.pii", "tag_value": "date_of_birth"},
        ])
        assert audit_mod.main(["--mode", "rulebook"]) == 1

    def test_rulebook_clean_exits_0(self, tmp_path, monkeypatch):
        env_dir = self._write_env(tmp_path)
        monkeypatch.chdir(env_dir)
        self._mock_seams(monkeypatch, [
            {"catalog": "prod_cat", "schema": "finance", "table": "customers",
             "column": "ssn", "tag_key": "pii_level", "tag_value": "masked_ssn"},
        ])
        assert audit_mod.main(["--mode", "rulebook"]) == 0

    def test_dropped_rule_orphan_tag_is_flagged(self, tmp_path, monkeypatch):
        """End-to-end: prod retains a custom governance tag whose key/rule was
        COMPLETELY removed from promoted config. The promoted config only knows
        pii_level, but prod still carries `data_residency=eu_only` (a non-class.*
        key no longer in any policy/mask). The audit must fetch it (unrestricted
        query) and flag it (exit 1)."""
        env_dir = self._write_env(tmp_path)
        monkeypatch.chdir(env_dir)
        self._mock_seams(monkeypatch, [
            # a governance tag under a key that no longer exists anywhere in config
            {"catalog": "prod_cat", "schema": "finance", "table": "customers",
             "column": "region", "tag_key": "data_residency", "tag_value": "eu_only"},
        ])
        assert audit_mod.main(["--mode", "rulebook"]) == 1

    def test_generated_draft_does_not_provide_coverage(self, tmp_path, monkeypatch):
        """A rule present ONLY in the local generated/ draft (not promoted) must
        NOT count as coverage: the matching prod tag is still flagged (exit 1)."""
        env_dir = self._write_env(tmp_path)
        # Promoted data_access only governs pii_level. Put a mask for a different
        # key in the generated draft — it must be ignored.
        gen_dir = env_dir / "generated"
        gen_dir.mkdir(parents=True)
        (gen_dir / "abac.auto.tfvars").write_text("""\
tag_policies = [
  { key = "draft_key", values = ["secret"], description = "" },
]
fgac_policies = [
  {
    name            = "draft_mask"
    policy_type     = "POLICY_TYPE_COLUMN_MASK"
    catalog         = "prod_cat"
    to_principals   = ["G"]
    match_condition = "hasTagValue('draft_key', 'secret')"
    match_alias     = "cols"
    function_name   = "m"
    function_catalog = "prod_cat"
    function_schema  = "finance"
  },
]
""")
        monkeypatch.chdir(env_dir)
        self._mock_seams(monkeypatch, [
            {"catalog": "prod_cat", "schema": "finance", "table": "customers",
             "column": "col", "tag_key": "draft_key", "tag_value": "secret"},
        ])
        assert audit_mod.main(["--mode", "rulebook"]) == 1
