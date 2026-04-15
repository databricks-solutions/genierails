"""Tests for catalog remapping logic (remap_generated_config.py)."""

import sys
from pathlib import Path

# Allow imports from shared/scripts/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from remap_generated_config import remap_hcl, remap_sql, parse_catalog_pairs


class TestParseCatalogPairs:
    """Tests for parse_catalog_pairs()."""

    def test_single_pair(self):
        pairs = parse_catalog_pairs(["dev_fin=prod_fin"])
        assert pairs == [("dev_fin", "prod_fin")]

    def test_multiple_pairs(self):
        pairs = parse_catalog_pairs(["dev_fin=prod_fin", "dev_hr=prod_hr"])
        # Sorted by source length descending
        assert ("dev_fin", "prod_fin") in pairs
        assert ("dev_hr", "prod_hr") in pairs

    def test_comma_separated(self):
        pairs = parse_catalog_pairs(["dev_fin=prod_fin,dev_hr=prod_hr"])
        assert len(pairs) == 2

    def test_sorted_longest_first(self):
        pairs = parse_catalog_pairs(["dev=prod", "dev_finance=prod_finance"])
        assert pairs[0] == ("dev_finance", "prod_finance")
        assert pairs[1] == ("dev", "prod")

    def test_empty_list(self):
        pairs = parse_catalog_pairs([])
        assert pairs == []

    def test_whitespace_trimmed(self):
        pairs = parse_catalog_pairs(["  dev_fin = prod_fin  "])
        assert pairs == [("dev_fin", "prod_fin")]


class TestRemapHcl:
    """Tests for remap_hcl()."""

    def test_table_reference_remapped(self):
        text = 'entity_name = "dev_fin.finance.transactions"'
        result = remap_hcl(text, [("dev_fin", "prod_fin")])
        assert "prod_fin.finance.transactions" in result
        assert "dev_fin" not in result

    def test_catalog_field_remapped(self):
        text = '  catalog = "dev_fin"'
        result = remap_hcl(text, [("dev_fin", "prod_fin")])
        assert 'catalog = "prod_fin"' in result

    def test_function_catalog_remapped(self):
        text = '  function_catalog = "dev_fin"'
        result = remap_hcl(text, [("dev_fin", "prod_fin")])
        assert 'function_catalog = "prod_fin"' in result

    def test_bare_quoted_catalog_remapped(self):
        text = 'some_field = "dev_fin"'
        result = remap_hcl(text, [("dev_fin", "prod_fin")])
        assert '"prod_fin"' in result

    def test_multiple_catalogs(self):
        text = """
tag_assignments = [
  { entity_name = "dev_fin.finance.accounts", tag_key = "pii" },
  { entity_name = "dev_hr.people.employees", tag_key = "pii" },
]
"""
        result = remap_hcl(text, [("dev_fin", "prod_fin"), ("dev_hr", "prod_hr")])
        assert "prod_fin.finance.accounts" in result
        assert "prod_hr.people.employees" in result
        assert "dev_fin" not in result
        assert "dev_hr" not in result

    def test_overlapping_catalog_names_longest_first(self):
        """Ensure 'dev_fin_v2' is remapped before 'dev_fin'."""
        text = 'entity_name = "dev_fin_v2.schema.table"'
        pairs = parse_catalog_pairs(["dev_fin=prod_fin", "dev_fin_v2=prod_fin_v2"])
        result = remap_hcl(text, pairs)
        assert "prod_fin_v2.schema.table" in result
        assert "prod_fin.schema" not in result  # Should NOT have been partially matched

    def test_no_change_when_catalog_not_found(self):
        text = 'entity_name = "other_catalog.schema.table"'
        result = remap_hcl(text, [("dev_fin", "prod_fin")])
        assert "other_catalog.schema.table" in result

    def test_preserves_non_catalog_content(self):
        text = """
groups = [
  { name = "analysts" },
]

tag_assignments = [
  { entity_name = "dev_fin.finance.accounts" },
]
"""
        result = remap_hcl(text, [("dev_fin", "prod_fin")])
        assert "analysts" in result  # Non-catalog content preserved
        assert "prod_fin.finance.accounts" in result

    def test_genie_space_configs_remapped(self):
        text = """
genie_space_configs = [
  {
    name = "Finance Analytics"
    uc_tables = [
      "dev_fin.finance.transactions",
      "dev_fin.finance.accounts",
    ]
  },
]
"""
        result = remap_hcl(text, [("dev_fin", "prod_fin")])
        assert "prod_fin.finance.transactions" in result
        assert "prod_fin.finance.accounts" in result
        assert "dev_fin" not in result


class TestRemapSql:
    """Tests for remap_sql()."""

    def test_use_catalog_remapped(self):
        text = "USE CATALOG dev_fin;\n"
        result = remap_sql(text, [("dev_fin", "prod_fin")])
        assert "USE CATALOG prod_fin;" in result

    def test_use_catalog_case_insensitive(self):
        text = "use catalog dev_fin;\n"
        result = remap_sql(text, [("dev_fin", "prod_fin")])
        assert "prod_fin" in result

    def test_catalog_prefixed_identifiers(self):
        text = "CREATE OR REPLACE FUNCTION dev_fin.schema.mask_ssn(val STRING)\n"
        result = remap_sql(text, [("dev_fin", "prod_fin")])
        assert "prod_fin.schema.mask_ssn" in result

    def test_multiple_catalogs_in_sql(self):
        text = """USE CATALOG dev_fin;
CREATE OR REPLACE FUNCTION dev_fin.finance.mask_amount(val DECIMAL)
  RETURNS DECIMAL RETURN ROUND(val, -3);

USE CATALOG dev_hr;
CREATE OR REPLACE FUNCTION dev_hr.people.mask_name(val STRING)
  RETURNS STRING RETURN LEFT(val, 1) || '***';
"""
        result = remap_sql(text, [("dev_fin", "prod_fin"), ("dev_hr", "prod_hr")])
        assert "USE CATALOG prod_fin;" in result
        assert "prod_fin.finance.mask_amount" in result
        assert "USE CATALOG prod_hr;" in result
        assert "prod_hr.people.mask_name" in result

    def test_no_change_when_catalog_not_found(self):
        text = "CREATE OR REPLACE FUNCTION other.schema.fn(val STRING)\n"
        result = remap_sql(text, [("dev_fin", "prod_fin")])
        assert "other.schema.fn" in result

    def test_comments_with_catalog_refs(self):
        text = "-- Masking functions for dev_fin.finance schema\nCREATE OR REPLACE FUNCTION dev_fin.finance.mask_id(v STRING)\n"
        result = remap_sql(text, [("dev_fin", "prod_fin")])
        assert "prod_fin.finance" in result
