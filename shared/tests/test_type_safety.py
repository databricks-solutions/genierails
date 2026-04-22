"""Unit tests for masking function type safety.

Ensures DECIMAL/DATE columns never get STRING-returning masking functions,
the arg count parser handles nested parentheses, and the PII autofix
detects untagged columns.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from validate_abac import parse_sql_function_arg_counts  # noqa: E402
from generate_abac import (  # noqa: E402
    autofix_untagged_pii_columns,
)


# ===========================================================================
#  parse_sql_function_arg_counts — nested parentheses in DECIMAL(18,2)
# ===========================================================================

class TestArgCountParser:

    def test_decimal_18_2_counts_as_one_arg(self, tmp_sql):
        sql = "CREATE OR REPLACE FUNCTION mask_amount_rounded(amount DECIMAL(18,2)) RETURNS DECIMAL(18,2) RETURN ROUND(amount, -2);"
        counts = parse_sql_function_arg_counts(tmp_sql(sql))
        assert counts["mask_amount_rounded"] == 1

    def test_two_decimal_args(self, tmp_sql):
        sql = "CREATE FUNCTION fn(a DECIMAL(18,2), b DECIMAL(10,4)) RETURNS DECIMAL(18,2) RETURN a + b;"
        counts = parse_sql_function_arg_counts(tmp_sql(sql))
        assert counts["fn"] == 2

    def test_zero_args(self, tmp_sql):
        sql = "CREATE FUNCTION filter_aml_compliance() RETURNS BOOLEAN RETURN TRUE;"
        counts = parse_sql_function_arg_counts(tmp_sql(sql))
        assert counts["filter_aml_compliance"] == 0

    def test_simple_string_arg(self, tmp_sql):
        sql = "CREATE FUNCTION mask_redact(val STRING) RETURNS STRING RETURN '[REDACTED]';"
        counts = parse_sql_function_arg_counts(tmp_sql(sql))
        assert counts["mask_redact"] == 1

    def test_date_arg(self, tmp_sql):
        sql = "CREATE FUNCTION mask_date_to_year(dt DATE) RETURNS DATE RETURN DATE_TRUNC('YEAR', dt);"
        counts = parse_sql_function_arg_counts(tmp_sql(sql))
        assert counts["mask_date_to_year"] == 1

    def test_create_or_replace_last_definition_wins(self, tmp_sql):
        sql = """\
CREATE OR REPLACE FUNCTION mask_amount_rounded(a DECIMAL(18,2), precision INT) RETURNS DECIMAL(18,2) RETURN ROUND(a, precision);
CREATE OR REPLACE FUNCTION mask_amount_rounded(amount DECIMAL(18,2)) RETURNS DECIMAL(18,2) RETURN ROUND(amount, -2);
"""
        counts = parse_sql_function_arg_counts(tmp_sql(sql))
        assert counts["mask_amount_rounded"] == 1, "Last CREATE OR REPLACE should win"

    def test_mixed_functions(self, tmp_sql):
        sql = """\
CREATE OR REPLACE FUNCTION mask_amount_rounded(amount DECIMAL(18,2)) RETURNS DECIMAL(18,2) RETURN ROUND(amount, -2);
CREATE OR REPLACE FUNCTION mask_redact(val STRING) RETURNS STRING RETURN '[REDACTED]';
CREATE OR REPLACE FUNCTION filter_aml() RETURNS BOOLEAN RETURN TRUE;
CREATE OR REPLACE FUNCTION mask_two_args(a STRING, b INT) RETURNS STRING RETURN a;
"""
        counts = parse_sql_function_arg_counts(tmp_sql(sql))
        assert counts["mask_amount_rounded"] == 1
        assert counts["mask_redact"] == 1
        assert counts["filter_aml"] == 0
        assert counts["mask_two_args"] == 2

    def test_catalog_schema_prefix(self, tmp_sql):
        sql = "CREATE OR REPLACE FUNCTION dev_bank.retail.mask_amount_rounded(amount DECIMAL(18,2)) RETURNS DECIMAL(18,2) RETURN ROUND(amount, -2);"
        counts = parse_sql_function_arg_counts(tmp_sql(sql))
        assert counts["mask_amount_rounded"] == 1


# ===========================================================================
#  autofix_untagged_pii_columns
# ===========================================================================

class TestUntaggedPiiAutofix:

    def _write_ddl(self, tmp_path, content):
        ddl_dir = tmp_path / "ddl"
        ddl_dir.mkdir(exist_ok=True)
        p = ddl_dir / "_fetched.sql"
        p.write_text(content)
        return p

    def test_detects_email_and_phone(self, tmp_path):
        tfvars = tmp_path / "abac.auto.tfvars"
        tfvars.write_text("""\
tag_assignments = []
fgac_policies = []
""")
        ddl = self._write_ddl(tmp_path, """\
CREATE TABLE dev_bank.retail.customers (
  customer_id BIGINT,
  email STRING,
  phone STRING,
  first_name STRING
);
""")
        n = autofix_untagged_pii_columns(tfvars, ddl_path=ddl)
        assert n == 2  # email + phone
        text = tfvars.read_text()
        assert "masked_email" in text
        assert "masked_phone" in text

    def test_skips_already_tagged_columns(self, tmp_path):
        tfvars = tmp_path / "abac.auto.tfvars"
        tfvars.write_text("""\
tag_assignments = [
  { entity_type = "columns", entity_name = "dev_bank.retail.customers.email", tag_key = "pii_level", tag_value = "masked_email" },
]
fgac_policies = []
""")
        ddl = self._write_ddl(tmp_path, """\
CREATE TABLE dev_bank.retail.customers (
  customer_id BIGINT,
  email STRING,
  phone STRING
);
""")
        n = autofix_untagged_pii_columns(tfvars, ddl_path=ddl)
        assert n == 1  # only phone (email already tagged)
        text = tfvars.read_text()
        assert "masked_phone" in text

    def test_detects_financial_columns(self, tmp_path):
        tfvars = tmp_path / "abac.auto.tfvars"
        tfvars.write_text("""\
tag_assignments = []
fgac_policies = []
""")
        ddl = self._write_ddl(tmp_path, """\
CREATE TABLE dev_bank.retail.accounts (
  account_id BIGINT,
  balance DECIMAL(18,2),
  credit_limit DECIMAL(18,2)
);
""")
        # Financial columns require mask_amount_rounded in SQL to be tagged
        sql = tmp_path / "masking_functions.sql"
        sql.write_text("CREATE FUNCTION mask_amount_rounded(amount DECIMAL(18,2)) RETURNS DECIMAL(18,2) RETURN ROUND(amount, -2);")
        n = autofix_untagged_pii_columns(tfvars, ddl_path=ddl, sql_path=sql)
        assert n == 2  # balance + credit_limit
        text = tfvars.read_text()
        assert "rounded_amounts" in text

    def test_skips_financial_columns_without_overlay(self, tmp_path):
        """Financial columns should NOT be tagged when mask_amount_rounded is not in SQL."""
        tfvars = tmp_path / "abac.auto.tfvars"
        tfvars.write_text("tag_assignments = []\nfgac_policies = []\n")
        ddl = self._write_ddl(tmp_path, """\
CREATE TABLE dev_bank.retail.accounts (
  account_id BIGINT,
  balance DECIMAL(18,2)
);
""")
        n = autofix_untagged_pii_columns(tfvars, ddl_path=ddl)
        assert n == 0  # no mask_amount_rounded → skip financial tags

    def test_detects_anz_identifiers(self, tmp_path):
        tfvars = tmp_path / "abac.auto.tfvars"
        tfvars.write_text("""\
tag_assignments = []
fgac_policies = []
""")
        ddl = self._write_ddl(tmp_path, """\
CREATE TABLE dev_bank.retail.customers (
  customer_id BIGINT,
  tfn STRING,
  medicare_number STRING,
  bsb STRING
);
""")
        n = autofix_untagged_pii_columns(tfvars, ddl_path=ddl)
        assert n == 3
        text = tfvars.read_text()
        assert "masked_tfn" in text
        assert "masked_medicare" in text
        assert "masked_bsb" in text

    def test_no_ddl_returns_zero(self, tmp_path):
        tfvars = tmp_path / "abac.auto.tfvars"
        tfvars.write_text("tag_assignments = []\nfgac_policies = []\n")
        n = autofix_untagged_pii_columns(tfvars, ddl_path=None)
        assert n == 0

    def test_no_pii_columns_returns_zero(self, tmp_path):
        tfvars = tmp_path / "abac.auto.tfvars"
        tfvars.write_text("tag_assignments = []\nfgac_policies = []\n")
        ddl = self._write_ddl(tmp_path, """\
CREATE TABLE dev_bank.retail.products (
  product_id BIGINT,
  product_name STRING,
  category STRING
);
""")
        n = autofix_untagged_pii_columns(tfvars, ddl_path=ddl)
        assert n == 0
