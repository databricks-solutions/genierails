"""Unit tests for the industry overlay feature.

Tests cover:
  - YAML file loading and integrity (financial_services, healthcare, retail)
  - Industry-specific content validation (identifiers, group templates, access patterns)
  - load_industry_overlays() in generate_abac.py
  - build_prompt() industry injection (solo and composed with countries)
  - _load_industry_categories() in validate_abac.py
  - _infer_column_categories() with industry-aware hints
  - FUNCTION_EXPECTED_CATEGORIES dynamic extension

No Databricks, LLM, or Terraform dependency required.
"""
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

from generate_abac import load_industry_overlays, build_prompt, INDUSTRIES_DIR
from validate_abac import (
    _load_industry_categories,
    _infer_column_categories,
    _country_hint_to_category,
    FUNCTION_EXPECTED_CATEGORIES,
)

AVAILABLE_INDUSTRIES = ["financial_services", "healthcare", "retail"]


# ===========================================================================
#  YAML File Integrity
# ===========================================================================

class TestIndustryYamlFileIntegrity:
    """Verify all industry YAML files parse correctly and have required fields."""

    @pytest.mark.parametrize("code", AVAILABLE_INDUSTRIES)
    def test_yaml_file_exists(self, code):
        path = INDUSTRIES_DIR / f"{code}.yaml"
        assert path.exists(), f"Industry overlay file missing: {path}"

    @pytest.mark.parametrize("code", AVAILABLE_INDUSTRIES)
    def test_yaml_parses_successfully(self, code):
        path = INDUSTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        assert isinstance(data, dict)

    @pytest.mark.parametrize("code", AVAILABLE_INDUSTRIES)
    def test_yaml_has_required_top_level_keys(self, code):
        path = INDUSTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        for key in ("code", "name", "regulations", "identifiers",
                     "masking_functions", "prompt_overlay",
                     "group_templates", "access_patterns"):
            assert key in data, f"{code}.yaml missing required key: {key}"

    @pytest.mark.parametrize("code", AVAILABLE_INDUSTRIES)
    def test_identifiers_have_required_fields(self, code):
        path = INDUSTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        for i, ident in enumerate(data["identifiers"]):
            for field in ("name", "column_hints", "category"):
                assert field in ident, (
                    f"{code}.yaml identifiers[{i}] ({ident.get('name', '?')}) "
                    f"missing field: {field}"
                )
            assert isinstance(ident["column_hints"], list)
            assert len(ident["column_hints"]) > 0

    @pytest.mark.parametrize("code", AVAILABLE_INDUSTRIES)
    def test_masking_functions_have_required_fields(self, code):
        path = INDUSTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        for i, fn in enumerate(data["masking_functions"]):
            for field in ("name", "signature", "comment", "body"):
                assert field in fn, (
                    f"{code}.yaml masking_functions[{i}] ({fn.get('name', '?')}) "
                    f"missing field: {field}"
                )

    @pytest.mark.parametrize("code", AVAILABLE_INDUSTRIES)
    def test_prompt_overlay_is_nonempty(self, code):
        path = INDUSTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        overlay = data["prompt_overlay"]
        assert isinstance(overlay, str)
        assert len(overlay) > 100, f"{code}.yaml prompt_overlay too short"

    @pytest.mark.parametrize("code", AVAILABLE_INDUSTRIES)
    def test_group_templates_have_required_fields(self, code):
        path = INDUSTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        templates = data["group_templates"]
        assert isinstance(templates, dict)
        assert len(templates) > 0, f"{code}.yaml has no group_templates"
        for name, defn in templates.items():
            assert "description" in defn, (
                f"{code}.yaml group_templates[{name}] missing 'description'"
            )
            assert "access_level" in defn, (
                f"{code}.yaml group_templates[{name}] missing 'access_level'"
            )

    @pytest.mark.parametrize("code", AVAILABLE_INDUSTRIES)
    def test_access_patterns_have_required_fields(self, code):
        path = INDUSTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        patterns = data["access_patterns"]
        assert isinstance(patterns, list)
        assert len(patterns) > 0, f"{code}.yaml has no access_patterns"
        for i, pat in enumerate(patterns):
            assert "name" in pat, f"{code}.yaml access_patterns[{i}] missing 'name'"
            assert "description" in pat, f"{code}.yaml access_patterns[{i}] missing 'description'"
            assert "guidance" in pat, f"{code}.yaml access_patterns[{i}] missing 'guidance'"

    @pytest.mark.parametrize("code", AVAILABLE_INDUSTRIES)
    def test_masking_functions_referenced_by_identifiers_exist(self, code):
        """Every masking_function referenced in identifiers must be defined."""
        path = INDUSTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        defined_fns = {fn["name"] for fn in data["masking_functions"]}
        base_fns = {"mask_email", "mask_phone", "mask_account_number",
                    "mask_redact", "mask_hash", "mask_nullify"}
        for ident in data["identifiers"]:
            fn = ident.get("masking_function")
            if fn is None:
                continue
            if fn not in base_fns:
                assert fn in defined_fns, (
                    f"{code}.yaml identifier '{ident['name']}' references "
                    f"masking_function '{fn}' not defined in masking_functions"
                )


# ===========================================================================
#  Financial Services Content
# ===========================================================================

class TestFinancialServicesContent:
    """Verify financial_services overlay contains expected identifiers."""

    def _load(self):
        with open(INDUSTRIES_DIR / "financial_services.yaml") as f:
            return yaml.safe_load(f)

    def test_has_core_identifiers(self):
        data = self._load()
        names = {i["name"] for i in data["identifiers"]}
        assert "Account Number" in names
        assert "Credit Card Number" in names
        assert "Transaction Amount" in names

    def test_account_column_hints(self):
        data = self._load()
        acct = next(i for i in data["identifiers"] if i["name"] == "Account Number")
        assert "account_number" in acct["column_hints"]
        assert "acct_no" in acct["column_hints"]

    def test_has_fraud_team_group_template(self):
        data = self._load()
        assert "fraud_team" in data["group_templates"]
        assert data["group_templates"]["fraud_team"]["access_level"] == "full"

    def test_has_analyst_group_template(self):
        data = self._load()
        assert "analyst" in data["group_templates"]
        assert data["group_templates"]["analyst"]["access_level"] == "masked"

    def test_has_marketing_group_template(self):
        data = self._load()
        assert "marketing" in data["group_templates"]
        assert data["group_templates"]["marketing"]["access_level"] == "anonymized"

    def test_has_pci_access_pattern(self):
        data = self._load()
        pattern_names = {p["name"] for p in data["access_patterns"]}
        assert "pci_isolation" in pattern_names

    def test_overlay_mentions_pci_dss(self):
        data = self._load()
        overlay = data["prompt_overlay"]
        assert "PCI DSS" in overlay
        assert "mask_card_last4" in overlay


# ===========================================================================
#  Healthcare Content
# ===========================================================================

class TestHealthcareContent:
    """Verify healthcare overlay contains expected identifiers."""

    def _load(self):
        with open(INDUSTRIES_DIR / "healthcare.yaml") as f:
            return yaml.safe_load(f)

    def test_has_core_identifiers(self):
        data = self._load()
        names = {i["name"] for i in data["identifiers"]}
        assert "Patient ID" in names
        assert "Diagnosis Code" in names
        assert "Medical Record Number" in names

    def test_patient_id_column_hints(self):
        data = self._load()
        pid = next(i for i in data["identifiers"] if i["name"] == "Patient ID")
        assert "patient_id" in pid["column_hints"]
        assert "mrn" in pid["column_hints"]

    def test_has_physician_group_template(self):
        data = self._load()
        assert "physician" in data["group_templates"]
        assert data["group_templates"]["physician"]["access_level"] == "full"

    def test_has_analyst_deidentified(self):
        data = self._load()
        assert "analyst" in data["group_templates"]
        assert data["group_templates"]["analyst"]["access_level"] == "de-identified"

    def test_has_break_glass_access_pattern(self):
        data = self._load()
        pattern_names = {p["name"] for p in data["access_patterns"]}
        assert "break_glass" in pattern_names

    def test_break_glass_mentions_audit(self):
        data = self._load()
        bg = next(p for p in data["access_patterns"] if p["name"] == "break_glass")
        assert "audit" in bg["guidance"].lower()

    def test_overlay_mentions_hipaa(self):
        data = self._load()
        overlay = data["prompt_overlay"]
        assert "HIPAA" in overlay
        assert "mask_patient_id" in overlay


# ===========================================================================
#  Retail Content
# ===========================================================================

class TestRetailContent:
    """Verify retail overlay contains expected identifiers."""

    def _load(self):
        with open(INDUSTRIES_DIR / "retail.yaml") as f:
            return yaml.safe_load(f)

    def test_has_core_identifiers(self):
        data = self._load()
        names = {i["name"] for i in data["identifiers"]}
        assert "Customer Email" in names
        assert "Phone Number" in names
        assert "Customer Name" in names

    def test_email_column_hints(self):
        data = self._load()
        email = next(i for i in data["identifiers"] if i["name"] == "Customer Email")
        assert "email" in email["column_hints"]
        assert "email_address" in email["column_hints"]

    def test_has_marketing_group_template(self):
        data = self._load()
        assert "marketing" in data["group_templates"]
        assert data["group_templates"]["marketing"]["access_level"] == "anonymized"

    def test_has_ops_group_template(self):
        data = self._load()
        assert "ops" in data["group_templates"]
        assert data["group_templates"]["ops"]["access_level"] == "partial"

    def test_has_right_to_deletion_pattern(self):
        data = self._load()
        pattern_names = {p["name"] for p in data["access_patterns"]}
        assert "right_to_deletion" in pattern_names

    def test_overlay_mentions_ccpa(self):
        data = self._load()
        overlay = data["prompt_overlay"]
        assert "CCPA" in overlay

    def test_uses_hashing_for_names(self):
        data = self._load()
        name_ident = next(i for i in data["identifiers"] if i["name"] == "Customer Name")
        assert name_ident["masking_function"] == "mask_name_hash"


# ===========================================================================
#  load_industry_overlays (generate_abac.py)
# ===========================================================================

class TestLoadIndustryOverlays:
    """Test the load_industry_overlays function in generate_abac.py."""

    def test_loads_single_industry(self):
        result = load_industry_overlays(["financial_services"])
        assert isinstance(result, str)
        assert len(result) > 0
        assert "Financial Services" in result

    def test_loads_multiple_industries(self):
        result = load_industry_overlays(["financial_services", "healthcare", "retail"])
        assert "Financial Services" in result
        assert "Healthcare" in result or "HIPAA" in result
        assert "Retail" in result or "CCPA" in result

    def test_case_insensitive(self):
        result = load_industry_overlays(["FINANCIAL_SERVICES"])
        assert "Financial Services" in result

    def test_missing_industry_exits(self):
        with pytest.raises(SystemExit):
            load_industry_overlays(["NONEXISTENT"])

    def test_returns_empty_for_empty_list(self):
        result = load_industry_overlays([])
        assert result == ""

    def test_includes_group_templates(self):
        result = load_industry_overlays(["financial_services"])
        assert "fraud_team" in result
        assert "analyst" in result

    def test_includes_access_patterns(self):
        result = load_industry_overlays(["healthcare"])
        assert "break_glass" in result


# ===========================================================================
#  build_prompt with industries (generate_abac.py)
# ===========================================================================

class TestBuildPromptWithIndustries:
    """Test that build_prompt correctly injects industry overlays."""

    DDL = "CREATE TABLE test.schema.t (id INT, account_number STRING);"

    def test_no_industries_produces_base_prompt(self):
        prompt = build_prompt(self.DDL)
        assert "Industry-Specific" not in prompt

    def test_industry_overlay_injected_before_my_tables(self):
        prompt = build_prompt(self.DDL, industries=["financial_services"])
        overlay_pos = prompt.find("Industry-Specific Identifiers")
        tables_pos = prompt.find("### MY TABLES")
        assert overlay_pos != -1, "Industry overlay not found in prompt"
        assert tables_pos != -1, "MY TABLES section not found"
        assert overlay_pos < tables_pos

    def test_multiple_industries_all_injected(self):
        prompt = build_prompt(self.DDL, industries=["financial_services", "healthcare", "retail"])
        assert "PCI DSS" in prompt
        assert "HIPAA" in prompt
        assert "CCPA" in prompt

    def test_industry_with_mode_governance(self):
        prompt = build_prompt(self.DDL, mode="governance", industries=["healthcare"])
        assert "GOVERNANCE-ONLY MODE" in prompt
        assert "HIPAA" in prompt

    def test_none_industries_no_overlay(self):
        prompt = build_prompt(self.DDL, industries=None)
        assert "Industry-Specific" not in prompt


# ===========================================================================
#  build_prompt with both countries and industries
# ===========================================================================

class TestBuildPromptWithBothOverlays:
    """Test that country and industry overlays compose correctly."""

    DDL = "CREATE TABLE test.schema.t (id INT, tfn STRING, patient_id STRING);"

    def test_both_overlays_present(self):
        prompt = build_prompt(self.DDL, countries=["ANZ"], industries=["healthcare"])
        assert "Country-Specific Identifiers" in prompt
        assert "Industry-Specific Identifiers" in prompt

    def test_country_appears_before_industry(self):
        prompt = build_prompt(self.DDL, countries=["ANZ"], industries=["healthcare"])
        country_pos = prompt.find("Country-Specific Identifiers")
        industry_pos = prompt.find("Industry-Specific Identifiers")
        assert country_pos < industry_pos

    def test_both_before_my_tables(self):
        prompt = build_prompt(self.DDL, countries=["ANZ"], industries=["financial_services"])
        country_pos = prompt.find("Country-Specific")
        industry_pos = prompt.find("Industry-Specific")
        tables_pos = prompt.find("### MY TABLES")
        assert country_pos < tables_pos
        assert industry_pos < tables_pos


# ===========================================================================
#  _load_industry_categories (validate_abac.py)
# ===========================================================================

class TestLoadIndustryCategories:
    """Test the industry category loading for validation."""

    def test_loads_financial_hints(self):
        hints, func_cats = _load_industry_categories(["financial_services"])
        assert "account_number" in hints
        assert hints["account_number"] == "financial_id"
        assert "card_number" in hints
        assert hints["card_number"] == "payment_card"

    def test_loads_healthcare_hints(self):
        hints, func_cats = _load_industry_categories(["healthcare"])
        assert "patient_id" in hints
        assert hints["patient_id"] == "patient_id"
        assert "diagnosis" in hints
        assert hints["diagnosis"] == "diagnosis"

    def test_loads_retail_hints(self):
        hints, func_cats = _load_industry_categories(["retail"])
        assert "email" in hints
        assert hints["email"] == "contact_info"
        assert "phone" in hints
        assert hints["phone"] == "contact_info"

    def test_loads_function_categories(self):
        _, func_cats = _load_industry_categories(["financial_services"])
        assert "mask_account_last4" in func_cats
        assert "financial_id" in func_cats["mask_account_last4"]
        assert "mask_card_last4" in func_cats
        assert "payment_card" in func_cats["mask_card_last4"]

    def test_multi_industry_merges(self):
        hints, func_cats = _load_industry_categories(["financial_services", "healthcare", "retail"])
        assert "account_number" in hints  # financial
        assert "patient_id" in hints      # healthcare
        assert "loyalty_number" in hints   # retail

    def test_missing_industry_skipped(self):
        hints, func_cats = _load_industry_categories(["NONEXISTENT"])
        assert hints == {}
        assert func_cats == {}

    def test_empty_list_returns_empty(self):
        hints, func_cats = _load_industry_categories([])
        assert hints == {}
        assert func_cats == {}


# ===========================================================================
#  _infer_column_categories with industry hints
# ===========================================================================

class TestInferColumnCategoriesWithIndustry:
    """Test that _infer_column_categories picks up industry-specific patterns."""

    @pytest.fixture(autouse=True)
    def _setup_industry_hints(self):
        """Load industry hints into the global dict, then restore after test."""
        original = _country_hint_to_category.copy()
        hints, _ = _load_industry_categories(["financial_services", "healthcare", "retail"])
        _country_hint_to_category.update(hints)
        yield
        _country_hint_to_category.clear()
        _country_hint_to_category.update(original)

    def test_account_number_detected(self):
        cats = _infer_column_categories("catalog.schema.table.account_number")
        assert "financial_id" in cats

    def test_patient_id_detected(self):
        cats = _infer_column_categories("catalog.schema.table.patient_id")
        assert "patient_id" in cats

    def test_loyalty_number_detected(self):
        cats = _infer_column_categories("catalog.schema.table.loyalty_number")
        assert "customer_profile" in cats

    def test_existing_us_patterns_still_work(self):
        """Industry hints should not break existing US pattern detection."""
        assert "email" in _infer_column_categories("cat.sch.tbl.email")
        assert "phone" in _infer_column_categories("cat.sch.tbl.phone")
        assert "ssn" in _infer_column_categories("cat.sch.tbl.ssn")


# ===========================================================================
#  FUNCTION_EXPECTED_CATEGORIES extension
# ===========================================================================

class TestFunctionExpectedCategoriesExtensionIndustry:
    """Test that industry overlays extend the function-to-category mapping."""

    def test_industry_categories_can_be_merged(self):
        """Verify that industry function categories can be merged without error."""
        _, func_cats = _load_industry_categories(["financial_services", "healthcare", "retail"])
        original = FUNCTION_EXPECTED_CATEGORIES.copy()
        FUNCTION_EXPECTED_CATEGORIES.update(func_cats)
        try:
            assert "mask_account_last4" in FUNCTION_EXPECTED_CATEGORIES
            assert "mask_patient_id" in FUNCTION_EXPECTED_CATEGORIES
            assert "mask_name_hash" in FUNCTION_EXPECTED_CATEGORIES
            assert "financial_id" in FUNCTION_EXPECTED_CATEGORIES["mask_account_last4"]
            assert "patient_id" in FUNCTION_EXPECTED_CATEGORIES["mask_patient_id"]
        finally:
            FUNCTION_EXPECTED_CATEGORIES.clear()
            FUNCTION_EXPECTED_CATEGORIES.update(original)
