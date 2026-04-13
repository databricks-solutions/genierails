"""Unit tests for the APJ country/region overlay feature.

Tests cover:
  - YAML file loading and integrity (ANZ, IN, SEA)
  - load_country_overlays() in generate_abac.py
  - build_prompt() country injection
  - _load_country_categories() in validate_abac.py
  - _infer_column_categories() with country-aware hints
  - FUNCTION_EXPECTED_CATEGORIES dynamic extension

No Databricks, LLM, or Terraform dependency required.
"""
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

from generate_abac import load_country_overlays, build_prompt, COUNTRIES_DIR
from validate_abac import (
    _load_country_categories,
    _infer_column_categories,
    _country_hint_to_category,
    FUNCTION_EXPECTED_CATEGORIES,
)

AVAILABLE_COUNTRIES = ["ANZ", "IN", "SEA"]


# ===========================================================================
#  YAML File Integrity
# ===========================================================================

class TestYamlFileIntegrity:
    """Verify all country YAML files parse correctly and have required fields."""

    @pytest.mark.parametrize("code", AVAILABLE_COUNTRIES)
    def test_yaml_file_exists(self, code):
        path = COUNTRIES_DIR / f"{code}.yaml"
        assert path.exists(), f"Country overlay file missing: {path}"

    @pytest.mark.parametrize("code", AVAILABLE_COUNTRIES)
    def test_yaml_parses_successfully(self, code):
        path = COUNTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        assert isinstance(data, dict)

    @pytest.mark.parametrize("code", AVAILABLE_COUNTRIES)
    def test_yaml_has_required_top_level_keys(self, code):
        path = COUNTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        for key in ("code", "name", "regulations", "identifiers",
                     "masking_functions", "prompt_overlay"):
            assert key in data, f"{code}.yaml missing required key: {key}"

    @pytest.mark.parametrize("code", AVAILABLE_COUNTRIES)
    def test_identifiers_have_required_fields(self, code):
        path = COUNTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        for i, ident in enumerate(data["identifiers"]):
            for field in ("name", "country", "column_hints", "category"):
                assert field in ident, (
                    f"{code}.yaml identifiers[{i}] ({ident.get('name', '?')}) "
                    f"missing field: {field}"
                )
            assert isinstance(ident["column_hints"], list)
            assert len(ident["column_hints"]) > 0

    @pytest.mark.parametrize("code", AVAILABLE_COUNTRIES)
    def test_masking_functions_have_required_fields(self, code):
        path = COUNTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        for i, fn in enumerate(data["masking_functions"]):
            for field in ("name", "signature", "comment", "body"):
                assert field in fn, (
                    f"{code}.yaml masking_functions[{i}] ({fn.get('name', '?')}) "
                    f"missing field: {field}"
                )

    @pytest.mark.parametrize("code", AVAILABLE_COUNTRIES)
    def test_prompt_overlay_is_nonempty(self, code):
        path = COUNTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        overlay = data["prompt_overlay"]
        assert isinstance(overlay, str)
        assert len(overlay) > 100, f"{code}.yaml prompt_overlay too short"

    @pytest.mark.parametrize("code", AVAILABLE_COUNTRIES)
    def test_masking_functions_referenced_by_identifiers_exist(self, code):
        """Every masking_function referenced in identifiers must be defined."""
        path = COUNTRIES_DIR / f"{code}.yaml"
        with open(path) as f:
            data = yaml.safe_load(f)
        defined_fns = {fn["name"] for fn in data["masking_functions"]}
        for ident in data["identifiers"]:
            fn = ident.get("masking_function")
            if fn is None:
                continue  # IFSC etc. intentionally have no masking function
            # Allow reuse of base library functions (mask_email, mask_phone, etc.)
            base_fns = {"mask_email", "mask_phone", "mask_account_number",
                        "mask_redact", "mask_hash", "mask_nullify",
                        "mask_passport"}
            if fn not in base_fns:
                assert fn in defined_fns, (
                    f"{code}.yaml identifier '{ident['name']}' references "
                    f"masking_function '{fn}' not defined in masking_functions"
                )


# ===========================================================================
#  ANZ-Specific Content
# ===========================================================================

class TestANZContent:
    """Verify ANZ overlay contains expected Australian and NZ identifiers."""

    def _load(self):
        with open(COUNTRIES_DIR / "ANZ.yaml") as f:
            return yaml.safe_load(f)

    def test_has_australian_identifiers(self):
        data = self._load()
        names = {i["name"] for i in data["identifiers"]}
        assert "Tax File Number (TFN)" in names
        assert "Medicare Number" in names
        assert "BSB (Bank State Branch)" in names

    def test_has_nz_identifiers(self):
        data = self._load()
        names = {i["name"] for i in data["identifiers"]}
        assert "IRD Number" in names
        assert "NHI Number (National Health Index)" in names

    def test_tfn_column_hints(self):
        data = self._load()
        tfn = next(i for i in data["identifiers"] if i["name"] == "Tax File Number (TFN)")
        assert "tfn" in tfn["column_hints"]
        assert "tax_file_number" in tfn["column_hints"]

    def test_overlay_mentions_both_countries(self):
        data = self._load()
        overlay = data["prompt_overlay"]
        assert "Australia" in overlay
        assert "New Zealand" in overlay
        assert "mask_tfn" in overlay
        assert "mask_ird" in overlay


# ===========================================================================
#  IN-Specific Content
# ===========================================================================

class TestINContent:
    """Verify India overlay contains expected identifiers."""

    def _load(self):
        with open(COUNTRIES_DIR / "IN.yaml") as f:
            return yaml.safe_load(f)

    def test_has_core_identifiers(self):
        data = self._load()
        names = {i["name"] for i in data["identifiers"]}
        assert "Aadhaar" in names
        assert "PAN (Permanent Account Number)" in names
        assert "GSTIN (GST Identification Number)" in names

    def test_aadhaar_column_hints_include_common_misspelling(self):
        data = self._load()
        aadhaar = next(i for i in data["identifiers"] if i["name"] == "Aadhaar")
        assert "aadhaar" in aadhaar["column_hints"]
        assert "aadhar" in aadhaar["column_hints"]  # common misspelling

    def test_pan_uses_india_specific_function(self):
        """PAN should use mask_pan_india, NOT mask_credit_card (disambiguation)."""
        data = self._load()
        pan = next(i for i in data["identifiers"]
                   if i["name"] == "PAN (Permanent Account Number)")
        assert pan["masking_function"] == "mask_pan_india"

    def test_overlay_warns_about_pan_disambiguation(self):
        data = self._load()
        overlay = data["prompt_overlay"]
        assert "mask_pan_india" in overlay
        # Should warn not to confuse with credit card PAN
        assert "credit card" in overlay.lower() or "NOT" in overlay


# ===========================================================================
#  SEA-Specific Content
# ===========================================================================

class TestSEAContent:
    """Verify SEA overlay contains expected identifiers for all 6 countries."""

    def _load(self):
        with open(COUNTRIES_DIR / "SEA.yaml") as f:
            return yaml.safe_load(f)

    def test_has_singapore_identifiers(self):
        data = self._load()
        names = {i["name"] for i in data["identifiers"]}
        assert "NRIC (National Registration Identity Card)" in names
        assert "FIN (Foreign Identification Number)" in names

    def test_has_malaysia_identifiers(self):
        data = self._load()
        names = {i["name"] for i in data["identifiers"]}
        assert "MyKad (Malaysian NRIC)" in names
        assert "EPF Number (KWSP)" in names

    def test_has_thailand_identifiers(self):
        data = self._load()
        names = {i["name"] for i in data["identifiers"]}
        assert "Thai National ID" in names
        assert "Thai Tax ID" in names

    def test_has_indonesia_identifiers(self):
        data = self._load()
        names = {i["name"] for i in data["identifiers"]}
        assert "NIK (Nomor Induk Kependudukan)" in names
        assert "NPWP (Tax ID)" in names
        assert "BPJS Number (Social Insurance)" in names

    def test_has_philippines_identifiers(self):
        data = self._load()
        names = {i["name"] for i in data["identifiers"]}
        assert "PhilSys ID (Philippine Identification System)" in names
        assert "SSS Number (Social Security System)" in names

    def test_has_vietnam_identifiers(self):
        data = self._load()
        names = {i["name"] for i in data["identifiers"]}
        assert "CCCD (Citizen Identity Card)" in names
        assert "MST (Tax Code)" in names

    def test_mykad_warns_about_dob_encoding(self):
        """MyKad overlay should warn that first 6 digits encode date of birth."""
        data = self._load()
        overlay = data["prompt_overlay"]
        assert "DOB" in overlay or "date of birth" in overlay.lower() or "birthdate" in overlay.lower()

    def test_nik_warns_about_embedded_birthdate(self):
        """Indonesian NIK overlay should warn about embedded birthdate."""
        data = self._load()
        overlay = data["prompt_overlay"]
        assert "NIK" in overlay
        assert "birthdate" in overlay.lower() or "birthdate" in overlay or "encode" in overlay.lower()

    def test_overlay_disambiguates_sg_vs_my_nric(self):
        data = self._load()
        overlay = data["prompt_overlay"]
        assert "9 char" in overlay.lower() or "9 characters" in overlay.lower()
        assert "12 digit" in overlay.lower() or "12 digits" in overlay.lower()

    def test_overlay_covers_all_six_regulations(self):
        data = self._load()
        overlay = data["prompt_overlay"]
        assert "PDPA 2012" in overlay  # Singapore
        assert "PDPA 2010" in overlay  # Malaysia
        assert "B.E. 2562" in overlay or "Thailand" in overlay  # Thailand
        assert "PDP Law" in overlay or "27/2022" in overlay  # Indonesia
        assert "RA 10173" in overlay or "DPA 2012" in overlay  # Philippines
        assert "Decree 13" in overlay or "13/2023" in overlay  # Vietnam


# ===========================================================================
#  load_country_overlays (generate_abac.py)
# ===========================================================================

class TestLoadCountryOverlays:
    """Test the load_country_overlays function in generate_abac.py."""

    def test_loads_single_country(self):
        result = load_country_overlays(["ANZ"])
        assert isinstance(result, str)
        assert len(result) > 0
        assert "TFN" in result

    def test_loads_multiple_countries(self):
        result = load_country_overlays(["ANZ", "IN", "SEA"])
        assert "TFN" in result
        assert "Aadhaar" in result
        assert "NRIC" in result

    def test_case_insensitive(self):
        result = load_country_overlays(["anz"])
        assert "TFN" in result

    def test_missing_country_exits(self):
        with pytest.raises(SystemExit):
            load_country_overlays(["NONEXISTENT"])

    def test_returns_empty_for_empty_list(self):
        result = load_country_overlays([])
        assert result == ""

    def test_overlay_contains_masking_function_signatures(self):
        result = load_country_overlays(["ANZ"])
        assert "mask_tfn" in result
        assert "mask_medicare" in result
        assert "mask_bsb" in result
        assert "mask_ird" in result


# ===========================================================================
#  build_prompt with countries (generate_abac.py)
# ===========================================================================

class TestBuildPromptWithCountries:
    """Test that build_prompt correctly injects country overlays."""

    DDL = "CREATE TABLE test.schema.t (id INT, tfn STRING);"

    def test_no_countries_produces_base_prompt(self):
        prompt = build_prompt(self.DDL)
        assert "Country-Specific" not in prompt
        assert "TFN" not in prompt

    def test_country_overlay_injected_before_my_tables(self):
        prompt = build_prompt(self.DDL, countries=["ANZ"])
        # Overlay should appear before MY TABLES section
        overlay_pos = prompt.find("Country-Specific Identifiers")
        tables_pos = prompt.find("### MY TABLES")
        assert overlay_pos != -1, "Country overlay not found in prompt"
        assert tables_pos != -1, "MY TABLES section not found"
        assert overlay_pos < tables_pos

    def test_multiple_countries_all_injected(self):
        prompt = build_prompt(self.DDL, countries=["ANZ", "IN", "SEA"])
        assert "Australia" in prompt
        assert "India" in prompt or "Aadhaar" in prompt
        assert "Singapore" in prompt or "NRIC" in prompt

    def test_country_with_mode_governance(self):
        prompt = build_prompt(self.DDL, mode="governance", countries=["ANZ"])
        assert "GOVERNANCE-ONLY MODE" in prompt
        assert "TFN" in prompt  # country overlay still present

    def test_country_with_mode_genie(self):
        prompt = build_prompt(self.DDL, mode="genie", countries=["IN"])
        assert "GENIE-ONLY MODE" in prompt
        assert "Aadhaar" in prompt  # country overlay still present

    def test_country_with_groups(self):
        prompt = build_prompt(
            self.DDL,
            group_names=["Analyst", "Admin"],
            countries=["SEA"],
        )
        assert "NRIC" in prompt
        assert "REQUIRED GROUP NAMES" in prompt

    def test_none_countries_no_overlay(self):
        prompt = build_prompt(self.DDL, countries=None)
        assert "Country-Specific" not in prompt


# ===========================================================================
#  _load_country_categories (validate_abac.py)
# ===========================================================================

class TestLoadCountryCategories:
    """Test the country category loading for validation."""

    def test_loads_anz_hints(self):
        hints, func_cats = _load_country_categories(["ANZ"])
        assert "tfn" in hints
        assert hints["tfn"] == "government_id"
        assert "medicare" in hints
        assert hints["medicare"] == "health_id"
        assert "bsb" in hints
        assert hints["bsb"] == "financial_id"

    def test_loads_india_hints(self):
        hints, func_cats = _load_country_categories(["IN"])
        assert "aadhaar" in hints
        assert "aadhar" in hints  # misspelling
        assert hints["aadhaar"] == "government_id"
        assert "pan" in hints

    def test_loads_sea_hints(self):
        hints, func_cats = _load_country_categories(["SEA"])
        assert "nric" in hints
        assert "mykad" in hints
        assert hints["nric"] == "government_id"
        assert hints["mykad"] == "government_id"

    def test_loads_function_categories(self):
        _, func_cats = _load_country_categories(["ANZ"])
        assert "mask_tfn" in func_cats
        assert "government_id" in func_cats["mask_tfn"]
        assert "mask_medicare" in func_cats
        assert "health_id" in func_cats["mask_medicare"]

    def test_multi_country_merges(self):
        hints, func_cats = _load_country_categories(["ANZ", "IN", "SEA"])
        # Should have hints from all three
        assert "tfn" in hints        # ANZ
        assert "aadhaar" in hints    # IN
        assert "mykad" in hints      # SEA

    def test_missing_country_skipped(self):
        """Missing country file should not crash, just skip."""
        hints, func_cats = _load_country_categories(["NONEXISTENT"])
        assert hints == {}
        assert func_cats == {}

    def test_empty_list_returns_empty(self):
        hints, func_cats = _load_country_categories([])
        assert hints == {}
        assert func_cats == {}


# ===========================================================================
#  _infer_column_categories with country hints
# ===========================================================================

class TestInferColumnCategoriesWithCountry:
    """Test that _infer_column_categories picks up country-specific patterns."""

    @pytest.fixture(autouse=True)
    def _setup_country_hints(self):
        """Load ANZ+IN+SEA hints into the global dict, then restore after test."""
        original = _country_hint_to_category.copy()
        hints, _ = _load_country_categories(["ANZ", "IN", "SEA"])
        _country_hint_to_category.update(hints)
        yield
        _country_hint_to_category.clear()
        _country_hint_to_category.update(original)

    def test_tfn_detected_as_government_id(self):
        cats = _infer_column_categories("catalog.schema.table.tax_file_number")
        assert "government_id" in cats

    def test_medicare_detected_as_health_id(self):
        cats = _infer_column_categories("catalog.schema.table.medicare_number")
        assert "health_id" in cats

    def test_bsb_detected_as_financial_id(self):
        cats = _infer_column_categories("catalog.schema.table.bsb")
        assert "financial_id" in cats

    def test_aadhaar_detected_as_government_id(self):
        cats = _infer_column_categories("catalog.schema.table.aadhaar_number")
        assert "government_id" in cats

    def test_nric_detected_as_government_id(self):
        cats = _infer_column_categories("catalog.schema.table.nric")
        assert "government_id" in cats

    def test_mykad_detected_as_government_id(self):
        cats = _infer_column_categories("catalog.schema.table.mykad")
        assert "government_id" in cats

    def test_ird_detected_as_government_id(self):
        cats = _infer_column_categories("catalog.schema.table.ird_number")
        assert "government_id" in cats

    def test_existing_us_patterns_still_work(self):
        """Country hints should not break existing US pattern detection."""
        assert "email" in _infer_column_categories("cat.sch.tbl.email")
        assert "phone" in _infer_column_categories("cat.sch.tbl.phone")
        assert "ssn" in _infer_column_categories("cat.sch.tbl.ssn")
        assert "card" in _infer_column_categories("cat.sch.tbl.credit_card")

    def test_unknown_column_returns_generic(self):
        cats = _infer_column_categories("catalog.schema.table.random_field")
        assert cats == {"generic"}

    def test_no_country_hints_loaded_returns_base_only(self):
        """When _country_hint_to_category is empty, only base US patterns work."""
        _country_hint_to_category.clear()
        cats = _infer_column_categories("catalog.schema.table.tfn")
        # 'tfn' has no US base pattern match → should be generic
        assert cats == {"generic"}


# ===========================================================================
#  FUNCTION_EXPECTED_CATEGORIES extension
# ===========================================================================

class TestFunctionExpectedCategoriesExtension:
    """Test that country overlays extend the function-to-category mapping."""

    def test_base_categories_exist(self):
        """Verify existing US categories are still present."""
        assert "mask_email" in FUNCTION_EXPECTED_CATEGORIES
        assert "mask_ssn" in FUNCTION_EXPECTED_CATEGORIES

    def test_country_categories_can_be_merged(self):
        """Verify that country function categories can be merged without error."""
        _, func_cats = _load_country_categories(["ANZ", "IN", "SEA"])
        # Temporarily merge
        original = FUNCTION_EXPECTED_CATEGORIES.copy()
        FUNCTION_EXPECTED_CATEGORIES.update(func_cats)
        try:
            assert "mask_tfn" in FUNCTION_EXPECTED_CATEGORIES
            assert "mask_aadhaar" in FUNCTION_EXPECTED_CATEGORIES
            assert "mask_nric" in FUNCTION_EXPECTED_CATEGORIES
            assert "mask_mykad" in FUNCTION_EXPECTED_CATEGORIES
            assert "government_id" in FUNCTION_EXPECTED_CATEGORIES["mask_tfn"]
        finally:
            FUNCTION_EXPECTED_CATEGORIES.clear()
            FUNCTION_EXPECTED_CATEGORIES.update(original)
