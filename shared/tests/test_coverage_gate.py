import subprocess
import sys
from pathlib import Path

import generate_abac
from treatment_derivation import derive_treatment_model, load_treatment_config
from validate_abac import ValidationResult, validate_coverage_gate


def _covered_config():
    cfg = {
        "tag_policies": [],
        "tag_assignments": [{
            "entity_type": "columns", "entity_name": "cat.sch.people.email",
            "tag_key": "pii_level", "tag_value": "masked_email",
        }],
        "fgac_policies": [],
    }
    return derive_treatment_model(cfg, load_treatment_config())[0]


def test_coverage_gate_passes_fully_covered_classification_set():
    cfg = _covered_config()
    result = ValidationResult()
    validate_coverage_gate(cfg, {"mask_email"}, "", result)
    assert result.passed
    assert "fully protected" in result.info[0]


def test_coverage_gate_passes_correctly_classified_card_and_amount_columns():
    cfg = {
        "tag_policies": [],
        "tag_assignments": [
            {"entity_type": "columns", "entity_name": "cat.sch.payments.credit_card_number", "tag_key": "pci_level", "tag_value": "masked_card_last4"},
            {"entity_type": "columns", "entity_name": "cat.sch.payments.amount", "tag_key": "financial_sensitivity", "tag_value": "rounded_amounts"},
        ],
        "fgac_policies": [],
    }
    derived, _ = derive_treatment_model(cfg, load_treatment_config())
    result = ValidationResult()
    validate_coverage_gate(
        derived, {"mask_credit_card_last4", "mask_amount_rounded"}, "", result,
    )
    assert result.passed, result.errors


def test_coverage_gate_rejects_treatment_only_column_without_mask():
    cfg = {
        "tag_assignments": [{
            "entity_type": "columns", "entity_name": "cat.sch.people.email",
            "tag_key": "gr_treatment", "tag_value": "email_partial",
        }],
        "fgac_policies": [],
    }
    result = ValidationResult()
    validate_coverage_gate(cfg, set(), "", result)
    assert not result.passed
    assert any("no covering column-mask policy" in error for error in result.errors)


def test_coverage_gate_rejects_treatment_only_column_with_missing_function():
    cfg = {
        "tag_assignments": [{
            "entity_type": "columns", "entity_name": "cat.sch.people.email",
            "tag_key": "gr_treatment", "tag_value": "email_partial",
        }],
        "fgac_policies": [{
            "name": "email", "policy_type": "POLICY_TYPE_COLUMN_MASK",
            "catalog": "cat", "match_condition": "hasTagValue('gr_treatment', 'email_partial')",
            "function_name": "mask_email",
        }],
    }
    result = ValidationResult()
    validate_coverage_gate(cfg, set(), "", result)
    assert not result.passed
    assert any("mask_email" in error for error in result.errors)


def test_coverage_gate_warns_but_passes_on_untagged_sensitive_columns():
    cfg = _covered_config()
    result = ValidationResult()
    ddl_columns = [
        "cat.sch.people.email",          # tagged → not listed
        "cat.sch.people.ssn",            # untagged, sensitive-looking
        "cat.sch.people.full_name",      # untagged, sensitive-looking
        "cat.sch.people.date_of_birth",  # untagged, sensitive-looking
        "cat.sch.people.customer_id",    # generic → not listed
    ]
    validate_coverage_gate(cfg, {"mask_email"}, "", result, ddl_columns=ddl_columns)
    assert result.passed  # non-blocking
    assert "fully protected" in result.info[0]
    assert len(result.warnings) == 1
    warning = result.warnings[0]
    assert "fail-open" in warning
    for col in ("people.ssn", "people.full_name", "people.date_of_birth"):
        assert col in warning
    assert "people.email" not in warning
    assert "customer_id" not in warning


def test_coverage_gate_cli_warns_untagged_columns_from_fetched_ddl(tmp_path):
    gen = tmp_path / "generated"
    gen.mkdir()
    (tmp_path / "ddl").mkdir()
    (tmp_path / "ddl" / "_fetched.sql").write_text(
        "CREATE TABLE cat.sch.people (\n  email string,\n  ssn string\n);\n"
    )
    tfvars = gen / "abac.auto.tfvars"
    tfvars.write_text('tag_assignments = []\nfgac_policies = []\n')
    sql = gen / "masking_functions.sql"
    sql.write_text("CREATE FUNCTION cat.sch.mask_email(x STRING) RETURNS STRING RETURN x;\n")
    script = Path(__file__).parents[1] / "validate_abac.py"
    completed = subprocess.run(
        [sys.executable, str(script), "--coverage-gate", str(tfvars), str(sql)],
        text=True, capture_output=True,
    )
    assert "fail-open" in completed.stdout
    assert "cat.sch.people.ssn" in completed.stdout
    assert "COVERAGE GATE —" not in completed.stdout.replace("COVERAGE GATE (non-blocking)", "")


def test_coverage_gate_groups_unmapped_native_classification():
    result = ValidationResult()
    validate_coverage_gate(
        {"tag_assignments": [], "fgac_policies": []},
        set(),
        "# gr.classification_unmapped: cat.sch.people.biometric|class.biometric\n",
        result,
    )
    assert not result.passed
    assert "detected tags with no mapping/rule" in result.errors[0]
    assert "cat.sch.people.biometric" in result.errors[0]


def test_coverage_gate_cli_exits_nonzero_for_classified_unprotected_column(tmp_path):
    tfvars = tmp_path / "abac.auto.tfvars"
    tfvars.write_text('''
groups = []
tag_policies = [{ key = "pii_level", description = "PII", values = ["masked_email"] }]
tag_assignments = [{ entity_type = "columns", entity_name = "cat.sch.people.email", tag_key = "pii_level", tag_value = "masked_email" }]
fgac_policies = []
group_members = {}
genie_space_configs = []
''')
    sql = tmp_path / "masking_functions.sql"
    sql.write_text("CREATE FUNCTION cat.sch.mask_email(x STRING) RETURNS STRING RETURN x;\n")
    script = Path(__file__).parents[1] / "validate_abac.py"
    completed = subprocess.run(
        [sys.executable, str(script), "--coverage-gate", str(tfvars), str(sql)],
        text=True, capture_output=True,
    )
    assert completed.returncode != 0
    assert "classified but unprotected columns" in completed.stdout
    assert "cat.sch.people.email" in completed.stdout


def test_policy_cap_errors_without_dropping(tmp_path, monkeypatch):
    monkeypatch.setattr(generate_abac, "_FGAC_PER_CATALOG_LIMIT", 1)
    tfvars = tmp_path / "abac.auto.tfvars"
    original = '''fgac_policies = [
      { name = "mask_one", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "cat" },
      { name = "mask_two", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "cat" }
    ]
    tag_assignments = []
    '''
    tfvars.write_text(original)
    try:
        generate_abac.autofix_fgac_policy_count(tfvars)
    except ValueError as exc:
        assert "no policies were dropped" in str(exc)
        assert "mask_one" in str(exc) and "mask_two" in str(exc)
    else:
        raise AssertionError("expected hard policy quota error")
    assert tfvars.read_text() == original
