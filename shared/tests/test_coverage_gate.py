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


def test_coverage_gate_rejects_column_mask_policy_with_zero_protected_columns():
    cfg = {
        "tag_assignments": [],
        "fgac_policies": [{
            "name": "orphaned_email_mask",
            "policy_type": "POLICY_TYPE_COLUMN_MASK",
            "catalog": "cat",
            "match_condition": "hasTagValue('gr_treatment', 'email_partial')",
            "function_name": "mask_email",
        }],
    }
    result = ValidationResult()
    validate_coverage_gate(cfg, {"mask_email"}, "", result)
    assert not result.passed
    assert any("vacuous" in error for error in result.errors)


def test_coverage_gate_passes_row_filter_only_config():
    cfg = {
        "tag_assignments": [{
            "entity_type": "tables",
            "entity_name": "cat.sch.regional_orders",
            "tag_key": "row_scope",
            "tag_value": "regional",
        }],
        "fgac_policies": [{
            "name": "regional_orders_filter",
            "policy_type": "POLICY_TYPE_ROW_FILTER",
            "catalog": "cat",
            "match_condition": "hasTagValue('row_scope', 'regional')",
            "function_name": "filter_region",
        }],
    }
    result = ValidationResult()
    validate_coverage_gate(cfg, {"filter_region"}, "", result)
    assert result.passed, result.errors
    assert any(
        "0 classified/treatment column(s) fully protected" in info
        for info in result.info
    )


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
    assert "COVERAGE CHECK —" not in completed.stdout.replace("COVERAGE CHECK (non-blocking)", "")


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


# ── first-exposure check ──────────────────────────────────────────────────────

_PEOPLE_DDL = [
    "cat.sch.people.email",        # tagged by _covered_config
    "cat.sch.people.ssn",          # untagged, sensitive-looking
    "cat.sch.people.amount",       # untagged, monetary: never blocks
    "cat.sch.people.customer_id",  # generic
]


def _gate(ddl_columns, first=(), acknowledged=(), cfg=None):
    result = ValidationResult()
    validate_coverage_gate(
        cfg or _covered_config(), {"mask_email"}, "", result,
        ddl_columns=ddl_columns,
        exposure={
            "first_exposure_tables": list(first),
            "acknowledged_columns": list(acknowledged),
            "acknowledge_file": "envs/prod/env.auto.tfvars",
        },
    )
    return result


def test_first_exposure_blocks_untagged_sensitive_column_and_says_how_to_fix():
    result = _gate(_PEOPLE_DDL, first=["cat.sch.people"])
    assert not result.passed
    [error] = [e for e in result.errors if "first exposure blocked" in e]
    assert "cat.sch.people.ssn (looks like: ssn)" in error
    assert "people.amount" not in error and "people.email" not in error
    # The message names every way out and the exact acknowledge syntax.
    assert "Wait for the UC Data Classification scan" in error
    assert "Tag the columns in Unity Catalog" in error
    assert 'envs/prod/env.auto.tfvars:\n         coverage_acknowledged_columns = ["cat.sch.people.ssn"]' in error
    assert "re-run the same make command" in error
    # Monetary amounts stay a warning even on first exposure.
    assert any("cat.sch.people.amount" in w for w in result.warnings)


def test_first_exposure_passes_once_the_column_is_tagged():
    cfg = _covered_config()
    cfg["tag_assignments"].append({
        "entity_type": "columns", "entity_name": "cat.sch.people.ssn",
        "tag_key": "gr_treatment", "tag_value": "mask_email",
    })
    # Give the new treatment a covering policy so only the first-exposure
    # check is under test.
    result = _gate(_PEOPLE_DDL, first=["cat.sch.people"], cfg=cfg)
    assert not any("first exposure" in e for e in result.errors)
    assert not any("people.ssn" in w for w in result.warnings)


def test_first_exposure_passes_once_the_column_is_acknowledged():
    result = _gate(_PEOPLE_DDL, first=["cat.sch.people"], acknowledged=["CAT.sch.people.SSN"])
    assert result.passed, result.errors
    assert any("1 acknowledged column(s)" in line and "cat.sch.people.ssn" in line for line in result.info)
    assert not any("people.ssn" in w for w in result.warnings)


def test_already_granted_table_keeps_todays_warning():
    result = _gate(_PEOPLE_DDL, first=[])
    assert result.passed
    [warning] = result.warnings
    assert "COVERAGE CHECK (non-blocking)" in warning
    assert "cat.sch.people.ssn" in warning


def test_only_the_new_table_blocks():
    ddl = _PEOPLE_DDL + ["cat.sch.orders.card_number", "cat.sch.orders.order_id"]
    result = _gate(ddl, first=["cat.sch.orders"])
    [error] = [e for e in result.errors if "first exposure blocked" in e]
    assert "cat.sch.orders.card_number" in error
    assert "people.ssn" not in error
    assert any("cat.sch.people.ssn" in w for w in result.warnings)


def test_missing_ddl_fails_closed_for_first_exposure():
    result = _gate(None, first=["cat.sch.people"])
    assert not result.passed
    assert any("no fetched DDL" in e and "cat.sch.people" in e for e in result.errors)


def test_table_absent_from_ddl_fails_closed_for_first_exposure():
    result = _gate(_PEOPLE_DDL, first=["cat.sch.people", "cat.sch.unfetched"])
    [error] = [e for e in result.errors if "no fetched DDL" in e]
    assert "cat.sch.unfetched" in error
    assert "cat.sch.people\n" not in error + "\n"


def test_missing_ddl_without_first_exposure_keeps_todays_behaviour():
    assert _gate(None, first=[]).passed


def _cli_env(tmp_path, ddl):
    layer = tmp_path / "prod" / "data_access"
    layer.mkdir(parents=True)
    if ddl is not None:
        (tmp_path / "prod" / "ddl").mkdir()
        (tmp_path / "prod" / "ddl" / "_fetched.sql").write_text(ddl)
    tfvars = layer / "abac.auto.tfvars"
    tfvars.write_text('tag_assignments = []\nfgac_policies = []\n')
    sql = layer / "masking_functions.sql"
    sql.write_text("CREATE FUNCTION cat.sch.mask_email(x STRING) RETURNS STRING RETURN x;\n")
    return tfvars, sql


def _cli(tmp_path, tfvars, sql, context):
    path = tmp_path / "exposure.json"
    path.write_text(context)
    script = Path(__file__).parents[1] / "validate_abac.py"
    return subprocess.run(
        [sys.executable, str(script), "--coverage-gate", str(tfvars), str(sql),
         "--ddl", str(tfvars.parents[1] / "ddl" / "_fetched.sql"), "--exposure-context", str(path)],
        text=True, capture_output=True,
    )


def test_cli_blocks_first_exposure_on_the_split_data_access_config(tmp_path):
    tfvars, sql = _cli_env(tmp_path, "CREATE TABLE cat.sch.people (\n  email string,\n  ssn string\n);\n")
    completed = _cli(tmp_path, tfvars, sql, '{"first_exposure_tables": ["cat.sch.people"]}')
    assert completed.returncode == 1
    assert "first exposure blocked" in completed.stdout
    assert "cat.sch.people.email" in completed.stdout and "cat.sch.people.ssn" in completed.stdout
    acknowledged = _cli(tmp_path, tfvars, sql, (
        '{"first_exposure_tables": ["cat.sch.people"],'
        ' "acknowledged_columns": ["cat.sch.people.email", "cat.sch.people.ssn"]}'
    ))
    # The bare fixture fails unrelated checks (no groups); only the
    # first-exposure block must be gone.
    assert "first exposure blocked" not in acknowledged.stdout
    assert "2 acknowledged column(s)" in acknowledged.stdout


def test_cli_fails_closed_without_ddl_or_with_a_bad_context(tmp_path):
    tfvars, sql = _cli_env(tmp_path, None)
    completed = _cli(tmp_path, tfvars, sql, '{"first_exposure_tables": ["cat.sch.people"]}')
    assert completed.returncode == 1
    assert "no fetched DDL" in completed.stdout
    malformed = _cli(tmp_path, tfvars, sql, '{"first_exposure_tables": "cat.sch.people"}')
    assert malformed.returncode == 1
    assert "unreadable exposure context" in malformed.stdout
