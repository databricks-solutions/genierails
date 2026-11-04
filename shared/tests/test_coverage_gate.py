import json
import subprocess
import sys
from pathlib import Path

import generate_abac
import pytest
from governance_policies import apply_model, build_model
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


def _deterministic_gate(tmp_path, model):
    """The coverage check over the tfvars write_deterministic_model writes for ``model``."""
    import hcl2

    tfvars = tmp_path / "abac.auto.tfvars"
    tfvars.write_text("tag_assignments = []\nfgac_policies = []\n")
    generate_abac.write_deterministic_model(tfvars, None, model)
    functions = {line.split("(", 1)[0].rsplit(" ", 1)[1]
                 for line in model.functions_sql.splitlines() if line.startswith("CREATE")}
    result = ValidationResult()
    validate_coverage_gate(hcl2.loads(tfvars.read_text()), functions, tfvars.read_text(), result)
    return tfvars.read_text(), result


def test_coverage_gate_blocks_a_multi_class_free_text_column_keeping_a_partial(tmp_path):
    import dataclasses

    from governance_policies import build_model

    notes = "cat.sch.notes.free_text"
    settings = {"access_tier_groups": ["raw", "regional", "masked"]}
    model = build_model({notes: ["email_address", "name", "phone_number"]}, {notes: "STRING"}, settings,
                        deployer_principal="deployer-sp")
    text, passed = _deterministic_gate(tmp_path, model)
    assert passed.errors == []
    assert "# gr.classification_mixed_partial: cat.sch.notes.free_text|email_partial,name_partial,phone_partial" in text
    # The same column given phone_partial, as the alphabetical tie-break did.
    leaky = build_model({notes: ["phone_number"]}, {notes: "STRING"}, settings, deployer_principal="deployer-sp")
    _text, failed = _deterministic_gate(tmp_path, dataclasses.replace(leaky, mixed_partial=model.mixed_partial))
    (error,) = failed.errors
    assert "multi-class columns keeping a partial treatment" in error
    assert f"{notes} (classes keep email_partial, name_partial, phone_partial; gr_treatment phone_partial__string__" \
        in error


def test_single_class_free_text_partial_is_advisory_without_changing_exit_status():
    settings = {"access_tier_groups": ["raw", "regional", "masked"]}
    classes = {
        "cat.sch.t.notes": ["phone_number"],
        "cat.sch.t.phone": ["phone_number"],
        "cat.sch.t.remarks": ["email_address", "phone_number"],
    }
    model = build_model(classes, {name: "STRING" for name in classes}, settings)
    cfg = apply_model({}, model) | settings
    result = ValidationResult()
    functions = {line.split("(", 1)[0].rsplit(" ", 1)[1]
                 for line in model.functions_sql.splitlines() if line.startswith("CREATE")}
    validate_coverage_gate(cfg, functions, "", result)
    assert result.passed
    assert len(result.warnings) == 1
    advisory = result.warnings[0]
    assert "COVERAGE ADVISORY (non-blocking)" in advisory
    assert "cat.sch.t.notes" in advisory
    assert "cat.sch.t.phone" not in advisory
    assert "cat.sch.t.remarks" not in advisory


@pytest.mark.parametrize("settings", [
    {"treatment_versions": {"phone_partial": {"partial": "redacted"}}},
    {"column_overrides": {"cat.sch.t.notes": {"partial": "redacted"}}},
    {"column_overrides": {"cat.sch.t.notes": {"treatment": "redact"}}},
])
def test_coverage_advisory_uses_the_effective_partial_tier(settings):
    config = {"access_tier_groups": ["raw", "regional", "masked"]} | settings
    model = build_model({"cat.sch.t.notes": ["phone_number"]},
                        {"cat.sch.t.notes": "STRING"}, config)
    result = ValidationResult()
    functions = {line.split("(", 1)[0].rsplit(" ", 1)[1]
                 for line in model.functions_sql.splitlines() if line.startswith("CREATE")}
    validate_coverage_gate(apply_model({}, model) | config, functions, "", result)
    assert result.passed and not result.warnings


def test_coverage_gate_warns_but_does_not_fail_for_existing_never_raw_raw_column_override():
    config = {
        "access_tier_groups": ["raw", "regional", "masked"],
        "column_overrides": {"cat.sch.t.cvv": {"partial": "raw"}},
    }
    # Existing generated configs can contain this override from before the
    # generator refused it.  The generated never-raw value remains fully
    # redacted; validate/check should warn, not crash or change the exit.
    model = build_model(
        {"cat.sch.t.cvv": ["card_security_code"]},
        {"cat.sch.t.cvv": "STRING"},
        {"access_tier_groups": ["raw", "regional", "masked"]},
    )
    result = ValidationResult()
    functions = {line.split("(", 1)[0].rsplit(" ", 1)[1]
                 for line in model.functions_sql.splitlines() if line.startswith("CREATE")}
    validate_coverage_gate(apply_model({}, model) | config, functions, "", result)
    assert result.passed
    assert result.errors == []
    assert result.warnings == [
        'COVERAGE ADVISORY (non-blocking) — never-raw raw column override ignored:\n'
        '    - column_overrides["cat.sch.t.cvv"].partial = "raw" has no effect: column '
        "cat.sch.t.cvv is never shown raw to anyone and no override exists; remove that "
        "column_overrides entry from the source env.auto.tfvars, run make generate there, "
        "then make promote-to"
    ]


@pytest.mark.parametrize(("settings", "env_extra", "warned"), [
    ({}, "", True),
    (
        {"treatment_versions": {"phone_partial": {"partial": "redacted"}}},
        'treatment_versions = { phone_partial = { partial = "redacted" } }\n', False,
    ),
    (
        {"column_overrides": {"cat.sch.t.details": {"partial": "redacted"}}},
        'column_overrides = { "cat.sch.t.details" = { partial = "redacted" } }\n', False,
    ),
])
def test_coverage_gate_cli_reads_env_mask_settings_and_escaped_comment(
    tmp_path, settings, env_extra, warned,
):
    env = tmp_path / "envs" / "dev"
    generated = env / "generated"
    (env / "ddl").mkdir(parents=True)
    generated.mkdir()
    settings = {"access_tier_groups": ["raw", "regional", "masked"]}
    (env / "env.auto.tfvars").write_text(
        'governance_mode = "deterministic"\n'
        'deployer_principal_id = "d0000000-0000-0000-0000-00000000d001"\n'
        'access_tier_groups = ["raw", "regional", "masked"]\n'
        + env_extra
    )
    (env / "ddl" / "_fetched.sql").write_text(
        "CREATE TABLE cat.sch.t (details STRING COMMENT 'Customer''s free text, e.g. (a > b), etc');\n"
    )
    model = build_model({"cat.sch.t.details": ["phone_number"]},
                        {"cat.sch.t.details": "STRING"},
                        {"access_tier_groups": ["raw", "regional", "masked"]} | settings)
    tfvars = generated / "abac.auto.tfvars"
    tfvars.write_text('''
groups = { raw = { description = "" }, regional = { description = "" }, masked = { description = "" } }
tag_policies = []
tag_assignments = []
fgac_policies = []
group_members = {}
genie_space_configs = []
''')
    sql = generated / "masking_functions.sql"
    generate_abac.write_deterministic_model(tfvars, sql, model)
    completed = subprocess.run(
        [sys.executable, str(Path(__file__).parents[1] / "validate_abac.py"),
         "--coverage-gate", str(tfvars), str(sql), "--summary-label", "coverage-gate"],
        text=True, capture_output=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert ("COVERAGE ADVISORY (non-blocking)" in completed.stdout) is warned
    assert ("cat.sch.t.details" in completed.stdout) is warned


def test_advisory_only_summary_keeps_the_one_line_pass(capsys):
    result = ValidationResult()
    result.ok("coverage complete")
    result.warn("COVERAGE ADVISORY (non-blocking) — review cat.sch.t.details")
    result.print_report("coverage-gate")
    assert capsys.readouterr().out.splitlines() == [
        "coverage-gate: [WARN] COVERAGE ADVISORY (non-blocking) — review cat.sch.t.details",
        "coverage-gate: PASS (1 checks)",
    ]


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


def test_cli_blocks_a_mixed_column_whose_marker_was_deleted(tmp_path):
    """The live refresh's classes reach the real validator through the exposure context."""
    notes = "cat.sch.notes.free_text"
    settings = {"access_tier_groups": ["raw", "regional", "masked"]}
    leaky = build_model({notes: ["phone_number"]}, {notes: "STRING"}, settings, deployer_principal="deployer-sp")
    mixed = build_model({notes: ["email_address", "name", "phone_number"]}, {notes: "STRING"}, settings,
                        deployer_principal="deployer-sp")
    layer = tmp_path / "prod" / "data_access"
    layer.mkdir(parents=True)
    tfvars, sql = layer / "abac.auto.tfvars", layer / "masking_functions.sql"
    context = json.dumps({"column_classes": {notes: ["class.email_address", "class.name", "class.phone_number"]}})
    # Only this check is asserted: the bare config fails the CLI's other checks too.
    for model, blocked in ((leaky, True), (mixed, False)):
        tfvars.write_text("tag_assignments = []\nfgac_policies = []\n")
        sql.write_text("")
        generate_abac.write_deterministic_model(tfvars, sql, model)
        # A hand edit (or an older generate) leaves no marker.
        tfvars.write_text("".join(line for line in tfvars.read_text().splitlines(keepends=True)
                                  if "classification_mixed_partial" not in line))
        result = _cli(tmp_path, tfvars, sql, context)
        assert ("multi-class columns keeping a partial treatment" in result.stdout) is blocked, result.stdout
        assert (f"{notes} (classes keep email_partial, name_partial, phone_partial" in result.stdout) is blocked


def test_docs_name_the_mixed_partial_class_record_compatibility_limit():
    root = Path(__file__).parents[2]
    docs = [
        root / "shared/docs/deterministic-policies.md",
        root / "shared/docs/design/deterministic-governance.md",
        root / "shared/examples/dev_to_prod/REFERENCE.md",
    ]
    for path in docs:
        text = path.read_text()
        assert "Compatibility limit" in text
        assert "generated/.live_refresh.json" in text
        assert "column_classes" in text
        assert "never hand-edit" in text


def test_cli_refuses_malformed_column_classes(tmp_path):
    tfvars, sql = _cli_env(tmp_path, None)
    result = _cli(tmp_path, tfvars, sql, json.dumps({"column_classes": {"cat.sch.t.c": "class.email_address"}}))
    assert result.returncode == 1
    assert "column_classes must map columns to lists of strings" in result.stdout


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
