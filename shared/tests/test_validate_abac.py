"""Unit tests for validate_abac.py validation functions.

Tests exercise the individual validate_* functions with synthetic config dicts
so no file I/O, Databricks, or LLM access is needed.
"""
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from validate_abac import (  # noqa: E402
    ValidationResult,
    validate_groups,
    validate_tag_policies,
    validate_tag_assignments,
    validate_fgac_policies,
    validate_policy_overlaps,
    validate_acl_groups,
    parse_sql_functions,
    parse_sql_function_arg_counts,
    validate_sql_deployment_blocks,
    load_validation_context,
    _condition_matches_tags,
    _infer_column_categories,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _result() -> ValidationResult:
    return ValidationResult()


class _GeneratedClassification:
    def classes_by_column(self, columns=None):
        classes = {"cat.sch.customers.email": ["class.email_address"]}
        return classes if columns is None else {name: classes[name] for name in columns}


def _real_deterministic_output(tmp_path):
    """Produce files in the real env/generated layout used by `make validate-generated`."""
    import hcl2
    from generate_abac import deterministic_model_for, write_deterministic_model

    env_dir = tmp_path / "envs" / "dev"
    generated = env_dir / "generated"
    generated.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text('''
governance_mode = "deterministic"
deployer_principal_id = "d0000000-0000-0000-0000-00000000d001"
access_tier_groups = ["raw", "regional", "masked"]
raw_exempt_principals = ["break-glass"]
''')
    tfvars = generated / "abac.auto.tfvars"
    sql = generated / "masking_functions.sql"
    tfvars.write_text('''
groups = {
  raw = { description = "Raw" }
  regional = { description = "Regional" }
  masked = { description = "Masked" }
}
tag_policies = []
tag_assignments = []
fgac_policies = []
''')
    runtime = hcl2.loads((env_dir / "env.auto.tfvars").read_text())
    model = deterministic_model_for(
        runtime, _GeneratedClassification(),
        "CREATE TABLE cat.sch.customers (email STRING);",
    )
    write_deterministic_model(tfvars, sql, model)
    cfg = hcl2.loads(tfvars.read_text()) | runtime
    return tfvars, sql, cfg


def test_validator_accepts_real_deterministic_generator_output(tmp_path):
    tfvars, sql, _ = _real_deterministic_output(tmp_path)
    run = subprocess.run(
        [sys.executable, str(Path(__file__).parent.parent / "validate_abac.py"), str(tfvars), str(sql)],
        text=True, capture_output=True, check=False,
    )
    assert run.returncode == 0, run.stdout + run.stderr


def test_coverage_runtime_render_uses_env_context_and_still_refuses_real_gap(tmp_path):
    from generate_abac import _replace_bracket_section

    tfvars, sql, _ = _real_deterministic_output(tmp_path)
    env_dir = tfvars.parents[1]
    coverage = env_dir / "generated" / ".runtime" / "data_access.coverage"
    coverage.mkdir(parents=True)
    coverage_tfvars = coverage / tfvars.name
    coverage_sql = coverage / sql.name
    shutil.copyfile(tfvars, coverage_tfvars)
    shutil.copyfile(sql, coverage_sql)
    command = [sys.executable, str(Path(__file__).parent.parent / "validate_abac.py"),
               "--coverage-gate", str(coverage_tfvars), str(coverage_sql)]

    valid = subprocess.run(command, text=True, capture_output=True, check=False)

    assert valid.returncode == 0, valid.stdout + valid.stderr
    assert "not defined in groups" not in valid.stdout
    assert "outside the registry" not in valid.stdout
    assert "matches multiple column-mask policies" not in valid.stdout

    coverage_tfvars.write_text(_replace_bracket_section(
        coverage_tfvars.read_text(), "fgac_policies", []))
    gap = subprocess.run(command, text=True, capture_output=True, check=False)

    assert gap.returncode == 1
    assert "no covering column-mask policy" in gap.stdout


def test_generated_governance_mode_cannot_override_environment(tmp_path):
    tfvars, sql, _ = _real_deterministic_output(tmp_path)
    env_file = tfvars.parent.parent / "env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace('"deterministic"', '"legacy"'))
    tfvars.write_text('governance_mode = "deterministic"\n' + tfvars.read_text())

    run = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).parent.parent / "validate_abac.py"),
            str(tfvars),
            str(sql),
        ],
        text=True, capture_output=True, check=False,
    )

    assert run.returncode == 1
    assert (
        "generated governance_mode 'deterministic' conflicts with env.auto.tfvars "
        "governance_mode 'legacy'; env.auto.tfvars is authoritative"
    ) in run.stdout


def test_generated_matching_governance_mode_is_accepted(tmp_path):
    tfvars, sql, _ = _real_deterministic_output(tmp_path)
    tfvars.write_text('governance_mode = "deterministic"\n' + tfvars.read_text())

    run = subprocess.run(
        [sys.executable, str(Path(__file__).parent.parent / "validate_abac.py"), str(tfvars), str(sql)],
        text=True, capture_output=True, check=False,
    )

    assert run.returncode == 0, run.stdout + run.stderr


def test_unparseable_env_tfvars_is_validation_error(tmp_path):
    tfvars, sql, _ = _real_deterministic_output(tmp_path)
    env_file = tfvars.parent.parent / "env.auto.tfvars"
    env_file.write_text("governance_mode = [\n")
    result = _result()

    load_validation_context({}, result, tfvars)

    assert any(f"Could not parse {env_file}" in error for error in result.errors)


def test_deterministic_validator_still_refuses_overlapping_double_mask(tmp_path):
    _, _, cfg = _real_deterministic_output(tmp_path)
    for policy in cfg["fgac_policies"]:
        policy["to_principals"] = ["account users"]
        policy["except_principals"] = []
    result = _result()
    validate_policy_overlaps(cfg, result)
    assert any("MULTIPLE_MASKS" in error for error in result.errors)


@pytest.mark.parametrize("counterexample", [
    "full_without_exceptions",
    "overlapping_named_groups",
    "dropped_partial_tier_exception",
    "swapped_functions",
    "undeclared_nested_group",
    "declared_nested_group",
    "account_users_title_case",
    "account_users_upper_case",
    "exempt_principal_in_partial_tier",
])
def test_deterministic_overlap_proof_refuses_reviewer_counterexamples(tmp_path, counterexample):
    _, _, cfg = _real_deterministic_output(tmp_path)
    partial = next(p for p in cfg["fgac_policies"] if p["function_name"].endswith("_partial"))
    full = next(p for p in cfg["fgac_policies"] if p["function_name"].endswith("_full"))
    if counterexample == "full_without_exceptions":
        partial["except_principals"] = []
        full["except_principals"] = []
    elif counterexample == "overlapping_named_groups":
        full["to_principals"] = ["masked"]
        full["except_principals"] = ["raw", "break-glass", "d0000000-0000-0000-0000-00000000d001"]
    elif counterexample == "dropped_partial_tier_exception":
        full["except_principals"].remove("regional")
    elif counterexample == "swapped_functions":
        partial["function_name"], full["function_name"] = full["function_name"], partial["function_name"]
    elif counterexample in {"undeclared_nested_group", "declared_nested_group"}:
        cfg["groups"]["outer"] = {"description": "Outer"}
        partial["to_principals"] = ["outer"]
        if counterexample == "declared_nested_group":
            cfg["group_members"] = {"outer": ["regional"]}
    elif counterexample in {"account_users_title_case", "account_users_upper_case"}:
        principal = "Account Users" if counterexample.endswith("title_case") else "ACCOUNT USERS"
        partial["to_principals"] = [principal]
        full["except_principals"].append(principal)
    elif counterexample == "exempt_principal_in_partial_tier":
        cfg["raw_exempt_principals"].append("svc-etl")
        cfg["groups"]["svc-etl"] = {"description": "Service"}
        partial["to_principals"] = ["svc-etl"]
        full["except_principals"] = [p for p in full["except_principals"] if p != "svc-etl"]
    result = _result()
    validate_policy_overlaps(cfg, result)
    assert any("MULTIPLE_MASKS" in error for error in result.errors)


def test_deterministic_validator_still_refuses_unknown_treatment(tmp_path):
    _, _, cfg = _real_deterministic_output(tmp_path)
    cfg["tag_policies"][0]["values"] = ["unknown_treatment"]
    result = _result()
    validate_tag_policies(cfg, result)
    assert any("not in the canonical registry" in error for error in result.errors)


def test_deterministic_validator_refuses_well_formed_unknown_treatment(tmp_path):
    _, _, cfg = _real_deterministic_output(tmp_path)
    cfg["tag_policies"][0]["values"] = ["bogus__string__deadbeef"]
    result = _result()
    validate_tag_policies(cfg, result)
    assert any("not in the canonical registry" in error for error in result.errors)


def test_deterministic_validator_still_refuses_undefined_except_group(tmp_path):
    _, _, cfg = _real_deterministic_output(tmp_path)
    cfg["fgac_policies"][0]["except_principals"].append("undefined-group")
    result = _result()
    tag_map = validate_tag_policies(cfg, result)
    validate_fgac_policies(cfg, set(cfg["groups"]), tag_map, None, result)
    assert any("except_principals group 'undefined-group' not defined" in error for error in result.errors)


@pytest.mark.parametrize(("field", "principal"), [
    ("to_principals", "unknown"),
    ("except_principals", "Unknown"),
])
def test_validator_refuses_config_principal_named_unknown(tmp_path, field, principal):
    _, _, cfg = _real_deterministic_output(tmp_path)
    cfg["groups"][principal] = {"description": "Reserved"}
    cfg["fgac_policies"][0][field] = [principal]
    result = _result()
    tag_map = validate_tag_policies(cfg, result)
    group_names = validate_groups(cfg, result)
    validate_fgac_policies(cfg, group_names, tag_map, None, result)
    assert any("reserved" in error and "unknown" in error for error in result.errors)


@pytest.mark.parametrize("key", ["access_tier_groups", "raw_exempt_principals"])
def test_validator_refuses_env_principal_named_unknown(tmp_path, key):
    tfvars, sql, _ = _real_deterministic_output(tmp_path)
    env_file = tfvars.parent.parent / "env.auto.tfvars"
    lines = env_file.read_text().splitlines()
    env_file.write_text("\n".join(
        f'{key} = ["unknown"]' if line.startswith(f"{key} = ") else line
        for line in lines
    ) + "\n")

    run = subprocess.run(
        [sys.executable, str(Path(__file__).parent.parent / "validate_abac.py"), str(tfvars), str(sql)],
        text=True, capture_output=True, check=False,
    )

    assert run.returncode == 1
    assert f"{key}: 'unknown' is reserved" in run.stdout


def test_never_raw_deterministic_policy_refuses_deployer_and_raw_exempt_exceptions():
    from governance_policies import build_model

    settings = {
        "governance_mode": "deterministic",
        "access_tier_groups": ["raw", "regional", "masked"],
        "raw_exempt_principals": ["break-glass"],
        "deployer_principal_id": "d0000000-0000-0000-0000-00000000d001",
    }
    model = build_model(
        {"cat.sch.payments.cvv": ["card_security_code"]},
        {"cat.sch.payments.cvv": "STRING"}, settings,
        deployer_principal="d0000000-0000-0000-0000-00000000d001",
    )
    policy = model.fgac_policies[0]
    policy["except_principals"] = ["d0000000-0000-0000-0000-00000000d001", "break-glass"]
    cfg = settings | {
        "groups": {name: {} for name in ("raw", "regional", "masked")},
        "tag_policies": [{"key": "gr_treatment", "values": model.tag_values}],
        "tag_assignments": model.tag_assignments,
        "fgac_policies": model.fgac_policies,
    }
    result = _result()
    tag_map = validate_tag_policies(cfg, result)
    validate_fgac_policies(cfg, set(cfg["groups"]), tag_map, None, result)
    assert any("never-raw deterministic treatments" in error for error in result.errors)


def test_validation_refuses_ambiguous_fallback_duplicate_functions(tmp_path):
    sql = tmp_path / "masking_functions.sql"
    sql.write_text(
        "SET timezone = 'UTC'; CREATE FUNCTION Mask() RETURNS INT RETURN 1; "
        "CREATE OR REPLACE FUNCTION mask() RETURNS INT RETURN 2;"
    )
    result = _result()
    validate_sql_deployment_blocks(sql, result)
    assert result.errors == [
        "Ambiguous masking SQL: function 'mask' has a definition that deployment would not execute; "
        "put each definition on its own semicolon-terminated line or remove the duplicate"
    ]


def test_clean_labelled_report_is_one_line_unless_verbose(capsys):
    result = _result()
    result.ok("first")
    result.ok("second")
    result.print_report("validate-generated")
    assert capsys.readouterr().out == "validate-generated: PASS (2 checks)\n"

    result.print_report("validate-generated", verbose=True)
    assert "ABAC Configuration Validation Report" in capsys.readouterr().out


@pytest.mark.parametrize("kind", ["warning", "error"])
def test_labelled_report_keeps_full_non_clean_report(kind, capsys):
    result = _result()
    result.ok("good")
    getattr(result, "warn" if kind == "warning" else "error")(kind)
    result.print_report("validate-generated")
    out = capsys.readouterr().out
    assert "ABAC Configuration Validation Report" in out
    assert f"[{ 'WARN' if kind == 'warning' else 'FAIL' }] {kind}" in out


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


def test_financial_compensation_columns_are_amount_category():
    for column in ("annual_salary", "hourly_wage", "total_compensation"):
        assert _infer_column_categories(f"catalog.hr.employees.{column}") == {"amount"}


def test_legacy_name_function_category_check_is_explicitly_skipped_only_in_deterministic_mode():
    policy = {
        "name": "phone", "policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "cat",
        "to_principals": ["analysts"], "except_principals": [],
        "match_condition": "hasTagValue('pii', 'phone')", "match_alias": "masked_col",
        "function_name": "mask_phone", "function_catalog": "cat", "function_schema": "sch",
    }
    assignment = {"entity_type": "columns", "entity_name": "cat.sch.t.free_text",
                  "tag_key": "pii", "tag_value": "phone"}
    base = {"fgac_policies": [policy], "tag_assignments": [assignment]}
    legacy = _result()
    validate_fgac_policies(base, {"analysts"}, {"pii": {"phone"}}, {"mask_phone"}, legacy)
    assert any("expected only" in error for error in legacy.errors)
    deterministic = _result()
    validate_fgac_policies(base | {"governance_mode": "deterministic"}, {"analysts"},
                           {"pii": {"phone"}}, {"mask_phone"}, deterministic)
    assert not any("expected only" in error for error in deterministic.errors)


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

    def test_empty_groups_passes_when_only_builtin_principal_is_referenced(self):
        r = _result()
        names = validate_groups({
            "groups": {},
            "fgac_policies": [{"to_principals": ["account users"]}],
        }, r)
        assert r.passed
        assert names == set()

    def test_builtin_principal_match_is_case_insensitive(self):
        r = _result()
        validate_groups({
            "groups": {},
            "fgac_policies": [{"to_principals": ["Account Users"]}],
        }, r)
        assert r.passed

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

    def test_non_canonical_registry_key_fails(self):
        r = _result()
        validate_tag_policies(
            {"tag_policies": [{"key": "aml_scope_deadbe", "values": ["aml_restricted"]}]},
            r,
        )
        assert not r.passed
        assert any("non-canonical" in e for e in r.errors)

    def test_unknown_registry_value_fails(self):
        r = _result()
        validate_tag_policies(
            {"tag_policies": [{"key": "pci_level", "values": ["masked_pan"]}]},
            r,
        )
        assert not r.passed
        assert any("canonical registry" in e for e in r.errors)


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

    def test_non_canonical_registry_value_fails(self):
        cfg = {
            "tag_assignments": [
                {
                    "entity_type": "tables",
                    "entity_name": "cat.schema.tbl",
                    "tag_key": "pci_level",
                    "tag_value": "restricted_card",
                }
            ]
        }
        r = _result()
        validate_tag_assignments(
            cfg,
            {"pci_level": {"public", "masked_card_last4", "redacted_card_full"}},
            r,
        )
        assert not r.passed
        assert any("non-canonical" in e for e in r.errors)

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


# ===========================================================================
#  validate_policy_overlaps
# ===========================================================================

class TestValidatePolicyOverlaps:

    @staticmethod
    def _policy(name: str, policy_type: str, condition: str) -> dict:
        policy = {"name": name, "policy_type": policy_type, "catalog": "main"}
        condition_key = (
            "match_condition"
            if policy_type == "POLICY_TYPE_COLUMN_MASK"
            else "when_condition"
        )
        policy[condition_key] = condition
        return policy

    def test_clean_config_passes(self):
        cfg = {
            "tag_assignments": [
                {
                    "entity_type": "columns",
                    "entity_name": "main.hr.employees.ssn",
                    "tag_key": "pii_level",
                    "tag_value": "Full_PII",
                }
            ],
            "fgac_policies": [
                self._policy(
                    "mask_full_pii",
                    "POLICY_TYPE_COLUMN_MASK",
                    "hasTagValue('pii_level', 'Full_PII')",
                ),
                self._policy(
                    "mask_limited_pii",
                    "POLICY_TYPE_COLUMN_MASK",
                    "hasTagValue('pii_level', 'Limited_PII')",
                ),
            ],
        }
        r = _result()
        validate_policy_overlaps(cfg, r)
        assert r.passed

    def test_two_masks_on_one_column_fails_with_actionable_message(self):
        cfg = {
            "tag_assignments": [
                {
                    "entity_type": "columns",
                    "entity_name": "main.hr.employees.ssn",
                    "tag_key": "pii_level",
                    "tag_value": "Full_PII",
                }
            ],
            "fgac_policies": [
                self._policy(
                    "mask_all_pii",
                    "POLICY_TYPE_COLUMN_MASK",
                    "hasTag('pii_level')",
                ),
                self._policy(
                    "mask_full_pii",
                    "POLICY_TYPE_COLUMN_MASK",
                    "hasTagValue('pii_level', 'Full_PII')",
                ),
            ],
        }
        r = _result()
        validate_policy_overlaps(cfg, r)
        assert not r.passed
        message = " ".join(r.errors)
        assert "main.hr.employees.ssn" in message
        assert "mask_all_pii" in message
        assert "mask_full_pii" in message
        assert "only one mask" in message
        assert "MULTIPLE_MASKS" in message

    def test_masks_overlap_across_spellings_of_one_column(self):
        # Unity Catalog identifiers are case-insensitive: these are one column,
        # and one policy's catalog is spelled differently. The message keeps
        # the configured spelling.
        assignment = {"entity_type": "columns", "tag_key": "pii_level", "tag_value": "Full_PII"}
        cfg = {
            "tag_assignments": [
                {**assignment, "entity_name": "main.HR.Employees.ssn"},
                {**assignment, "entity_name": "main.hr.employees.SSN", "tag_key": "region", "tag_value": "us"},
            ],
            "fgac_policies": [
                {**self._policy("mask_full_pii", "POLICY_TYPE_COLUMN_MASK",
                                "hasTagValue('pii_level', 'Full_PII')"), "catalog": "MAIN"},
                self._policy("mask_us", "POLICY_TYPE_COLUMN_MASK", "hasTagValue('region', 'us')"),
            ],
        }
        r = _result()
        validate_policy_overlaps(cfg, r)
        assert not r.passed
        message = " ".join(r.errors)
        assert "main.HR.Employees.ssn" in message and "mask_full_pii" in message and "mask_us" in message

    def test_two_row_filters_on_one_table_fail(self):
        cfg = {
            "tag_assignments": [
                {
                    "entity_type": "tables",
                    "entity_name": "main.sales.orders",
                    "tag_key": "region_scope",
                    "tag_value": "global",
                }
            ],
            "fgac_policies": [
                self._policy(
                    "filter_tagged_tables",
                    "POLICY_TYPE_ROW_FILTER",
                    "hasTag('region_scope')",
                ),
                self._policy(
                    "filter_global_tables",
                    "POLICY_TYPE_ROW_FILTER",
                    "hasTagValue('region_scope', 'global')",
                ),
            ],
        }
        r = _result()
        validate_policy_overlaps(cfg, r)
        assert not r.passed
        message = " ".join(r.errors)
        assert "main.sales.orders" in message
        assert "filter_tagged_tables" in message
        assert "filter_global_tables" in message
        assert "only one row filter" in message

    def test_column_aware_row_filter_accepts_one_argument(self, tmp_path):
        from validate_abac import validate_fgac_policies

        cfg = {
            "groups": {"analysts": {}},
            "tag_policies": [{"key": "region_scope", "values": ["region_code"]}],
            "fgac_policies": [{
                "name": "filter_region",
                "policy_type": "POLICY_TYPE_ROW_FILTER",
                "catalog": "main",
                "to_principals": ["analysts"],
                "match_condition": "hasTagValue('region_scope', 'region_code')",
                "match_alias": "region_code",
                "function_name": "filter_allowed_region",
                "function_catalog": "main",
                "function_schema": "security",
            }],
        }
        r = _result()
        validate_fgac_policies(
            cfg,
            {"analysts"},
            {"region_scope": {"region_code"}},
            {"filter_allowed_region"},
            r,
            {"filter_allowed_region": 1},
        )
        assert not any("binds" in warning for warning in r.warnings)


# ===========================================================================
#  validate_acl_groups
# ===========================================================================

class TestValidateAclGroups:

    def _groups(self) -> set:
        return {"Analyst", "Manager", "Clinical_Staff"}

    def test_valid_acl_groups_pass(self):
        cfg = {
            "genie_space_configs": {
                "Finance Analytics": {
                    "acl_groups": ["Analyst", "Manager"],
                }
            }
        }
        r = _result()
        validate_acl_groups(cfg, self._groups(), r)
        assert r.passed

    def test_undefined_group_in_acl_fails(self):
        cfg = {
            "genie_space_configs": {
                "Finance Analytics": {
                    "acl_groups": ["Analyst", "Ghost_Group"],
                }
            }
        }
        r = _result()
        validate_acl_groups(cfg, self._groups(), r)
        assert not r.passed
        assert any("Ghost_Group" in e for e in r.errors)

    def test_empty_acl_groups_no_error(self):
        cfg = {
            "genie_space_configs": {
                "Finance Analytics": {
                    "acl_groups": [],
                }
            }
        }
        r = _result()
        validate_acl_groups(cfg, self._groups(), r)
        assert r.passed

    def test_missing_acl_groups_no_error(self):
        cfg = {
            "genie_space_configs": {
                "Finance Analytics": {
                    "title": "Finance",
                }
            }
        }
        r = _result()
        validate_acl_groups(cfg, self._groups(), r)
        assert r.passed

    def test_no_genie_space_configs_no_error(self):
        cfg = {}
        r = _result()
        validate_acl_groups(cfg, self._groups(), r)
        assert r.passed

    def test_account_users_builtin_passes(self):
        cfg = {
            "genie_space_configs": {
                "Finance Analytics": {
                    "acl_groups": ["Analyst", "account users"],
                }
            }
        }
        r = _result()
        validate_acl_groups(cfg, self._groups(), r)
        assert r.passed

    def test_multiple_spaces_validated_independently(self):
        cfg = {
            "genie_space_configs": {
                "Finance Analytics": {
                    "acl_groups": ["Analyst"],
                },
                "Clinical Analytics": {
                    "acl_groups": ["Clinical_Staff", "Bad_Group"],
                },
            }
        }
        r = _result()
        validate_acl_groups(cfg, self._groups(), r)
        assert not r.passed
        assert any("Bad_Group" in e for e in r.errors)
        # Finance should pass, only Clinical should fail
        assert not any("Analyst" in e for e in r.errors)


# ---------------------------------------------------------------------------
# The shared account tag policy holds deterministic values (live AWS Phase A, N3)
# ---------------------------------------------------------------------------

DETERMINISTIC_VALUE = "card_last4__string__951e00ac"
SHARED_POLICY = [{"key": "gr_treatment", "values": ["account_last4", DETERMINISTIC_VALUE]}]


def _envs_with_account_policy(tmp_path, values):
    envs = tmp_path / "envs"
    (envs / "account").mkdir(parents=True)
    (envs / "dev").mkdir()
    rendered = ", ".join(f'"{v}"' for v in values)
    (envs / "account" / "abac.auto.tfvars").write_text(
        f'tag_policies = [{{ key = "gr_treatment", description = "t", values = [{rendered}] }}]\n')
    (envs / "dev" / "env.auto.tfvars").write_text("")  # legacy
    (envs / "dev" / "abac.auto.tfvars").write_text("groups = {}\n")
    return envs


def test_account_layer_accepts_deterministic_values_next_to_legacy_ones(tmp_path):
    envs = _envs_with_account_policy(tmp_path, ["account_last4", DETERMINISTIC_VALUE])
    result = _result()

    cfg = load_validation_context({"tag_policies": SHARED_POLICY}, result, envs / "account" / "abac.auto.tfvars")
    validate_tag_policies(cfg, result)

    assert result.errors == []


def test_a_legacy_env_accepts_the_shared_account_policy_it_loads(tmp_path):
    envs = _envs_with_account_policy(tmp_path, ["account_last4", DETERMINISTIC_VALUE])
    result = _result()

    cfg = load_validation_context({"groups": {}}, result, envs / "dev" / "abac.auto.tfvars")
    validate_tag_policies(cfg, result)

    assert cfg["tag_policies"][0]["values"] == ["account_last4", DETERMINISTIC_VALUE]
    assert result.errors == []


@pytest.mark.parametrize("value", ["not_a_treatment", "not_a_treatment__string__951e00ac"])
def test_account_layer_still_refuses_unknown_values(tmp_path, value):
    envs = _envs_with_account_policy(tmp_path, [value])
    result = _result()

    cfg = load_validation_context({"tag_policies": [{"key": "gr_treatment", "values": [value]}]},
                                  result, envs / "account" / "abac.auto.tfvars")
    validate_tag_policies(cfg, result)

    assert any(f"value '{value}' is not in the canonical registry" in e for e in result.errors)


def test_a_legacy_env_still_refuses_deterministic_values_in_its_own_config(tmp_path):
    envs = _envs_with_account_policy(tmp_path, ["account_last4"])
    own = {"tag_policies": SHARED_POLICY, "tag_assignments": [
        {"entity_type": "columns", "entity_name": "c.s.t.card", "tag_key": "gr_treatment",
         "tag_value": DETERMINISTIC_VALUE}]}
    result = _result()

    cfg = load_validation_context(own, result, envs / "dev" / "generated" / "abac.auto.tfvars")
    tag_map = validate_tag_policies(cfg, result)
    validate_tag_assignments(cfg, tag_map, result)

    assert any(f"value '{DETERMINISTIC_VALUE}' is not in the canonical registry" in e for e in result.errors)
    assert any(f"tag_value '{DETERMINISTIC_VALUE}' is not in the canonical registry" in e for e in result.errors)
