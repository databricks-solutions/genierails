"""Unit tests for verify_effective_access.py.

These exercise the *pure* comparison and spec-derivation logic with mocked
query results — no Databricks connection, no warehouse, no service principals.
The live workspace path (EffectiveAccessVerifier / verify_effective_access_live)
is guarded behind GENIERAILS_LIVE_VERIFY. Network-free tests below assert the
guard and exercise provisioning behavior with mocked SDK clients.

Guiding principle under test: a verification check only PASSES when it
conclusively proves the policy took effect. Anything it could not verify
(missing data, failed query, no overlap, all-null values, empty spec) is
NON-PASSING (FAIL or INCONCLUSIVE) and blocks the gate.
"""
import sys
from dataclasses import replace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from verify_effective_access import (  # noqa: E402
    PASS,
    FAIL,
    INCONCLUSIVE,
    DEFAULT_ADMIN_TIER,
    OUT_OF_TIER_PRINCIPAL,
    ColumnMaskCheck,
    RowFilterCheck,
    VerificationSpec,
    CheckResult,
    EffectiveAccessReport,
    EffectiveAccessVerifier,
    KeyPick,
    TestPrincipal as VerificationPrincipal,
    parse_tag_conditions,
    resolve_columns_for_condition,
    derive_spec_from_config,
    evaluate_column_mask_check,
    evaluate_tiered_column_mask_check,
    most_privileged_tier,
    evaluate_row_filter_check,
    evaluate_effective_access,
    load_spec_from_file,
    load_spec_from_tfvars,
    main,
    verify_effective_access_live,
    write_result_file,
)


def _tiered_check():
    return ColumnMaskCheck(
        table="cat.sch.people", column="email", key_column="id",
        masked_principals=(), unmasked_principals=(),
        expected_tiers=(("raw_group", "raw"), ("analyst", "partial"), ("viewer", "full")),
        partial_function="cat.gov.email_partial", full_function="cat.gov.email_full")


def test_tiered_exact_output_rejects_partial_and_full_swapped():
    check = _tiered_check()
    expected = {"raw": [(1, "alice@example.com")], "partial": [(1, "a***@example.com")], "full": [(1, "[redacted]")]}
    actual = {"raw_group": expected["raw"], "analyst": expected["full"], "viewer": expected["partial"]}
    result = evaluate_tiered_column_mask_check(check, actual, expected)
    assert result.status == FAIL
    assert result.evidence["mismatches_by_principal"] == {"analyst": 1, "viewer": 1}


def test_tiered_indistinguishable_sample_is_inconclusive():
    check = _tiered_check()
    same = [(1, None), (2, "")]
    actual = {principal: same for principal, _ in check.expected_tiers}
    result = evaluate_tiered_column_mask_check(check, actual, {tier: same for tier in ("raw", "partial", "full")})
    assert result.status == INCONCLUSIVE
    assert "cannot be distinguished" in result.detail


def test_tiered_identical_partial_and_full_outputs_can_pass():
    check = replace(_tiered_check(), full_function=_tiered_check().partial_function)
    raw = [(key, f"value-{key}") for key in range(12)]
    redacted = [(key, "[REDACTED]") for key in range(12)]
    expected = {"raw": raw, "partial": redacted, "full": redacted}
    actual = {principal: expected[tier] for principal, tier in check.expected_tiers}
    result = evaluate_tiered_column_mask_check(check, actual, expected)
    assert result.status == PASS
    assert result.evidence["per_principal_compared"]["analyst"] == 12
    assert result.evidence["per_principal_compared"]["viewer"] == 12


def test_tiered_different_functions_with_coinciding_sample_are_inconclusive():
    check = _tiered_check()
    raw = [(key, f"value-{key}") for key in range(12)]
    same = [(key, "[REDACTED]") for key in range(12)]
    actual = {"raw_group": raw, "analyst": same, "viewer": same}
    result = evaluate_tiered_column_mask_check(
        check, actual, {"raw": raw, "partial": same, "full": same})
    assert result.status == INCONCLUSIVE
    assert "different expected functions" in result.detail


@pytest.mark.parametrize("duplicate_in", ["viewer", "expected-full"])
def test_tiered_duplicate_keys_are_inconclusive_before_rows_are_collapsed(duplicate_in):
    check = replace(_tiered_check(), full_function=_tiered_check().partial_function)
    raw = [(key, f"value-{key}") for key in range(12)]
    masked = [(key, "[R]") for key in range(12)]
    expected = {"raw": raw, "partial": list(masked), "full": list(masked)}
    actual = {"raw_group": raw, "analyst": masked, "viewer": list(masked)}
    if duplicate_in == "viewer":
        actual["viewer"] = masked[:3] + [(3, raw[3][1]), (3, "[R]")] + masked[4:]
    else:
        expected["full"] = masked + [(3, "[R]")]
    result = evaluate_tiered_column_mask_check(check, actual, expected)
    assert result.status == INCONCLUSIVE
    assert "not unique" in result.detail


def test_dual_tier_principal_gets_most_privileged_tier():
    assert most_privileged_tier(["viewer", "raw_group"], ["raw_group", "analyst", "viewer"]) == "raw"
    assert most_privileged_tier(["viewer", "analyst"], ["raw_group", "analyst", "viewer"]) == "partial"


@pytest.mark.parametrize(("principal", "wrong_tier"), [("analyst", "full"), ("viewer", "partial")])
def test_tiered_partial_samples_cannot_hide_swapped_outputs(principal, wrong_tier):
    check = _tiered_check()
    expected = {
        "raw": [(1, "alice@example.com"), (2, "bob@example.com")],
        "partial": [(1, "a***@example.com"), (2, "[redacted]")],
        "full": [(1, "[redacted]"), (2, "[redacted]")],
    }
    actual = {
        "raw_group": expected["raw"],
        "analyst": expected["partial"],
        "viewer": expected["full"],
    }
    actual[principal] = [expected[wrong_tier][1]]
    result = evaluate_tiered_column_mask_check(check, actual, expected)
    assert result.status == INCONCLUSIVE
    assert result.evidence["missing"] == [principal]


def test_tiered_nulls_are_not_raw_leaks():
    check = _tiered_check()
    expected = {
        "raw": [(1, "alice@example.com"), (2, None)],
        "partial": [(1, "a***@example.com"), (2, None)],
        "full": [(1, "[redacted]"), (2, None)],
    }
    actual = {principal: expected[tier] for principal, tier in check.expected_tiers}
    assert evaluate_tiered_column_mask_check(check, actual, expected).status == PASS


def test_tiered_raw_leak_has_dedicated_failure_evidence():
    check = _tiered_check()
    expected = {"raw": [(1, "alice@example.com")], "partial": [(1, "a***@example.com")],
                "full": [(1, "[redacted]")]}
    actual = {"raw_group": expected["raw"], "analyst": expected["raw"], "viewer": expected["full"]}
    result = evaluate_tiered_column_mask_check(check, actual, expected)
    assert result.status == FAIL
    assert result.evidence["raw_leaks_by_principal"] == {"analyst": 1}


def test_tiered_all_moving_masked_principals_error_is_inconclusive():
    check = replace(_tiered_check(), moving_principals=("analyst", "viewer"))
    expected = {"raw": [(1, "alice@example.com")], "partial": [(1, "a***@example.com")],
                "full": [(1, "[redacted]")]}
    result = evaluate_tiered_column_mask_check(
        check, {"raw_group": expected["raw"]}, expected,
        {"analyst": "more than one mask", "viewer": "More than one mask"})
    assert result.status == INCONCLUSIVE
    assert "no masked principal" in result.detail


def test_moving_principal_permission_error_is_a_failure():
    check = replace(_tiered_check(), moving_principals=("analyst",))
    expected = {"raw": [(1, "raw")], "partial": [(1, "part")], "full": [(1, "full")]}
    result = evaluate_tiered_column_mask_check(
        check, {"raw_group": expected["raw"], "viewer": expected["full"]}, expected,
        {"analyst": "PERMISSION_DENIED: SELECT"})
    assert result.status == FAIL
    assert "PERMISSION_DENIED" in result.detail


def test_tiered_fixed_point_bound_is_inconclusive():
    check = _tiered_check()
    raw = [(key, f"value-{key}") for key in range(12)]
    partial = raw[:11] + [(11, "masked")]
    full = [(key, "[R]") for key in range(12)]
    result = evaluate_tiered_column_mask_check(
        check,
        {"raw_group": raw, "analyst": partial, "viewer": full},
        {"raw": raw, "partial": partial, "full": full},
    )
    assert result.status == INCONCLUSIVE
    assert "too many sampled rows" in result.detail


def test_row_filtered_masked_tier_with_only_null_raw_row_has_no_proof():
    check = _tiered_check()
    expected = {
        "raw": [(1, None), (2, "raw")],
        "partial": [(1, "n/a"), (2, "partial")],
        "full": [(1, "[R]"), (2, "[R]")],
    }
    actual = {
        "raw_group": expected["raw"],
        "analyst": [expected["partial"][0]],
        "viewer": expected["full"],
    }
    result = evaluate_tiered_column_mask_check(check, actual, expected)
    assert result.status == INCONCLUSIVE
    assert result.evidence["per_principal_compared"]["analyst"] == 0


def test_empty_raw_value_never_counts_as_mask_proof():
    check = replace(
        _tiered_check(), expected_tiers=(("raw_group", "raw"), ("viewer", "full")))
    expected = {"raw": [(1, "")], "partial": [(1, "[R]")], "full": [(1, "[R]")]}
    result = evaluate_tiered_column_mask_check(
        check, {"raw_group": expected["raw"], "viewer": expected["full"]}, expected)
    assert result.status == INCONCLUSIVE
    assert "no masked principal returned a distinguishing row" in result.detail


def test_moving_principal_combined_permission_and_mask_error_is_failure():
    check = replace(_tiered_check(), moving_principals=("analyst",))
    expected = {"raw": [(1, "raw")], "partial": [(1, "part")], "full": [(1, "full")]}
    result = evaluate_tiered_column_mask_check(
        check, {"raw_group": expected["raw"], "viewer": expected["full"]}, expected,
        {"analyst": "PERMISSION_DENIED; column has more than one mask"})
    assert result.status == FAIL


def test_declared_tier_must_match_overlapping_memberships():
    check = replace(_tiered_check(), expected_tiers=_tiered_check().expected_tiers + (("dual", "full"),))
    expected = {"raw": [(1, "alice@example.com")], "partial": [(1, "a***@example.com")],
                "full": [(1, "[redacted]")]}
    actual = {principal: expected[tier] for principal, tier in check.expected_tiers}
    result = evaluate_tiered_column_mask_check(
        check, actual, expected, principal_memberships={"dual": ("raw_group", "viewer")})
    assert result.status == INCONCLUSIVE
    assert "memberships resolve to raw" in result.detail


def test_deterministic_tfvars_uses_env_settings_and_derives_overlap_and_outsider(tmp_path):
    tfvars = tmp_path / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    tfvars.write_text('''
fgac_policies = [
  { name = "partial", policy_type = "POLICY_TYPE_COLUMN_MASK", to_principals = ["analyst"],
    match_condition = "hasTagValue('gr_treatment', 'email_partial')",
    function_catalog = "cat", function_schema = "gov", function_name = "partial_email" },
  { name = "full", policy_type = "POLICY_TYPE_COLUMN_MASK", to_principals = ["account users"],
    except_principals = ["raw", "analyst"], match_condition = "hasTagValue('gr_treatment', 'email_partial')",
    function_catalog = "cat", function_schema = "gov", function_name = "full_email" },
]
tag_assignments = [
  { entity_type = "columns", entity_name = "cat.sch.people.email", tag_key = "gr_treatment", tag_value = "email_partial" },
]
''')
    env.write_text('''governance_mode = "deterministic"
access_tier_groups = ["raw", "analyst", "viewer"]
raw_exempt_principals = ["etl_group"]
''')
    spec = load_spec_from_tfvars(tfvars, env_file=env, key_column="id")
    check = spec.column_masks[0]
    expectations = dict(check.expected_tiers)
    assert (check.partial_function, check.full_function) == ("cat.gov.partial_email", "cat.gov.full_email")
    assert expectations["__out_of_tier__"] == "full"
    assert expectations["__dual_tier__"] == "raw"
    assert expectations["etl_group"] == "raw"
    assert spec.principal_memberships["__dual_tier__"] == ("raw", "viewer")


def test_deterministic_two_tier_spec_uses_full_for_unused_partial(tmp_path):
    tfvars = tmp_path / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    tfvars.write_text('''fgac_policies = [{ name = "full", policy_type = "POLICY_TYPE_COLUMN_MASK",
      to_principals = ["account users"], except_principals = ["raw"],
      match_condition = "hasTagValue('gr_treatment', 'email_partial')",
      function_catalog = "cat", function_schema = "gov", function_name = "full_email" }]
tag_assignments = [{ entity_type = "columns", entity_name = "cat.sch.people.email",
  tag_key = "gr_treatment", tag_value = "email_partial" }]
''')
    env.write_text('governance_mode = "deterministic"\naccess_tier_groups = ["raw", "viewer"]\n')
    check = load_spec_from_tfvars(tfvars, env_file=env, key_column="id").column_masks[0]
    assert check.partial_function == check.full_function == "cat.gov.full_email"
    assert set(dict(check.expected_tiers).values()) == {"raw", "full"}


def test_deterministic_three_tier_never_raw_spec_uses_full_for_unused_partial(tmp_path):
    tfvars = tmp_path / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    tfvars.write_text('''fgac_policies = [{ name = "full", policy_type = "POLICY_TYPE_COLUMN_MASK",
      to_principals = ["account users"], match_condition = "hasTagValue('gr_treatment', 'secret')",
      function_catalog = "cat", function_schema = "gov", function_name = "redact" }]
tag_assignments = [{ entity_type = "columns", entity_name = "cat.sch.people.api_key",
  tag_key = "gr_treatment", tag_value = "secret" }]
''')
    env.write_text(
        'governance_mode = "deterministic"\naccess_tier_groups = ["raw", "analyst", "viewer"]\n')
    spec = load_spec_from_tfvars(tfvars, env_file=env, key_column="id")
    check = spec.column_masks[0]
    assert check.partial_function == check.full_function == "cat.gov.redact"
    assert "partial" not in set(dict(check.expected_tiers).values())


def test_deterministic_spec_rejects_raw_exempt_user_email(tmp_path):
    tfvars = tmp_path / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    tfvars.write_text("fgac_policies = []\ntag_assignments = []\n")
    env.write_text('''governance_mode = "deterministic"
access_tier_groups = ["raw", "viewer"]
raw_exempt_principals = ["alice@example.com"]
''')
    with pytest.raises(ValueError, match="user emails"):
        load_spec_from_tfvars(tfvars, env_file=env, key_column="id")


@pytest.mark.parametrize("setting", ["access_tier_groups", "raw_exempt_principals"])
@pytest.mark.parametrize("principal", ["", " ", "\t"])
def test_deterministic_spec_rejects_empty_or_whitespace_group_names(
    tmp_path, setting, principal,
):
    tfvars = tmp_path / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    tfvars.write_text("fgac_policies = []\ntag_assignments = []\n")
    env.write_text(
        'governance_mode = "deterministic"\n'
        f'{setting} = {__import__("json").dumps([principal])}\n')
    with pytest.raises(ValueError, match="non-empty group names"):
        load_spec_from_tfvars(tfvars, env_file=env, key_column="id")


def test_cli_missing_promoted_tfvars_reports_prerequisite(tmp_path):
    with pytest.raises(SystemExit) as exc:
        main(["--from-tfvars", str(tmp_path / "data_access" / "abac.auto.tfvars"), "--print-spec"])
    assert "promoted data-access config not found" in str(exc.value)
    assert "make promote" in str(exc.value)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _mask_check(masked=("Junior_Analyst",), unmasked=("Compliance_Officer",)):
    return ColumnMaskCheck(
        table="fin.finance.customers",
        column="ssn",
        key_column="customer_id",
        masked_principals=tuple(masked),
        unmasked_principals=tuple(unmasked),
        policy_name="mask_pii_ssn",
    )


def _filter_check(restricted=("Junior_Analyst",), unrestricted=("Compliance_Officer",)):
    return RowFilterCheck(
        table="fin.finance.transactions",
        restricted_principals=tuple(restricted),
        unrestricted_principals=tuple(unrestricted),
        policy_name="filter_aml_clearance",
    )


# ---------------------------------------------------------------------------
# Column-mask comparison
# ---------------------------------------------------------------------------
class TestColumnMaskComparison:
    def test_masking_effective_passes(self):
        """Masked principal sees masked value, unmasked sees raw -> PASS."""
        check = _mask_check()
        values = {
            "Junior_Analyst":     {1: "XXX-XX-6789", 2: "XXX-XX-1111"},
            "Compliance_Officer": {1: "123-45-6789", 2: "987-65-1111"},
        }
        result = evaluate_column_mask_check(check, values)
        assert result.status == PASS
        assert result.evidence["masked_ok"] == 2

    def test_leak_fails(self):
        """Masked principal sees the SAME raw value -> FAIL (leak)."""
        check = _mask_check()
        values = {
            "Junior_Analyst":     {1: "123-45-6789", 2: "XXX-XX-1111"},
            "Compliance_Officer": {1: "123-45-6789", 2: "987-65-1111"},
        }
        result = evaluate_column_mask_check(check, values)
        assert result.status == FAIL
        assert result.evidence["leaks_by_principal"] == {"Junior_Analyst": 1}
        assert "leaked" in result.detail

    def test_some_null_but_one_maskable_row_passes(self):
        """NULL/empty raw values are skipped, but a maskable row still proves it."""
        check = _mask_check()
        values = {
            "Junior_Analyst":     {1: None, 2: "", 3: "XXX-XX-3333"},
            "Compliance_Officer": {1: None, 2: "", 3: "111-22-3333"},
        }
        result = evaluate_column_mask_check(check, values)
        assert result.status == PASS
        assert result.evidence["masked_ok"] == 1

    def test_all_null_or_empty_raw_is_inconclusive(self):
        """If every raw value is NULL/empty the dataset cannot prove masking."""
        check = _mask_check()
        values = {
            "Junior_Analyst":     {1: None, 2: ""},
            "Compliance_Officer": {1: None, 2: ""},
        }
        result = evaluate_column_mask_check(check, values)
        assert result.status == INCONCLUSIVE
        assert not result.ok
        assert "NULL" in result.detail or "null" in result.detail

    def test_value_normalization_across_types(self):
        """Numeric-looking values compare after string-normalization."""
        check = ColumnMaskCheck(
            table="fin.finance.transactions", column="amount",
            key_column="txn_id",
            masked_principals=("Junior_Analyst",),
            unmasked_principals=("Compliance_Officer",),
        )
        values = {
            "Junior_Analyst":     {1: 1200.00, 2: 3400.00},   # rounded
            "Compliance_Officer": {1: 1234.56, 2: 3456.78},   # raw
        }
        result = evaluate_column_mask_check(check, values)
        assert result.status == PASS

    def test_no_unmasked_principal_is_inconclusive(self):
        check = _mask_check(unmasked=("Compliance_Officer",))
        values = {"Junior_Analyst": {1: "XXX-XX-6789"}}
        result = evaluate_column_mask_check(check, values)
        assert result.status == INCONCLUSIVE
        assert not result.ok

    def test_no_masked_principal_is_inconclusive(self):
        check = _mask_check()
        values = {"Compliance_Officer": {1: "123-45-6789"}}
        result = evaluate_column_mask_check(check, values)
        assert result.status == INCONCLUSIVE
        assert not result.ok

    def test_no_overlapping_rows_is_inconclusive(self):
        """No maskable row shared between masked and unmasked -> cannot verify."""
        check = _mask_check()
        values = {
            "Junior_Analyst":     {5: "XXX-XX-0000"},
            "Compliance_Officer": {1: "123-45-6789"},
        }
        result = evaluate_column_mask_check(check, values)
        assert result.status == INCONCLUSIVE
        assert not result.ok

    def test_admin_baseline_used_as_raw(self):
        check = _mask_check(unmasked=(DEFAULT_ADMIN_TIER,))
        values = {
            "Junior_Analyst":     {1: "XXX-XX-6789"},
            DEFAULT_ADMIN_TIER:   {1: "123-45-6789"},
        }
        result = evaluate_column_mask_check(check, values)
        assert result.status == PASS

    def test_partial_leak_fails_even_if_some_rows_masked(self):
        check = _mask_check()
        values = {
            "Junior_Analyst":     {1: "XXX-XX-6789", 2: "987-65-1111"},  # row 2 leaks
            "Compliance_Officer": {1: "123-45-6789", 2: "987-65-1111"},
        }
        result = evaluate_column_mask_check(check, values)
        assert result.status == FAIL
        assert result.evidence["leaked_rows"] == 1

    def test_conflicting_higher_tier_values_fail(self):
        """Two 'unmasked' principals disagreeing on the raw value -> FAIL.

        One of them must actually be masked differently, so no raw ground truth
        can be trusted. This is the case a mere differ-check would wrongly pass.
        """
        check = ColumnMaskCheck(
            table="fin.finance.customers", column="ssn", key_column="customer_id",
            masked_principals=("Junior_Analyst",),
            unmasked_principals=("Senior_Analyst", DEFAULT_ADMIN_TIER),
        )
        values = {
            "Junior_Analyst":   {1: "XXX-XX-6789"},
            "Senior_Analyst":   {1: "1XX-XX-6789"},   # differently masked!
            DEFAULT_ADMIN_TIER: {1: "123-45-6789"},   # truly raw
        }
        result = evaluate_column_mask_check(check, values)
        assert result.status == FAIL
        assert "disagree" in result.detail
        assert not result.ok

    def test_masked_equal_to_baseline_is_leak(self):
        """A masked principal matching the (trusted) baseline is a leak -> FAIL,
        never a silent pass."""
        check = _mask_check(masked=("Junior_Analyst",), unmasked=("Compliance_Officer",))
        values = {
            "Junior_Analyst":     {1: "123-45-6789"},
            "Compliance_Officer": {1: "123-45-6789"},
        }
        result = evaluate_column_mask_check(check, values)
        assert result.status == FAIL
        assert not result.ok

    def test_query_failure_fails(self):
        """A recorded query error on an involved principal -> FAIL."""
        check = _mask_check()
        values = {"Compliance_Officer": {1: "123-45-6789"}}
        errors = {"Junior_Analyst": "PERMISSION_DENIED: SELECT on customers"}
        result = evaluate_column_mask_check(check, values, errors)
        assert result.status == FAIL
        assert "query failed" in result.detail
        assert not result.ok


# ---------------------------------------------------------------------------
# Row-filter comparison
# ---------------------------------------------------------------------------
class TestRowFilterComparison:
    def test_filter_effective_passes(self):
        check = _filter_check()
        counts = {"Junior_Analyst": 8, "Compliance_Officer": 15}
        result = evaluate_row_filter_check(check, counts)
        assert result.status == PASS

    def test_no_restriction_fails(self):
        check = _filter_check()
        counts = {"Junior_Analyst": 15, "Compliance_Officer": 15}
        result = evaluate_row_filter_check(check, counts)
        assert result.status == FAIL
        assert "not effective" in result.detail

    def test_restricted_sees_more_fails(self):
        check = _filter_check()
        counts = {"Junior_Analyst": 20, "Compliance_Officer": 15}
        result = evaluate_row_filter_check(check, counts)
        assert result.status == FAIL

    def test_restricted_sees_zero_passes(self):
        check = _filter_check()
        counts = {"Junior_Analyst": 0, "Compliance_Officer": 15}
        result = evaluate_row_filter_check(check, counts)
        assert result.status == PASS

    def test_missing_unrestricted_is_inconclusive(self):
        check = _filter_check()
        counts = {"Junior_Analyst": 5}
        result = evaluate_row_filter_check(check, counts)
        assert result.status == INCONCLUSIVE
        assert not result.ok

    def test_missing_restricted_is_inconclusive(self):
        check = _filter_check()
        counts = {"Compliance_Officer": 15}
        result = evaluate_row_filter_check(check, counts)
        assert result.status == INCONCLUSIVE
        assert not result.ok

    def test_zero_baseline_is_inconclusive(self):
        check = _filter_check()
        counts = {"Junior_Analyst": 0, "Compliance_Officer": 0}
        result = evaluate_row_filter_check(check, counts)
        assert result.status == INCONCLUSIVE
        assert "cannot demonstrate restriction" in result.detail

    def test_none_count_for_restricted_is_inconclusive(self):
        """A restricted principal with no collected count is a gap, not a pass."""
        check = _filter_check(restricted=("Junior_Analyst", "Senior_Analyst"))
        counts = {"Junior_Analyst": None, "Senior_Analyst": 8, "Compliance_Officer": 15}
        result = evaluate_row_filter_check(check, counts)
        assert result.status == INCONCLUSIVE
        assert not result.ok

    def test_query_failure_fails(self):
        check = _filter_check()
        counts = {"Compliance_Officer": 15}
        errors = {"Junior_Analyst": "PERMISSION_DENIED"}
        result = evaluate_row_filter_check(check, counts, errors)
        assert result.status == FAIL
        assert "query failed" in result.detail
        assert not result.ok


# ---------------------------------------------------------------------------
# Tag-condition parsing / column resolution
# ---------------------------------------------------------------------------
class TestTagConditionParsing:
    def test_parse_single_condition(self):
        assert parse_tag_conditions("hasTagValue('pii_level', 'masked_ssn')") == [
            ("pii_level", "masked_ssn")
        ]

    def test_parse_double_quotes(self):
        assert parse_tag_conditions('hasTagValue("pci_level", "redacted_cvv")') == [
            ("pci_level", "redacted_cvv")
        ]

    def test_parse_multiple_conditions(self):
        cond = "hasTagValue('a','1') OR hasTagValue('b','2')"
        assert parse_tag_conditions(cond) == [("a", "1"), ("b", "2")]

    def test_parse_empty(self):
        assert parse_tag_conditions("") == []
        assert parse_tag_conditions(None) == []

    def test_resolve_columns(self):
        tag_assignments = [
            {"entity_type": "columns", "entity_name": "fin.finance.customers.ssn",
             "tag_key": "pii_level", "tag_value": "masked_ssn"},
            {"entity_type": "columns", "entity_name": "fin.finance.customers.email",
             "tag_key": "pii_level", "tag_value": "masked_email"},
        ]
        cols = resolve_columns_for_condition(
            "hasTagValue('pii_level', 'masked_ssn')", tag_assignments
        )
        assert cols == [{"table": "fin.finance.customers", "column": "ssn"}]

    def test_resolve_tables(self):
        tag_assignments = [
            {"entity_type": "tables", "entity_name": "fin.finance.transactions",
             "tag_key": "compliance_scope", "tag_value": "aml_restricted"},
        ]
        tables = resolve_columns_for_condition(
            "hasTagValue('compliance_scope', 'aml_restricted')",
            tag_assignments, entity_type="tables",
        )
        assert tables == [{"table": "fin.finance.transactions", "column": ""}]

    def test_resolve_handles_hcl_list_wrapped_scalars(self):
        """hcl2 wraps scalars in single-item lists — resolution must cope."""
        tag_assignments = [
            {"entity_type": ["columns"], "entity_name": ["fin.finance.customers.ssn"],
             "tag_key": ["pii_level"], "tag_value": ["masked_ssn"]},
        ]
        cols = resolve_columns_for_condition(
            "hasTagValue('pii_level', 'masked_ssn')", tag_assignments
        )
        assert cols == [{"table": "fin.finance.customers", "column": "ssn"}]


# ---------------------------------------------------------------------------
# Spec derivation from config
# ---------------------------------------------------------------------------
class TestDeriveSpec:
    FGAC = [
        {
            "name": "mask_pii_ssn", "policy_type": "POLICY_TYPE_COLUMN_MASK",
            "to_principals": ["Junior_Analyst"],
            "match_condition": "hasTagValue('pii_level', 'masked_ssn')",
            "function_name": "mask_ssn",
        },
        {
            "name": "filter_aml", "policy_type": "POLICY_TYPE_ROW_FILTER",
            "to_principals": ["Junior_Analyst", "Senior_Analyst"],
            "when_condition": "hasTagValue('compliance_scope', 'aml_restricted')",
            "function_name": "filter_aml_clearance",
        },
    ]
    TAGS = [
        {"entity_type": "columns", "entity_name": "fin.finance.customers.ssn",
         "tag_key": "pii_level", "tag_value": "masked_ssn"},
        {"entity_type": "tables", "entity_name": "fin.finance.transactions",
         "tag_key": "compliance_scope", "tag_value": "aml_restricted"},
    ]
    GROUPS = ["Junior_Analyst", "Senior_Analyst", "Compliance_Officer"]

    def test_derives_column_mask(self):
        spec = derive_spec_from_config(
            self.FGAC, self.TAGS, self.GROUPS, key_column="customer_id",
        )
        assert len(spec.column_masks) == 1
        mc = spec.column_masks[0]
        assert mc.table == "fin.finance.customers"
        assert mc.column == "ssn"
        assert mc.key_column == "customer_id"
        assert mc.masked_principals == ("Junior_Analyst",)
        assert "Senior_Analyst" in mc.unmasked_principals
        assert "Compliance_Officer" in mc.unmasked_principals
        assert DEFAULT_ADMIN_TIER in mc.unmasked_principals
        assert "Junior_Analyst" not in mc.unmasked_principals

    def test_derives_row_filter(self):
        spec = derive_spec_from_config(self.FGAC, self.TAGS, self.GROUPS)
        assert len(spec.row_filters) == 1
        rf = spec.row_filters[0]
        assert rf.table == "fin.finance.transactions"
        assert set(rf.restricted_principals) == {"Junior_Analyst", "Senior_Analyst"}
        assert "Compliance_Officer" in rf.unrestricted_principals

    def test_all_users_mask_only_exceptions_are_unmasked(self):
        """A mask targeting 'account users' masks every concrete tier except the
        exceptions, and the exceptions are the only raw baseline (admin is also
        a member of 'account users' so it is NOT a baseline)."""
        fgac = [{
            "name": "mask_cvv", "policy_type": "POLICY_TYPE_COLUMN_MASK",
            "to_principals": ["account users"],
            "except_principals": ["Compliance_Officer"],
            "match_condition": "hasTagValue('pci_level', 'redacted_cvv')",
        }]
        tags = [{"entity_type": "columns",
                 "entity_name": "fin.finance.credit_cards.cvv",
                 "tag_key": "pci_level", "tag_value": "redacted_cvv"}]
        spec = derive_spec_from_config(
            fgac, tags, ["Junior_Analyst", "Senior_Analyst", "Compliance_Officer"],
        )
        mc = spec.column_masks[0]
        assert "account users" not in mc.masked_principals
        assert "account users" not in mc.unmasked_principals
        assert set(mc.masked_principals) == {"Junior_Analyst", "Senior_Analyst"}
        assert mc.unmasked_principals == ("Compliance_Officer",)
        assert DEFAULT_ADMIN_TIER not in mc.unmasked_principals

    def test_key_column_by_table_overrides_default(self):
        spec = derive_spec_from_config(
            self.FGAC, self.TAGS, self.GROUPS,
            key_column="id",
            key_column_by_table={"fin.finance.customers": "customer_id"},
        )
        assert spec.column_masks[0].key_column == "customer_id"

    def test_unmatched_condition_yields_no_check(self):
        fgac = [{
            "name": "mask_orphan", "policy_type": "POLICY_TYPE_COLUMN_MASK",
            "to_principals": ["Junior_Analyst"],
            "match_condition": "hasTagValue('nonexistent', 'value')",
        }]
        spec = derive_spec_from_config(fgac, self.TAGS, self.GROUPS)
        assert spec.column_masks == []
        assert spec.is_empty()

    def test_spec_principals_excludes_admin(self):
        spec = derive_spec_from_config(self.FGAC, self.TAGS, self.GROUPS)
        assert DEFAULT_ADMIN_TIER not in spec.principals
        assert "Junior_Analyst" in spec.principals

    def test_hcl_list_wrapped_policy_fields(self):
        """Fields parsed by hcl2 as single-item lists still derive correctly."""
        fgac = [{
            "name": ["mask_pii_ssn"], "policy_type": ["POLICY_TYPE_COLUMN_MASK"],
            "to_principals": ["Junior_Analyst"],
            "match_condition": ["hasTagValue('pii_level', 'masked_ssn')"],
        }]
        spec = derive_spec_from_config(fgac, self.TAGS, self.GROUPS, key_column="customer_id")
        assert len(spec.column_masks) == 1
        assert spec.column_masks[0].policy_name == "mask_pii_ssn"


# ---------------------------------------------------------------------------
# Report aggregation
# ---------------------------------------------------------------------------
class TestReport:
    def test_report_passed_when_all_pass(self):
        spec = VerificationSpec(column_masks=[_mask_check()], row_filters=[_filter_check()])
        column_values = {
            ("fin.finance.customers", "ssn"): {
                "Junior_Analyst":     {1: "XXX-XX-6789"},
                "Compliance_Officer": {1: "123-45-6789"},
            }
        }
        row_counts = {"fin.finance.transactions": {"Junior_Analyst": 8, "Compliance_Officer": 15}}
        report = evaluate_effective_access(spec, column_values, row_counts)
        assert report.passed
        assert report.counts()[PASS] == 2
        assert report.failures == []

    def test_report_fails_on_leak(self):
        spec = VerificationSpec(column_masks=[_mask_check()])
        column_values = {
            ("fin.finance.customers", "ssn"): {
                "Junior_Analyst":     {1: "123-45-6789"},
                "Compliance_Officer": {1: "123-45-6789"},
            }
        }
        report = evaluate_effective_access(spec, column_values, {})
        assert not report.passed
        assert len(report.failures) == 1

    def test_report_propagates_query_errors(self):
        spec = VerificationSpec(column_masks=[_mask_check()])
        column_values = {("fin.finance.customers", "ssn"): {"Compliance_Officer": {1: "123-45-6789"}}}
        column_errors = {("fin.finance.customers", "ssn"): {"Junior_Analyst": "boom"}}
        report = evaluate_effective_access(spec, column_values, {}, column_errors, {})
        assert not report.passed
        assert report.results[0].status == FAIL

    def test_summary_renders_markers(self):
        report = EffectiveAccessReport()
        report.add(CheckResult("column-mask", "x", PASS, "ok"))
        report.add(CheckResult("row-filter", "y", FAIL, "leaked"))
        text = report.summary()
        assert "✓" in text and "✗" in text
        assert "NOT VERIFIED" in text

    def test_summary_never_says_all_effective_when_masks_were_skipped(self):
        report = EffectiveAccessReport(
            results=[CheckResult("row-filter", "rows", PASS, "ok")],
            not_verified=[CheckResult("column-mask", "mask", INCONCLUSIVE, "no key")],
        )
        text = report.summary()
        assert "ROW FILTERS EFFECTIVE" in text
        assert "1 mask check(s) NOT VERIFIED" in text
        assert "ALL EFFECTIVE" not in text

    def test_inconclusive_does_not_pass(self):
        """A single inconclusive check must block the whole report."""
        report = EffectiveAccessReport()
        report.add(CheckResult("column-mask", "x", PASS, "ok"))
        report.add(CheckResult("column-mask", "y", INCONCLUSIVE, "no data"))
        assert not report.passed
        assert len(report.failures) == 1
        assert "NOT VERIFIED" in report.summary()

    def test_empty_report_does_not_pass(self):
        """Verifying nothing is not success."""
        report = EffectiveAccessReport()
        assert not report.passed
        assert "no checks were run" in report.summary()

    def test_empty_spec_report_does_not_pass(self):
        report = evaluate_effective_access(VerificationSpec(), {}, {})
        assert not report.passed


# ---------------------------------------------------------------------------
# Spec file loading
# ---------------------------------------------------------------------------
class TestSpecLoading:
    def test_load_spec_from_json(self, tmp_path):
        spec_file = tmp_path / "spec.json"
        spec_file.write_text("""
        {
          "column_masks": [
            {"table": "c.s.t", "column": "ssn", "key_column": "id",
             "masked_principals": ["Junior"], "unmasked_principals": ["Senior"],
             "policy_name": "m1"}
          ],
          "row_filters": [
            {"table": "c.s.t2", "restricted_principals": ["Junior"],
             "unrestricted_principals": ["Senior"], "policy_name": "f1"}
          ]
        }
        """)
        spec = load_spec_from_file(spec_file)
        assert len(spec.column_masks) == 1
        assert spec.column_masks[0].column == "ssn"
        assert len(spec.row_filters) == 1
        assert spec.row_filters[0].table == "c.s.t2"

    @staticmethod
    def _tagged_key_tfvars(tmp_path, match_condition):
        tfvars = tmp_path / "abac.auto.tfvars"
        tfvars.write_text(f'''
fgac_policies = [{{
  name = "mask_ssn"
  policy_type = "POLICY_TYPE_COLUMN_MASK"
  to_principals = ["Junior"]
  match_condition = "{match_condition}"
}}]
tag_assignments = [
  {{ entity_type = "columns", entity_name = "c.s.t.ssn", tag_key = "pii", tag_value = "ssn" }},
  {{ entity_type = "columns", entity_name = "C.S.T.Customer_ID", tag_key = "class", tag_value = "identifier" }},
]
''')
        return tfvars

    def test_tfvars_accepts_a_key_tag_no_mask_policy_matches(self, tmp_path):
        tfvars = self._tagged_key_tfvars(tmp_path, "hasTagValue('pii', 'ssn')")
        spec = load_spec_from_tfvars(tfvars, key_column="customer_id")
        assert [c.key_column for c in spec.column_masks] == ["customer_id"]

    def test_tfvars_rejects_case_insensitive_mask_matched_key(self, tmp_path):
        tfvars = self._tagged_key_tfvars(
            tmp_path, "hasTagValue('pii', 'ssn') OR hasTagValue('class', 'identifier')")
        with pytest.raises(ValueError) as exc:
            load_spec_from_tfvars(tfvars, key_column="customer_id")
        assert str(exc.value) == (
            "ERROR: row-pairing key customer_id may be masked for Junior, __admin__ on c.s.t "
            "(it has 1 column tag(s) a column-mask policy matches); "
            "choose a unique, non-null, unmasked key")


# ---------------------------------------------------------------------------
# CLI: empty spec must not report success
# ---------------------------------------------------------------------------
class TestCliEmptySpec:
    def test_main_returns_error_for_empty_spec(self, tmp_path, capsys):
        spec_file = tmp_path / "empty.json"
        spec_file.write_text('{"column_masks": [], "row_filters": []}')
        rc = main(["--spec", str(spec_file)])
        assert rc == 2
        err = capsys.readouterr().err
        assert "no effective-access checks" in err.lower()

    def test_main_dry_run_prints_spec(self, tmp_path, capsys):
        spec_file = tmp_path / "spec.json"
        spec_file.write_text(
            '{"column_masks": [{"table": "c.s.t", "column": "ssn", "key_column": "id",'
            '"masked_principals": ["Jr"], "unmasked_principals": ["Sr"]}],'
            '"row_filters": []}'
        )
        rc = main(["--spec", str(spec_file)])
        assert rc == 0
        out = capsys.readouterr().out
        assert "dry run" in out

    def test_main_rejects_sensitive_key_column(self, tmp_path):
        spec_file = tmp_path / "spec.json"
        spec_file.write_text(
            '{"column_masks": [{"table": "c.s.t", "column": "ssn", '
            '"key_column": "ssn", "masked_principals": ["Jr"], '
            '"unmasked_principals": ["Sr"]}], "row_filters": []}'
        )
        with pytest.raises(SystemExit, match="itself classified sensitive/masked"):
            main(["--spec", str(spec_file)])

    def test_main_rejects_case_insensitive_sensitive_key_column(self, tmp_path):
        spec_file = tmp_path / "spec.json"
        spec_file.write_text(
            '{"column_masks": [{"table": "c.s.t", "column": "SSN", '
            '"key_column": "ssn", "masked_principals": ["Jr"], '
            '"unmasked_principals": ["Sr"]}], "row_filters": []}'
        )
        with pytest.raises(SystemExit, match="itself classified sensitive/masked"):
            main(["--spec", str(spec_file)])

    # A keyless mask check is no longer skipped: its key is picked and proven
    # live, per table, so the dry run says so instead of "skipped".
    def test_main_keyless_check_is_picked_live(self, tmp_path, capsys):
        spec_file = tmp_path / "spec.json"
        spec_file.write_text(
            '{"column_masks": [{"table": "c.s.t", "column": "ssn", '
            '"masked_principals": ["Jr"], "unmasked_principals": ["Sr"]}], '
            '"row_filters": []}'
        )
        assert main(["--spec", str(spec_file)]) == 0
        out = capsys.readouterr().out
        assert "key=auto (primary key or an id-like column; picked and proven live)" in out
        assert "skipped" not in out

    # make release passes --require-mask-checks: a keyless check is no longer
    # refused up front (the key is picked live; a table with none is blocking).
    def test_main_require_mask_checks_accepts_keyless_checks_for_live_picking(self, tmp_path, capsys):
        spec_file = tmp_path / "spec.json"
        spec_file.write_text(
            '{"column_masks": [{"table": "c.s.t", "column": "ssn", '
            '"masked_principals": ["Jr"], "unmasked_principals": ["Sr"]}], '
            '"row_filters": []}'
        )
        assert main(["--spec", str(spec_file), "--require-mask-checks", "--key-column", "id"]) == 0
        assert "key=auto ('id' if the table has it" in capsys.readouterr().out

    # A mask verify-access can derive no check for must fail the strict run,
    # not leave it passing on the checks it could derive: "everyone except
    # analysts" has no concrete masked tier when the account config lists no
    # other groups.
    def test_main_require_mask_checks_fails_when_a_tagged_mask_yields_no_check(self, tmp_path, capsys):
        tfvars = tmp_path / "abac.auto.tfvars"
        tfvars.write_text("""
fgac_policies = [
  { name = "everyone", policy_type = "POLICY_TYPE_COLUMN_MASK", to_principals = ["account users"],
    except_principals = ["analysts"], match_condition = "hasTagValue('gr_treatment', 'redact')" },
  { name = "analysts", policy_type = "POLICY_TYPE_COLUMN_MASK", to_principals = ["analysts"],
    match_condition = "hasTagValue('gr_treatment', 'name_partial')" },
]
tag_assignments = [
  { entity_type = "columns", entity_name = "cat.sch.t.ssn", tag_key = "gr_treatment", tag_value = "redact" },
  { entity_type = "columns", entity_name = "cat.sch.t.name", tag_key = "gr_treatment", tag_value = "name_partial" },
]
""")
        account = tmp_path / "account.auto.tfvars"
        account.write_text("groups = {}\n")
        args = ["--from-tfvars", str(tfvars), "--account-tfvars", str(account), "--key-column", "id"]
        assert main(args) == 0  # non-strict: the one derivable check dry-runs
        capsys.readouterr()
        assert main(args + ["--require-mask-checks"]) == 2
        err = capsys.readouterr().err
        assert "cat.sch.t.ssn" in err and "would NOT be verified" in err

    def test_main_require_mask_checks_accepts_keyed_checks(self, tmp_path, capsys):
        spec_file = tmp_path / "spec.json"
        spec_file.write_text(
            '{"column_masks": [{"table": "c.s.t", "column": "ssn", "key_column": "id",'
            '"masked_principals": ["Jr"], "unmasked_principals": ["Sr"]}],'
            '"row_filters": []}'
        )
        assert main(["--spec", str(spec_file), "--require-mask-checks"]) == 0
        assert "dry run" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Live guard — must be airtight
# ---------------------------------------------------------------------------
class TestLiveGuard:
    def test_orchestrator_disabled_without_env(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GENIERAILS_LIVE_VERIFY", raising=False)
        with pytest.raises(RuntimeError, match="Live verification is disabled"):
            verify_effective_access_live(
                VerificationSpec(column_masks=[_mask_check()]),
                tmp_path / "auth.auto.tfvars",
            )

    def test_verifier_construction_blocked_without_env(self, monkeypatch):
        """The live class itself cannot even be instantiated without the flag."""
        monkeypatch.delenv("GENIERAILS_LIVE_VERIFY", raising=False)
        with pytest.raises(RuntimeError, match="Live verification is disabled"):
            EffectiveAccessVerifier({"host": "h", "client_id": "c", "client_secret": "s"})

    def test_verifier_methods_reguard_if_flag_unset_after_construction(self, monkeypatch):
        """Even if constructed with the flag, a live method re-checks it."""
        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        verifier = EffectiveAccessVerifier(
            {"host": "h", "client_id": "c", "client_secret": "s",
             "account_host": "a", "account_id": "1"}
        )
        monkeypatch.delenv("GENIERAILS_LIVE_VERIFY", raising=False)
        # No network is reached — the guard raises first.
        with pytest.raises(RuntimeError, match="Live verification is disabled"):
            verifier.provision_principal("Junior_Analyst")
        with pytest.raises(RuntimeError, match="Live verification is disabled"):
            verifier.resolve_warehouse()

    def test_missing_configured_key_column_has_clear_error(self, monkeypatch):
        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        verifier = EffectiveAccessVerifier(
            {"host": "h", "client_id": "c", "client_secret": "s"},
            warehouse_id="warehouse-123",
        )
        monkeypatch.setattr(verifier, "_ws_for", lambda principal: object())
        monkeypatch.setattr(
            verifier, "run_query",
            lambda ws, sql, params=None: (_ for _ in ()).throw(
                RuntimeError("[UNRESOLVED_COLUMN] customer_id cannot be resolved")
            ),
        )
        check = _mask_check()
        principal = VerificationPrincipal("Junior", "test", "app", "secret")
        with pytest.raises(
            RuntimeError,
            match="verification key column 'customer_id' is missing or inaccessible",
        ):
            verifier.collect_column_values(principal, check)

    def test_non_column_query_error_keeps_accurate_message(self, monkeypatch):
        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        verifier = EffectiveAccessVerifier(
            {"host": "h", "client_id": "c", "client_secret": "s"},
            warehouse_id="warehouse-123",
        )
        monkeypatch.setattr(verifier, "_ws_for", lambda principal: object())
        original = RuntimeError("PERMISSION_DENIED: SELECT denied on table c.s.t")
        monkeypatch.setattr(
            verifier, "run_query",
            lambda ws, sql, params=None: (_ for _ in ()).throw(original),
        )
        with pytest.raises(RuntimeError, match="PERMISSION_DENIED") as exc:
            verifier.collect_column_values(
                VerificationPrincipal("Junior", "test", "app", "secret"),
                _mask_check(),
            )
        assert exc.value is original

    def test_verifier_does_not_pick_an_arbitrary_workspace_warehouse(self, monkeypatch):
        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        verifier = EffectiveAccessVerifier(
            {"host": "h", "client_id": "c", "client_secret": "s"}
        )
        with pytest.raises(RuntimeError, match="Arbitrary workspace warehouse selection is disabled"):
            verifier.resolve_warehouse()


class TestTemporaryWarehouseAccess:
    @staticmethod
    def _verifier(monkeypatch, permissions):
        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        verifier = EffectiveAccessVerifier(
            {"host": "h", "client_id": "c", "client_secret": "s",
             "account_host": "a", "account_id": "1"},
            warehouse_id="warehouse-123",
        )
        verifier._admin_ws = type("AdminWorkspace", (), {"permissions": permissions})()
        return verifier

    def test_grants_can_use_to_temporary_principal(self, monkeypatch):
        calls = []

        class FakePermissions:
            def update(self, request_object_type, request_object_id, **kwargs):
                calls.append((request_object_type, request_object_id, kwargs))

        verifier = self._verifier(monkeypatch, FakePermissions())
        verifier.grant_warehouse_use(VerificationPrincipal(
            tier="viewers",
            display_name="genierails-verify-viewers",
            application_id="app-123",
            client_secret="secret",
            sp_id="456",
        ))

        object_type, object_id, kwargs = calls[0]
        access = kwargs["access_control_list"][0]
        assert object_type == "warehouses"
        assert object_id == "warehouse-123"
        assert access.service_principal_name == "app-123"
        assert access.permission_level.value == "CAN_USE"

    def test_outsider_table_access_is_exact_and_reversible(self, monkeypatch):
        verifier = self._verifier(monkeypatch, object())
        statements = []
        monkeypatch.setattr(verifier, "run_query", lambda _ws, sql: statements.append(sql))
        principal = VerificationPrincipal(
            "__out_of_tier__", "test-outsider", "01234567-89ab-cdef", "secret", "456")

        verifier.grant_outsider_table_access(
            principal, ["cat.sales.customers", "cat.sales.customers"])
        verifier.grant_outsider_table_access(
            principal, ["cat.sales.customers"], revoke=True)

        assert statements == [
            "GRANT USE CATALOG ON CATALOG `cat` TO `01234567-89ab-cdef`",
            "GRANT USE SCHEMA ON SCHEMA `cat`.`sales` TO `01234567-89ab-cdef`",
            "GRANT SELECT ON TABLE `cat`.`sales`.`customers` TO `01234567-89ab-cdef`",
            "REVOKE SELECT ON TABLE `cat`.`sales`.`customers` FROM `01234567-89ab-cdef`",
            "REVOKE USE SCHEMA ON SCHEMA `cat`.`sales` FROM `01234567-89ab-cdef`",
            "REVOKE USE CATALOG ON CATALOG `cat` FROM `01234567-89ab-cdef`",
        ]

    def test_outsider_revoke_attempts_every_privilege_after_an_error(self, monkeypatch):
        verifier = self._verifier(monkeypatch, object())
        statements = []

        def fail_first(_ws, sql):
            statements.append(sql)
            if "SELECT" in sql:
                raise RuntimeError("already absent")

        monkeypatch.setattr(verifier, "run_query", fail_first)
        principal = VerificationPrincipal(
            "__out_of_tier__", "test-outsider", "app", "secret", "456")
        with pytest.raises(RuntimeError, match="failed to revoke 1"):
            verifier.grant_outsider_table_access(
                principal, ["cat.sales.customers"], revoke=True)
        assert len(statements) == 3

    @pytest.mark.parametrize("membership", ["viewers", 'team"blue\\ops', "équipe"])
    def test_provision_assigns_exact_temporary_group_before_return(
        self, monkeypatch, membership,
    ):
        from types import SimpleNamespace
        from unittest.mock import Mock

        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        verifier = EffectiveAccessVerifier({
            "host": "h", "client_id": "c", "client_secret": "s",
            "account_host": "a", "account_id": "1", "workspace_id": "123",
        })
        sp = SimpleNamespace(
            id="456", application_id="app-123",
            display_name="genierails-verify-viewers")
        nonexact_sp = SimpleNamespace(
            id="999", application_id="wrong-app",
            display_name="Genierails-Verify-Viewers")
        group = SimpleNamespace(id="789", display_name=membership, members=[])
        group_filters = []

        def list_groups(**kwargs):
            group_filters.append(kwargs["filter"])
            return [group]
        create_sp = Mock()
        account = SimpleNamespace(
            service_principals=SimpleNamespace(
                list=lambda **_: [nonexact_sp, sp], create=create_sp,
            ),
            service_principal_secrets=SimpleNamespace(
                create=lambda **_: SimpleNamespace(secret="secret"),
            ),
            groups=SimpleNamespace(
                list=list_groups, patch=Mock(),
            ),
            workspace_assignment=SimpleNamespace(update=Mock()),
        )
        workspace = SimpleNamespace(
            service_principals=SimpleNamespace(list=lambda **_: [sp]),
        )
        verifier._account = account
        verifier._admin_ws = workspace

        principal = verifier.provision_principal("viewers", memberships=(membership,))

        account.workspace_assignment.update.assert_called_once()
        call = account.workspace_assignment.update.call_args.kwargs
        assert call["workspace_id"] == 123
        assert call["principal_id"] == 456
        assert call["permissions"][0].value == "USER"
        assert principal.application_id == "app-123"
        create_sp.assert_not_called()
        assert group_filters == [
            f"displayName eq {__import__('json').dumps(membership, ensure_ascii=False)}"]

    @pytest.mark.parametrize("returned_names", [
        ["analysts-prefix"],
        ["Analysts"],
        ["analysts", "analysts"],
    ])
    def test_provision_refuses_nonexact_or_ambiguous_group_matches(
        self, monkeypatch, returned_names,
    ):
        from types import SimpleNamespace
        from unittest.mock import Mock

        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        verifier = EffectiveAccessVerifier({
            "host": "h", "client_id": "c", "client_secret": "s",
            "account_host": "a", "account_id": "1", "workspace_id": "123",
        })
        create = Mock()
        create_secret = Mock()
        verifier._account = SimpleNamespace(
            groups=SimpleNamespace(list=lambda **_: [
                SimpleNamespace(id=str(index), display_name=name, members=[])
                for index, name in enumerate(returned_names)]),
            service_principals=SimpleNamespace(create=create),
            service_principal_secrets=SimpleNamespace(create=create_secret),
        )
        with pytest.raises(RuntimeError, match="expected exactly one"):
            verifier.provision_principal("analysts")
        create.assert_not_called()
        create_secret.assert_not_called()

    def test_warehouse_grant_failure_aborts_verification_setup(self, monkeypatch):
        class FailingPermissions:
            def update(self, *args, **kwargs):
                raise RuntimeError("warehouse permission denied")

        verifier = self._verifier(monkeypatch, FailingPermissions())
        principal = VerificationPrincipal("viewers", "test", "app-123", "secret", "456")

        with pytest.raises(RuntimeError, match="warehouse permission denied"):
            verifier.grant_warehouse_use(principal)

    def test_orchestrator_cleans_up_when_warehouse_grant_fails(self, monkeypatch, tmp_path):
        deleted = []

        class FakeVerifier:
            def __init__(self, auth, warehouse_id=""):
                pass

            def resolve_warehouse(self):
                return "warehouse-123"

            def resolve_principal_groups(self, memberships):
                return list(memberships)

            def provision_principal(self, tier, memberships=None, *, resolved_groups=None):
                return VerificationPrincipal(tier, f"test-{tier}", "app-123", "secret", "456")

            def grant_warehouse_use(self, principal):
                raise RuntimeError("warehouse permission denied")

            def deprovision_principal(self, principal):
                deleted.append(principal.sp_id)

        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        monkeypatch.setattr("verify_effective_access.load_auth", lambda path: {
            "host": "h", "client_id": "c", "client_secret": "s",
        })
        monkeypatch.setattr("verify_effective_access.EffectiveAccessVerifier", FakeVerifier)

        with pytest.raises(RuntimeError, match="warehouse permission denied"):
            verify_effective_access_live(
                VerificationSpec(column_masks=[_mask_check(masked=("viewers",), unmasked=())]),
                tmp_path / "auth.auto.tfvars",
            )

        assert deleted == ["456"]

    def test_all_group_lookups_precede_any_principal_or_secret_creation(
        self, monkeypatch, tmp_path,
    ):
        events = []

        class FakeVerifier:
            def __init__(self, auth, warehouse_id=""):
                self.mask_config = None

            def resolve_warehouse(self):
                return "warehouse-123"

            def resolve_principal_groups(self, memberships):
                name = tuple(memberships)
                events.append(("lookup", name))
                if name == ("viewers",):
                    raise RuntimeError("expected exactly one")
                return list(memberships)

            def provision_principal(self, tier, memberships=None, *, resolved_groups=None):
                events.append(("create", tier))
                raise AssertionError("pre-flight failure must prevent provisioning")

        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        monkeypatch.setattr("verify_effective_access.load_auth", lambda path: {
            "host": "h", "client_id": "c", "client_secret": "s",
        })
        monkeypatch.setattr("verify_effective_access.EffectiveAccessVerifier", FakeVerifier)
        spec = VerificationSpec(row_filters=[RowFilterCheck(
            table="cat.sch.people",
            restricted_principals=("analysts", "viewers"),
            unrestricted_principals=(),
        )])

        with pytest.raises(RuntimeError, match="expected exactly one"):
            verify_effective_access_live(spec, tmp_path / "auth.auto.tfvars")

        assert events == [
            ("lookup", ("analysts",)),
            ("lookup", ("viewers",)),
        ]


class TestTieredLiveGrantLifecycle:
    @staticmethod
    def _run(monkeypatch, tmp_path, *, revoke_error=None, keep=False, sample_error=None, log=None):
        log = [] if log is None else log
        raw = {key: f"raw-{key}" for key in range(12)}
        partial = {key: f"partial-{key}" for key in range(12)}
        full = {key: "[R]" for key in range(12)}
        values = {"raw": raw, "partial": partial, "full": full}

        class FakeVerifier:
            def __init__(self, auth, warehouse_id=""):
                self.mask_config = None

            def resolve_warehouse(self):
                return "warehouse"

            def resolve_principal_groups(self, memberships):
                log.append(("lookup", tuple(memberships)))
                return list(memberships)

            def provision_principal(
                self, tier, memberships=None, *, resolved_groups=None,
            ):
                log.append(("provision", tier))
                return VerificationPrincipal(tier, f"test-{tier}", f"app-{tier}", "secret", tier)

            def grant_warehouse_use(self, principal):
                log.append(("warehouse", principal.tier))

            def grant_outsider_table_access(self, principal, tables, *, revoke=False):
                log.append(("revoke" if revoke else "grant", principal.tier, tuple(tables)))
                if revoke and revoke_error:
                    raise revoke_error

            def deprovision_principal(self, principal):
                log.append(("deprovision", principal.tier))

            def key_mask_metadata(self, principal, check):
                return []

            def collect_column_values(self, principal, check, limit=25, keys=None, salt=None):
                if sample_error and principal.tier == "analyst":
                    raise sample_error
                tier = dict(check.expected_tiers).get(principal.tier, "raw")
                selected = list(keys) if keys is not None else list(raw)[:limit]
                return [(key, values[tier][key]) for key in selected]

            def prove_key_unique(self, principal, check, keys):
                return ""

            def count_rows_with_keys(self, principal, check, keys):
                return len(keys)

            def expected_tier_values(self, principal, check, keys):
                return {tier: [(key, tier_values[key]) for key in keys]
                        for tier, tier_values in values.items()}

        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        monkeypatch.setenv("GENIERAILS_VERIFY_PROPAGATION_SLEEP", "0")
        monkeypatch.setattr("verify_effective_access.load_auth", lambda path: {
            "client_id": "admin", "client_secret": "secret"})
        monkeypatch.setattr("verify_effective_access.EffectiveAccessVerifier", FakeVerifier)
        monkeypatch.setattr(
            "verify_effective_access.pick_pairing_keys",
            lambda verifier, principal, checks, **kwargs: {
                check.table.lower(): KeyPick(check.table, "id", "explicit") for check in checks},
        )
        check = ColumnMaskCheck(
            table="cat.sch.people", column="email", key_column="id",
            masked_principals=(), unmasked_principals=(),
            expected_tiers=(("raw_group", "raw"), ("analyst", "partial"),
                            ("viewer", "full"), (OUT_OF_TIER_PRINCIPAL, "full"),
                            (DEFAULT_ADMIN_TIER, "raw")),
            partial_function="cat.gov.partial", full_function="cat.gov.full")
        spec = VerificationSpec(column_masks=[check], principal_memberships={
            "raw_group": ("raw_group",), "analyst": ("analyst",),
            "viewer": ("viewer",), OUT_OF_TIER_PRINCIPAL: ()})
        report = verify_effective_access_live(
            spec, tmp_path / "auth.auto.tfvars", keep_principals=keep)
        return report, log, spec

    @pytest.mark.parametrize("keep", [False, True])
    def test_live_flow_grants_then_revokes_outsider_even_when_principal_is_kept(
        self, monkeypatch, tmp_path, keep,
    ):
        report, log, _spec = self._run(monkeypatch, tmp_path, keep=keep)
        assert report.passed
        assert ("grant", OUT_OF_TIER_PRINCIPAL, ("cat.sch.people",)) in log
        assert ("revoke", OUT_OF_TIER_PRINCIPAL, ("cat.sch.people",)) in log
        assert log.index(("grant", OUT_OF_TIER_PRINCIPAL, ("cat.sch.people",))) < log.index(
            ("revoke", OUT_OF_TIER_PRINCIPAL, ("cat.sch.people",)))
        assert (("deprovision", OUT_OF_TIER_PRINCIPAL) in log) is (not keep)

    def test_failed_revoke_is_reported_and_written_as_failure(self, monkeypatch, tmp_path, capsys):
        report, _log, spec = self._run(
            monkeypatch, tmp_path, keep=True, revoke_error=RuntimeError("revoke denied"))
        assert not report.passed
        cleanup = next(result for result in report.results if result.kind == "cleanup")
        assert cleanup.status == FAIL
        assert "app-__out_of_tier__" in cleanup.detail
        assert "REVOKE" in cleanup.detail and "cat.sch.people" in cleanup.detail
        assert "ERROR" in capsys.readouterr().err
        result_file = tmp_path / "result.json"
        write_result_file(result_file, report, spec)
        payload = __import__("json").loads(result_file.read_text())
        assert payload["passed"] is False
        assert payload["cleanup_failures"][0]["target"] == "app-__out_of_tier__"
        assert "REVOKE SELECT" in payload["cleanup_failures"][0]["detail"]

    def test_exception_and_keyboard_interrupt_still_revoke(self, monkeypatch, tmp_path):
        error_log = []
        report, _unused, _spec = self._run(
            monkeypatch, tmp_path, sample_error=RuntimeError("query failed"), log=error_log)
        assert not report.passed
        assert ("revoke", OUT_OF_TIER_PRINCIPAL, ("cat.sch.people",)) in error_log

        interrupt_log = []
        with pytest.raises(KeyboardInterrupt):
            self._run(
                monkeypatch, tmp_path, sample_error=KeyboardInterrupt("cancelled"),
                log=interrupt_log)
        assert ("revoke", OUT_OF_TIER_PRINCIPAL, ("cat.sch.people",)) in interrupt_log

    def test_sigterm_and_sighup_handlers_raise_for_finally_cleanup(self, monkeypatch, tmp_path):
        installed = []
        real_getsignal = __import__("signal").getsignal

        def remember(signum, handler):
            installed.append((signum, handler))

        monkeypatch.setattr("verify_effective_access.signal.getsignal", real_getsignal)
        monkeypatch.setattr("verify_effective_access.signal.signal", remember)
        report, _log, _spec = self._run(monkeypatch, tmp_path)
        assert report.passed
        import signal as signal_module
        for signum in (signal_module.SIGTERM, signal_module.SIGHUP):
            handler = next(handler for installed_signum, handler in installed
                           if installed_signum == signum and callable(handler))
            with pytest.raises(KeyboardInterrupt, match="cleaning up"):
                handler(signum, None)
            assert installed[-2:][0 if signum == signal_module.SIGTERM else 1] == (
                signum, real_getsignal(signum))


# ---------------------------------------------------------------------------
# required_mask_columns: the columns Terraform actually masks
# ---------------------------------------------------------------------------
class TestRequiredMaskColumns:
    @staticmethod
    def _cols(policy_overrides, assignments):
        from verify_effective_access import required_mask_columns
        policy = {"name": "m", "policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "cat",
                  "to_principals": ["analysts"], "match_condition": "hasTagValue('pii', 'ssn')"}
        policy.update(policy_overrides)
        return required_mask_columns([policy], [
            {"entity_type": t, "entity_name": n, "tag_key": k, "tag_value": v} for t, n, k, v in assignments])

    def test_tagged_column_in_the_policy_catalog(self):
        assert self._cols({}, [("columns", "cat.sch.t.ssn", "pii", "ssn")]) == {("cat.sch.t", "ssn")}

    def test_other_catalog_is_not_masked(self):
        assert self._cols({}, [("columns", "cat2.sch.t.ssn", "pii", "ssn")]) == set()

    def test_fully_excepted_targets_mask_nobody(self):
        assert self._cols({"except_principals": ["analysts"]}, [("columns", "cat.sch.t.ssn", "pii", "ssn")]) == set()

    def test_account_users_with_exceptions_still_masks_the_rest(self):
        assert self._cols({"to_principals": ["account users"], "except_principals": ["admins"]},
                          [("columns", "cat.sch.t.ssn", "pii", "ssn")]) == {("cat.sch.t", "ssn")}

    def test_and_needs_every_clause_on_the_same_column(self):
        cond = "hasTagValue('pii', 'ssn') AND hasTagValue('region', 'us')"
        assert self._cols({"match_condition": cond}, [
            ("columns", "cat.sch.t.ssn", "pii", "ssn"),
            ("columns", "cat.sch.t.ssn", "region", "us"),
            ("columns", "cat.sch.t.other", "pii", "ssn"),   # only one clause
            ("columns", "cat.sch.u.x", "region", "us"),
        ]) == {("cat.sch.t", "ssn")}

    def test_or_with_parentheses(self):
        cond = "(hasTagValue('pii', 'ssn') OR hasTagValue('pii', 'tfn')) AND hasTag('region')"
        assert self._cols({"match_condition": cond}, [
            ("columns", "cat.sch.t.ssn", "pii", "ssn"), ("columns", "cat.sch.t.ssn", "region", "us"),
            ("columns", "cat.sch.t.tfn", "pii", "tfn"), ("columns", "cat.sch.t.tfn", "region", "au"),
            ("columns", "cat.sch.t.bare", "pii", "tfn"),
        ]) == {("cat.sch.t", "ssn"), ("cat.sch.t", "tfn")}

    def test_has_tag_matches_any_value(self):
        assert self._cols({"match_condition": "hasTag('pii')"}, [
            ("columns", "cat.sch.t.ssn", "pii", "ssn"), ("columns", "cat.sch.t.name", "other", "x"),
        ]) == {("cat.sch.t", "ssn")}

    def test_when_condition_is_judged_on_the_table_tags(self):
        assignments = [("columns", "cat.sch.t.ssn", "pii", "ssn"), ("columns", "cat.sch.u.ssn", "pii", "ssn"),
                       ("tables", "cat.sch.t", "domain", "hr")]
        assert self._cols({"when_condition": "hasTagValue('domain', 'hr')"}, assignments) == {("cat.sch.t", "ssn")}

    # Unity Catalog identifiers are case-insensitive; tags are not.
    def test_policy_catalog_matches_case_insensitively(self):
        assert self._cols({"catalog": "CAT"}, [("columns", "cat.sch.t.ssn", "pii", "ssn")]) == {("cat.sch.t", "ssn")}
        assert self._cols({}, [("columns", "Cat.Sch.T.SSN", "pii", "ssn")]) == {("cat.sch.t", "ssn")}

    def test_spellings_of_one_column_are_one_entity(self):
        cond = "hasTagValue('pii', 'ssn') AND hasTagValue('region', 'us')"
        assert self._cols({"match_condition": cond}, [
            ("columns", "cat.sch.t.ssn", "pii", "ssn"), ("columns", "CAT.SCH.T.SSN", "region", "us"),
        ]) == {("cat.sch.t", "ssn")}

    def test_when_condition_finds_the_table_case_insensitively(self):
        assignments = [("columns", "cat.sch.t.ssn", "pii", "ssn"), ("tables", "CAT.Sch.T", "domain", "hr")]
        assert self._cols({"when_condition": "hasTagValue('domain', 'hr')"}, assignments) == {("cat.sch.t", "ssn")}

    def test_tag_keys_and_values_stay_case_sensitive(self):
        assert self._cols({}, [("columns", "cat.sch.t.a", "PII", "ssn"), ("columns", "cat.sch.t.b", "pii", "SSN")]) == set()

    @pytest.mark.parametrize("cond", ["has_tag_value('pii', 'ssn')", "has_tag('pii')", "hasTagValue('pii', 'ssn') XOR"])
    def test_an_unreadable_condition_fails_closed(self, cond):
        with pytest.raises(ValueError, match="cannot tell which columns"):
            self._cols({"match_condition": cond}, [("columns", "cat.sch.t.ssn", "pii", "ssn")])

    def test_strict_verify_refuses_an_unreadable_mask_condition(self, tmp_path, capsys):
        tfvars = tmp_path / "abac.auto.tfvars"
        tfvars.write_text("""
fgac_policies = [
  { name = "m", policy_type = "POLICY_TYPE_COLUMN_MASK", to_principals = ["analysts"],
    match_condition = "has_tag_value('pii', 'ssn')" },
]
tag_assignments = [
  { entity_type = "columns", entity_name = "cat.sch.t.ssn", tag_key = "pii", tag_value = "ssn" },
]
""")
        assert main(["--from-tfvars", str(tfvars), "--key-column", "id", "--require-mask-checks"]) == 2
        assert "cannot tell which columns" in capsys.readouterr().err
