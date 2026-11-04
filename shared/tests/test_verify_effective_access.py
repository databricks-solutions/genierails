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
import json
import re
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from verify_effective_access import (  # noqa: E402
    PASS,
    FAIL,
    INCONCLUSIVE,
    DEFAULT_ADMIN_TIER,
    OUT_OF_TIER_PRINCIPAL,
    DUAL_TIER_PRINCIPAL,
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
    weaker_explicit_spec_checks,
    main,
    verify_effective_access_live,
    write_result_file,
)


def test_gate_spec_must_name_every_derived_principal_and_not_weaken_tiers():
    derived = VerificationSpec(column_masks=[ColumnMaskCheck(
        "cat.sch.people", "email", "id", (), (),
        expected_tiers=(("finance", "raw"), ("analyst", "partial"), ("viewer", "full")))])
    omitted = VerificationSpec(column_masks=[ColumnMaskCheck(
        "cat.sch.people", "email", "id", (), (),
        expected_tiers=(("finance", "raw"), ("viewer", "full")))])
    weakened = VerificationSpec(column_masks=[ColumnMaskCheck(
        "cat.sch.people", "email", "id", (), (),
        expected_tiers=(("finance", "raw"), ("analyst", "raw"), ("viewer", "full")))])
    assert weaker_explicit_spec_checks(derived, omitted) == [
        "cat.sch.people.email: analyst expected partial (missing)"]
    assert weaker_explicit_spec_checks(derived, weakened) == [
        "cat.sch.people.email: analyst expected partial, got raw"]


def test_gate_spec_may_add_checks_and_make_expectations_stricter():
    derived = VerificationSpec(column_masks=[ColumnMaskCheck(
        "cat.sch.people", "email", "id", (), (), expected_tiers=(("analyst", "partial"),))])
    explicit = VerificationSpec(column_masks=[
        ColumnMaskCheck("cat.sch.people", "email", "id", (), (),
                        expected_tiers=(("analyst", "full"), ("extra-tier", "full"))),
        ColumnMaskCheck("cat.sch.people", "phone", "id", ("extra-tier",), ()),
    ])
    assert weaker_explicit_spec_checks(derived, explicit) == []
    assert {c.column for c in explicit.column_masks} == {"email", "phone"}


def test_gate_spec_cannot_drop_or_weaken_a_derived_row_filter_principal():
    derived = VerificationSpec(row_filters=[RowFilterCheck(
        "cat.sch.orders", ("regional",), ("global",))])
    explicit = VerificationSpec(row_filters=[RowFilterCheck(
        "cat.sch.orders", (), ("regional", "global"))])
    assert weaker_explicit_spec_checks(derived, explicit) == [
        "cat.sch.orders: regional expected restricted row access (missing or weakened)"]


def _derived_gate_spec():
    tiers = (("finance", "raw"), ("analyst", "partial"),
             ("viewer", "full"), ("__admin__", "raw"))
    return VerificationSpec(
        column_masks=[ColumnMaskCheck(
            "c.s.p", "ssn", "id", (), (), expected_tiers=tiers,
            partial_function="c.s.mask_partial", full_function="c.s.mask_full")],
        principal_memberships={"finance": ("finance",), "analyst": ("analyst",),
                               "viewer": ("viewer",), "__admin__": ("__admin__",)},
    )


def test_gate_rejects_tiered_check_replaced_by_untiered_probe_p1():
    explicit = VerificationSpec(column_masks=[ColumnMaskCheck(
        "c.s.p", "ssn", "id", ("analyst", "viewer"), ("finance", "__admin__"))])
    problems = weaker_explicit_spec_checks(_derived_gate_spec(), explicit)
    assert any("tiered exact-output check replaced by an untiered check" in p for p in problems)


def test_gate_rejects_changed_expected_output_function_probe_p2():
    check = _derived_gate_spec().column_masks[0]
    explicit = VerificationSpec(column_masks=[replace(check, full_function=check.partial_function)])
    assert any("full_function expected c.s.mask_full, got c.s.mask_partial" in p
               for p in weaker_explicit_spec_checks(_derived_gate_spec(), explicit))


def test_gate_rejects_changed_effective_memberships_probe_p3():
    derived = _derived_gate_spec()
    explicit = VerificationSpec(
        column_masks=[replace(derived.column_masks[0], expected_tiers=(
            ("finance", "raw"), ("analyst", "full"),
            ("viewer", "full"), ("__admin__", "raw")))],
        principal_memberships={"analyst": ()},
    )
    assert any("analyst memberships expected ['analyst'], got []" in p
               for p in weaker_explicit_spec_checks(derived, explicit))


def test_gate_rejects_added_moving_principal_probe_p4():
    derived = _derived_gate_spec()
    explicit = VerificationSpec(column_masks=[replace(
        derived.column_masks[0], moving_principals=("analyst",))],
        principal_memberships=derived.principal_memberships)
    assert any("moving_principals may not add ['analyst']" in p
               for p in weaker_explicit_spec_checks(derived, explicit))


def test_minimum_from_tfvars_cli_refuses_a_weaker_explicit_spec(tmp_path):
    tfvars = tmp_path / "abac.auto.tfvars"
    tfvars.write_text('''fgac_policies = [{
      name = "mask", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "cat",
      to_principals = ["analysts"], match_condition = "hasTagValue('gr_treatment', 'redact')",
      function_name = "mask_redact", function_catalog = "cat", function_schema = "sch"
    }]
    tag_assignments = [{ entity_type = "columns", entity_name = "cat.sch.t.ssn",
                         tag_key = "gr_treatment", tag_value = "redact" }]
    ''')
    env_file = tmp_path / "env.auto.tfvars"
    env_file.write_text('governance_mode = "legacy"\n')
    spec_file = tmp_path / "spec.json"
    spec_file.write_text('''{"column_masks": [{"table": "cat.sch.t", "column": "ssn",
      "unmasked_principals": ["__admin__"]}]}''')
    with pytest.raises(SystemExit, match="analysts expected full"):
        main(["--spec", str(spec_file), "--minimum-from-tfvars", str(tfvars),
              "--minimum-env-file", str(env_file), "--print-spec"])


@pytest.mark.parametrize("partial,full", [
    ("c.s.identity", "c.s.identity"),
    ("c.s.mask_ssn", "c.s.identity"),
])
def test_gate_rejects_legacy_check_tiered_with_non_policy_functions_p8(tmp_path, partial, full):
    derived = VerificationSpec(column_masks=[ColumnMaskCheck(
        "c.s.p", "ssn", "id", ("analyst",), ("finance",), mask_function="c.s.mask_ssn")])
    explicit = VerificationSpec(column_masks=[ColumnMaskCheck(
        "c.s.p", "ssn", "id", (), (), expected_tiers=(("analyst", "full"), ("finance", "raw")),
        partial_function=partial, full_function=full)])
    assert any("untiered check may become tiered" in p
               for p in weaker_explicit_spec_checks(derived, explicit))


def test_gate_allows_legacy_check_tiered_with_the_policy_function_p8b():
    derived = VerificationSpec(column_masks=[ColumnMaskCheck(
        "c.s.p", "ssn", "id", ("analyst",), ("finance",), mask_function="c.s.mask_ssn")])
    explicit = VerificationSpec(column_masks=[ColumnMaskCheck(
        "c.s.p", "ssn", "id", (), (), expected_tiers=(("analyst", "full"), ("finance", "raw")),
        partial_function="C.S.MASK_SSN", full_function="c.s.mask_ssn")])
    assert weaker_explicit_spec_checks(derived, explicit) == []


@pytest.mark.parametrize("payload,field", [
    ({"bogus_top_level": 1}, "root.bogus_top_level"),
    ({"column_masks": [{"table": "c.s.p", "column": "ssn", "skip": True}]},
     "column_masks[0].skip"),
    ({"column_masks": [{"table": "c.s.p", "column": "ssn", "expected_tier": {}}]},
     "column_masks[0].expected_tier"),
])
def test_gate_spec_strict_loader_rejects_unknown_fields_p9(tmp_path, payload, field):
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match=re.escape(field)):
        load_spec_from_file(path, strict=True)


def test_gate_spec_strict_loader_rejects_unknown_tier_on_added_principal_p10(tmp_path):
    path = tmp_path / "spec.json"
    path.write_text(json.dumps({"column_masks": [{
        "table": "c.s.p", "column": "ssn", "expected_tiers": {"extra": "bogus"}}]}))
    with pytest.raises(ValueError, match="unknown tier value.*extra.*bogus"):
        load_spec_from_file(path, strict=True)


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


def _email_partial_tfvars(tmp_path, overrides):
    tfvars = tmp_path / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    tfvars.write_text('''fgac_policies = [{ name = "full", policy_type = "POLICY_TYPE_COLUMN_MASK",
      to_principals = ["account users"], except_principals = ["raw", "analyst"],
      match_condition = "hasTagValue('gr_treatment', 'email_partial')",
      function_catalog = "cat", function_schema = "gov", function_name = "full_email" }]
tag_assignments = [{ entity_type = "columns", entity_name = "cat.sch.people.email",
  tag_key = "gr_treatment", tag_value = "email_partial" }]
''')
    env.write_text('governance_mode = "deterministic"\n'
                   'access_tier_groups = ["raw", "analyst", "viewer"]\n'
                   f"column_overrides = {overrides}\n")
    return tfvars, env


def test_deterministic_spec_matches_mixed_case_column_override_key(tmp_path):
    """A hand-written mixed-case key is the same UC column; generate honours it."""
    tfvars, env = _email_partial_tfvars(
        tmp_path, '{ "CAT.sch.People.EMAIL" = { partial = "raw" } }')
    check = load_spec_from_tfvars(tfvars, env_file=env, key_column="id").column_masks[0]
    assert dict(check.expected_tiers)["analyst"] == "raw"


def test_deterministic_spec_refuses_case_only_duplicate_override_keys(tmp_path):
    tfvars, env = _email_partial_tfvars(
        tmp_path, '{ "cat.sch.people.email" = { partial = "raw" }, '
                  '"CAT.sch.People.EMAIL" = { partial = "masked" } }')
    with pytest.raises(ValueError, match="CAT.sch.People.EMAIL and cat.sch.people.email"):
        load_spec_from_tfvars(tfvars, env_file=env, key_column="id")


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


DEPLOYER_APP_ID = "c2309647-459e-4bad-9883-1b932eaf1b17"


def _deployer_excepted_tfvars(tmp_path, mode):
    """Policies as build_policies writes them when deployer_principal_id is set."""
    tfvars = tmp_path / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    tfvars.write_text(f'''fgac_policies = [
  {{ name = "partial", policy_type = "POLICY_TYPE_COLUMN_MASK", to_principals = ["analyst"],
    except_principals = ["raw", "{DEPLOYER_APP_ID}"],
    match_condition = "hasTagValue('gr_treatment', 'email_partial')",
    function_catalog = "cat", function_schema = "gov", function_name = "partial_email" }},
  {{ name = "full", policy_type = "POLICY_TYPE_COLUMN_MASK", to_principals = ["account users"],
    except_principals = ["raw", "analyst", "{DEPLOYER_APP_ID}"],
    match_condition = "hasTagValue('gr_treatment', 'email_partial')",
    function_catalog = "cat", function_schema = "gov", function_name = "full_email" }},
  {{ name = "secret", policy_type = "POLICY_TYPE_COLUMN_MASK", to_principals = ["account users"],
    except_principals = [], match_condition = "hasTagValue('gr_treatment', 'secret')",
    function_catalog = "cat", function_schema = "gov", function_name = "redact" }},
]
tag_assignments = [
  {{ entity_type = "columns", entity_name = "cat.sch.people.email", tag_key = "gr_treatment", tag_value = "email_partial" }},
  {{ entity_type = "columns", entity_name = "cat.sch.people.api_key", tag_key = "gr_treatment", tag_value = "secret" }},
]
''')
    env.write_text(f'''{mode}access_tier_groups = ["raw", "analyst", "viewer"]
deployer_principal_id = "{DEPLOYER_APP_ID}"
''')
    return tfvars, env


def test_deterministic_spec_checks_deployer_as_admin_baseline_not_tier_group(tmp_path):
    tfvars, env = _deployer_excepted_tfvars(tmp_path, 'governance_mode = "deterministic"\n')
    spec = load_spec_from_tfvars(tfvars, env_file=env, key_column="id")
    assert DEPLOYER_APP_ID not in spec.principals
    assert DEPLOYER_APP_ID not in spec.principal_memberships
    assert spec.principals == {"raw", "analyst", "viewer", "__out_of_tier__", "__dual_tier__"}
    by_column = {c.column: c for c in spec.column_masks}
    for check in by_column.values():
        assert DEPLOYER_APP_ID not in check.masked_principals + check.unmasked_principals
        assert DEPLOYER_APP_ID not in dict(check.expected_tiers)
        assert DEFAULT_ADMIN_TIER not in check.masked_principals
    assert dict(by_column["email"].expected_tiers)[DEFAULT_ADMIN_TIER] == "raw"
    assert dict(by_column["api_key"].expected_tiers)[DEFAULT_ADMIN_TIER] == "full"
    # The policy fingerprint inputs are the deployed policies, unchanged.
    excepts = [p["except_principals"] for p in spec.mask_config["fgac_policies"]]
    assert sum(DEPLOYER_APP_ID in e for e in excepts) == 2


def test_deterministic_spec_refuses_deployer_principal_id_equal_to_a_tier_group(tmp_path):
    tfvars, env = _deployer_excepted_tfvars(tmp_path, 'governance_mode = "deterministic"\n')
    tfvars.write_text(tfvars.read_text().replace('to_principals = ["analyst"]',
                                                 f'to_principals = ["{DEPLOYER_APP_ID}"]'))
    env.write_text(env.read_text().replace('"analyst"', f'"{DEPLOYER_APP_ID}"'))
    with pytest.raises(ValueError, match="deployer_principal_id .* is also a configured group"):
        load_spec_from_tfvars(tfvars, env_file=env, key_column="id")


def test_legacy_generation_spec_aliases_deployer_without_deterministic_expectations(tmp_path):
    tfvars, env = _deployer_excepted_tfvars(tmp_path, "")
    spec = load_spec_from_tfvars(tfvars, env_file=env, key_column="id")
    assert DEPLOYER_APP_ID not in spec.principals
    assert all(DEPLOYER_APP_ID not in c.masked_principals + c.unmasked_principals
               for c in spec.column_masks)
    assert all(not c.expected_tiers for c in spec.column_masks)


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

    @pytest.mark.parametrize("placeholder", ["unknown", "UNKNOWN", "UnKnOwN"])
    def test_deleted_principal_placeholder_is_never_a_tier(self, placeholder):
        fgac = [{
            "name": "mask_cvv", "policy_type": "POLICY_TYPE_COLUMN_MASK",
            "to_principals": ["account users"],
            "except_principals": [placeholder],
            "match_condition": "hasTagValue('pci_level', 'redacted_cvv')",
        }]
        tags = [{"entity_type": "columns",
                 "entity_name": "fin.finance.credit_cards.cvv",
                 "tag_key": "pci_level", "tag_value": "redacted_cvv"}]

        spec = derive_spec_from_config(
            fgac, tags, ["raw", "analyst", "viewer", placeholder],
        )

        check = spec.column_masks[0]
        assert set(check.masked_principals) == {"raw", "analyst", "viewer"}
        assert check.unmasked_principals == ()
        assert all(principal.lower() != "unknown" for principal in spec.principals)

    @pytest.mark.parametrize("placeholder", ["unknown", "UNKNOWN"])
    def test_verified_column_refuses_unknown_to_principal(self, placeholder):
        fgac = [{
            "name": "mask_email", "policy_type": "POLICY_TYPE_COLUMN_MASK",
            "to_principals": [placeholder],
            "match_condition": "hasTagValue('pii_level', 'masked_email')",
        }]
        tags = [{"entity_type": "columns", "entity_name": "fin.crm.people.email",
                 "tag_key": "pii_level", "tag_value": "masked_email"}]

        with pytest.raises(ValueError, match="mask_email.*verified column.*deleted-principal placeholder"):
            derive_spec_from_config(fgac, tags, ["raw", "viewer"])

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

    def test_real_preflight_method_reraises_nontransient_query_error(self, monkeypatch):
        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        verifier = EffectiveAccessVerifier(
            {"host": "h", "client_id": "c", "client_secret": "s"},
            warehouse_id="warehouse-123",
        )
        principal = VerificationPrincipal("viewer", "test", "app", "secret")
        monkeypatch.setattr(verifier, "_ws_for", lambda _principal: object())
        monkeypatch.setattr(
            verifier, "run_query",
            lambda _ws, _sql: (_ for _ in ()).throw(RuntimeError("invalid credentials")),
        )
        with pytest.raises(
            RuntimeError, match="^test principal not query-capable: invalid credentials$",
        ):
            verifier.assert_query_capable(principal)

    def test_real_preflight_retries_transient_entitlement_propagation(self, monkeypatch):
        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        verifier = EffectiveAccessVerifier(
            {"host": "h", "client_id": "c", "client_secret": "s"},
            warehouse_id="warehouse-123",
        )
        attempts = []
        monkeypatch.setattr(verifier, "_ws_for", lambda _principal: object())

        def query(_ws, sql):
            attempts.append(sql)
            if len(attempts) < 3:
                raise RuntimeError(
                    "This API is disabled for users without the databricks-sql-access "
                    "or workspace-consume entitlements")
            return [[1]]

        monkeypatch.setattr(verifier, "run_query", query)
        monkeypatch.setattr("verify_effective_access.time.sleep", lambda _seconds: None)
        verifier.assert_query_capable(
            VerificationPrincipal("viewer", "test", "app", "secret"))
        assert attempts == ["SELECT 1", "SELECT 1", "SELECT 1"]

    def test_real_preflight_fails_when_transient_retry_cap_is_exhausted(self, monkeypatch):
        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        verifier = EffectiveAccessVerifier(
            {"host": "h", "client_id": "c", "client_secret": "s"},
            warehouse_id="warehouse-123",
        )
        clock = iter((0.0, 121.0))
        monkeypatch.setattr(verifier, "_ws_for", lambda _principal: object())
        monkeypatch.setattr("verify_effective_access.time.monotonic", lambda: next(clock))
        monkeypatch.setattr(
            verifier, "run_query",
            lambda _ws, _sql: (_ for _ in ()).throw(
                RuntimeError("user does not have permission to use warehouse")),
        )
        with pytest.raises(RuntimeError, match="^test principal not query-capable:"):
            verifier.assert_query_capable(
                VerificationPrincipal("viewer", "test", "app", "secret"))

    @staticmethod
    def _preflight_verifier(monkeypatch):
        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        verifier = EffectiveAccessVerifier(
            {"host": "h", "client_id": "c", "client_secret": "s"},
            warehouse_id="warehouse-123",
        )
        monkeypatch.setattr(verifier, "_ws_for", lambda _principal: object())
        return verifier

    class _Clock:
        """Fake monotonic clock that only advances when the code sleeps."""

        def __init__(self, monkeypatch):
            self.now, self.sleeps = 0.0, []
            monkeypatch.setattr("verify_effective_access.time.monotonic", lambda: self.now)
            monkeypatch.setattr("verify_effective_access.time.sleep", self.sleep)

        def sleep(self, seconds):
            self.sleeps.append(seconds)
            self.now += seconds

    def test_nontransient_preflight_error_is_not_retried(self, monkeypatch):
        verifier = self._preflight_verifier(monkeypatch)
        clock = self._Clock(monkeypatch)
        calls = []

        def query(_ws, sql):
            calls.append(sql)
            raise RuntimeError("invalid credentials")

        monkeypatch.setattr(verifier, "run_query", query)
        with pytest.raises(
            RuntimeError, match="^test principal not query-capable: invalid credentials$",
        ):
            verifier.assert_query_capable(
                VerificationPrincipal("viewer", "test", "app", "secret"))
        assert calls == ["SELECT 1"]
        assert clock.sleeps == []

    @pytest.mark.parametrize("message", [
        "This API is disabled for users without the databricks-sql-access "
        "or workspace-consume entitlements",
        "User does not have permission to use warehouse warehouse-123",
        "Principal does not have CAN USE permission on warehouse",
        "Principal does not have CAN_USE permission on warehouse",
    ])
    def test_each_transient_marker_is_retried_with_backoff(self, monkeypatch, message):
        import verify_effective_access as vea

        verifier = self._preflight_verifier(monkeypatch)
        clock = self._Clock(monkeypatch)
        calls = []

        def query(_ws, sql):
            calls.append(sql)
            if len(calls) < 3:
                raise RuntimeError(message)
            return [[1]]

        monkeypatch.setattr(verifier, "run_query", query)
        verifier.assert_query_capable(VerificationPrincipal("viewer", "test", "app", "secret"))
        assert calls == ["SELECT 1"] * 3
        assert clock.sleeps == [vea.QUERY_PREFLIGHT_BACKOFF_SECONDS] * 2

    def test_transient_preflight_retry_is_bounded_by_deadline(self, monkeypatch):
        import verify_effective_access as vea

        verifier = self._preflight_verifier(monkeypatch)
        clock = self._Clock(monkeypatch)
        calls = []

        def query(_ws, sql):
            calls.append(sql)
            if len(calls) > 1000:
                pytest.fail("pre-flight retry is unbounded")
            raise RuntimeError("User does not have permission to use warehouse warehouse-123")

        monkeypatch.setattr(verifier, "run_query", query)
        with pytest.raises(
            RuntimeError, match="^test principal not query-capable: .*permission to use warehouse",
        ):
            verifier.assert_query_capable(
                VerificationPrincipal("viewer", "test", "app", "secret"))
        timeout = vea.QUERY_PREFLIGHT_TIMEOUT_SECONDS
        backoff = vea.QUERY_PREFLIGHT_BACKOFF_SECONDS
        assert len(calls) == int(timeout / backoff) + 1  # attempts at t=0, 2, ..., timeout
        assert clock.now == timeout

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

    @pytest.mark.parametrize("tier", [OUT_OF_TIER_PRINCIPAL, DUAL_TIER_PRINCIPAL])
    def test_grants_query_entitlement_to_synthetic_principal(self, monkeypatch, tier):
        from unittest.mock import Mock

        service_principals = SimpleNamespace(patch=Mock())
        verifier = self._verifier(monkeypatch, object())
        verifier._admin_ws.service_principals = service_principals
        principal = VerificationPrincipal(
            tier, f"test-{tier}", f"app-{tier}", "secret",
            f"account-id-{tier}", f"workspace-id-{tier}")

        verifier.grant_query_entitlement(principal)

        call = service_principals.patch.call_args
        assert call.args[0] == f"workspace-id-{tier}"
        operation = call.kwargs["operations"][0]
        assert operation.path == "entitlements"
        assert operation.value == [{"value": "workspace-consume"}]

    @pytest.mark.parametrize("tier", [OUT_OF_TIER_PRINCIPAL, DUAL_TIER_PRINCIPAL])
    def test_deprovisions_synthetic_principal(self, monkeypatch, tier):
        from unittest.mock import Mock

        verifier = self._verifier(monkeypatch, object())
        delete = Mock()
        verifier._account = SimpleNamespace(
            service_principals=SimpleNamespace(delete=delete))

        verifier.deprovision_principal(VerificationPrincipal(
            tier, f"test-{tier}", f"app-{tier}", "secret", f"id-{tier}"))

        delete.assert_called_once_with(f"id-{tier}")

    def test_outsider_table_access_is_exact_and_reversible(self, monkeypatch):
        verifier = self._verifier(monkeypatch, object())
        statements = []
        monkeypatch.setattr(verifier, "run_query", lambda _ws, sql: statements.append(sql))
        principal = VerificationPrincipal(
            "__out_of_tier__", "test-outsider", "01234567-89ab-cdef", "secret", "456")

        verifier.grant_temporary_table_access(
            principal, ["cat.sales.customers", "cat.sales.customers"])
        verifier.grant_temporary_table_access(
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
            verifier.grant_temporary_table_access(
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
                get=lambda _id: SimpleNamespace(groups=[
                    SimpleNamespace(value="789", display=membership, type="direct"),
                    SimpleNamespace(value="555", display="parent", type="indirect"),
                ]),
            ),
            service_principal_secrets=SimpleNamespace(
                create=lambda **_: SimpleNamespace(secret="secret"),
            ),
            groups=SimpleNamespace(
                list=list_groups, patch=Mock(),
            ),
            workspace_assignment=SimpleNamespace(update=Mock()),
        )
        workspace_sp = SimpleNamespace(id="workspace-456", application_id="app-123")
        workspace = SimpleNamespace(
            service_principals=SimpleNamespace(list=lambda **_: [workspace_sp]),
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
        assert principal.workspace_sp_id == "workspace-456"
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

    @staticmethod
    def _provisioning_account(monkeypatch, *, existing=(), visible=True, fail=None):
        """Fake account/workspace clients; ``fail`` names the step that raises."""
        from unittest.mock import Mock

        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        monkeypatch.setenv("GENIERAILS_VERIFY_WORKSPACE_SYNC_TIMEOUT", "0")
        monkeypatch.setattr("verify_effective_access.time.sleep", lambda _seconds: None)
        verifier = EffectiveAccessVerifier({
            "host": "h", "client_id": "c", "client_secret": "s",
            "account_host": "a", "account_id": "1", "workspace_id": "123",
        })
        created = SimpleNamespace(
            id="456", application_id="app-new", display_name="genierails-verify-viewers")

        def step(name, result=None):
            def call(*_args, **_kwargs):
                if fail == name:
                    raise RuntimeError(f"{name} failed")
                return result
            return Mock(side_effect=call)

        verifier._account = SimpleNamespace(
            service_principals=SimpleNamespace(
                list=lambda **_: list(existing),
                create=Mock(return_value=created),
                get=Mock(side_effect=lambda sp_id: next(
                    sp for sp in existing if sp.id == sp_id)),
                delete=Mock(),
            ),
            service_principal_secrets=SimpleNamespace(
                create=step("secret", SimpleNamespace(secret="secret"))),
            groups=SimpleNamespace(patch=step("group-patch")),
            workspace_assignment=SimpleNamespace(update=step("assignment")),
        )
        workspace_sp = SimpleNamespace(id="workspace-456", application_id="app-new")
        verifier._admin_ws = SimpleNamespace(service_principals=SimpleNamespace(
            list=lambda **_: [workspace_sp] if visible else []))
        return verifier

    @pytest.mark.parametrize(("fail", "visible", "error"), [
        ("secret", True, RuntimeError("secret failed")),
        ("group-patch", True, RuntimeError("group-patch failed")),
        ("assignment", True, RuntimeError("assignment failed")),
        (None, False, TimeoutError("did not become visible before timeout")),
    ])
    def test_provision_deletes_created_principal_when_setup_fails(
        self, monkeypatch, fail, visible, error,
    ):
        verifier = self._provisioning_account(monkeypatch, visible=visible, fail=fail)
        group = SimpleNamespace(id="789", display_name="viewers", members=[])

        with pytest.raises(type(error), match=str(error.args[0])):
            verifier.provision_principal("viewers", resolved_groups=[group])

        sps = verifier._account.service_principals
        sps.create.assert_called_once()
        sps.delete.assert_called_once_with("456")

    def test_provision_failure_surfaces_original_error_when_cleanup_fails(
        self, monkeypatch, capsys,
    ):
        verifier = self._provisioning_account(monkeypatch, fail="group-patch")
        verifier._account.service_principals.delete.side_effect = RuntimeError("delete denied")
        group = SimpleNamespace(id="789", display_name="viewers", members=[])

        with pytest.raises(RuntimeError, match="^group-patch failed$"):
            verifier.provision_principal("viewers", resolved_groups=[group])

        err = capsys.readouterr().err
        assert "could not delete partially provisioned genierails-verify-viewers" in err
        assert "app-new" in err and "delete denied" in err

    def test_provision_failure_surfaces_original_error_when_cleanup_interrupted(
        self, monkeypatch, capsys,
    ):
        verifier = self._provisioning_account(monkeypatch, fail="group-patch")
        verifier._account.service_principals.delete.side_effect = KeyboardInterrupt(
            "delete interrupted")
        group = SimpleNamespace(id="789", display_name="viewers", members=[])

        with pytest.raises(RuntimeError, match="^group-patch failed$"):
            verifier.provision_principal("viewers", resolved_groups=[group])

        err = capsys.readouterr().err
        assert "could not delete partially provisioned genierails-verify-viewers" in err
        assert "delete interrupted" in err

    def test_provision_does_not_delete_a_principal_it_did_not_create(self, monkeypatch):
        existing = SimpleNamespace(
            id="321", application_id="app-old", display_name="genierails-verify-viewers",
            groups=[])
        verifier = self._provisioning_account(
            monkeypatch, existing=[existing], fail="group-patch")
        group = SimpleNamespace(id="789", display_name="viewers", members=[])

        with pytest.raises(RuntimeError, match="group-patch failed"):
            verifier.provision_principal("viewers", resolved_groups=[group])

        sps = verifier._account.service_principals
        sps.create.assert_not_called()
        sps.delete.assert_not_called()

    def test_provision_refuses_reused_principal_with_stale_memberships(self, monkeypatch):
        existing = SimpleNamespace(
            id="321", application_id="app-old", display_name="genierails-verify-viewers",
            groups=[
                SimpleNamespace(value="789", display="viewers", type="direct"),
                SimpleNamespace(value="999", display="admins", type="direct"),
                SimpleNamespace(value="555", display="all-staff", type="indirect"),
            ])
        verifier = self._provisioning_account(monkeypatch, existing=[existing])
        group = SimpleNamespace(id="789", display_name="viewers", members=[])

        with pytest.raises(RuntimeError, match=(
            r"'genierails-verify-viewers' already exists and is a member of account "
            r"group\(s\) outside its tier: admins\. Delete the service principal")):
            verifier.provision_principal("viewers", resolved_groups=[group])

        account = verifier._account
        account.service_principals.get.assert_called_once_with("321")
        account.service_principal_secrets.create.assert_not_called()
        account.groups.patch.assert_not_called()
        account.workspace_assignment.update.assert_not_called()
        account.service_principals.delete.assert_not_called()

    def test_provision_reuses_principal_whose_memberships_match(self, monkeypatch):
        existing = SimpleNamespace(
            id="321", application_id="app-old", display_name="genierails-verify-viewers",
            groups=[SimpleNamespace(value="789", display="viewers", type="direct")])
        verifier = self._provisioning_account(monkeypatch, existing=[existing])
        verifier._admin_ws.service_principals.list = lambda **_: [
            SimpleNamespace(id="workspace-321", application_id="app-old")]
        group = SimpleNamespace(
            id="789", display_name="viewers", members=[SimpleNamespace(value="321")])

        principal = verifier.provision_principal("viewers", resolved_groups=[group])

        assert (principal.sp_id, principal.workspace_sp_id) == ("321", "workspace-321")
        verifier._account.service_principals.create.assert_not_called()
        verifier._account.groups.patch.assert_not_called()

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

            def grant_query_entitlement(self, principal):
                pass

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
    def _run(monkeypatch, tmp_path, *, revoke_error=None, keep=False, sample_error=None, log=None,
             pre_grant=False, cleanup_error=None):
        log = [] if log is None else log
        granted = set()
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

            def grant_query_entitlement(self, principal):
                log.append(("entitlement", principal.tier))

            def assert_query_capable(self, principal):
                log.append(("preflight", principal.tier))

            def grant_temporary_table_access(self, principal, tables, *, revoke=False):
                # A revoke before the principal's first grant is the cleanup of earlier runs' leftovers.
                kind = "grant" if not revoke else "revoke" if principal.tier in granted else "cleanup"
                log.append((kind, principal.tier, tuple(tables)))
                granted.add(principal.tier) if kind == "grant" else None
                if kind == "revoke" and revoke_error:
                    raise revoke_error
                if kind == "cleanup" and cleanup_error:
                    raise cleanup_error

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
        monkeypatch.setattr(
            "verify_effective_access.time.sleep",
            lambda seconds: log.append(("propagation-wait", seconds)),
        )
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
            spec, tmp_path / "auth.auto.tfvars", keep_principals=keep,
            **({"pre_grant": True} if pre_grant else {}))
        return report, log, spec

    TIERS = ("raw_group", "analyst", "viewer", OUT_OF_TIER_PRINCIPAL)

    def test_without_pre_grant_only_the_outsider_gets_temporary_access(self, monkeypatch, tmp_path):
        report, log, _spec = self._run(monkeypatch, tmp_path)
        assert report.passed
        assert [entry for entry in log if entry[0] in ("grant", "revoke")] == [
            ("grant", OUT_OF_TIER_PRINCIPAL, ("cat.sch.people",)),
            ("revoke", OUT_OF_TIER_PRINCIPAL, ("cat.sch.people",))]

    @pytest.mark.parametrize("keep", [False, True])
    def test_pre_grant_gives_every_test_principal_access_before_it_queries_and_revokes_it(
        self, monkeypatch, tmp_path, keep,
    ):
        report, log, _spec = self._run(monkeypatch, tmp_path, keep=keep, pre_grant=True)
        assert report.passed
        for tier in self.TIERS:
            grant = ("grant", tier, ("cat.sch.people",))
            revoke = ("revoke", tier, ("cat.sch.people",))
            assert log.count(grant) == log.count(revoke) == 1, tier
            assert log.index(("provision", tier)) < log.index(grant) < log.index(("preflight", tier))
            assert log.index(grant) < log.index(revoke)
            # Revoked before any principal is deleted (or kept).
            assert all(log.index(revoke) < i for i, entry in enumerate(log) if entry[0] == "deprovision")
        # Only the test principals themselves: never the admin baseline.
        assert {entry[1] for entry in log if entry[0] in ("grant", "revoke")} == set(self.TIERS)

    @pytest.mark.parametrize("pre_grant", [False, True])
    def test_every_run_first_revokes_what_a_killed_run_left_on_each_principal(self, monkeypatch, tmp_path, pre_grant):
        report, log, _spec = self._run(monkeypatch, tmp_path, pre_grant=pre_grant)
        assert report.passed
        first_grant = min(i for i, entry in enumerate(log) if entry[0] == "grant")
        first_query = min(log.index(("preflight", tier)) for tier in self.TIERS)
        for tier in self.TIERS:
            cleanup = ("cleanup", tier, ("cat.sch.people",))
            assert log.count(cleanup) == 1, tier
            assert log.index(("provision", tier)) < log.index(cleanup) < first_grant < first_query
        assert not [entry for entry in log if entry[0] == "cleanup" and entry[1] == DEFAULT_ADMIN_TIER]

    @pytest.mark.parametrize("pre_grant", [False, True])
    def test_a_leftover_grant_that_can_t_be_revoked_refuses_the_run(self, monkeypatch, tmp_path, pre_grant):
        log = []
        with pytest.raises(RuntimeError, match=r"refusing to verify: test principal app-\S+ .* earlier run"):
            self._run(monkeypatch, tmp_path, cleanup_error=RuntimeError("revoke denied"), log=log,
                      pre_grant=pre_grant)
        # Nothing was granted or queried, and the principals were still deleted.
        assert not [entry for entry in log if entry[0] in ("grant", "preflight")]
        assert {entry[1] for entry in log if entry[0] == "deprovision"} == set(self.TIERS)

    def test_pre_grant_access_is_revoked_after_an_error_and_an_interrupt(self, monkeypatch, tmp_path):
        error_log = []
        report, _unused, _spec = self._run(
            monkeypatch, tmp_path, sample_error=RuntimeError("query failed"), log=error_log, pre_grant=True)
        assert not report.passed
        interrupt_log = []
        with pytest.raises(KeyboardInterrupt):
            self._run(monkeypatch, tmp_path, sample_error=KeyboardInterrupt("cancelled"), log=interrupt_log,
                      pre_grant=True)
        for log in (error_log, interrupt_log):
            for tier in self.TIERS:
                assert ("revoke", tier, ("cat.sch.people",)) in log, tier

    def test_a_failed_pre_grant_revoke_fails_the_run_and_names_each_principal(self, monkeypatch, tmp_path):
        report, log, _spec = self._run(
            monkeypatch, tmp_path, keep=True, revoke_error=RuntimeError("revoke denied"), pre_grant=True)
        assert not report.passed
        # One failed revoke doesn't stop the others.
        assert {entry[1] for entry in log if entry[0] == "revoke"} == set(self.TIERS)
        failed = {result.target for result in report.results if result.kind == "cleanup" and result.status == FAIL}
        assert failed == {f"app-{tier}" for tier in self.TIERS}
        detail = next(r.detail for r in report.results if r.kind == "cleanup" and r.target == "app-analyst")
        assert "temporary test-principal (analyst) access was not fully revoked" in detail
        assert "REVOKE SELECT ON TABLE `cat`.`sch`.`people` FROM `app-analyst`" in detail

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
        for tier in ("raw_group", "analyst", "viewer", OUT_OF_TIER_PRINCIPAL):
            assert ("entitlement", tier) in log
            assert ("preflight", tier) in log
            assert log.index(("propagation-wait", 0)) < log.index(("preflight", tier))

    def test_preflight_query_failure_is_clear_and_cleans_up(self, monkeypatch, tmp_path):
        deleted = []

        class FakeVerifier:
            def __init__(self, auth, warehouse_id=""):
                self.mask_config = None

            def resolve_warehouse(self):
                return "warehouse"

            def resolve_principal_groups(self, memberships):
                return list(memberships)

            def provision_principal(self, tier, memberships=None, *, resolved_groups=None):
                return VerificationPrincipal(tier, f"test-{tier}", f"app-{tier}", "secret", tier)

            def grant_query_entitlement(self, principal):
                pass

            def grant_warehouse_use(self, principal):
                pass

            def assert_query_capable(self, principal):
                raise RuntimeError("test principal not query-capable: entitlement disabled")

            def grant_temporary_table_access(self, principal, tables, *, revoke=False):
                pass

            def deprovision_principal(self, principal):
                deleted.append(principal.tier)

        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        monkeypatch.setenv("GENIERAILS_VERIFY_PROPAGATION_SLEEP", "0")
        monkeypatch.setattr("verify_effective_access.load_auth", lambda path: {
            "client_id": "admin", "client_secret": "secret"})
        monkeypatch.setattr("verify_effective_access.EffectiveAccessVerifier", FakeVerifier)
        spec = VerificationSpec(row_filters=[RowFilterCheck(
            "cat.sch.people", (OUT_OF_TIER_PRINCIPAL,), ())],
            principal_memberships={OUT_OF_TIER_PRINCIPAL: ()})

        with pytest.raises(RuntimeError, match="test principal not query-capable: entitlement disabled"):
            verify_effective_access_live(spec, tmp_path / "auth.auto.tfvars")
        assert deleted == [OUT_OF_TIER_PRINCIPAL]

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


class TestResultChecks:
    """The result file reports, per check and principal, the access each run proved."""

    def _spec(self):
        tiered = ColumnMaskCheck(
            table="cat.sch.people", column="ssn", key_column="id",
            masked_principals=("analyst",), unmasked_principals=(DEFAULT_ADMIN_TIER,),
            policy_name="mask_ssn", expected_tiers=(("analyst", "partial"), (DEFAULT_ADMIN_TIER, "raw")),
            partial_function="cat.gov.partial", full_function="cat.gov.full")
        legacy = ColumnMaskCheck(
            table="cat.sch.people", column="email", key_column="id",
            masked_principals=("analyst",), unmasked_principals=("raw_group",), policy_name="mask_email")
        rows = RowFilterCheck(table="cat.sch.people", restricted_principals=("analyst",),
                              unrestricted_principals=("raw_group",), policy_name="rf")
        return VerificationSpec(column_masks=[tiered, legacy], row_filters=[rows]), tiered, legacy, rows

    def _write(self, tmp_path, report, spec):
        path = tmp_path / "result.json"
        write_result_file(path, report, spec)
        return __import__("json").loads(path.read_text())

    def test_a_pass_reports_each_principal_at_its_expected_level(self, tmp_path):
        spec, tiered, legacy, rows = self._spec()
        report = EffectiveAccessReport()
        for check in (tiered, legacy, rows):
            report.add(CheckResult(check.describe().split()[0], check.describe(), PASS, "ok"))
        payload = self._write(tmp_path, report, spec)
        assert payload["passed"] is True
        by_column = {(c["kind"], c["column"]): c for c in payload["checks"]}
        assert by_column[("column-mask", "ssn")]["principals"] == {
            "analyst": {"expected": "partial", "observed": "partial"},
            DEFAULT_ADMIN_TIER: {"expected": "raw", "observed": "raw"}}
        assert by_column[("column-mask", "ssn")]["full_function"] == "cat.gov.full"
        assert by_column[("column-mask", "email")]["principals"]["analyst"]["observed"] == "masked"
        assert by_column[("row-filter", "")]["principals"]["analyst"]["observed"] == "restricted"

    def test_a_leak_reports_raw_and_does_not_change_the_verdict(self, tmp_path):
        spec, tiered, legacy, rows = self._spec()
        report = EffectiveAccessReport()
        report.add(CheckResult("column-mask", tiered.describe(), FAIL, "leak",
                               {"raw_leaks_by_principal": {"analyst": 3}}))
        payload = self._write(tmp_path, report, spec)
        assert payload["passed"] is False
        ssn = next(c for c in payload["checks"] if c["column"] == "ssn")
        assert ssn["status"] == FAIL
        assert ssn["principals"]["analyst"] == {"expected": "partial", "observed": "raw"}
        assert ssn["principals"][DEFAULT_ADMIN_TIER]["observed"] == "unproven"
        not_run = [c for c in payload["checks"] if c["column"] != "ssn"]
        assert {c["status"] for c in not_run} == {"NOT VERIFIED"}
        assert all(p["observed"] == "unproven" for c in not_run for p in c["principals"].values())

    def test_mismatch_and_inconclusive_are_never_reported_as_the_expected_level(self, tmp_path):
        spec, tiered, _legacy, _rows = self._spec()
        report = EffectiveAccessReport()
        report.add(CheckResult("column-mask", tiered.describe(), INCONCLUSIVE, "no rows",
                               {"mismatches_by_principal": {"analyst": 1}}))
        ssn = next(c for c in self._write(tmp_path, report, spec)["checks"] if c["column"] == "ssn")
        assert ssn["principals"]["analyst"]["observed"] == "mismatch"
        assert ssn["principals"][DEFAULT_ADMIN_TIER]["observed"] == "unproven"


# ---------------------------------------------------------------------------
# temporary_table_access: who gets temporary USE/SELECT on which checked tables
# ---------------------------------------------------------------------------
class TestTemporaryTableAccess:
    @staticmethod
    def _spec():
        from verify_effective_access import DEFAULT_ADMIN_TIER, OUT_OF_TIER_PRINCIPAL
        people = ColumnMaskCheck(
            table="cat.a.people", column="email", key_column="id", masked_principals=(), unmasked_principals=(),
            expected_tiers=(("raw_group", "raw"), ("analyst", "partial"), (OUT_OF_TIER_PRINCIPAL, "full"),
                            (DEFAULT_ADMIN_TIER, "raw")))
        orders = ColumnMaskCheck(
            table="other.b.orders", column="card", key_column="id",
            masked_principals=("viewer",), unmasked_principals=("raw_group",))
        return VerificationSpec(column_masks=[people, orders],
                                row_filters=[RowFilterCheck("cat.a.visits", ("viewer",), ("raw_group",))])

    @staticmethod
    def _principals(*tiers):
        return {tier: VerificationPrincipal(tier, f"test-{tier}", f"app-{tier}", "s", tier) for tier in tiers}

    def test_without_pre_grant_only_the_outsider_gets_every_mask_checked_table(self):
        from verify_effective_access import DEFAULT_ADMIN_TIER, OUT_OF_TIER_PRINCIPAL, temporary_table_access
        principals = self._principals(DEFAULT_ADMIN_TIER, "raw_group", "analyst", "viewer", OUT_OF_TIER_PRINCIPAL)
        assert temporary_table_access(self._spec(), principals) == {
            OUT_OF_TIER_PRINCIPAL: ["cat.a.people", "other.b.orders"]}
        assert temporary_table_access(self._spec(), self._principals("raw_group")) == {}

    def test_pre_grant_gives_each_principal_only_the_tables_it_checks(self):
        from verify_effective_access import DEFAULT_ADMIN_TIER, OUT_OF_TIER_PRINCIPAL, temporary_table_access
        principals = self._principals(DEFAULT_ADMIN_TIER, "raw_group", "analyst", "viewer", OUT_OF_TIER_PRINCIPAL)
        assert temporary_table_access(self._spec(), principals, pre_grant=True) == {
            OUT_OF_TIER_PRINCIPAL: ["cat.a.people", "other.b.orders"],
            "analyst": ["cat.a.people"],
            "raw_group": ["cat.a.people", "cat.a.visits", "other.b.orders"],
            "viewer": ["cat.a.visits", "other.b.orders"],
        }

    def test_an_unprovisioned_tier_gets_nothing(self):
        from verify_effective_access import OUT_OF_TIER_PRINCIPAL, temporary_table_access
        assert temporary_table_access(self._spec(), self._principals("analyst"), pre_grant=True) == {
            "analyst": ["cat.a.people"]}
        assert OUT_OF_TIER_PRINCIPAL not in temporary_table_access(self._spec(), {}, pre_grant=True)

    def test_a_tier_principal_s_grants_name_the_principal_never_its_group(self, monkeypatch):
        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        verifier = EffectiveAccessVerifier({"host": "h", "client_id": "c", "client_secret": "s"})
        verifier._admin_ws = object()
        statements = []
        monkeypatch.setattr(verifier, "run_query", lambda _ws, sql: statements.append(sql))
        principal = VerificationPrincipal("analyst", "genierails-verify-analyst", "app-9", "secret", "7")
        verifier.grant_temporary_table_access(principal, ["cat.a.people"])
        verifier.grant_temporary_table_access(principal, ["cat.a.people"], revoke=True)
        assert statements == [
            "GRANT USE CATALOG ON CATALOG `cat` TO `app-9`",
            "GRANT USE SCHEMA ON SCHEMA `cat`.`a` TO `app-9`",
            "GRANT SELECT ON TABLE `cat`.`a`.`people` TO `app-9`",
            "REVOKE SELECT ON TABLE `cat`.`a`.`people` FROM `app-9`",
            "REVOKE USE SCHEMA ON SCHEMA `cat`.`a` FROM `app-9`",
            "REVOKE USE CATALOG ON CATALOG `cat` FROM `app-9`",
        ]

    @pytest.mark.parametrize("argv, expected", [([], False), (["--pre-grant-access"], True)])
    def test_the_cli_flag_reaches_the_live_run(self, monkeypatch, tmp_path, argv, expected):
        import verify_effective_access as vea
        spec = tmp_path / "spec.json"
        spec.write_text(json.dumps({"column_masks": [], "row_filters": [
            {"table": "cat.a.people", "restricted_principals": ["viewer"], "unrestricted_principals": ["raw"]}]}))
        seen = {}

        def live(spec, auth_file, **kwargs):
            seen.update(kwargs)
            return vea.EffectiveAccessReport(results=[])

        monkeypatch.setattr(vea, "verify_effective_access_live", live)
        assert vea.main(["--spec", str(spec), "--auth-file", str(tmp_path / "auth"), "--live", *argv]) in (0, 1)
        assert seen["pre_grant"] is expected


# -- identical partial/full outputs (ssn, redact) and never-raw columns (cvv) --------------------------
# The live AWS dev-2 run 21fce9f7649e failed at verify-created: ssn and redact columns get partial = full =
# "redacted" under two differently named functions with identical bodies, and a never-raw cvv column masks
# every principal, the deployer included, so there is no raw baseline at all. Both were INCONCLUSIVE.
# The body generate writes for both ssn functions and the cvv function (redact_string).
REDACT_BODY = "CAST((CASE WHEN value IS NULL THEN NULL ELSE '[REDACTED]' END) AS STRING)"


def _ssn_check():
    return ColumnMaskCheck(
        table="cat.sch.people", column="ssn", key_column="id",
        masked_principals=(), unmasked_principals=(),
        expected_tiers=(("raw_group", "raw"), ("analyst", "partial"), ("viewer", "full"),
                        (DEFAULT_ADMIN_TIER, "raw")),
        partial_function="cat.gov.gr_mask_ssn__string__8277f91e_partial",
        full_function="cat.gov.gr_mask_ssn__string__8277f91e_full")


def _ssn_rows():
    raw = [(key, f"123-45-{key:04d}") for key in range(12)] + [(12, None)]
    redacted = [(key, "[REDACTED]") for key in range(12)] + [(12, None)]
    return {"raw": raw, "partial": redacted, "full": list(redacted)}


def test_identical_partial_and_full_functions_verify_by_exact_output():
    check, expected = _ssn_check(), _ssn_rows()
    actual = {principal: expected[tier] for principal, tier in check.expected_tiers}
    result = evaluate_tiered_column_mask_check(check, actual, expected, function_equivalence="")
    assert result.status == PASS, result.detail
    assert result.evidence["per_principal_compared"] == {"analyst": 12, "viewer": 12, "raw_group": 0, DEFAULT_ADMIN_TIER: 0}


@pytest.mark.parametrize("equivalence", [None, "the functions' definitions differ"])
def test_coinciding_outputs_of_unproven_functions_stay_inconclusive(equivalence):
    check, expected = _ssn_check(), _ssn_rows()
    actual = {principal: expected[tier] for principal, tier in check.expected_tiers}
    result = evaluate_tiered_column_mask_check(check, actual, expected, function_equivalence=equivalence)
    assert result.status == INCONCLUSIVE
    assert "not proven identical" in result.detail and (equivalence or "") in result.detail


@pytest.mark.parametrize(("principal", "tier", "evidence"), [
    ("analyst", "raw", "raw_leaks_by_principal"), ("viewer", "raw", "raw_leaks_by_principal"),
    ("raw_group", "full", "mismatches_by_principal")])
def test_identical_functions_still_fail_a_broken_mask(principal, tier, evidence):
    check, expected = _ssn_check(), _ssn_rows()
    actual = {p: expected[t] for p, t in check.expected_tiers}
    actual[principal] = expected[tier]
    result = evaluate_tiered_column_mask_check(check, actual, expected, function_equivalence="")
    assert result.status == FAIL
    assert principal in result.evidence[evidence]


def test_identical_functions_still_fail_a_different_masked_value():
    check, expected = _ssn_check(), _ssn_rows()
    actual = {p: expected[t] for p, t in check.expected_tiers}
    actual["viewer"] = [(key, "***-**-****" if value else value) for key, value in expected["full"]]
    result = evaluate_tiered_column_mask_check(check, actual, expected, function_equivalence="")
    assert result.status == FAIL and result.evidence["mismatches_by_principal"] == {"viewer": 12}


def _cvv_check():
    fn = "cat.gov.gr_mask_card_security_code__string__1a2b3c4d_full"
    return ColumnMaskCheck(
        table="cat.sch.payments", column="cvv", key_column="id",
        masked_principals=(), unmasked_principals=(),
        expected_tiers=(("raw_group", "full"), ("analyst", "full"), ("viewer", "full"),
                        (OUT_OF_TIER_PRINCIPAL, "full"), (DEFAULT_ADMIN_TIER, "full")),
        mask_function=fn, partial_function=fn, full_function=fn)


def _cvv_seen():
    """What every principal, the admin baseline included, reads: the constant (NULL stays NULL)."""
    return [(key, "[REDACTED]") for key in range(12)] + [(12, None)]


def _cvv_eval(actual=None, baseline=None, **kwargs):
    check = _cvv_check()
    seen = _cvv_seen()
    baseline = seen if baseline is None else baseline
    actual = {p: seen for p, _ in check.expected_tiers} if actual is None else actual
    actual.setdefault(DEFAULT_ADMIN_TIER, baseline)
    expected = {"raw": baseline, "partial": baseline, "full": baseline}
    return evaluate_tiered_column_mask_check(check, actual, expected, **kwargs)


def _never_raw_pass():
    from verify_effective_access import NeverRawProof
    return NeverRawProof(PASS, "", "[REDACTED]")


def test_never_raw_column_passes_when_everyone_reads_only_the_constant():
    result = _cvv_eval(never_raw_proof=_never_raw_pass())
    assert result.status == PASS, result.detail
    assert result.evidence["never_raw"] is True
    assert result.evidence["per_principal_compared"][DEFAULT_ADMIN_TIER] == 12
    assert set(result.evidence["per_principal_compared"]) == {p for p, _ in _cvv_check().expected_tiers}


def test_never_raw_column_without_a_mask_proof_is_inconclusive():
    result = _cvv_eval()
    assert result.status == INCONCLUSIVE and "not proven" in result.detail


@pytest.mark.parametrize("status", [FAIL, INCONCLUSIVE])
def test_never_raw_mask_proof_outcome_is_the_check_outcome(status):
    from verify_effective_access import NeverRawProof
    result = _cvv_eval(never_raw_proof=NeverRawProof(status, "the live policy excepts a principal"))
    assert result.status == status and "excepts a principal" in result.detail


def test_never_raw_raw_leak_to_a_tier_fails():
    check = _cvv_check()
    leaked = _cvv_seen()
    leaked[3] = (3, "737")
    actual = {p: _cvv_seen() for p, _ in check.expected_tiers}
    actual["raw_group"] = leaked
    result = _cvv_eval(actual, never_raw_proof=_never_raw_pass())
    assert result.status == FAIL and result.evidence["unexpected_by_principal"] == {"raw_group": 1}
    assert "737" not in result.detail + json.dumps(result.evidence)


def test_never_raw_raw_value_seen_by_the_deployer_baseline_fails():
    raw = [(key, f"{key:03d}") for key in range(12)] + [(12, None)]
    check = _cvv_check()
    actual = {p: _cvv_seen() for p, _ in check.expected_tiers if p != DEFAULT_ADMIN_TIER}
    result = _cvv_eval(actual, baseline=raw, never_raw_proof=_never_raw_pass())
    assert result.status == FAIL
    assert result.evidence["unexpected_by_principal"][DEFAULT_ADMIN_TIER] == 24  # its read and its own sample


def test_never_raw_wrong_constant_fails():
    check = _cvv_check()
    wrong = [(key, "***" if value else value) for key, value in _cvv_seen()]
    result = _cvv_eval({p: wrong for p, _ in check.expected_tiers}, baseline=wrong,
                       never_raw_proof=_never_raw_pass())
    assert result.status == FAIL
    assert set(result.evidence["unexpected_by_principal"]) == {p for p, _ in check.expected_tiers}


def test_never_raw_null_where_the_baseline_reads_the_constant_fails():
    check = _cvv_check()
    nulled = [(key, None) for key, _ in _cvv_seen()]
    actual = {p: _cvv_seen() for p, _ in check.expected_tiers}
    actual["viewer"] = nulled
    result = _cvv_eval(actual, never_raw_proof=_never_raw_pass())
    assert result.status == FAIL and result.evidence["unexpected_by_principal"] == {"viewer": 12}


@pytest.mark.parametrize("rows", [[], [(99, "[REDACTED]")], [(12, None)]])
def test_never_raw_principal_without_a_paired_masked_row_is_inconclusive(rows):
    check = _cvv_check()
    actual = {p: _cvv_seen() for p, _ in check.expected_tiers}
    actual["analyst"] = rows
    result = _cvv_eval(actual, never_raw_proof=_never_raw_pass())
    assert result.status == INCONCLUSIVE and result.evidence["missing"] == ["analyst"]


def test_a_check_with_a_raw_admin_baseline_is_not_never_raw():
    # Only partial/full tiers but the admin reads raw: the ordinary raw-baseline comparison runs.
    check = replace(_cvv_check(), expected_tiers=(("viewer", "full"),))
    raw = [(key, f"{key:03d}") for key in range(12)]
    masked = [(key, "[REDACTED]") for key in range(12)]
    result = evaluate_tiered_column_mask_check(check, {"viewer": masked},
                                               {"raw": raw, "partial": masked, "full": masked})
    assert result.status == PASS and "never_raw" not in result.evidence


def test_never_raw_constant_problem_needs_one_masked_output():
    from verify_effective_access import never_raw_constant_problem, never_raw_probes
    probes = never_raw_probes("STRING")
    assert len(set(probes)) == 2
    assert never_raw_constant_problem(probes, ["[REDACTED]", "[REDACTED]"]) == ""
    assert never_raw_constant_problem(probes, [None, None]) == ""
    assert "not a constant" in never_raw_constant_problem(probes, list(probes))
    assert "not a constant" in never_raw_constant_problem(probes, ["[REDACTED]", "x"])
    assert "unchanged" in never_raw_constant_problem(probes, [probes[0], probes[0]])
    assert "one output per probe" in never_raw_constant_problem(probes, [])
    assert never_raw_probes("DECIMAL(38, 9)") and never_raw_probes("DATE")
    assert never_raw_probes("STRING); DROP TABLE x; --") == () and never_raw_probes("MAP<STRING,INT>") == ()


def test_routines_equivalence_needs_identical_definitions_and_types():
    from verify_effective_access import routines_equivalence_problem
    routine = ("SQL", "SQL", "YES", "CONTAINS_SQL", REDACT_BODY, "STRING")
    params = [("value", "STRING")]
    assert routines_equivalence_problem(routine, params, routine, params) == ""
    # Formatting, case and the parameter's name don't matter.
    renamed = routine[:4] + ("cast((case when v is null then null\n else '[REDACTED]' end) as string)",) + routine[5:]
    assert routines_equivalence_problem(routine, params, renamed, [("v", "STRING")]) == ""
    other_literal = routine[:4] + (REDACT_BODY.replace("[REDACTED]", "[redacted]"),) + routine[5:]
    assert "definitions differ" in routines_equivalence_problem(routine, params, other_literal, params)
    assert "return type" in routines_equivalence_problem(routine, params, routine[:5] + ("INT",), params)
    assert "argument types" in routines_equivalence_problem(routine, params, routine, [("value", "INT")])
    assert "determinism" in routines_equivalence_problem(routine, params, routine[:2] + ("NO",) + routine[3:],
                                                         params)
    assert "could not be compared" in routines_equivalence_problem(routine, params, routine[:4] + ("",) + routine[5:],
                                                                   params)


# -- the live proofs, as the admin -----------------------------------------------------------------
class _FakeCatalogSql:
    """Answers the admin's metadata and probe SQL for one column and its mask function(s)."""

    def __init__(self, policies_function="cat.gov.gr_mask_card_security_code__string__1a2b3c4d_full"):
        self.routines = {
            "gr_mask_card_security_code__string__1a2b3c4d_full": (REDACT_BODY, "STRING"),
            "gr_mask_ssn__string__8277f91e_partial": (REDACT_BODY, "STRING"),
            "gr_mask_ssn__string__8277f91e_full": (REDACT_BODY, "STRING"),
        }
        self.direct = 0
        self.probe = lambda values: ["[REDACTED]" for _ in values]
        self.sql: list[str] = []

    def run_query(self, ws, sql, params=None):
        self.sql.append(sql)
        params = params or {}
        if "information_schema.column_masks" in sql:
            return [(self.direct,)]
        if "information_schema.column_tags" in sql:
            return [("gr_treatment", "card_security_code__string__1a2b3c4d")]
        if "information_schema.routines" in sql:
            body = self.routines.get(params["n"])
            return [("SQL", "SQL", "YES", "CONTAINS_SQL", body[0], body[1])] if body else []
        if "information_schema.parameters" in sql:
            body = self.routines.get(params["n"])
            return [("value", body[1])] if body else []
        if sql.startswith("SELECT `cat`.`gov`."):
            return [tuple(self.probe([params[k] for k in sorted(params)]))]
        raise AssertionError(sql)


def _live_verifier(monkeypatch, fake):
    monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
    verifier = EffectiveAccessVerifier({"host": "h", "client_id": "c", "client_secret": "s"}, warehouse_id="w")
    monkeypatch.setattr(verifier, "_ws_for", lambda principal: object())
    monkeypatch.setattr(verifier, "run_query", fake.run_query)
    return verifier


def _cvv_policy(**overrides):
    policy = {"name": "gr_mask_cat_cvv_full", "policy_type": "POLICY_TYPE_COLUMN_MASK",
              "to_principals": ["account users"], "except_principals": [],
              "column_mask": {"function_name": _cvv_check().mask_function, "on_column": "c"},
              "match_columns": [{"alias": "c", "condition":
                                 "hasTagValue('gr_treatment', 'card_security_code__string__1a2b3c4d')"}]}
    policy.update(overrides)
    return policy


ADMIN_PRINCIPAL = VerificationPrincipal(DEFAULT_ADMIN_TIER, "admin-baseline", "c", "s")


def test_never_raw_mask_proof_passes_the_attached_constant_mask(monkeypatch):
    fake = _FakeCatalogSql()
    proof = _live_verifier(monkeypatch, fake).never_raw_mask_proof(ADMIN_PRINCIPAL, _cvv_check(), [_cvv_policy()])
    assert (proof.status, proof.constant) == (PASS, "[REDACTED]"), proof.detail
    [probe] = [sql for sql in fake.sql if sql.startswith("SELECT `cat`")]
    assert probe == ("SELECT `cat`.`gov`.`gr_mask_card_security_code__string__1a2b3c4d_full`(CAST(:p0 AS STRING)), "
                     "`cat`.`gov`.`gr_mask_card_security_code__string__1a2b3c4d_full`(CAST(:p1 AS STRING))")


@pytest.mark.parametrize(("policies", "direct", "why"), [
    ([], 0, "0 live column-mask policies"),
    ([_cvv_policy(except_principals=["deployer-sp"])], 0, "without exception"),
    ([_cvv_policy(to_principals=["analyst", "viewer", "raw_group"])], 0, "without exception"),
    ([_cvv_policy(column_mask={"function_name": "cat.gov.other", "on_column": "c"})], 0, "not the configured"),
    ([_cvv_policy()], 1, "attached to the column directly"),
])
def test_never_raw_mask_proof_fails_a_mask_not_attached_to_everyone(monkeypatch, policies, direct, why):
    fake = _FakeCatalogSql()
    fake.direct = direct
    proof = _live_verifier(monkeypatch, fake).never_raw_mask_proof(ADMIN_PRINCIPAL, _cvv_check(), policies)
    assert proof.status == FAIL and why in proof.detail


@pytest.mark.parametrize(("probe", "why"), [
    (lambda values: list(values), "not a constant mask"),
    (lambda values: ["[REDACTED]", "[R]"], "not a constant mask")])
def test_never_raw_mask_proof_fails_a_function_that_is_not_one_constant(monkeypatch, probe, why):
    fake = _FakeCatalogSql()
    fake.probe = probe
    proof = _live_verifier(monkeypatch, fake).never_raw_mask_proof(ADMIN_PRINCIPAL, _cvv_check(), [_cvv_policy()])
    assert proof.status == FAIL and why in proof.detail


def test_never_raw_mask_proof_of_a_caller_dependent_function_is_inconclusive(monkeypatch):
    fake = _FakeCatalogSql()
    fake.routines["gr_mask_card_security_code__string__1a2b3c4d_full"] = (
        "CASE WHEN is_account_group_member('admins') THEN value ELSE '[REDACTED]' END", "STRING")
    proof = _live_verifier(monkeypatch, fake).never_raw_mask_proof(ADMIN_PRINCIPAL, _cvv_check(), [_cvv_policy()])
    assert proof.status == INCONCLUSIVE and "caller-independent" in proof.detail


# Masks that return the constant for both fixed probes (and for the sampled
# value 123) but leak an unprobed input such as 999 raw: every one is built
# only from allowlisted builtins, so only the body's structure gives it away.
_PROBES = "'genierails-never-raw-probe-a', 'genierails-never-raw-probe-b'"
LEAKING_NEVER_RAW_BODIES = {
    "case-in": f"CASE WHEN value IN ({_PROBES}, '123') THEN '[REDACTED]' ELSE value END",
    "case-not-in": f"CASE WHEN value NOT IN ({_PROBES}, '123') THEN value ELSE '[REDACTED]' END",
    "comparison": "CASE WHEN value = '999' THEN value ELSE '[REDACTED]' END",
    "comparison-after-null-passthrough": (
        "CASE WHEN value IS NULL THEN NULL WHEN value <> '999' THEN '[REDACTED]' ELSE value END"),
    "function-call": "CASE WHEN length(value) = 3 AND value <> '123' THEN value ELSE '[REDACTED]' END",
    "call-in-null-test": "CASE WHEN nullif(value, '999') IS NULL THEN value ELSE '[REDACTED]' END",
    "concat": f"concat('[REDACTED]', CASE WHEN value IN ({_PROBES}, '123') THEN '' ELSE value END)",
    "pipe-concat": "'[REDACTED]' || CASE WHEN value = '999' THEN value ELSE '' END",
    "if": "if(value = '999', value, '[REDACTED]')",
    "coalesce": "coalesce(nullif(value, '999'), value)",
    "null-test-is-not-the-whole-condition": (
        "CASE WHEN value IS NULL OR value = '999' THEN value ELSE '[REDACTED]' END"),
    "wrapped": f"CAST((CASE WHEN value IN ({_PROBES}, '123') THEN '[REDACTED]' ELSE value END) AS STRING)",
}


def _leaking_probe(values):
    """What a leaking body returns for the two fixed probes: the constant."""
    return ["[REDACTED]" for _ in values]


@pytest.mark.parametrize("body", list(LEAKING_NEVER_RAW_BODIES.values()), ids=list(LEAKING_NEVER_RAW_BODIES))
def test_never_raw_mask_proof_of_an_argument_dependent_body_is_inconclusive(monkeypatch, body):
    from verify_effective_access import fixed_point_function_problem, never_raw_structure_problem

    # Each body passes the caller-independence allowlist and the probes ...
    assert fixed_point_function_problem(
        routine_body="SQL", external_language="SQL", is_deterministic="YES", sql_data_access="CONTAINS_SQL",
        definition=body, parameters=["value"]) == ""
    assert "depend on its argument" in never_raw_structure_problem(body, "value")
    fake = _FakeCatalogSql()
    fake.routines["gr_mask_card_security_code__string__1a2b3c4d_full"] = (body, "STRING")
    fake.probe = _leaking_probe
    proof = _live_verifier(monkeypatch, fake).never_raw_mask_proof(ADMIN_PRINCIPAL, _cvv_check(), [_cvv_policy()])
    # ... but is not proven constant, so the column can't PASS even when every
    # sampled read shows only the constant.
    assert proof.status == INCONCLUSIVE and "depend on its argument" in proof.detail, proof
    assert not [sql for sql in fake.sql if sql.startswith("SELECT `cat`")]
    result = _cvv_eval(never_raw_proof=proof)
    assert result.status == INCONCLUSIVE and "depend on its argument" in result.detail


@pytest.mark.parametrize("body", [
    REDACT_BODY,
    "CASE WHEN value IS NULL THEN NULL ELSE '[REDACTED]' END",
    "CASE WHEN value IS NOT NULL THEN '[REDACTED]' END",
    "CASE WHEN Value is   null THEN CAST(NULL AS STRING) ELSE '[REDACTED]' END",
    "'[REDACTED]'",
    "CAST(NULL AS DATE)",
    "TRY_CAST((CAST(NULL AS DECIMAL(38, 9))) AS DECIMAL(38, 9))",
])
def test_never_raw_structure_allows_only_a_null_test_of_the_argument(body):
    from verify_effective_access import never_raw_structure_problem

    assert never_raw_structure_problem(body, "VALUE") == ""
    # A body that never names the parameter is a constant.
    assert never_raw_structure_problem(body, "other_arg") == ""


@pytest.mark.parametrize(("body", "parameter"), [
    ("CASE WHEN value IS NULL THEN value ELSE '[REDACTED]' END", "value"),
    ("CASE WHEN null IS NULL THEN NULL ELSE '[REDACTED]' END", "null"),
    ("CASE WHEN value IS NULL", "value"),
    ("CASE WHEN value IS NOT NULL", "value"),
    ("'[REDACTED]' /* unterminated", "value"),
    ("", "value"),
    (None, "value"),
    (REDACT_BODY, ""),
])
def test_never_raw_structure_fails_closed(body, parameter):
    from verify_effective_access import never_raw_structure_problem

    assert never_raw_structure_problem(body, parameter)


def test_every_shipped_never_raw_body_is_proven_argument_independent():
    from deterministic_governance import NEVER_RAW_TREATMENTS
    from mask_library import load_library, sql_body
    from verify_effective_access import fixed_point_function_problem, never_raw_structure_problem

    library = load_library()
    checked = 0
    for name in NEVER_RAW_TREATMENTS:
        treatment = library["treatments"][name]
        for family in treatment["types"]:
            sql_type = {"NUMERIC": "DECIMAL(10,2)"}.get(family, family)
            body = sql_body(treatment["full"], sql_type)
            assert never_raw_structure_problem(body, "value") == "", (name, body)
            caller_problem = fixed_point_function_problem(
                routine_body="SQL", external_language="SQL", is_deterministic="YES",
                sql_data_access="CONTAINS_SQL", definition=body, parameters=["value"])
            # The allowlist reads DECIMAL(p, s) as a call, so a NUMERIC
            # never-raw column stays INCONCLUSIVE (fail closed); the rest pass.
            assert (caller_problem == "") == (family != "NUMERIC"), (name, body, caller_problem)
            checked += 1
    assert checked >= 4
    # The live cvv column's function body is the one these tests' fake serves.
    assert sql_body(library["treatments"]["card_security_code"]["full"], "STRING") == REDACT_BODY


def test_function_equivalence_compares_the_live_definitions(monkeypatch):
    fake = _FakeCatalogSql()
    verifier = _live_verifier(monkeypatch, fake)
    check = _ssn_check()
    assert verifier.function_equivalence_problem(ADMIN_PRINCIPAL, check.partial_function, check.full_function) == ""
    fake.routines["gr_mask_ssn__string__8277f91e_full"] = (REDACT_BODY.replace("ELSE '", "ELSE '*"), "STRING")
    assert "definitions differ" in verifier.function_equivalence_problem(
        ADMIN_PRINCIPAL, check.partial_function, check.full_function)
    del fake.routines["gr_mask_ssn__string__8277f91e_full"]
    with pytest.raises(RuntimeError, match="0 routines named"):
        verifier.function_equivalence_problem(ADMIN_PRINCIPAL, check.partial_function, check.full_function)
