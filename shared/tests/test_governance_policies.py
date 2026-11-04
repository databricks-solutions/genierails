"""Deterministic tier policies: shapes, exclusivity, never-raw, fail-safe and naming."""

import os
import subprocess
from itertools import product
from pathlib import Path

import pytest

from governance_policies import (
    FAILSAFE_TREATMENT, DeterministicGovernanceError, apply_model, base_treatment, build_model,
    build_policies, column_comments_from_ddl, column_types_from_ddl, expand_principals,
    free_text_partial_columns, model_fingerprint,
    parse_treatment_value, principal_sets_are_disjoint, replace_sql_library, resolve_column,
    sql_library_block,
)
from validate_abac import ValidationResult, validate_coverage_gate

TIERS = ["raw", "regional", "masked"]


def test_free_text_partial_heuristic_is_advisory_and_centralized():
    classes = {
        "cat.sch.t.notes": ["phone_number"],
        "cat.sch.t.phone": ["phone_number"],
        "cat.sch.t.remarks": ["email_address", "phone_number"],
        "cat.sch.t.details": ["phone_number"],
        "cat.sch.t.message": ["card_pin"],
        "cat.sch.t.memo": ["compensation"],
        "cat.sch.t.text": ["us_ssn"],
    }
    types = {name: "STRING" for name in classes} | {"cat.sch.t.memo": "BIGINT"}
    model = _model(classes, types)
    comments = column_comments_from_ddl(
        "CREATE TABLE cat.sch.t (\n"
        " details STRING COMMENT 'Customer''s free text, e.g. (a > b), etc',\n"
        " phone STRING COMMENT 'Mobile phone used for text messages'\n"
        ");"
    )
    assert comments == {
        "cat.sch.t.details": "Customer's free text, e.g. (a > b), etc",
        "cat.sch.t.phone": "Mobile phone used for text messages",
    }
    assert column_types_from_ddl(
        "CREATE TABLE cat.sch.t (details STRING COMMENT 'Customer''s free text, e.g. (a > b), etc');"
    ) == {"cat.sch.t.details": "STRING"}
    assert free_text_partial_columns(model.columns, comments) == ["cat.sch.t.details", "cat.sch.t.notes"]
    assert model.columns["cat.sch.t.notes"].partial == "last4"
    assert base_treatment(model.columns["cat.sch.t.remarks"].value) == "redact"


@pytest.mark.parametrize("settings", [
    {"treatment_versions": {"phone_partial": {"partial": "redacted"}}},
    {"column_overrides": {"cat.sch.t.notes": {"partial": "redacted"}}},
    {"column_overrides": {"cat.sch.t.notes": {"treatment": "redact"}}},
])
def test_free_text_advisory_clears_when_the_effective_partial_tier_is_fully_redacted(settings):
    model = _model({"cat.sch.t.notes": ["phone_number"]}, **settings)
    assert free_text_partial_columns(model.columns) == []
    assert model.columns["cat.sch.t.notes"].partial_sql == model.columns["cat.sch.t.notes"].full_sql


def _model(classes, types=None, **settings):
    types = types or {column.lower(): "STRING" for column in classes}
    return build_model(classes, types, {"access_tier_groups": TIERS, **settings},
                       deployer_principal="deployer-sp")


def test_ordinary_treatment_has_partial_and_full_policies_with_the_tier_shape():
    model = _model({"cat.sch.t.email": ["email_address"]})
    partial, full = sorted(model.fgac_policies, key=lambda p: p["name"].endswith("_full"))
    assert partial["to_principals"] == ["regional"]
    assert partial["except_principals"] == ["raw", "deployer-sp"]
    assert full["to_principals"] == ["account users"]
    assert full["except_principals"] == ["raw", "regional", "deployer-sp"]
    value = model.tag_values[0]
    assert partial["function_name"] == f"gr_mask_{value}_partial"
    assert full["function_name"] == f"gr_mask_{value}_full"
    assert partial["match_condition"] == full["match_condition"] == f"hasTagValue('gr_treatment', '{value}')"


def _mask_for(policies, memberships):
    """Which of a value's policies apply to a principal in ``memberships``."""
    principals = set(memberships) | {"account users"}
    return [p["name"].rsplit("_", 1)[1] for p in policies
            if principals & set(p["to_principals"]) and not principals & set(p.get("except_principals") or [])]


@pytest.mark.parametrize("tiers", [["raw"], ["raw", "masked"], ["raw", "a", "masked"], ["raw", "a", "b", "masked"]])
def test_every_principal_gets_at_most_one_mask_and_the_most_privileged_tier(tiers):
    policies = build_policies(values_by_catalog={"cat": ["email_partial__string__0a1b2c3d"]},
                              access_tier_groups=tiers, raw_exempt_principals=["etl"],
                              deployer_principal="sp")
    pool = tiers + ["etl", "sp", "outsider"]
    for flags in product([False, True], repeat=len(pool)):
        member = [name for name, on in zip(pool, flags) if on]
        masks = _mask_for(policies, member)
        assert len(masks) <= 1, (member, masks)
        if {tiers[0], "etl", "sp"} & set(member):
            expected = []
        elif set(tiers[1:-1]) & set(member):
            expected = ["partial"]
        else:
            expected = ["full"]  # the last tier, and anyone outside the tiers
        assert masks == expected, member


def test_partial_and_full_effective_sets_are_disjoint_with_nested_groups():
    members = {"regional": ["alice", "nested"], "nested": ["bob"], "raw": ["admin", "alice"],
               "account users": ["alice", "bob", "admin", "outsider"]}
    assert expand_principals(["regional"], members) >= {"alice", "bob"}
    policies = build_policies(values_by_catalog={"cat": ["email_partial__string__0a1b2c3d"]},
                              access_tier_groups=TIERS)
    partial, full = policies
    assert principal_sets_are_disjoint(partial["to_principals"], partial["except_principals"],
                                       full["to_principals"], full["except_principals"], members)


@pytest.mark.parametrize("classes", [["card_security_code"], ["secret"], ["card_pin", "name"], ["future_class"]])
def test_never_raw_and_unmapped_are_full_for_everyone_including_the_deployer(classes):
    model = _model({"cat.sch.t.c": classes}, raw_exempt_principals=["etl"])
    assert len(model.fgac_policies) == 1
    (policy,) = model.fgac_policies
    assert policy["to_principals"] == ["account users"]
    assert policy["except_principals"] == []
    assert "_partial" not in model.functions_sql


def test_unmapped_class_wins_and_is_reported():
    model = _model({"cat.sch.t.c": ["email_address", "future_class"]})
    assert base_treatment(model.tag_values[0]) == FAILSAFE_TREATMENT
    assert model.unmapped == [("cat.sch.t.c", "future_class")]


def test_never_raw_class_names_the_column_even_when_another_class_is_stricter_by_partial():
    item = resolve_column("c.s.t.x", ["health_data", "card_pin"], "STRING")
    assert item.never_raw and item.treatment == "card_pin"


def test_functions_are_exactly_typed_since_unity_catalog_has_no_overloading():
    model = _model({"cat.sch.t.dob": ["date_of_birth"], "cat.sch.t.amt": ["compensation"]},
                   {"cat.sch.t.dob": "TIMESTAMP_NTZ", "cat.sch.t.amt": "decimal(10, 2)"})
    sql = model.functions_sql
    assert sql.startswith("USE CATALOG cat;\nUSE SCHEMA sch;")
    assert "(value TIMESTAMP_NTZ) RETURNS TIMESTAMP_NTZ" in sql
    assert "(value DECIMAL(10,2)) RETURNS DECIMAL(10,2)" in sql
    names = [line.split("(", 1)[0] for line in sql.splitlines() if line.startswith("CREATE")]
    assert len(names) == len(set(names)) == 4
    assert {parse_treatment_value(v)[1] for v in model.tag_values} == {"timestamp_ntz", "decimal_10_2"}


def test_complex_types_and_deferred_overrides_are_refused():
    with pytest.raises(DeterministicGovernanceError, match="scalar"):
        _model({"cat.sch.t.c": ["email_address"]}, {"cat.sch.t.c": "ARRAY<STRING>"})
    with pytest.raises(DeterministicGovernanceError, match="tier_access_overrides"):
        _model({"cat.sch.t.c": ["email_address"]}, tier_access_overrides={"email_partial": {"regional": "full"}})
    with pytest.raises(DeterministicGovernanceError, match="no column type"):
        build_model({"cat.sch.t.c": ["email_address"]}, {}, {"access_tier_groups": TIERS})


def test_generation_follows_the_mask_sql_not_the_principals():
    base = _model({"cat.sch.t.ip": ["ip_address"]})
    moved = build_model({"cat.sch.t.ip": ["ip_address"]}, {"cat.sch.t.ip": "STRING"},
                        {"access_tier_groups": ["raw", "other", "masked"]})
    assert base.tag_values == moved.tag_values
    changed = _model({"cat.sch.t.ip": ["ip_address"]}, treatment_versions={"ip_address": {"partial": "redacted"}})
    assert changed.tag_values != base.tag_values
    assert base_treatment(changed.tag_values[0]) == "ip_address"
    # Same inputs, same output.
    assert _model({"cat.sch.t.ip": ["ip_address"]}) == base


def test_one_tier_is_raw_only_and_two_tiers_have_no_partial_policy():
    one = build_policies(values_by_catalog={"c": ["email_partial__string__0a1b2c3d"]}, access_tier_groups=["raw"])
    assert [(p["to_principals"], p["except_principals"]) for p in one] == [(["account users"], ["raw"])]
    two = build_policies(values_by_catalog={"c": ["email_partial__string__0a1b2c3d"]},
                         access_tier_groups=["raw", "masked"])
    assert [p["name"].endswith("_full") for p in two] == [True]


def test_column_types_are_read_from_fetched_ddl():
    ddl = """-- fetched
CREATE TABLE cat.sch.orders (
  id BIGINT NOT NULL,
  amount DECIMAL(10, 2) COMMENT 'x',
  tags ARRAY<STRING>,
  meta STRUCT<a: INT, b: STRING>,
  email STRING
) USING delta;
"""
    assert column_types_from_ddl(ddl) == {
        "cat.sch.orders.id": "BIGINT", "cat.sch.orders.amount": "DECIMAL(10, 2)",
        "cat.sch.orders.tags": "ARRAY<STRING>", "cat.sch.orders.meta": "STRUCT<a: INT, b: STRING>",
        "cat.sch.orders.email": "STRING",
    }


def test_apply_model_replaces_masks_and_legacy_sensitivity_tags_but_keeps_row_filters():
    model = _model({"cat.sch.t.email": ["email_address"]})
    cfg = {
        "tag_policies": [{"key": "pii_level", "values": ["masked_email"]}, {"key": "row_scope", "values": ["anz"]}],
        "tag_assignments": [
            {"entity_type": "columns", "entity_name": "cat.sch.t.email", "tag_key": "pii_level", "tag_value": "masked_email"},
            {"entity_type": "tables", "entity_name": "cat.sch.t", "tag_key": "row_scope", "tag_value": "anz"},
        ],
        "fgac_policies": [
            {"name": "legacy", "policy_type": "POLICY_TYPE_COLUMN_MASK"},
            {"name": "rf", "policy_type": "POLICY_TYPE_ROW_FILTER"},
        ],
    }
    out = apply_model(cfg, model)
    assert [p["name"] for p in out["fgac_policies"]][0] == "rf"
    assert all(p["comment"] == "GenieRails deterministic governance policy" for p in out["fgac_policies"][1:])
    assert {a["tag_key"] for a in out["tag_assignments"]} == {"row_scope", "gr_treatment"}
    assert [p["key"] for p in out["tag_policies"]] == ["row_scope", "gr_treatment"]
    assert out["tag_policies"][-1]["values"] == model.tag_values


def test_sql_library_block_round_trips_and_fingerprint_tracks_principals():
    model = _model({"cat.sch.t.email": ["email_address"]})
    sql = replace_sql_library("USE CATALOG cat;\nCREATE FUNCTION legacy(v STRING) RETURNS STRING RETURN v;\n",
                              model.functions_sql)
    assert sql.startswith("USE CATALOG cat;\nCREATE FUNCTION legacy")
    assert sql_library_block(sql) == model.functions_sql
    assert replace_sql_library(sql, model.functions_sql) == sql
    cfg = apply_model({}, model)
    moved = apply_model({}, build_model({"cat.sch.t.email": ["email_address"]}, {"cat.sch.t.email": "STRING"},
                                        {"access_tier_groups": ["raw", "x", "masked"]},
                                        deployer_principal="deployer-sp"))
    assert model_fingerprint(cfg, model.functions_sql) != model_fingerprint(moved, model.functions_sql)
    assert model_fingerprint(cfg, model.functions_sql) == model_fingerprint(dict(cfg), model.functions_sql)


def test_coverage_check_accepts_a_deterministic_model_and_still_blocks_gaps():
    model = _model({"cat.sch.t.email": ["email_address"], "cat.sch.t.c": ["future_class"]})
    cfg = apply_model({}, model)
    functions = {line.split("(", 1)[0].rsplit(" ", 1)[1]
                 for line in model.functions_sql.splitlines() if line.startswith("CREATE")}
    clean = ValidationResult()
    validate_coverage_gate(cfg, functions, "", clean)
    assert clean.errors == []
    # An unmapped class is redacted, but blocks new access until it is mapped.
    marker = ValidationResult()
    validate_coverage_gate(cfg, functions, "# gr.classification_unmapped: cat.sch.t.c|class.future_class\n", marker)
    assert any("no mapping/rule" in error for error in marker.errors)
    missing = ValidationResult()
    validate_coverage_gate(cfg, functions - {f"gr_mask_{model.tag_values[0]}_full"}, "", missing)
    assert any("missing a masking function" in error for error in missing.errors)


NOTES = "cat.sch.notes.free_text"


def _functions(model):
    return {line.split("(", 1)[0].rsplit(" ", 1)[1]
            for line in model.functions_sql.splitlines() if line.startswith("CREATE")}


@pytest.mark.parametrize("classes", [
    ["email_address", "name", "phone_number"], ["class.phone_number", "class.name", "class.email_address"],
])
def test_a_free_text_column_mixing_partial_classes_is_redacted_for_every_masked_tier(classes):
    item = resolve_column(NOTES, classes, "STRING")
    assert (item.treatment, item.partial, item.full, item.never_raw) == ("redact", "redacted", "redacted", False)
    model = _model({NOTES: classes})
    (value,) = model.tag_values
    assert base_treatment(value) == "redact"
    assert {p["function_name"] for p in model.fgac_policies} == {f"gr_mask_{value}_partial", f"gr_mask_{value}_full"}
    # The partial tier sees [REDACTED], never a last-4 of the note.
    assert "last4" not in item.partial_sql and "right(" not in item.partial_sql
    assert item.partial_sql == item.full_sql
    assert model.mixed_partial == [(NOTES, ["email_partial", "name_partial", "phone_partial"])]


def test_mixed_partial_is_recorded_for_every_mixed_column():
    model = _model({NOTES: ["email_address", "phone_number"], "cat.sch.t.email": ["email_address"],
                    "cat.sch.t.acct": ["bank_number", "iban_code"], "cat.sch.t.pin": ["email_address", "card_pin"]})
    assert model.mixed_partial == [(NOTES, ["email_partial", "phone_partial"])]
    assert model.overridden_mixed == []
    assert base_treatment(model.columns["cat.sch.t.acct"].value) == "account_last4"
    assert model.columns["cat.sch.t.pin"].never_raw


@pytest.mark.parametrize("override", [
    {"treatment": "phone_partial"}, {"treatment": "email_partial", "partial": "partial"},
    {"partial": "last4"}, {"treatment": "ssn", "partial": "last4"},
])
def test_an_override_never_gives_a_mixed_column_a_partial_version(override):
    # Regression (PR #223 review): a phone_partial override kept the note's last 4.
    classes = ["email_address", "name", "phone_number"]
    item = resolve_column(NOTES, classes, "STRING", column_overrides={NOTES: override})
    assert (item.treatment, item.partial, item.full, item.never_raw) == ("redact", "redacted", "redacted", False)
    assert item.partial_sql == item.full_sql
    model = _model({NOTES: classes}, column_overrides={NOTES: override})
    assert base_treatment(model.tag_values[0]) == "redact"
    assert model.mixed_partial == [(NOTES, ["email_partial", "name_partial", "phone_partial"])]
    assert model.overridden_mixed == [NOTES]


def test_an_override_to_redact_or_never_raw_is_kept_on_a_mixed_column():
    classes = ["email_address", "phone_number"]
    kept = _model({NOTES: classes}, column_overrides={NOTES: {"treatment": "redact"}})
    assert base_treatment(kept.tag_values[0]) == "redact" and kept.overridden_mixed == []
    pin = resolve_column(NOTES, classes, "STRING", column_overrides={NOTES: {"treatment": "card_pin"}})
    assert pin.treatment == "card_pin" and pin.never_raw
    # treatment_versions of a redacted class can't open a mixed column either.
    item = resolve_column(NOTES, ["email_address", "us_ssn", "phone_number"], "STRING",
                          treatment_versions={"ssn": {"partial": "last4"}})
    assert (item.treatment, item.partial) == ("redact", "redacted")
    assert resolve_column("c.s.t.ssn", ["us_ssn"], "STRING", treatment_versions={"ssn": {"partial": "last4"}}).partial == "last4"


def test_resolve_column_keeps_never_raw_when_raw_column_override_is_present():
    item = resolve_column(
        "cat.sch.t.cvv",
        ["card_security_code"],
        "STRING",
        column_overrides={"cat.sch.t.cvv": {"partial": "raw"}},
    )
    assert item.treatment == "card_security_code"
    assert item.never_raw
    assert item.partial_sql == item.full_sql


def test_generation_refuses_column_override_raw_on_never_raw_column_with_clear_error():
    with pytest.raises(DeterministicGovernanceError) as raised:
        _model(
            {"cat.sch.t.cvv": ["card_security_code"]},
            column_overrides={"cat.sch.t.cvv": {"partial": "raw"}},
        )
    assert str(raised.value) == (
        'column_overrides["cat.sch.t.cvv"].partial may not be raw; column cat.sch.t.cvv '
        "is never shown raw to anyone and no override exists"
    )


@pytest.mark.parametrize("partial", ["raw", "last4"])
def test_redact_treatment_versions_cannot_open_a_mixed_column(partial):
    item = resolve_column(
        NOTES, ["email_address", "name", "phone_number"], "STRING",
        treatment_versions={"redact": {"partial": partial}},
    )
    assert (item.value, item.partial, item.full) == (
        "redact__string__df6fec27", "redacted", "redacted",
    )
    assert item.partial_sql == item.full_sql


@pytest.mark.parametrize(("never_raw", "other", "expected"), [
    ("card_security_code", "date_of_birth", "card_security_code__string__0ede9584"),
    ("card_security_code", "medical_date", "card_security_code__string__0ede9584"),
    ("card_security_code", "compensation", "card_security_code__string__0ede9584"),
    ("card_security_code", "credit_score", "card_security_code__string__0ede9584"),
    ("card_pin", "date_of_birth", "card_pin__string__9f963477"),
    ("card_pin", "medical_date", "card_pin__string__9f963477"),
    ("card_pin", "compensation", "card_pin__string__9f963477"),
    ("card_pin", "credit_score", "card_pin__string__9f963477"),
    ("card_track_data", "date_of_birth", "card_track_data__string__666ff6e2"),
    ("card_track_data", "medical_date", "card_track_data__string__666ff6e2"),
    ("card_track_data", "compensation", "card_track_data__string__666ff6e2"),
    ("card_track_data", "credit_score", "card_track_data__string__666ff6e2"),
])
def test_resolve_column_keeps_main_values_for_never_raw_pairs(never_raw, other, expected):
    item = resolve_column("cat.sch.t.c", [never_raw, other], "STRING")
    assert item.value == expected
    assert item.never_raw and item.partial == item.full == "null"


@pytest.mark.parametrize(("sql_type", "expected"), [
    ("DATE", "card_pin__date__70193ac9"),
    ("BIGINT", "card_pin__bigint__c61464db"),
    ("DECIMAL(12,2)", "card_pin__decimal_12_2__0542eeb4"),
])
def test_resolve_column_pins_never_raw_date_and_numeric_values(sql_type, expected):
    item = resolve_column("cat.sch.t.c", ["card_pin", "date_of_birth"], sql_type)
    assert item.value == expected and item.never_raw


def test_coverage_check_blocks_a_mixed_column_that_keeps_a_partial_treatment():
    mixed = _model({NOTES: ["email_address", "name", "phone_number"]})
    marker = f"# gr.classification_mixed_partial: {NOTES}|email_partial,name_partial,phone_partial\n"
    good = ValidationResult()
    validate_coverage_gate(apply_model({}, mixed), _functions(mixed), marker, good)
    assert good.errors == []
    # The pre-fix output: the alphabetical tie-break gave the note phone_partial.
    leaky = _model({NOTES: ["phone_number"]})
    bad = ValidationResult()
    validate_coverage_gate(apply_model({}, leaky), _functions(leaky), marker, bad)
    (error,) = bad.errors
    assert "multi-class columns keeping a partial treatment" in error
    assert NOTES in error and leaky.tag_values[0] in error and "expected redact" in error
    # A never-raw treatment keeps no partial version.
    strict = _model({NOTES: ["email_address", "name", "phone_number", "card_pin"]})
    result = ValidationResult()
    validate_coverage_gate(apply_model({}, strict), _functions(strict), marker, result)
    assert result.errors == []
    # Any other treatment on a mixed column is blocked, even one redacted by
    # default: treatment_versions could give ssn last4 (the generator gives redact).
    ssn = _model({NOTES: ["email_address", "us_ssn"]})
    result = ValidationResult()
    validate_coverage_gate(apply_model({}, ssn), _functions(ssn), marker, result)
    assert any("expected redact" in error for error in result.errors)


LIVE = {NOTES: ["class.email_address", "class.name", "class.phone_number"]}


def _hand_edited(classes=("phone_number",)):
    """A config whose note carries phone_partial and no marker (deleted or never written)."""
    leaky = _model({NOTES: list(classes)})
    return apply_model({}, leaky), _functions(leaky), leaky.tag_values[0]


def test_coverage_check_judges_a_mixed_column_from_its_live_classes_without_the_marker():
    # Regression (PR #223 review): deleting the marker hid the mixed column.
    cfg, functions, value = _hand_edited()
    blocked = ValidationResult()
    validate_coverage_gate(cfg, functions, "", blocked, exposure={"column_classes": LIVE})
    (error,) = blocked.errors
    assert "multi-class columns keeping a partial treatment" in error
    assert f"{NOTES} (classes keep email_partial, name_partial, phone_partial; gr_treatment {value}" in error
    # Column names from UC may differ in case from the config.
    upper = ValidationResult()
    validate_coverage_gate(cfg, functions, "", upper, exposure={"column_classes": {NOTES.upper(): LIVE[NOTES]}})
    assert len(upper.errors) == 1
    # The live classes of a single-format column don't block it.
    single = ValidationResult()
    validate_coverage_gate(cfg, functions, "", single,
                           exposure={"column_classes": {NOTES: ["class.phone_number"]}})
    assert single.errors == []
    # The generated (redacted) model passes on the same live classes.
    good = _model({NOTES: LIVE[NOTES]})
    passed = ValidationResult()
    validate_coverage_gate(apply_model({}, good), _functions(good), "", passed, exposure={"column_classes": LIVE})
    assert passed.errors == []


def test_coverage_check_blocks_an_overridden_mixed_column_from_its_live_classes():
    # The pre-fix output of a phone_partial override: no marker, phone_partial.
    cfg, functions, value = _hand_edited()
    assert value.startswith("phone_partial__string__")
    result = ValidationResult()
    validate_coverage_gate(cfg, functions, "", result, exposure={"column_classes": LIVE})
    assert any(NOTES in error and "expected redact" in error for error in result.errors)


ROOT = Path(__file__).resolve().parents[2]


def _layer(tmp_path, target, env_text):
    env_dir = tmp_path / "aws" / "envs" / "dev"
    env_dir.mkdir(parents=True, exist_ok=True)
    (env_dir / "env.auto.tfvars").write_text(env_text)
    # A runner that fails proves the layer was skipped before Terraform.
    runner = tmp_path / "runner.sh"
    runner.write_text("#!/bin/sh\necho RUNNER-CALLED; exit 7\n")
    runner.chmod(0o755)
    from tests.script_stubs import without_ci

    environ = {k: v for k, v in without_ci(os.environ).items() if k not in {"MAKEFLAGS", "MAKELEVEL"}}
    return subprocess.run(
        ["make", "-f", str(ROOT / "aws" / "Makefile"), target, "LAYER=workspace", "TARGET_ENV=dev",
         f"LAYER_ENV_DIR={env_dir}", f"ROOT_RUNNER={runner}", f"SHARED_ROOT={ROOT / 'shared'}"],
        cwd=ROOT, env=environ, text=True, capture_output=True, timeout=60,
    )


@pytest.mark.parametrize("target", ["_plan-layer", "_apply-layer"])
def test_governance_only_env_skips_the_workspace_layer(tmp_path, target):
    from scripts import environment_lock

    # _apply-layer runs inside make rehearse, under its env lock (release_gate.py apply-guard).
    env_dir = tmp_path / "aws" / "envs" / "dev"
    env_dir.mkdir(parents=True)
    ok, _message, nonce = environment_lock.acquire(env_dir, os.getpid(), "rehearse")
    assert ok
    os.environ[environment_lock.nonce_var(env_dir)] = nonce  # as make's _ENV_LOCK exports it
    try:
        result = _layer(tmp_path, target, 'governance_mode = "deterministic"\nuc_tables = ["cat.sch.t"]\n')
    finally:
        os.environ.pop(environment_lock.nonce_var(env_dir), None)
        environment_lock.release_lock(env_dir, os.getpid())
    assert result.returncode == 0, result.stdout + result.stderr
    assert "governance-only (deterministic, no agents); GenieRails creates and grants nothing" in result.stdout
    assert "RUNNER-CALLED" not in result.stdout


@pytest.mark.parametrize("env_text", [
    'uc_tables = ["cat.sch.t"]\n',  # legacy
    'governance_mode = "deterministic"\ngenie_spaces = [{ name = "a", genie_space_id = "x", acl_groups = [] }]\n',
])
def test_legacy_or_agent_envs_still_reach_the_workspace_layer(tmp_path, env_text):
    result = _layer(tmp_path, "_plan-layer", env_text)
    assert "governance-only" not in result.stdout
