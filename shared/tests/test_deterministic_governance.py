from pathlib import Path
import shutil
import subprocess

import pytest

from deterministic_governance import (
    AccessResolution,
    NEVER_RAW_TREATMENTS,
    PARTIAL_VERSIONS,
    resolve_precedence,
    validate_config,
)


def errors(**overrides):
    cfg = {"access_tier_groups": ["raw", "partial", "full"]}
    cfg.update(overrides)
    return validate_config(cfg)


def test_empty_legacy_config_is_valid():
    assert validate_config({}) == []


def test_raw_exempt_identities_must_be_verifiable_groups():
    assert errors(raw_exempt_principals=["etl_group"]) == []
    assert errors(raw_exempt_principals=["etl@example.com"])
    # Shape alone is not ambiguous: account groups may legitimately have a
    # UUID/hex display name. Live verification resolves that exact group name;
    # an application ID with no identically named group fails provisioning.
    assert errors(raw_exempt_principals=["12345678-1234-1234-1234-123456789abc"]) == []


def _terraform_variable_block(path: Path, name: str) -> str:
    source = path.read_text()
    start = source.index(f'variable "{name}" {{')
    depth = 0
    for index in range(start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start:index + 1]
    raise AssertionError(f"unterminated variable {name} in {path}")


@pytest.mark.parametrize("root_name", ["data_access", "workspace"])
def test_python_accepted_uuid_group_passes_terraform_validate_and_plan(tmp_path, root_name):
    terraform = shutil.which("terraform")
    if not terraform:
        pytest.skip("terraform is not installed")
    principal = "12345678-1234-1234-1234-123456789abc"
    assert errors(raw_exempt_principals=[principal]) == []
    root = Path(__file__).resolve().parents[1] / "roots" / root_name / "main.tf"
    fixture = tmp_path / root_name
    fixture.mkdir()
    (fixture / "main.tf").write_text(
        _terraform_variable_block(root, "raw_exempt_principals")
        + '\noutput "accepted" { value = var.raw_exempt_principals }\n')
    commands = [
        [terraform, "init", "-backend=false", "-input=false", "-no-color"],
        [terraform, "validate", "-no-color"],
        [terraform, "plan", "-refresh=false", "-lock=false", "-input=false", "-no-color",
         f'-var=raw_exempt_principals=["{principal}"]'],
    ]
    for command in commands:
        result = subprocess.run(command, cwd=fixture, text=True, capture_output=True, timeout=120)
        assert result.returncode == 0, result.stdout + result.stderr


def test_governance_mode_and_raw_exempt_principals():
    assert errors(governance_mode="deterministic", raw_exempt_principals=["etl"]) == []
    assert errors(governance_mode="future")
    assert errors(governance_mode=[])
    assert errors(raw_exempt_principals="etl")
    for principal in ("", " ", "\t"):
        assert errors(raw_exempt_principals=[principal])


def test_one_two_and_three_plus_tiers_are_valid():
    for tiers in (["a"], ["a", "b"], ["a", "b", "c"], ["a", "b", "c", "d"]):
        assert errors(access_tier_groups=tiers) == []


def test_tiers_are_strings_unique_and_nonempty():
    assert errors(access_tier_groups="a")
    assert errors(access_tier_groups=["a", 2])
    assert errors(access_tier_groups=["a", ""])
    assert errors(access_tier_groups=["a", "a"])


def test_treatment_versions_only_allows_partial_known_treatment_and_version():
    assert errors(treatment_versions={"email_partial": {"partial": "partial"}}) == []
    assert errors(treatment_versions={"ssn": {"partial": "last4"}}) == []
    for treatment in PARTIAL_VERSIONS:
        version = next(iter(PARTIAL_VERSIONS[treatment]))
        assert errors(treatment_versions={treatment: {"partial": version}}) == []
    assert errors(treatment_versions={"missing": {"partial": "partial"}})
    assert errors(treatment_versions={"email_partial": {"full": "redacted"}})
    assert errors(treatment_versions={"email_partial": {"partial": "missing"}})
    assert errors(treatment_versions={"email_partial": {"partial": "raw"}})
    assert errors(treatment_versions={"generic_partial": {"partial": "raw"}})
    for treatment in NEVER_RAW_TREATMENTS:
        result = errors(treatment_versions={treatment: {"partial": "raw"}})
        assert any("may not be raw" in error for error in result)


def test_tier_override_validates_treatment_group_access_and_existing_tier():
    assert errors(tier_access_overrides={"email_partial": {"partial": "raw"}}) == []
    assert errors(tier_access_overrides={"missing": {"partial": "raw"}})
    assert errors(tier_access_overrides={"email_partial": {"missing": "raw"}})
    assert errors(tier_access_overrides={"email_partial": {"partial": "sometimes"}})
    assert errors(access_tier_groups=["only"], tier_access_overrides={"email_partial": {"only": "full"}})
    assert errors(access_tier_groups=["raw", "full"], tier_access_overrides={"email_partial": {"full": "partial"}})
    for treatment in NEVER_RAW_TREATMENTS:
        assert errors(tier_access_overrides={treatment: {"partial": "raw"}})


def test_column_override_accepts_partial_or_treatment_and_refuses_full():
    assert errors(column_overrides={"cat.sch.tbl.col": {"partial": "prefix_3"}}) == []
    assert errors(column_overrides={"cat.sch.tbl.col": {"treatment": "redact"}}) == []
    assert errors(column_overrides={"bad": {"partial": "prefix_3"}})
    assert errors(column_overrides={"cat.sch.tbl.col": {"full": "redacted"}})
    assert errors(column_overrides={"cat.sch.tbl.col": {"partial": "missing"}})
    assert errors(column_overrides={"cat.sch.tbl.col": {"treatment": "missing"}})
    assert errors(column_overrides={"cat.sch.tbl.col": {"partial": "raw", "treatment": "redact"}})
    assert errors(column_overrides={"cat.sch.tbl.col": {"keep_current": True}}) == []
    assert errors(column_overrides={"cat.sch.tbl.col": {"keep_current": False}})


@pytest.mark.xfail(strict=True, reason="needs the column's live class.* tags, which reach config with deterministic policies in step 5")
def test_column_treatment_override_must_be_stricter_than_class_derived_treatment():
    assert errors(column_overrides={"cat.sch.tbl.col": {"treatment": "email_partial"}})


def test_row_filter_schema_groups_literals_and_conflicts():
    rule = {"table": "cat.sch.tbl", "column": "region", "values_by_group": {"partial": ["APAC"]}}
    assert errors(row_filters=[rule]) == []
    assert errors(row_filters=[{**rule, "extra": 1}])
    missing_table = {"column": "region", "values_by_group": {"partial": ["APAC"]}}
    assert errors(row_filters=[missing_table])
    assert errors(row_filters=[{**rule, "table": "bad"}])
    assert errors(row_filters=[{**rule, "table": "a..b"}])
    assert errors(row_filters=[{**rule, "values_by_group": {"missing": ["APAC"]}}])
    assert errors(row_filters=[{**rule, "values_by_group": {"partial": [1]}}])
    assert errors(row_filters=[rule, {**rule, "values_by_group": {"partial": ["EMEA"]}}])
    assert errors(row_filters=[rule, rule]) == []
    assert errors(row_filters=[{**rule, "values_by_group": {"raw": ["APAC"]}}])


def test_missing_acl_is_feature_gated_and_explicit_empty_is_allowed():
    spaces = [{"name": "X"}]
    assert errors(genie_spaces=spaces) == []
    result = errors(require_acl_groups=True, genie_spaces=spaces)
    assert result == ["agent X has no acl_groups — list the groups that may run it"]
    assert errors(require_acl_groups=True, genie_spaces=[{"name": "X", "acl_groups": []}]) == []
    assert errors(genie_spaces=[{"name": "X", "acl_groups": [1]}])
    assert errors(genie_spaces=[{"name": "X", "acl_groups": [""]}])
    assert errors(genie_spaces=[{"name": "X", "acl_groups": ["g", "g"]}])
    assert errors(genie_spaces=[{"name": "X", "delete": True}]) == []
    assert errors(genie_spaces=[{"name": "X", "delete": "yes"}])


def test_acknowledgement_environment_variable_formats():
    cfg = {"access_tier_groups": ["raw"]}
    assert validate_config(cfg, ack_unclassified="cat.sch.tbl.col") == []
    assert validate_config(cfg, ack_unclassified="cat.sch.tbl.a,cat.sch.tbl.b") == []
    assert validate_config(cfg, ack_unclassified="cat.sch.tbl")
    assert validate_config(cfg, ack_weaken="cat.sch.tbl.col:principal") == []
    assert validate_config(cfg, ack_weaken="cat.sch.tbl.col:team one") == []
    assert validate_config(cfg, ack_weaken="cat.sch.tbl.col")
    assert validate_config(cfg, ack_weaken="cat.sch.tbl.col:")


def test_precedence_resolver_selects_partial_version_only_after_access():
    args = dict(column="c.s.t.x", treatment="email_partial", group="t2", library_default="partial", access_tier_groups=["t1", "t2", "t3"])
    assert resolve_precedence(**args) == AccessResolution("partial", "partial")
    assert resolve_precedence(**args, treatment_versions={"email_partial": {"partial": "redacted"}}) == AccessResolution("partial", "redacted")
    assert resolve_precedence(**args, treatment_versions={"email_partial": {"partial": "redacted"}}, column_overrides={"c.s.t.x": {"partial": "prefix_3"}}) == AccessResolution("partial", "prefix_3")
    assert resolve_precedence(**args, column_overrides={"c.s.t.x": {"partial": "raw"}}) == AccessResolution("raw")


def test_tier_access_override_is_not_cancelled_by_version_selection():
    args = dict(column="c.s.t.x", treatment="email_partial", group="t2", library_default="partial", access_tier_groups=["t1", "t2", "t3"], treatment_versions={"email_partial": {"partial": "partial"}})
    assert resolve_precedence(**args, tier_access_overrides={"email_partial": {"t2": "raw"}}) == AccessResolution("raw")
    assert resolve_precedence(**args, tier_access_overrides={"email_partial": {"t2": "full"}}, column_overrides={"c.s.t.x": {"partial": "raw"}}) == AccessResolution("full")


def test_tier1_raw_tier3_full_outsider_full_and_most_privileged_membership_wins():
    args = dict(column="c.s.t.x", treatment="email_partial", library_default="partial", access_tier_groups=["t1", "t2", "t3"], treatment_versions={"email_partial": {"partial": "redacted"}})
    assert resolve_precedence(**args, group="t1") == AccessResolution("raw")
    assert resolve_precedence(**args, group="t3", column_overrides={"c.s.t.x": {"partial": "raw"}}) == AccessResolution("full")
    assert resolve_precedence(**args, group="outsider") == AccessResolution("full")
    assert resolve_precedence(**args, group=["t2", "t3"]) == AccessResolution("partial", "redacted")
    assert resolve_precedence(**args, group=["t1", "t3"]) == AccessResolution("raw")


def test_raw_view_precedence_never_raw_deployer_exempt_tier1_then_overrides():
    base = dict(column="c.s.t.x", treatment="email_partial", group="t2", library_default="partial", access_tier_groups=["t1", "t2", "t3"], principal="etl", deployer_principal="deployer", raw_exempt_principals=["etl"], tier_access_overrides={"email_partial": {"t2": "full"}})
    assert resolve_precedence(**base) == AccessResolution("raw")
    assert resolve_precedence(**{**base, "principal": "deployer"}) == AccessResolution("raw")
    assert resolve_precedence(**{**base, "principal": "viewer"}) == AccessResolution("full")
    never_raw = {**base, "treatment": "secret", "principal": "etl"}
    assert resolve_precedence(**never_raw) == AccessResolution("full")
    # Section 3: the deployer SP is not exempt from never-raw.
    assert resolve_precedence(**{**never_raw, "principal": "deployer"}) == AccessResolution("full")
    flagged = {**base, "treatment": "redact", "library_default": "redacted", "never_raw": True}
    for principal in ("etl", "deployer"):
        assert resolve_precedence(**{**flagged, "principal": principal}) == AccessResolution("full")
    assert resolve_precedence(**{**flagged, "principal": "viewer", "group": "t1"}) == AccessResolution("full")


def test_wrong_typed_nested_values_return_clean_errors():
    assert errors(treatment_versions={"ssn": {"partial": ["x"]}})
    assert errors(column_overrides={"a.b.c.d": {"partial": ["x"]}})
    assert errors(tier_access_overrides={"ssn": {"partial": ["raw"]}})
    assert errors(row_filters=[{"table": ["x"], "column": "r", "values_by_group": {}}])


def test_every_shared_treatment_fails_closed_by_tier_and_unknowns_are_full():
    for treatment in PARTIAL_VERSIONS:
        args = dict(column="c.s.t.x", treatment=treatment, library_default=next(iter(PARTIAL_VERSIONS[treatment])), access_tier_groups=["t1", "t2", "t3"])
        tier1 = resolve_precedence(**args, group="t1")
        assert tier1.access == ("full" if treatment in NEVER_RAW_TREATMENTS else "raw")
        assert resolve_precedence(**args, group="t3").access == "full"
        assert resolve_precedence(**args, group="outsider").access == "full"
    assert resolve_precedence(column="c.s.t.x", treatment="unknown", group="t1", library_default="raw", access_tier_groups=["t1", "t2", "t3"]) == AccessResolution("full")
    assert errors(treatment_versions={"unknown": {"partial": "redacted"}})
