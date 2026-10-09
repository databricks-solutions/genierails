from deterministic_governance import NEVER_RAW_TREATMENTS, resolve_precedence, validate_config


def errors(**overrides):
    cfg = {"access_tier_groups": ["raw", "partial", "full"]}
    cfg.update(overrides)
    return validate_config(cfg)


def test_empty_legacy_config_is_valid():
    assert validate_config({}) == []


def test_governance_mode_raw_exempt_principals_and_hash_fallback():
    assert errors(governance_mode="deterministic", raw_exempt_principals=["etl"], hash_fallback="redact") == []
    assert errors(governance_mode="future")
    assert errors(governance_mode=[])
    assert errors(raw_exempt_principals="etl")
    assert errors(raw_exempt_principals=[""])
    assert errors(hash_fallback="raw")
    assert errors(hash_fallback=[])


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
    assert errors(treatment_versions={"missing": {"partial": "partial"}})
    assert errors(treatment_versions={"email_partial": {"full": "redacted"}})
    assert errors(treatment_versions={"email_partial": {"partial": "missing"}})
    for treatment in NEVER_RAW_TREATMENTS:
        result = errors(treatment_versions={treatment: {"partial": "raw"}})
        assert any("never-raw" in error for error in result)


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


def test_row_filter_schema_groups_literals_and_conflicts():
    rule = {"table": "cat.sch.tbl", "column": "region", "values_by_group": {"partial": ["APAC"]}}
    assert errors(row_filters=[rule]) == []
    assert errors(row_filters=[{**rule, "extra": 1}])
    missing_table = {"column": "region", "values_by_group": {"partial": ["APAC"]}}
    assert errors(row_filters=[missing_table])
    assert errors(row_filters=[{**rule, "table": "bad"}])
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


def test_precedence_resolver_each_level():
    args = dict(column="c.s.t.x", treatment="email_partial", group="analysts", library_default="default")
    assert resolve_precedence(**args) == "default"
    assert resolve_precedence(**args, tier_access_overrides={"email_partial": {"analysts": "raw"}}) == "raw"
    assert resolve_precedence(**args, tier_access_overrides={"email_partial": {"analysts": "raw"}}, treatment_versions={"email_partial": {"partial": "partial"}}) == "partial"
    assert resolve_precedence(**args, tier_access_overrides={"email_partial": {"analysts": "raw"}}, treatment_versions={"email_partial": {"partial": "partial"}}, column_overrides={"c.s.t.x": {"partial": "prefix_3"}}) == "prefix_3"
    assert resolve_precedence(**args, column_overrides={"c.s.t.x": {"treatment": "redact"}}) == "redact"


def test_raw_view_precedence_never_raw_deployer_exempt_tier1_then_overrides():
    base = dict(column="c.s.t.x", treatment="email_partial", group="analysts", library_default="partial", principal="etl", deployer_principal="deployer", raw_exempt_principals=["etl"], tier1_group="ops", tier_access_overrides={"email_partial": {"analysts": "full"}})
    assert resolve_precedence(**base) == "raw"
    assert resolve_precedence(**{**base, "principal": "ops"}) == "raw"
    assert resolve_precedence(**{**base, "principal": "deployer"}) == "raw"
    assert resolve_precedence(**{**base, "principal": "viewer"}) == "full"
    never_raw = {**base, "treatment": "secret", "principal": "etl"}
    assert resolve_precedence(**never_raw) == "full"
    assert resolve_precedence(column="c.s.t.x", treatment="secret", group="g", library_default="raw") == "full"
    # The deployer SP is the sole never-raw exception in section 3.
    assert resolve_precedence(**{**never_raw, "principal": "deployer"}) == "raw"
