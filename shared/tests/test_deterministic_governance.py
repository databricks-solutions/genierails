from deterministic_governance import resolve_precedence, validate_config


def errors(**overrides):
    cfg = {"access_tier_groups": ["raw", "partial", "full"]}
    cfg.update(overrides)
    return validate_config(cfg)


def test_empty_legacy_config_is_valid():
    assert validate_config({}) == []


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


def test_tier_override_validates_treatment_group_access_and_existing_tier():
    assert errors(tier_access_overrides={"email_partial": {"partial": "raw"}}) == []
    assert errors(tier_access_overrides={"missing": {"partial": "raw"}})
    assert errors(tier_access_overrides={"email_partial": {"missing": "raw"}})
    assert errors(tier_access_overrides={"email_partial": {"partial": "sometimes"}})
    assert errors(access_tier_groups=["only"], tier_access_overrides={"email_partial": {"only": "full"}})
    assert errors(access_tier_groups=["raw", "full"], tier_access_overrides={"email_partial": {"full": "partial"}})


def test_column_override_accepts_partial_or_treatment_and_refuses_full():
    assert errors(column_overrides={"cat.sch.tbl.col": {"partial": "prefix_3"}}) == []
    assert errors(column_overrides={"cat.sch.tbl.col": {"treatment": "redact"}}) == []
    assert errors(column_overrides={"bad": {"partial": "prefix_3"}})
    assert errors(column_overrides={"cat.sch.tbl.col": {"full": "redacted"}})
    assert errors(column_overrides={"cat.sch.tbl.col": {"partial": "missing"}})
    assert errors(column_overrides={"cat.sch.tbl.col": {"treatment": "missing"}})
    assert errors(column_overrides={"cat.sch.tbl.col": {"partial": "raw", "treatment": "redact"}})


def test_row_filter_schema_groups_literals_and_conflicts():
    rule = {"table": "cat.sch.tbl", "column": "region", "values_by_group": {"partial": ["APAC"]}}
    assert errors(row_filters=[rule]) == []
    assert errors(row_filters=[{**rule, "extra": 1}])
    assert errors(row_filters=[{**rule, "table": "bad"}])
    assert errors(row_filters=[{**rule, "values_by_group": {"missing": ["APAC"]}}])
    assert errors(row_filters=[{**rule, "values_by_group": {"partial": [1]}}])
    assert errors(row_filters=[rule, {**rule, "values_by_group": {"partial": ["EMEA"]}}])
    assert errors(row_filters=[rule, rule]) == []


def test_missing_acl_is_feature_gated_and_explicit_empty_is_allowed():
    spaces = [{"name": "X"}]
    assert errors(genie_spaces=spaces) == []
    result = errors(require_acl_groups=True, genie_spaces=spaces)
    assert result == ["agent X has no acl_groups — list the groups that may run it"]
    assert errors(require_acl_groups=True, genie_spaces=[{"name": "X", "acl_groups": []}]) == []
    assert errors(genie_spaces=[{"name": "X", "acl_groups": [1]}])


def test_precedence_resolver_each_level():
    args = dict(column="c.s.t.x", treatment="email_partial", group="analysts", library_default="default")
    assert resolve_precedence(**args) == "default"
    assert resolve_precedence(**args, tier_access_overrides={"email_partial": {"analysts": "raw"}}) == "raw"
    assert resolve_precedence(**args, tier_access_overrides={"email_partial": {"analysts": "raw"}}, treatment_versions={"email_partial": {"partial": "partial"}}) == "partial"
    assert resolve_precedence(**args, tier_access_overrides={"email_partial": {"analysts": "raw"}}, treatment_versions={"email_partial": {"partial": "partial"}}, column_overrides={"c.s.t.x": {"partial": "prefix_3"}}) == "prefix_3"
    assert resolve_precedence(**args, column_overrides={"c.s.t.x": {"treatment": "redact"}}) == "redact"
