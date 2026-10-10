"""verify-access must not pass when its row-pairing key can't pair rows.

Each tier's rows are paired with the admin baseline's by one key column. A
repeated or NULL key lets two tiers compare *different* rows under the same key,
so a mask that is not applied can look applied (a false PASS). These tests run
the real live orchestrator against a fake SQL warehouse (no workspace): it
applies per-tier row filters and column masks, and — like a real warehouse —
returns rows that tie on the ORDER BY key in no fixed order across principals.
"""
import hashlib
import json
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import verify_effective_access as vea  # noqa: E402
from verify_effective_access import (  # noqa: E402
    DEFAULT_ADMIN_TIER,
    INCONCLUSIVE,
    FAIL,
    PASS,
    ColumnMaskCheck,
    TestPrincipal as VerificationPrincipal,
    VerificationSpec,
    evaluate_column_mask_check,
    main,
    verify_effective_access_live,
)

TABLE = "cat.sch.customers"
JUNIOR = "Junior_Analyst"
SENIOR = "Senior_Analyst"


def _mask(_value):
    return "XXX-MASKED"


class FakeWarehouse:
    """Just enough SQL for the queries verify_effective_access issues."""

    def __init__(self, rows, *, row_filters=None, masks=None, tags=None,
                 hide_masks_from_metadata=False, metadata_error=None,
                 table_tags=(), table_tags_error=None):
        self.rows = rows                      # [{"id": ..., "ssn": ...}, ...]
        self.row_filters = row_filters or {}  # tier -> predicate(row)
        self.masks = masks or {}              # tier -> {column: fn}
        self.tags = tags or {}                # column -> [(tag_name, tag_value), ...]
        self.hide_masks_from_metadata = hide_masks_from_metadata
        self.metadata_error = metadata_error
        self.table_tags = list(table_tags)    # [(tag_name, tag_value), ...]
        self.table_tags_error = table_tags_error
        self.statements = []                  # (tier, sql, params)

    def view(self, tier):
        rows = [r for r in self.rows if self.row_filters.get(tier, lambda r: True)(r)]
        masks = self.masks.get(tier, {})
        out = [{c: masks[c](v) if c in masks else v for c, v in r.items()} for r in rows]
        # Rows tying on the sort key come back in no fixed order: the admin gets
        # them in one order, every other principal in the reverse.
        return out if tier == DEFAULT_ADMIN_TIER else out[::-1]

    @staticmethod
    def _keep(where_col, in_list, params):
        if where_col is None:
            return lambda r: True
        wanted = {params[name.strip().lstrip(":")] for name in in_list.split(",")}
        return lambda r: r[where_col] is not None and str(r[where_col]) in wanted

    def run(self, tier, sql, params):
        params = params or {}
        self.statements.append((tier, sql, dict(params)))
        cell = lambda v: None if v is None else str(v)  # noqa: E731 — data_array is strings
        m = re.fullmatch(r"SELECT (COUNT\(\*\)|tag_name, tag_value) FROM "
                         r"system\.information_schema\.(column_masks|column_tags|table_tags) .*", sql)
        if m:
            if self.metadata_error:
                raise RuntimeError(self.metadata_error)
            if m.group(2) == "table_tags":
                if self.table_tags_error:
                    raise RuntimeError(self.table_tags_error)
                return [list(t) for t in self.table_tags]
            column = params["k"]
            if m.group(2) == "column_tags":
                tags = self.tags.get(column, [])
                return [list(t) for t in tags] if m.group(1) != "COUNT(*)" else [[str(len(tags))]]
            masked = {c for cols in self.masks.values() for c in cols}
            return [["0" if self.hide_masks_from_metadata else str(int(column in masked))]]
        m = re.fullmatch(
            r"SELECT `(\w+)`, `(\w+)` FROM (\S+)(?: WHERE `(\w+)` IN \(([^)]*)\))? "
            r"ORDER BY (`\w+`|`\w+` IS NULL DESC, xxhash64\(:salt, `\w+`\)) LIMIT (\d+)", sql)
        if m:
            key, col, _, where_col, in_list, order, limit = m.groups()
            rows = [r for r in self.view(tier) if self._keep(where_col, in_list, params)(r)]
            if "xxhash64" in order:
                # NULLs first, then a salted hash of the key; repeats stay adjacent.
                rows.sort(key=lambda r: (r[key] is not None, hashlib.sha256(
                    f"{params['salt']}|{r[key]}".encode()).hexdigest()))
            else:
                rows.sort(key=lambda r: (r[key] is not None, str(r[key])))  # NULLs first; stable
            return [[cell(r[key]), cell(r[col])] for r in rows[: int(limit)]]
        m = re.fullmatch(
            r"SELECT COUNT\(\*\), COUNT\(DISTINCT `(\w+)`\), COUNT\(`\w+`\) FROM (\S+) "
            r"WHERE `(\w+)` IN \(([^)]*)\)", sql)
        if m:
            key, _, where_col, in_list = m.groups()
            rows = [r for r in self.view(tier) if self._keep(where_col, in_list, params)(r)]
            vals = [r[key] for r in rows]
            return [[str(len(rows)), str(len({v for v in vals if v is not None})),
                     str(sum(v is not None for v in vals))]]
        m = re.fullmatch(r"SELECT COUNT\(\*\) FROM (\S+)(?: WHERE `(\w+)` IN \(([^)]*)\))?", sql)
        if m:
            _, where_col, in_list = m.groups()
            return [[str(sum(1 for r in self.view(tier) if self._keep(where_col, in_list, params)(r)))]]
        raise AssertionError(f"fake warehouse cannot run: {sql}")


@pytest.fixture
def warehouse(monkeypatch):
    holder = {}

    class FakeVerifier(vea.EffectiveAccessVerifier):
        def resolve_principal_groups(self, memberships):
            return list(memberships)

        def provision_principal(self, tier, memberships=None, *, resolved_groups=None):
            return VerificationPrincipal(tier, f"verify-{tier}", f"app-{tier}", "secret", f"sp-{tier}")

        def grant_warehouse_use(self, principal):
            pass

        def deprovision_principal(self, principal):
            pass

        def _ws_for(self, principal):
            return principal.tier

        def run_query(self, ws, sql, parameters=None):
            return holder["wh"].run(ws, sql.strip(), parameters)

    monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
    monkeypatch.setenv("GENIERAILS_VERIFY_PROPAGATION_SLEEP", "0")
    monkeypatch.setenv("GENIERAILS_VERIFY_SAMPLE_SALT", SALT)
    monkeypatch.setattr(vea, "load_auth", lambda path: {
        "host": "h", "client_id": "admin-app", "client_secret": "s"})
    monkeypatch.setattr(vea, "EffectiveAccessVerifier", FakeVerifier)

    def install(wh):
        holder["wh"] = wh
        return wh
    install.current = None
    return install


SALT = "test-salt"


def _hash(key):
    return hashlib.sha256(f"{SALT}|{key}".encode()).hexdigest()


def _sampled(wh, tier):
    """The keys ``tier``'s own (salted) sample returned."""
    [(_, _, params)] = [s for s in wh.statements if s[0] == tier and "xxhash64" in s[1]]
    view = sorted((r for r in wh.view(tier)), key=lambda r: (r["id"] is not None, _hash(r["id"])))
    return {r["id"] for r in view[:25]}


def _read_keys(wh, tier):
    """The key set ``tier`` read back (the union of every tier's sample)."""
    [(_, _, params)] = [s for s in wh.statements if s[0] == tier and " IN (" in s[1]]
    return set(params.values())


def _admin():
    return VerificationPrincipal(DEFAULT_ADMIN_TIER, "admin", "admin-app", "s")


def _verifier():
    return vea.EffectiveAccessVerifier({"host": "h", "client_id": "c", "client_secret": "s"},
                                       warehouse_id="wh-1")


def _check(masked=(JUNIOR,), unmasked=(DEFAULT_ADMIN_TIER,), column="ssn"):
    return ColumnMaskCheck(TABLE, column, "id", tuple(masked), tuple(unmasked), "mask_ssn")


def _verify(tmp_path, *checks):
    return verify_effective_access_live(
        VerificationSpec(column_masks=list(checks)), tmp_path / "auth.auto.tfvars",
        warehouse_id="wh-1",
    ).results


def _unique_rows(n=30):
    return [{"id": f"KEY-{i:04d}", "ssn": f"SSN-{i:04d}-RAW"} for i in range(1, n + 1)]


# ---------------------------------------------------------------------------
# (b) a non-unique key must not let an unapplied mask pass
# ---------------------------------------------------------------------------
def test_duplicate_keys_with_an_unapplied_mask_do_not_pass(tmp_path, warehouse):
    # Every key names two rows; the mask is NOT applied to Junior_Analyst.
    rows = [{"id": f"KEY-{i:04d}", "ssn": f"SSN-{i:04d}-{n}"} for i in range(1, 31) for n in "AB"]
    warehouse(FakeWarehouse(rows))
    [result] = _verify(tmp_path, _check())
    assert result.status == INCONCLUSIVE
    assert "row-pairing key id is not unique" in result.detail
    assert "choose a unique, non-null, unmasked key" in result.detail


def test_a_repeat_of_a_sampled_key_outside_the_sample_is_caught(warehouse):
    # The 25 sampled keys are unique in the sample; key 25 repeats elsewhere.
    rows = _unique_rows(25) + [{"id": "KEY-0025", "ssn": "SSN-0025-OTHER"}] + _unique_rows(40)[25:]
    wh = warehouse(FakeWarehouse(rows))
    keys = [f"KEY-{i:04d}" for i in range(1, 26)]
    problem = _verifier().prove_key_unique(_admin(), _check(), keys)
    assert problem.startswith("row-pairing key id is not unique / has NULLs")
    assert "25 sampled keys match 26 rows" in problem
    assert [t for t, _, _ in wh.statements] == [DEFAULT_ADMIN_TIER]


def test_the_sampled_rows_cover_a_repeated_key_together(tmp_path, warehouse):
    # Repeats of a key hash alike, so a sample takes them together.
    rows = _unique_rows(60) + [{"id": f"KEY-{i:04d}", "ssn": f"SSN-{i:04d}-OTHER"} for i in range(1, 61)]
    warehouse(FakeWarehouse(rows))
    [result] = _verify(tmp_path, _check())
    assert result.status == INCONCLUSIVE
    assert "row-pairing key id is not unique (" in result.detail


# ---------------------------------------------------------------------------
# (c) NULL keys must not pass
# ---------------------------------------------------------------------------
def test_null_keys_with_an_unapplied_mask_do_not_pass(tmp_path, warehouse):
    rows = [{"id": None, "ssn": f"SSN-{i:04d}-RAW"} for i in range(30)] + _unique_rows(5)
    warehouse(FakeWarehouse(rows))
    [result] = _verify(tmp_path, _check())
    assert result.status == INCONCLUSIVE
    assert "row-pairing key id has NULLs" in result.detail


# ---------------------------------------------------------------------------
# a masked key column gets a clear message, and doesn't hide a leak elsewhere
# ---------------------------------------------------------------------------
def test_a_masked_key_column_is_reported_clearly(tmp_path, warehouse):
    wh = warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"id": _mask, "ssn": _mask}}))
    [result] = _verify(tmp_path, _check())
    assert result.status == INCONCLUSIVE
    assert result.detail == (
        f"row-pairing key id may be masked for {JUNIOR}, {DEFAULT_ADMIN_TIER} on {TABLE} "
        "(it has 1 column mask(s)); choose a unique, non-null, unmasked key")
    assert not [s for s in wh.statements if TABLE.split(".")[-1] in s[1]]  # no row read


def test_a_masked_key_blocks_even_when_another_tier_leaks(tmp_path, warehouse):
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"id": _mask, "ssn": _mask}}))
    [result] = _verify(tmp_path, _check(masked=(JUNIOR, SENIOR)))  # Senior: mask not applied
    assert result.status == INCONCLUSIVE
    assert "row-pairing key id may be masked" in result.detail


def test_a_key_permuting_mask_with_an_unapplied_value_mask_does_not_pass(tmp_path, warehouse):
    # Junior sees every key shifted onto the next row's key (still unique and
    # overlapping), and the ssn mask is NOT applied to it.
    ids = [r["id"] for r in _unique_rows(60)]
    shift = dict(zip(ids, ids[1:] + ids[:1]))
    warehouse(FakeWarehouse(_unique_rows(60), masks={JUNIOR: {"id": shift.get}}))
    [result] = _verify(tmp_path, _check())
    assert result.status == INCONCLUSIVE
    assert f"row-pairing key id may be masked for {JUNIOR}, {DEFAULT_ADMIN_TIER}" in result.detail


def test_an_admin_masked_key_does_not_pass(tmp_path, warehouse):
    ids = [r["id"] for r in _unique_rows(60)]
    shift = dict(zip(ids, ids[1:] + ids[:1]))
    # The admin (the only raw baseline) sees permuted keys; Junior's ssn mask
    # is NOT applied.
    warehouse(FakeWarehouse(_unique_rows(60), masks={DEFAULT_ADMIN_TIER: {"id": shift.get}}))
    [result] = _verify(tmp_path, _check())
    assert result.status == INCONCLUSIVE
    assert "row-pairing key id may be masked" in result.detail


CLASS_TAGS = {"id": [("class.customer_id", ""), ("class.identifier", "")]}


def _mask_config(match_condition="hasTagValue('pii', 'ssn')", **policy):
    return {
        "fgac_policies": [{"name": "mask_ssn", "policy_type": "POLICY_TYPE_COLUMN_MASK",
                           "catalog": "cat", "to_principals": [JUNIOR],
                           "match_condition": match_condition, **policy}],
        "tag_assignments": [{"entity_type": "columns", "entity_name": f"{TABLE}.ssn",
                             "tag_key": "pii", "tag_value": "ssn"}],
    }


def _verify_with_config(tmp_path, mask_config, *checks):
    return verify_effective_access_live(
        VerificationSpec(column_masks=list(checks), mask_config=mask_config),
        tmp_path / "auth.auto.tfvars", warehouse_id="wh-1",
    ).results


def test_a_class_tagged_key_no_mask_policy_matches_passes(tmp_path, warehouse):
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}, tags=CLASS_TAGS))
    [result] = _verify_with_config(tmp_path, _mask_config(), _check())
    assert result.status == PASS, result.detail


@pytest.mark.parametrize("policy", [
    {"match_condition": "hasTag('class.identifier')"},
    {"match_condition": "hasTagValue('pii', 'ssn') OR hasTag('class.customer_id')"},
])
def test_a_key_tag_a_mask_policy_matches_is_inconclusive(tmp_path, warehouse, policy):
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}, tags=CLASS_TAGS))
    [result] = _verify_with_config(tmp_path, _mask_config(**policy), _check())
    assert result.status == INCONCLUSIVE
    assert result.detail == (
        f"row-pairing key id may be masked for {JUNIOR}, {DEFAULT_ADMIN_TIER} on {TABLE} "
        "(it has 2 column tag(s) a column-mask policy matches); "
        "choose a unique, non-null, unmasked key")


@pytest.mark.parametrize("policy", [
    {"match_condition": "hasTag('class.identifier')", "catalog": "other_catalog"},
    {"match_condition": "hasTag('class.identifier')", "except_principals": [JUNIOR]},
    {"match_condition": "hasTag('class.identifier') AND hasTagValue('pii', 'ssn')"},
])
def test_a_key_tag_no_policy_resolves_for_passes(tmp_path, warehouse, policy):
    # Another catalog's policy, a policy excepting all its targets, and a
    # condition the key's tags don't fully satisfy all leave the key unmasked.
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}, tags=CLASS_TAGS))
    cfg = _mask_config(**policy)
    cfg["fgac_policies"].append(_mask_config()["fgac_policies"][0] | {"name": "mask_ssn_2"})
    [result] = _verify_with_config(tmp_path, cfg, _check())
    assert result.status == PASS, result.detail


def test_a_live_column_mask_on_the_key_is_inconclusive(tmp_path, warehouse):
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask, "id": lambda v: v}}))
    [result] = _verify_with_config(tmp_path, _mask_config(), _check())
    assert result.status == INCONCLUSIVE
    assert "(it has 1 column mask(s))" in result.detail


def test_a_tagged_key_without_mask_policies_to_check_fails_closed(tmp_path, warehouse):
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}, tags=CLASS_TAGS))
    [result] = _verify(tmp_path, _check())   # e.g. a --spec run: no config
    assert result.status == INCONCLUSIVE
    assert "no mask policies were given to check them against" in result.detail


def test_a_key_tag_with_unreadable_mask_policies_fails_closed(tmp_path, warehouse):
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}, tags=CLASS_TAGS))
    [result] = _verify_with_config(tmp_path, _mask_config("has_tag_value('pii', 'ssn')"), _check())
    assert result.status == INCONCLUSIVE
    assert "the mask policies can't be checked" in result.detail


@pytest.mark.parametrize("table_tags,status", [
    ([("domain", "customer")], INCONCLUSIVE),   # the when_condition holds: key masked
    ([("domain", "orders")], PASS),             # it doesn't: the policy can't reach the key
    ([], PASS),
])
def test_a_when_condition_is_judged_on_the_live_table_tags(tmp_path, warehouse, table_tags, status):
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}, tags=CLASS_TAGS,
                            table_tags=table_tags))
    cfg = _mask_config()
    cfg["fgac_policies"].append({
        "name": "mask_ids", "policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "cat",
        "to_principals": [JUNIOR], "match_condition": "hasTag('class.identifier')",
        "when_condition": "hasTagValue('domain', 'customer')"})
    [result] = _verify_with_config(tmp_path, cfg, _check())
    assert result.status == status, result.detail
    if status == INCONCLUSIVE:
        assert "column tag(s) a column-mask policy matches" in result.detail


def test_unreadable_table_tags_fail_closed(tmp_path, warehouse):
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}, tags=CLASS_TAGS,
                            table_tags_error="PERMISSION_DENIED: table_tags"))
    cfg = _mask_config(when_condition="hasTagValue('domain', 'customer')")
    [result] = _verify_with_config(tmp_path, cfg, _check())
    assert result.status == INCONCLUSIVE
    assert "could not read its column masks/tags" in result.detail


def test_table_tags_are_read_only_when_a_policy_needs_them(tmp_path, warehouse):
    wh = warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}, tags=CLASS_TAGS,
                                 table_tags_error="PERMISSION_DENIED: table_tags"))
    [result] = _verify_with_config(tmp_path, _mask_config(), _check())
    assert result.status == PASS, result.detail
    assert not [s for s in wh.statements if "table_tags" in s[1]]


@pytest.mark.parametrize("unrelated", [
    {"catalog": "other_catalog"},           # another catalog's policy
    {"except_principals": [JUNIOR]},        # every target excepted
])
def test_an_unrelated_when_condition_policy_reads_no_table_tags(tmp_path, warehouse, unrelated):
    wh = warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}, tags=CLASS_TAGS,
                                 table_tags_error="PERMISSION_DENIED: table_tags"))
    cfg = _mask_config()
    cfg["fgac_policies"].append(cfg["fgac_policies"][0] | {
        "name": "mask_ids", "match_condition": "hasTag('class.identifier')",
        "when_condition": "hasTagValue('domain', 'customer')", **unrelated})
    [result] = _verify_with_config(tmp_path, cfg, _check())
    assert result.status == PASS, result.detail
    assert not [s for s in wh.statements if "table_tags" in s[1]]


def test_a_relevant_when_condition_policy_reads_table_tags_and_fails_closed(tmp_path, warehouse):
    wh = warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}, tags=CLASS_TAGS,
                                 table_tags_error="PERMISSION_DENIED: table_tags"))
    cfg = _mask_config()
    cfg["fgac_policies"].append(cfg["fgac_policies"][0] | {
        "name": "mask_ids", "catalog": "CAT", "match_condition": "hasTag('class.identifier')",
        "when_condition": "hasTagValue('domain', 'customer')"})
    [result] = _verify_with_config(tmp_path, cfg, _check())
    assert result.status == INCONCLUSIVE
    assert "could not read its column masks/tags" in result.detail
    assert [t for t, sql, _ in wh.statements if "table_tags" in sql] == [DEFAULT_ADMIN_TIER]


def test_mask_policies_use_table_tags_is_scoped_like_the_matcher():
    policy = {"name": "m", "policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "cat",
              "to_principals": ["g"], "when_condition": "hasTag('domain')"}
    use = lambda **kw: vea.mask_policies_use_table_tags(  # noqa: E731
        {"fgac_policies": [policy | kw]}, TABLE)
    assert use()
    assert use(catalog="CAT")                               # catalog names are case-insensitive
    assert not use(catalog="other")
    assert not use(except_principals=["g"])
    assert not use(when_condition="")
    assert not use(policy_type="POLICY_TYPE_ROW_FILTER")


def test_tfvars_and_live_paths_refuse_a_key_with_the_same_message(tmp_path, warehouse):
    tfvars = tmp_path / "abac.auto.tfvars"
    tfvars.write_text('''
tag_assignments = [
  { entity_type = "columns", entity_name = "cat.sch.customers.ssn", tag_key = "pii", tag_value = "ssn" },
  { entity_type = "columns", entity_name = "cat.sch.customers.id", tag_key = "class.customer_id", tag_value = "yes" },
  { entity_type = "columns", entity_name = "cat.sch.customers.id", tag_key = "class.identifier", tag_value = "yes" },
]
fgac_policies = [
  { name = "mask_ssn", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "cat",
    to_principals = ["Junior_Analyst"], match_condition = "hasTagValue('pii', 'ssn')" },
  { name = "mask_ids", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "cat",
    to_principals = ["Junior_Analyst"], match_condition = "hasTag('class.identifier')" },
]
''')
    with pytest.raises(ValueError) as exc:
        vea.load_spec_from_tfvars(tfvars, key_column="id")
    # The same key and tags, live, against the same policies.
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}, tags=CLASS_TAGS))
    cfg = _mask_config()
    cfg["fgac_policies"].append(cfg["fgac_policies"][0] | {
        "name": "mask_ids", "match_condition": "hasTag('class.identifier')"})
    [result] = _verify_with_config(tmp_path, cfg, _check(masked=(JUNIOR,)))
    assert str(exc.value) == "ERROR: " + result.detail


def test_from_tfvars_carries_the_mask_config(tmp_path):
    tfvars = tmp_path / "abac.auto.tfvars"
    tfvars.write_text('''
tag_assignments = [
  { entity_type = "columns", entity_name = "cat.sch.customers.ssn", tag_key = "pii", tag_value = "ssn" },
]
fgac_policies = [
  { name = "mask_ssn", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "cat",
    to_principals = ["Junior_Analyst"], match_condition = "hasTagValue('pii', 'ssn')",
    function_name = "mask_ssn" },
]
''')
    spec = vea.load_spec_from_tfvars(tfvars, key_column="id")
    assert spec.mask_config["fgac_policies"][0]["name"] == "mask_ssn"
    assert spec.mask_config["tag_assignments"][0]["entity_name"] == f"{TABLE}.ssn"


def test_unreadable_key_metadata_is_inconclusive(tmp_path, warehouse):
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}},
                            metadata_error="PERMISSION_DENIED: system.information_schema"))
    [result] = _verify(tmp_path, _check())
    assert result.status == INCONCLUSIVE
    assert "may be masked" in result.detail and "could not read its column masks/tags" in result.detail


def test_a_key_mask_missing_from_metadata_is_caught_by_the_admin_lookup(tmp_path, warehouse):
    hidden = lambda v: v.replace("KEY-", "ALIAS-")  # noqa: E731 — keys the admin never sees
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"id": hidden}},
                            hide_masks_from_metadata=True))
    [result] = _verify(tmp_path, _check())
    assert result.status == INCONCLUSIVE
    assert result.detail.startswith(f"row-pairing key id may be masked for {JUNIOR} on {TABLE}")


# ---------------------------------------------------------------------------
# row filters: tiers compare the admin's sampled rows
# ---------------------------------------------------------------------------
def test_a_row_filtered_tier_is_compared_on_the_shared_rows(tmp_path, warehouse):
    even = lambda r: int(r["id"][-4:]) % 2 == 0  # noqa: E731
    wh = warehouse(FakeWarehouse(_unique_rows(60), row_filters={JUNIOR: even},
                                 masks={JUNIOR: {"ssn": _mask}}))
    [result] = _verify(tmp_path, _check())
    assert result.status == PASS
    sample, read = [s for s in wh.statements if s[0] == JUNIOR]
    assert " WHERE `id` IN (" in read[1]
    read_keys = set(read[2].values())
    # Junior reads the admin's sample and its own; every even one is compared.
    assert {k for k in read_keys if int(k[-4:]) % 2 == 0} >= _sampled(wh, JUNIOR)
    assert result.evidence["per_principal_compared"] == {
        JUNIOR: sum(1 for k in read_keys if int(k[-4:]) % 2 == 0)}


def test_a_tier_filtered_off_the_admin_sample_is_compared_on_its_own_rows(tmp_path, warehouse):
    late = lambda r: int(r["id"][-4:]) > 25  # noqa: E731
    warehouse.current = warehouse(FakeWarehouse(_unique_rows(60), row_filters={SENIOR: late},
                            masks={JUNIOR: {"ssn": _mask}, SENIOR: {"ssn": _mask}}))
    wh = warehouse.current
    [result] = _verify(tmp_path, _check(masked=(JUNIOR, SENIOR)))
    assert result.status == PASS
    read_keys = _read_keys(wh, SENIOR)
    assert _sampled(wh, SENIOR) <= read_keys
    assert result.evidence["per_principal_compared"] == {
        JUNIOR: len(read_keys), SENIOR: sum(1 for k in read_keys if int(k[-4:]) > 25)}


def test_leaked_rows_only_a_tier_sees_do_not_pass(tmp_path, warehouse):
    # Junior sees rows 21-60; its mask covers only rows 1-25, so its rows
    # 26-60 leak, though the admin's sample has none of Junior's leaking rows.
    late = lambda r: int(r["id"][-4:]) > 20  # noqa: E731
    partial = lambda v: "XXX-MASKED" if int(v[4:8]) <= 25 else v  # noqa: E731
    wh = warehouse(FakeWarehouse(_unique_rows(60), row_filters={JUNIOR: late},
                                 masks={JUNIOR: {"ssn": partial}}))
    [result] = _verify(tmp_path, _check())
    assert result.status == FAIL
    leaking = {k for k in _read_keys(wh, JUNIOR) if int(k[-4:]) > 25}
    # Some leaking rows came only from Junior's own sample, and are counted.
    assert leaking & _sampled(wh, JUNIOR) - _sampled(wh, DEFAULT_ADMIN_TIER)
    assert result.evidence["leaks_by_principal"] == {JUNIOR: len(leaking)}


def test_rows_a_masked_tier_sees_without_a_baseline_do_not_pass(tmp_path, warehouse):
    # The only unmasked tier (Senior) can't see the rows Junior samples.
    early = lambda r: int(r["id"][-4:]) <= 25  # noqa: E731
    warehouse(FakeWarehouse(_unique_rows(60), row_filters={SENIOR: early, JUNIOR: lambda r: not early(r)},
                            masks={JUNIOR: {"ssn": _mask}, DEFAULT_ADMIN_TIER: {"ssn": _mask}}))
    [result] = _verify(tmp_path, _check(unmasked=(SENIOR,)))
    assert result.status == INCONCLUSIVE
    assert "no unmasked principal sees" in result.detail


# ---------------------------------------------------------------------------
# identifiers are validated and quoted, never interpolated raw
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("table,column,key", [
    ("cat.sch.t`; DROP TABLE x; --", "ssn", "id"),
    ("cat.sch.customers", "ssn` FROM other.t --", "id"),
    ("cat.sch.customers", "ssn", "id) OR (1=1"),
    ("cat.sch", "ssn", "id"),
    ("cat.sch.cust omers", "ssn", "id"),
])
def test_hostile_identifiers_are_refused_before_any_query(tmp_path, warehouse, table, column, key):
    wh = warehouse(FakeWarehouse(_unique_rows()))
    check = ColumnMaskCheck(table, column, key, (JUNIOR,), (DEFAULT_ADMIN_TIER,), "m")
    with pytest.raises(ValueError, match="unsafe SQL"):
        _verify(tmp_path, check)
    assert wh.statements == []


def test_hostile_identifiers_are_refused_by_the_cli(tmp_path, capsys):
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps({"column_masks": [{
        "table": "cat.sch.t`; DROP TABLE x", "column": "ssn", "key_column": "id",
        "masked_principals": [JUNIOR], "unmasked_principals": [DEFAULT_ADMIN_TIER]}]}))
    with pytest.raises(SystemExit, match="unsafe SQL identifier"):
        main(["--spec", str(spec), "--print-spec"])


def test_identifiers_are_backtick_quoted():
    assert vea.quote_identifier("my-catalog_1") == "`my-catalog_1`"
    assert vea.quote_table("my-cat.sch.t") == "`my-cat`.`sch`.`t`"
    for bad in ("a`b", "", "a b", "a.b", "-x"):
        with pytest.raises(ValueError):
            vea.quote_identifier(bad)


def test_a_tier_that_sees_no_rows_at_all_is_still_skipped(tmp_path, warehouse):
    warehouse(FakeWarehouse(_unique_rows(), row_filters={SENIOR: lambda r: False},
                            masks={JUNIOR: {"ssn": _mask}}))
    [result] = _verify(tmp_path, _check(masked=(JUNIOR, SENIOR)))
    assert result.status == PASS


# ---------------------------------------------------------------------------
# existing correct cases still pass; the proof runs once per table
# ---------------------------------------------------------------------------
def test_unique_keys_with_an_applied_mask_pass(tmp_path, warehouse):
    wh = warehouse(FakeWarehouse(
        [{**r, "email": f"user{i}@raw.example"} for i, r in enumerate(_unique_rows())],
        masks={JUNIOR: {"ssn": _mask, "email": _mask}}))
    results = _verify(tmp_path, _check(), _check(column="email", unmasked=(SENIOR, DEFAULT_ADMIN_TIER)))
    assert [r.status for r in results] == [PASS, PASS]
    assert results[0].evidence["masked_ok"] == 25
    proofs = [s for s in wh.statements if s[1].startswith("SELECT COUNT(*), COUNT(DISTINCT")]
    assert len(proofs) == 1
    # Key values are bound as parameters, never written into the SQL text.
    assert not any("KEY-" in sql for _, sql, _ in wh.statements)


def test_unapplied_mask_with_a_unique_key_still_fails(tmp_path, warehouse):
    warehouse(FakeWarehouse(_unique_rows()))
    [result] = _verify(tmp_path, _check())
    assert result.status == FAIL
    assert result.evidence["leaked_rows"] == 25


# ---------------------------------------------------------------------------
# (f) FAIL details, the summary and the result file never carry row values
# ---------------------------------------------------------------------------
def _run_main(tmp_path, *extra):
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps({"column_masks": [{
        "table": TABLE, "column": "ssn", "key_column": "id",
        "masked_principals": [JUNIOR], "unmasked_principals": [SENIOR, DEFAULT_ADMIN_TIER],
    }]}))
    result_file = tmp_path / "result.json"
    rc = main(["--spec", str(spec), "--auth-file", str(tmp_path / "auth.auto.tfvars"),
               "--warehouse-id", "wh-1", "--live", "--result-file", str(result_file), *extra])
    return rc, result_file.read_text()


def test_fail_output_redacts_row_and_key_values(tmp_path, warehouse, capsys):
    # Senior is masked differently (disagrees with admin), Junior not at all.
    warehouse(FakeWarehouse(_unique_rows(), masks={SENIOR: {"ssn": lambda v: v[:5] + "****"}}))
    rc, result_file = _run_main(tmp_path)
    out = capsys.readouterr()
    assert rc == 1
    for text in (out.out, out.err, result_file):
        assert "SSN-" not in text and "KEY-" not in text
    assert "[FAIL]" in out.out and "per principal" in out.out


def test_leak_detail_names_counts_and_key_column_only():
    check = _check()
    rows = {DEFAULT_ADMIN_TIER: {"KEY-1": "SSN-1-RAW"}, JUNIOR: {"KEY-1": "SSN-1-RAW"}}
    result = evaluate_column_mask_check(check, rows)
    assert result.status == FAIL
    assert "SSN-" not in result.detail and "KEY-" not in result.detail
    assert "by key id" in result.detail and JUNIOR in result.detail
    assert "SSN-" not in repr(result.evidence) and "KEY-" not in repr(result.evidence)


@pytest.mark.parametrize("require", [False, True])
def test_inconclusive_pairing_fails_the_run(tmp_path, warehouse, capsys, require):
    rows = [{"id": f"KEY-{i:04d}", "ssn": f"SSN-{i:04d}-{n}"} for i in range(1, 31) for n in "AB"]
    warehouse(FakeWarehouse(rows))
    rc, result_file = _run_main(tmp_path, *(["--require-mask-checks"] if require else []))
    assert rc == 1
    assert json.loads(result_file)["passed"] is False
    assert json.loads(result_file)["mask_checks_passed"] == 0
    assert "[INCONCLUSIVE]" in capsys.readouterr().out


def test_correct_case_passes_through_main(tmp_path, warehouse, capsys):
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}))
    rc, result_file = _run_main(tmp_path, "--require-mask-checks")
    assert rc == 0, capsys.readouterr().out
    assert json.loads(result_file)["mask_checks_passed_by_key"] == {"id": 1}


# ---------------------------------------------------------------------------
# the evaluator's in-sample check (pure)
# ---------------------------------------------------------------------------
def test_evaluator_rejects_a_repeated_key_in_fetched_rows():
    rows = {DEFAULT_ADMIN_TIER: [("1", "a"), ("1", "b")], JUNIOR: [("1", "x")]}
    result = evaluate_column_mask_check(_check(), rows)
    assert result.status == INCONCLUSIVE
    assert "is not unique" in result.detail


def test_evaluator_rejects_a_null_key():
    result = evaluate_column_mask_check(_check(), {DEFAULT_ADMIN_TIER: {None: "a"}, JUNIOR: {None: "x"}})
    assert result.status == INCONCLUSIVE
    assert "has NULLs" in result.detail


# ---------------------------------------------------------------------------
# the sample is spread across the table, reproducible from the logged salt,
# and its size is stated in the summary
# ---------------------------------------------------------------------------
def test_the_sample_is_spread_by_the_salt_not_the_lowest_keys(tmp_path, warehouse, monkeypatch):
    wh = warehouse(FakeWarehouse(_unique_rows(200), masks={JUNIOR: {"ssn": _mask}}))
    [result] = _verify(tmp_path, _check())
    assert result.status == PASS
    first = _sampled(wh, DEFAULT_ADMIN_TIER)
    assert first != {f"KEY-{i:04d}" for i in range(1, 26)}
    assert max(int(k[-4:]) for k in first) > 100
    # Same salt, same sample; another salt, another sample.
    wh.statements.clear()
    _verify(tmp_path, _check())
    assert _sampled(wh, DEFAULT_ADMIN_TIER) == first
    monkeypatch.setenv("GENIERAILS_VERIFY_SAMPLE_SALT", "other-salt")
    wh.statements.clear()
    _verify(tmp_path, _check())
    [(_, _, params)] = [s for s in wh.statements if s[0] == DEFAULT_ADMIN_TIER and "xxhash64" in s[1]]
    assert params == {"salt": "other-salt"}


def test_a_fresh_salt_is_used_and_logged_when_none_is_set(tmp_path, warehouse, monkeypatch, capsys):
    monkeypatch.delenv("GENIERAILS_VERIFY_SAMPLE_SALT")
    wh = warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}))
    report = verify_effective_access_live(
        VerificationSpec(column_masks=[_check()]), tmp_path / "auth.auto.tfvars", warehouse_id="wh-1")
    [salt] = {s[2]["salt"] for s in wh.statements if "xxhash64" in s[1]}
    assert len(salt) == 16 and salt != SALT
    assert f"GENIERAILS_VERIFY_SAMPLE_SALT={salt}" in capsys.readouterr().out
    assert f"spread by salt {salt}" in report.summary()


def test_the_summary_states_the_sample_size(tmp_path, warehouse, capsys):
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}))
    rc, _ = _run_main(tmp_path)
    out = capsys.readouterr().out
    assert rc == 0
    assert "Sample: checked 25 sampled rows per tier per masked column" in out
    assert "a bounded sample, not every row" in out
    assert "KEY-" not in out and "SSN-" not in out


# ---------------------------------------------------------------------------
# long key lists are read in batches, never one oversized statement
# ---------------------------------------------------------------------------
def test_long_key_lists_are_batched(warehouse):
    wh = warehouse(FakeWarehouse(_unique_rows(250)))
    keys = [f"KEY-{i:04d}" for i in range(1, 251)]
    verifier, check = _verifier(), _check()
    rows = verifier.collect_column_values(_admin(), check, len(keys), keys)
    assert sorted(k for k, _ in rows) == keys
    assert verifier.prove_key_unique(_admin(), check, keys) == ""
    assert verifier.count_rows_with_keys(_admin(), check, keys) == 250
    assert wh.statements and max(len(p) for _, _, p in wh.statements) <= vea.KEY_PARAM_BATCH
    assert len(wh.statements) == 9  # 3 batches x 3 calls


def test_an_oversized_key_list_has_a_clear_error():
    with pytest.raises(ValueError, match=r"101 key values in one statement \(max 100\)"):
        vea._key_filter("id", [str(i) for i in range(101)])
