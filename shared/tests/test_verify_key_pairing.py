"""verify-access must not pass when its row-pairing key can't pair rows.

Each tier's rows are paired with the admin baseline's by one key column. A
repeated or NULL key lets two tiers compare *different* rows under the same key,
so a mask that is not applied can look applied (a false PASS). These tests run
the real live orchestrator against a fake SQL warehouse (no workspace): it
applies per-tier row filters and column masks, and — like a real warehouse —
returns rows that tie on the ORDER BY key in no fixed order across principals.
"""
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
                 hide_masks_from_metadata=False, metadata_error=None):
        self.rows = rows                      # [{"id": ..., "ssn": ...}, ...]
        self.row_filters = row_filters or {}  # tier -> predicate(row)
        self.masks = masks or {}              # tier -> {column: fn}
        self.tags = tags or {}                # column -> number of tags
        self.hide_masks_from_metadata = hide_masks_from_metadata
        self.metadata_error = metadata_error
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
        m = re.fullmatch(r"SELECT COUNT\(\*\) FROM system\.information_schema\.(column_masks|column_tags) .*", sql)
        if m:
            if self.metadata_error:
                raise RuntimeError(self.metadata_error)
            column = params["k"]
            if m.group(1) == "column_tags":
                return [[str(self.tags.get(column, 0))]]
            masked = {c for cols in self.masks.values() for c in cols}
            return [["0" if self.hide_masks_from_metadata else str(int(column in masked))]]
        m = re.fullmatch(
            r"SELECT `(\w+)`, `(\w+)` FROM (\S+)(?: WHERE `(\w+)` IN \(([^)]*)\))? "
            r"ORDER BY `\w+` LIMIT (\d+)", sql)
        if m:
            key, col, _, where_col, in_list, limit = m.groups()
            rows = [r for r in self.view(tier) if self._keep(where_col, in_list, params)(r)]
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
        def provision_principal(self, tier):
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
    monkeypatch.setattr(vea, "load_auth", lambda path: {
        "host": "h", "client_id": "admin-app", "client_secret": "s"})
    monkeypatch.setattr(vea, "EffectiveAccessVerifier", FakeVerifier)

    def install(wh):
        holder["wh"] = wh
        return wh
    return install


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


def test_a_repeat_of_the_last_sampled_key_past_the_limit_is_caught(tmp_path, warehouse):
    # The 25 sampled keys are unique in the sample; key 25 repeats at row 26.
    rows = _unique_rows(25) + [{"id": "KEY-0025", "ssn": "SSN-0025-OTHER"}] + _unique_rows(40)[25:]
    wh = warehouse(FakeWarehouse(rows))
    [result] = _verify(tmp_path, _check())
    assert result.status == INCONCLUSIVE
    assert "row-pairing key id is not unique / has NULLs" in result.detail
    assert "25 sampled keys match 26 rows" in result.detail
    proofs = [s for s in wh.statements if s[1].startswith("SELECT COUNT(*), COUNT(DISTINCT")]
    assert [t for t, _, _ in proofs] == [DEFAULT_ADMIN_TIER]


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


def test_a_tagged_key_column_is_refused(tmp_path, warehouse):
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"ssn": _mask}}, tags={"id": 2}))
    [result] = _verify(tmp_path, _check())
    assert result.status == INCONCLUSIVE
    assert "may be masked" in result.detail and "2 column tag(s)" in result.detail


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
    # Junior's own first 25 (even) keys, all read back with the admin's sample.
    assert result.evidence["per_principal_compared"] == {JUNIOR: 25}
    junior_reads = [s for s in wh.statements if s[0] == JUNIOR]
    assert len(junior_reads) == 2 and " WHERE `id` IN (" in junior_reads[1][1]
    expected = {f"KEY-{i:04d}" for i in range(1, 26)} | {f"KEY-{i:04d}" for i in range(2, 51, 2)}
    assert set(junior_reads[1][2].values()) == expected


def test_a_tier_filtered_off_the_admin_sample_is_compared_on_its_own_rows(tmp_path, warehouse):
    late = lambda r: int(r["id"][-4:]) > 25  # noqa: E731
    warehouse(FakeWarehouse(_unique_rows(60), row_filters={SENIOR: late},
                            masks={JUNIOR: {"ssn": _mask}, SENIOR: {"ssn": _mask}}))
    [result] = _verify(tmp_path, _check(masked=(JUNIOR, SENIOR)))
    assert result.status == PASS
    # Junior reads the admin's rows 1-25 and Senior's own 26-50.
    assert result.evidence["per_principal_compared"] == {JUNIOR: 50, SENIOR: 25}


def test_leaked_rows_only_a_tier_sees_do_not_pass(tmp_path, warehouse):
    # Junior sees rows 21-60; its mask covers only rows 1-25, so rows 26-45 of
    # its own sample leak — none of them in the admin's sample (rows 1-25).
    late = lambda r: int(r["id"][-4:]) > 20  # noqa: E731
    partial = lambda v: "XXX-MASKED" if int(v[4:8]) <= 25 else v  # noqa: E731
    warehouse(FakeWarehouse(_unique_rows(60), row_filters={JUNIOR: late},
                            masks={JUNIOR: {"ssn": partial}}))
    [result] = _verify(tmp_path, _check())
    assert result.status == FAIL
    assert result.evidence["leaks_by_principal"] == {JUNIOR: 20}


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
