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

    def __init__(self, rows, *, row_filters=None, masks=None):
        self.rows = rows                      # [{"id": ..., "ssn": ...}, ...]
        self.row_filters = row_filters or {}  # tier -> predicate(row)
        self.masks = masks or {}              # tier -> {column: fn}
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
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"id": _mask, "ssn": _mask}}))
    [result] = _verify(tmp_path, _check())
    assert result.status == INCONCLUSIVE
    assert result.detail == (
        f"row-pairing key id is masked for {JUNIOR} on {TABLE}; "
        "choose a unique, non-null, unmasked key")


def test_a_masked_key_for_one_tier_still_fails_a_leak_in_another(tmp_path, warehouse):
    warehouse(FakeWarehouse(_unique_rows(), masks={JUNIOR: {"id": _mask, "ssn": _mask}}))
    [result] = _verify(tmp_path, _check(masked=(JUNIOR, SENIOR)))  # Senior: mask not applied
    assert result.status == FAIL
    assert "leaked the raw value" in result.detail


# ---------------------------------------------------------------------------
# row filters: tiers compare the admin's sampled rows
# ---------------------------------------------------------------------------
def test_a_row_filtered_tier_is_compared_on_the_shared_rows(tmp_path, warehouse):
    even = lambda r: int(r["id"][-4:]) % 2 == 0  # noqa: E731
    wh = warehouse(FakeWarehouse(_unique_rows(), row_filters={JUNIOR: even},
                                 masks={JUNIOR: {"ssn": _mask}}))
    [result] = _verify(tmp_path, _check())
    assert result.status == PASS
    # The 12 even keys among the admin's first 25, not Junior's own first 25.
    assert result.evidence["per_principal_compared"] == {JUNIOR: 12}
    junior_reads = [s for s in wh.statements if s[0] == JUNIOR]
    assert len(junior_reads) == 1 and " WHERE `id` IN (" in junior_reads[0][1]
    assert sorted(junior_reads[0][2].values()) == [f"KEY-{i:04d}" for i in range(1, 26)]


def test_a_tier_filtered_off_every_sampled_row_is_reported_not_skipped(tmp_path, warehouse):
    late = lambda r: int(r["id"][-4:]) > 25  # noqa: E731
    warehouse(FakeWarehouse(_unique_rows(), row_filters={SENIOR: late},
                            masks={JUNIOR: {"ssn": _mask}, SENIOR: {"ssn": _mask}}))
    [result] = _verify(tmp_path, _check(masked=(JUNIOR, SENIOR)))
    assert result.status == INCONCLUSIVE
    assert f"{SENIOR} sees none of the 25 rows of {TABLE} the admin baseline sampled" in result.detail


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
