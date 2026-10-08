"""verify-access must not call a value the mask leaves unchanged a leak, and must
never let that exception produce a false PASS.

Found on the live AWS run: prod release failed on a correctly masked
date_of_birth column. mask_date_to_year maps a date to 1 January of its year,
so the one birth date already on 1 January looks the same masked or not; the
salted sample (#90) only sometimes includes it, so release failed at random.

Rows whose raw value the mask maps to itself (fixed points) are left out of the
comparison only when all of this holds, as the admin reads it live:
- the live mask on the column is exactly the configured function with no extra
  USING arguments (ABAC masks don't show in information_schema.column_masks, so
  it is the one live column-mask policy the column's live tags match, applying
  to every masked tier, with no directly attached mask);
- that function is a deterministic one-argument SQL function whose body calls
  only known caller-independent builtins (no current_user / is_member / session
  / time / random / UDF calls), read from information_schema.routines;
- per tier, at least FIXED_POINT_MIN_COMPARED rows are still compared and fixed
  points are at most FIXED_POINT_MAX_SHARE of its sampled rows.
Otherwise the strict rule stays: a masked value equal to the raw value is a
leak (FAIL), or the check is INCONCLUSIVE — never PASS.
"""

import json
import re
import sys
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED))

import verify_effective_access as vea  # noqa: E402
from verify_effective_access import (  # noqa: E402
    DEFAULT_ADMIN_TIER, FAIL, INCONCLUSIVE, PASS, ColumnMaskCheck, derive_spec_from_config,
    evaluate_column_mask_check, fixed_point_function_problem, live_mask_problem, load_spec_from_file,
)
from tests.test_verify_key_pairing import (  # noqa: E402,F401  (warehouse is a fixture)
    JUNIOR, SENIOR, TABLE, FakeWarehouse, _verify, warehouse,
)

YEAR_MASK = "cat.sch.mask_date_to_year"
YEAR_BODY = "CASE WHEN dt IS NULL THEN NULL ELSE MAKE_DATE(YEAR(dt), 1, 1) END"
TAG = ("gr_treatment", "date_year")


def _year(value):
    return None if value is None else f"{value[:4]}-01-01"


def _check(mask_function=YEAR_MASK, masked=(JUNIOR,)):
    return ColumnMaskCheck(TABLE, "dob", "id", tuple(masked), (DEFAULT_ADMIN_TIER,),
                           "gr_mask_date_year", mask_function)


# ── the evaluator (pure) ────────────────────────────────────────────────────

def _rows(n, fixed=()):
    """n raw dates, plus their year-masked values; keys in ``fixed`` are 1 January."""
    raw = {f"k{i:03d}": (f"{1900 + i}-01-01" if i in fixed else f"{1900 + i}-03-22") for i in range(n)}
    return raw, {k: _year(v) for k, v in raw.items()}


def _evaluate(raw, masked, fixed_keys=None):
    values = {DEFAULT_ADMIN_TIER: list(raw.items()), JUNIOR: list(masked.items())}
    return evaluate_column_mask_check(_check(), values, fixed_point_keys=fixed_keys)


def test_without_fixed_point_exclusion_an_unchanged_value_is_a_leak():
    raw, masked = _rows(25, fixed={0})
    result = _evaluate(raw, masked)
    assert result.status == FAIL and "1 row(s) leaked" in result.detail


def test_one_fixed_point_in_25_rows_is_left_out():
    raw, masked = _rows(25, fixed={0})
    result = _evaluate(raw, masked, {"k000"})
    assert result.status == PASS, result.detail
    assert result.evidence["fixed_point_rows"] == 1 and result.evidence["masked_ok"] == 24


def test_a_real_leak_still_fails_beside_a_fixed_point():
    raw, masked = _rows(25, fixed={0})
    masked["k005"] = raw["k005"]
    result = _evaluate(raw, masked, {"k000"})
    assert result.status == FAIL and "1 row(s) leaked" in result.detail


def test_near_identity_199_fixed_1_changed_is_inconclusive():
    raw, masked = _rows(200, fixed=set(range(199)))
    result = _evaluate(raw, masked, {f"k{i:03d}" for i in range(199)})
    assert result.status == INCONCLUSIVE, result.detail
    assert "1 compared, 199 unchanged of 200 sampled" in result.detail


@pytest.mark.parametrize("n, fixed, status", [
    (20, 2, PASS),           # 10% unchanged, 18 compared
    (20, 3, INCONCLUSIVE),   # 15% unchanged
    (5, 1, INCONCLUSIVE),    # 20% unchanged, and only 4 compared
])
def test_the_exclusion_is_bounded(n, fixed, status):
    raw, masked = _rows(n, fixed=set(range(fixed)))
    assert _evaluate(raw, masked, {f"k{i:03d}" for i in range(fixed)}).status == status


def test_small_samples_without_fixed_points_are_unchanged():
    raw, masked = _rows(3)
    assert _evaluate(raw, masked, set()).status == PASS


def test_only_fixed_points_prove_nothing():
    raw, masked = _rows(4, fixed={0, 1, 2, 3})
    assert _evaluate(raw, masked, set(raw)).status == INCONCLUSIVE


# ── which functions may have fixed points excluded (pure) ───────────────────

def _function_problem(**overrides):
    args = dict(routine_body="SQL", external_language=None, is_deterministic="true",
                sql_data_access="CONTAINS_SQL", definition=YEAR_BODY, parameters=1)
    args.update(overrides)
    return fixed_point_function_problem(**args)


def test_the_year_mask_is_eligible():
    assert _function_problem() == ""


@pytest.mark.parametrize("overrides, message", [
    ({"routine_body": "EXTERNAL", "external_language": "PYTHON"}, "not a SQL function"),
    ({"is_deterministic": "false"}, "not declared deterministic"),
    ({"sql_data_access": "READS_SQL_DATA"}, "reads or modifies data"),
    ({"parameters": 2}, "takes 2 arguments"),
    ({"definition": None}, "could not be read"),
    ({"definition": "CASE WHEN dt IS NULL THEN 'open"}, "could not be parsed"),
    ({"definition": "CASE WHEN is_account_group_member('admins') THEN dt ELSE MAKE_DATE(YEAR(dt), 1, 1) END"},
     "depends on the caller"),
    ({"definition": "CASE WHEN current_user() = 'a' THEN dt ELSE dt END"}, "depends on the caller"),
    ({"definition": "IF(is_member('x'), dt, NULL)"}, "depends on the caller"),
    ({"definition": "IF(session_user = 'x', dt, NULL)"}, "depends on the caller"),
    ({"definition": "IF(current_timestamp() > TIMESTAMP'2030-01-01', dt, NULL)"}, "depends on the caller"),
    ({"definition": "IF(rand() > 0.5, dt, NULL)"}, "depends on the caller"),
    ({"definition": "cat.sch.other_mask(dt)"}, "calls another (qualified) function"),
    ({"definition": "my_udf(dt)"}, "not a known caller-independent builtin"),
    ({"definition": "(SELECT max(d) FROM t)"}, "depends on the caller"),
])
def test_anything_else_keeps_the_strict_rule(overrides, message):
    assert message in _function_problem(**overrides)


# ── which live mask the column has (pure) ───────────────────────────────────

def _policy(**overrides):
    policy = {"name": "p_date_year", "policy_type": "POLICY_TYPE_COLUMN_MASK", "to_principals": [JUNIOR],
              "match_columns": [{"condition": "hasTagValue('gr_treatment', 'date_year')", "alias": "a"}],
              "column_mask": {"function_name": YEAR_MASK, "on_column": "a"}}
    policy.update(overrides)
    return policy


def _live_problem(policies, tags=(TAG,), table_tags=(), direct=0, check=None):
    return live_mask_problem(check or _check(), policies, list(tags), list(table_tags), direct)


def test_the_configured_live_policy_is_proven():
    assert _live_problem([_policy(), _policy(name="other", match_columns=[
        {"condition": "hasTagValue('gr_treatment', 'redact')", "alias": "b"}])]) == ""


@pytest.mark.parametrize("policies, kwargs, message", [
    ([_policy()], {"direct": 1}, "attached to the column directly"),
    ([], {}, "0 live column-mask policies"),
    ([_policy()], {"tags": [("gr_treatment", "redact")]}, "0 live column-mask policies"),
    ([_policy(), _policy(name="dup")], {}, "2 live column-mask policies"),
    ([_policy(column_mask={"function_name": "cat.sch.other", "on_column": "a"})], {}, "not the configured"),
    ([_policy(column_mask={"function_name": YEAR_MASK, "on_column": "a", "using": [{"alias": "x"}]})], {},
     "extra USING arguments"),
    ([_policy(to_principals=[SENIOR])], {}, "does not apply to every masked tier"),
    ([_policy(to_principals=["account users"], except_principals=[JUNIOR])], {},
     "does not apply to every masked tier"),
    ([_policy(when_condition="has_tag_value('x', 'y')")], {}, "can't evaluate"),
])
def test_any_doubt_about_the_live_mask_keeps_the_strict_rule(policies, kwargs, message):
    assert message in _live_problem(policies, **kwargs)


def test_a_when_condition_is_judged_on_the_table_tags():
    policy = _policy(when_condition="hasTagValue('domain', 'hr')")
    assert "0 live column-mask policies" in _live_problem([policy])
    assert _live_problem([policy], table_tags=[("domain", "hr")]) == ""


# ── the spec carries the policy's mask function ─────────────────────────────

TAGS = [{"entity_type": "columns", "entity_name": f"{TABLE}.dob", "tag_key": TAG[0], "tag_value": TAG[1]}]


def _config_policy(**extra):
    return {"name": "gr_mask_date_year", "policy_type": "POLICY_TYPE_COLUMN_MASK",
            "to_principals": [JUNIOR], "match_condition": "hasTagValue('gr_treatment', 'date_year')",
            "function_catalog": "cat", "function_schema": "sch", "function_name": "mask_date_to_year",
            **extra}


def test_derived_checks_name_the_policy_mask_function():
    spec = derive_spec_from_config([_config_policy()], TAGS, [JUNIOR], key_column="id")
    assert [c.mask_function for c in spec.column_masks] == [YEAR_MASK]


@pytest.mark.parametrize("missing", ["function_catalog", "function_schema", "function_name"])
def test_an_incomplete_function_name_leaves_the_strict_comparison(missing):
    spec = derive_spec_from_config([_config_policy(**{missing: ""})], TAGS, [JUNIOR], key_column="id")
    assert [c.mask_function for c in spec.column_masks] == [""]


def test_a_spec_file_may_name_the_mask_function(tmp_path):
    path = tmp_path / "spec.json"
    path.write_text(json.dumps({"column_masks": [
        {"table": TABLE, "column": "dob", "key_column": "id", "masked_principals": [JUNIOR],
         "unmasked_principals": [DEFAULT_ADMIN_TIER], "mask_function": YEAR_MASK},
        {"table": TABLE, "column": "ssn", "key_column": "id", "masked_principals": [JUNIOR],
         "unmasked_principals": [DEFAULT_ADMIN_TIER]},
    ]}))
    assert [c.mask_function for c in load_spec_from_file(path).column_masks] == [YEAR_MASK, ""]


# ── live: the verifier reads the live mask, the routine, then fixed points ──

class MaskingWarehouse(FakeWarehouse):
    """FakeWarehouse plus the live mask metadata and the fixed-point query."""

    def __init__(self, rows, *, functions, routine=("SQL", None, "true", "CONTAINS_SQL", YEAR_BODY),
                 parameters=1, direct_masks=(), policies=None, **kwargs):
        super().__init__(rows, tags={"dob": [TAG]}, **kwargs)
        self.functions = functions                      # "`cat`.`sch`.`fn`" -> python callable
        self.routine, self.parameters = routine, parameters
        self.direct_masks = set(direct_masks)
        self.policies = [_policy()] if policies is None else policies

    def run(self, tier, sql, params):
        params = params or {}
        if sql.startswith("SELECT COUNT(*) FROM system.information_schema.column_masks"):
            self.statements.append((tier, sql, dict(params)))
            return [[str(int(params["k"] in self.direct_masks))]]
        if "FROM system.information_schema.routines" in sql:
            self.statements.append((tier, sql, dict(params)))
            return [list(self.routine)] if self.routine else []
        if "FROM system.information_schema.parameters" in sql:
            self.statements.append((tier, sql, dict(params)))
            return [[str(self.parameters)]]
        m = re.fullmatch(r"SELECT `(\w+)` FROM (\S+) WHERE `(\w+)` IN \(([^)]*)\) "
                         r"AND \((\S+)\(`(\w+)`\) <=> `\w+`\)", sql)
        if not m:
            return super().run(tier, sql, params)
        self.statements.append((tier, sql, dict(params)))
        key, _, where_col, in_list, function, column = m.groups()
        fn = self.functions[function]
        rows = [r for r in self.view(tier) if self._keep(where_col, in_list, params)(r)]
        return [[str(r[key])] for r in rows if fn(r[column]) == r[column]]


def _dob_rows(n=25, jan_1=(0,)):
    """n rows (all sampled: SAMPLE_ROWS is 25); keys in ``jan_1`` have a 1 January birth date."""
    return [{"id": f"KEY-{i:04d}", "dob": f"{1950 + i}-01-01" if i in jan_1 else f"{1950 + i}-0{1 + i % 9}-1{i % 9}"}
            for i in range(n)]


@pytest.fixture
def live(warehouse, monkeypatch):
    def install(rows=None, *, tier_mask=_year, function=_year, **kwargs):
        wh = warehouse(MaskingWarehouse(rows or _dob_rows(), functions={"`cat`.`sch`.`mask_date_to_year`": function},
                                        masks={JUNIOR: {"dob": tier_mask}}, **kwargs))
        monkeypatch.setattr(vea.EffectiveAccessVerifier, "run_api",
                            lambda self, ws, path, query=None: {"policies": wh.policies})
        return wh
    return install


def _fixed_point_queries(wh):
    return [s for s in wh.statements if "<=>" in s[1]]


def test_live_the_real_date_case_passes_with_one_1_january_row(live, tmp_path):
    wh = live()
    [result] = _verify(tmp_path, _check())
    assert result.status == PASS, result.detail
    assert result.evidence["fixed_point_rows"] == 1 and result.evidence["masked_ok"] == 24
    assert {tier for tier, _, _ in _fixed_point_queries(wh)} == {DEFAULT_ADMIN_TIER}
    assert all(tier == DEFAULT_ADMIN_TIER for tier, sql, _ in wh.statements if "information_schema.routines" in sql)


def test_live_caller_sensitive_mask_is_not_a_pass(live, tmp_path):
    # Codex's counterexample: the function is the identity for the admin on one
    # row (it checks group membership), and that row is raw for the tier too
    # (the mask is missing there). Excluding it would PASS on the other rows.
    rows = _dob_rows(jan_1=())
    leaked = rows[3]["dob"]
    wh = live(rows,
              tier_mask=lambda v: v if v == leaked else _year(v),
              function=lambda v: v if v == leaked else _year(v),
              routine=("SQL", None, "true", "CONTAINS_SQL",
                       "CASE WHEN is_account_group_member('admins') AND dt = DATE'1953-04-13' "
                       "THEN dt ELSE MAKE_DATE(YEAR(dt), 1, 1) END"))
    [result] = _verify(tmp_path, _check())
    assert result.status == FAIL and "1 row(s) leaked" in result.detail
    assert not _fixed_point_queries(wh)


def test_live_a_config_live_function_mismatch_is_strict(live, tmp_path):
    wh = live(policies=[_policy(column_mask={"function_name": "cat.sch.other_mask", "on_column": "a"})])
    [result] = _verify(tmp_path, _check())
    assert result.status == FAIL and "1 row(s) leaked" in result.detail
    assert not _fixed_point_queries(wh)


@pytest.mark.parametrize("kwargs", [
    {"policies": [_policy(column_mask={"function_name": YEAR_MASK, "on_column": "a",
                                       "using": [{"alias": "other_col"}]})]},
    {"parameters": 2},
])
def test_live_a_multi_argument_mask_is_strict(live, tmp_path, kwargs):
    wh = live(**kwargs)
    [result] = _verify(tmp_path, _check())
    assert result.status == FAIL and not _fixed_point_queries(wh)


def test_live_near_identity_mask_is_inconclusive(live, tmp_path):
    # The mask leaves 24 of 25 sampled values unchanged and changes one.
    rows = _dob_rows(jan_1=())
    changed = rows[7]["dob"]
    near_identity = lambda v: _year(v) if v == changed else v  # noqa: E731
    live(rows, tier_mask=near_identity, function=near_identity,
         routine=("SQL", None, "true", "CONTAINS_SQL", "IF(dt = DATE'1957-08-17', MAKE_DATE(YEAR(dt), 1, 1), dt)"))
    [result] = _verify(tmp_path, _check())
    assert result.status == INCONCLUSIVE, result.detail
    assert "1 compared, 24 unchanged of 25 sampled" in result.detail


def test_live_a_python_udf_is_strict(live, tmp_path):
    wh = live(routine=("EXTERNAL", "PYTHON", "true", "NO_SQL", "return dt.replace(month=1, day=1)"))
    [result] = _verify(tmp_path, _check())
    assert result.status == FAIL and not _fixed_point_queries(wh)


def test_live_a_directly_attached_mask_is_strict(live, tmp_path):
    wh = live(direct_masks={"dob"})
    [result] = _verify(tmp_path, _check())
    assert result.status == FAIL and not _fixed_point_queries(wh)


def test_live_without_a_known_function_an_unchanged_value_still_fails(live, tmp_path):
    wh = live()
    [result] = _verify(tmp_path, _check(mask_function=""))
    assert result.status == FAIL and "1 row(s) leaked" in result.detail
    assert not _fixed_point_queries(wh)


def test_live_if_the_admin_cannot_read_the_mask_it_stays_strict(live, tmp_path, monkeypatch, capsys):
    live()

    def broken(self, ws, path, query=None):
        raise RuntimeError("PERMISSION_DENIED: policies")
    monkeypatch.setattr(vea.EffectiveAccessVerifier, "run_api", broken)
    [result] = _verify(tmp_path, _check())
    assert result.status == FAIL
    assert "an unchanged value counts as a leak" in capsys.readouterr().out


def test_live_an_unsafe_function_name_is_never_put_in_sql(live, tmp_path):
    wh = live()
    [result] = _verify(tmp_path, _check(mask_function="cat.sch.fn`; DROP TABLE x; --"))
    assert result.status == FAIL
    assert not [s for s in wh.statements if "<=>" in s[1] or "DROP" in s[1]]


def test_live_a_real_leak_is_still_caught(live, tmp_path):
    live(tier_mask=lambda v: v)  # mask not applied for the tier
    [result] = _verify(tmp_path, _check())
    assert result.status == FAIL and "leaked" in result.detail


def test_live_policies_are_read_across_pages(live, tmp_path, monkeypatch):
    wh = live()
    pages = [{"policies": [_policy(name="other", match_columns=[
        {"condition": "hasTagValue('gr_treatment', 'redact')", "alias": "b"}])], "next_page_token": "p2"},
             {"policies": wh.policies}]
    seen = []

    def paged(self, ws, path, query=None):
        seen.append(dict(query or {}))
        return pages[len(seen) - 1]
    monkeypatch.setattr(vea.EffectiveAccessVerifier, "run_api", paged)
    [result] = _verify(tmp_path, _check())
    assert result.status == PASS, result.detail
    assert seen == [{"include_inherited": "true"}, {"include_inherited": "true", "page_token": "p2"}]
