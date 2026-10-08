"""verify-access must not call a value the mask leaves unchanged a leak.

Found on the live AWS run: prod release failed on a correctly masked
date_of_birth column. mask_date_to_year maps a date to 1 January of its year,
so the one birth date already on 1 January looks the same masked or not, and
the evaluator counted "masked value == raw value" as a leak. The salted sample
(#90) only sometimes includes that row, so release failed at random. The admin
baseline now evaluates the policy's own mask function on the sampled raw
values; rows it maps to themselves can't demonstrate masking and are left out,
like NULL raw values. A tier left with no other row is INCONCLUSIVE, never PASS.
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
    evaluate_column_mask_check, load_spec_from_file,
)
from tests.test_verify_key_pairing import (  # noqa: E402,F401  (warehouse is a fixture)
    JUNIOR, TABLE, FakeWarehouse, _verify, warehouse,
)

YEAR_MASK = "cat.sch.mask_date_to_year"


def _year(value):
    return None if value is None else f"{value[:4]}-01-01"


def _check(mask_function=YEAR_MASK):
    return ColumnMaskCheck(TABLE, "dob", "id", (JUNIOR,), (DEFAULT_ADMIN_TIER,),
                           "gr_mask_date_year", mask_function)


# ── the evaluator ───────────────────────────────────────────────────────────

RAW = {"k1": "1980-01-01", "k2": "1987-03-22", "k3": "1990-07-04"}
MASKED = {k: _year(v) for k, v in RAW.items()}  # k1 is unchanged by the mask


def _values(masked=MASKED):
    return {DEFAULT_ADMIN_TIER: list(RAW.items()), JUNIOR: list(masked.items())}


def test_an_unchanged_value_alone_used_to_fail_as_a_leak():
    result = evaluate_column_mask_check(_check(), _values())
    assert result.status == FAIL and "1 row(s) leaked" in result.detail


def test_a_fixed_point_is_left_out_and_the_rest_proves_the_mask():
    result = evaluate_column_mask_check(_check(), _values(), fixed_point_keys={"k1"})
    assert result.status == PASS, result.detail
    assert "1 row(s) whose raw value the mask leaves unchanged were not compared" in result.detail
    assert result.evidence["fixed_point_rows"] == 1 and result.evidence["masked_ok"] == 2


def test_a_real_leak_still_fails_beside_a_fixed_point():
    leaked = {**MASKED, "k2": RAW["k2"]}  # k2 should have become 1987-01-01
    result = evaluate_column_mask_check(_check(), _values(leaked), fixed_point_keys={"k1"})
    assert result.status == FAIL and "1 row(s) leaked" in result.detail


def test_only_fixed_points_prove_nothing():
    raw = {"k1": "1980-01-01", "k2": "1999-01-01"}
    values = {DEFAULT_ADMIN_TIER: list(raw.items()), JUNIOR: list(raw.items())}
    result = evaluate_column_mask_check(_check(), values, fixed_point_keys={"k1", "k2"})
    assert result.status == INCONCLUSIVE
    assert "one the mask leaves unchanged" in result.detail


def test_fixed_point_keys_outside_the_sample_are_ignored():
    result = evaluate_column_mask_check(_check(), _values(), fixed_point_keys={"k1", "other"})
    assert result.status == PASS and result.evidence["fixed_point_rows"] == 1


# ── the spec carries the policy's mask function ─────────────────────────────

def _policy(**extra):
    return {"name": "gr_mask_date_year", "policy_type": "POLICY_TYPE_COLUMN_MASK",
            "to_principals": [JUNIOR], "match_condition": "hasTagValue('gr_treatment', 'date_year')",
            "function_catalog": "cat", "function_schema": "sch", "function_name": "mask_date_to_year",
            **extra}


TAGS = [{"entity_type": "columns", "entity_name": f"{TABLE}.dob",
         "tag_key": "gr_treatment", "tag_value": "date_year"}]


def test_derived_checks_name_the_policy_mask_function():
    spec = derive_spec_from_config([_policy()], TAGS, [JUNIOR], key_column="id")
    assert [c.mask_function for c in spec.column_masks] == [YEAR_MASK]


@pytest.mark.parametrize("missing", ["function_catalog", "function_schema", "function_name"])
def test_an_incomplete_function_name_leaves_the_strict_comparison(missing):
    spec = derive_spec_from_config([_policy(**{missing: ""})], TAGS, [JUNIOR], key_column="id")
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


# ── live: the admin baseline evaluates the mask on the sampled raw values ──

class MaskingWarehouse(FakeWarehouse):
    """FakeWarehouse that also evaluates mask functions for the fixed-point query."""

    def __init__(self, rows, functions, *, fail_fixed_points=False, **kwargs):
        super().__init__(rows, **kwargs)
        self.functions = functions          # "`cat`.`sch`.`fn`" -> python callable
        self.fail_fixed_points = fail_fixed_points

    def run(self, tier, sql, params):
        m = re.fullmatch(r"SELECT `(\w+)` FROM (\S+) WHERE `(\w+)` IN \(([^)]*)\) "
                         r"AND \((\S+)\(`(\w+)`\) <=> `\w+`\)", sql)
        if not m:
            return super().run(tier, sql, params)
        self.statements.append((tier, sql, dict(params or {})))
        if self.fail_fixed_points:
            raise RuntimeError("PERMISSION_DENIED: EXECUTE on function")
        key, _, where_col, in_list, function, column = m.groups()
        fn = self.functions[function]
        rows = [r for r in self.view(tier) if self._keep(where_col, in_list, params)(r)]
        return [[str(r[key])] for r in rows if fn(r[column]) == r[column]]


def _dob_rows(n=30):
    rows = [{"id": f"KEY-{i:04d}", "dob": f"{1950 + i}-0{1 + i % 9}-1{i % 9}"} for i in range(1, n + 1)]
    rows[0]["dob"] = "1951-01-01"  # the one birth date on 1 January
    return rows


def _warehouse(install, **kwargs):
    return install(MaskingWarehouse(_dob_rows(), {"`cat`.`sch`.`mask_date_to_year`": _year},
                                    masks={JUNIOR: {"dob": _year}}, **kwargs))


def test_live_release_passes_with_a_fixed_point_in_the_sample(warehouse, tmp_path):
    wh = _warehouse(warehouse)
    [result] = _verify(tmp_path, _check())
    assert result.status == PASS, result.detail
    assert result.evidence["fixed_point_rows"] == 1
    fixed_point_queries = [s for s in wh.statements if "<=>" in s[1]]
    assert fixed_point_queries and {tier for tier, _, _ in fixed_point_queries} == {DEFAULT_ADMIN_TIER}


def test_live_without_a_known_function_an_unchanged_value_still_fails(warehouse, tmp_path):
    wh = _warehouse(warehouse)
    [result] = _verify(tmp_path, _check(mask_function=""))
    assert result.status == FAIL and "1 row(s) leaked" in result.detail
    assert not [s for s in wh.statements if "<=>" in s[1]]


def test_live_if_the_admin_cannot_evaluate_the_mask_it_stays_strict(warehouse, tmp_path, capsys):
    _warehouse(warehouse, fail_fixed_points=True)
    [result] = _verify(tmp_path, _check())
    assert result.status == FAIL and "1 row(s) leaked" in result.detail
    assert "an unchanged value counts as a leak" in capsys.readouterr().out


def test_live_an_unsafe_function_name_is_never_put_in_sql(warehouse, tmp_path):
    wh = _warehouse(warehouse)
    [result] = _verify(tmp_path, _check(mask_function="cat.sch.fn`; DROP TABLE x; --"))
    assert result.status == FAIL  # strict comparison, nothing evaluated
    assert not [s for s in wh.statements if "<=>" in s[1] or "DROP" in s[1]]


def test_live_a_real_leak_is_still_caught(warehouse, tmp_path):
    rows = _dob_rows()
    install = warehouse
    install(MaskingWarehouse(rows, {"`cat`.`sch`.`mask_date_to_year`": _year},
                             masks={JUNIOR: {"dob": lambda v: v}}))  # mask not applied
    [result] = _verify(tmp_path, _check())
    assert result.status == FAIL and "leaked" in result.detail
