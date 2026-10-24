"""audit-rulebook resolves a deterministic env's class.* tags through its generation.

The fixture is a real generated envs/dev: deterministic `make generate`
(test_deterministic_generate's env) split into account + data_access by
split_abac_config.py, so every gr_treatment is a versioned mask-library value
(``ssn__string__<generation>``), not the legacy treatment_config.json value
(``ssn_last4``) the class used to be looked up as.
"""

import re
import subprocess
import sys
from pathlib import Path

import hcl2
import pytest

import generate_abac
import scripts.audit_schema_drift as audit_mod
from governance_policies import TAG_KEY, parse_treatment_value, value_sql_type
from scripts.audit_schema_drift import (
    build_rulebook,
    extract_fgac_policies,
    extract_tag_policies,
    find_uncovered_tags,
    is_tag_covered,
)
from scripts.coverage_fix import regenerate_lines
from sensitivity_source import ClassificationSource
from tests.test_deterministic_generate import TABLES, TAG_ROWS, _generate, env_dir  # noqa: F401

SPLIT = Path(__file__).resolve().parents[1] / "scripts" / "split_abac_config.py"


def _build(env_dir, monkeypatch, tag_rows=TAG_ROWS):
    """Generate from ``tag_rows`` and split, as make generate + promote do."""
    monkeypatch.setattr(generate_abac, "_fetch_live_classification_source",
                        lambda *a, **k: ClassificationSource(tag_rows=tag_rows))
    _generate(env_dir, monkeypatch)
    subprocess.run(
        [sys.executable, str(SPLIT), "generated/abac.auto.tfvars", "../account/abac.auto.tfvars",
         "data_access/abac.auto.tfvars", "abac.auto.tfvars"],
        cwd=env_dir, check=True, capture_output=True,
    )
    monkeypatch.setattr(audit_mod, "extract_managed_tables", lambda env: list(TABLES))
    monkeypatch.setattr(audit_mod, "_get_sdk_client", lambda env: object())
    monkeypatch.setattr(audit_mod, "_get_warehouse_id", lambda env, w: "wh-123")
    return env_dir


@pytest.fixture
def dev(env_dir, monkeypatch):  # noqa: F811
    """envs/dev after a deterministic generate and the promote split."""
    return _build(env_dir, monkeypatch)


def _promoted(env_dir):
    return hcl2.loads((env_dir / "data_access" / "abac.auto.tfvars").read_text())


def _write_promoted(env_dir, cfg):
    from scripts.split_abac_config import DATA_ACCESS_KEYS, write_tfvars

    write_tfvars(env_dir / "data_access" / "abac.auto.tfvars", cfg, "# test", DATA_ACCESS_KEYS)


def _treatment(env_dir, column):
    return next(a["tag_value"] for a in _promoted(env_dir)["tag_assignments"]
                if a["tag_key"] == TAG_KEY and a["entity_name"].endswith(column))


def _applied(env_dir, tag_rows):
    """What information_schema.column_tags holds: the class.* facts and the released gr_treatment."""
    rows = [dict(zip(("catalog", "schema", "table", "column", "tag_key", "tag_value"), row))
            for row in tag_rows]
    for item in _promoted(env_dir)["tag_assignments"]:
        if item["tag_key"] == TAG_KEY:
            catalog, schema, table, column = item["entity_name"].split(".")
            rows.append({"catalog": catalog, "schema": schema, "table": table, "column": column,
                         "tag_key": TAG_KEY, "tag_value": item["tag_value"]})
    return rows


def _audit(env_dir, monkeypatch, capsys, tag_rows=TAG_ROWS, applied_rows=None):
    applied = _applied(env_dir, applied_rows if applied_rows is not None else tag_rows)
    monkeypatch.setattr(audit_mod, "_query_applied_tags", lambda w, wh, tables: applied)
    monkeypatch.setattr(audit_mod, "_query_live_classes",
                        lambda w, wh, tables: ClassificationSource(tag_rows=tag_rows).classes_by_column())
    capsys.readouterr()
    rc = audit_mod.main(["--mode", "rulebook", "--env-name", "dev"])
    return rc, capsys.readouterr().out


def _drift_lines(out):
    return [line.strip() for line in out.splitlines() if re.match(r"\s+dev_fin\.", line)]


def test_fixture_has_versioned_treatments_the_legacy_vocabulary_never_matches(dev):
    """The bug: a class resolved through treatment_config.json names a legacy value."""
    ssn = _treatment(dev, ".ssn")
    assert parse_treatment_value(ssn)[0] == "ssn"
    rulebook = build_rulebook(extract_tag_policies(dev), extract_fgac_policies(dev))
    assert rulebook["class_treatments"]["us_ssn"] == "ssn_last4"
    assert not is_tag_covered("dev_fin", "class.us_ssn", "", rulebook)
    assert is_tag_covered("dev_fin", TAG_KEY, ssn, rulebook)


def test_live_generated_env_has_no_rulebook_drift(dev, monkeypatch, capsys):
    rc, out = _audit(dev, monkeypatch, capsys)
    assert rc == 0, out
    assert "No drift detected." in out
    assert "scaffold-treatments" not in out


def test_an_unmapped_class_still_drifts_and_points_at_scaffold_treatments(dev, monkeypatch, capsys):
    rows = TAG_ROWS + [("dev_fin", "payments", "customers", "customer_id", "class.loyalty_tier", "")]
    rc, out = _audit(dev, monkeypatch, capsys, rows)
    assert rc == 1
    assert _drift_lines(out) == [
        "dev_fin.payments.customers.customer_id  [class.loyalty_tier=]  -- class not mapped — no "
        "mask-library treatment or tag_treatments entry maps class.loyalty_tier (the column is "
        "fail-safe redacted)",
    ]
    assert "make scaffold-treatments ENV=dev TREATMENT=<mask-library treatment>" in out
    assert "make generate ENV=dev\n" not in out


def test_an_unmapped_class_drifts_even_once_its_column_is_generated(env_dir, monkeypatch, capsys):  # noqa: F811
    """Generation gives an unmapped class the fail-safe redaction, which is masked;
    the class is still unmapped, so it is still drift."""
    rows = TAG_ROWS + [("dev_fin", "payments", "customers", "customer_id", "class.loyalty_tier", "")]
    dev = _build(env_dir, monkeypatch, rows)
    assert _treatment(dev, ".customer_id").startswith("unmapped_redact__bigint__")
    rc, out = _audit(dev, monkeypatch, capsys, rows)
    assert rc == 1
    assert [line.split("  ")[0] for line in _drift_lines(out)] == ["dev_fin.payments.customers.customer_id"]
    assert "class not mapped" in out


@pytest.mark.parametrize("suffix", ["_full", "_partial"])
def test_a_removed_mask_policy_drifts_and_points_at_generate(dev, monkeypatch, capsys, suffix):
    """Without the partial policy the middle tier, excepted from the full one, reads raw."""
    cfg = _promoted(dev)
    email = _treatment(dev, ".email")
    function = f"gr_mask_{email}{suffix}"
    before = len(cfg["fgac_policies"])
    cfg["fgac_policies"] = [p for p in cfg["fgac_policies"] if p.get("function_name") != function]
    assert len(cfg["fgac_policies"]) == before - 1
    _write_promoted(dev, cfg)

    rc, out = _audit(dev, monkeypatch, capsys)
    assert rc == 1
    assert _drift_lines(out) == [
        f"dev_fin.payments.customers.email  [class.email_address=]  -- treatment not covered — no "
        f"promoted column mask in catalog dev_fin applies {function} to {TAG_KEY}={email}",
    ]
    assert "make generate ENV=dev" in out
    assert "scaffold-treatments" not in out


def test_a_new_class_on_a_generated_column_drifts_until_regenerated(dev, monkeypatch, capsys):
    """Live classes that derive another value than the promoted one: the promoted mask
    no longer matches what generation would protect the column with."""
    rows = TAG_ROWS + [("dev_fin", "payments", "customers", "email", "class.us_ssn", "")]
    rc, out = _audit(dev, monkeypatch, capsys, rows)
    assert rc == 1
    lines = _drift_lines(out)
    assert [line.split("  ")[0] for line in lines] == ["dev_fin.payments.customers.email"] * 2
    assert all(f"but the promoted config assigns {_treatment(dev, '.email')}" in line for line in lines)
    assert "make generate ENV=dev" in out


def test_a_class_tag_on_a_column_with_no_promoted_treatment_drifts(dev, monkeypatch, capsys):
    cfg = _promoted(dev)
    cfg["tag_assignments"] = [a for a in cfg["tag_assignments"] if not a["entity_name"].endswith(".cvv")]
    _write_promoted(dev, cfg)
    rc, out = _audit(dev, monkeypatch, capsys)
    assert rc == 1
    assert _drift_lines(out) == [
        "dev_fin.payments.payments.cvv  [class.card_security_code=]  -- treatment not covered — "
        f"no {TAG_KEY} for this column in the promoted config",
    ]


def test_a_class_tag_the_live_reading_misses_fails_closed(dev):
    rulebook = build_rulebook([], [])
    rulebook["deterministic_classes"] = {"unmapped": set(), "columns": {}}
    tag = {"catalog": "dev_fin", "schema": "payments", "table": "customers", "column": "ssn",
           "tag_key": "class.us_ssn", "tag_value": ""}
    [flagged] = find_uncovered_tags([tag], rulebook)
    assert flagged["reason"] == "no live class.* reading for this column"


def test_non_class_tags_keep_the_promoted_vocabulary_check(dev, monkeypatch, capsys):
    rows = TAG_ROWS + [("dev_fin", "payments", "customers", "email", "data_residency", "eu")]
    rc, out = _audit(dev, monkeypatch, capsys, TAG_ROWS, applied_rows=rows)
    assert rc == 1
    assert _drift_lines(out) == [
        "dev_fin.payments.customers.email  [data_residency=eu]  -- unknown tag key — no tag_policy "
        "declares it and no column mask in this catalog references it",
    ]


def test_legacy_class_tags_keep_the_treatment_config_lookup():
    """Legacy envs are unchanged: no per-column resolution, no class_gap annotation."""
    rulebook = build_rulebook([{"key": "gr_treatment", "values": ["ssn_last4"]}], [])
    assert "deterministic_classes" not in rulebook
    covered = {"catalog": "c", "schema": "s", "table": "t", "column": "ssn",
               "tag_key": "class.us_ssn", "tag_value": ""}
    gap = {**covered, "column": "x", "tag_key": "class.loyalty_tier"}
    assert find_uncovered_tags([covered, gap], rulebook) == [{
        **gap,
        "reason": "unknown tag key — no tag_policy declares it and no column mask in this catalog references it",
    }]


def test_regenerate_advice_in_a_promoted_env_runs_through_its_source(tmp_path):
    (tmp_path / "dev").mkdir()
    (tmp_path / "dev" / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    prod = tmp_path / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text('governance_mode = "deterministic"\npromote_from = "dev"\n')
    text = "\n".join(regenerate_lines("prod", prod))
    assert "make generate ENV=dev" in text
    assert "make rehearse ENV=dev, make promote-to ENV=prod" in text
    assert "make release-plan ENV=prod" in text
    assert "scaffold-treatments" not in text


@pytest.mark.parametrize(("value", "sql_type"), [
    ("ssn__string__8277f91e", "STRING"),
    ("unmapped_redact__decimal_10_2__0123abcd", "DECIMAL(10,2)"),
    ("dob__timestamp_ntz__0123abcd", "TIMESTAMP_NTZ"),
    ("ssn_last4", None),
])
def test_value_sql_type(value, sql_type):
    assert value_sql_type(value) == sql_type
