"""A re-run of generate keeps reviewed rules unless --allow-rule-changes."""
import sys
from pathlib import Path

import hcl2
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import generate_abac  # noqa: E402
from scripts.merge_space_configs import keep_reviewed_rules, load_reviewed_rules  # noqa: E402

AMOUNT = "dev_fin.payments.payments.amount"
EMAIL = "dev_fin.payments.customers.email"
PHONE = "dev_fin.payments.customers.phone"


def _assignment(column, treatment):
    return f"""  {{
    entity_type = "columns"
    entity_name = "{column}"
    tag_key     = "gr_treatment"
    tag_value   = "{treatment}"
  }},"""


def _mask(treatment, function, principals='["viewers"]'):
    return f"""  {{
    name             = "gr_mask_dev_fin_{treatment}"
    policy_type      = "POLICY_TYPE_COLUMN_MASK"
    catalog          = "dev_fin"
    to_principals    = {principals}
    match_condition  = "hasTagValue('gr_treatment', '{treatment}')"
    match_alias      = "gr_treatment_{treatment}"
    function_name    = "{function}"
    function_catalog = "dev_fin"
    function_schema  = "payments"
  }},"""


FUNCTIONS = {
    "mask_email": "RETURN CONCAT('***@', SPLIT(val, '@')[1]);",
    "mask_amount_rounded": "RETURN ROUND(val, -2);",
    "mask_phone": "RETURN CONCAT('***', RIGHT(val, 4));",
}


def _write_draft(directory, columns, *, principals='["viewers"]', bodies=None):
    """columns: {column: (treatment, function)}"""
    directory.mkdir(parents=True, exist_ok=True)
    treatments = {t: f for t, f in columns.values()}
    abac = (
        "# GENERATED ABAC CONFIG (FIRST DRAFT)\n"
        'tag_policies = [\n  { key = "gr_treatment", values = ['
        + ", ".join(f'"{t}"' for t in treatments) + "] },\n]\n\n"
        "tag_assignments = [\n"
        + "\n".join(_assignment(c, t) for c, (t, _) in columns.items())
        + "\n]\n\nfgac_policies = [\n"
        + "\n".join(_mask(t, f, principals) for t, f in treatments.items())
        + "\n]\n"
    )
    (directory / "abac.auto.tfvars").write_text(abac)
    bodies = {**FUNCTIONS, **(bodies or {})}
    sql = "-- GENERATED MASKING FUNCTIONS\nUSE CATALOG dev_fin;\nUSE SCHEMA payments;\n\n" + "\n\n".join(
        f"CREATE OR REPLACE FUNCTION {f}(val STRING)\nRETURNS STRING\n{bodies[f]}"
        for f in treatments.values()
    ) + "\n"
    (directory / "masking_functions.sql").write_text(sql)


def _rerun(tmp_path, reviewed_columns, new_columns, *, allow=False, **new_kwargs):
    reviewed_dir = tmp_path / "reviewed"
    _write_draft(reviewed_dir, reviewed_columns)
    reviewed = load_reviewed_rules(reviewed_dir)
    generated = tmp_path / "generated"
    _write_draft(generated, new_columns, **new_kwargs)
    messages = keep_reviewed_rules(
        reviewed, generated / "abac.auto.tfvars", generated / "masking_functions.sql",
        allow_changes=allow,
    )
    cfg = hcl2.loads((generated / "abac.auto.tfvars").read_text())
    sql = (generated / "masking_functions.sql").read_text()
    return messages, cfg, sql


def _treatments(cfg):
    return {a["entity_name"]: a["tag_value"] for a in cfg["tag_assignments"]}


def _policy(cfg, name):
    return next((p for p in cfg["fgac_policies"] if p["name"] == name), None)


REVIEWED = {
    EMAIL: ("email_mask", "mask_email"),
    AMOUNT: ("round_amount", "mask_amount_rounded"),
}


def test_model_dropping_a_rule_keeps_it_with_one_line_per_rule(tmp_path):
    messages, cfg, sql = _rerun(tmp_path, REVIEWED, {EMAIL: ("email_mask", "mask_email")})

    assert _treatments(cfg)[AMOUNT] == "round_amount"
    assert _policy(cfg, "gr_mask_dev_fin_round_amount")["function_name"] == "mask_amount_rounded"
    assert "round_amount" in next(p for p in cfg["tag_policies"] if p["key"] == "gr_treatment")["values"]
    assert "FUNCTION mask_amount_rounded" in sql and "ROUND(val, -2)" in sql
    assert messages == [
        f"  kept reviewed rule {AMOUNT} → round_amount (model proposed removing it); "
        "re-run with --allow-rule-changes to accept",
        "  kept reviewed rule policy gr_mask_dev_fin_round_amount (model proposed removing it); "
        "re-run with --allow-rule-changes to accept",
        "  kept reviewed rule function dev_fin.payments.mask_amount_rounded (model proposed "
        "removing it); re-run with --allow-rule-changes to accept",
    ]


def test_model_changing_a_rule_keeps_the_reviewed_version(tmp_path):
    messages, cfg, sql = _rerun(
        tmp_path, REVIEWED,
        {EMAIL: ("redact", "mask_redact"), AMOUNT: ("round_amount", "mask_amount_rounded")},
        principals='["viewers", "regional_analysts"]',
        bodies={"mask_redact": "RETURN '[REDACTED]';", "mask_amount_rounded": "RETURN ROUND(val, 0);"},
    )

    assert _treatments(cfg) == {EMAIL: "email_mask", AMOUNT: "round_amount"}
    assert _policy(cfg, "gr_mask_dev_fin_round_amount")["to_principals"] == ["viewers"]
    assert _policy(cfg, "gr_mask_dev_fin_email_mask")["to_principals"] == ["viewers"]
    assert "ROUND(val, -2)" in sql and "ROUND(val, 0)" not in sql
    assert "FUNCTION mask_email" in sql
    assert any(f"{EMAIL} → email_mask (model proposed changing it)" in m for m in messages)
    assert any("policy gr_mask_dev_fin_round_amount (model proposed changing it)" in m for m in messages)
    assert any("mask_amount_rounded (model proposed changing it)" in m for m in messages)


def test_new_tag_is_added_and_reviewed_rules_untouched(tmp_path):
    messages, cfg, sql = _rerun(
        tmp_path, REVIEWED, {**REVIEWED, PHONE: ("phone_mask", "mask_phone")},
    )

    assert messages == []
    assert _treatments(cfg) == {EMAIL: "email_mask", AMOUNT: "round_amount", PHONE: "phone_mask"}
    assert _policy(cfg, "gr_mask_dev_fin_phone_mask")
    assert "FUNCTION mask_phone" in sql


def test_allow_rule_changes_accepts_the_new_draft(tmp_path):
    new_columns = {EMAIL: ("redact", "mask_redact")}
    generated = tmp_path / "expected"
    _write_draft(generated, new_columns, bodies={"mask_redact": "RETURN '[REDACTED]';"})

    messages, cfg, sql = _rerun(
        tmp_path, REVIEWED, new_columns, allow=True,
        bodies={"mask_redact": "RETURN '[REDACTED]';"},
    )

    assert (tmp_path / "generated" / "abac.auto.tfvars").read_text() == (
        generated / "abac.auto.tfvars").read_text()
    assert sql == (generated / "masking_functions.sql").read_text()
    assert _treatments(cfg) == {EMAIL: "redact"}
    assert any(AMOUNT in m and "accepted model change" in m for m in messages)


def test_first_run_has_no_reviewed_rules(tmp_path):
    assert load_reviewed_rules(tmp_path) is None
    # A genie-mode import (Phase 0) carries no rules either.
    (tmp_path / "abac.auto.tfvars").write_text('genie_space_configs = {\n  "A" = { title = "A" }\n}\n')
    assert load_reviewed_rules(tmp_path) is None


def test_unreadable_reviewed_draft_fails_before_the_model_is_called(tmp_path, monkeypatch, capsys):
    generated = tmp_path / "generated"
    generated.mkdir()
    (generated / "abac.auto.tfvars").write_text("tag_assignments = [ {\n")
    with pytest.raises(ValueError, match="--allow-rule-changes"):
        load_reviewed_rules(generated)

    def model_called(*_a, **_k):
        raise AssertionError("model must not be called")

    monkeypatch.setattr(generate_abac, "call_with_retries", model_called)
    monkeypatch.setattr(generate_abac, "configure_databricks_env", lambda *_: None)
    (tmp_path / "ddl").mkdir()
    (tmp_path / "ddl" / "t.sql").write_text("CREATE TABLE c.s.t (id INT);\n")
    (tmp_path / "auth.auto.tfvars").write_text("")
    monkeypatch.setattr(sys, "argv", [
        "generate_abac.py", "--auth-file", str(tmp_path / "auth.auto.tfvars"),
        "--tables", "c.s.t", "--catalog", "c", "--schema", "s", "--create-groups",
        "--out-dir", str(generated), "--ddl-dir", str(tmp_path / "ddl"),
    ])
    monkeypatch.setattr(generate_abac, "fetch_tables_from_databricks",
                        lambda *_a, **_k: ("CREATE TABLE c.s.t (id INT);", [("c", "s")]))
    with pytest.raises(SystemExit) as exc:
        generate_abac.main()
    assert exc.value.code == 1
    assert "cannot read the reviewed rules" in capsys.readouterr().out
