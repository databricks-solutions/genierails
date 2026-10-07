"""A re-run of generate keeps reviewed rules unless --allow-rule-changes."""
import subprocess
import sys
from pathlib import Path

import hcl2
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import generate_abac  # noqa: E402
import scripts.merge_space_configs as merge_space_configs  # noqa: E402
from scripts.merge_space_configs import (  # noqa: E402
    footprint_from_ddl,
    keep_reviewed_rules,
    load_reviewed_rules,
    sql_tokens,
)

SHARED = Path(__file__).parent.parent
AMOUNT = "dev_fin.payments.payments.amount"
EMAIL = "dev_fin.payments.customers.email"
PHONE = "dev_fin.payments.customers.phone"
HINT = "re-run with --allow-rule-changes to accept"

FUNCTIONS = {
    "mask_email": "RETURN CONCAT('***@', SPLIT(val, '@')[1]);",
    "mask_amount_rounded": "RETURN ROUND(val, -2);",
    "mask_phone": "RETURN CONCAT('***', RIGHT(val, 4));",
    "mask_redact": "RETURN '[REDACTED]';",
}
TREATMENT_FUNCTIONS = {
    "email_mask": "mask_email",
    "round_amount": "mask_amount_rounded",
    "phone_mask": "mask_phone",
    "redact": "mask_redact",
}


# ---------------------------------------------------------------------------
# Draft builders
# ---------------------------------------------------------------------------

def _mask(name, treatment, function, principals=("viewers",)):
    return {
        "name": name,
        "policy_type": "POLICY_TYPE_COLUMN_MASK",
        "catalog": "dev_fin",
        "to_principals": list(principals),
        "match_condition": f"hasTagValue('gr_treatment', '{treatment}')",
        "match_alias": f"gr_treatment_{treatment}",
        "function_name": function,
        "function_catalog": "dev_fin",
        "function_schema": "payments",
    }


def _write_draft(directory, columns, *, policies=None, overrides=None, bodies=None,
                 principals=("viewers",), functions=None):
    """Write abac.auto.tfvars + masking_functions.sql.

    columns: {column: treatment}. Policies default to one gr_mask per treatment.
    """
    directory.mkdir(parents=True, exist_ok=True)
    treatments = list(dict.fromkeys(columns.values()))
    if policies is None:
        policies = [
            _mask(f"gr_mask_dev_fin_{t}", t, TREATMENT_FUNCTIONS[t], principals)
            for t in treatments
        ]
    cfg = {
        "tag_policies": [{"key": "gr_treatment", "values": treatments}],
        "tag_assignments": [
            {"entity_type": "columns", "entity_name": c, "tag_key": "gr_treatment", "tag_value": t}
            for c, t in columns.items()
        ],
        "fgac_policies": policies,
    }
    if overrides:
        cfg["treatment_overrides"] = [
            {"entity_name": c, "treatment": t} for c, t in overrides.items()
        ]
    text = "# GENERATED ABAC CONFIG (FIRST DRAFT)\n" + "".join(
        f"\n{section} = {merge_space_configs._render_value(items)}\n"
        for section, items in cfg.items()
    )
    (directory / "abac.auto.tfvars").write_text(text)
    bodies = {**FUNCTIONS, **(bodies or {})}
    if functions is None:
        functions = sorted({p["function_name"] for p in policies})
    sql = "-- GENERATED MASKING FUNCTIONS\nUSE CATALOG dev_fin;\nUSE SCHEMA payments;\n\n" + "\n\n".join(
        f"CREATE OR REPLACE FUNCTION {f}(val STRING)\nRETURNS STRING\n{bodies[f]}"
        for f in functions
    ) + "\n"
    (directory / "masking_functions.sql").write_text(sql)


DDL = """-- Table: dev_fin.payments.customers
CREATE TABLE dev_fin.payments.customers (
  customer_id BIGINT,
  email STRING,
  phone STRING
);

-- Table: dev_fin.payments.payments
CREATE TABLE dev_fin.payments.payments (
  payment_id BIGINT,
  amount DECIMAL(18,2)
);"""

REVIEWED = {EMAIL: "email_mask", AMOUNT: "round_amount"}


def _rerun(tmp_path, new_columns, *, reviewed_columns=REVIEWED, reviewed_kwargs=None,
           allow=False, ddl=DDL, partial=False, **new_kwargs):
    reviewed_dir = tmp_path / "reviewed"
    _write_draft(reviewed_dir, reviewed_columns, **(reviewed_kwargs or {}))
    reviewed = load_reviewed_rules(reviewed_dir)
    generated = tmp_path / "generated"
    _write_draft(generated, new_columns, **new_kwargs)
    messages = keep_reviewed_rules(
        reviewed, generated / "abac.auto.tfvars", generated / "masking_functions.sql",
        footprint=footprint_from_ddl(ddl), partial_footprint=partial, allow_changes=allow,
    )
    cfg = hcl2.loads((generated / "abac.auto.tfvars").read_text())
    sql = (generated / "masking_functions.sql").read_text()
    return messages, cfg, sql


def _treatments(cfg):
    return {a["entity_name"]: a["tag_value"] for a in cfg["tag_assignments"]}


def _policy_names(cfg):
    return [p["name"] for p in cfg["fgac_policies"]]


def _policy(cfg, name):
    return next((p for p in cfg["fgac_policies"] if p["name"] == name), None)


# ---------------------------------------------------------------------------
# Keep / change / add / accept
# ---------------------------------------------------------------------------

def test_model_dropping_a_rule_keeps_it_with_one_line_per_rule(tmp_path):
    messages, cfg, sql = _rerun(tmp_path, {EMAIL: "email_mask"})

    assert _treatments(cfg)[AMOUNT] == "round_amount"
    assert _policy(cfg, "gr_mask_dev_fin_round_amount")["function_name"] == "mask_amount_rounded"
    assert "round_amount" in next(p for p in cfg["tag_policies"] if p["key"] == "gr_treatment")["values"]
    assert "FUNCTION mask_amount_rounded" in sql and "ROUND(val, -2)" in sql
    assert messages == [
        f"  kept reviewed rule {AMOUNT} → round_amount (model proposed removing it); {HINT}",
        f"  kept reviewed rule policy gr_mask_dev_fin_round_amount (model proposed removing it); {HINT}",
        "  kept reviewed rule function dev_fin.payments.mask_amount_rounded "
        f"(model proposed removing it); {HINT}",
    ]


def test_model_changing_a_rule_keeps_the_reviewed_version(tmp_path):
    messages, cfg, sql = _rerun(
        tmp_path, {EMAIL: "redact", AMOUNT: "round_amount"},
        principals=("viewers", "regional_analysts"),
        bodies={"mask_amount_rounded": "RETURN ROUND(val, 0);"},
    )

    assert _treatments(cfg) == {EMAIL: "email_mask", AMOUNT: "round_amount"}
    assert _policy(cfg, "gr_mask_dev_fin_round_amount")["to_principals"] == ["viewers"]
    assert _policy(cfg, "gr_mask_dev_fin_email_mask")["to_principals"] == ["viewers"]
    assert "ROUND(val, -2)" in sql and "ROUND(val, 0)" not in sql
    assert "FUNCTION mask_email" in sql
    assert f"  kept reviewed rule {EMAIL} → email_mask (model proposed changing it); {HINT}" in messages
    assert any("policy gr_mask_dev_fin_round_amount (model proposed changing it)" in m for m in messages)
    assert any("mask_amount_rounded (model proposed changing it)" in m for m in messages)


def test_new_column_is_added_and_reviewed_rules_untouched(tmp_path):
    messages, cfg, sql = _rerun(tmp_path, {**REVIEWED, PHONE: "phone_mask"})

    assert messages == []
    assert _treatments(cfg) == {EMAIL: "email_mask", AMOUNT: "round_amount", PHONE: "phone_mask"}
    assert _policy(cfg, "gr_mask_dev_fin_phone_mask")
    assert "FUNCTION mask_phone" in sql


def test_allow_rule_changes_accepts_the_new_draft(tmp_path):
    expected = tmp_path / "expected"
    _write_draft(expected, {EMAIL: "redact"})

    messages, cfg, sql = _rerun(tmp_path, {EMAIL: "redact"}, allow=True)

    assert (tmp_path / "generated" / "abac.auto.tfvars").read_text() == (
        expected / "abac.auto.tfvars").read_text()
    assert sql == (expected / "masking_functions.sql").read_text()
    assert _treatments(cfg) == {EMAIL: "redact"}
    assert any(AMOUNT in m and "accepted model change" in m for m in messages)


def test_first_run_has_no_reviewed_rules(tmp_path):
    assert load_reviewed_rules(tmp_path) is None
    # A genie-mode import (Phase 0) carries no rules either.
    (tmp_path / "abac.auto.tfvars").write_text('genie_space_configs = {\n  "A" = { title = "A" }\n}\n')
    assert load_reviewed_rules(tmp_path) is None


# ---------------------------------------------------------------------------
# Footprint: stale reviewed rules are dropped, not restored
# ---------------------------------------------------------------------------

DDL_WITHOUT_PAYMENTS = DDL.split("\n\n-- Table: dev_fin.payments.payments")[0]


def test_removed_table_drops_its_reviewed_rules_as_stale(tmp_path):
    messages, cfg, sql = _rerun(
        tmp_path, {EMAIL: "email_mask"},
        reviewed_kwargs={"overrides": {AMOUNT: "round_amount"}},
        ddl=DDL_WITHOUT_PAYMENTS,
    )

    assert _treatments(cfg) == {EMAIL: "email_mask"}
    assert "treatment_overrides" not in cfg
    assert _policy_names(cfg) == ["gr_mask_dev_fin_email_mask"]
    assert messages == [
        f"  dropped stale reviewed rule {AMOUNT} → round_amount (column no longer exists)",
        f"  dropped stale reviewed rule override {AMOUNT} → round_amount (column no longer exists)",
        "  dropped stale reviewed rule policy gr_mask_dev_fin_round_amount (its columns no longer exist)",
        "  dropped stale reviewed rule function dev_fin.payments.mask_amount_rounded "
        "(only stale policies used it)",
    ]
    assert "mask_amount_rounded" not in sql


def test_removed_column_is_stale_but_partial_footprint_keeps_unscanned_tables(tmp_path):
    ddl = DDL.replace("  email STRING,\n", "")
    messages, cfg, _ = _rerun(tmp_path, {AMOUNT: "round_amount"}, ddl=ddl)
    assert f"  dropped stale reviewed rule {EMAIL} → email_mask (column no longer exists)" in messages
    assert EMAIL not in _treatments(cfg)

    # SPACE= / --tables: a table outside this run's scan is not evidence of removal.
    messages, cfg, _ = _rerun(
        tmp_path, {EMAIL: "email_mask"}, ddl=DDL_WITHOUT_PAYMENTS, partial=True,
    )
    assert _treatments(cfg)[AMOUNT] == "round_amount"
    assert not any("stale" in m for m in messages)


def test_table_whose_columns_cannot_be_parsed_is_not_treated_as_stale(tmp_path):
    ddl = "-- Table: dev_fin.payments.customers\nCREATE TABLE dev_fin.payments.customers (\n" \
          "  email STRING\n);\n\nCREATE TABLE dev_fin.payments.payments\n"
    assert footprint_from_ddl(ddl)["dev_fin.payments.payments"] is None
    messages, cfg, _ = _rerun(tmp_path, {EMAIL: "email_mask"}, ddl=ddl)
    assert _treatments(cfg)[AMOUNT] == "round_amount"
    assert not any("stale" in m for m in messages)


# ---------------------------------------------------------------------------
# Protected targets: never both the reviewed rule and a model addition
# ---------------------------------------------------------------------------

def test_differently_named_model_mask_for_a_reviewed_column_is_discarded(tmp_path):
    messages, cfg, _ = _rerun(
        tmp_path, REVIEWED,
        policies=[
            _mask("gr_mask_dev_fin_email_mask", "email_mask", "mask_email"),
            _mask("mask_amounts_custom", "round_amount", "mask_amount_rounded"),
        ],
    )

    assert _policy_names(cfg) == ["gr_mask_dev_fin_email_mask", "gr_mask_dev_fin_round_amount"]
    assert messages == [
        f"  kept reviewed rule policy gr_mask_dev_fin_round_amount (model proposed removing it); {HINT}",
        f"  kept reviewed rule {AMOUNT} → round_amount (model proposed a conflicting policy "
        f"mask_amounts_custom); {HINT}",
    ]


def test_reviewed_targets_match_case_insensitively(tmp_path):
    shouted = AMOUNT.upper()
    custom = dict(_mask("mask_amounts_custom", "round_amount", "mask_amount_rounded"),
                  catalog="DEV_FIN")
    messages, cfg, _ = _rerun(
        tmp_path, {EMAIL: "email_mask", shouted: "round_amount"},
        policies=[_mask("gr_mask_dev_fin_email_mask", "email_mask", "mask_email"), custom],
        overrides={shouted: "redact"},
    )

    assert _treatments(cfg) == REVIEWED
    assert "treatment_overrides" not in cfg
    assert _policy_names(cfg) == ["gr_mask_dev_fin_email_mask", "gr_mask_dev_fin_round_amount"]
    assert (f"  kept reviewed rule {AMOUNT} → round_amount (model proposed a conflicting "
            f"policy mask_amounts_custom); {HINT}") in messages
    assert any("conflicting override → redact" in m for m in messages)
    assert not any("model proposed removing it" in m and AMOUNT in m for m in messages)


def test_model_mask_covering_only_new_columns_is_added(tmp_path):
    messages, cfg, _ = _rerun(
        tmp_path, {**REVIEWED, PHONE: "phone_mask"},
        policies=[
            _mask("gr_mask_dev_fin_email_mask", "email_mask", "mask_email"),
            _mask("gr_mask_dev_fin_round_amount", "round_amount", "mask_amount_rounded"),
            _mask("mask_phone_custom", "phone_mask", "mask_phone"),
        ],
    )
    assert messages == []
    assert "mask_phone_custom" in _policy_names(cfg)


def test_new_model_override_on_a_reviewed_column_is_discarded(tmp_path):
    messages, cfg, _ = _rerun(
        tmp_path, {EMAIL: "email_mask", AMOUNT: "redact"},
        overrides={AMOUNT: "redact"},
    )

    assert _treatments(cfg)[AMOUNT] == "round_amount"
    assert "treatment_overrides" not in cfg
    assert f"  kept reviewed rule {AMOUNT} → round_amount (model proposed changing it); {HINT}" in messages
    assert (f"  kept reviewed rule {AMOUNT} → round_amount (model proposed a conflicting override "
            f"→ redact); {HINT}") in messages


def test_reviewed_override_is_kept_and_new_column_override_is_added(tmp_path):
    messages, cfg, _ = _rerun(
        tmp_path, {**REVIEWED, PHONE: "redact"},
        reviewed_kwargs={"overrides": {AMOUNT: "round_amount"}},
        overrides={PHONE: "redact"},
    )
    overrides = {o["entity_name"]: o["treatment"] for o in cfg["treatment_overrides"]}
    assert overrides == {AMOUNT: "round_amount", PHONE: "redact"}
    assert any(f"override {AMOUNT} → round_amount (model proposed removing it)" in m for m in messages)


# ---------------------------------------------------------------------------
# Masking functions: renames, dangling references, SQL-aware comparison
# ---------------------------------------------------------------------------

def test_function_rename_restores_the_reviewed_function_the_policy_uses(tmp_path):
    messages, cfg, sql = _rerun(
        tmp_path, REVIEWED,
        policies=[
            _mask("gr_mask_dev_fin_email_mask", "email_mask", "mask_email"),
            _mask("gr_mask_dev_fin_round_amount", "round_amount", "mask_amount_round"),
        ],
        bodies={"mask_amount_round": "RETURN ROUND(val, -3);"},
    )

    assert _policy(cfg, "gr_mask_dev_fin_round_amount")["function_name"] == "mask_amount_rounded"
    assert "FUNCTION mask_amount_rounded" in sql and "ROUND(val, -2)" in sql
    assert any("mask_amount_rounded (model proposed removing it)" in m for m in messages)


def test_kept_policy_with_a_function_defined_nowhere_fails_clearly(tmp_path):
    reviewed_policies = [
        _mask("gr_mask_dev_fin_email_mask", "email_mask", "mask_email"),
        _mask("gr_mask_dev_fin_round_amount", "round_amount", "mask_custom_amount"),
    ]
    with pytest.raises(ValueError, match=(
        "reviewed policy gr_mask_dev_fin_round_amount uses function "
        "dev_fin.payments.mask_custom_amount.*--allow-rule-changes"
    )):
        _rerun(
            tmp_path, {EMAIL: "email_mask"},
            reviewed_kwargs={"policies": reviewed_policies, "functions": ["mask_email"]},
        )


def test_function_missing_from_new_sql_is_restored_for_unchanged_policy(tmp_path):
    messages, _, sql = _rerun(tmp_path, REVIEWED, functions=["mask_email"])
    assert "FUNCTION mask_amount_rounded" in sql
    assert any("mask_amount_rounded (model proposed removing it)" in m for m in messages)


TWO_SCHEMA_SQL = """USE CATALOG {first_catalog};
USE SCHEMA {first_schema};

CREATE OR REPLACE FUNCTION mask_email(val STRING)
RETURNS STRING
RETURN CONCAT('***@', SPLIT(val, '@')[1]);

CREATE OR REPLACE FUNCTION mask_amount_rounded(val STRING)
RETURNS STRING
RETURN ROUND(val, {first_digits});

USE CATALOG other_fin;
USE SCHEMA other_schema;

CREATE OR REPLACE FUNCTION mask_amount_rounded(val STRING)
RETURNS STRING
RETURN ROUND(val, -5);
"""


def _rerun_with_sql(tmp_path, reviewed_sql, new_sql, *, reviewed_policies=None, new_policies=None):
    _write_draft(tmp_path / "reviewed", REVIEWED, policies=reviewed_policies)
    (tmp_path / "reviewed" / "masking_functions.sql").write_text(reviewed_sql)
    reviewed = load_reviewed_rules(tmp_path / "reviewed")
    generated = tmp_path / "generated"
    _write_draft(generated, REVIEWED, policies=new_policies)
    (generated / "masking_functions.sql").write_text(new_sql)
    messages = keep_reviewed_rules(
        reviewed, generated / "abac.auto.tfvars", generated / "masking_functions.sql",
    )
    return messages, (generated / "masking_functions.sql").read_text()


def test_function_blocks_take_the_use_context_before_them_not_after():
    sql = TWO_SCHEMA_SQL.format(first_catalog="dev_fin", first_schema="payments", first_digits=-2)
    assert sorted(merge_space_configs._function_blocks_by_key(sql)) == [
        ("dev_fin", "payments", "mask_amount_rounded"),
        ("dev_fin", "payments", "mask_email"),
        ("other_fin", "other_schema", "mask_amount_rounded"),
    ]


def test_same_function_name_in_another_schema_does_not_satisfy_a_qualified_reference(tmp_path):
    reviewed_sql = TWO_SCHEMA_SQL.format(
        first_catalog="dev_fin", first_schema="payments", first_digits=-2)
    # The model keeps the policy but drops the dev_fin.payments copy of its function.
    new_sql = reviewed_sql.replace(
        "CREATE OR REPLACE FUNCTION mask_amount_rounded(val STRING)\nRETURNS STRING\n"
        "RETURN ROUND(val, -2);\n\n", "")

    messages, sql = _rerun_with_sql(tmp_path, reviewed_sql, new_sql)

    assert "ROUND(val, -2)" in sql and "ROUND(val, -5)" in sql
    assert messages == [
        "  kept reviewed rule function dev_fin.payments.mask_amount_rounded "
        f"(model proposed removing it); {HINT}"
    ]


def test_qualified_reference_defined_only_in_another_schema_fails_clearly(tmp_path):
    # Reviewed SQL defines mask_amount_rounded only in other_fin.other_schema,
    # but the policy calls dev_fin.payments.mask_amount_rounded.
    reviewed_sql = TWO_SCHEMA_SQL.format(
        first_catalog="dev_fin", first_schema="payments", first_digits=-2,
    ).replace("CREATE OR REPLACE FUNCTION mask_amount_rounded(val STRING)\nRETURNS STRING\n"
              "RETURN ROUND(val, -2);\n\n", "")
    changed = [
        _mask("gr_mask_dev_fin_email_mask", "email_mask", "mask_email"),
        _mask("gr_mask_dev_fin_round_amount", "round_amount", "mask_amount_rounded",
              principals=("viewers", "regional_analysts")),
    ]
    with pytest.raises(ValueError, match="dev_fin.payments.mask_amount_rounded"):
        _rerun_with_sql(tmp_path, reviewed_sql, reviewed_sql, new_policies=changed)


def test_unqualified_reference_matches_by_name_and_qualifiers_ignore_case(tmp_path):
    unqualified = _mask("gr_mask_dev_fin_round_amount", "round_amount", "mask_amount_rounded")
    unqualified.pop("function_catalog")
    unqualified.pop("function_schema")
    policies = [_mask("gr_mask_dev_fin_email_mask", "email_mask", "mask_email"), unqualified]
    other_only = TWO_SCHEMA_SQL.format(
        first_catalog="dev_fin", first_schema="payments", first_digits=-2,
    ).replace("CREATE OR REPLACE FUNCTION mask_amount_rounded(val STRING)\nRETURNS STRING\n"
              "RETURN ROUND(val, -2);\n\n", "")
    changed = [policies[0], dict(unqualified, to_principals=["viewers", "regional_analysts"])]
    messages, _ = _rerun_with_sql(
        tmp_path, other_only, other_only, reviewed_policies=policies, new_policies=changed,
    )
    assert messages == [
        f"  kept reviewed rule policy gr_mask_dev_fin_round_amount (model proposed changing it); {HINT}"
    ]

    # USE `DEV_FIN`.`Payments` defines the function dev_fin.payments references.
    mixed_case = TWO_SCHEMA_SQL.format(
        first_catalog="`DEV_FIN`", first_schema="Payments", first_digits=-2)
    messages, _ = _rerun_with_sql(tmp_path / "case", mixed_case, mixed_case)
    assert messages == []


@pytest.mark.parametrize("reviewed_body,new_body", [
    ("RETURN 'REDACTED';", "RETURN 'redacted';"),
    ("RETURN 'A  B';", "RETURN 'A B';"),
    ("RETURN \"X\";", "RETURN \"x\";"),
    ("RETURN `Col`;", "RETURN `col`;"),
    ("RETURN '-- not a comment';", "RETURN '-- not a COMMENT';"),
    ("RETURN '/* kept */';", "RETURN '/* KEPT */';"),
    ("RETURN 'it''s';", "RETURN 'it''S';"),
    ("RETURN 'a\\'b';", "RETURN 'a\\'B';"),
    ("RETURN 'unterminated;", "RETURN 'unterminated;"),
])
def test_sql_literal_differences_keep_the_reviewed_body(tmp_path, reviewed_body, new_body):
    _write_draft(tmp_path / "reviewed", REVIEWED, bodies={"mask_email": "RETURN 'x';"})
    reviewed_cfg, reviewed_sql = load_reviewed_rules(tmp_path / "reviewed")
    reviewed = (reviewed_cfg, reviewed_sql.replace("RETURN 'x';", reviewed_body))
    generated = tmp_path / "generated"
    _write_draft(generated, REVIEWED, bodies={"mask_email": new_body})

    messages = keep_reviewed_rules(
        reviewed, generated / "abac.auto.tfvars", generated / "masking_functions.sql",
    )

    assert messages == [
        f"  kept reviewed rule function dev_fin.payments.mask_email (model proposed changing it); {HINT}"
    ]
    assert reviewed_body in (generated / "masking_functions.sql").read_text()


@pytest.mark.parametrize("new_body", [
    "return   concat('***@',\n  SPLIT(val, '@')[1]);",
    "RETURN /* note */ CONCAT('***@', SPLIT(val, '@')[1]); -- trailing",
    "RETURN CONCAT('***@', /* a /* nested */ comment */ SPLIT(val, '@')[1]);",
])
def test_sql_case_whitespace_and_comments_outside_literals_are_not_changes(tmp_path, new_body):
    messages, _, _ = _rerun(tmp_path, REVIEWED, bodies={"mask_email": new_body})
    assert messages == []


def test_sql_tokens_preserve_literals_and_drop_comments():
    assert sql_tokens("SELECT 'A  b' /* x */ , `C d` -- y\n FROM T") == [
        "select", "'A  b'", ",", "`C d`", "from", "t",
    ]
    assert sql_tokens("AS $$ return 'X' $$") == ["as", "$$ return 'X' $$"]


# ---------------------------------------------------------------------------
# Reading the reviewed draft
# ---------------------------------------------------------------------------

def test_missing_reviewed_sql_is_an_error_when_policies_use_functions(tmp_path):
    _write_draft(tmp_path, REVIEWED)
    (tmp_path / "masking_functions.sql").unlink()
    with pytest.raises(ValueError, match="masking_functions.sql: missing.*--allow-rule-changes"):
        load_reviewed_rules(tmp_path)


@pytest.mark.parametrize("content", [
    b"\xff\xfe\x00 not utf-8",
    b"USE CATALOG dev_fin;\nCREATE FUNCTION f(v STRING) RETURNS STRING RETURN 'oops;\n",
    b"USE CATALOG dev_fin;\nCREATE FUNCTION f(v STRING) RETURNS STRING RETURN v /* open\n",
])
def test_unreadable_reviewed_sql_is_an_error(tmp_path, content):
    _write_draft(tmp_path, REVIEWED)
    (tmp_path / "masking_functions.sql").write_bytes(content)
    with pytest.raises(ValueError, match="cannot read the reviewed rules.*--allow-rule-changes"):
        load_reviewed_rules(tmp_path)


def test_unreadable_reviewed_abac_is_an_error(tmp_path):
    (tmp_path / "abac.auto.tfvars").write_text("tag_assignments = [ {\n")
    with pytest.raises(ValueError, match="--allow-rule-changes"):
        load_reviewed_rules(tmp_path)


# ---------------------------------------------------------------------------
# End to end: generate_abac.main() -> validation -> coverage gate
# ---------------------------------------------------------------------------

E2E_SQL = """```sql
USE CATALOG dev_fin;
USE SCHEMA payments;

CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CASE WHEN email IS NULL THEN NULL ELSE CONCAT('***@', SPLIT(email, '@')[1]) END;

CREATE OR REPLACE FUNCTION mask_amount_rounded(amount DECIMAL(18,2))
RETURNS DECIMAL(18,2)
RETURN ROUND(amount, -2);
```"""

# spend_limit: a reviewed rule that name-based DDL inference would not re-add,
# so a model drop really is a drop.
LIMIT = "dev_fin.payments.payments.spend_limit"
E2E_DDL = DDL.replace("  amount DECIMAL(18,2)", "  spend_limit DECIMAL(18,2)")
E2E_DDL_WITHOUT_PAYMENTS = DDL_WITHOUT_PAYMENTS
E2E_TAGS = {
    EMAIL: ("pii_level", "masked_email", "mask_email"),
    "email_generic": ("pii_level", "masked", "mask_pii_partial"),
    LIMIT: ("financial_sensitivity", "rounded_amounts", "mask_amount_rounded"),
}


def _model_response(columns):
    """columns: {column: E2E_TAGS key}"""
    assignments = "\n".join(
        f'  {{ entity_type = "columns", entity_name = "{c}", '
        f'tag_key = "{E2E_TAGS[k][0]}", tag_value = "{E2E_TAGS[k][1]}" }},'
        for c, k in columns.items()
    )
    policies = "\n".join(
        f'''  {{
    name = "mask_{c.rsplit('.', 1)[1]}"
    policy_type = "POLICY_TYPE_COLUMN_MASK"
    catalog = "dev_fin"
    to_principals = ["viewers"]
    match_condition = "hasTagValue('{E2E_TAGS[k][0]}', '{E2E_TAGS[k][1]}')"
    match_alias = "cols"
    function_name = "{E2E_TAGS[k][2]}"
    function_catalog = "dev_fin"
    function_schema = "payments"
  }},''' for c, k in columns.items()
    )
    return E2E_SQL + f'''

```hcl
groups = {{
  "payments_ops" = {{ description = "Full access" }}
  "viewers" = {{ description = "Masked" }}
}}

tag_policies = [
  {{ key = "pii_level", description = "PII", values = ["masked_email", "masked"] }},
  {{ key = "financial_sensitivity", description = "Fin", values = ["rounded_amounts"] }},
]

tag_assignments = [
{assignments}
]

fgac_policies = [
{policies}
]
```'''


@pytest.fixture
def env_dir(tmp_path, monkeypatch):
    (tmp_path / "auth.auto.tfvars").write_text("")
    (tmp_path / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "Payments", uc_tables = '
        '["dev_fin.payments.customers", "dev_fin.payments.payments"] }]\n'
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(generate_abac, "WORK_DIR", tmp_path)
    monkeypatch.setattr(generate_abac, "_fetch_live_classification_source", lambda *a, **k: None)
    monkeypatch.setattr(generate_abac, "_fetch_live_tag_policy_values", lambda: {})
    monkeypatch.setattr(generate_abac, "list_account_group_names", lambda cfg: None)
    monkeypatch.setattr(generate_abac, "configure_databricks_env", lambda cfg: None)
    return tmp_path


def _generate(env_dir, monkeypatch, columns, *extra, ddl=E2E_DDL):
    monkeypatch.setattr(generate_abac, "fetch_tables_from_databricks",
                        lambda refs, cfg: (ddl, [("dev_fin", "payments")]))
    monkeypatch.setattr(generate_abac, "call_with_retries",
                        lambda *a, **k: _model_response(columns))
    monkeypatch.setattr(sys, "argv", [
        "generate_abac.py", "--auth-file", str(env_dir / "auth.auto.tfvars"),
        "--groups", "payments_ops,viewers", "--out-dir", str(env_dir / "generated"), *extra,
    ])
    generate_abac.main()  # exits non-zero if validation fails


def _coverage_gate(env_dir):
    result = subprocess.run(
        [sys.executable, str(SHARED / "validate_abac.py"), "--coverage-gate",
         "generated/abac.auto.tfvars", "generated/masking_functions.sql"],
        cwd=env_dir, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def _generated(env_dir):
    cfg = hcl2.loads((env_dir / "generated" / "abac.auto.tfvars").read_text())
    return {
        a["entity_name"]: a["tag_value"]
        for a in cfg["tag_assignments"] if a["tag_key"] == "gr_treatment"
    }, _policy_names(cfg)


def test_end_to_end_rerun_keeps_dropped_rule_through_validation_and_gate(env_dir, monkeypatch, capfd):
    _generate(env_dir, monkeypatch, {EMAIL: EMAIL, LIMIT: LIMIT})
    assert "Coverage check: 2" in _coverage_gate(env_dir)
    first = _generated(env_dir)
    capfd.readouterr()

    _generate(env_dir, monkeypatch, {EMAIL: EMAIL})  # the model drops round_amount
    out = capfd.readouterr().out
    assert f"kept reviewed rule {LIMIT} → round_amount (model proposed removing it); {HINT}" in out
    assert "RESULT: PASS" in out
    assert _generated(env_dir) == first
    assert "Coverage check: 2" in _coverage_gate(env_dir)

    # The table is removed from the footprint: its reviewed rules are stale.
    _generate(env_dir, monkeypatch, {EMAIL: EMAIL}, ddl=E2E_DDL_WITHOUT_PAYMENTS)
    out = capfd.readouterr().out
    assert f"dropped stale reviewed rule {LIMIT} → round_amount (column no longer exists)" in out
    assert "dropped stale reviewed rule policy gr_mask_dev_fin_round_amount" in out
    assert _generated(env_dir) == ({EMAIL: "email_partial"}, ["gr_mask_dev_fin_email_partial"])
    assert "Coverage check: 1" in _coverage_gate(env_dir)


def test_end_to_end_space_rerun_keeps_reviewed_rule(env_dir, monkeypatch, capfd):
    _generate(env_dir, monkeypatch, {EMAIL: EMAIL, LIMIT: LIMIT})
    first = _generated(env_dir)
    capfd.readouterr()

    # SPACE=: the per-space merge would let the model's email change win.
    _generate(env_dir, monkeypatch, {EMAIL: "email_generic", LIMIT: LIMIT}, "--space", "Payments")
    out = capfd.readouterr().out
    assert f"kept reviewed rule {EMAIL} → email_partial (model proposed changing it); {HINT}" in out
    assert "RESULT: PASS" in out
    treatments, policies = _generated(env_dir)
    assert treatments == first[0]
    assert set(first[1]) <= set(policies)
    _coverage_gate(env_dir)


def test_end_to_end_missing_reviewed_sql_stops_unless_flag(env_dir, monkeypatch, capfd):
    _generate(env_dir, monkeypatch, {EMAIL: EMAIL, LIMIT: LIMIT})
    (env_dir / "generated" / "masking_functions.sql").unlink()
    capfd.readouterr()

    def model_must_not_run(*_a, **_k):
        raise AssertionError("model must not be called")

    monkeypatch.setattr(generate_abac, "call_with_retries", model_must_not_run)
    monkeypatch.setattr(sys, "argv", [
        "generate_abac.py", "--auth-file", str(env_dir / "auth.auto.tfvars"),
        "--groups", "payments_ops,viewers", "--out-dir", str(env_dir / "generated"),
    ])
    with pytest.raises(SystemExit) as exc:
        generate_abac.main()
    assert exc.value.code == 1
    assert "masking_functions.sql: missing" in capfd.readouterr().out

    _generate(env_dir, monkeypatch, {EMAIL: EMAIL}, "--allow-rule-changes")
    assert _generated(env_dir)[0] == {EMAIL: "email_partial"}
    _coverage_gate(env_dir)


def test_delta_mode_never_reads_or_merges_reviewed_rules(monkeypatch, tmp_path):
    def must_not_run(*_a, **_k):
        raise AssertionError("--delta must not use the sticky-rules merge")

    monkeypatch.setattr(merge_space_configs, "load_reviewed_rules", must_not_run)
    monkeypatch.setattr(merge_space_configs, "keep_reviewed_rules", must_not_run)
    ran = []
    monkeypatch.setattr(generate_abac, "_run_delta_mode", lambda auth: ran.append(auth))
    monkeypatch.setattr(sys, "argv", [
        "generate_abac.py", "--delta", "--auth-file", str(tmp_path / "auth.auto.tfvars"),
    ])
    generate_abac.main()
    assert ran
