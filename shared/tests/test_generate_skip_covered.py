"""generate drafts rules only for columns the reviewed rulebook doesn't cover.

These run the real generate_abac.main() path; only the Databricks fetches and
the model client are faked. The fake model records each prompt and drafts a
rule for every sensitive column it is shown, so whatever reaches the model is
visible in the prompt and in the draft.
"""
import sys
from pathlib import Path

import hcl2
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import generate_abac  # noqa: E402
import scripts.merge_space_configs as merge_space_configs  # noqa: E402
from scripts.merge_space_configs import _render_value, merge_into_assembled  # noqa: E402
from validate_abac import parse_ddl_columns  # noqa: E402

SHARED = Path(__file__).parent.parent

EMAIL = "dev_fin.payments.customers.email"
PHONE = "dev_fin.payments.customers.phone"
LIMIT = "dev_fin.payments.payments.spend_limit"
CONTACT_EMAIL = "dev_fin.payments.contacts.contact_email"

CUSTOMERS = """-- Table: dev_fin.payments.customers
CREATE TABLE dev_fin.payments.customers (
  customer_id BIGINT,
  email STRING COMMENT 'Customer email, e.g. jane@example.com',
  phone STRING
);"""
PAYMENTS = """-- Table: dev_fin.payments.payments
CREATE TABLE dev_fin.payments.payments (
  payment_id BIGINT,
  spend_limit DECIMAL(18,2)
);"""
CONTACTS = """-- Table: dev_fin.payments.contacts
CREATE TABLE dev_fin.payments.contacts (
  contact_id BIGINT,
  contact_email STRING
);"""
CRM = """-- Table: dev_fin.crm.contacts
CREATE TABLE dev_fin.crm.contacts (
  email STRING,
  phone STRING
);"""
DDL_BY_TABLE = {
    "dev_fin.payments.customers": CUSTOMERS,
    "dev_fin.payments.payments": PAYMENTS,
    "dev_fin.payments.contacts": CONTACTS,
    "dev_fin.crm.contacts": CRM,
}

# column name -> (tag_key, tag_value, masking function, its SQL type)
SENSITIVE = {
    "email": ("pii_level", "masked_email", "mask_email", "STRING"),
    "contact_email": ("pii_level", "masked_email", "mask_email", "STRING"),
    "phone": ("pii_level", "masked_phone", "mask_phone", "STRING"),
    "spend_limit": ("financial_sensitivity", "rounded_amounts", "mask_amount_rounded", "DECIMAL(18,2)"),
}
BODIES = {
    "mask_email": "CASE WHEN v IS NULL THEN NULL ELSE CONCAT('***@', SPLIT(v, '@')[1]) END",
    "mask_phone": "CASE WHEN v IS NULL THEN NULL ELSE CONCAT('***', RIGHT(v, 4)) END",
    "mask_amount_rounded": "ROUND(v, -2)",
}
ALTERED_BODIES = {
    "mask_email": "CASE WHEN v IS NULL THEN NULL ELSE '***' END",
    "mask_phone": "CASE WHEN v IS NULL THEN NULL ELSE '***' END",
    "mask_amount_rounded": "ROUND(v, 0)",
}


def tables_section(prompt):
    """The part of the prompt that carries the tables (the template mentions
    'email' etc. in its own instructions)."""
    return prompt.split("### MY TABLES", 1)[1]


def prompt_columns(prompt):
    return {c.lower() for c in parse_ddl_columns(tables_section(prompt))}


class FakeModel:
    """Stands in for call_with_retries; drafts rules for the columns it sees."""

    def __init__(self, principals=("viewers",), bodies=BODIES):
        self.prompts = []
        self.principals = list(principals)
        self.bodies = bodies

    def __call__(self, _call_fn, prompt, _model, _max_retries):
        self.prompts.append(prompt)
        seen = [c for c in parse_ddl_columns(tables_section(prompt))
                if c.rsplit(".", 1)[1] in SENSITIVE]
        functions = sorted({SENSITIVE[c.rsplit(".", 1)[1]][2:] for c in seen})
        catalog_schemas = sorted({tuple(c.split(".")[:2]) for c in seen}) or [("dev_fin", "payments")]
        sql = "\n\n".join(
            f"USE CATALOG {cat};\nUSE SCHEMA {sch};\n\n" + "\n\n".join(
                f"CREATE OR REPLACE FUNCTION {fn}(v {typ})\nRETURNS {typ}\n"
                f"RETURN {self.bodies[fn]};"
                for fn, typ in functions
            )
            for cat, sch in catalog_schemas
        ) if functions else "-- nothing to mask"
        assignments = [
            {"entity_type": "columns", "entity_name": c,
             "tag_key": SENSITIVE[c.rsplit(".", 1)[1]][0],
             "tag_value": SENSITIVE[c.rsplit(".", 1)[1]][1]}
            for c in seen
        ]
        policies = [
            {
                "name": f"mask_{c.replace('.', '_')}",
                "policy_type": "POLICY_TYPE_COLUMN_MASK",
                "catalog": c.split(".")[0],
                "to_principals": self.principals,
                "match_condition": "hasTagValue('{}', '{}')".format(*SENSITIVE[c.rsplit(".", 1)[1]][:2]),
                "match_alias": "cols",
                "function_name": SENSITIVE[c.rsplit(".", 1)[1]][2],
                "function_catalog": c.split(".")[0],
                "function_schema": c.split(".")[1],
            }
            for c in seen
        ]
        hcl = (
            'groups = {\n  "payments_ops" = { description = "Full access" }\n'
            '  "viewers" = { description = "Masked" }\n}\n\n'
            'tag_policies = [\n'
            '  { key = "pii_level", description = "PII", values = ["masked_email", "masked_phone"] },\n'
            '  { key = "financial_sensitivity", description = "Fin", values = ["rounded_amounts"] },\n'
            ']\n\n'
            f"tag_assignments = {_render_value(assignments)}\n\n"
            f"fgac_policies = {_render_value(policies)}\n"
        )
        return f"```sql\n{sql}\n```\n\n```hcl\n{hcl}```"


def _model_must_not_run(*_a, **_k):
    raise AssertionError("the governance model must not be called")


@pytest.fixture
def env_dir(tmp_path, monkeypatch):
    (tmp_path / "auth.auto.tfvars").write_text("")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(generate_abac, "WORK_DIR", tmp_path)
    monkeypatch.setattr(generate_abac, "_fetch_live_classification_source", lambda *a, **k: None)
    monkeypatch.setattr(generate_abac, "_fetch_live_tag_policy_values", lambda: {})
    monkeypatch.setattr(generate_abac, "list_account_group_names", lambda cfg: None)
    monkeypatch.setattr(generate_abac, "configure_databricks_env", lambda cfg: None)

    def fetch(refs, _cfg):
        ddl = "\n\n".join(DDL_BY_TABLE[r] for r in refs)
        return ddl, sorted({tuple(r.split(".")[:2]) for r in refs})

    monkeypatch.setattr(generate_abac, "fetch_tables_from_databricks", fetch)
    return tmp_path


def _spaces(env_dir, spaces):
    """spaces: {name: [tables]}"""
    body = ",\n".join(
        f'  {{ name = "{name}", acl_groups = ["viewers"], '
        f'uc_tables = [{", ".join(chr(34) + t + chr(34) for t in tables)}] }}'
        for name, tables in spaces.items()
    )
    (env_dir / "env.auto.tfvars").write_text(f"genie_spaces = [\n{body}\n]\n")


def _generate(env_dir, monkeypatch, model, *extra):
    monkeypatch.setattr(generate_abac, "call_with_retries", model)
    monkeypatch.setattr(sys, "argv", [
        "generate_abac.py", "--auth-file", str(env_dir / "auth.auto.tfvars"),
        "--groups", "payments_ops,viewers", "--out-dir", str(env_dir / "generated"), *extra,
    ])
    generate_abac.main()  # exits non-zero if validation fails


def _rules(env_dir):
    cfg = hcl2.loads((env_dir / "generated" / "abac.auto.tfvars").read_text())
    treatments = {
        a["entity_name"]: a["tag_value"]
        for a in cfg.get("tag_assignments") or [] if a["tag_key"] == "gr_treatment"
    }
    policies = {p["name"]: p for p in cfg.get("fgac_policies") or []}
    return treatments, policies


def _functions(env_dir):
    sql = (env_dir / "generated" / "masking_functions.sql").read_text()
    return merge_space_configs._function_blocks_by_key(sql)


def _coverage_gate(env_dir):
    import subprocess
    result = subprocess.run(
        [sys.executable, str(SHARED / "validate_abac.py"), "--coverage-gate",
         "generated/abac.auto.tfvars", "generated/masking_functions.sql"],
        cwd=env_dir, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def _agent_a_live(env_dir, monkeypatch, capfd):
    _spaces(env_dir, {"Payments": ["dev_fin.payments.customers", "dev_fin.payments.payments"]})
    model = FakeModel()
    _generate(env_dir, monkeypatch, model)
    assert {EMAIL, PHONE, LIMIT} <= prompt_columns(model.prompts[0])
    capfd.readouterr()
    return _rules(env_dir), _functions(env_dir)


# ---------------------------------------------------------------------------
# Adding agent B that shares agent A's table
# ---------------------------------------------------------------------------

def test_agent_sharing_a_live_table_sends_no_covered_columns(env_dir, monkeypatch, capfd):
    (treatments_a, policies_a), functions_a = _agent_a_live(env_dir, monkeypatch, capfd)
    assert {EMAIL, PHONE, LIMIT} <= set(treatments_a)

    _spaces(env_dir, {
        "Payments": ["dev_fin.payments.customers", "dev_fin.payments.payments"],
        "Risk": ["dev_fin.payments.customers"],
    })
    model = FakeModel(principals=("payments_ops",), bodies=ALTERED_BODIES)
    _generate(env_dir, monkeypatch, model, "--space", "Risk")
    out = capfd.readouterr().out

    [prompt] = model.prompts
    sent = prompt_columns(prompt)
    assert EMAIL not in sent and PHONE not in sent
    assert "dev_fin.payments.customers.customer_id" in sent
    assert "jane@example.com" not in prompt  # a covered column's comment
    assert "governance: 2 of 3 column(s) already covered by reviewed rules" in out
    assert "kept reviewed rule" not in out  # the sticky merge had nothing to restore
    treatments, policies = _rules(env_dir)
    assert treatments == treatments_a
    assert {name: policies[name] for name in policies_a} == policies_a
    assert _functions(env_dir) == functions_a
    _coverage_gate(env_dir)


def test_new_uncovered_column_is_drafted_alone_and_reuses_the_existing_policy(
        env_dir, monkeypatch, capfd):
    (treatments_a, policies_a), functions_a = _agent_a_live(env_dir, monkeypatch, capfd)
    email_policy = next(n for n, p in policies_a.items() if "email_partial" in p["match_condition"])

    _spaces(env_dir, {
        "Payments": ["dev_fin.payments.customers", "dev_fin.payments.payments"],
        "Risk": ["dev_fin.payments.customers", "dev_fin.payments.contacts"],
    })
    # The model would mask the new column for other principals with another body.
    model = FakeModel(principals=("payments_ops",), bodies=ALTERED_BODIES)
    _generate(env_dir, monkeypatch, model, "--space", "Risk")
    out = capfd.readouterr().out

    [prompt] = model.prompts
    sent = prompt_columns(prompt)
    assert {c for c in sent if c.rsplit(".", 1)[1] in SENSITIVE} == {CONTACT_EMAIL}
    assert "kept reviewed rule" not in out
    treatments, policies = _rules(env_dir)
    assert treatments == {**treatments_a, CONTACT_EMAIL: "email_partial"}
    # The existing (catalog, treatment) policy now also covers the new column,
    # unchanged; no second policy or function was created for it.
    assert policies == policies_a
    assert [n for n, p in policies.items() if "email_partial" in p["match_condition"]] == [email_policy]
    assert _functions(env_dir) == functions_a
    assert "Coverage check: 4" in _coverage_gate(env_dir)


def test_full_rerun_with_a_new_table_drafts_only_the_new_columns(env_dir, monkeypatch, capfd):
    (treatments_a, policies_a), functions_a = _agent_a_live(env_dir, monkeypatch, capfd)

    _spaces(env_dir, {
        "Payments": ["dev_fin.payments.customers", "dev_fin.payments.payments"],
        "Risk": ["dev_fin.payments.contacts"],
    })
    model = FakeModel(principals=("payments_ops",), bodies=ALTERED_BODIES)
    _generate(env_dir, monkeypatch, model)
    out = capfd.readouterr().out

    sent = prompt_columns(model.prompts[0])
    assert {c for c in sent if c.rsplit(".", 1)[1] in SENSITIVE} == {CONTACT_EMAIL}
    assert "dev_fin.payments.payments.payment_id" in sent
    assert "kept reviewed rule" not in out
    treatments, policies = _rules(env_dir)
    assert treatments == {**treatments_a, CONTACT_EMAIL: "email_partial"}
    assert policies == policies_a
    assert _functions(env_dir) == functions_a
    _coverage_gate(env_dir)


def test_new_treatment_gets_its_own_policy_and_function(env_dir, monkeypatch, capfd):
    _spaces(env_dir, {"Payments": ["dev_fin.payments.customers"]})
    _generate(env_dir, monkeypatch, FakeModel())
    treatments_a, policies_a = _rules(env_dir)
    functions_a = _functions(env_dir)
    capfd.readouterr()

    _spaces(env_dir, {
        "Payments": ["dev_fin.payments.customers"],
        "Risk": ["dev_fin.payments.payments"],
    })
    _generate(env_dir, monkeypatch, FakeModel(), "--space", "Risk")
    treatments, policies = _rules(env_dir)
    assert treatments == {**treatments_a, LIMIT: "round_amount"}
    assert {n: policies[n] for n in policies_a} == policies_a
    [new] = set(policies) - set(policies_a)
    assert "round_amount" in policies[new]["match_condition"]
    functions = _functions(env_dir)
    assert {k: functions[k] for k in functions_a} == functions_a
    assert any(k[2] == "mask_amount_rounded" for k in functions)
    _coverage_gate(env_dir)


# ---------------------------------------------------------------------------
# Nothing uncovered: no governance model call at all
# ---------------------------------------------------------------------------

def _genie_agent(monkeypatch, instructions):
    def fetch(space_id, _cfg, quick_check_only=False):
        return (["dev_fin.crm.contacts"],
                {"title": "Contacts", "description": "CRM contacts",
                 "instructions": instructions},
                "Contacts", True)
    monkeypatch.setattr(generate_abac, "fetch_tables_from_genie_space", fetch)


def test_everything_covered_skips_the_model_and_still_imports_genie_config(
        env_dir, monkeypatch, capfd):
    (env_dir / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "Contacts", genie_space_id = "01f0contacts" }]\n'
    )
    _genie_agent(monkeypatch, "v1 instructions")
    _generate(env_dir, monkeypatch, FakeModel())
    rules_a, functions_a = _rules(env_dir), _functions(env_dir)
    assert set(rules_a[0]) == {"dev_fin.crm.contacts.email", "dev_fin.crm.contacts.phone"}
    capfd.readouterr()

    _genie_agent(monkeypatch, "v2 instructions")
    _generate(env_dir, monkeypatch, _model_must_not_run)
    out = capfd.readouterr().out
    assert "governance: all 2 columns already covered by reviewed rules — no draft needed" in out
    assert "RESULT: PASS" in out
    cfg = hcl2.loads((env_dir / "generated" / "abac.auto.tfvars").read_text())
    assert cfg["genie_space_configs"]["Contacts"]["instructions"] == "v2 instructions"
    assert _rules(env_dir) == rules_a and _functions(env_dir) == functions_a
    _coverage_gate(env_dir)

    # SPACE= over a fully covered footprint skips the model too.
    _genie_agent(monkeypatch, "v3 instructions")
    _generate(env_dir, monkeypatch, _model_must_not_run, "--space", "Contacts")
    out = capfd.readouterr().out
    assert "governance: all 2 columns already covered by reviewed rules — no draft needed" in out
    cfg = hcl2.loads((env_dir / "generated" / "abac.auto.tfvars").read_text())
    assert cfg["genie_space_configs"]["Contacts"]["instructions"] == "v3 instructions"
    assert _rules(env_dir) == rules_a and _functions(env_dir) == functions_a
    _coverage_gate(env_dir)


def test_everything_covered_dry_run_prints_no_prompt(env_dir, monkeypatch, capfd):
    (env_dir / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "Contacts", genie_space_id = "01f0contacts" }]\n'
    )
    _genie_agent(monkeypatch, "v1 instructions")
    _generate(env_dir, monkeypatch, FakeModel())
    capfd.readouterr()
    with pytest.raises(SystemExit) as exc:
        _generate(env_dir, monkeypatch, _model_must_not_run, "--dry-run")
    assert exc.value.code == 0
    out = capfd.readouterr().out
    assert "no draft needed" in out and "### MY TABLES" not in out


# ---------------------------------------------------------------------------
# --allow-rule-changes re-drafts everything
# ---------------------------------------------------------------------------

def test_allow_rule_changes_sends_covered_columns(env_dir, monkeypatch, capfd):
    _agent_a_live(env_dir, monkeypatch, capfd)
    model = FakeModel()
    _generate(env_dir, monkeypatch, model, "--allow-rule-changes")
    assert {EMAIL, PHONE, LIMIT} <= prompt_columns(model.prompts[0])
    assert "already covered" not in capfd.readouterr().out


def test_allow_rule_changes_sends_covered_columns_even_when_all_covered(env_dir, monkeypatch, capfd):
    (env_dir / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "Contacts", genie_space_id = "01f0contacts" }]\n'
    )
    _genie_agent(monkeypatch, "v1 instructions")
    _generate(env_dir, monkeypatch, FakeModel())
    model = FakeModel()
    _generate(env_dir, monkeypatch, model, "--allow-rule-changes")
    assert prompt_columns(model.prompts[0]) == {"dev_fin.crm.contacts.email", "dev_fin.crm.contacts.phone"}


# ---------------------------------------------------------------------------
# Per-space combine fails closed on conflicting drafts
# ---------------------------------------------------------------------------

def _mask(name, treatment, function, principals=("viewers",)):
    return {
        "name": name, "policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "dev_fin",
        "to_principals": list(principals),
        "match_condition": f"hasTagValue('gr_treatment', '{treatment}')",
        "match_alias": f"gr_treatment_{treatment}", "function_name": function,
        "function_catalog": "dev_fin", "function_schema": "payments",
    }


def _write(directory, assignments, policies, functions, overrides=None):
    directory.mkdir(parents=True, exist_ok=True)
    cfg = {
        "tag_policies": [{"key": "gr_treatment", "values": ["email_partial", "redact"]}],
        "tag_assignments": [
            {"entity_type": "columns", "entity_name": c, "tag_key": "gr_treatment", "tag_value": t}
            for c, t in assignments.items()
        ],
        "fgac_policies": policies,
    }
    if overrides:
        cfg["treatment_overrides"] = [{"entity_name": c, "treatment": t} for c, t in overrides.items()]
    (directory / "abac.auto.tfvars").write_text("".join(
        f"{section} = {_render_value(items)}\n\n" for section, items in cfg.items()
    ))
    (directory / "masking_functions.sql").write_text(
        "USE CATALOG dev_fin;\nUSE SCHEMA payments;\n\n" + "\n\n".join(
            f"CREATE OR REPLACE FUNCTION {name}(v STRING)\nRETURNS STRING\nRETURN {body};"
            for name, body in functions.items()
        ) + "\n"
    )


ASSEMBLED = dict(
    assignments={EMAIL: "email_partial"},
    policies=[_mask("gr_mask_dev_fin_email_partial", "email_partial", "mask_email")],
    functions={"mask_email": "'***'"},
)


@pytest.mark.parametrize("space, conflict", [
    (dict(ASSEMBLED, assignments={EMAIL: "redact"}),
     f"{EMAIL} gr_treatment: 'email_partial' (assembled) vs 'redact' (space risk)"),
    (dict(ASSEMBLED, overrides={EMAIL: "redact"}),
     f"treatment override {EMAIL}: 'email_partial' (assembled) vs 'redact' (space risk)"),
    (dict(ASSEMBLED, policies=[_mask("gr_mask_dev_fin_email_partial", "email_partial", "mask_email",
                                     principals=("payments_ops",))]),
     "policy gr_mask_dev_fin_email_partial differs"),
    (dict(ASSEMBLED, functions={"mask_email": "'[hidden]'"}),
     "function dev_fin.payments.mask_email differs"),
])
def test_conflicting_per_space_drafts_fail_closed(tmp_path, space, conflict):
    generated = tmp_path / "generated"
    overrides = {EMAIL: "email_partial"} if "overrides" in space else None
    _write(generated, **dict(ASSEMBLED, overrides=overrides))
    _write(generated / "spaces" / "risk", **space)
    before = {p.name: p.read_text() for p in generated.glob("*.*")}

    with pytest.raises(ValueError) as exc:
        merge_into_assembled(generated, "risk")
    assert conflict in str(exc.value)
    assert "--allow-rule-changes" in str(exc.value)
    assert {p.name: p.read_text() for p in generated.glob("*.*")} == before

    merge_into_assembled(generated, "risk", allow_changes=True)  # today's behavior


def test_identical_per_space_rules_merge_without_conflict(tmp_path):
    generated = tmp_path / "generated"
    _write(generated, **ASSEMBLED)
    _write(generated / "spaces" / "risk", **ASSEMBLED)
    merge_into_assembled(generated, "risk")


def test_merge_script_reports_the_conflict_and_exits_nonzero(tmp_path):
    import subprocess
    generated = tmp_path / "generated"
    _write(generated, **ASSEMBLED)
    _write(generated / "spaces" / "risk", **dict(ASSEMBLED, assignments={EMAIL: "redact"}))
    script = SHARED / "scripts" / "merge_space_configs.py"
    result = subprocess.run([sys.executable, str(script), str(generated), "risk"],
                            capture_output=True, text=True)
    assert result.returncode == 1
    assert "ERROR: per-space draft for 'risk' conflicts with the assembled rules" in result.stderr
    result = subprocess.run([sys.executable, str(script), str(generated), "risk",
                             "--allow-rule-changes"], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
