"""Fixing a coverage gap prod found: real `make` runs against fake Unity Catalog data.

Prod's classifier tags a column with a class.* value dev never saw. Two fixes:

  a) reuse an existing mask: make scaffold-treatments ENV=prod TREATMENT=<name>
     maps the class to it, then the prod pipeline passes with no dev change;
  b) a new kind of mask: make scaffold-treatments ENV=prod adds a REVIEW stub,
     make materialize-treatment ENV=dev TREATMENT=<new> adds its mask to dev's
     rules without any dev column, and promote-to carries it to prod.

Every step is the real target through aws/Makefile -> Makefile.shared, run on a
copy of shared/ (so treatment_config.json edits stay in the test;
GENIERAILS_TEST_SHARED_SRC copies another checkout's shared/ instead, e.g. to
show these fail on an older main). Only the
Databricks SDK is faked (on PYTHONPATH): it answers the class.* tag and DDL
reads from a JSON file.
"""

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import hcl2
import pytest

pytestmark = pytest.mark.gnu_make

REPO = Path(__file__).resolve().parents[2]
TABLE = "finance.customers"
DEV_TABLE, PROD_TABLE = f"dev_fin.{TABLE}", f"prod_fin.{TABLE}"
COLUMNS = [["customer_id", "BIGINT"], ["email", "STRING"], ["cvv", "STRING"],
           ["vat_number", "STRING"], ["vat_amount", "DECIMAL(10,2)"]]

FAKE_SDK = '''import json, os, types

def _uc():
    with open(os.environ["FAKE_UC"]) as handle:
        return json.load(handle)

class _Statements:
    def execute_statement(self, statement, warehouse_id=None, wait_timeout=None):
        from .service.sql import StatementState
        if _uc().get("fail_sql"):
            raise RuntimeError("PERMISSION_DENIED: no SELECT on system.information_schema")
        if "data_classification.results" in statement:
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
        rows = [row for row in _uc()["tags"] if f"'{row[0]}'" in statement or row[0] in statement]
        if "LIKE 'class.%'" in statement:
            rows = [row for row in rows if row[4].lower().startswith("class.")]
        return types.SimpleNamespace(
            statement_id="s", status=types.SimpleNamespace(state=StatementState.SUCCEEDED),
            result=types.SimpleNamespace(data_array=rows))

class _Tables:
    def get(self, full_name):
        catalog, schema, name = full_name.split(".")
        columns = [types.SimpleNamespace(name=c, type_text=t, comment=None)
                   for c, t in _uc()["tables"][full_name]]
        return types.SimpleNamespace(full_name=full_name, catalog_name=catalog, schema_name=schema,
                                     name=name, columns=columns, comment=None)

class _Warehouses:
    def list(self):
        return [types.SimpleNamespace(id="wh")]

class WorkspaceClient:
    def __init__(self, *args, **kwargs):
        self.statement_execution = _Statements()
        self.tables = _Tables()
        self.warehouses = _Warehouses()
'''
FAKE_SQL = '''import enum
class StatementState(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
'''

# What make generate + make rehearse leave in dev: email gets a partial mask,
# cvv full redaction, so prod has both masks after promotion.
DEV_ABAC = f'''groups = {{
  "analysts" = {{ description = "Analysts" }}
}}

tag_policies = [
  {{ key = "gr_treatment", description = "GenieRails treatment", values = ["redact", "email_partial"] }}
]

tag_assignments = [
  {{ entity_type = "columns", entity_name = "{DEV_TABLE}.cvv", tag_key = "gr_treatment", tag_value = "redact" }},
  {{ entity_type = "columns", entity_name = "{DEV_TABLE}.email", tag_key = "gr_treatment", tag_value = "email_partial" }}
]

fgac_policies = [
  {{
    name             = "gr_mask_dev_fin_redact"
    policy_type      = "POLICY_TYPE_COLUMN_MASK"
    catalog          = "dev_fin"
    to_principals    = ["analysts"]
    comment          = "GenieRails treatment redact; strictest-wins derivation"
    match_condition  = "hasTagValue('gr_treatment', 'redact')"
    match_alias      = "gr_treatment_redact"
    function_name    = "mask_redact"
    function_catalog = "dev_fin"
    function_schema  = "finance"
  }},
  {{
    name             = "gr_mask_dev_fin_email_partial"
    policy_type      = "POLICY_TYPE_COLUMN_MASK"
    catalog          = "dev_fin"
    to_principals    = ["analysts"]
    comment          = "GenieRails treatment email_partial; strictest-wins derivation"
    match_condition  = "hasTagValue('gr_treatment', 'email_partial')"
    match_alias      = "gr_treatment_email_partial"
    function_name    = "mask_email"
    function_catalog = "dev_fin"
    function_schema  = "finance"
  }}
]
'''
DEV_SQL = '''USE CATALOG dev_fin;
USE SCHEMA finance;

CREATE OR REPLACE FUNCTION mask_redact(input STRING)
RETURNS STRING
RETURN CASE WHEN input IS NULL THEN NULL ELSE '[REDACTED]' END;

CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CASE WHEN email IS NULL THEN NULL ELSE concat('***@', split_part(email, '@', 2)) END;

CREATE OR REPLACE FUNCTION mask_pii_partial(input STRING)
RETURNS STRING
RETURN CASE WHEN input IS NULL THEN NULL ELSE concat(left(input, 1), '***') END;
'''


def _clean_env():
    """The caller's env minus make state, with pip unable to install anything.

    generate_abac.py pip-installs a missing databricks-sdk at import; the fake
    SDK provides what it checks for, and this keeps any slip from reaching an
    index or the real Python.
    """
    env = {k: v for k, v in os.environ.items()
           if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "MODE", "TREATMENT", "ENV_DIR",
                        "ALLOW_UNKNOWN_TYPE", "GENIERAILS_RERUN_TARGET", "PIP_FIND_LINKS",
                        "PIP_INDEX_URL", "PIP_EXTRA_INDEX_URL", "PIP_CONFIG_FILE")}
    env.update({"PIP_NO_INDEX": "1", "PIP_REQUIRE_VIRTUALENV": "1"})
    return env


def _tags(catalog, extra=()):
    rows = [[catalog, "finance", "customers", "email", "class.email_address", ""],
            [catalog, "finance", "customers", "cvv", "class.card_security_code", ""]]
    return rows + [[catalog, "finance", "customers", col, label, ""] for col, label in extra]


class Repo:
    """A throwaway aws/ + shared/ checkout with dev rehearsed and prod promoted."""

    def __init__(self, root: Path, shared_src: Path):
        self.root = root
        self.shared = root / "shared"
        shutil.copytree(shared_src, self.shared, ignore=shutil.ignore_patterns(
            "tests", "__pycache__", ".terraform", "*.tfstate*", ".pytest_cache"))
        (root / "aws").mkdir()
        shutil.copy(REPO / "aws" / "Makefile", root / "aws" / "Makefile")
        sdk = root / "sdk" / "databricks" / "sdk"
        (sdk / "service").mkdir(parents=True)
        (sdk.parent / "__init__.py").write_text("")
        (sdk / "__init__.py").write_text(FAKE_SDK)
        # generate_abac pip-upgrades a databricks.sdk without useragent.
        (sdk / "useragent.py").write_text("")
        (sdk / "service" / "__init__.py").write_text("")
        (sdk / "service" / "sql.py").write_text(FAKE_SQL)
        self.uc = root / "uc.json"
        self.set_uc()
        for env in ("dev", "prod"):
            assert self.make("setup", f"ENV={env}").returncode == 0
        self.envs = root / "aws" / "envs"
        for env, table in (("dev", DEV_TABLE), ("prod", PROD_TABLE)):
            (self.envs / env / "auth.auto.tfvars").write_text(
                f'databricks_workspace_host = "https://{env}.example.invalid"\n')
            text = f'uc_tables = ["{table}"]\nsql_warehouse_id = "wh"\n'
            if env == "prod":
                text += 'promote_from = "dev"\ncatalog_map = { dev_fin = "prod_fin" }\n'
            (self.envs / env / "env.auto.tfvars").write_text(text)
        (self.envs / "dev" / "generated" / "abac.auto.tfvars").write_text(DEV_ABAC)
        (self.envs / "dev" / "generated" / "masking_functions.sql").write_text(DEV_SQL)
        (self.envs / "dev" / "ddl" / "_fetched.sql").write_text(self.ddl(DEV_TABLE))
        result = self.make("promote-to", "ENV=prod")
        assert result.returncode == 0, result.stdout + result.stderr

    @staticmethod
    def ddl(table):
        cols = ",\n".join(f"  {name} {kind}" for name, kind in COLUMNS)
        return f"-- Table: {table}\nCREATE TABLE {table} (\n{cols}\n);\n"

    def set_uc(self, prod_extra=()):
        self.uc.write_text(json.dumps({
            "tables": {DEV_TABLE: COLUMNS, PROD_TABLE: COLUMNS},
            "tags": _tags("dev_fin") + _tags("prod_fin", prod_extra),
        }))

    def make(self, *args):
        env = _clean_env()
        env["PYTHONPATH"] = str(self.root / "sdk")
        env["FAKE_UC"] = str(self.uc)
        return subprocess.run(["make", "--no-print-directory", *args], cwd=self.root / "aws",
                              text=True, capture_output=True, env=env, timeout=300)

    def config(self):
        return json.loads((self.shared / "treatment_config.json").read_text())

    def generated(self, env):
        return hcl2.loads((self.envs / env / "generated" / "abac.auto.tfvars").read_text())

    def sql(self, env):
        return (self.envs / env / "generated" / "masking_functions.sql").read_text()

    def prod_pipeline(self):
        """The steps make release ENV=prod runs before it applies anything."""
        for target in ("derive-assignments", "validate-generated", "coverage-gate"):
            result = self.make(target, "ENV=prod")
            if result.returncode:
                return target, result
        result = self.make("promote", "ENV=prod")
        if result.returncode:
            return "promote", result
        result = self.make("audit-rulebook", "ENV=prod")
        return ("audit-rulebook" if result.returncode else None), result


@pytest.fixture
def repo(tmp_path):
    shared_src = Path(os.environ.get("GENIERAILS_TEST_SHARED_SRC", REPO / "shared"))
    return Repo(tmp_path, shared_src)


def _out(result):
    return result.stdout + result.stderr


def _treatment(config, value):
    return next((t for t in config["treatments"] if t["value"] == value), None)


def _masks(cfg, value):
    return [p for p in cfg.get("fgac_policies") or []
            if f"'gr_treatment', '{value}'" in p.get("match_condition", "")]


# ── end to end ───────────────────────────────────────────────────────────────

def test_e2e_prod_gap_reuse_existing_mask_then_prod_coverage_passes(repo):
    assert repo.prod_pipeline()[0] is None  # promoted rules cover prod today
    repo.set_uc(prod_extra=[("vat_number", "class.vat_number")])
    step, result = repo.prod_pipeline()
    assert step == "derive-assignments"
    out = _out(result)
    assert "unmapped class.* findings: prod_fin.finance.customers.vat_number=class.vat_number" in out
    assert "make scaffold-treatments ENV=prod TREATMENT=<name>" in out
    assert "make materialize-treatment ENV=dev TREATMENT=<new>" in out
    suggested = re.search(
        r"Existing treatments for prod_fin\.finance\.customers\.vat_number \(STRING\): ([^\n]*)", out)
    assert suggested, out
    names = suggested.group(1).split(", ")
    # Only treatments derivation would keep on this generic column are offered.
    assert "redact" in names and "generic_partial" in names
    assert "card_last4" not in names and "email_partial" not in names

    before = (repo.generated("prod").get("fgac_policies"), repo.sql("prod"), repo.config())
    result = repo.make("scaffold-treatments", "ENV=prod", "TREATMENT=redact")
    out = _out(result)
    assert result.returncode == 0, out
    assert "REUSED gr_treatment=redact" in out
    assert "shared/treatment_config.json: added class.vat_number to the class_labels of treatment redact" in out
    assert "Next: commit shared/treatment_config.json, then run: make release ENV=prod" in out

    config = repo.config()
    assert _treatment(config, "redact")["class_labels"] == ["class.vat_number"]
    assert len(config["treatments"]) == len(before[2]["treatments"])  # no new treatment
    assert repo.generated("prod").get("fgac_policies") == before[0]   # deployed names untouched
    assert repo.sql("prod") == before[1]                              # no new UDF

    step, result = repo.prod_pipeline()
    assert step is None, _out(result)
    derived = {a["entity_name"]: a["tag_value"] for a in repo.generated("prod")["tag_assignments"]}
    assert derived[f"{PROD_TABLE}.vat_number"] == "redact"


def test_e2e_prod_gap_new_mask_materialized_in_dev_then_promoted_and_covered(repo):
    repo.set_uc(prod_extra=[("vat_number", "class.vat_number")])
    assert repo.prod_pipeline()[0] == "derive-assignments"

    result = repo.make("scaffold-treatments", "ENV=prod")
    out = _out(result)
    assert result.returncode == 0, out
    assert "ADDED class.vat_number -> gr_treatment=vat_number_redacted" in out
    assert "make materialize-treatment ENV=dev TREATMENT=vat_number_redacted" in out
    assert "make promote-to ENV=prod" in out
    # Promotion left prod no assignments; the re-derive must still keep the
    # promoted masks, under their deployed names.
    names = {p["name"] for p in repo.generated("prod")["fgac_policies"]}
    assert {"gr_mask_dev_fin_redact", "gr_mask_dev_fin_email_partial"} <= names

    dev_assignments = repo.generated("dev")["tag_assignments"]
    result = repo.make("materialize-treatment", "ENV=dev", "TREATMENT=vat_number_redacted")
    out = _out(result)
    assert result.returncode == 0, out
    assert "added mask policy gr_mask_dev_fin_vat_number_redacted (catalog dev_fin)" in out
    assert "added function dev_fin.finance.mask_vat_number_redact" in out
    assert "make promote-to ENV=prod" in out
    dev = repo.generated("dev")
    assert dev["tag_assignments"] == dev_assignments  # no dev column needed the tag
    [mask] = _masks(dev, "vat_number_redacted")
    assert mask["function_name"] == "mask_vat_number_redact"
    assert mask["to_principals"] == ["analysts"]
    assert "CREATE OR REPLACE FUNCTION mask_vat_number_redact(input STRING)" in repo.sql("dev")
    for target in ("validate-generated", "coverage-gate"):
        result = repo.make(target, "ENV=dev")
        assert result.returncode == 0, _out(result)

    result = repo.make("promote-to", "ENV=prod")
    assert result.returncode == 0, _out(result)
    step, result = repo.prod_pipeline()
    assert step is None, _out(result)
    prod = repo.generated("prod")
    derived = {a["entity_name"]: a["tag_value"] for a in prod["tag_assignments"]}
    assert derived[f"{PROD_TABLE}.vat_number"] == "vat_number_redacted"
    [mask] = _masks(prod, "vat_number_redacted")
    assert mask["catalog"] == "prod_fin" and mask["function_catalog"] == "prod_fin"
    assert "USE CATALOG prod_fin;" in repo.sql("prod")


# ── scaffold-treatments TREATMENT=<existing> ─────────────────────────────────

def test_reuse_refuses_unknown_treatment_and_changes_nothing(repo):
    repo.set_uc(prod_extra=[("vat_number", "class.vat_number")])
    before = (repo.shared / "treatment_config.json").read_text()
    result = repo.make("scaffold-treatments", "ENV=prod", "TREATMENT=no_such_mask")
    assert result.returncode != 0
    assert "TREATMENT=no_such_mask is not a treatment in shared/treatment_config.json" in _out(result)
    assert "Existing treatments: redact," in _out(result)
    assert (repo.shared / "treatment_config.json").read_text() == before


def test_reuse_refuses_a_udf_type_that_does_not_fit_the_column(repo):
    repo.set_uc(prod_extra=[("vat_amount", "class.vat_amount")])
    before = (repo.shared / "treatment_config.json").read_text()
    result = repo.make("scaffold-treatments", "ENV=prod", "TREATMENT=redact")
    out = _out(result)
    assert result.returncode != 0
    assert "prod_fin.finance.customers.vat_amount (class.vat_amount) is DECIMAL(10,2) but " \
           "mask_redact takes STRING" in out
    assert "nothing was changed" in out
    assert (repo.shared / "treatment_config.json").read_text() == before
    # DECIMAL(18,2) masks don't fit DECIMAL(10,2) either: refused, not guessed.
    result = repo.make("scaffold-treatments", "ENV=prod", "TREATMENT=round_amount")
    assert result.returncode != 0 and "takes DECIMAL(18,2)" in _out(result)


def test_reuse_refuses_an_unknown_column_type_unless_overridden(repo):
    repo.set_uc(prod_extra=[("vat_number", "class.vat_number")])
    uc = json.loads(repo.uc.read_text())
    uc["tables"][PROD_TABLE] = [c for c in COLUMNS if c[0] != "vat_number"]
    repo.uc.write_text(json.dumps(uc))
    before = (repo.shared / "treatment_config.json").read_text()
    result = repo.make("scaffold-treatments", "ENV=prod", "TREATMENT=redact")
    assert result.returncode != 0
    assert "cannot tell the data type of prod_fin.finance.customers.vat_number" in _out(result)
    assert "ALLOW_UNKNOWN_TYPE=1" in _out(result)
    assert (repo.shared / "treatment_config.json").read_text() == before

    result = repo.make("scaffold-treatments", "ENV=prod", "TREATMENT=redact", "ALLOW_UNKNOWN_TYPE=1")
    assert result.returncode == 0, _out(result)
    assert "class.vat_number" in _treatment(repo.config(), "redact")["class_labels"]


def test_reuse_of_a_treatment_not_masked_in_prod_points_at_materialize(repo):
    repo.set_uc(prod_extra=[("vat_number", "class.vat_number")])
    result = repo.make("scaffold-treatments", "ENV=prod", "TREATMENT=generic_partial")
    out = _out(result)
    assert result.returncode == 0, out
    assert "The generic_partial mask is not in this env's rules for catalog(s) prod_fin yet." in out
    assert "make materialize-treatment ENV=dev TREATMENT=generic_partial, make rehearse ENV=dev, " \
           "make promote-to ENV=prod, make release ENV=prod" in out
    # Skipping them, release's live derive stops and prints the same steps.
    step, result = repo.prod_pipeline()
    assert step == "derive-assignments"
    assert "no matching column-mask policy" in _out(result)
    assert "make materialize-treatment ENV=dev TREATMENT=generic_partial" in _out(result)
    assert "make promote-to ENV=prod, make release ENV=prod" in _out(result)
    # Following those steps closes the gap.
    for args in (("materialize-treatment", "ENV=dev", "TREATMENT=generic_partial"),
                 ("promote-to", "ENV=prod")):
        result = repo.make(*args)
        assert result.returncode == 0, _out(result)
    step, result = repo.prod_pipeline()
    assert step is None, _out(result)


def test_reuse_in_dev_changes_only_the_shared_config(repo):
    abac = repo.envs / "dev" / "generated" / "abac.auto.tfvars"
    abac.write_text(abac.read_text().replace(
        "tag_assignments = [",
        f"tag_assignments = [\n# gr.classification_unmapped: {DEV_TABLE}.vat_number|class.vat_number", 1))
    result = repo.make("coverage-gate", "ENV=dev")
    out = _out(result)
    assert result.returncode != 0
    assert "detected tags with no mapping/rule" in out
    assert "make scaffold-treatments ENV=dev TREATMENT=<name>" in out
    assert "then commit shared/treatment_config.json and run make rehearse ENV=dev" in out

    generated = {p: p.read_bytes() for p in (repo.envs / "dev" / "generated").iterdir()}
    result = repo.make("scaffold-treatments", "ENV=dev", "TREATMENT=redact")
    out = _out(result)
    assert result.returncode == 0, out
    assert "class.vat_number" in _treatment(repo.config(), "redact")["class_labels"]
    # Only shared/treatment_config.json changed; the marker stays until regenerate.
    assert {p: p.read_bytes() for p in (repo.envs / "dev" / "generated").iterdir()} == generated
    assert "Next: commit shared/treatment_config.json, then run: make generate ENV=dev, " \
           "then make rehearse ENV=dev" in out


def test_reuse_refuses_a_treatment_derivation_would_upgrade(repo):
    """card_last4 on a generic STRING column is upgraded to redact by derivation."""
    repo.set_uc(prod_extra=[("vat_number", "class.vat_number")])
    before = (repo.shared / "treatment_config.json").read_text()
    result = repo.make("scaffold-treatments", "ENV=prod", "TREATMENT=card_last4")
    out = _out(result)
    assert result.returncode != 0
    assert "prod_fin.finance.customers.vat_number (class.vat_number) does not look like a " \
           "mask_credit_card_last4 column, so derivation would mask it with redact" in out
    assert "use TREATMENT=redact" in out and "nothing was changed" in out
    assert (repo.shared / "treatment_config.json").read_text() == before

    # The suggested treatment closes the gap end to end with no rulebook drift.
    result = repo.make("scaffold-treatments", "ENV=prod", "TREATMENT=redact")
    assert result.returncode == 0, _out(result)
    step, result = repo.prod_pipeline()
    assert step is None, _out(result)
    assert "No drift detected." in result.stdout


# ── materialize-treatment ────────────────────────────────────────────────────

def test_materialize_is_idempotent(repo):
    args = ("materialize-treatment", "ENV=dev", "TREATMENT=round_amount")
    assert repo.make(*args).returncode == 0
    snapshot = (repo.generated("dev"), repo.sql("dev"))
    result = repo.make(*args)
    assert result.returncode == 0, _out(result)
    assert "already present; nothing changed." in _out(result)
    assert (repo.generated("dev"), repo.sql("dev")) == snapshot
    assert len(_masks(repo.generated("dev"), "round_amount")) == 1
    assert repo.sql("dev").count("FUNCTION mask_amount_rounded") == 1


def test_materialize_keeps_reviewed_policy_and_function(repo):
    # A reviewed mask for the treatment: principals and UDF body differ from
    # what materialize would write, and must survive untouched.
    abac = repo.envs / "dev" / "generated" / "abac.auto.tfvars"
    abac.write_text(abac.read_text().replace('to_principals    = ["analysts"]\n    comment          = '
                                             '"GenieRails treatment redact',
                                             'to_principals    = ["analysts", "auditors"]\n'
                                             '    comment          = "GenieRails treatment redact', 1))
    sql = repo.envs / "dev" / "generated" / "masking_functions.sql"
    sql.write_text(sql.read_text().replace("'[REDACTED]'", "'[HIDDEN]'"))
    before = (abac.read_text(), sql.read_text())
    result = repo.make("materialize-treatment", "ENV=dev", "TREATMENT=redact")
    assert result.returncode == 0, _out(result)
    assert "kept reviewed policy gr_mask_dev_fin_redact (catalog dev_fin)" in _out(result)
    assert (abac.read_text(), sql.read_text()) == before


def test_materialized_mask_survives_regenerate_and_scaffold(repo):
    """Sticky reviewed rules (#65): a mask no column uses yet is still a reviewed rule."""
    assert repo.make("materialize-treatment", "ENV=dev", "TREATMENT=round_amount").returncode == 0
    dev = repo.envs / "dev" / "generated"
    reviewed = (repo.generated("dev"), repo.sql("dev"))

    # make generate re-drafts from the model; a draft without the mask keeps it.
    from scripts.merge_space_configs import keep_reviewed_rules
    (dev / "abac.auto.tfvars").write_text(DEV_ABAC)
    (dev / "masking_functions.sql").write_text(DEV_SQL)
    messages = keep_reviewed_rules(reviewed, dev / "abac.auto.tfvars", dev / "masking_functions.sql")
    assert any("kept reviewed rule policy gr_mask_dev_fin_round_amount" in m for m in messages)
    assert _masks(repo.generated("dev"), "round_amount")
    assert "FUNCTION mask_amount_rounded" in repo.sql("dev")

    # make scaffold-treatments (new kind) re-derives masks; it keeps this one.
    abac = dev / "abac.auto.tfvars"
    abac.write_text(abac.read_text().replace(
        "tag_assignments = [",
        f"tag_assignments = [\n# gr.classification_unmapped: {DEV_TABLE}.vat_number|class.vat_number", 1))
    result = repo.make("scaffold-treatments", "ENV=dev")
    assert result.returncode == 0, _out(result)
    assert _masks(repo.generated("dev"), "round_amount")
    assert _masks(repo.generated("dev"), "vat_number_redacted")
    for target in ("validate-generated", "coverage-gate"):
        result = repo.make(target, "ENV=dev")
        assert result.returncode == 0, _out(result)


def test_materialize_refuses_past_the_policy_limit(repo):
    abac = repo.envs / "dev" / "generated" / "abac.auto.tfvars"
    filler = "".join(
        f'''  {{
    name             = "filter_{i}"
    policy_type      = "POLICY_TYPE_ROW_FILTER"
    catalog          = "dev_fin"
    to_principals    = ["analysts"]
    when_condition   = "hasTagValue('gr_treatment', 'redact')"
    function_name    = "mask_redact"
    function_catalog = "dev_fin"
    function_schema  = "finance"
  }},
''' for i in range(98))
    abac.write_text(abac.read_text().replace("fgac_policies = [\n", "fgac_policies = [\n" + filler, 1))
    before = (abac.read_text(), repo.sql("dev"))
    result = repo.make("materialize-treatment", "ENV=dev", "TREATMENT=card_last4")
    assert result.returncode != 0
    assert "Unity Catalog allows 100 policies per catalog" in _out(result)
    assert "dev_fin (101 policies)" in _out(result)
    assert (abac.read_text(), repo.sql("dev")) == before


def test_materialize_refuses_prod_and_a_missing_treatment(repo):
    result = repo.make("materialize-treatment", "ENV=prod", "TREATMENT=redact")
    assert result.returncode != 0
    assert "ENV=prod is not allowed: prod's rules change only by promotion" in _out(result)
    result = repo.make("materialize-treatment", "ENV=dev")
    assert result.returncode != 0 and "set TREATMENT=<treatment>" in _out(result)
    result = repo.make("materialize-treatment", "ENV=dev", "TREATMENT=nope")
    assert result.returncode != 0
    assert "TREATMENT=nope is not a treatment" in _out(result)


@pytest.mark.parametrize("how", ["env_dir", "symlink", "promotion_target"])
def test_materialize_refuses_to_write_a_promoted_env(repo, how):
    """ENV=dev ENV_DIR=envs/prod, a symlinked env, or any env with promote_from."""
    prod = repo.envs / "prod"
    if how == "env_dir":
        args, expect = ("ENV=dev", f"ENV_DIR={prod}"), f"ENV_DIR={prod} is not envs/dev"
    elif how == "symlink":
        (repo.envs / "stage").symlink_to(prod, target_is_directory=True)
        args, expect = ("ENV=stage",), "envs/stage is a symlink"
    else:
        shutil.copytree(repo.envs / "dev", repo.envs / "stg", symlinks=True)
        with (repo.envs / "stg" / "env.auto.tfvars").open("a") as handle:
            handle.write('promote_from = "dev"\n')
        prod = repo.envs / "stg"
        args, expect = ("ENV=stg",), "ENV=stg is not allowed: stg's rules change only by promotion"
    before = {p: p.read_bytes() for p in prod.rglob("*") if p.is_file() and not p.is_symlink()}
    result = repo.make("materialize-treatment", *args, "TREATMENT=round_amount")
    assert result.returncode != 0
    assert expect in _out(result)
    assert {p: p.read_bytes() for p in prod.rglob("*")
            if p.is_file() and not p.is_symlink()} == before


def test_materialize_needs_a_udf_definition(repo):
    # ssn_last4 has no udf_signature/udf_body in treatment_config.json and dev's
    # SQL doesn't define mask_ssn: refuse rather than emit a dangling policy.
    before = repo.generated("dev")
    result = repo.make("materialize-treatment", "ENV=dev", "TREATMENT=ssn_last4")
    assert result.returncode != 0
    assert "does not define mask_ssn" in _out(result)
    assert repo.generated("dev") == before


# ── release / maintain messages ──────────────────────────────────────────────

def test_rulebook_drift_is_reported_as_drift_with_both_fixes(repo):
    repo.set_uc(prod_extra=[("vat_number", "class.vat_number")])
    assert repo.make("audit-rulebook", "ENV=prod").returncode != 0
    # Through release's own wrapper: drift (1) is not mistaken for an error (2).
    makefile = (repo.shared / "Makefile.shared").read_text()
    assert "_RUN_AUDIT_RULEBOOK" in makefile
    rc_file = repo.root / "rc"
    env = _clean_env() | {"PYTHONPATH": str(repo.root / "sdk"), "FAKE_UC": str(repo.uc),
                          "GENIERAILS_AUDIT_RC_FILE": str(rc_file),
                          "GENIERAILS_RERUN_TARGET": "maintain"}
    result = subprocess.run(["make", "--no-print-directory", "audit-rulebook", "ENV=prod"],
                            cwd=repo.root / "aws", text=True, capture_output=True, env=env)
    assert result.returncode == 2 and rc_file.read_text().strip() == "1"
    out = _out(result)
    assert "RULEBOOK DRIFT" in out and "class.vat_number" in out
    assert "make scaffold-treatments ENV=prod TREATMENT=<name>" in out
    assert "then commit shared/treatment_config.json and run make maintain ENV=prod" in out
    assert "make materialize-treatment ENV=dev TREATMENT=<new>" in out


def test_audit_error_is_reported_as_an_error_not_drift(repo):
    uc = json.loads(repo.uc.read_text())
    repo.uc.write_text(json.dumps({**uc, "fail_sql": True}))
    rc_file = repo.root / "rc"
    env = _clean_env() | {"PYTHONPATH": str(repo.root / "sdk"), "FAKE_UC": str(repo.uc),
                          "GENIERAILS_AUDIT_RC_FILE": str(rc_file)}
    result = subprocess.run(["make", "--no-print-directory", "audit-rulebook", "ENV=prod"],
                            cwd=repo.root / "aws", text=True, capture_output=True, env=env)
    assert result.returncode == 2 and rc_file.read_text().strip() == "2"
    assert "PERMISSION_DENIED" in _out(result)
    assert "How to fix" not in _out(result)
