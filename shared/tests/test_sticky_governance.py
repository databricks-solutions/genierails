"""Deterministic envs keep governing a table after every agent drops it."""

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import hcl2
import pytest

import generate_abac
from sensitivity_source import ClassificationSource
from scripts import sticky_governance as sticky
from scripts.sticky_governance import GovernedTablesError, check_layout, load_governed_tables, main, ungovern
from tests.terraform_helpers import shared_copy, skip_if_providers_unavailable, tf, tf_env, tf_init

SHARED = Path(__file__).parents[1]
SCRIPT = SHARED / "scripts/derive_assignments.py"
SPEC = importlib.util.spec_from_file_location("derive_assignments_sticky", SCRIPT)
DERIVE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DERIVE)

CUSTOMERS, ORDERS = "prod.sales.customers", "prod.sales.orders"
PROMOTED = '''tag_policies = [{ key = "gr_treatment", description = "reviewed", values = ["redact", "email_partial", "ssn_last4"] }]
tag_assignments = [
]
fgac_policies = [
  { name = "m_redact", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "prod", to_principals = ["g"], match_condition = "hasTagValue('gr_treatment', 'redact')", function_name = "mask_redact", function_schema = "security" },
  { name = "m_email", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "prod", to_principals = ["g"], match_condition = "hasTagValue('gr_treatment', 'email_partial')", function_name = "mask_email", function_schema = "security" },
  { name = "m_ssn", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "prod", to_principals = ["g"], match_condition = "hasTagValue('gr_treatment', 'ssn_last4')", function_name = "mask_ssn", function_schema = "security" },
]
'''
CLASSES = {"email": "class.email_address", "ssn": "class.us_ssn"}


def _env(env, tables=("sales.customers", "sales.orders"), mode="deterministic"):
    (env / "generated").mkdir(parents=True, exist_ok=True)
    (env / "data_access").mkdir(exist_ok=True)
    (env / "auth.auto.tfvars").write_text('databricks_workspace_host = "https://unused.invalid"\n')
    listed = ", ".join(f'"{t}"' for t in tables)
    (env / "env.auto.tfvars").write_text(
        f'governance_mode = "{mode}"\nuc_catalog = "prod"\n'
        f'genie_spaces = [{{ name = "A", uc_tables = [{listed}] }}]\n'
    )
    config = env / "generated/abac.auto.tfvars"
    if not config.exists():
        config.write_text(PROMOTED)
    return env


def _live(monkeypatch, columns):
    """Fake live class.* tags; returns the table refs each read scanned."""
    scanned = []

    def fetch(table_refs, runtime, require_native=False):
        scanned.append(sorted(table_refs))
        refs = {ref.lower() for ref in table_refs}
        rows = [(*column.split("."), CLASSES[kind], "") for column, kind in columns.items()
                if ".".join(column.split(".")[:3]).lower() in refs]
        return ClassificationSource(tag_rows=rows)

    monkeypatch.setattr(DERIVE, "_fetch_live_classification_source", fetch)
    return scanned


def _derive(env):
    DERIVE.derive_assignments(env / "generated/abac.auto.tfvars", env / "auth.auto.tfvars", env / "env.auto.tfvars")
    items = hcl2.loads((env / "generated/abac.auto.tfvars").read_text())["tag_assignments"]
    return {i["entity_name"]: i["tag_value"] for i in items if i["tag_key"] == "gr_treatment"}


def _state(env, tags, name="treatment"):
    def key(column, value):
        return column if name == "treatment" else f"columns|{column}|gr_treatment|{value}"
    (env / "data_access/terraform.tfstate").write_text(json.dumps({"resources": [{
        "module": "module.data_access", "mode": "managed",
        "type": "databricks_entity_tag_assignment", "name": name,
        "instances": [{"index_key": key(c, v), "attributes": {
            "entity_type": "columns", "entity_name": c, "tag_key": "gr_treatment", "tag_value": v,
        }} for c, v in tags.items()],
    }]}))


def _manifest(env):
    return json.loads((env / "generated/governed_tables.json").read_text())


# -- Dropped tables stay governed, with treatments from current tags ----------

def test_dropping_a_table_from_every_agent_keeps_its_tags(tmp_path, monkeypatch):
    env = _env(tmp_path)
    scanned = _live(monkeypatch, {f"{CUSTOMERS}.email": "email", f"{ORDERS}.ssn": "ssn"})
    assert _derive(env) == {f"{CUSTOMERS}.email": "email_partial", f"{ORDERS}.ssn": "ssn_last4"}
    assert _manifest(env) == {"tables": [CUSTOMERS, ORDERS]}

    _env(env, tables=("sales.customers",))
    assert _derive(env) == {f"{CUSTOMERS}.email": "email_partial", f"{ORDERS}.ssn": "ssn_last4"}
    assert ORDERS in scanned[-1]


def test_treatment_change_replaces_the_value_and_keeps_one_tag_per_column(tmp_path, monkeypatch):
    env = _env(tmp_path)
    _live(monkeypatch, {f"{ORDERS}.ssn": "ssn"})
    assert _derive(env) == {f"{ORDERS}.ssn": "ssn_last4"}
    _env(env, tables=("sales.customers",))
    _live(monkeypatch, {f"{ORDERS}.ssn": "email"})
    assert _derive(env) == {f"{ORDERS}.ssn": "email_partial"}


def test_renamed_column_of_a_dropped_table_loses_its_old_tag(tmp_path, monkeypatch):
    env = _env(tmp_path)
    _live(monkeypatch, {f"{ORDERS}.ssn": "ssn"})
    _derive(env)
    _env(env, tables=("sales.customers",))
    _live(monkeypatch, {f"{ORDERS}.tax_id": "ssn"})
    assert set(_derive(env)) == {f"{ORDERS}.tax_id"}


def test_manifest_stores_only_table_names(tmp_path, monkeypatch):
    env = _env(tmp_path)
    _live(monkeypatch, {f"{ORDERS}.ssn": "ssn"})
    _derive(env)
    assert "ssn" not in (env / "generated/governed_tables.json").read_text()


# -- A missing or edited manifest cannot drop or weaken governance ------------

REFUSED = "governed_tables.json is missing but this env has recorded governance"


def test_manifest_deleted_before_any_apply_is_refused(tmp_path, monkeypatch):
    # Governance recorded but not yet applied (no state): starting empty would
    # drop orders once its agent is gone.
    env = _env(tmp_path)
    _live(monkeypatch, {f"{ORDERS}.ssn": "ssn"})
    _derive(env)
    assert _manifest(env) == {"tables": [ORDERS]}
    (env / "generated/governed_tables.json").unlink()
    _env(env, tables=("sales.customers",))
    with pytest.raises(GovernedTablesError, match=REFUSED) as refused:
        _derive(env)
    assert f"make rebuild-governed-tables ENV={env.resolve().name}" in str(refused.value)
    assert main(["rebuild", "--env-dir", str(env)]) == 0
    assert _manifest(env) == {"tables": [ORDERS]}
    assert _derive(env) == {f"{ORDERS}.ssn": "ssn_last4"}


def test_manifest_deleted_after_apply_is_refused_then_rebuilt_from_state(tmp_path, monkeypatch):
    env = _env(tmp_path, tables=("sales.customers",))
    _state(env, {f"{ORDERS}.ssn": "ssn_last4"})
    with pytest.raises(GovernedTablesError, match=r"treatment tags are deployed"):
        load_governed_tables(env)
    sticky.rebuild(env)
    assert _manifest(env) == {"tables": [ORDERS]}


def test_manifest_deleted_from_git_is_refused_then_rebuilt_from_history(tmp_path):
    env = _env(tmp_path / "dev", tables=())
    (env / "generated/abac.auto.tfvars").write_text(PROMOTED)
    sticky.save_governed_tables(env, [ORDERS])
    git = ["git", "-C", str(env), "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"]
    subprocess.run(["git", "init", "-q", str(env)], check=True)
    subprocess.run([*git, "add", "generated/governed_tables.json"], check=True)
    subprocess.run([*git, "commit", "-qm", "record"], check=True)
    subprocess.run([*git, "rm", "-q", "generated/governed_tables.json"], check=True)
    subprocess.run([*git, "commit", "-qm", "lose it"], check=True)
    with pytest.raises(GovernedTablesError, match="in git history"):
        load_governed_tables(env)
    sticky.rebuild(env)
    assert _manifest(env) == {"tables": [ORDERS]}


def test_only_a_fresh_env_starts_empty_and_its_first_record_succeeds(tmp_path):
    env = _env(tmp_path)
    assert load_governed_tables(env) == []
    # make generate records right after writing its first config.
    (env / "generated/abac.auto.tfvars").write_text(PROMOTED.replace("tag_assignments = [\n", (
        'tag_assignments = [\n  { entity_type = "columns", entity_name = "prod.sales.orders.ssn", '
        'tag_key = "gr_treatment", tag_value = "ssn_last4" },\n')))
    assert main(["record", "--env-dir", str(env), "--config", str(env / "generated/abac.auto.tfvars")]) == 0
    assert _manifest(env) == {"tables": [ORDERS]}


def test_hand_removed_table_stays_governed_while_deployed(tmp_path, monkeypatch):
    env = _env(tmp_path, tables=("sales.customers",))
    _state(env, {f"{ORDERS}.ssn": "ssn_last4"})
    (env / "generated/governed_tables.json").write_text('{"tables": []}\n')
    _live(monkeypatch, {f"{ORDERS}.ssn": "ssn"})
    assert _derive(env) == {f"{ORDERS}.ssn": "ssn_last4"}


def test_hand_edited_treatment_is_replaced_by_current_tags(tmp_path, monkeypatch):
    env = _env(tmp_path, tables=("sales.customers",))
    (env / "generated/governed_tables.json").write_text(json.dumps({"tables": [ORDERS]}))
    _live(monkeypatch, {f"{ORDERS}.ssn": "ssn"})
    (env / "generated/abac.auto.tfvars").write_text(PROMOTED.replace("tag_assignments = [\n", (
        'tag_assignments = [\n  { entity_type = "columns", entity_name = "prod.sales.orders.ssn", '
        'tag_key = "gr_treatment", tag_value = "email_partial" },\n')))
    assert _derive(env) == {f"{ORDERS}.ssn": "ssn_last4"}


@pytest.mark.parametrize("content", [
    "not json",
    '{"tables": {"prod.sales.orders": {"prod.sales.orders.ssn": "raw"}}}',
    '{"tables": ["prod.sales"]}',
    '{"tables": ["prod.sales.*"]}',
    '{"tables": [7]}',
    '[]',
])
def test_invalid_manifest_fails_closed(tmp_path, content):
    env = _env(tmp_path)
    (env / "generated/governed_tables.json").write_text(content)
    with pytest.raises(GovernedTablesError, match="governed_tables.json"):
        load_governed_tables(env)


def test_unreadable_state_fails_closed(tmp_path):
    env = _env(tmp_path)
    (env / "data_access/terraform.tfstate").write_text("{")
    with pytest.raises(GovernedTablesError, match="terraform.tfstate"):
        load_governed_tables(env)


def _generate(env, monkeypatch):
    scanned = []

    def stop(refs, cfg):
        scanned.extend(refs)
        raise SystemExit(0)

    monkeypatch.chdir(env)
    monkeypatch.setattr(generate_abac, "WORK_DIR", env)
    monkeypatch.setattr(generate_abac, "fetch_tables_from_databricks", stop)
    monkeypatch.setattr(generate_abac, "list_account_group_names", lambda cfg: None)
    monkeypatch.setattr(generate_abac, "configure_databricks_env", lambda cfg: None)
    monkeypatch.setattr(sys, "argv", ["generate_abac.py", "--auth-file", str(env / "auth.auto.tfvars"),
                                      "--groups", "g", "--out-dir", str(env / "generated")])
    with pytest.raises(SystemExit):
        generate_abac.main()
    return {ref.lower() for ref in scanned}


def test_generate_scans_governed_tables_no_agent_uses(tmp_path, monkeypatch):
    env = _env(tmp_path, tables=("sales.customers",))
    (env / "generated/governed_tables.json").write_text('{"tables": []}\n')
    _state(env, {f"{ORDERS}.ssn": "ssn_last4"})
    (env / "generated/abac.auto.tfvars").unlink()
    assert ORDERS in _generate(env, monkeypatch)


def test_generate_refuses_when_the_record_was_lost(tmp_path, monkeypatch, capsys):
    env = _env(tmp_path, tables=("sales.customers",))
    (env / "generated/abac.auto.tfvars").write_text(PROMOTED.replace("tag_assignments = [\n", (
        'tag_assignments = [\n  { entity_type = "columns", entity_name = "prod.sales.orders.ssn", '
        'tag_key = "gr_treatment", tag_value = "ssn_last4" },\n')))
    assert _generate(env, monkeypatch) == set()
    assert REFUSED in capsys.readouterr().out


# -- Legacy envs are untouched -----------------------------------------------

def test_legacy_env_has_no_governed_set_and_no_layout_check(tmp_path, monkeypatch):
    env = _env(tmp_path, tables=("sales.customers",), mode="legacy")
    _state(env, {f"{ORDERS}.ssn": "ssn_last4"}, name="assignments")
    assert load_governed_tables(env) == []
    assert sticky.record(env, [{"entity_type": "columns", "entity_name": f"{ORDERS}.ssn",
                                "tag_key": "gr_treatment", "tag_value": "redact"}]) == []
    check_layout(env, "dev")
    scanned = _live(monkeypatch, {f"{ORDERS}.ssn": "ssn", f"{CUSTOMERS}.email": "email"})
    assert _derive(env) == {f"{CUSTOMERS}.email": "email_partial"}
    assert scanned == [[CUSTOMERS]]
    assert not (env / "generated/governed_tables.json").exists()


# -- Old-address envs are refused with exact state mv commands ----------------

def test_old_address_tags_are_refused_with_state_mv_commands(tmp_path, capsys):
    env = _env(tmp_path)
    _state(env, {f"{ORDERS}.ssn": "ssn_last4"}, name="assignments")
    assert main(["check-layout", "--env-dir", str(env), "--env-name", "dev"]) == 1
    err = capsys.readouterr().err
    assert "nothing was planned or applied" in err
    assert (
        "data_access dev state-mv "
        "'module.data_access.databricks_entity_tag_assignment.assignments[\"columns|prod.sales.orders.ssn|gr_treatment|ssn_last4\"]' "
        "'module.data_access.databricks_entity_tag_assignment.treatment[\"columns|prod.sales.orders.ssn|gr_treatment\"]'"
    ) in err
    assert f"ENVS_DIR={env.resolve().parent} " in err


def test_new_address_tags_pass_the_layout_check(tmp_path):
    env = _env(tmp_path)
    _state(env, {f"{ORDERS}.ssn": "ssn_last4"})
    check_layout(env, "dev")


@pytest.mark.parametrize("recipe", ["_plan-layer:", "_apply-layer:"])
def test_data_access_plan_and_apply_run_the_layout_check(recipe):
    text = (SHARED / "Makefile.shared").read_text()
    body = text[text.index(f"\n{recipe}"):]
    body = body[:body.index("\n\n")]
    assert body.index("check-layout") < body.index("_prepare-classification")


# -- ungovern ----------------------------------------------------------------

def _governed(env):
    sticky.save_governed_tables(env, [CUSTOMERS, ORDERS])
    _state(env, {f"{ORDERS}.ssn": "ssn_last4"})
    (env / "generated/abac.auto.tfvars").write_text(PROMOTED.replace("tag_assignments = [\n", (
        'tag_assignments = [\n'
        '  { entity_type = "columns", entity_name = "prod.sales.orders.ssn", tag_key = "gr_treatment", tag_value = "ssn_last4" },\n'
        '  { entity_type = "columns", entity_name = "prod.sales.customers.email", tag_key = "gr_treatment", tag_value = "email_partial" },\n'
    )))
    return env


@pytest.mark.parametrize("uses", [
    '"sales.orders"', '"SALES.Orders"', '"prod.sales.orders"', '"sales.*"', '"prod.sales.orders.ssn"',
])
def test_ungovern_refuses_a_table_an_agent_still_uses(tmp_path, uses, capsys):
    env = _governed(_env(tmp_path, tables=()))
    (env / "env.auto.tfvars").write_text(
        f'governance_mode = "deterministic"\nuc_catalog = "prod"\ngenie_spaces = [{{ name = "A", uc_tables = [{uses}] }}]\n')
    assert main(["ungovern", "--env-dir", str(env), "--table", "prod.sales.ORDERS"]) == 1
    assert "a configured agent still uses it" in capsys.readouterr().err


def test_ungovern_refuses_a_table_in_the_discovered_footprint(tmp_path):
    env = _governed(_env(tmp_path, tables=()))
    (env / "data_access/discovered_uc_tables.auto.tfvars").write_text('discovered_uc_tables = ["prod.sales.orders"]\n')
    with pytest.raises(GovernedTablesError, match="still uses it"):
        ungovern(env, ORDERS)


def test_ungovern_refuses_a_table_in_a_declared_footprint(tmp_path):
    env = _governed(_env(tmp_path, tables=()))
    with open(env / "env.auto.tfvars", "a") as handle:
        handle.write('declared_footprint = [{ table = "prod.sales.orders", columns = ["ssn"] }]\n')
    with pytest.raises(GovernedTablesError, match="still uses it"):
        ungovern(env, ORDERS)


@pytest.mark.parametrize("table, message", [
    ("prod.sales", "catalog.schema.table"),
    ("prod.sales.*", "catalog.schema.table"),
    ("prod.sales.unknown", "is not governed"),
])
def test_ungovern_refuses_bad_or_ungoverned_tables(tmp_path, table, message):
    env = _governed(_env(tmp_path, tables=("sales.customers",)))
    with pytest.raises(GovernedTablesError, match=message):
        ungovern(env, table)


def test_ungovern_refuses_legacy_envs(tmp_path):
    env = _env(tmp_path, tables=(), mode="legacy")
    with pytest.raises(GovernedTablesError, match="deterministic"):
        ungovern(env, ORDERS)


def test_ungovern_preview_changes_nothing(tmp_path, capsys):
    env = _governed(_env(tmp_path, tables=("sales.customers",)))
    before = (env / "generated/abac.auto.tfvars").read_text()
    ungovern(env, "PROD.sales.orders")
    assert "prod.sales.orders.ssn" in capsys.readouterr().out
    assert (env / "generated/abac.auto.tfvars").read_text() == before
    assert _manifest(env) == {"tables": [CUSTOMERS, ORDERS]}


def test_ungovern_commit_removes_only_that_tables_tags(tmp_path, monkeypatch):
    env = _governed(_env(tmp_path, tables=("sales.customers",)))
    ungovern(env, "PROD.sales.orders", commit=True)
    items = hcl2.loads((env / "generated/abac.auto.tfvars").read_text())["tag_assignments"]
    assert [i["entity_name"] for i in items] == [f"{CUSTOMERS}.email"]
    assert _manifest(env) == {"tables": [CUSTOMERS]}
    # The tags stay deployed until the apply, so the table is only left out
    # while make ungovern's apply runs (it sets this variable).
    assert load_governed_tables(env) == [CUSTOMERS, ORDERS]
    monkeypatch.setenv(sticky.UNGOVERN_ENV, "prod.sales.ORDERS")
    assert load_governed_tables(env) == [CUSTOMERS]
    _live(monkeypatch, {f"{ORDERS}.ssn": "ssn", f"{CUSTOMERS}.email": "email"})
    assert _derive(env) == {f"{CUSTOMERS}.email": "email_partial"}
    assert _manifest(env) == {"tables": [CUSTOMERS]}


# -- make ungovern -------------------------------------------------------------

def _make(*args, stdin="", ci=None):
    env = {k: v for k, v in os.environ.items()
           if k not in ("MAKEFLAGS", "MAKELEVEL", "GNUMAKEFLAGS", "CI", "GENIERAILS_ALLOW_CI_APPLY")}
    if ci:
        env["CI"] = ci
    return subprocess.run(["make", "--no-print-directory", "ungovern", *args], cwd=SHARED.parent / "aws",
                          input=stdin, text=True, capture_output=True, env=env)


def test_make_ungovern_validates_before_creating_env_folders(tmp_path):
    missing = tmp_path / "envs/nope"
    result = _make("ENV=nope", f"ENV_DIR={missing}", "TABLE=prod.sales.orders")
    assert result.returncode != 0
    assert "deterministic" in result.stderr
    assert not (tmp_path / "envs").exists()


def test_make_ungovern_requires_typed_confirmation(tmp_path):
    env = _governed(_env(tmp_path / "dev", tables=("sales.customers",)))
    before = (env / "generated/abac.auto.tfvars").read_text()
    result = _make("ENV=dev", f"ENV_DIR={env}", "TABLE=prod.sales.orders", stdin="yes\n")
    assert result.returncode != 0
    assert "not confirmed; nothing changed" in result.stderr
    assert "prod.sales.orders.ssn" in result.stdout
    assert (env / "generated/abac.auto.tfvars").read_text() == before
    assert _manifest(env) == {"tables": [CUSTOMERS, ORDERS]}



def test_make_rebuild_governed_tables_restores_a_lost_record(tmp_path):
    env = _env(tmp_path / "dev", tables=())
    _state(env, {f"{ORDERS}.ssn": "ssn_last4"})
    env_vars = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "GNUMAKEFLAGS")}
    result = subprocess.run(["make", "--no-print-directory", "rebuild-governed-tables", "ENV=dev", f"ENV_DIR={env}"],
                            cwd=SHARED.parent / "aws", text=True, capture_output=True, env=env_vars)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _manifest(env) == {"tables": [ORDERS]}

def test_make_ungovern_refuses_in_ci(tmp_path):
    result = _make("ENV=dev", f"ENV_DIR={tmp_path}", "TABLE=a.b.c", "YES=1", ci="true")
    assert result.returncode != 0
    assert "cannot run in CI" in result.stderr


def test_make_ungovern_checks_then_confirms_then_applies_with_the_table_excluded():
    text = (SHARED / "Makefile.shared").read_text()
    recipe = text[text.index("\nungovern:"):text.index("\nscaffold-treatments:")]
    assert recipe.count("$(_STICKY) ungovern ") == 2 and "--commit" in recipe
    assert recipe.index("$(_STICKY) ungovern ") < recipe.index("Type YES") < recipe.index("_ENV_LOCK")
    assert 'GENIERAILS_UNGOVERN_TABLE="$(TABLE)" $(MAKE) --no-print-directory apply-governance' in recipe
    assert "_guarded-bootstrap" not in recipe.splitlines()[1]


# -- Terraform: apply order and legacy upgrade ---------------------------------

def _reaches(edges, start):
    seen, todo = set(), [start]
    while todo:
        for nxt in edges.get(todo.pop(), ()):
            if nxt not in seen:
                seen.add(nxt)
                todo.append(nxt)
    return seen


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_treatment_retag_waits_for_functions_and_policies_and_grants_wait_for_it(tmp_path):
    root = shared_copy(tmp_path) / "roots" / "data_access"
    env = tf_env(tmp_path)
    skip_if_providers_unavailable(tf(root, "init", "-input=false", env=env))
    graph = tf(root, "graph", env=env)
    assert graph.returncode == 0, graph.stdout + graph.stderr
    edges = {}  # "a" -> "b": a depends on b
    for a, b in re.findall(r'"module\.data_access\.([^"]+)" -> "module\.data_access\.([^"]+)"', graph.stdout):
        edges.setdefault(a, set()).add(b)
    treatment = "databricks_entity_tag_assignment.treatment"
    # The account layer adds the allowed value first; here: functions and
    # policies, then the retag, then any new SELECT grant.
    assert {"terraform_data.masking_functions", "databricks_policy_info.policies"} <= _reaches(edges, treatment)
    for earlier in ("databricks_policy_info.policies", "terraform_data.masking_functions",
                    "time_sleep.wait_for_tag_propagation"):
        assert treatment not in _reaches(edges, earlier), earlier
    assert treatment in _reaches(edges, "databricks_grant.table_access")
    assert treatment in _reaches(edges, "time_sleep.wait_for_policy_enforcement")


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_legacy_env_applied_with_the_old_module_replans_with_no_changes(tmp_path):
    root = shared_copy(tmp_path) / "modules" / "data_access"
    env = tf_env(tmp_path)
    tf_init(root, env=env)
    result = tf(root, "test", "-no-color", "-verbose", "-filter=tests/stable_treatment_tags.tftest.hcl", env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    replan = result.stdout[result.stdout.index('run "legacy_replan_with_new_module"'):]
    assert replan.split("\n", 1)[0].endswith("pass")
    assert "No changes. Your infrastructure matches the configuration." in replan
    assert not re.search(r"# \S+ (will be|must be)", replan), replan
