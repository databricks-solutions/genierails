"""Genie agent ID files and adoption markers under GENIERAILS_STATE_DIR.

The ID file is the only record of a created agent's ID (Terraform state keeps
only its path). With the state dir set it lives in <dir>/<cloud>/<env>/records/,
so a fresh checkout still knows which agents GenieRails created. make
migrate-state-dir moves the files and records the in-checkout path the state
still names, so the create resource and its handoff are recognised (never
recreated) until the next apply records the new path.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import agent_files
from scripts import agent_ownership as ownership
from scripts import genie_adopt_preflight as pf
from scripts import state_layout

SHARED = Path(__file__).resolve().parents[1]
KEY = "sales"
HOST = "https://example.invalid"
CREATE_ID = "tfdata-sales"
SETTINGS = 'genie_space_id = ""\nacl_groups = ["a"]\n'


def _payload() -> dict:
    return {"version": 2, "config": {"sample_questions": []}, "data_sources": {"tables": []}}


def _checkout_env(checkout: Path, *, settings: str = SETTINGS) -> Path:
    """A promoted deterministic prod env as a checkout commits it."""
    env = checkout / "aws" / "envs" / "prod"
    (env / "agents").mkdir(parents=True)
    (env / "env.auto.tfvars").write_text('governance_mode = "deterministic"\npromote_from = "dev"\n')
    (env / "agents" / "Sales.auto.tfvars").write_text(settings)
    ownership.write_payload(_payload(), agent_files.capture_path(env, "Sales"))
    return env


def _created_state(id_file: str, space_id: str = "prod-1") -> dict:
    """The workspace state a create left: terraform_data.genie_space[sales] and the ID it recorded."""
    return {"version": 4, "serial": 3, "lineage": "l", "resources": [
        {"module": "module.workspace", "mode": "managed", "type": "terraform_data", "name": "genie_space",
         "instances": [{"index_key": KEY, "attributes": {"id": CREATE_ID, "input": {"id_file": id_file}}}]},
        {"module": "module.workspace", "mode": "data", "type": "external", "name": "genie_space_id",
         "instances": [{"index_key": KEY, "attributes": {
             "query": {"id_file": id_file}, "result": {"id": space_id, "id_file": id_file}}}]},
    ]}


def _fresh_checkout(source_env: Path, checkout: Path) -> Path:
    """A clean checkout of the same commit: committed files only (no ignored dot files)."""
    env = checkout / "aws" / "envs" / "prod"
    shutil.copytree(source_env / "agents", env / "agents")
    shutil.copy2(source_env / "env.auto.tfvars", env / "env.auto.tfvars")
    return env


@pytest.fixture
def migrated(tmp_path, monkeypatch):
    """Checkout A created the agent in the in-checkout layout, then make migrate-state-dir ran."""
    monkeypatch.delenv(state_layout.STATE_DIR_ENV, raising=False)
    a = _checkout_env(tmp_path / "a")
    old_path = str(a / f".genie_space_id_{KEY}")
    (a / "terraform.tfstate").write_text(json.dumps(_created_state(old_path)))
    (a / f".genie_space_id_{KEY}").write_text("prod-1\n")
    (a / ".genie_adopted_other").write_text("")  # another agent's markers move too
    (a / ".genie_adopt_required_other").write_text("adoption required\n")
    assert ownership.binding_now(a, agent_files.load_all(a)[0]) == ("existing", CREATE_ID)
    state = tmp_path / "state"
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(state))
    state_layout.migrate(a, "prod")
    return a, old_path, state.resolve() / "aws" / "prod" / "records"


# -- a fresh checkout sharing the state dir -----------------------------------------

def test_a_fresh_checkout_sees_a_created_agent_as_existing(migrated, tmp_path):
    a, _old, records = migrated
    b = _fresh_checkout(a, tmp_path / "b")
    agent = agent_files.load_all(b)[0]
    assert ownership.id_file(b, "Sales") == records / f".genie_space_id_{KEY}"
    assert ownership.agent_id(b, "Sales") == "prod-1"
    # release-plan's agents.json classification: the agent exists; nothing is created.
    assert ownership.binding_now(b, agent) == ("existing", CREATE_ID)
    planned = ownership._planned(agent, b, "prod")
    assert planned["binding"] == "existing" and planned["genie_space_id"] == "prod-1"
    assert planned["create_resource_id"] == CREATE_ID
    # The create resource still names the old in-checkout path: recognised through the moves record.
    assert ownership.verify_created(b, "Sales", KEY, None) == "prod-1"
    assert pf.check_id_files(b, warn=False) == 0


def test_an_unrelated_recorded_id_file_is_still_refused_after_migration(migrated, tmp_path):
    a, _old, records = migrated
    b = _fresh_checkout(a, tmp_path / "b")
    layer = records.parent / "layers" / "workspace" / "terraform.tfstate"
    layer.write_text(json.dumps(_created_state(str(tmp_path / "attacker" / f".genie_space_id_{KEY}"))))
    with pytest.raises(ownership.AgentError, match="names the ID file"):
        ownership.verify_created(b, "Sales", KEY, None)


# -- legacy checkout copies refuse ---------------------------------------------------

@pytest.mark.parametrize("name", [f".genie_space_id_{KEY}", f".genie_adopted_{KEY}", f".genie_adopt_required_{KEY}"])
def test_a_genie_record_left_in_the_checkout_is_refused(tmp_path, monkeypatch, name):
    env = _checkout_env(tmp_path)
    (env / name).write_text("prod-1\n")
    (tmp_path / "state").mkdir()
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(tmp_path / "state"))
    # (RecordError is a ValueError; genie_adopt_preflight imports state_layout under its own name.)
    for read in (lambda: ownership.agent_id(env, "Sales"), lambda: ownership.adopted_marker(env, "Sales"),
                 lambda: pf.id_file_for(env, KEY), lambda: pf.marker_for(env, KEY)):
        with pytest.raises(ValueError, match="make migrate-state-dir ENV=prod"):
            read()
    (env / "terraform.tfstate").unlink(missing_ok=True)
    layer = tmp_path / "state" / "aws" / "prod" / "layers" / "workspace"
    layer.mkdir(parents=True)
    (layer / "terraform.tfstate").write_text(json.dumps(_created_state(str(env / f".genie_space_id_{KEY}"))))
    assert pf.check_id_files(env, warn=False) == 1  # refused, never "nothing missing"
    cli = subprocess.run([sys.executable, str(SHARED / "scripts" / "state_layout.py"), "records-dir",
                          "--env-dir", str(env), "--env", "prod"], text=True, capture_output=True)
    assert cli.returncode == 2 and "still in the checkout" in cli.stderr
    assert (env / name).is_file()  # nothing moved or dropped


def test_without_the_state_dir_genie_records_stay_in_the_checkout(tmp_path, monkeypatch):
    monkeypatch.delenv(state_layout.STATE_DIR_ENV, raising=False)
    env = _checkout_env(tmp_path)
    assert ownership.id_file(env, "Sales") == env / f".genie_space_id_{KEY}"
    assert ownership.adopted_marker(env, "Sales") == env / f".genie_adopted_{KEY}"
    assert pf.marker_for(env, KEY) == env / f".genie_adopt_required_{KEY}"
    cli = subprocess.run([sys.executable, str(SHARED / "scripts" / "state_layout.py"), "records-dir",
                          "--env-dir", str(env), "--env", "prod"], text=True, capture_output=True)
    assert cli.returncode == 0 and cli.stdout.strip() == ""


# -- make migrate-state-dir moves them --------------------------------------------------

def test_migrate_state_dir_moves_genie_records_and_their_recorded_paths(migrated):
    a, old_path, records = migrated
    assert (records / f".genie_space_id_{KEY}").read_text() == "prod-1\n"
    assert (records / ".genie_adopted_other").is_file() and (records / ".genie_adopt_required_other").is_file()
    assert not list(a.glob(".genie_*"))
    moves = json.loads((records / state_layout.GENIE_ID_MOVES).read_text())
    assert moves[old_path] == str(records / f".genie_space_id_{KEY}")
    assert set(moves.values()) == {str(records / f".genie_space_id_{KEY}")}


def test_genie_record_migration_resumes_and_never_rewrites_a_move(tmp_path, monkeypatch):
    monkeypatch.delenv(state_layout.STATE_DIR_ENV, raising=False)
    env = _checkout_env(tmp_path)
    (env / "terraform.tfstate").write_text(json.dumps(_created_state(str(env / f".genie_space_id_{KEY}"))))
    (env / f".genie_space_id_{KEY}").write_text("prod-1\n")
    records = tmp_path / "state" / "aws" / "prod" / "records"
    records.mkdir(parents=True)
    # A crash after copying the ID file, before removing its source.
    shutil.copy2(env / f".genie_space_id_{KEY}", records / f".genie_space_id_{KEY}")
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(tmp_path / "state"))
    with pytest.raises(state_layout.RecordError, match="migrate-state-dir"):
        ownership.agent_id(env, "Sales")
    state_layout.migrate(env, "prod")
    assert not (env / f".genie_space_id_{KEY}").exists()
    assert ownership.agent_id(env, "Sales") == "prod-1"
    before = (records / state_layout.GENIE_ID_MOVES).read_text()

    # A moves entry never changes: the same old path moved elsewhere refuses.
    (records / state_layout.GENIE_ID_MOVES).write_text(json.dumps({str(env / f".genie_space_id_{KEY}"): "/else"}))
    (env / f".genie_space_id_{KEY}").write_text("prod-1\n")
    with pytest.raises(RuntimeError, match="already moved to /else"):
        state_layout.migrate(env, "prod")
    assert (env / f".genie_space_id_{KEY}").is_file()
    assert json.loads(before)


# -- Terraform: the handoff and the create resource survive the move ----------------------

def _tf_env(tmp_path, monkeypatch):
    from tests.terraform_helpers import tf_env

    for name, value in tf_env(tmp_path).items():
        if name.startswith("TF_"):
            monkeypatch.setenv(name, value)
    monkeypatch.delenv("TF_DATA_DIR", raising=False)  # terraform_layer.sh sets its own


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_a_fresh_checkout_keeps_the_create_to_id_handoff_after_migration(tmp_path, monkeypatch):
    """The real workspace root, from a fresh checkout sharing the state dir: the agent, now
    configured by its ID, is still the create resource's handoff (no recreate, no lost ACL record)."""
    from tests.terraform_helpers import shared_copy

    monkeypatch.delenv(state_layout.STATE_DIR_ENV, raising=False)
    runner = shared_copy(tmp_path / "copy") / "scripts" / "terraform_layer.sh"
    spaces = '[{ name = "Sales", genie_space_id = "01handoff", uc_tables = ["cat.sch.t"] }]'
    abac = ('databricks_account_id = "account"\ndatabricks_client_id = "sp"\n'
            'databricks_client_secret = "secret"\ndatabricks_workspace_id = "123"\n'
            f'databricks_workspace_host = "{HOST}"\nsql_warehouse_id = "warehouse"\n'
            f'groups = {{ analysts = {{}} }}\ngenie_spaces = {spaces}\n')
    a = tmp_path / "a" / "aws" / "envs" / "prod"
    a.mkdir(parents=True)
    (a / "abac.auto.tfvars").write_text(abac)
    old = f"{a}/.genie_space_id_{KEY}"
    attrs = {"id": "2afb-create",
             "input": {"value": {"id_file": old}, "type": ["object", {"id_file": "string"}]},
             "output": {"value": {"id_file": old}, "type": ["object", {"id_file": "string"}]},
             "triggers_replace": {"value": {"host": HOST}, "type": ["object", {"host": "string"}]}}
    (a / "terraform.tfstate").write_text(json.dumps({
        "version": 4, "terraform_version": "1.11.4", "serial": 1, "lineage": "test", "outputs": {},
        "resources": [
            {"module": "module.workspace", "mode": "managed", "type": "terraform_data", "name": "genie_space",
             "provider": 'provider["terraform.io/builtin/terraform"]',
             "instances": [{"index_key": KEY, "schema_version": 0, "attributes": attrs,
                            "sensitive_attributes": []}]},
            {"module": "module.workspace", "mode": "managed", "type": "null_resource",
             "name": "genie_space_acls_created", "provider": 'provider["registry.terraform.io/hashicorp/null"]',
             "instances": [{"index_key": KEY, "schema_version": 0, "sensitive_attributes": [],
                            "attributes": {"id": "1", "triggers": {"space_create_id": "2afb-create",
                                                                   "groups": "analysts"}}}]},
        ]}))
    (a / f".genie_space_id_{KEY}").write_text("01handoff\n")
    (tmp_path / "state").mkdir()
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(tmp_path / "state"))
    state_layout.migrate(a, "prod")
    b = tmp_path / "b" / "aws" / "envs" / "prod"
    b.mkdir(parents=True)
    (b / "abac.auto.tfvars").write_text(abac)
    _tf_env(tmp_path, monkeypatch)
    expression = ('jsonencode([keys(local.created_acl_handoffs), keys(local.created_acl_handoff_candidates), '
                  'local.genie_id_dir])')
    result = subprocess.run([str(runner), "workspace", "prod", "console"], input=expression + "\n",
                            capture_output=True, text=True, timeout=600,
                            env={**os.environ, "LAYER_ENV_DIR": str(b), "ENVS_DIR": str(b.parent)})
    assert result.returncode == 0, result.stdout + result.stderr
    value = json.loads(json.loads([line for line in result.stdout.splitlines() if line.strip()][-1]))
    records = (tmp_path / "state").resolve() / "aws" / "prod" / "records"
    assert value == [[KEY], [KEY], str(records)]


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_the_moved_id_file_path_updates_the_create_resource_in_place(tmp_path):
    """terraform_data.genie_space after migration: only input.id_file changes, so the plan is an
    in-place update. No replace, no create (genie_space.sh create never runs), no trash."""
    from tests.terraform_helpers import skip_if_providers_unavailable, tf
    from tests.test_champion_final_run import WORKSPACE_TF, _plan_actions, _resource_block

    block = _resource_block(WORKSPACE_TF.read_text(), 'resource "terraform_data" "genie_space" {')
    block = block.replace("local.managed_created_spaces", "local.new_spaces")
    block = re.sub(r"\n  depends_on = \[.*?\n  \]\n", "\n", block, flags=re.S)
    root = tmp_path / "root"
    root.mkdir()
    (root / "main.tf").write_text(
        'variable "databricks_workspace_host" {}\nvariable "databricks_client_id" {}\n'
        'variable "databricks_client_secret" { sensitive = true }\n'
        'variable "genie_id_file_prefix" {}\nvariable "genie_script_path" {}\n'
        'locals {\n  shared_warehouse_id = "wh"\n'
        '  new_spaces = { sales = { uc_tables = ["c.s.t"], sql_warehouse_id = "", name = "Sales",'
        ' config = { title = "" } } }\n}\n' + block)
    old = str(tmp_path / "checkout" / ".genie_space_id")
    new = str(tmp_path / "state" / "aws" / "prod" / "records" / ".genie_space_id")
    attrs = {"id": "2afb-create",
             "input": {"value": {"id_file": f"{old}_{KEY}"}, "type": ["object", {"id_file": "string"}]},
             "output": {"value": {"id_file": f"{old}_{KEY}"}, "type": ["object", {"id_file": "string"}]},
             "triggers_replace": {"value": {"host": HOST}, "type": ["object", {"host": "string"}]}}
    (root / "terraform.tfstate").write_text(json.dumps({
        "version": 4, "terraform_version": "1.11.4", "serial": 1, "lineage": "test", "outputs": {},
        "resources": [{"mode": "managed", "type": "terraform_data", "name": "genie_space",
                       "provider": 'provider["terraform.io/builtin/terraform"]',
                       "instances": [{"index_key": KEY, "schema_version": 0, "attributes": attrs,
                                      "sensitive_attributes": []}]}]}))
    env = {**os.environ, "TF_IN_AUTOMATION": "1", "TF_DATA_DIR": str(tmp_path / ".terraform")}
    skip_if_providers_unavailable(tf(root, "init", "-input=false", env=env))
    tfvars = {"databricks_workspace_host": HOST, "databricks_client_id": "sp", "databricks_client_secret": "s",
              "genie_script_path": "false"}
    assert _plan_actions(root, {**tfvars, "genie_id_file_prefix": old}, env) == {}
    assert _plan_actions(root, {**tfvars, "genie_id_file_prefix": new}, env) == {
        f'terraform_data.genie_space["{KEY}"]': ["update"]}
