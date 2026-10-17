"""Rollout step 11: capture, per-agent files, JSON-aware remap, prod ownership, detach.

Offline throughout: the Genie API is an in-memory fake that records every
call, and the Terraform runner is a stub script.
"""

from __future__ import annotations

import copy
import json
import re
import sys
import os
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.script_stubs import without_ci

import agent_files
from agent_files import AgentFileError
from scripts import agent_ownership as ownership
from scripts import agent_promote
from scripts import state_layout

SHARED = Path(__file__).resolve().parents[1]
ROOT = SHARED.parent
FIXTURE = SHARED / "tests" / "fixtures" / "genie_roundtrip" / "get_space_response.json"


def _response() -> dict:
    return json.loads(FIXTURE.read_text())


def _payload() -> dict:
    return json.loads(_response()["serialized_space"])


def _env(tmp_path: Path, name: str = "dev", env_cfg: str = "", **agents: str) -> Path:
    env_dir = tmp_path / "envs" / name
    (env_dir / "agents").mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text(env_cfg)
    for agent, text in agents.items():
        (env_dir / "agents" / f"{agent.replace('_', ' ')}.auto.tfvars").write_text(text)
    return env_dir


class FakeGenie:
    """In-memory Genie spaces API: GET/PATCH/POST/DELETE on /api/2.0/genie/spaces."""

    def __init__(self, spaces: dict[str, dict] | None = None, reject: str = "", titles: dict | None = None):
        self.spaces = copy.deepcopy(spaces or {})
        self.calls: list[tuple[str, str, dict]] = []
        self.reject = reject
        self.titles = dict(titles or {})
        self._next = 0

    def do(self, method, path, query=None, body=None):
        self.calls.append((method, path, {"query": query, "body": body}))
        if method == "GET" and path == "/api/2.0/genie/spaces":
            return {"spaces": [{"space_id": k, "title": t} for k, t in sorted(self.titles.items())]}
        space_id = path.rstrip("/").rsplit("/", 1)[-1]
        if method == "GET":
            space = self.spaces[space_id]
            return {"space_id": space_id, "etag": "1", "serialized_space": json.dumps(space)}
        if method == "PATCH":
            sent = json.loads(body["serialized_space"])
            if self.reject and self.reject in sent:
                raise RuntimeError(f"INVALID_PARAMETER_VALUE: unknown field {self.reject}")
            self.spaces[space_id] = sent
            return {"space_id": space_id}
        if method == "POST":
            self._next += 1
            new_id = f"scratch-{self._next}"
            self.spaces[new_id] = json.loads(body["serialized_space"])
            return {"space_id": new_id}
        if method == "DELETE":
            self.spaces.pop(space_id, None)
            return {}
        raise AssertionError(method)

    def methods(self):
        return [call[0] for call in self.calls]


def _all_ids(value) -> list[str]:
    if isinstance(value, dict):
        found = [value["id"]] if isinstance(value.get("id"), str) else []
        return found + [i for item in value.values() for i in _all_ids(item)]
    if isinstance(value, list):
        return [i for item in value for i in _all_ids(item)]
    return []


# ── Loader ───────────────────────────────────────────────────────────────────

def test_loader_merges_agent_files_into_genie_spaces(tmp_path):
    env = _env(tmp_path, env_cfg='governance_mode = "deterministic"\n',
               Sales='genie_space_id = "id-1"\nacl_groups = ["analysts"]\n',
               Ops='acl_groups = []\nuc_tables = ["c.s.t"]\nsql_warehouse_id = "wh"\n')
    spaces = agent_files.merged_config(env)["genie_spaces"]
    assert spaces == [
        {"name": "Ops", "genie_space_id": "", "sql_warehouse_id": "wh", "uc_tables": ["c.s.t"],
         "acl_groups": [], "key": "ops", "detached": False},
        {"name": "Sales", "genie_space_id": "id-1", "sql_warehouse_id": "", "uc_tables": [],
         "acl_groups": ["analysts"], "key": "sales", "detached": False},
    ]
    # Idempotent: merging an already merged config changes nothing.
    assert agent_files.merged_config(env, agent_files.merged_config(env))["genie_spaces"] == spaces


def test_loader_takes_tables_from_the_capture(tmp_path):
    env = _env(tmp_path, Sales='genie_space_id = "id-1"\nacl_groups = ["a"]\n')
    ownership.write_payload(_payload(), agent_files.capture_path(env, "Sales"))
    assert agent_files.load(env, "Sales")["uc_tables"] == ["dev_sales.crm.accounts", "dev_sales.crm.opportunities"]


@pytest.mark.parametrize("text, message", [
    ('genie_space_id = "x"\nowner = "me"\n', "unknown setting"),
    ('detached = true\nacl_groups = ["a"]\n', "acl_groups = \\[\\]"),
    ('detached = true\ndelete = true\nacl_groups = []\n', "not both"),
    ('acl_groups = "a"\n', "list of group names"),
    ('detached = "yes"\n', "true or false"),
])
def test_loader_refuses_bad_agent_files(tmp_path, text, message):
    env = _env(tmp_path, Sales=text)
    with pytest.raises(AgentFileError, match=message):
        agent_files.merged_config(env)


def test_loader_refuses_an_agent_defined_twice_or_an_orphan_capture(tmp_path):
    env = _env(tmp_path, env_cfg='genie_spaces = [{ name = "Sales", genie_space_id = "x" }]\n',
               Sales='acl_groups = ["a"]\n')
    with pytest.raises(AgentFileError, match="keep only the agent file"):
        agent_files.merged_config(env)
    other = _env(tmp_path, name="prod")
    (other / "agents" / "Lost.space.json").write_text("{}")
    with pytest.raises(AgentFileError, match="captured config without settings for Lost"):
        agent_files.merged_config(other)


@pytest.mark.parametrize("name", ["../x", "a/b", ".hidden", "", "x\"y"])
def test_agent_names_are_never_paths(name):
    with pytest.raises(AgentFileError):
        agent_files.settings_path(Path("/tmp"), name)


def test_delete_true_leaves_genie_spaces(tmp_path):
    env = _env(tmp_path, Sales='genie_space_id = "id-1"\nacl_groups = []\ndelete = true\n')
    assert agent_files.merged_config(env)["genie_spaces"] == []


def _ws_state(env_dir: Path) -> Path:
    """The workspace state file where the active layout keeps it (GENIERAILS_STATE_DIR or the env dir)."""
    path = state_layout.root_state_paths(env_dir)["workspace"]
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _state(env_dir: Path, *keys: str) -> None:
    resources = [{"module": "module.workspace", "mode": "managed", "type": "null_resource",
                  "name": "genie_space_acls", "instances": [{"index_key": k} for k in keys]}]
    _ws_state(env_dir).write_text(json.dumps({"resources": resources}))


def test_terraform_spaces_keep_a_detached_agent_only_while_state_holds_it(tmp_path):
    env = _env(tmp_path, Sales='genie_space_id = "id-1"\nacl_groups = []\ndetached = true\n')
    _state(env, "sales")
    assert [s["name"] for s in agent_files.terraform_spaces(env, "workspace")] == ["Sales"]
    _state(env)
    assert agent_files.terraform_spaces(env, "workspace") == []
    # Its tables stay governed.
    assert [s["name"] for s in agent_files.terraform_spaces(env, "data_access")] == ["Sales"]


def test_terraform_spaces_mark_captured_agents_for_the_workspace_layer_only(tmp_path):
    env = _env(tmp_path, Sales='genie_space_id = "id-1"\nacl_groups = ["a"]\n', Ops='acl_groups = ["a"]\n')
    ownership.write_payload({"version": 2}, agent_files.capture_path(env, "Sales"))
    workspace = {s["name"]: s for s in agent_files.terraform_spaces(env, "workspace")}
    assert workspace["Sales"]["captured"] is True and workspace["Ops"]["captured"] is False
    assert all("captured" not in s for s in agent_files.terraform_spaces(env, "data_access"))
    assert agent_files.terraform_spaces(tmp_path / "none", "workspace") is None


def _fake_terraform(tmp_path: Path) -> tuple[dict, Path]:
    log = tmp_path / "terraform.log"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    terraform = bin_dir / "terraform"
    # Record the args and the agent var-file's content (it is gone after the run).
    terraform.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> \"{log}\"\n"
        "for a in \"$@\"; do case \"$a\" in -var-file=*agents.tfvars.json) "
        f"cat \"${{a#-var-file=}}\" > \"{tmp_path}/agents.json\";; esac; done\n")
    terraform.chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}
    return env, log


@pytest.mark.parametrize("layer", ["workspace", "data_access"])
def test_layer_runner_passes_the_merge_after_env_tfvars(tmp_path, layer):
    env_dir = _env(tmp_path, env_cfg="", Sales='genie_space_id = "id-1"\nacl_groups = ["a"]\n')
    layer_dir = env_dir if layer == "workspace" else env_dir / "data_access"
    layer_dir.mkdir(exist_ok=True)
    (layer_dir / "env.auto.tfvars").write_text("")
    env, log = _fake_terraform(tmp_path)
    result = subprocess.run([SHARED / "scripts" / "terraform_layer.sh", layer, "dev", "plan"],
                            text=True, capture_output=True, env={**env, "LAYER_ENV_DIR": str(layer_dir)})
    assert result.returncode == 0, result.stdout + result.stderr
    args = shlex.split(log.read_text().splitlines()[1])
    agent_arg = next(a for a in args if a.endswith("agents.tfvars.json"))
    assert args.index(f"-var-file={layer_dir / 'env.auto.tfvars'}") < args.index(agent_arg)
    assert json.loads((tmp_path / "agents.json").read_text())["genie_spaces"][0]["name"] == "Sales"
    assert not Path(agent_arg.split("=", 1)[1]).exists(), "the merged var-file is temporary"


def test_layer_runner_stops_on_a_bad_agent_file(tmp_path):
    env_dir = _env(tmp_path, Sales='bogus = 1\n')
    env, log = _fake_terraform(tmp_path)
    result = subprocess.run([SHARED / "scripts" / "terraform_layer.sh", "workspace", "dev", "plan"],
                            text=True, capture_output=True, env={**env, "LAYER_ENV_DIR": str(env_dir)})
    assert result.returncode != 0 and "unknown setting" in result.stderr
    assert all(" plan " not in f" {line} " for line in log.read_text().splitlines()[1:])


def test_validate_and_table_readers_read_the_same_merge(tmp_path):
    from table_readers import agents

    env = _env(tmp_path, env_cfg='governance_mode = "deterministic"\n', Sales='acl_groups = ["a"]\nbogus = 1\n')
    result = subprocess.run(["python3", SHARED / "scripts" / "validate_env_config.py", env / "env.auto.tfvars"],
                            text=True, capture_output=True)
    assert result.returncode == 1 and "unknown setting" in result.stderr
    (env / "agents" / "Sales.auto.tfvars").write_text('acl_groups = ["a"]\nuc_tables = ["c.s.t"]\n')
    assert agents(env) == [{"name": "Sales", "acl_groups": ["a"], "tables": ["c.s.t"]}]


# ── Capture ──────────────────────────────────────────────────────────────────

def test_capture_strips_outer_fields_and_keeps_every_inner_id(tmp_path):
    env = _env(tmp_path, Sales='genie_space_id = "01f0aa11bb22cc33dd44ee55ff660001"\nacl_groups = ["a"]\n')
    calls = []
    getter = lambda path, query: calls.append((path, query)) or _response()  # noqa: E731
    before = {p.name for p in env.rglob("*")}
    assert ownership.capture(env, "dev", "Sales", getter) is True
    saved = json.loads(agent_files.capture_path(env, "Sales").read_text())
    assert saved == _payload()
    assert not {"space_id", "etag", "access_control_list", "title", "warehouse_id"} & set(saved)
    assert sorted(_all_ids(saved)) == sorted(_all_ids(_payload())) and len(_all_ids(saved)) == 10
    # Only the capture file was written; one GET, nothing else.
    assert {p.name for p in env.rglob("*")} - before == {"Sales.space.json"}
    assert calls == [("/api/2.0/genie/spaces/01f0aa11bb22cc33dd44ee55ff660001", {"include_serialized_space": "true"})]
    # Idempotent bytes.
    assert ownership.capture(env, "dev", "Sales", getter) is False


def test_capture_drops_outer_fields_even_inside_the_payload(tmp_path):
    response = {"serialized_space": json.dumps({"space_id": "x", "etag": "y", "version": 2})}
    assert ownership.payload_from_response(response) == {"version": 2}


@pytest.mark.parametrize("response, message", [
    ({"space_id": "x"}, "no serialized_space"),
    ({"serialized_space": ""}, "empty serialized_space"),
    ({"serialized_space": "not-json"}, "not valid JSON"),
    ({"serialized_space": "[1]"}, "JSON object"),
])
def test_capture_refuses_a_bad_export(response, message):
    with pytest.raises(ownership.AgentError, match=message):
        ownership.payload_from_response(response)


def test_capture_is_dev_only_and_needs_an_id(tmp_path):
    env = _env(tmp_path, Sales='acl_groups = ["a"]\n')
    with pytest.raises(ownership.AgentError, match="only with ENV=dev"):
        ownership.capture(env, "prod", "Sales", lambda *a: {})
    with pytest.raises(ownership.AgentError, match="has no genie_space_id"):
        ownership.capture(env, "dev", "Sales", lambda *a: {})


def test_capture_never_drafts_governance_calls_a_model_or_writes_to_genie():
    import inspect

    module = (SHARED / "scripts" / "agent_ownership.py").read_text()
    assert "generate_abac" not in module and "serving" not in module
    source = inspect.getsource(ownership.capture) + inspect.getsource(ownership.import_new)
    assert '"GET"' in source and not any(m in source for m in ('"PATCH"', '"POST"', '"PUT"', '"DELETE"'))


def test_generate_imports_only_agents_not_yet_captured(tmp_path):
    env = _env(tmp_path, New='genie_space_id = "new-1"\nacl_groups = ["a"]\n',
               Old='genie_space_id = "old-1"\nacl_groups = ["a"]\n', Draft='acl_groups = ["a"]\n')
    old = agent_files.capture_path(env, "Old")
    old.write_text('{"curated": "in git"}\n')
    calls = []
    getter = lambda path, query: calls.append(path) or _response()  # noqa: E731
    assert ownership.import_new(env, "dev", getter) == ["New"]
    assert calls == ["/api/2.0/genie/spaces/new-1"]
    assert old.read_text() == '{"curated": "in git"}\n', "an agent already in git is never re-captured"
    assert ownership.import_new(env, "dev", getter) == [] and len(calls) == 1
    assert ownership.import_new(env, "prod", getter) == []


def _git_repo(env):
    """Make the env's checkout a git repo and commit what is in it."""
    root = env.parent.parent
    git = ["git", "-C", str(root), "-c", "user.email=t@example.com", "-c", "user.name=t"]
    if not (root / ".git").exists():
        subprocess.run([*git, "init", "-q"], check=True)
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "--allow-empty", "-m", "commit"], check=True)


@pytest.mark.parametrize("deleted", ["in the working tree", "in a commit"])
def test_generate_refuses_an_agent_whose_committed_capture_is_gone(tmp_path, deleted):
    env = _env(tmp_path, Sales='genie_space_id = "sales-1"\nacl_groups = ["a"]\n',
               New='genie_space_id = "new-1"\nacl_groups = ["a"]\n')
    capture = agent_files.capture_path(env, "Sales")
    capture.write_text('{"curated": "reviewed"}\n')
    (env / "agents" / "New.auto.tfvars").unlink()
    _git_repo(env)
    (env / "agents" / "New.auto.tfvars").write_text('genie_space_id = "new-1"\nacl_groups = ["a"]\n')
    capture.unlink()
    if deleted == "in a commit":
        _git_repo(env)
    calls = []
    getter = lambda path, query: calls.append(path) or _response()  # noqa: E731
    before = sorted(p.name for p in (env / "agents").iterdir())

    for check in (True, False):
        with pytest.raises(ownership.AgentError) as refused:
            ownership.import_new(env, "dev", getter, check=check)
        assert 'agents/Sales.space.json: restore it from git, or replace it with the live dev agent: ' \
               'make capture ENV=dev SPACE="Sales"' in str(refused.value)
        assert "Nothing was written." in str(refused.value)

    # Refused before any read or write, the new agent's capture included.
    assert calls == [] and sorted(p.name for p in (env / "agents").iterdir()) == before
    # make capture replaces it on purpose; then the new agent imports once.
    ownership.capture(env, "dev", "Sales", getter)
    assert ownership.import_new(env, "dev", getter) == ["New"]
    assert ownership.import_new(env, "dev", getter) == []
    assert calls == ["/api/2.0/genie/spaces/sales-1", "/api/2.0/genie/spaces/new-1"]


def test_a_new_agent_in_a_git_checkout_is_captured_once(tmp_path):
    env = _env(tmp_path, New='genie_space_id = "new-1"\nacl_groups = ["a"]\n')
    _git_repo(env)  # its settings file is committed; no capture ever was
    calls = []
    getter = lambda path, query: calls.append(path) or _response()  # noqa: E731

    assert ownership.import_new(env, "dev", getter, check=True) == ["New"] and calls == []
    assert ownership.import_new(env, "dev", getter) == ["New"]
    assert ownership.import_new(env, "dev", getter) == []
    assert calls == ["/api/2.0/genie/spaces/new-1"]
    assert not ownership.capture_committed(env, "New")
    _git_repo(env)
    assert ownership.capture_committed(env, "New")


# ── Remap ────────────────────────────────────────────────────────────────────

def test_remap_changes_identifiers_in_values_never_keys_or_prose():
    source = {
        "dev_sales.key": "dev_sales.crm.accounts",
        "items": [
            "SELECT * FROM dev_sales.crm.accounts JOIN `dev_sales`.crm.o USING (id)",
            "the dev_sales team owns this agent",
            "mydev_sales.crm.t and x.dev_sales.t",
            "dev_sales",
            "warehouse a1b2c3d4e5f60718 / a1b2c3d4e5f60718x",
        ],
        "n": 3, "flag": True, "none": None,
    }
    assert agent_promote.remap_payload(source, {"dev_sales": "prod_sales"}, {"a1b2c3d4e5f60718": "ffff0000"}) == {
        "dev_sales.key": "prod_sales.crm.accounts",
        "items": [
            "SELECT * FROM prod_sales.crm.accounts JOIN `prod_sales`.crm.o USING (id)",
            "the dev_sales team owns this agent",
            "mydev_sales.crm.t and x.dev_sales.t",
            "prod_sales",
            "warehouse ffff0000 / a1b2c3d4e5f60718x",
        ],
        "n": 3, "flag": True, "none": None,
    }


def test_remap_prefers_the_longest_catalog_and_keeps_every_id():
    payload = _payload()
    mapped = agent_promote.remap_payload(payload, {"dev": "prod", "dev_sales": "prod_sales"})
    text = json.dumps(mapped)
    assert "dev_sales." not in text.replace("dev_sales team", "")
    assert "prod_sales.crm.accounts" in text and "`prod_sales`.crm.opportunities" in text
    assert "the dev_sales team owns this agent" in text  # prose
    assert _all_ids(mapped) == _all_ids(payload)
    assert agent_promote.remap_payload(mapped, {"prod_sales": "dev_sales"}) == payload


# ── promote-to ───────────────────────────────────────────────────────────────

def _promote_envs(tmp_path):
    dev = _env(tmp_path, env_cfg='sql_warehouse_id = "a1b2c3d4e5f60718"\n',
               Sales='genie_space_id = "dev-id"\nacl_groups = ["sales_analysts"]\n',
               Draft='genie_space_id = "draft-id"\nacl_groups = ["a"]\n')
    ownership.write_payload(_payload(), agent_files.capture_path(dev, "Sales"))
    prod = tmp_path / "envs" / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text('sql_warehouse_id = "prodwh0000000001"\n')
    return dev, prod


def test_promote_seeds_settings_once_and_rewrites_the_capture(tmp_path):
    dev, prod = _promote_envs(tmp_path)
    assert agent_promote.promote(dev, prod, {"dev_sales": "prod_sales"}) == ["Sales"]
    settings = prod / "agents" / "Sales.auto.tfvars"
    seeded = agent_files.load(prod, "Sales")
    assert seeded["genie_space_id"] == "" and seeded["acl_groups"] == ["sales_analysts"]
    assert "dev-id" not in settings.read_text()
    assert not (prod / "agents" / "Draft.space.json").exists(), "an uncaptured agent is not promoted"
    promoted = json.loads((prod / "agents" / "Sales.space.json").read_text())
    assert promoted == agent_promote.remap_payload(_payload(), {"dev_sales": "prod_sales"},
                                                   {"a1b2c3d4e5f60718": "prodwh0000000001"})
    # Env-owned: a prod edit survives the next promotion; the capture is rewritten.
    settings.write_text('genie_space_id = ""\nacl_groups = ["prod_sales_readers"]\n')
    (prod / "agents" / "Sales.space.json").write_text("{}")
    agent_promote.promote(dev, prod, {"dev_sales": "prod_sales"})
    assert agent_files.load(prod, "Sales")["acl_groups"] == ["prod_sales_readers"]
    assert json.loads((prod / "agents" / "Sales.space.json").read_text()) == promoted


def test_promote_is_a_pure_function_of_the_committed_files(tmp_path, monkeypatch):
    dev, prod = _promote_envs(tmp_path)
    monkeypatch.setattr(ownership, "_api", lambda *a: pytest.fail("promote-to read live Databricks"))
    _ws_state(prod).write_text("not read")
    agent_promote.promote(dev, prod, {"dev_sales": "prod_sales"})
    first = {p.name: p.read_bytes() for p in (prod / "agents").iterdir()}
    shutil.rmtree(prod / "agents")
    agent_promote.promote(dev, prod, {"dev_sales": "prod_sales"})
    assert {p.name: p.read_bytes() for p in (prod / "agents").iterdir()} == first


def test_promote_keeps_agents_only_in_the_target(tmp_path, capsys):
    dev, prod = _promote_envs(tmp_path)
    (prod / "agents").mkdir()
    (prod / "agents" / "Legacy.auto.tfvars").write_text('acl_groups = []\n')
    agent_promote.promote(dev, prod, {})
    assert (prod / "agents" / "Legacy.auto.tfvars").is_file()
    assert "Legacy: only in prod; kept" in capsys.readouterr().out


def test_promote_preserves_a_detached_target_agent(tmp_path):
    dev, prod = _promote_envs(tmp_path)
    (prod / "agents").mkdir()
    settings = prod / "agents" / "Sales.auto.tfvars"
    settings.write_text('genie_space_id = ""\nacl_groups = []\ndetached = true\n')

    with pytest.raises(AgentFileError, match="committed detached"):
        agent_promote.promote(dev, prod, {"dev_sales": "prod_sales"})

    assert agent_files.load(prod, "Sales")["detached"] is True
    assert agent_files.load(prod, "Sales")["acl_groups"] == []


def test_promote_refuses_a_committed_detached_tombstone_without_receipts(tmp_path, monkeypatch):
    dev, prod = _promote_envs(tmp_path)
    (prod / "agents").mkdir()
    (prod / "agents" / "Sales.auto.tfvars").write_text(
        'genie_space_id = ""\nacl_groups = []\ndetached = true\n'
    )
    monkeypatch.setattr(ownership, "load_receipt", lambda *_: pytest.fail("promotion read a receipt"))
    before = {p.relative_to(prod): p.read_bytes() for p in prod.rglob("*") if p.is_file()}

    with pytest.raises(AgentFileError, match="committed detached"):
        agent_promote.promote(dev, prod, {"dev_sales": "prod_sales"})

    assert {p.relative_to(prod): p.read_bytes() for p in prod.rglob("*") if p.is_file()} == before


@pytest.mark.parametrize("source_name", ["sales", "SALES", "Sales-"])
def test_promote_matches_detached_tombstones_by_space_key(tmp_path, source_name):
    dev, prod = _promote_envs(tmp_path)
    source_settings = dev / "agents" / "Sales.auto.tfvars"
    source_capture = dev / "agents" / "Sales.space.json"
    source_settings.rename(dev / "agents" / f"{source_name}.auto.tfvars")
    source_capture.rename(dev / "agents" / f"{source_name}.space.json")
    (prod / "agents").mkdir()
    (prod / "agents" / "Sales.auto.tfvars").write_text(
        'genie_space_id = ""\nacl_groups = []\ndetached = true\n'
    )

    with pytest.raises(AgentFileError, match="committed detached"):
        agent_promote.promote(dev, prod, {"dev_sales": "prod_sales"})


def test_promote_keeps_removed_capture_for_detached_source(tmp_path):
    dev, prod = _promote_envs(tmp_path)
    (dev / "agents" / "Sales.auto.tfvars").write_text(
        'genie_space_id = "dev-id"\nacl_groups = []\ndetached = true\n'
    )
    (prod / "agents").mkdir()
    (prod / "agents" / "Sales.auto.tfvars").write_text(
        'genie_space_id = ""\nacl_groups = []\ndetached = true\n'
    )
    (prod / "ack.txt").write_text("prod:detached-agent:sales\n")

    agent_promote.promote(dev, prod, {"dev_sales": "prod_sales"})

    assert not (prod / "agents" / "Sales.space.json").exists()


@pytest.mark.parametrize("mutation", ["active", "deleted"])
def test_governance_tombstone_refuses_agent_only_revival(tmp_path, mutation):
    dev, prod = _promote_envs(tmp_path)
    (prod / "agents").mkdir()
    settings = prod / "agents" / "Sales.auto.tfvars"
    settings.write_text('genie_space_id = ""\nacl_groups = []\ndetached = true\n')
    (prod / "ack.txt").write_text("prod:detached-agent:sales\n")
    if mutation == "active":
        settings.write_text('genie_space_id = ""\nacl_groups = ["anyone"]\n')
    else:
        settings.unlink()

    with pytest.raises(AgentFileError, match="governance-reviewed PR"):
        agent_promote.promote(dev, prod, {})


def test_removing_governance_tombstone_allows_revival(tmp_path):
    dev, prod = _promote_envs(tmp_path)
    (prod / "ack.txt").write_text("")

    agent_promote.promote(dev, prod, {"dev_sales": "prod_sales"})

    assert agent_files.load(prod, "Sales")["acl_groups"] == ["sales_analysts"]


def test_gap3_promote_checks_tombstone_when_source_has_no_agents(tmp_path):
    dev = _env(tmp_path)
    prod = _env(tmp_path, name="prod", Sales='genie_space_id = ""\nacl_groups = ["anyone"]\n')
    (prod / "ack.txt").write_text("prod:detached-agent:sales\n")

    with pytest.raises(AgentFileError, match="governance-reviewed PR"):
        agent_promote.promote(dev, prod, {})


def test_gap2_release_plan_enforces_tombstone_without_capture(tmp_path):
    prod = _env(tmp_path, name="prod", Sales='genie_space_id = ""\nacl_groups = ["anyone"]\n')
    (prod / "ack.txt").write_text("prod:detached-agent:sales\n")

    with pytest.raises(ownership.AgentError, match="still retired"):
        ownership.check_detach_acks(prod, "prod")


# ── Prod ownership ───────────────────────────────────────────────────────────

def _prod(tmp_path, settings='genie_space_id = ""\nacl_groups = ["a"]\n', payload=None) -> Path:
    prod = _env(tmp_path, name="prod", Sales=settings)
    ownership.write_payload(payload or _payload(), agent_files.capture_path(prod, "Sales"))
    (prod / ".genie_space_id_sales").write_text("prod-1\n")
    return prod


def test_sync_pushes_the_full_capture_once_and_reports_each_field(tmp_path, capsys):
    prod = _prod(tmp_path)
    live = copy.deepcopy(_payload())
    live["instructions"]["text_instructions"][0]["content"] = ["edited in prod UI"]
    del live["benchmarks"]
    live["config"]["sample_questions"].pop()
    api = FakeGenie({"prod-1": live})
    reports = ownership.sync(prod, "prod", api)
    assert api.methods() == ["GET", "PATCH"]
    assert api.calls[1][1] == "/api/2.0/genie/spaces/prod-1", "never a new agent or another ID"
    assert api.spaces["prod-1"] == _payload(), "full replacement"
    assert reports["Sales"] == [
        "benchmarks (added)",
        "config.sample_questions[id=01f0sq000000000000000000000002] (added)",
        "instructions.text_instructions[id=01f0ti000000000000000000000001].content",
    ]
    out = capsys.readouterr().out
    assert "overwrote instructions.text_instructions[id=01f0ti000000000000000000000001].content" in out


def test_sync_fails_when_any_field_is_rejected_and_never_retries_without_it(tmp_path):
    prod = _prod(tmp_path)
    api = FakeGenie({"prod-1": {}}, reject="benchmarks")
    with pytest.raises(ownership.AgentError, match="rejected the captured config"):
        ownership.sync(prod, "prod", api)
    assert api.methods() == ["GET", "PATCH"]


def test_sync_refuses_an_answer_for_another_agent(tmp_path):
    prod = _prod(tmp_path)

    class Moved(FakeGenie):
        def do(self, method, path, query=None, body=None):
            result = super().do(method, path, query, body)
            return {"space_id": "other"} if method == "PATCH" else result

    with pytest.raises(ownership.AgentError, match="not prod-1"):
        ownership.sync(prod, "prod", Moved({"prod-1": {}}))


def test_dev_agent_config_is_never_overwritten(tmp_path):
    dev = _env(tmp_path, Sales='genie_space_id = "dev-1"\nacl_groups = ["a"]\n')
    ownership.write_payload(_payload(), agent_files.capture_path(dev, "Sales"))
    api = FakeGenie({"dev-1": {"curated": True}})
    assert ownership.sync(dev, "dev", api) == {}
    assert api.calls == []


def test_sync_skips_detached_agents_and_needs_an_id(tmp_path):
    prod = _prod(tmp_path, settings='acl_groups = []\ndetached = true\n')
    api = FakeGenie({"prod-1": {}})
    assert ownership.sync(prod, "prod", api) == {} and api.calls == []
    other = _prod(tmp_path / "x")
    (other / ".genie_space_id_sales").unlink()
    with pytest.raises(ownership.AgentError, match="has no ID after apply"):
        ownership.sync(other, "prod", FakeGenie())


def test_delete_true_deletes_an_agent_configured_by_id_once_acknowledged(tmp_path):
    prod = _env(tmp_path, name="prod", Old='genie_space_id = "old-1"\nacl_groups = []\ndelete = true\n')
    api = FakeGenie({"old-1": {}})
    with pytest.raises(ownership.AgentError, match="add prod:delete-agent:old to envs/prod/ack.txt"):
        ownership.delete_agents(prod, "prod", api)
    assert api.calls == []
    (prod / "ack.txt").write_text("prod:delete-agent:old\n")
    assert ownership.delete_agents(prod, "prod", api) == ["Old"] and "old-1" not in api.spaces


def _git_repo(path: Path) -> str:
    subprocess.run(["git", "init", "-q", str(path)], check=True)
    for key, value in (("user.email", "t@example.com"), ("user.name", "t"), ("commit.gpgsign", "false")):
        subprocess.run(["git", "-C", str(path), "config", key, value], check=True)
    subprocess.run(["git", "-C", str(path), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(path), "commit", "-qm", "c", "--no-verify"], check=True)
    return subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"], check=True, text=True,
                          capture_output=True).stdout.strip()


def test_receipt_outside_git_records_no_commit(tmp_path, capsys):
    prod = _prod(tmp_path)
    assert ownership.write_receipt(prod, "prod")["commit"] == ""
    assert "records no commit" in capsys.readouterr().out


def test_receipt_keeps_prod_ids_out_of_git_and_restores_lost_id_files(tmp_path, monkeypatch):
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    prod = _prod(tmp_path)
    head = _git_repo(tmp_path)
    tracked_before = subprocess.run(["git", "-C", str(tmp_path), "status", "--porcelain"],
                                    text=True, capture_output=True).stdout
    receipt = ownership.write_receipt(prod, "prod")
    assert receipt["commit"] == head and receipt["agents"]["Sales"]["genie_space_id"] == "prod-1"
    assert "prod-1" not in (prod / "agents" / "Sales.auto.tfvars").read_text()
    (prod / ".genie_space_id_sales").unlink()
    assert ownership.legacy_receipt_path(prod, "prod").is_relative_to(tmp_path / "state")
    api = FakeGenie({"prod-1": _payload()})
    assert ownership.restore_ids(prod, "prod", api) == ["Sales"]
    assert (prod / ".genie_space_id_sales").read_text() == "prod-1\n"
    assert ownership.restore_ids(prod, "prod", api) == [], "an ID file in place is never overwritten"
    # Only ignored/untracked runtime files changed: no committed file was written.
    assert subprocess.run(["git", "-C", str(tmp_path), "diff", "--stat"], text=True,
                          capture_output=True).stdout == ""
    assert tracked_before == ""


# ── Detach ───────────────────────────────────────────────────────────────────

STATE_LIST = "\n".join([
    'module.workspace.null_resource.genie_space_acls_created["sales"]',
    'module.workspace.terraform_data.genie_space["sales"]',
    'module.workspace.null_resource.genie_space_config["sales"]',
    'module.workspace.terraform_data.genie_space["sales_eu"]',
    'module.workspace.null_resource.genie_space_acls["salesx"]',
    'module.data_access.databricks_grant.table["sales"]',
])


def _runner(tmp_path: Path) -> tuple[Path, Path]:
    log = tmp_path / "runner.log"
    runner = tmp_path / "runner.sh"
    runner.write_text("#!/bin/sh\n"
                      f"printf '%s\\n' \"$*\" >> \"{log}\"\n"
                      f"if [ \"$3\" = state-list ]; then cat <<'EOF'\n{STATE_LIST}\nEOF\nfi\n")
    runner.chmod(0o755)
    return runner, log


def _released_detached(tmp_path):
    prod = _prod(tmp_path, settings='genie_space_id = ""\nacl_groups = []\ndetached = true\n')
    head = _git_repo(tmp_path)
    ownership.write_receipt(prod, "prod", commit=head)
    return prod, head


@pytest.fixture
def detach_lock(monkeypatch):
    monkeypatch.setattr(ownership, "_lock_owner", lambda env_dir: "detach-agent")


def test_detach_removes_exactly_the_agents_addresses(tmp_path, detach_lock):
    prod, head = _released_detached(tmp_path)
    runner, log = _runner(tmp_path)
    removed = ownership.detach(prod, "prod", "Sales", runner, commit=head, clean=True)
    assert removed == [
        'module.workspace.null_resource.genie_space_acls_created["sales"]',
        'module.workspace.terraform_data.genie_space["sales"]',
        'module.workspace.null_resource.genie_space_config["sales"]',
    ]
    calls = log.read_text().splitlines()
    assert calls[0] == "workspace prod state-list"
    assert calls[1:] == [f"workspace prod state-rm {a}" for a in removed]
    assert all("destroy" not in c and "apply" not in c for c in calls)


def test_detach_refuses_unless_the_detached_change_is_the_released_head(tmp_path, detach_lock):
    runner, log = _runner(tmp_path)
    attached = _prod(tmp_path / "a")
    with pytest.raises(ownership.AgentError, match="not detached"):
        ownership.detach(attached, "prod", "Sales", runner, commit="x", clean=True)
    prod, head = _released_detached(tmp_path / "b")
    with pytest.raises(ownership.AgentError, match="not the last released commit"):
        ownership.detach(prod, "prod", "Sales", runner, commit="0" * 40, clean=True)
    with pytest.raises(ownership.AgentError, match="uncommitted changes"):
        ownership.detach(prod, "prod", "Sales", runner, commit=head, clean=False)
    # Released while still attached, detached only in the checkout.
    receipt_path = ownership.legacy_receipt_path(prod, "prod")
    receipt = json.loads(receipt_path.read_text())
    receipt["agents"]["Sales"].update(detached=False, acl_groups=["a"])
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(ownership.AgentError, match="did not have Sales detached"):
        ownership.detach(prod, "prod", "Sales", runner, commit=head, clean=True)
    receipt_path.unlink()
    with pytest.raises(ownership.AgentError, match="no release receipt"):
        ownership.detach(prod, "prod", "Sales", runner, commit=head, clean=True)
    ownership.write_receipt(prod, "prod", commit="")
    with pytest.raises(ownership.AgentError, match="recorded no commit"):
        ownership.detach(prod, "prod", "Sales", runner, commit=head, clean=True)
    assert not log.exists(), "no state command runs before every check passed"


def test_removing_an_agent_still_in_state_is_refused(tmp_path):
    env = _env(tmp_path, name="prod", env_cfg='governance_mode = "deterministic"\n',
               Sales='acl_groups = []\ndetached = true\n')
    _state(env, "sales", "ops")
    with pytest.raises(ownership.AgentError, match="still holds them: ops"):
        ownership.guard_removals(env)
    (env / "agents" / "Ops.auto.tfvars").write_text('acl_groups = []\ndelete = true\n')
    ownership.guard_removals(env)  # delete = true is the other way out
    (env / "agents" / "Ops.auto.tfvars").unlink()
    _state(env, "sales")
    ownership.guard_removals(env)
    (env / "agents" / "Sales.auto.tfvars").unlink()
    with pytest.raises(ownership.AgentError, match="make detach-agent"):
        ownership.guard_removals(env)
    _state(env)
    ownership.guard_removals(env)  # after detach-agent: removal plans no destroy


def test_legacy_envs_keep_remove_means_destroy(tmp_path):
    env = _env(tmp_path, env_cfg="")
    shutil.rmtree(env / "agents")
    _state(env, "gone")
    ownership.guard_removals(env)


# ── Round trip ───────────────────────────────────────────────────────────────

def test_round_trip_fidelity_for_every_field_type(tmp_path):
    """Export, import into a scratch space, export again: the diff is empty."""
    source = _payload()
    # Every field type the format has is in the recorded fixture.
    assert set(source) == {"version", "config", "data_sources", "instructions", "benchmarks"}
    assert set(source["instructions"]) == {"text_instructions", "example_question_sqls", "sql_functions",
                                           "join_specs", "sql_snippets"}
    assert set(source["instructions"]["sql_snippets"]) == {"filters", "expressions", "measures"}
    assert set(source["data_sources"]) == {"tables", "metric_views"}

    api = FakeGenie({"01f0aa11bb22cc33dd44ee55ff660001": source})
    dev = _env(tmp_path, Sales='genie_space_id = "01f0aa11bb22cc33dd44ee55ff660001"\nacl_groups = ["a"]\n')
    ownership.capture(dev, "dev", "Sales", lambda path, query: api.do("GET", path, query=query))
    exported = json.loads(agent_files.capture_path(dev, "Sales").read_text())

    scratch = api.do("POST", "/api/2.0/genie/spaces", body={"serialized_space": json.dumps(exported)})["space_id"]
    prod = _env(tmp_path, name="prod", Sales='acl_groups = ["a"]\n')
    (prod / "agents" / "Sales.space.json").write_text(ownership.render(exported))
    (prod / ".genie_space_id_sales").write_text(scratch + "\n")
    assert ownership.sync(prod, "prod", api)["Sales"] == []
    again = ownership.payload_from_response(api.do("GET", f"/api/2.0/genie/spaces/{scratch}"))
    assert ownership.changed_fields(exported, again) == []
    assert ownership.render(again) == ownership.render(exported)
    api.do("DELETE", f"/api/2.0/genie/spaces/{scratch}")
    assert scratch not in api.spaces


def test_live_round_trip_script_is_gated_and_cleans_up():
    script = (SHARED / "scripts" / "genie_roundtrip_live.py").read_text()
    assert 'DATABRICKS_LIVE_TESTS' in script and "finally:" in script and '"DELETE"' in script
    result = subprocess.run(["python3", SHARED / "scripts" / "genie_roundtrip_live.py", "--space-id", "x"],
                            text=True, capture_output=True, env={k: v for k, v in os.environ.items()
                                                                 if k != "DATABRICKS_LIVE_TESTS"})
    assert result.returncode == 2 and "DATABRICKS_LIVE_TESTS=1" in result.stderr


@pytest.mark.skipif(os.environ.get("DATABRICKS_LIVE_TESTS") != "1", reason="live: set DATABRICKS_LIVE_TESTS=1")
def test_live_round_trip():  # pragma: no cover - live only
    space_id = os.environ.get("GENIE_ROUNDTRIP_SPACE_ID", "")
    if not space_id:
        pytest.skip("set GENIE_ROUNDTRIP_SPACE_ID to the agent to export")
    result = subprocess.run(["python3", SHARED / "scripts" / "genie_roundtrip_live.py", "--space-id", space_id],
                            text=True, capture_output=True)
    assert result.returncode == 0, result.stdout + result.stderr


# ── Makefile ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("cloud", ["aws", "azure"])
@pytest.mark.parametrize("target", ["capture", "detach-agent"])
def test_make_dry_run_for_agent_targets(cloud, target, tmp_path):
    env = {k: v for k, v in without_ci(os.environ).items() if k not in ("MAKEFLAGS", "MAKELEVEL")}
    result = subprocess.run(["make", "-n", "-C", str(ROOT / cloud), target, "ENV=dev", "SPACE=Sales"],
                            text=True, capture_output=True, env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "agent_ownership.py" in result.stdout


def test_capture_target_refuses_other_envs_and_needs_space():
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL")}
    no_space = subprocess.run(["make", "-C", str(ROOT / "aws"), "capture", "ENV=dev"],
                              text=True, capture_output=True, env=env)
    assert no_space.returncode != 0 and 'SPACE="<agent>" is required' in no_space.stderr
    prod = subprocess.run(["make", "-C", str(ROOT / "aws"), "capture", "ENV=prod", "SPACE=Sales"],
                          text=True, capture_output=True, env=env)
    assert prod.returncode != 0 and "only with ENV=dev" in prod.stderr


def test_release_pushes_agents_after_apply_and_writes_the_receipt_last():
    makefile = (SHARED / "Makefile.shared").read_text()
    release = makefile[makefile.index("\nrelease:"):makefile.index("\nmaintain:")]
    order = [release.index(s) for s in ("restore-ids", '$(MAKE) apply ENV="$(ENV)"', "$(_AGENT_OWNERSHIP) sync",
                                        "verify-access ENV", "$(_AGENT_OWNERSHIP) receipt", "Release complete")]
    assert order == sorted(order)
    rehearse = makefile[makefile.index("\nrehearse:"):makefile.index("\noverride _RUN_AUDIT_RULEBOOK")]
    maintain = makefile[makefile.index("\nmaintain:"):makefile.index("\nvalidate:")]
    for recipe in (rehearse, maintain):
        assert "_AGENT_OWNERSHIP" not in recipe, "only release pushes agent config"
    generate = makefile[makefile.index("\ngenerate:"):makefile.index("\n# Agents (scripts/agent_ownership.py")]
    assert "import-new" in generate and " capture " not in generate


def test_detach_agent_is_an_operations_command_that_takes_the_env_lock():
    makefile = (SHARED / "Makefile.shared").read_text()
    recipe = makefile[makefile.index("\ndetach-agent:"):]
    recipe = recipe[:recipe.index("\n\n")]
    assert "_guard-not-ci-apply" in recipe.splitlines()[1] and "_ENV_LOCK,detach-agent" in recipe


# ── Step 9 integration: manifest, release lock, release receipt ──────────────

def test_detach_refuses_outside_its_own_env_lock(tmp_path, monkeypatch):
    prod, head = _released_detached(tmp_path)
    runner, log = _runner(tmp_path)
    for owner in (None, "release", "maintain"):
        monkeypatch.setattr(ownership, "_lock_owner", lambda env_dir, owner=owner: owner)
        with pytest.raises(ownership.AgentError, match="only as make detach-agent"):
            ownership.detach(prod, "prod", "Sales", runner, commit=head, clean=True)
    assert not log.exists()


def _deterministic_prod(tmp_path, monkeypatch, settings='genie_space_id = ""\nacl_groups = ["a"]\n'):
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    prod = _env(tmp_path, name="prod", env_cfg='governance_mode = "deterministic"\npromote_from = "dev"\n',
                Sales=settings)
    ownership.write_payload(_payload(), agent_files.capture_path(prod, "Sales"))
    _created(prod, "sales", "prod-1")
    return prod


def _step9_receipt(prod: Path, commit: str, agents: dict) -> None:
    from scripts import release_gate as rg

    cloud = rg.cloud_dir_of(prod)
    rg._write_once(rg.release_dir(cloud, "prod") / f"{rg._now().replace(':', '')}.json",
                   {"kind": "release", "version": rg.VERSION, "env": "prod", "commit": commit,
                    "created_at": rg._now(), "agents": agents})


def test_deterministic_detach_reads_the_step9_release_receipt(tmp_path, monkeypatch, detach_lock):
    prod = _deterministic_prod(tmp_path, monkeypatch, 'genie_space_id = ""\nacl_groups = []\ndetached = true\n')
    head = _git_repo(tmp_path)
    runner, log = _runner(tmp_path)
    with pytest.raises(ownership.AgentError, match="has no release receipt"):
        ownership.detach(prod, "prod", "Sales", runner, commit=head, clean=True)
    # The legacy receipt doesn't count in a deterministic env.
    ownership.write_receipt(prod, "prod", commit=head)
    with pytest.raises(ownership.AgentError, match="has no release receipt"):
        ownership.detach(prod, "prod", "Sales", runner, commit=head, clean=True)
    _step9_receipt(prod, head, {"Sales": {"genie_space_id": "prod-1", "acl_groups": ["a"], "detached": False}})
    with pytest.raises(ownership.AgentError, match="did not have Sales detached"):
        ownership.detach(prod, "prod", "Sales", runner, commit=head, clean=True)
    _step9_receipt(prod, head, {"Sales": {"genie_space_id": "prod-1", "acl_groups": [], "detached": True}})
    assert len(ownership.detach(prod, "prod", "Sales", runner, commit=head, clean=True)) == 3


def _released(space_id="prod-1", **overrides) -> dict:
    return {"genie_space_id": space_id, "key": "sales", "lifecycle": "active", "detached": False,
            "config_sha256": ownership._sha(ownership.render(_payload())), "binding": "existing",
            "create_resource_id": "tfdata-sales"} | overrides


def test_deterministic_ids_are_recovered_from_the_step9_release_receipt(tmp_path, monkeypatch):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    (prod / ".genie_space_id_sales").unlink()
    api = FakeGenie({"prod-1": _payload()})
    assert ownership.restore_ids(prod, "prod", api) == []
    _step9_receipt(prod, "c1", {"Sales": _released()})
    assert ownership.restore_ids(prod, "prod", api) == ["Sales"]
    assert (prod / ".genie_space_id_sales").read_text() == "prod-1\n"


def test_the_release_receipt_records_each_agents_id_and_detach_state(tmp_path, monkeypatch):
    from scripts import release_gate as rg
    from tests.test_release_gate import Repo

    repo = Repo(tmp_path, monkeypatch)
    repo.commit({"aws/envs/prod/agents/Sales.auto.tfvars": 'genie_space_id = ""\nacl_groups = []\ndetached = true\n'})
    (repo.prod / ".genie_space_id_sales").write_text("prod-9\n")
    manifest = tmp_path / "m.json"
    manifest.write_text(json.dumps({"kind": "release-manifest", "version": rg.VERSION, "id": "m", "env": "prod",
                                    "commit": repo.head(), "effective_digest": "e", "live_snapshot": "l"}))
    receipt = json.loads(rg.release_receipt(repo.prod, "prod", manifest).read_text())
    assert receipt["agents"]["Sales"] == {"genie_space_id": "prod-9", "key": "sales", "acl_groups": [],
                                          "detached": True, "delete": False, "lifecycle": "detached",
                                          "config_sha256": None, "binding": "adopt",
                                          "create_resource_id": None, "created_space_id": None}
    assert "prod-9" not in repo.git("show", "HEAD:aws/envs/prod/agents/Sales.auto.tfvars")


def test_detached_receipt_tombstone_survives_the_next_real_receipt(tmp_path, monkeypatch):
    from scripts import release_gate as rg

    prod = _deterministic_prod(
        tmp_path, monkeypatch, 'genie_space_id = ""\nacl_groups = []\ndetached = true\n'
    )
    stamps = iter(["2026-01-01T00:00:01Z", "2026-01-01T00:00:01Z",
                   "2026-01-01T00:00:02Z", "2026-01-01T00:00:02Z"])
    monkeypatch.setattr(rg, "_now", lambda: next(stamps))
    first = ownership.receipt_agents(prod)
    _step9_receipt(prod, "c1", first)
    (prod / "agents" / "Sales.auto.tfvars").unlink()
    (prod / "agents" / "Sales.space.json").unlink()
    second = ownership.receipt_agents(prod)
    _step9_receipt(prod, "c2", second)

    assert ownership.load_receipt(prod, "prod")["agents"]["Sales"]["lifecycle"] == "detached"


def test_release_plan_refuses_old_receipt_reactivation_without_governance_ack(tmp_path, monkeypatch):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    _step9_receipt(prod, "c1", {"Sales": _released(detached=True, lifecycle="detached")})

    with pytest.raises(ownership.AgentError, match="revive-agent:sales"):
        ownership.plan(prod, "prod", tmp_path / "plan")


def test_gap1_old_receipt_requires_ack_for_uncaptured_agent(tmp_path, monkeypatch):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    (prod / "agents" / "Sales.space.json").unlink()
    _step9_receipt(prod, "c1", {"Sales": _released(detached=True, lifecycle="detached")})

    with pytest.raises(ownership.AgentError, match="revive-agent:sales"):
        ownership.check_detach_acks(prod, "prod")


def test_release_plan_records_what_release_will_push_with_the_overwrite_report(tmp_path, monkeypatch, capsys):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    (prod / "agents" / "New.auto.tfvars").write_text('acl_groups = ["a"]\n')
    ownership.write_payload({"version": 2}, agent_files.capture_path(prod, "New"))
    (prod / "agents" / "Old.auto.tfvars").write_text('acl_groups = []\ndetached = true\n')
    live = copy.deepcopy(_payload())
    live["instructions"]["text_instructions"][0]["content"] = ["edited in the prod UI"]
    api = FakeGenie({"prod-1": live})
    plan_dir = tmp_path / "plan"
    plan_dir.mkdir()
    result = ownership.plan(prod, "prod", plan_dir, api)["agents"]
    assert set(api.methods()) == {"GET"}, "release-plan reads, never writes"
    assert result["Sales"]["genie_space_id"] == "prod-1" and result["Sales"]["action"] == "push"
    assert result["Sales"]["report"] == ["instructions.text_instructions[id=01f0ti000000000000000000000001].content"]
    assert result["New"]["genie_space_id"] == "" and result["New"]["live_sha256"] is None
    assert result["New"]["binding"] == "create" and result["Sales"]["binding"] == "existing"
    assert result["Old"] == {"action": "none", "key": "old", "genie_space_id": "", "reason": "detached"}
    assert json.loads((plan_dir / "agents.json").read_text()) == {"agents": result}
    assert "overwrites instructions.text_instructions" in capsys.readouterr().out


def _manifest(tmp_path, prod, api) -> Path:
    from scripts import release_gate as rg

    plan_dir = tmp_path / "plan"
    plan_dir.mkdir(exist_ok=True)
    agents = ownership.plan(prod, "prod", plan_dir, api)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"kind": "release-manifest", "version": rg.VERSION, "env": "prod", "agents": agents}))
    return path


@pytest.fixture
def release_lock(monkeypatch):
    monkeypatch.setattr(ownership, "_lock_owner", lambda env_dir: "release")


def test_release_pushes_exactly_the_manifests_agent_config(tmp_path, monkeypatch, release_lock):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    api = FakeGenie({"prod-1": {"version": 1}})
    manifest = _manifest(tmp_path, prod, api)
    api.calls.clear()
    assert ownership.sync_manifest(prod, "prod", manifest, api)["Sales"]
    # Checked after the saved plans, and again immediately before the PATCH.
    assert api.methods() == ["GET", "GET", "PATCH"] and api.spaces["prod-1"] == _payload()


def test_a_new_agent_gets_the_id_its_saved_plan_created(tmp_path, monkeypatch, release_lock):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    _forget_state(prod)
    api = FakeGenie({})
    manifest = _manifest(tmp_path, prod, api)
    assert [c[1] for c in api.calls] == ["/api/2.0/genie/spaces"], "release-plan only looked for a title clash"
    _created(prod, "sales", "created-1")  # the saved workspace plan's create
    api.spaces["created-1"] = {}
    ownership.sync_manifest(prod, "prod", manifest, api)
    assert api.spaces["created-1"] == _payload()


@pytest.mark.parametrize("drift, message", [
    ("capture", "changed since release-plan"),
    ("live", "changed in the workspace since release-plan"),
    ("id", "nothing is ever pushed to another agent"),
    ("agent", "the agents changed since release-plan"),
])
def test_release_refuses_agent_drift_since_release_plan_and_pushes_nothing(tmp_path, monkeypatch, release_lock,
                                                                         drift, message):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    (prod / "agents" / "Ops.auto.tfvars").write_text('acl_groups = ["a"]\n')
    _created(prod, "ops", "ops-1")
    ownership.write_payload({"version": 2}, agent_files.capture_path(prod, "Ops"))
    api = FakeGenie({"prod-1": {"version": 1}, "ops-1": {"version": 1}})
    manifest = _manifest(tmp_path, prod, api)
    if drift == "capture":
        ownership.write_payload({"version": 3}, agent_files.capture_path(prod, "Sales"))
    elif drift == "live":
        api.spaces["prod-1"] = {"version": 1, "edited": True}
    elif drift == "id":
        (prod / ".genie_space_id_sales").write_text("other-1\n")
    else:
        (prod / "agents" / "Extra.auto.tfvars").write_text('acl_groups = []\n')
    api.calls.clear()
    with pytest.raises(ownership.AgentError, match=message):
        ownership.sync_manifest(prod, "prod", manifest, api)
    assert "PATCH" not in api.methods(), "every agent is checked before any is pushed"


def test_agent_config_is_pushed_only_under_releases_lock(tmp_path, monkeypatch):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    api = FakeGenie({"prod-1": {}})
    manifest = _manifest(tmp_path, prod, api)
    for owner in (None, "maintain", "rehearse", "detach-agent"):
        monkeypatch.setattr(ownership, "_lock_owner", lambda env_dir, owner=owner: owner)
        with pytest.raises(ownership.AgentError, match="only by make release MANIFEST="):
            ownership.sync_manifest(prod, "prod", manifest, api)
    with pytest.raises(ownership.AgentError, match="only from an approved manifest"):
        ownership.sync(prod, "prod", api)
    assert "PATCH" not in api.methods()


def test_a_manifest_without_an_agent_plan_is_refused_when_agents_are_captured(tmp_path, monkeypatch, release_lock):
    from scripts import release_gate as rg

    prod = _deterministic_prod(tmp_path, monkeypatch)
    manifest = tmp_path / "old.json"
    manifest.write_text(json.dumps({"kind": "release-manifest", "version": rg.VERSION, "env": "prod"}))
    with pytest.raises(ownership.AgentError, match="records no agent plan"):
        ownership.sync_manifest(prod, "prod", manifest, FakeGenie())


def test_the_release_targets_carry_agents_through_the_manifest():
    makefile = (SHARED / "Makefile.shared").read_text()
    plan = makefile[makefile.index("\nrelease-plan:"):makefile.index("\n_release-plan-roots:")]
    order = [plan.index(s) for s in ("$(_AGENT_OWNERSHIP) check-deletes", "$(_AGENT_OWNERSHIP) check-detach",
                                     "_ENV_LOCK,release-plan",
                                     "$(_AGENT_OWNERSHIP) restore-ids",
                                     "$(_AGENT_OWNERSHIP) guard-removals", "_release-plan-roots",
                                     "$(_AGENT_OWNERSHIP) plan", "manifest-write")]
    assert order == sorted(order)
    release = makefile[makefile.index("\n_release-manifest:"):makefile.index("\nmaintain:")]
    order = [release.index(s) for s in ("manifest-check", "_ENV_LOCK,release", "$(_AGENT_OWNERSHIP) preflight",
                                        "sync-tags", "apply-saved", "apply-plan",
                                        '$(_AGENT_OWNERSHIP) sync --env-dir "$(ENV_DIR)" --env "$(ENV)" --manifest',
                                        "verify-access ENV", "release-receipt")]
    assert order == sorted(order)
    roots = makefile[makefile.index("\n_release-plan-roots:"):makefile.index("\n_release-manifest:")]
    assert roots.index("guard-removals") < roots.index('workspace "$(ENV)" plan')


def test_make_release_plan_then_release_push_agents_through_the_manifest(tmp_path, monkeypatch):
    """The real recipes (step 9's make harness): agents are planned under release-plan's lock and pushed
    by release MANIFEST= after the saved workspace plan, before verify-access and the receipt."""
    from tests.test_release_gate import Repo
    from tests.test_release_gate_make import Make

    repo = Repo(tmp_path, monkeypatch)
    make = Make(tmp_path, repo)
    agents = make._recorder("agents")
    assert make("rehearse", "dev").returncode == 0
    result = make("release-plan", "prod", stubs={"agent_ownership.py": str(agents)})
    assert result.returncode == 0, result.stdout + result.stderr
    calls = make.calls()
    agent_calls = [c[1] for c in calls if c[0] == "agents"]
    assert agent_calls == ["check-deletes", "check-detach", "restore-ids", "guard-removals", "plan"]
    manifest = Path(result.stdout.split("make release ENV=prod MANIFEST=")[-1].strip())
    assert json.loads(manifest.read_text())["agents"] == {"agents": {}}
    make.reset()
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", stubs={"agent_ownership.py": str(agents)})
    assert result.returncode == 0, result.stdout + result.stderr
    calls = make.calls()
    flat = [c[0] if c[0] in ("agents", "runner") else next((a for a in c if not a.startswith("-") and "=" not in a), "?")
            for c in calls]
    preflight, sync = [c for c in calls if c[0] == "agents"]
    assert preflight[1] == "preflight" and sync[1] == "sync"
    assert sync[sync.index("--manifest") + 1] == str(manifest) == preflight[preflight.index("--manifest") + 1]
    first = flat.index("agents")
    assert first < flat.index("sync-tags") and first < flat.index("staged") and first < flat.index("runner")
    assert flat.index("runner") < len(flat) - flat[::-1].index("agents") - 1 < flat.index("verify-access")


def test_make_release_plan_refuses_revived_tombstone_before_lock_or_plan(tmp_path, monkeypatch):
    from scripts import release_gate as rg
    from tests.test_release_gate import Repo
    from tests.test_release_gate_make import Make, _no_lock

    repo = Repo(tmp_path, monkeypatch)
    repo.commit({
        "aws/envs/prod/agents/Retired.auto.tfvars":
            'genie_space_id = ""\nacl_groups = ["anyone"]\n',
        "aws/envs/prod/ack.txt": "prod:detached-agent:retired\n",
    })
    make = Make(tmp_path, repo)
    calls = make._recorder("agents")
    real = SHARED / "scripts" / "agent_ownership.py"
    wrapper = make._write(
        "agents-wrapper",
        f'#!/bin/sh\nif [ "$1" = check-detach ]; then exec {sys.executable} {real} "$@"; fi\n'
        f'exec {calls} "$@"\n',
    )
    assert make("rehearse", "dev").returncode == 0
    make.reset()

    result = make("release-plan", "prod", stubs={"agent_ownership.py": str(wrapper)})

    assert result.returncode != 0 and "still retired" in result.stderr, result.stdout + result.stderr
    assert _no_lock(repo, "prod")
    assert not rg.manifests_dir(repo.cloud, "prod").exists()
    assert "_release-plan-roots" not in make.names()
    assert not any(call[0] == "runner" and "plan" in call for call in make.calls())


def test_make_release_fails_without_a_receipt_when_the_agent_push_fails(tmp_path, monkeypatch):
    from scripts import release_gate as rg

    repo, make, script, spaces, manifest = _make_release(tmp_path, monkeypatch)
    (spaces.parent / "spaces.json.reject").write_text("benchmarks")  # Genie rejects one field
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", stubs={"agent_ownership.py": str(script)})
    assert result.returncode != 0 and "Genie agent config was not pushed" in result.stderr
    assert "rejected the captured config" in result.stderr
    assert rg.last_release(repo.cloud, "prod") is None
    assert "verify-access" not in make.names()

# ── Review fixes (PR #129): preflight before any mutation, ID identity, delete acks ──

def _created(env_dir: Path, key: str, space_id: str, *, record: str | None = "same", id_file: Path | None = None,
             resource_id: str | None = None) -> None:
    """What a create resource leaves: terraform_data.genie_space[key] and the agent ID it recorded in state
    (data.external.genie_space_id[key]) in the workspace state, and the ID file. Merged into any state."""
    path = _ws_state(env_dir)
    state = json.loads(path.read_text()) if path.is_file() else {"version": 4, "serial": 1, "resources": []}
    named = str(id_file or env_dir / f".genie_space_id_{key}")
    entries = {("managed", "terraform_data"): {"id": resource_id or f"tfdata-{key}", "input": {"id_file": named}}}
    if record is not None:
        entries[("data", "external")] = {"query": {"id_file": named},
                                         "result": {"id": space_id if record == "same" else record, "id_file": named}}
    for (mode, type_), attributes in entries.items():
        name = "genie_space" if type_ == "terraform_data" else "genie_space_id"
        resource = next((r for r in state["resources"] if (r["mode"], r["type"], r["name"]) == (mode, type_, name)),
                        None)
        if resource is None:
            resource = {"module": "module.workspace", "mode": mode, "type": type_, "name": name, "instances": []}
            state["resources"].append(resource)
        resource["instances"] = [i for i in resource["instances"] if i["index_key"] != key]
        resource["instances"].append({"index_key": key, "attributes": attributes})
    state["serial"] += 1
    path.write_text(json.dumps(state))
    (env_dir / f".genie_space_id_{key}").write_text(space_id + "\n")


def _forget_state(env_dir: Path) -> None:
    _ws_state(env_dir).unlink(missing_ok=True)
    (env_dir / ".genie_space_id_sales").unlink(missing_ok=True)


def test_a_promoted_env_never_takes_an_agent_id_from_git(tmp_path, monkeypatch):
    prod = _deterministic_prod(tmp_path, monkeypatch, 'genie_space_id = "prod-1"\nacl_groups = ["a"]\n')
    with pytest.raises(AgentFileError, match="genie_space_id must be empty in prod"):
        agent_files.load_all(prod)
    staging = _env(tmp_path / "s", name="staging", env_cfg='governance_mode = "deterministic"\npromote_from = "dev"\n',
                   Sales='genie_space_id = "x"\nacl_groups = ["a"]\n')
    with pytest.raises(AgentFileError, match="never in git"):
        agent_files.merged_config(staging)
    dev = _env(tmp_path / "d", env_cfg='governance_mode = "deterministic"\n', Sales='genie_space_id = "x"\nacl_groups = ["a"]\n')
    assert agent_files.load(dev, "Sales")["genie_space_id"] == "x"  # dev's agents are named by ID
    legacy = _env(tmp_path / "l", name="prod", Sales='genie_space_id = "x"\nacl_groups = ["a"]\n')
    assert agent_files.load(legacy, "Sales")["genie_space_id"] == "x"


@pytest.mark.parametrize("entry, outcome", [
    ({"key": "sales_2"}, "has key 'sales_2'"),
    ({"lifecycle": "detached", "detached": True}, None),
    ({"lifecycle": "deleted"}, None),
    ({"config_sha256": None}, "records no config"),
    ({"config_sha256": "0" * 64}, "isn't the one release last pushed"),
])
def test_receipt_recovery_is_bound_to_the_released_identity(tmp_path, monkeypatch, entry, outcome):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    (prod / ".genie_space_id_sales").unlink()
    _step9_receipt(prod, "c1", {"Sales": _released(**entry)})
    api = FakeGenie({"prod-1": _payload()})
    if outcome is None:  # released detached or deleted: an agent with the name now is a new one (name reuse)
        assert ownership.restore_ids(prod, "prod", api) == []
    else:
        with pytest.raises(ownership.AgentError, match=outcome):
            ownership.restore_ids(prod, "prod", api)
    assert not (prod / ".genie_space_id_sales").exists()


def test_recovery_refuses_an_id_whose_live_config_drifted_or_is_gone(tmp_path, monkeypatch):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    (prod / ".genie_space_id_sales").unlink()
    _step9_receipt(prod, "c1", {"Sales": _released()})
    with pytest.raises(ownership.AgentError, match="isn't the one release last pushed"):
        ownership.restore_ids(prod, "prod", FakeGenie({"prod-1": {"version": 2, "someone": "else"}}))
    gone = FakeGenie({})
    gone.do = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("404 RESOURCE_DOES_NOT_EXIST"))
    with pytest.raises(ownership.AgentError, match="could not read agent"):
        ownership.restore_ids(prod, "prod", gone)


def _new_agent_manifest(tmp_path, monkeypatch):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    _forget_state(prod)
    api = FakeGenie({})
    return prod, api, _manifest(tmp_path, prod, api)


def test_a_planned_new_agent_refuses_an_id_file_that_appeared_before_release(tmp_path, monkeypatch, release_lock):
    prod, api, manifest = _new_agent_manifest(tmp_path, monkeypatch)
    entry = json.loads(manifest.read_text())["agents"]["agents"]["Sales"]
    assert entry["binding"] == "create" and entry["genie_space_id"] == ""
    (prod / ".genie_space_id_sales").write_text("someone-else\n")  # pre-created between plan and release
    with pytest.raises(ownership.AgentError, match="appeared after release-plan"):
        ownership.preflight(prod, "prod", manifest, api)
    with pytest.raises(ownership.AgentError, match="didn't create"):
        ownership.sync_manifest(prod, "prod", manifest, api)
    assert "PATCH" not in api.methods()


def test_a_planned_new_agent_is_pushed_only_if_the_saved_create_made_it(tmp_path, monkeypatch, release_lock):
    prod, api, manifest = _new_agent_manifest(tmp_path, monkeypatch)
    ownership.preflight(prod, "prod", manifest, api)
    _created(prod, "other", "x")  # a create resource, but not this agent's
    (prod / ".genie_space_id_sales").write_text("created-1\n")
    with pytest.raises(ownership.AgentError, match="didn't create 'Sales'"):
        ownership.sync_manifest(prod, "prod", manifest, api)
    _created(prod, "sales", "created-1")
    ownership.adopted_marker(prod, "Sales").write_text("")  # create title-adopted instead of creating
    with pytest.raises(ownership.AgentError, match="adopted an existing agent"):
        ownership.sync_manifest(prod, "prod", manifest, api)
    assert "PATCH" not in api.methods()


def test_a_planned_new_agent_refuses_a_live_agent_with_its_title(tmp_path, monkeypatch, release_lock):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    _forget_state(prod)
    api = FakeGenie({"old-1": {}}, titles={"old-1": "Sales"})
    with pytest.raises(ownership.AgentError, match="already exists .old-1."):
        ownership.plan(prod, "prod", tmp_path, api)
    api.titles.clear()
    manifest = _manifest(tmp_path, prod, api)
    api.titles["late-1"] = "Sales"  # created between plan and release
    with pytest.raises(ownership.AgentError, match="would adopt it by title"):
        ownership.preflight(prod, "prod", manifest, api)


@pytest.mark.parametrize("drift", ["live", "id", "capture", "agent", "lifecycle"])
def test_preflight_refuses_any_agent_drift_before_anything_is_applied(tmp_path, monkeypatch, release_lock, drift):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    api = FakeGenie({"prod-1": {"version": 1}})
    manifest = _manifest(tmp_path, prod, api)
    if drift == "live":
        api.spaces["prod-1"] = {"version": 1, "edited": True}
    elif drift == "id":
        (prod / ".genie_space_id_sales").write_text("other-1\n")
    elif drift == "capture":
        ownership.write_payload({"version": 3}, agent_files.capture_path(prod, "Sales"))
    elif drift == "agent":
        (prod / "agents" / "Extra.auto.tfvars").write_text('acl_groups = []\n')
    else:
        (prod / "agents" / "Sales.auto.tfvars").write_text('acl_groups = []\ndetached = true\n')
    with pytest.raises(ownership.AgentError, match="since release-plan|another agent|run make release-plan"):
        ownership.preflight(prod, "prod", manifest, api)
    assert set(api.methods()) <= {"GET"}


def test_sync_rechecks_the_live_config_immediately_before_each_patch(tmp_path, monkeypatch, release_lock):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    api = FakeGenie({"prod-1": {"version": 1}})
    manifest = _manifest(tmp_path, prod, api)
    gets = []
    original = api.do

    def do(method, path, query=None, body=None):
        if method == "GET":
            gets.append(path)
            if len(gets) == 2:  # edited after the first re-check, before the PATCH
                api.spaces["prod-1"] = {"version": 1, "edited": True}
        return original(method, path, query, body)

    api.do = do
    with pytest.raises(ownership.AgentError, match="changed in the workspace"):
        ownership.sync_manifest(prod, "prod", manifest, api)
    assert "PATCH" not in api.methods()


def test_the_manifest_and_receipt_record_each_agents_binding(tmp_path, monkeypatch):
    from scripts import release_gate as rg
    from tests.test_release_gate import Repo

    repo = Repo(tmp_path, monkeypatch)
    capture = ownership.render(_payload())
    repo.commit({"aws/envs/prod/agents/Sales.auto.tfvars": 'genie_space_id = ""\nacl_groups = ["a"]\n',
                 "aws/envs/prod/agents/Sales.space.json": capture})
    _created(repo.prod, "sales", "prod-9")
    manifest = tmp_path / "m.json"
    manifest.write_text(json.dumps({
        "kind": "release-manifest", "version": rg.VERSION, "id": "m", "env": "prod", "commit": repo.head(),
        "effective_digest": "e", "live_snapshot": "l",
        "agents": {"agents": {"Sales": {"action": "push", "key": "sales", "binding": "create",
                                        "genie_space_id": "", "capture_sha256": ownership._sha(capture)}}}}))
    released = json.loads(rg.release_receipt(repo.prod, "prod", manifest).read_text())["agents"]["Sales"]
    assert released["binding"] == "create" and released["create_resource_id"] == "tfdata-sales"
    assert released["config_sha256"] == ownership._sha(capture) and released["lifecycle"] == "active"
    assert released["genie_space_id"] == "prod-9" and released["key"] == "sales"


def test_delete_needs_a_committed_ack_recorded_in_the_manifest(tmp_path, monkeypatch, release_lock):
    prod = _deterministic_prod(tmp_path, monkeypatch, 'acl_groups = []\ndelete = true\n')
    api = FakeGenie({"prod-1": {}})
    with pytest.raises(ownership.AgentError, match="add prod:delete-agent:sales"):
        ownership.plan(prod, "prod", tmp_path, api)
    (prod / "ack.txt").write_text("prod:delete-agent:sales\n")
    entry = ownership.plan(prod, "prod", tmp_path, api)["agents"]["Sales"]
    assert entry == {"action": "delete", "key": "sales", "genie_space_id": "prod-1",
                     "ack": "prod:delete-agent:sales", "via": "saved workspace plan"}
    manifest = _manifest(tmp_path, prod, api)
    (prod / "ack.txt").write_text("")
    with pytest.raises(ownership.AgentError, match="needs a reviewed acknowledgement"):
        ownership.preflight(prod, "prod", manifest, api)
    (prod / "ack.txt").write_text("prod:delete-agent:sales\n")
    ownership.sync_manifest(prod, "prod", manifest, api)
    assert api.methods().count("DELETE") == 0, "a promoted env's agent is trashed by the saved plan, not the API"


def test_delete_agent_acks_parse_and_reconcile(released_prod):
    from ack_file import AckFileError, parse_text
    from scripts import release_gate as rg

    assert parse_text("prod:delete-agent:sales_eu\n", "prod")[0].key == "prod:delete-agent:sales_eu"
    for bad in ("prod:delete-agent:Sales EU", "prod:delete-agent:sales:x", "prod:delete-agent:"):
        with pytest.raises(AckFileError):
            parse_text(bad + "\n", "prod")
    (released_prod / "ack.txt").write_text("prod:delete-agent:sales\n")
    assert rg.reports(released_prod, "prod")["unmatched_acknowledgements"] == ["prod:delete-agent:sales"]
    (released_prod / "agents").mkdir()
    (released_prod / "agents" / "Sales.auto.tfvars").write_text('acl_groups = []\ndelete = true\n')
    report = rg.reports(released_prod, "prod")
    assert report["ok"] and report["agent_deletions"] == {
        "prod:delete-agent:sales": "deletes Genie agent Sales and its conversations"}


from tests.test_release_gate import _deploy, _write_generated, released_prod  # noqa: E402,F401

FAKE_AGENT_SCRIPT = r'''#!{python}
"""agent_ownership.py with the Genie API faked from a JSON file (no Databricks)."""
import json, sys
sys.path[:0] = [{shared!r}, {shared!r} + "/scripts"]
from scripts import agent_ownership as o
from tests.test_agent_ownership import FakeGenie
import os
spaces = json.load(open({spaces!r}))
reject = open({spaces!r} + ".reject").read() if os.path.exists({spaces!r} + ".reject") else ""
api = FakeGenie(spaces, reject=reject)
o._api = lambda env_dir: api
with open({log!r}, "a") as log:
    log.write(json.dumps(["agents"] + sys.argv[1:]) + "\n")
rc = o.main(sys.argv[1:])
with open({spaces!r} + ".calls", "a") as calls:
    calls.writelines(json.dumps(c[:2]) + "\n" for c in api.calls)
sys.exit(rc)
'''


def _make_release(tmp_path, monkeypatch, *, new_agent=False):
    import sys as _sys
    from tests.test_release_gate import Repo
    from tests.test_release_gate_make import Make

    repo = Repo(tmp_path, monkeypatch)
    repo.commit({"aws/envs/prod/agents/Sales.auto.tfvars": 'genie_space_id = ""\nacl_groups = ["a"]\n',
                 "aws/envs/prod/agents/Sales.space.json": ownership.render(_payload())})
    if not new_agent:
        _created(repo.prod, "sales", "prod-1")
    make = Make(tmp_path, repo)
    spaces = tmp_path / "spaces.json"
    spaces.write_text(json.dumps({"prod-1": {"version": 1}}))
    script = make._write("agents-fake", FAKE_AGENT_SCRIPT.format(
        python=_sys.executable, shared=str(SHARED), spaces=str(spaces), log=str(make.log)))
    assert make("rehearse", "dev").returncode == 0
    result = make("release-plan", "prod", stubs={"agent_ownership.py": str(script)})
    assert result.returncode == 0, result.stdout + result.stderr
    manifest = Path(result.stdout.split("make release ENV=prod MANIFEST=")[-1].strip())
    make.reset()
    return repo, make, script, spaces, manifest


MUTATING = ("sync-tags", "wait-tag-policies", "staged", "runner")


@pytest.mark.parametrize("drift", ["live", "id", "new-id"])
def test_make_release_refuses_agent_drift_before_any_tag_policy_or_saved_plan(tmp_path, monkeypatch, drift):
    repo, make, script, spaces, manifest = _make_release(tmp_path, monkeypatch, new_agent=drift == "new-id")
    if drift == "live":
        spaces.write_text(json.dumps({"prod-1": {"version": 1, "edited in the prod UI": True}}))
    elif drift == "id":
        (repo.prod / ".genie_space_id_sales").write_text("other-1\n")  # replaced between plan and release
    else:
        (repo.prod / ".genie_space_id_sales").write_text("pre-created-1\n")  # planned as a new agent
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", stubs={"agent_ownership.py": str(script)})
    assert result.returncode != 0 and "nothing was applied" in result.stderr, result.stdout + result.stderr
    names = make.names()
    assert not [n for n in names if n in MUTATING], names
    assert "agents" in names
    assert not (spaces.parent / "spaces.json.calls").exists() or "PATCH" not in (
        spaces.parent / "spaces.json.calls").read_text()
    from scripts import release_gate as rg
    assert rg.last_release(repo.cloud, "prod") is None


def test_make_release_pushes_after_the_saved_plans_when_nothing_drifted(tmp_path, monkeypatch):
    repo, make, script, spaces, manifest = _make_release(tmp_path, monkeypatch)
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", stubs={"agent_ownership.py": str(script)})
    assert result.returncode == 0, result.stdout + result.stderr
    calls = (spaces.parent / "spaces.json.calls").read_text()
    assert '["PATCH", "/api/2.0/genie/spaces/prod-1"]' in calls
    from scripts import release_gate as rg
    assert rg.last_release(repo.cloud, "prod")["agents"]["Sales"]["genie_space_id"] == "prod-1"


def test_make_a_non_final_staged_phase_pushes_no_agent_config(tmp_path, monkeypatch):
    """A staged policy phase is not a completed release: no agent push, no release receipt."""
    repo, make, script, spaces, manifest = _make_release(tmp_path, monkeypatch)
    make.exit_code("staged", 10)
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", stubs={"agent_ownership.py": str(script)})
    assert result.returncode == 0 and "Release phase applied" in result.stdout, result.stdout + result.stderr
    agent_calls = [c[1] for c in make.calls() if c[0] == "agents"]
    assert agent_calls == ["preflight"]
    assert "PATCH" not in (spaces.parent / "spaces.json.calls").read_text()
    from scripts import release_gate as rg
    assert rg.last_release(repo.cloud, "prod") is None


def test_make_release_plan_refuses_an_unacknowledged_delete_before_the_lock(tmp_path, monkeypatch):
    from tests.test_release_gate import Repo
    from tests.test_release_gate_make import Make, _no_lock

    repo = Repo(tmp_path, monkeypatch)
    repo.commit({"aws/envs/prod/agents/Sales.auto.tfvars": 'genie_space_id = ""\nacl_groups = []\ndelete = true\n'})
    make = Make(tmp_path, repo)
    assert make("rehearse", "dev").returncode == 0
    make.reset()
    result = make("release-plan", "prod")
    assert result.returncode != 0 and "prod:delete-agent:sales" in result.stderr
    assert _no_lock(repo, "prod")
    assert "derive-assignments" not in make.names() and "_release-plan-roots" not in make.names()


# ── Re-review fixes (PR #129 at 8574a92) ──────────────────────────────────────

@pytest.mark.parametrize("entry, state, message", [
    # R1: a forged receipt naming a create resource that doesn't exist.
    ({"binding": "create", "create_resource_id": "tfdata-forged"}, "kept", "names create resource tfdata-forged, the state holds tfdata-sales"),
    ({"binding": "create", "create_resource_id": "tfdata-sales"}, "none", "holds no terraform_data.genie_space"),
    # R1: binding=existing with no create resource recorded while state holds one.
    ({"binding": "existing", "create_resource_id": None}, "kept", "records no create resource, but the state holds"),
    ({"binding": "create", "create_resource_id": None}, "kept", "records no create resource. If it is"),
    ({"binding": "mystery"}, "kept", "no known binding"),
    ({"binding": None, "create_resource_id": None}, "kept", "no known binding"),  # an older receipt
    ({}, "elsewhere", "names the ID file"),
    ({}, "other-record", "recorded prod-other in state"),
])
def test_receipt_recovery_enforces_the_recorded_binding_and_create_resource(tmp_path, monkeypatch, entry, state,
                                                                           message):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    if state == "none":
        _ws_state(prod).unlink()
    elif state == "elsewhere":
        _created(prod, "sales", "prod-1", id_file=tmp_path / "attacker" / ".genie_space_id_sales")
    elif state == "other-record":
        _created(prod, "sales", "prod-1", record="prod-other")
    (prod / ".genie_space_id_sales").unlink()
    _step9_receipt(prod, "c1", {"Sales": _released(**entry)})
    api = FakeGenie({"prod-1": _payload()})  # its live config matches: binding alone must refuse
    with pytest.raises(ownership.AgentError, match=message):
        ownership.restore_ids(prod, "prod", api)
    assert not (prod / ".genie_space_id_sales").exists()


def test_receipt_recovery_accepts_a_matching_binding(tmp_path, monkeypatch):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    (prod / ".genie_space_id_sales").unlink()
    _step9_receipt(prod, "c1", {"Sales": _released(binding="create", create_resource_id="tfdata-sales")})
    assert ownership.restore_ids(prod, "prod", FakeGenie({"prod-1": _payload()})) == ["Sales"]


def test_a_created_agent_needs_its_exact_id_file_and_the_id_its_create_recorded(tmp_path, monkeypatch, release_lock):
    """R2: same basename in another directory, a swapped ID file, wrong key, missing record, adoption."""
    cases = {
        "elsewhere": (dict(id_file=tmp_path / "attacker" / ".genie_space_id_sales"), "names the ID file"),
        "swapped": (dict(record="created-real"), "holds created-1, but the create resource recorded created-real"),
        "no-record": (dict(record=None), "recorded no agent ID in state"),
    }
    for case, (kwargs, message) in cases.items():
        prod, api, manifest = _new_agent_manifest(tmp_path / case, monkeypatch)
        ownership.preflight(prod, "prod", manifest, api)
        _created(prod, "sales", "created-1", **kwargs)
        api.spaces["created-1"] = {}
        with pytest.raises(ownership.AgentError, match=message):
            ownership.sync_manifest(prod, "prod", manifest, api)
        assert "PATCH" not in api.methods(), case
    prod, api, manifest = _new_agent_manifest(tmp_path / "key", monkeypatch)
    _created(prod, "sales_2", "created-1", id_file=prod / ".genie_space_id_sales")
    with pytest.raises(ownership.AgentError, match="under key 'sales_2', not 'sales'"):
        ownership.sync_manifest(prod, "prod", manifest, api)
    prod, api, manifest = _new_agent_manifest(tmp_path / "missing", monkeypatch)
    with pytest.raises(ownership.AgentError, match="no create resource terraform_data.genie_space\\['sales'\\]"):
        ownership.sync_manifest(prod, "prod", manifest, api)


def test_an_existing_agent_with_a_swapped_id_file_is_refused_by_its_recorded_id(tmp_path, monkeypatch, release_lock):
    """R2: the ID file swapped to another agent ID is caught against the ID the create recorded in state."""
    prod = _deterministic_prod(tmp_path, monkeypatch)
    api = FakeGenie({"prod-1": {"version": 1}, "prod-2": {"version": 1}})
    manifest = _manifest(tmp_path, prod, api)
    entry = json.loads(manifest.read_text())["agents"]["agents"]["Sales"]
    assert entry["binding"] == "existing" and entry["create_resource_id"] == "tfdata-sales"
    _created(prod, "sales", "prod-1", resource_id="tfdata-replaced")
    with pytest.raises(ownership.AgentError, match="create_resource_id: 'tfdata-sales' -> 'tfdata-replaced'"):
        ownership.preflight(prod, "prod", manifest, api)
    _created(prod, "sales", "prod-1", record="prod-2")  # the file says prod-1, the create recorded prod-2
    with pytest.raises(ownership.AgentError, match="recorded prod-2 in state"):
        ownership.preflight(prod, "prod", manifest, api)
    _created(prod, "sales", "prod-1")
    ownership.preflight(prod, "prod", manifest, api)
    assert "PATCH" not in api.methods()


def test_two_agents_swapping_id_files_are_refused_before_anything(tmp_path, monkeypatch, release_lock):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    (prod / "agents" / "Ops.auto.tfvars").write_text('acl_groups = ["a"]\n')
    ownership.write_payload({"version": 2}, agent_files.capture_path(prod, "Ops"))
    _created(prod, "ops", "ops-1")
    api = FakeGenie({"prod-1": {"version": 1}, "ops-1": {"version": 1}})
    manifest = _manifest(tmp_path, prod, api)
    (prod / ".genie_space_id_sales").write_text("ops-1\n")
    (prod / ".genie_space_id_ops").write_text("prod-1\n")
    with pytest.raises(ownership.AgentError, match="nothing is ever pushed to another agent"):
        ownership.preflight(prod, "prod", manifest, api)
    with pytest.raises(ownership.AgentError, match="nothing is ever pushed to another agent"):
        ownership.sync_manifest(prod, "prod", manifest, api)
    assert "PATCH" not in api.methods()


def test_an_adopted_agent_must_keep_the_planned_id_through_its_create(tmp_path, monkeypatch, release_lock):
    prod = _deterministic_prod(tmp_path, monkeypatch)
    _ws_state(prod).unlink()  # an operator wrote the ID file; no create resource yet
    api = FakeGenie({"prod-1": {"version": 1}})
    manifest = _manifest(tmp_path, prod, api)
    assert json.loads(manifest.read_text())["agents"]["agents"]["Sales"]["binding"] == "adopt"
    ownership.preflight(prod, "prod", manifest, api)
    _created(prod, "sales", "prod-1", record="prod-9")
    with pytest.raises(ownership.AgentError, match="holds prod-1, but the create resource recorded prod-9"):
        ownership.sync_manifest(prod, "prod", manifest, api)
    _created(prod, "sales", "prod-1")
    ownership.sync_manifest(prod, "prod", manifest, api)
    assert api.spaces["prod-1"] == _payload()


def test_the_receipt_refuses_a_created_agent_whose_id_file_moved_after_sync(tmp_path, monkeypatch):
    from scripts import release_gate as rg
    from tests.test_release_gate import Repo

    repo = Repo(tmp_path, monkeypatch)
    capture = ownership.render(_payload())
    repo.commit({"aws/envs/prod/agents/Sales.auto.tfvars": 'genie_space_id = ""\nacl_groups = ["a"]\n',
                 "aws/envs/prod/agents/Sales.space.json": capture})
    _created(repo.prod, "sales", "prod-9")
    (repo.prod / ".genie_space_id_sales").write_text("someone-else\n")
    manifest = tmp_path / "m.json"
    manifest.write_text(json.dumps({
        "kind": "release-manifest", "version": rg.VERSION, "id": "m", "env": "prod", "commit": repo.head(),
        "effective_digest": "e", "live_snapshot": "l",
        "agents": {"agents": {"Sales": {"action": "push", "key": "sales", "binding": "create",
                                        "genie_space_id": "", "capture_sha256": ownership._sha(capture)}}}}))
    with pytest.raises(ownership.AgentError, match="holds someone-else, but the create resource recorded prod-9"):
        rg.release_receipt(repo.prod, "prod", manifest)
    assert rg.last_release(repo.cloud, "prod") is None


def test_the_id_reader_records_an_id_file_and_reads_nothing_else(tmp_path):
    reader = SHARED / "scripts" / "genie_id_record.py"
    (tmp_path / ".genie_space_id_sales").write_text("prod-1\n")
    for path, expected in ((tmp_path / ".genie_space_id_sales", "prod-1"), (tmp_path / "missing", "")):
        out = subprocess.run(["python3", reader], input=json.dumps({"id_file": str(path)}), text=True,
                             capture_output=True, check=True).stdout
        assert json.loads(out) == {"id": expected, "id_file": str(path.resolve())}


def test_deterministic_create_paths_record_the_created_id_in_state():
    module = (SHARED / "modules" / "workspace" / "main.tf").read_text()
    block = module[module.index('data "external" "genie_space_id"'):]
    block = block[:block.index("\n}\n")]
    assert "local.managed_created_spaces" in block and "depends_on = [terraform_data.genie_space]" in block
    assert 'id_file = "${var.genie_id_file_prefix}_${each.key}"' in block
    root = (SHARED / "roots" / "workspace" / "main.tf").read_text()
    assert "record_created_agent_ids         = local.deterministic" in root


def _legacy_release(tmp_path, monkeypatch, *, agent: str, ack: str = ""):
    from tests.test_release_gate import Repo
    from tests.test_release_gate_make import LEGACY_ENV, Make

    repo = Repo(tmp_path, monkeypatch)
    files = {"aws/envs/prod/env.auto.tfvars": LEGACY_ENV, "aws/envs/prod/agents/Old.auto.tfvars": agent}
    if ack:
        files["aws/envs/prod/ack.txt"] = ack
    repo.commit(files)
    make = Make(tmp_path, repo)
    agents = make._recorder("agents-legacy")
    real = SHARED / "scripts" / "agent_ownership.py"
    # check-deletes runs for real; every other agent command is recorded only.
    wrapper = make._write("agents-wrapper", f"#!/bin/sh\nif [ \"$1\" = check-deletes ]; then exec {sys.executable} {real} \"$@\"; fi\n"
                                            f"exec {agents} \"$@\"\n")
    return repo, make, make("release", "prod", stubs={"agent_ownership.py": str(wrapper)})


@pytest.mark.parametrize("agent, ack, message", [
    ('genie_space_id = "old-1"\nacl_groups = []\ndelete = true\n', "", "add prod:delete-agent:old"),
    ('genie_space_id = "old-1"\nacl_groups = []\n', "prod:delete-agent:old\n", "stale line"),
])
def test_legacy_release_refuses_delete_acks_before_the_lock(tmp_path, monkeypatch, agent, ack, message):
    """R3: a missing or stale legacy delete ack takes no lock and applies or pushes nothing."""
    from tests.test_release_gate_make import _no_lock

    repo, make, result = _legacy_release(tmp_path, monkeypatch, agent=agent, ack=ack)
    assert result.returncode != 0 and message in result.stderr, result.stdout + result.stderr
    assert _no_lock(repo, "prod")
    names = make.names()
    assert not {"apply", "derive-assignments", "agents-legacy"} & set(names), names


def test_legacy_release_with_its_delete_ack_proceeds(tmp_path, monkeypatch):
    repo, make, result = _legacy_release(tmp_path, monkeypatch, agent='genie_space_id = "old-1"\nacl_groups = []\n'
                                         'delete = true\n', ack="prod:delete-agent:old\n")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "apply" in make.names()
    assert [c[1] for c in make.calls() if c[0] == "agents-legacy"] == ["restore-ids", "sync", "receipt"]


def test_legacy_delete_rechecks_its_ack_immediately_before_delete(tmp_path):
    prod = _env(tmp_path, name="prod", Old='genie_space_id = "old-1"\nacl_groups = []\ndelete = true\n')
    (prod / "ack.txt").write_text("prod:delete-agent:old\nprod:delete-agent:gone\n")
    api = FakeGenie({"old-1": {}})
    with pytest.raises(ownership.AgentError, match="stale line"):
        ownership.delete_agents(prod, "prod", api)
    assert api.calls == []


# ── PR #160: every script keys an agent by its Terraform key, never its file name ──

def _split(tmp_path: Path, name: str, env_cfg: str, file_names: str = "") -> Path:
    env_dir = tmp_path / "envs" / name
    env_dir.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text(env_cfg)
    agent_files.split(env_dir, file_names)
    return env_dir


def test_a_renamed_split_file_keeps_its_key_for_ids_markers_acks_state_and_receipts(tmp_path, monkeypatch):
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    dev = _split(tmp_path, "dev", 'genie_spaces = [{ genie_space_id = "01ef0c" }]\n', "01ef0c=walkthrough")
    agent = agent_files.load(dev, "walkthrough")
    assert agent["key"] == "01ef0c"
    assert ownership.id_file(dev, "walkthrough").name == ".genie_space_id_01ef0c"
    assert ownership.adopted_marker(dev, "walkthrough").name == ".genie_adopted_01ef0c"
    _created(dev, "01ef0c", "01ef0c")
    assert ownership.binding_now(dev, agent) == ("existing", "tfdata-01ef0c")
    assert ownership.verify_created(dev, "walkthrough", agent["key"], "01ef0c") == "01ef0c"
    receipt = ownership.receipt_agents(dev)["walkthrough"]
    assert (receipt["key"], receipt["create_resource_id"], receipt["created_space_id"]) == (
        "01ef0c", "tfdata-01ef0c", "01ef0c")
    settings = dev / "agents" / "walkthrough.auto.tfvars"
    settings.write_text(settings.read_text() + "delete = true\n")
    assert ownership.delete_consequences(dev, "dev") == {
        "dev:delete-agent:01ef0c": "deletes Genie agent walkthrough and its conversations"}
    assert ownership._planned(agent_files.load(dev, "walkthrough"), dev, "dev")["ack"] == "dev:delete-agent:01ef0c"


def _alike_prod(tmp_path, monkeypatch) -> tuple[Path, dict]:
    """A promoted prod whose two titles sanitize alike, split: 'A b' is a_b, 'a-b' is a_b--1."""
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    prod = _split(tmp_path, "prod", 'governance_mode = "deterministic"\npromote_from = "dev"\n'
                  'genie_spaces = [{ name = "A b", acl_groups = ["a"] }, { name = "a-b", acl_groups = ["a"] }]\n')
    payloads = {"A b": {"version": 1, "who": "A b"}, "a-b": {"version": 1, "who": "a-b"}}
    for name, payload in payloads.items():
        ownership.write_payload(payload, agent_files.capture_path(prod, name))
    _created(prod, "a_b", "prod-first")
    _created(prod, "a_b--1", "prod-second")
    return prod, payloads


def test_titles_that_sanitize_alike_never_share_an_id_file_or_a_push(tmp_path, monkeypatch, release_lock):
    prod, payloads = _alike_prod(tmp_path, monkeypatch)
    assert ownership.agent_id(prod, "A b") == "prod-first"
    assert ownership.agent_id(prod, "a-b") == "prod-second"
    receipt = ownership.receipt_agents(prod)
    assert {n: (e["key"], e["genie_space_id"], e["create_resource_id"]) for n, e in receipt.items()} == {
        "A b": ("a_b", "prod-first", "tfdata-a_b"), "a-b": ("a_b--1", "prod-second", "tfdata-a_b--1")}
    api = FakeGenie({"prod-first": {"version": 0}, "prod-second": {"version": 0}})
    manifest = _manifest(tmp_path, prod, api)
    ownership.sync_manifest(prod, "prod", manifest, api)
    assert api.spaces == {"prod-first": payloads["A b"], "prod-second": payloads["a-b"]}


def test_recovery_of_one_of_two_alike_titles_restores_only_its_own_id_file(tmp_path, monkeypatch):
    prod, payloads = _alike_prod(tmp_path, monkeypatch)
    sha = ownership._sha(ownership.render(payloads["a-b"]))
    _step9_receipt(prod, "c1", {"a-b": _released("prod-second", key="a_b--1", config_sha256=sha,
                                                 create_resource_id="tfdata-a_b--1")})
    (prod / ".genie_space_id_a_b--1").unlink()
    api = FakeGenie({"prod-second": payloads["a-b"], "prod-first": payloads["A b"]})
    assert ownership.restore_ids(prod, "prod", api) == ["a-b"]
    assert (prod / ".genie_space_id_a_b--1").read_text() == "prod-second\n"
    assert (prod / ".genie_space_id_a_b").read_text() == "prod-first\n"


def test_detaching_one_of_two_alike_titles_forgets_only_its_addresses(tmp_path, monkeypatch):
    prod, _ = _alike_prod(tmp_path, monkeypatch)
    lines = [f'module.workspace.terraform_data.genie_space["{k}"]' for k in ("a_b", "a_b--1")]
    assert ownership.agent_addresses(lines, agent_files.load(prod, "a-b")) == [lines[1]]
    assert ownership.agent_addresses(lines, agent_files.load(prod, "A b")) == [lines[0]]


def test_a_new_split_agent_is_checked_against_live_agents_by_its_display_title(tmp_path, monkeypatch):
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    title = "Walk (prod_cat.demo)"
    prod = _split(tmp_path, "prod", 'governance_mode = "deterministic"\npromote_from = "dev"\n'
                  f'genie_spaces = [{{ name = "{title}", acl_groups = ["a"] }}]\n')
    ownership.write_payload(_payload(), agent_files.capture_path(prod, "walk_prod_cat_demo"))
    with pytest.raises(ownership.AgentError, match=re.escape(f"a live agent titled {title!r} already exists")):
        ownership.plan(prod, "prod", tmp_path, FakeGenie({}, titles={"old-1": title}))


# promote-to carries the display name and the target's own key.

def _split_dev(tmp_path, entries: str, *, id_to_name: dict | None = None, file_names: str = "") -> Path:
    dev = _split(tmp_path, "dev", f"genie_spaces = [{entries}]\n", file_names)
    for name in agent_files.names(dev):
        ownership.write_payload(_payload(), agent_files.capture_path(dev, name))
    if id_to_name:
        (dev / "generated").mkdir()
        (dev / "generated" / "abac.auto.tfvars").write_text(
            "genie_space_id_to_name = " + json.dumps(id_to_name).replace(":", " =") + "\n")
    return dev


def _prod_dir(tmp_path, text: str = "") -> Path:
    prod = tmp_path / "envs" / "prod"
    prod.mkdir(parents=True, exist_ok=True)
    (prod / "env.auto.tfvars").write_text(text)
    return prod


def _files(env_dir: Path) -> dict:
    return {p.relative_to(env_dir): p.read_bytes() for p in env_dir.rglob("*") if p.is_file()}


def test_promote_carries_the_remapped_display_name_and_the_targets_key(tmp_path):
    dev = _split_dev(tmp_path, '{ name = "Walk (dev_cat.demo)", genie_space_id = "01ef0d" }')
    prod = _prod_dir(tmp_path)
    assert agent_promote.promote(dev, prod, {"dev_cat": "prod_cat"}) == ["walk_dev_cat_demo"]
    seeded = agent_files.load(prod, "walk_dev_cat_demo")
    assert (seeded["display_name"], seeded["key"]) == ("Walk (prod_cat.demo)", "walk_prod_cat_demo")
    assert agent_files.merged_config(prod)["genie_spaces"][0]["name"] == "Walk (prod_cat.demo)"
    first = _files(prod)
    agent_promote.promote(dev, prod, {"dev_cat": "prod_cat"})
    assert _files(prod) == first, "the next promotion matches the agent it seeded"


def test_promote_titles_an_unnamed_agent_from_its_ids_title_never_its_id(tmp_path):
    dev = _split_dev(tmp_path, '{ genie_space_id = "01ef0c" }', file_names="01ef0c=walkthrough",
                     id_to_name={"01ef0c": "Walk (dev_cat.demo)"})
    prod = _prod_dir(tmp_path)
    agent_promote.promote(dev, prod, {"dev_cat": "prod_cat"})
    seeded = agent_files.load(prod, "walkthrough")
    assert (seeded["display_name"], seeded["key"]) == ("Walk (prod_cat.demo)", "walk_prod_cat_demo")
    assert "01ef0c" not in (prod / "agents" / "walkthrough.auto.tfvars").read_text()


def test_promote_refuses_an_unnamed_agent_it_cannot_title(tmp_path):
    dev = _split_dev(tmp_path, '{ genie_space_id = "01ef0c" }')
    prod = _prod_dir(tmp_path)
    with pytest.raises(AgentFileError, match="add name ="):
        agent_promote.promote(dev, prod, {})
    assert _files(prod) == {Path("env.auto.tfvars"): b""}


def test_promote_refuses_while_the_target_keeps_the_remapped_title_inline(tmp_path):
    dev = _split_dev(tmp_path, '{ name = "Walk (dev_cat.demo)", genie_space_id = "01ef0d" }')
    prod = _prod_dir(tmp_path, 'genie_spaces = [{ name = "Walk (prod_cat.demo)" }]\n')
    before = _files(prod)
    with pytest.raises(AgentFileError, match="Run make split-agent-files ENV=prod"):
        agent_promote.promote(dev, prod, {"dev_cat": "prod_cat"})
    assert _files(prod) == before


def test_promote_matches_a_target_split_on_its_own_and_seeds_no_second_agent(tmp_path, capsys):
    dev = _split_dev(tmp_path, '{ name = "Walk (dev_cat.demo)", genie_space_id = "01ef0d" }')
    prod = _split(tmp_path, "prod", 'genie_spaces = [{ name = "Walk (prod_cat.demo)", acl_groups = ["prod_a"] }]\n')
    assert agent_files.names(prod) == ["walk_prod_cat_demo"]
    agent_promote.promote(dev, prod, {"dev_cat": "prod_cat"})
    assert agent_files.names(prod) == ["walk_prod_cat_demo"]
    assert (prod / "agents" / "walk_prod_cat_demo.space.json").is_file()
    assert agent_files.load(prod, "walk_prod_cat_demo")["acl_groups"] == ["prod_a"]
    out = capsys.readouterr().out
    assert "walk_dev_cat_demo -> walk_prod_cat_demo: promoted" in out and "only in prod" not in out


def test_promote_keeps_two_titles_that_sanitize_alike_as_two_target_agents(tmp_path):
    dev = _split_dev(tmp_path, '{ name = "A b", genie_space_id = "01ef01" }, { name = "a-b", genie_space_id = "01ef02" }')
    prod = _prod_dir(tmp_path)
    agent_promote.promote(dev, prod, {})
    assert {a["name"]: (a["display_name"], a["key"]) for a in agent_files.load_all(prod)} == {
        "A b": ("A b", "a_b"), "a-b": ("a-b", "a_b--2")}
    first = _files(prod)
    agent_promote.promote(dev, prod, {})
    assert _files(prod) == first
