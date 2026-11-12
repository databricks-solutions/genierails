from pathlib import Path
import importlib.util
from argparse import Namespace
from unittest.mock import Mock
import json
import os
import shutil
import subprocess

import pytest


MODULE = Path(__file__).parents[1] / "examples" / "dev_to_prod" / "setup_sample_env.py"
ROOT = Path(__file__).parents[2]
SPEC = importlib.util.spec_from_file_location("setup_sample_env", MODULE)
sample = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(sample)


def _load_sample_copy(tmp_path):
    script = tmp_path / "repo" / "shared/examples/dev_to_prod/setup_sample_env.py"
    script.parent.mkdir(parents=True)
    shutil.copy2(MODULE, script)
    spec = importlib.util.spec_from_file_location(f"setup_sample_env_copy_{id(tmp_path)}", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _sample_args():
    return Namespace(
        catalog="catalog", schema="schema", warehouse_id="warehouse-1", rows=1,
        skip_agent=False, create_groups=False,
    )


def _run_sample_setup(module, monkeypatch, capsys):
    monkeypatch.setattr(module, "_run_sql", lambda *args: None)
    client = Mock()
    client.config.host = "https://workspace"
    client.api_client.do.return_value = {"space_id": "space-1"}

    module.setup(_sample_args(), client)

    return capsys.readouterr().out


def test_existing_space_same_warehouse_is_a_noop():
    sample._reconcile_existing_space(
        {"warehouse_id": "warehouse-1", "tables": ["c.s.t"], "title": "Title"},
        "warehouse-1", ["c.s.t"], "Title",
    )


def test_existing_space_rejects_implicit_warehouse_change():
    import pytest

    with pytest.raises(RuntimeError, match="--teardown"):
        sample._reconcile_existing_space(
            {"warehouse_id": "warehouse-1"}, "warehouse-2", [], "Title"
        )


def test_setup_existing_space_warns_on_drift_without_patch(tmp_path, monkeypatch, capsys):
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(sample, "STATE_FILE", state_file)
    monkeypatch.setattr(sample, "_run_sql", lambda *args: None)
    client = Mock()
    client.config.host = "https://workspace"
    client.api_client.do.return_value = {}
    key = "https://workspace|catalog|schema"
    state_file.write_text(__import__("json").dumps({key: {
        "catalog": "catalog", "schema": "schema", "schema_created": True,
        "space_id": "space-1", "warehouse_id": "warehouse-1",
        "tables": ["catalog.schema.old"], "title": "Old title",
    }}))
    args = Namespace(
        catalog="catalog", schema="schema", warehouse_id="warehouse-1", rows=1,
    )

    sample.setup(args, client)

    assert "Configuration is NOT re-applied" in capsys.readouterr().out
    assert all(call.args[0] != "PATCH" for call in client.api_client.do.call_args_list)


def _account_with(existing):
    account = Mock()
    account.config.account_id = "acct-1"
    account.config.host = "https://accounts.cloud.databricks.com"
    account.groups.list.side_effect = lambda filter: [
        Mock(display_name=name, id=f"id-{name}") for name in existing if f'"{name}"' in filter
    ]
    account.groups.create.side_effect = lambda display_name: Mock(id=f"new-{display_name}")
    return account


def _teardown_state(state_file, entry):
    state_file.write_text(json.dumps({"https://workspace|catalog|schema": {
        "catalog": "catalog", "schema": "schema", **entry,
    }}))


def _workspace():
    client = Mock()
    client.config.host = "https://workspace"
    return client


def test_create_groups_records_only_groups_it_created():
    first, *rest = sample.SAMPLE_GROUPS
    account = _account_with([first])
    state = {}

    sample._ensure_groups(account, state)

    assert state["groups_created"] == rest
    assert state["group_ids"] == {name: f"new-{name}" for name in rest}
    assert state["account_id"] == "acct-1"
    assert state["account_host"] == "https://accounts.cloud.databricks.com"
    assert [c.kwargs["display_name"] for c in account.groups.create.call_args_list] == rest


def test_create_groups_requires_account_id():
    import pytest

    with pytest.raises(RuntimeError, match="--account-id"):
        sample._account_client(Namespace(account_id=None, account_profile=None), Mock())


def test_snippet_includes_access_tier_groups_only_when_created():
    plain = sample._tfvars(["c.s.t"], "wh-1")
    tiers = sample._tfvars(["c.s.t"], "wh-1", sample.SAMPLE_GROUPS)

    assert "access_tier_groups" not in plain
    assert f'access_tier_groups = ["{sample.SAMPLE_GROUPS[0]}", ' in tiers
    # The agent ID lives in the agent's own file, not in env.auto.tfvars.
    assert "genie_spaces" not in plain and "genie_spaces" not in tiers


def test_agent_file_acl_is_created_tiers_else_placeholder():
    plain = sample._agent_tfvars("space-1")
    tiers = sample._agent_tfvars("space-1", sample.SAMPLE_GROUPS)

    assert 'genie_space_id = "space-1"' in plain and 'genie_space_id = "space-1"' in tiers
    assert f'acl_groups     = ["{sample.ACL_PLACEHOLDER}"]' in plain
    assert 'acl_groups     = [' + ", ".join(f'"{g}"' for g in sample.SAMPLE_GROUPS) + "]" in tiers


def test_account_host_follows_workspace_cloud():
    assert sample._account_host("https://adb-1.2.azuredatabricks.net") == "https://accounts.azuredatabricks.net"
    assert sample._account_host("https://dbc-1.cloud.databricks.com") == "https://accounts.cloud.databricks.com"


RECORDED = {"account_id": "acct-1", "account_host": "https://accounts.gcp.databricks.com"}


def _teardown_args(*extra):
    return sample.parser().parse_args(["--catalog", "catalog", "--schema", "schema", "--teardown", *extra])


@pytest.fixture
def teardown_env(tmp_path, monkeypatch):
    """Isolated state file, no ambient account settings, and a fake AccountClient."""
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(sample, "STATE_FILE", state_file)
    for var in ("DATABRICKS_ACCOUNT_ID", "DATABRICKS_ACCOUNT_HOST", "DATABRICKS_ACCOUNT_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    built = []

    def install(account):
        def factory(**kwargs):
            built.append(kwargs)
            if "profile" not in kwargs:
                account.config.account_id = kwargs["account_id"]
                account.config.host = kwargs["host"]
            return account
        monkeypatch.setattr(sample, "AccountClient", factory)
        return built

    return state_file, install


def _recorded_state(state_file, names, **extra):
    _teardown_state(state_file, {
        "space_id": "space-1", "groups_created": list(names),
        "group_ids": {name: f"recorded-{name}" for name in names}, **RECORDED, **extra,
    })


def test_teardown_deletes_recorded_group_ids_in_the_recorded_account(teardown_env):
    """The documented teardown passes no --account-id; a group now holding the name is untouched."""
    state_file, install = teardown_env
    name = sample.SAMPLE_GROUPS[0]
    account = _account_with([name])
    built = install(account)
    _recorded_state(state_file, [name])

    assert sample.teardown(_teardown_args(), _workspace()) is True

    assert built == [{"account_id": "acct-1", "host": "https://accounts.gcp.databricks.com",
                      "product": "genierails-dev-to-prod", "product_version": "1.0"}]
    account.groups.delete.assert_called_once_with(id=f"recorded-{name}")
    account.groups.list.assert_not_called()
    assert not state_file.exists()


@pytest.mark.parametrize(("flags", "env", "expected"), [
    (["--account-id", "acct-OTHER"], {}, "account ID acct-OTHER"),
    ([], {"DATABRICKS_ACCOUNT_ID": "acct-OTHER"}, "account ID acct-OTHER"),
    ([], {"DATABRICKS_ACCOUNT_HOST": "https://accounts.cloud.databricks.com"}, "DATABRICKS_ACCOUNT_HOST"),
])
def test_teardown_refuses_a_conflicting_account_before_removing_anything(
    teardown_env, monkeypatch, flags, env, expected
):
    state_file, install = teardown_env
    for var, value in env.items():
        monkeypatch.setenv(var, value)
    account = _account_with([])
    built = install(account)
    _recorded_state(state_file, [sample.SAMPLE_GROUPS[0]], schema_created=True)
    before = state_file.read_text()
    workspace = _workspace()

    with pytest.raises(RuntimeError, match=expected) as raised:
        sample.teardown(_teardown_args(*flags), workspace)

    assert "acct-1" in str(raised.value) and "Nothing was removed" in str(raised.value)
    assert built == []
    account.groups.delete.assert_not_called()
    workspace.api_client.do.assert_not_called()
    assert state_file.read_text() == before


def test_teardown_refuses_an_account_profile_for_another_account(teardown_env):
    state_file, install = teardown_env
    account = _account_with([])
    account.config.account_id = "acct-OTHER"
    account.config.host = "https://accounts.gcp.databricks.com"
    install(account)
    _recorded_state(state_file, [sample.SAMPLE_GROUPS[0]])
    before = state_file.read_text()

    with pytest.raises(RuntimeError, match="--account-profile other"):
        sample.teardown(_teardown_args("--account-profile", "other"), _workspace())

    account.groups.delete.assert_not_called()
    assert state_file.read_text() == before


def test_not_found_in_the_recorded_account_drops_the_record(teardown_env):
    state_file, install = teardown_env
    account = _account_with([])
    account.groups.delete.side_effect = RuntimeError("RESOURCE_DOES_NOT_EXIST: 404")
    install(account)
    _recorded_state(state_file, [sample.SAMPLE_GROUPS[0]])

    assert sample.teardown(_teardown_args(), _workspace()) is True
    assert not state_file.exists()


def test_a_mid_teardown_group_failure_saves_state_and_reports_incomplete(
    teardown_env, monkeypatch, capsys
):
    state_file, install = teardown_env
    monkeypatch.setattr(sample, "_client", lambda args: _workspace())
    first, second = sample.SAMPLE_GROUPS[:2]
    account = _account_with([])
    account.groups.delete.side_effect = [None, RuntimeError("PERMISSION_DENIED: 403")]
    install(account)
    _recorded_state(state_file, [first, second], schema_created=True, warehouse_id="wh")
    dropped = []
    monkeypatch.setattr(sample, "_run_sql", lambda *args: dropped.append(args))

    assert sample.main(["--catalog", "catalog", "--schema", "schema", "--teardown"]) == 1

    out = capsys.readouterr().out
    assert "Teardown INCOMPLETE" in out
    assert f"account group {second}" in out and "schema catalog.schema" in out
    assert "Stopped because: could not remove account group" in out and "403" in out
    assert dropped == []  # stopped at the failed step
    remaining = next(iter(json.loads(state_file.read_text()).values()))
    assert remaining["space_id"] == ""
    assert remaining["groups_created"] == [second]
    assert remaining["group_ids"] == {second: f"recorded-{second}"}
    assert remaining["schema_created"] is True


def test_preflight_refuses_when_matching_config_cannot_read_the_account(teardown_env):
    """config matches the recorded account, but the credentials cannot act on it."""
    state_file, install = teardown_env
    account = _account_with([])
    account.groups.get.side_effect = RuntimeError("PERMISSION_DENIED: not an account admin")
    install(account)
    _recorded_state(state_file, [sample.SAMPLE_GROUPS[0]], schema_created=True, warehouse_id="wh")
    before = state_file.read_text()
    workspace = _workspace()

    with pytest.raises(RuntimeError, match="Nothing was removed") as raised:
        sample.teardown(_teardown_args(), workspace)

    assert "not an account admin" in str(raised.value) and "acct-1" in str(raised.value)
    workspace.api_client.do.assert_not_called()
    account.groups.delete.assert_not_called()
    assert state_file.read_text() == before


def test_preflight_accepts_an_already_deleted_recorded_group(teardown_env):
    state_file, install = teardown_env
    account = _account_with([])
    account.groups.get.side_effect = RuntimeError("RESOURCE_DOES_NOT_EXIST: 404")
    account.groups.delete.side_effect = RuntimeError("RESOURCE_DOES_NOT_EXIST: 404")
    install(account)
    _recorded_state(state_file, [sample.SAMPLE_GROUPS[0]])

    assert sample.teardown(_teardown_args(), _workspace()) is True
    assert not state_file.exists()


def _legacy_state(state_file, names):
    """What the first --create-groups release wrote (and the live sample run's state)."""
    _teardown_state(state_file, {"space_id": "space-1", "schema_created": True,
                                 "warehouse_id": "wh", "groups_created": list(names)})


def test_legacy_names_are_never_deleted_without_the_opt_in(teardown_env, monkeypatch, capsys):
    state_file, install = teardown_env
    monkeypatch.setenv("DATABRICKS_ACCOUNT_ID", "acct-1")  # an account ID alone is not consent
    monkeypatch.setattr(sample, "_run_sql", lambda *args: None)
    monkeypatch.setattr(sample, "_client", lambda args: _workspace())
    account = _account_with(list(sample.SAMPLE_GROUPS))
    built = install(account)
    _legacy_state(state_file, sample.SAMPLE_GROUPS)

    assert sample.main(["--catalog", "catalog", "--schema", "schema", "--teardown"]) == 1

    out = capsys.readouterr().out
    assert "Teardown INCOMPLETE" in out
    assert "known by name only: " + ", ".join(sample.SAMPLE_GROUPS) in out
    assert "--delete-legacy-groups-by-name" in out and "Account Console" in out
    assert built == []
    account.groups.delete.assert_not_called()
    remaining = next(iter(json.loads(state_file.read_text()).values()))
    assert remaining["groups_created"] == list(sample.SAMPLE_GROUPS)
    assert remaining["space_id"] == "" and remaining["schema_created"] is False


def test_legacy_opt_in_warns_then_deletes_only_unambiguous_matches(teardown_env, monkeypatch, capsys):
    state_file, install = teardown_env
    monkeypatch.setattr(sample, "_run_sql", lambda *args: None)
    present, missing = sample.SAMPLE_GROUPS[:2]
    account = _account_with([present])
    install(account)
    _legacy_state(state_file, [present, missing])

    complete = sample.teardown(
        _teardown_args("--delete-legacy-groups-by-name", "--account-id", "acct-1"), _workspace()
    )

    out = capsys.readouterr().out
    assert out.index("WARNING: --delete-legacy-groups-by-name") < out.index(f"Removing legacy account group {present}")
    account.groups.delete.assert_called_once_with(id=f"id-{present}")
    assert complete is False  # the missing one could not be confirmed, so its record is kept
    remaining = next(iter(json.loads(state_file.read_text()).values()))
    assert remaining["groups_created"] == [missing]


def test_legacy_opt_in_still_needs_an_account_id(teardown_env):
    state_file, install = teardown_env
    install(_account_with([]))
    _legacy_state(state_file, [sample.SAMPLE_GROUPS[0]])
    before = state_file.read_text()

    with pytest.raises(RuntimeError, match="--account-id"):
        sample.teardown(_teardown_args("--delete-legacy-groups-by-name"), _workspace())
    assert state_file.read_text() == before


def test_skip_agent_seeds_tables_without_creating_a_genie_agent(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sample, "STATE_FILE", tmp_path / "state.json")
    statements = []
    monkeypatch.setattr(sample, "_run_sql", lambda client, wh, sql: statements.append(sql))
    client = Mock()
    client.config.host = "https://prod"
    args = Namespace(catalog="prod_cat", schema="schema", warehouse_id="wh", rows=1, skip_agent=True)

    sample.setup(args, client)

    assert any(sql.startswith("CREATE TABLE IF NOT EXISTS") for sql in statements)
    client.api_client.do.assert_not_called()
    assert "Skipping the Genie agent" in capsys.readouterr().out


def test_state_file_is_written_at_the_repo_root_owner_only(tmp_path, monkeypatch):
    state_file = tmp_path / "repo" / ".dev_to_prod_sample_env.json"
    monkeypatch.setattr(sample, "STATE_FILE", state_file)
    monkeypatch.setattr(sample, "LEGACY_STATE_FILE", tmp_path / "repo" / "shared/examples/dev_to_prod/.dev_to_prod_sample_env.json")

    sample._save_states({"key": {"schema": "schema"}})

    assert state_file.exists()
    assert state_file.stat().st_mode & 0o777 == 0o600
    assert json.loads(state_file.read_text()) == {"key": {"schema": "schema"}}


def test_legacy_state_file_is_migrated_to_the_repo_root(tmp_path, monkeypatch):
    state_file = tmp_path / "repo" / ".dev_to_prod_sample_env.json"
    legacy = tmp_path / "repo" / "shared/examples/dev_to_prod/.dev_to_prod_sample_env.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_text(json.dumps({"old": {"schema": "old"}}))
    monkeypatch.setattr(sample, "STATE_FILE", state_file)
    monkeypatch.setattr(sample, "LEGACY_STATE_FILE", legacy)

    assert sample._load_states() == {"old": {"schema": "old"}}

    assert not legacy.exists()
    assert state_file.exists()
    assert state_file.stat().st_mode & 0o777 == 0o600


def test_legacy_migration_refuses_conflicting_shared_keys_without_writing(tmp_path, monkeypatch):
    state_file = tmp_path / "repo" / ".dev_to_prod_sample_env.json"
    legacy = tmp_path / "repo" / "shared/examples/dev_to_prod/.dev_to_prod_sample_env.json"
    legacy.parent.mkdir(parents=True)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(json.dumps({"same": {"schema": "s"}, "old": {"groups_created": ["g1"]}}))
    state_file.write_text(json.dumps({"same": {"schema": "DIFFERENT"}, "root": {"schema_created": True}}))
    before_root = state_file.read_text()
    before_legacy = legacy.read_text()
    monkeypatch.setattr(sample, "STATE_FILE", state_file)
    monkeypatch.setattr(sample, "LEGACY_STATE_FILE", legacy)

    with pytest.raises(RuntimeError, match="disagree.*same") as raised:
        sample._load_states()

    message = str(raised.value)
    assert str(legacy) in message and str(state_file) in message
    assert state_file.read_text() == before_root
    assert legacy.read_text() == before_legacy


def test_state_file_refuses_root_symlink_without_touching_target(tmp_path, monkeypatch):
    state_file = tmp_path / "repo" / ".dev_to_prod_sample_env.json"
    target = tmp_path / "precious.json"
    target.write_text("PRECIOUS")
    state_file.parent.mkdir(parents=True)
    state_file.symlink_to(target)
    monkeypatch.setattr(sample, "STATE_FILE", state_file)
    monkeypatch.setattr(sample, "LEGACY_STATE_FILE", tmp_path / "repo" / "shared/examples/dev_to_prod/.dev_to_prod_sample_env.json")

    with pytest.raises(RuntimeError, match="symlinked ownership state"):
        sample._save_states({"key": {"schema": "schema"}})

    assert target.read_text() == "PRECIOUS"
    assert state_file.is_symlink()


def test_legacy_state_file_refuses_symlink_without_migrating(tmp_path, monkeypatch):
    state_file = tmp_path / "repo" / ".dev_to_prod_sample_env.json"
    legacy = tmp_path / "repo" / "shared/examples/dev_to_prod/.dev_to_prod_sample_env.json"
    target = tmp_path / "legacy-target.json"
    target.write_text(json.dumps({"old": {"schema": "old"}}))
    legacy.parent.mkdir(parents=True)
    legacy.symlink_to(target)
    monkeypatch.setattr(sample, "STATE_FILE", state_file)
    monkeypatch.setattr(sample, "LEGACY_STATE_FILE", legacy)

    with pytest.raises(RuntimeError, match="symlinked ownership state"):
        sample._load_states()

    assert legacy.is_symlink()
    assert not state_file.exists()
    assert json.loads(target.read_text()) == {"old": {"schema": "old"}}


def test_default_state_file_writes_repo_root_not_shared(tmp_path):
    copied = _load_sample_copy(tmp_path)
    repo = tmp_path / "repo"

    copied._save_states({"key": {"schema": "schema"}})

    assert copied.STATE_FILE == repo / ".dev_to_prod_sample_env.json"
    assert copied.STATE_FILE.exists()
    assert copied.STATE_FILE.stat().st_mode & 0o777 == 0o600
    assert not (repo / "shared/examples/dev_to_prod/.dev_to_prod_sample_env.json").exists()


@pytest.mark.parametrize("cloud", ["aws", "azure"])
@pytest.mark.parametrize("placeholder_exists", [False, True])
def test_setup_reminds_to_delete_seeded_agent_placeholder_only_when_present(
    tmp_path, monkeypatch, capsys, cloud, placeholder_exists
):
    copied = _load_sample_copy(tmp_path)
    placeholder = tmp_path / f"repo/{cloud}/envs/dev/agents/my_agent.auto.tfvars"
    if placeholder_exists:
        placeholder.parent.mkdir(parents=True)
        placeholder.write_text("# seeded placeholder\n")

    out = _run_sample_setup(copied, monkeypatch, capsys)

    reminder = f"Delete {cloud}/envs/dev/agents/my_agent.auto.tfvars if setup seeded that placeholder file."
    assert out.count(reminder) == (1 if placeholder_exists else 0)


def test_setup_ignores_placeholder_at_repo_root_envs(tmp_path, monkeypatch, capsys):
    copied = _load_sample_copy(tmp_path)
    wrong = tmp_path / "repo/envs/dev/agents/my_agent.auto.tfvars"
    wrong.parent.mkdir(parents=True)
    wrong.write_text("# wrong place\n")

    out = _run_sample_setup(copied, monkeypatch, capsys)

    assert "Delete " not in out


def test_setup_reminder_detects_placeholder_written_by_real_make_setup(tmp_path, monkeypatch, capsys):
    repo = tmp_path / "repo"
    aws = repo / "aws"
    aws.mkdir(parents=True)
    shutil.copy2(ROOT / "aws" / "Makefile", aws / "Makefile")
    env = {k: v for k, v in os.environ.items()
           if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "_SEED_WALKTHROUGH")}

    result = subprocess.run(
        ["make", "--no-print-directory", "setup", "ENV=dev", "COMMIT=0", "GITIGNORE=off",
         f"SHARED_ROOT={ROOT / 'shared'}"],
        cwd=aws, text=True, capture_output=True, env=env, stdin=subprocess.DEVNULL,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert (aws / "envs/dev/agents/my_agent.auto.tfvars").is_file()
    copied = _load_sample_copy(tmp_path)

    out = _run_sample_setup(copied, monkeypatch, capsys)

    reminder = "Delete aws/envs/dev/agents/my_agent.auto.tfvars if setup seeded that placeholder file."
    assert out.count(reminder) == 1


def test_legacy_opt_in_preflight_failure_removes_nothing(teardown_env, monkeypatch):
    state_file, install = teardown_env
    account = _account_with([])
    account.groups.list.side_effect = RuntimeError("401 Unauthorized")
    install(account)
    _legacy_state(state_file, [sample.SAMPLE_GROUPS[0]])
    before = state_file.read_text()
    workspace = _workspace()

    with pytest.raises(RuntimeError, match="Nothing was removed"):
        sample.teardown(_teardown_args("--delete-legacy-groups-by-name", "--account-id", "acct-1"), workspace)

    workspace.api_client.do.assert_not_called()
    assert state_file.read_text() == before


def test_legacy_opt_in_delete_failure_reports_incomplete(teardown_env, monkeypatch, capsys):
    state_file, install = teardown_env
    monkeypatch.setattr(sample, "_run_sql", lambda *args: None)
    name = sample.SAMPLE_GROUPS[0]
    account = _account_with([name])
    account.groups.delete.side_effect = RuntimeError("PERMISSION_DENIED: 403")
    install(account)
    _legacy_state(state_file, [name])

    complete = sample.teardown(
        _teardown_args("--delete-legacy-groups-by-name", "--account-id", "acct-1"), _workspace()
    )

    out = capsys.readouterr().out
    assert complete is False
    assert "Teardown INCOMPLETE" in out and f"account group {name}" in out
    assert "Stopped because:" in out and "403" in out
    remaining = next(iter(json.loads(state_file.read_text()).values()))
    assert remaining["groups_created"] == [name]
    assert remaining["schema_created"] is True  # stopped before the schema step


def test_untracked_existing_schema_names_the_per_clone_state_file_to_copy(tmp_path, monkeypatch):
    """A fresh clone has no ownership state, so a schema an earlier clone created is refused."""
    state_file = tmp_path / "repo" / ".dev_to_prod_sample_env.json"
    monkeypatch.setattr(sample, "STATE_FILE", state_file)
    monkeypatch.setattr(sample, "LEGACY_STATE_FILE", tmp_path / "repo/shared/examples/dev_to_prod/.dev_to_prod_sample_env.json")

    def run_sql(client, warehouse_id, sql):
        raise RuntimeError("[SCHEMA_ALREADY_EXISTS] Cannot create schema because it already exists")

    monkeypatch.setattr(sample, "_run_sql", run_sql)
    client = Mock()
    client.config.host = "https://workspace"
    args = Namespace(catalog="cat", schema="dev_to_prod_demo", warehouse_id="wh", rows=1, skip_agent=True)

    with pytest.raises(RuntimeError) as raised:
        sample.setup(args, client)

    message = str(raised.value)
    assert "cat.dev_to_prod_demo already exists but is not tracked as owned by this script" in message
    assert f"recorded per clone in the git-ignored {state_file}" in message
    assert f"copy that clone's .dev_to_prod_sample_env.json to this clone's {state_file}" in message
    assert "otherwise choose another --schema" in message
    assert not state_file.exists()


# Agent-name uniqueness (aws-6/azure-4): a prod create adopts the one existing
# Genie agent whose title matches exactly, so a fixed sample name let a second
# repo on a shared prod workspace take over the first repo's prod agent.
def test_sample_agent_name_is_derived_from_the_dev_catalog():
    import sys
    sys.path.insert(0, str(ROOT / "shared"))
    from agent_files import check_name

    assert not hasattr(sample, "SAMPLE_AGENT")
    assert sample.sample_agent_name("dev_finance") == "Dev-to-Prod Walkthrough dev_finance"
    assert sample.sample_agent_name("louis_dev") != sample.sample_agent_name("other_dev")
    # Any UC catalog name still gives a valid agent file name.
    for catalog in ("dev_finance", "my-cat", "cat with space", "weird`cat", "a..b", "Cat.ü"):
        assert check_name(sample.sample_agent_name(catalog)).startswith("Dev-to-Prod Walkthrough ")


def test_setup_names_the_agent_file_after_the_catalog_and_warns_about_title_takeover(
    tmp_path, monkeypatch, capsys
):
    copied = _load_sample_copy(tmp_path)

    out = _run_sample_setup(copied, monkeypatch, capsys)

    assert "envs/dev/agents/Dev-to-Prod Walkthrough catalog.auto.tfvars" in out
    assert "envs/dev/agents/Dev-to-Prod Walkthrough.auto.tfvars" not in out
    assert out.count(sample.TITLE_WARNING) == 1
    warning = " ".join(sample.TITLE_WARNING.split())
    for fact in ("its title in prod", "unique in the prod workspace", "exactly this title",
                 "make release ENV=prod takes that agent over instead of creating a new one",
                 "Pick another name"):
        assert fact in warning, fact
    # The warning follows the agent file it is about.
    assert out.index("genie_space_id = ") < out.index(sample.TITLE_WARNING)


def test_title_warning_matches_how_prod_adopts_by_title():
    # The warning describes genie_space.sh's create: exactly one exact-title
    # match is adopted; several matches refuse.
    script = (ROOT / "shared/scripts/genie_space.sh").read_text()
    assert 'if space_title == os.environ["TITLE_TO_MATCH"]:' in script
    assert 'Adopted existing Genie agent ${matching_ids[0]} titled' in script
    assert "Multiple Genie agents have the exact title" in script
    # The default title is the agent's name.
    module = (ROOT / "shared/modules/workspace/main.tf").read_text()
    assert 'GENIE_TITLE              = each.value.config.title != "" ? each.value.config.title : each.value.name' in module
