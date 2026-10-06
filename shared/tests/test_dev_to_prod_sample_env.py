from pathlib import Path
import importlib.util
from argparse import Namespace
from unittest.mock import Mock
import json

import pytest


MODULE = Path(__file__).parents[1] / "examples" / "dev_to_prod" / "setup_sample_env.py"
SPEC = importlib.util.spec_from_file_location("setup_sample_env", MODULE)
sample = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(sample)


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
    plain = sample._tfvars("space-1", ["c.s.t"], "wh-1")
    tiers = sample._tfvars("space-1", ["c.s.t"], "wh-1", sample.SAMPLE_GROUPS)

    assert "access_tier_groups" not in plain
    assert f'access_tier_groups = ["{sample.SAMPLE_GROUPS[0]}", ' in tiers


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


def test_a_failed_recorded_delete_keeps_the_unconfirmed_records(teardown_env):
    state_file, install = teardown_env
    first, second = sample.SAMPLE_GROUPS[:2]
    account = _account_with([])
    account.groups.delete.side_effect = [None, RuntimeError("boom")]
    install(account)
    _recorded_state(state_file, [first, second])

    with pytest.raises(RuntimeError, match=second):
        sample.teardown(_teardown_args(), _workspace())

    remaining = next(iter(json.loads(state_file.read_text()).values()))
    assert remaining["groups_created"] == [second]
    assert remaining["group_ids"] == {second: f"recorded-{second}"}


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
