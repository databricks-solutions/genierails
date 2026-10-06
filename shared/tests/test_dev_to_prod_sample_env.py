from pathlib import Path
import importlib.util
from argparse import Namespace
from unittest.mock import Mock
import json


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


def test_teardown_deletes_recorded_group_ids_without_account_id_flag(tmp_path, monkeypatch):
    """The documented teardown command passes no --account-id."""
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(sample, "STATE_FILE", state_file)
    monkeypatch.delenv("DATABRICKS_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_ACCOUNT_HOST", raising=False)
    name = sample.SAMPLE_GROUPS[0]
    # A different group now holds the name: it must not be touched.
    account = _account_with([name])
    seen = {}
    monkeypatch.setattr(sample, "AccountClient", lambda **kw: seen.update(kw) or account)
    _teardown_state(state_file, {
        "groups_created": [name], "group_ids": {name: "recorded-id"},
        "account_id": "acct-1", "account_host": "https://accounts.gcp.databricks.com",
    })
    args = sample.parser().parse_args(["--catalog", "catalog", "--schema", "schema", "--teardown"])

    sample.teardown(args, _workspace())

    assert seen["account_id"] == "acct-1"
    assert seen["host"] == "https://accounts.gcp.databricks.com"
    account.groups.delete.assert_called_once_with(id="recorded-id")
    account.groups.list.assert_not_called()
    assert not state_file.exists()


def test_teardown_of_legacy_names_only_state_looks_up_only_created_groups(tmp_path, monkeypatch):
    """State from the first --create-groups release records names but no IDs."""
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(sample, "STATE_FILE", state_file)
    created, reused = sample.SAMPLE_GROUPS[0], sample.SAMPLE_GROUPS[1]
    account = _account_with([created, reused])
    monkeypatch.setattr(sample, "_account_client", lambda args, client, state: account)
    _teardown_state(state_file, {"groups_created": [created]})

    sample.teardown(Namespace(catalog="catalog", schema="schema", warehouse_id=None,
                              account_id="acct-1", account_profile=None), _workspace())

    account.groups.delete.assert_called_once_with(id=f"id-{created}")
    assert not state_file.exists()


def test_teardown_of_legacy_state_without_any_account_id_fails_and_keeps_state(tmp_path, monkeypatch):
    import pytest

    state_file = tmp_path / "state.json"
    monkeypatch.setattr(sample, "STATE_FILE", state_file)
    monkeypatch.setattr(sample, "AccountClient", Mock())
    _teardown_state(state_file, {"groups_created": [sample.SAMPLE_GROUPS[0]]})

    with pytest.raises(RuntimeError, match="--account-id"):
        sample.teardown(Namespace(catalog="catalog", schema="schema", warehouse_id=None,
                                  account_id=None, account_profile=None), _workspace())
    assert json.loads(state_file.read_text())  # nothing forgotten


def test_teardown_records_progress_so_a_failed_delete_can_be_retried(tmp_path, monkeypatch):
    import pytest

    state_file = tmp_path / "state.json"
    monkeypatch.setattr(sample, "STATE_FILE", state_file)
    first, second = sample.SAMPLE_GROUPS[:2]
    account = _account_with([])
    account.groups.delete.side_effect = [None, RuntimeError("boom")]
    monkeypatch.setattr(sample, "_account_client", lambda args, client, state: account)
    _teardown_state(state_file, {
        "groups_created": [first, second], "group_ids": {first: "id-1", second: "id-2"},
        "account_id": "acct-1",
    })

    with pytest.raises(RuntimeError, match=second):
        sample.teardown(Namespace(catalog="catalog", schema="schema", warehouse_id=None,
                                  account_id=None, account_profile=None), _workspace())

    remaining = next(iter(json.loads(state_file.read_text()).values()))
    assert remaining["groups_created"] == [second]
    assert remaining["group_ids"] == {second: "id-2"}


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
