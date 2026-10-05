from pathlib import Path
import importlib.util
from argparse import Namespace
from unittest.mock import Mock


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
    account.groups.list.side_effect = lambda filter: [
        Mock(display_name=name, id=f"id-{name}") for name in existing if f'"{name}"' in filter
    ]
    return account


def test_create_groups_records_only_groups_it_created():
    first, *rest = sample.SAMPLE_GROUPS
    account = _account_with([first])
    state = {}

    sample._ensure_groups(account, state)

    assert state["groups_created"] == rest
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


def test_teardown_removes_tracked_groups(tmp_path, monkeypatch):
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(sample, "STATE_FILE", state_file)
    name = sample.SAMPLE_GROUPS[0]
    account = _account_with([name])
    monkeypatch.setattr(sample, "_account_client", lambda args, client: account)
    client = Mock()
    client.config.host = "https://workspace"
    state_file.write_text(__import__("json").dumps({"https://workspace|catalog|schema": {
        "catalog": "catalog", "schema": "schema", "groups_created": [name],
    }}))

    sample.teardown(Namespace(catalog="catalog", schema="schema", warehouse_id=None), client)

    account.groups.delete.assert_called_once_with(id=f"id-{name}")
    assert not state_file.exists()
