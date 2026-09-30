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
