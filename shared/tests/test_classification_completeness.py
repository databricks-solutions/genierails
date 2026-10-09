import json
import sys
import time
from pathlib import Path

import pytest
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import classification_completeness as cc


def test_manifest_write_remaps_catalog_and_keeps_class_tags(tmp_path):
    source = tmp_path / "abac.auto.tfvars"
    source.write_text('''tag_assignments = [
      { entity_type = "columns", entity_name = "dev.s.t.email", tag_key = "class.email_address", tag_value = "true" },
      { entity_type = "columns", entity_name = "dev.s.t.email", tag_key = "gr_treatment", tag_value = "email" },
    ]\n''')
    destination = tmp_path / "generated/expected_classification.json"
    cc.write_manifest(source, destination, "dev=prod")
    assert json.loads(destination.read_text()) == {
        "prod.s.t.email": ["class.email_address", "gr_treatment:email"]}


def test_compare_and_ack_parsing_are_case_insensitive_and_trimmed():
    expected = {"prod.s.t.email": ["class.email_address"], "prod.s.t.ssn": ["class.us_ssn"]}
    assert cc.missing_classification(expected, ["PROD.s.t.email"]) == ["prod.s.t.ssn"]
    ack = cc.parse_ack(" prod.s.t.ssn, ,prod.s.t.phone ")
    assert cc.missing_classification(expected, ["prod.s.t.email"], ack) == []


def test_legacy_warns_but_deterministic_blocks(tmp_path, monkeypatch, capsys):
    env = tmp_path / "prod"; (env / "generated").mkdir(parents=True)
    (env / "generated/expected_classification.json").write_text('{"prod.s.t.email": ["class.email_address"]}\n')
    monkeypatch.setattr(cc, "live_classified_columns", lambda _env, _timeout: set())
    assert cc.main(["check", "--env-dir", str(env), "--mode", "legacy"]) == 0
    assert "WARNING" in capsys.readouterr().err
    assert cc.main(["check", "--env-dir", str(env), "--mode", "deterministic"]) == 1
    assert "ERROR" in capsys.readouterr().err


def test_empty_manifest_skips_live_query_in_both_modes(tmp_path, monkeypatch, capsys):
    env = tmp_path / "prod"
    (env / "generated").mkdir(parents=True)
    (env / "generated/expected_classification.json").write_text("{}\n")
    monkeypatch.setattr(cc, "live_classified_columns", lambda *_args: (_ for _ in ()).throw(AssertionError()))
    assert cc.main(["check", "--env-dir", str(env), "--mode", "legacy"]) == 0
    assert cc.main(["check", "--env-dir", str(env), "--mode", "deterministic"]) == 0
    assert "deterministic classification manifest is empty" in capsys.readouterr().out


def test_missing_manifest_blocks_only_deterministic(tmp_path, capsys):
    env = tmp_path / "prod"
    env.mkdir()
    assert cc.main(["check", "--env-dir", str(env), "--mode", "legacy"]) == 0
    assert cc.main(["check", "--env-dir", str(env), "--mode", "deterministic"]) == 1
    assert "requires generated/expected_classification.json" in capsys.readouterr().err


def test_live_query_error_is_warn_only_for_legacy(tmp_path, monkeypatch, capsys):
    env = tmp_path / "prod"
    (env / "generated").mkdir(parents=True)
    (env / "generated/expected_classification.json").write_text('{"prod.s.t.email": ["x"]}\n')
    monkeypatch.setattr(cc, "live_classified_columns", lambda *_args: (_ for _ in ()).throw(TimeoutError("deadline")))
    assert cc.main(["check", "--env-dir", str(env), "--mode", "legacy"]) == 0
    assert "WARNING" in capsys.readouterr().err
    assert cc.main(["check", "--env-dir", str(env), "--mode", "deterministic"]) == 1
    assert "ERROR" in capsys.readouterr().err


def test_live_check_has_a_hard_wall_timeout(tmp_path, monkeypatch):
    monkeypatch.setattr(
        cc, "_live_classified_columns_query",
        lambda *_args: time.sleep(1),
    )
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="hard timeout"):
        cc.live_classified_columns(tmp_path, 0.01)
    assert time.monotonic() - started < 0.5


def test_result_paging_deadline_raises_timeout_not_name_error(tmp_path, monkeypatch):
    monkeypatch.setattr(cc, "_load_hcl", lambda path: {
        "databricks_workspace_host": "h", "databricks_client_id": "c",
        "databricks_client_secret": "s", "sql_warehouse_id": "w"})
    succeeded = "SUCCEEDED"
    statement = SimpleNamespace(
        status=SimpleNamespace(state=succeeded),
        result=SimpleNamespace(data_array=[]),
        manifest=SimpleNamespace(total_chunk_count=2), statement_id="id")
    execution = SimpleNamespace(execute_statement=lambda **kwargs: statement)
    monkeypatch.setitem(sys.modules, "databricks.sdk", SimpleNamespace(
        WorkspaceClient=lambda **kwargs: SimpleNamespace(statement_execution=execution)))
    monkeypatch.setitem(sys.modules, "databricks.sdk.service.sql", SimpleNamespace(
        StatementState=SimpleNamespace(SUCCEEDED=succeeded, FAILED="f", CANCELED="c", CLOSED="x")))
    with pytest.raises(TimeoutError, match="paging exceeded its deadline"):
        cc._live_classified_columns_query(tmp_path, time.monotonic() - 1)
