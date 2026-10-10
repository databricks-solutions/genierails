import json
from pathlib import Path

from scripts.sticky_governance import load_manifest, main, merge_assignments


def _env(tmp_path: Path, mode="deterministic") -> Path:
    (tmp_path / "generated").mkdir()
    (tmp_path / "env.auto.tfvars").write_text(f'governance_mode = "{mode}"\n')
    return tmp_path


def _tag(column="cat.sch.orders.email", value="email_partial"):
    return {"entity_type": "columns", "entity_name": column,
            "tag_key": "gr_treatment", "tag_value": value}


def test_treatment_changes_update_one_persisted_column_key(tmp_path):
    env = _env(tmp_path)
    assert merge_assignments(env, [_tag(value="email_partial")])[-1]["tag_value"] == "email_partial"
    result = merge_assignments(env, [_tag(value="redact")])
    assert result == [_tag(value="redact")]
    assert load_manifest(env) == {"cat.sch.orders": {"cat.sch.orders.email": "redact"}}


def test_dropping_agent_table_keeps_sticky_assignments(tmp_path):
    env = _env(tmp_path)
    merge_assignments(env, [_tag()])
    assert merge_assignments(env, []) == [_tag()]


def test_legacy_environment_is_a_zero_change_passthrough(tmp_path):
    env = _env(tmp_path, "legacy"); assignments = [_tag()]
    assert merge_assignments(env, assignments) is assignments
    assert not (env / "generated/governed_tables.json").exists()


def test_ungovern_refuses_configured_agent_then_succeeds(tmp_path, capsys):
    env = _env(tmp_path)
    (env / "env.auto.tfvars").write_text(
        'governance_mode = "deterministic"\n'
        'genie_spaces = [{ name = "A", uc_tables = ["cat.sch.orders"] }]\n')
    merge_assignments(env, [_tag()])
    assert main(["check-ungovern", "--env-dir", str(env), "--table", "cat.sch.orders"]) == 1
    (env / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    assert main(["check-ungovern", "--env-dir", str(env), "--table", "cat.sch.orders"]) == 0
    assert "treatment tag cat.sch.orders.email" in capsys.readouterr().out
    assert main(["commit-ungovern", "--env-dir", str(env), "--table", "cat.sch.orders"]) == 0
    assert load_manifest(env) == {}
