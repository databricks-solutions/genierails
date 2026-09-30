"""Unit tests for the scheduled steady-state governance wrapper.

The wrapper is glue only: it resolves the target env directory and shells out to
the existing entrypoints. These tests exercise that glue without a Databricks
connection, LLM call, or Terraform — subprocess is stubbed so no real script
runs.
"""
import sys
from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import run_scheduled_governance as rsg  # noqa: E402

REPO_ROOT = Path(rsg.REPO_ROOT)


def test_resolve_env_dir_relative_is_repo_relative():
    resolved = rsg._resolve_env_dir("aws/envs/prod")
    assert resolved == (REPO_ROOT / "aws" / "envs" / "prod").resolve()


def test_resolve_env_dir_absolute_is_unchanged(tmp_path):
    resolved = rsg._resolve_env_dir(str(tmp_path))
    assert resolved == tmp_path


def test_bad_env_dir_returns_2(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv",
                        ["prog", "--env-dir", "aws/envs/does-not-exist", "--step", "audit"])
    assert rsg.main() == 2
    assert "env directory not found" in capsys.readouterr().err


def test_gitignored_envs_dir_absent_from_checkout():
    # Regression guard: the repo's envs/ is .gitignore'd, so a fresh Git checkout
    # of this branch does NOT contain the env the job scans. Without a config
    # source the wrapper cannot locate it — hence materialization is required.
    assert not (REPO_ROOT / "aws" / "envs" / "prod").exists()


def test_materialize_env_dir_copies_config_into_checkout(tmp_path):
    # Simulate a runtime-visible config source (UC Volume / workspace path)...
    source = tmp_path / "volume_prod"
    (source / "data_access").mkdir(parents=True)
    (source / "auth.auto.tfvars").write_text("# auth\n")
    (source / "env.auto.tfvars").write_text("# env\n")
    (source / "data_access" / "abac.auto.tfvars").write_text("tag_assignments = [\n]\n")

    # ...and the (initially absent) env dir inside the checkout.
    env_dir = tmp_path / "checkout" / "aws" / "envs" / "prod"
    assert not env_dir.exists()

    rsg._materialize_env_dir(str(source), env_dir)

    assert env_dir.is_dir()
    assert (env_dir / "auth.auto.tfvars").read_text() == "# auth\n"
    assert (env_dir / "env.auto.tfvars").exists()
    assert (env_dir / "data_access" / "abac.auto.tfvars").exists()


def test_materialize_missing_source_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        rsg._materialize_env_dir(str(tmp_path / "nope"), tmp_path / "env")


def test_main_materializes_env_dir_before_running_steps(tmp_path, monkeypatch):
    # End-to-end: with --config-source, main() materializes the env dir (which
    # does not exist beforehand) and then runs the steps against it.
    source = tmp_path / "volume_prod"
    (source / "data_access").mkdir(parents=True)
    (source / "data_access" / "abac.auto.tfvars").write_text("tag_assignments = [\n]\n")
    env_dir = tmp_path / "checkout" / "aws" / "envs" / "prod"

    seen = {}

    def fake_audit(ed):
        seen["audit_exists"] = ed.is_dir()
        return 0

    monkeypatch.setattr(rsg, "_audit", fake_audit)
    monkeypatch.setattr(rsg, "_delta", lambda ed, auth_file, catalog="": 0)
    monkeypatch.setattr(rsg, "_coverage", lambda ed: 0)
    monkeypatch.setattr(sys, "argv",
                        ["prog", "--env-dir", str(env_dir),
                         "--config-source", str(source), "--step", "all"])

    assert rsg.main() == 0
    assert env_dir.is_dir()               # materialized
    assert seen["audit_exists"] is True   # existed before the first step ran


def test_main_missing_config_source_and_env_dir_returns_2(tmp_path, monkeypatch, capsys):
    # Nonexistent config source -> materialize fails -> exit 2 with guidance.
    env_dir = tmp_path / "checkout" / "aws" / "envs" / "prod"
    monkeypatch.setattr(sys, "argv",
                        ["prog", "--env-dir", str(env_dir),
                         "--config-source", str(tmp_path / "missing"), "--step", "all"])
    assert rsg.main() == 2
    assert "config source not found" in capsys.readouterr().err


def test_coverage_no_config_fails_loudly(tmp_path, capsys):
    # No config in either layer -> hard failure, never a silent pass.
    assert rsg._coverage(tmp_path) == 1
    assert "no ABAC config to validate" in capsys.readouterr().err


def test_coverage_falls_back_to_data_access_layer(tmp_path, monkeypatch):
    # No generated/ config, but a split data_access/ config exists.
    da = tmp_path / "data_access"
    da.mkdir()
    (da / "abac.auto.tfvars").write_text("tag_assignments = [\n]\n")
    calls = {}

    def fake_run(cmd, cwd):
        calls["cmd"] = cmd
        return 0

    monkeypatch.setattr(rsg, "_run", fake_run)
    assert rsg._coverage(tmp_path) == 0
    assert str(da / "abac.auto.tfvars") in calls["cmd"]


def test_audit_invokes_existing_script(tmp_path, monkeypatch):
    calls = {}

    def fake_run(cmd, cwd):
        calls["cmd"] = cmd
        calls["cwd"] = cwd
        return 0

    monkeypatch.setattr(rsg, "_run", fake_run)
    assert rsg._audit(tmp_path) == 0
    assert calls["cmd"][1].endswith("scripts/audit_schema_drift.py")
    assert calls["cwd"] == tmp_path


def test_delta_invokes_generate_abac_with_delta_flags(tmp_path, monkeypatch):
    calls = {}
    monkeypatch.setattr(rsg, "_run", lambda cmd, cwd: calls.setdefault("cmd", cmd) or 0)
    rsg._delta(tmp_path, "auth.auto.tfvars")
    cmd = calls["cmd"]
    assert cmd[1].endswith("generate_abac.py")
    assert "--delta" in cmd
    assert cmd[cmd.index("--auth-file") + 1] == "auth.auto.tfvars"
    assert "--catalog" not in cmd  # omitted when no catalog given


def test_delta_threads_catalog_when_set(tmp_path, monkeypatch):
    calls = {}
    monkeypatch.setattr(rsg, "_run", lambda cmd, cwd: calls.setdefault("cmd", cmd) or 0)
    rsg._delta(tmp_path, "auth.auto.tfvars", catalog="prod_fin")
    cmd = calls["cmd"]
    assert cmd[cmd.index("--catalog") + 1] == "prod_fin"


def test_coverage_includes_masking_sql_when_present(tmp_path, monkeypatch):
    gen = tmp_path / "generated"
    gen.mkdir()
    (gen / "abac.auto.tfvars").write_text("# empty\n")
    (gen / "masking_functions.sql").write_text("-- empty\n")
    calls = {}
    monkeypatch.setattr(rsg, "_run", lambda cmd, cwd: calls.setdefault("cmd", cmd) or 0)
    rsg._coverage(tmp_path)
    cmd = calls["cmd"]
    assert cmd[1].endswith("validate_abac.py")
    assert str(gen / "abac.auto.tfvars") in cmd
    assert str(gen / "masking_functions.sql") in cmd


def test_step_all_runs_all_three_steps(tmp_path, monkeypatch):
    ran = []
    monkeypatch.setattr(rsg, "_audit", lambda env_dir: ran.append("audit") or 0)
    monkeypatch.setattr(rsg, "_delta", lambda env_dir, auth_file, catalog="": ran.append("delta") or 0)
    monkeypatch.setattr(rsg, "_coverage", lambda env_dir: ran.append("coverage") or 0)
    monkeypatch.setattr(sys, "argv", ["prog", "--env-dir", str(tmp_path), "--step", "all"])
    assert rsg.main() == 0
    assert ran == ["audit", "delta", "coverage"]


def test_step_all_remembers_last_nonzero_exit(tmp_path, monkeypatch):
    # A drift exit (1) from audit must not stop later steps, but is remembered.
    monkeypatch.setattr(rsg, "_audit", lambda env_dir: 1)
    monkeypatch.setattr(rsg, "_delta", lambda env_dir, auth_file, catalog="": 0)
    monkeypatch.setattr(rsg, "_coverage", lambda env_dir: 0)
    monkeypatch.setattr(sys, "argv", ["prog", "--env-dir", str(tmp_path), "--step", "all"])
    assert rsg.main() == 1
