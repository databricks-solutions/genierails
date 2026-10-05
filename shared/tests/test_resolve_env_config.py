import subprocess
from pathlib import Path

import pytest

from scripts.resolve_env_config import resolve_verify_key, resolve_warehouse

SHARED = Path(__file__).resolve().parent.parent


def test_verify_key_prefers_explicit_then_persisted(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text('verify_key_column = "customer_id"\n')
    assert resolve_verify_key(tmp_path) == "customer_id"
    assert resolve_verify_key(tmp_path, " order_id ") == "order_id"


def test_verify_key_is_declared_in_every_root_that_loads_env_tfvars():
    for root in ("workspace", "data_access"):
        text = (SHARED / "roots" / root / "main.tf").read_text()
        assert 'variable "verify_key_column"' in text


def test_warehouse_resolution_order_and_unique_per_space(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text('''
sql_warehouse_id = ""
genie_spaces = [
  { name = "a", sql_warehouse_id = "space-wh" },
  { name = "b", sql_warehouse_id = "space-wh" },
]
''')
    assert resolve_warehouse(tmp_path) == "space-wh"
    assert resolve_warehouse(tmp_path, "explicit-wh") == "explicit-wh"


def test_unique_per_space_warehouse_ignores_inheriting_empty_spaces(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text('''
genie_spaces = [
  { name = "explicit", sql_warehouse_id = "space-wh" },
  { name = "inherits", sql_warehouse_id = "" },
]
''')
    assert resolve_warehouse(tmp_path) == "space-wh"


def test_warehouse_resolution_fails_on_ambiguity(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text('''
genie_spaces = [
  { name = "a", sql_warehouse_id = "wh-a" },
  { name = "b", sql_warehouse_id = "wh-b" },
]
''')
    with pytest.raises(ValueError, match="ambiguous warehouse"):
        resolve_warehouse(tmp_path)


def test_warehouse_falls_back_to_workspace_terraform_output(tmp_path, monkeypatch):
    (tmp_path / "env.auto.tfvars").write_text('sql_warehouse_id = ""\n')
    runner = tmp_path / "runner"
    runner.write_text("#!/bin/sh\n")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(
        a[0], 0, '+ terraform output\n"auto-wh"\n', ""
    ))
    assert resolve_warehouse(
        tmp_path, terraform_runner=runner, env_name="dev"
    ) == "auto-wh"


@pytest.mark.parametrize("stdout", [
    "",
    "Warning: No outputs found\n╵\n",
    "null\n",
    '""\n',
])
def test_terraform_missing_or_empty_output_is_unresolved(tmp_path, monkeypatch, stdout):
    (tmp_path / "env.auto.tfvars").write_text("")
    runner = tmp_path / "runner"
    runner.write_text("#!/bin/sh\n")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(
        a[0], 0, stdout, ""
    ))
    with pytest.raises(ValueError, match="no SQL warehouse|missing or invalid JSON"):
        resolve_warehouse(tmp_path, terraform_runner=runner, env_name="dev")


def test_terraform_nonzero_exit_reports_failure(tmp_path, monkeypatch):
    (tmp_path / "env.auto.tfvars").write_text("")
    runner = tmp_path / "runner"
    runner.write_text("#!/bin/sh\n")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(
        a[0], 1, "", "state unavailable"
    ))
    with pytest.raises(ValueError, match="Terraform warehouse output failed: state unavailable"):
        resolve_warehouse(tmp_path, terraform_runner=runner, env_name="dev")


def test_missing_terraform_runner_reports_clear_error(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text("")
    with pytest.raises(ValueError, match="Terraform runner not found"):
        resolve_warehouse(
            tmp_path, terraform_runner=tmp_path / "missing-runner", env_name="dev"
        )


def test_terraform_timeout_reports_clear_error(tmp_path, monkeypatch):
    (tmp_path / "env.auto.tfvars").write_text("")
    runner = tmp_path / "runner"
    runner.write_text("#!/bin/sh\n")
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])
    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(ValueError, match="timed out"):
        resolve_warehouse(tmp_path, terraform_runner=runner, env_name="dev")


def test_invalid_warehouse_id_is_rejected(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text('sql_warehouse_id = "Warning: nope"\n')
    with pytest.raises(ValueError, match="invalid SQL warehouse ID"):
        resolve_warehouse(tmp_path)
