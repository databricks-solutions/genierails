"""Fixes from the final live champion run.

Covers: a rotated SP secret neither forces masking functions to be replaced
nor breaks their destroy-time drop, and the LLM spinner stays quiet in logs.
"""

import io
import re
from pathlib import Path

import deploy_masking_functions as dmf
import generate_abac

SHARED = Path(__file__).parents[1]
DATA_ACCESS_TF = SHARED / "modules/data_access/main.tf"
HOST = "https://dbc-example.cloud.databricks.com"


def _write_auth(directory: Path, host: str = HOST, secret: str = "current-secret") -> None:
    (directory / "auth.auto.tfvars").write_text(
        f'databricks_workspace_host = "{host}"\n'
        'databricks_client_id = "sp-client"\n'
        f'databricks_client_secret = "{secret}"\n'
    )


def _stale_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", HOST + "/")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "sp-client")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "stale-secret")


def test_drop_uses_current_secret_from_auth_file(tmp_path, monkeypatch):
    _stale_env(monkeypatch)
    _write_auth(tmp_path)
    sql = tmp_path / "masking_functions.sql"
    sql.write_text("")

    assert dmf.refresh_credentials_from_auth_file(str(sql)) is True
    assert dmf.os.environ["DATABRICKS_CLIENT_SECRET"] == "current-secret"


def test_auth_file_for_another_host_is_ignored(tmp_path, monkeypatch):
    _stale_env(monkeypatch)
    _write_auth(tmp_path, host="https://other.cloud.databricks.com")
    sql = tmp_path / "masking_functions.sql"
    sql.write_text("")

    assert dmf.refresh_credentials_from_auth_file(str(sql)) is False
    assert dmf.os.environ["DATABRICKS_CLIENT_SECRET"] == "stale-secret"


def test_missing_auth_file_keeps_provisioner_credentials(tmp_path, monkeypatch):
    _stale_env(monkeypatch)
    sql = tmp_path / "masking_functions.sql"
    sql.write_text("")

    assert dmf.refresh_credentials_from_auth_file(str(sql)) is False
    assert dmf.os.environ["DATABRICKS_CLIENT_SECRET"] == "stale-secret"


def test_secret_rotation_does_not_replace_masking_functions():
    tf = DATA_ACCESS_TF.read_text()
    block = tf[tf.index('resource "null_resource" "deploy_masking_functions"'):]
    block = block[: block.index("\nresource ")]
    ignored = re.search(r"ignore_changes\s*=\s*\[(.*?)\n\s*\]", block, re.S)
    assert ignored, "deploy_masking_functions must ignore credential trigger changes"
    assert 'triggers["client_id"]' in ignored.group(1)
    assert 'triggers["client_secret"]' in ignored.group(1)


def test_spinner_writes_one_line_when_not_a_tty(monkeypatch):
    buf = io.StringIO()  # isatty() is False, like a CI log or `make ... > log`
    monkeypatch.setattr(generate_abac.sys, "stderr", buf)
    with generate_abac.Spinner("Calling LLM"):
        generate_abac.time.sleep(0.25)

    out = buf.getvalue()
    assert out.count("Calling LLM") == 1
    assert "\r" not in out
    assert not any(frame in out for frame in generate_abac.Spinner.FRAMES)
