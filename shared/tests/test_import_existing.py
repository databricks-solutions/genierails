import os
import re
import subprocess
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPT = Path(__file__).parents[1] / "scripts/import_existing.sh"


def _cleanup_python() -> str:
    source = SCRIPT.read_text()
    match = re.search(
        r"cleanup_stale_tag_assignments\(\) \{\n  python3 - << 'PYEOF'\n(.*?)\nPYEOF\n\}",
        source,
        re.DOTALL,
    )
    assert match
    return match.group(1)


class _State:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


def _response(state, value=None, statement_id="lookup"):
    rows = [] if value is None else [[value]]
    return SimpleNamespace(
        statement_id=statement_id,
        status=SimpleNamespace(state=state, error=None),
        result=SimpleNamespace(data_array=rows),
    )


@pytest.mark.parametrize(
    ("initial", "polled", "lookup_raises", "poll_raises", "expected_unsets"),
    [
        (_response(_State.SUCCEEDED, "pii"), None, False, False, 0),
        (_response(_State.SUCCEEDED, "wrong"), None, False, False, 1),
        (_response(_State.FAILED), None, False, False, 1),
        (_response(_State.PENDING), _response(_State.SUCCEEDED), False, False, 1),
        (None, None, True, False, 1),
        (_response(_State.PENDING), None, False, True, 1),
        (_response(_State.PENDING), _response(_State.PENDING), False, False, 1),
        (_response(_State.PENDING), _response(_State.SUCCEEDED, "pii"), False, False, 0),
    ],
    ids=[
        "success-match",
        "success-mismatch",
        "failed",
        "pending-without-row",
        "lookup-raises",
        "poll-raises",
        "poll-timeout",
        "pending-to-success-match",
    ],
)
def test_stale_tag_lookup_fails_closed(
    monkeypatch, tmp_path, initial, polled, lookup_raises, poll_raises, expected_unsets
):
    (tmp_path / "abac.auto.tfvars").write_text(
        'tag_assignments = [{ entity_type = "tables", entity_name = "cat.sch.tbl", '
        'tag_key = "sensitivity", tag_value = "pii" }]\n'
    )
    (tmp_path / "env.auto.tfvars").write_text('sql_warehouse_id = "wh"\n')

    calls = []

    class Execution:
        def execute_statement(self, *, statement, **_kwargs):
            calls.append(statement)
            if statement.startswith("SELECT"):
                if lookup_raises:
                    raise RuntimeError("lookup failed")
                return initial
            return _response(_State.SUCCEEDED, statement_id="unset")

        def get_statement(self, _statement_id):
            if poll_raises:
                raise RuntimeError("poll failed")
            assert polled is not None
            return polled

    client = SimpleNamespace(statement_execution=Execution())
    sdk = types.ModuleType("databricks.sdk")
    sdk.WorkspaceClient = lambda **_kwargs: client
    sql = types.ModuleType("databricks.sdk.service.sql")
    sql.StatementState = _State
    monkeypatch.setitem(sys.modules, "databricks.sdk", sdk)
    monkeypatch.setitem(sys.modules, "databricks.sdk.service.sql", sql)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    exec(compile(_cleanup_python(), str(SCRIPT), "exec"), {"__name__": "__main__"})

    assert sum(statement.startswith("ALTER TABLE") for statement in calls) == expected_unsets


def test_failed_optional_import_continues_and_surfaces_failure(tmp_path):
    data_access = tmp_path / "env" / "data_access"
    data_access.mkdir(parents=True)
    (data_access / "abac.auto.tfvars").write_text(
        'tag_assignments = [\n'
        '  { entity_type = "tables", entity_name = "cat.sch.first", tag_key = "k", tag_value = "v" },\n'
        '  { entity_type = "tables", entity_name = "cat.sch.second", tag_key = "k", tag_value = "v" }\n'
        ']\n'
    )
    runner = tmp_path / "terraform-runner"
    runner.write_text(
        "#!/usr/bin/env bash\n"
        "if [ \"$3\" = state ]; then exit 0; fi\n"
        "echo \"$*\" >> \"$RUNNER_LOG\"\n"
        "if [[ \"$*\" == *first* ]]; then echo optional-import-failed; exit 1; fi\n"
    )
    runner.chmod(0o755)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    python = fake_bin / "python3"
    python.symlink_to(sys.executable)

    env = os.environ | {
        "TERRAFORM_RUNNER": str(runner),
        "RUNNER_LOG": str(tmp_path / "runner.log"),
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
    }
    result = subprocess.run(
        ["bash", SCRIPT, "--tag-assignments-only"],
        cwd=data_access,
        env=env,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0
    assert "optional-import-failed" in result.stderr
    assert "Failed to import" in result.stdout
    log = (tmp_path / "runner.log").read_text()
    assert "first" in log
    assert "second" in log


@pytest.mark.parametrize(("manage_groups", "imports"), [("false", False), ("true", True)])
def test_account_group_import_runs_only_when_groups_are_managed(tmp_path, manage_groups, imports):
    """Consume mode has no databricks_group.groups resource to import into."""
    account = tmp_path / "account"
    account.mkdir()
    (account / "abac.auto.tfvars").write_text('groups = { tier_a = {} }\n')
    (account / "env.auto.tfvars").write_text(f"manage_groups = {manage_groups}\n")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    # Stand-in for the group lookup: print one name:id pair.
    (fake_bin / "python3").write_text("#!/usr/bin/env bash\ncat >/dev/null; echo tier_a:123\n")
    (fake_bin / "python3").chmod(0o755)

    result = subprocess.run(
        ["bash", SCRIPT, "--groups-only", "--dry-run"],
        cwd=account,
        env=os.environ | {"PATH": f"{fake_bin}:{os.environ['PATH']}"},
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert ('databricks_group.groups["tier_a"]' in result.stdout) is imports
