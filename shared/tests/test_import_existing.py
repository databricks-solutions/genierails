import json
import os
import re
import subprocess
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests import fake_uc


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


def test_deterministic_cleanup_skips_treatments_but_cleans_other_tags(monkeypatch, tmp_path):
    monkeypatch.setenv("GENIERAILS_IMPORT_SKIP_TREATMENT_TAGS", "true")
    (tmp_path / "abac.auto.tfvars").write_text(
        'tag_assignments = [\n'
        '  { entity_type = "columns", entity_name = "cat.sch.tbl.ssn", tag_key = "gr_treatment", tag_value = "redact__string__12345678" },\n'
        '  { entity_type = "tables", entity_name = "cat.sch.tbl", tag_key = "sensitivity", tag_value = "pii" }\n'
        ']\n'
    )
    (tmp_path / "env.auto.tfvars").write_text(
        'governance_mode = "deterministic"\nsql_warehouse_id = "wh"\n'
    )
    calls = []

    class Execution:
        def execute_statement(self, *, statement, **_kwargs):
            calls.append(statement)
            if statement.startswith("SELECT"):
                return _response(_State.SUCCEEDED, "old")
            return _response(_State.SUCCEEDED, statement_id="unset")

    client = SimpleNamespace(statement_execution=Execution())
    sdk = types.ModuleType("databricks.sdk")
    sdk.WorkspaceClient = lambda **_kwargs: client
    sql = types.ModuleType("databricks.sdk.service.sql")
    sql.StatementState = _State
    monkeypatch.setitem(sys.modules, "databricks.sdk", sdk)
    monkeypatch.setitem(sys.modules, "databricks.sdk.service.sql", sql)
    monkeypatch.chdir(tmp_path)
    exec(compile(_cleanup_python(), str(SCRIPT), "exec"), {"__name__": "__main__"})

    alters = [statement for statement in calls if statement.startswith("ALTER TABLE")]
    assert alters == ["ALTER TABLE cat.sch.tbl UNSET TAGS ('sensitivity')"]


def test_deterministic_import_skips_treatments_including_steady_state_retag(tmp_path):
    data_access = tmp_path / "env" / "data_access"
    data_access.mkdir(parents=True)
    (tmp_path / "env" / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    (data_access / "abac.auto.tfvars").write_text(
        'tag_assignments = [\n'
        '  { entity_type = "columns", entity_name = "cat.sch.tbl.ssn", tag_key = "gr_treatment", tag_value = "redact__string__newvalue" },\n'
        '  { entity_type = "tables", entity_name = "cat.sch.tbl", tag_key = "sensitivity", tag_value = "pii" }\n'
        ']\n'
    )
    result = subprocess.run(
        ["bash", SCRIPT, "--tag-assignments-only", "--dry-run", "--skip-treatment-tags"], cwd=data_access,
        env=os.environ, text=True, capture_output=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "gr_treatment" not in result.stdout
    assert 'assignments["tables|cat.sch.tbl|sensitivity|pii"]' in result.stdout


STALE_TREATMENT = (
    'tag_assignments = [\n'
    '  { entity_type = "columns", entity_name = "cat.sch.tbl.ssn", tag_key = "gr_treatment", tag_value = "redact__string__newvalue" },\n'
    '  { entity_type = "tables", entity_name = "cat.sch.tbl", tag_key = "sensitivity", tag_value = "pii" },\n'
    '  { entity_type = "tables", entity_name = "cat.sch.tbl", tag_key = "owner", tag_value = "fin" }\n'
    ']\n'
)
# Live UC: the treatment tag still has its legacy value, sensitivity is stale, owner is current.
LIVE = {
    "column_tags": [["cat.sch.tbl.ssn", "gr_treatment", "redact"]],
    "table_tags": [["cat.sch.tbl", "sensitivity", "public"], ["cat.sch.tbl", "owner", "fin"]],
    "warehouses": [{"id": "wh", "name": "Shared", "state": "RUNNING"}],
}


def _layer(tmp_path, env_file=None, abac=STALE_TREATMENT):
    data_access = tmp_path / "env" / "data_access"
    data_access.mkdir(parents=True)
    if env_file is not None:
        (tmp_path / "env" / "env.auto.tfvars").write_text(env_file)
    (data_access / "abac.auto.tfvars").write_text(abac)
    return data_access


def _run(data_access, fake, *args):
    return subprocess.run(["bash", SCRIPT, "--tag-assignments-only", *args], cwd=data_access,
                          env=os.environ | fake, text=True, capture_output=True)


@pytest.mark.parametrize("env_file", ["not hcl {\n", 'governance_mode = "determinstic"\n', None],
                         ids=["unparseable", "unknown-mode", "missing"])
def test_unreadable_governance_mode_stops_before_any_import_or_unset(tmp_path, env_file):
    data_access = _layer(tmp_path, env_file)
    fake = fake_uc.install(tmp_path, LIVE)
    result = _run(data_access, fake)
    assert result.returncode == 3, result.stdout + result.stderr
    assert "cannot tell who owns gr_treatment tags" in result.stderr
    assert fake_uc.events(fake) == []  # no SQL, no runner call: nothing imported or cleared


def test_a_stale_layer_local_env_file_does_not_hide_a_deterministic_env(tmp_path):
    """The env-level file decides, never data_access/env.auto.tfvars (a stale copy, not the symlink)."""
    data_access = _layer(tmp_path, 'governance_mode = "deterministic"\n')
    (data_access / "env.auto.tfvars").write_text('governance_mode = "legacy"\nsql_warehouse_id = "wh"\n')
    fake = fake_uc.install(tmp_path, LIVE)
    result = _run(data_access, fake)
    assert result.returncode == 0, result.stdout + result.stderr
    assert fake_uc.sql_writes(fake) == ["ALTER TABLE cat.sch.tbl UNSET TAGS ('sensitivity')"]
    imports = fake_uc.runner_imports(fake)
    assert imports and not [argv for argv in imports if "gr_treatment" in argv[3]]


def test_a_missing_governance_mode_is_legacy_as_everywhere_else(tmp_path):
    data_access = _layer(tmp_path, 'sql_warehouse_id = "wh"\n')
    fake = fake_uc.install(tmp_path, LIVE)
    result = _run(data_access, fake)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "ALTER TABLE cat.sch.tbl ALTER COLUMN ssn UNSET TAGS ('gr_treatment')" in fake_uc.sql_writes(fake)


@pytest.mark.parametrize("ci", [False, True])
def test_large_idempotent_state_is_quiet_and_does_not_retry_import(tmp_path, ci):
    data_access = _layer(tmp_path, DETERMINISTIC, abac=(
        'tag_assignments = [{ entity_type = "tables", entity_name = "cat.sch.tbl", '
        'tag_key = "owner", tag_value = "fin" }]\n'))
    runner = tmp_path / "state-runner"
    runner.write_text(f'''#!{sys.executable}
import sys
address = {TAG_IMPORT!r}
if sys.argv[3] == "state-list":
    print(address)
    for i in range(20000):
        print(f"module.data_access.fake.resource[{{i}}]-" + "x" * 100)
elif sys.argv[3] == "import":
    print("Error: Resource already managed by Terraform", file=sys.stderr)
    sys.exit(1)
''')
    runner.chmod(0o755)
    record = _write_record(tmp_path / "imports.jsonl", [
        {"kind": "import", "layer": "data_access", "address": TAG_IMPORT,
         "id": "tables,cat.sch.tbl,owner"},
    ])
    env = os.environ | {"TERRAFORM_RUNNER": str(runner)} | ({"CI": "true"} if ci else {})

    result = subprocess.run(["bash", SCRIPT, "--skip-treatment-tags", f"--apply-recorded={record}"], cwd=data_access,
                            env=env, text=True, capture_output=True)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Already in state" in result.stdout
    assert "Resource already managed by Terraform" not in result.stdout + result.stderr


@pytest.mark.parametrize("ci", [False, True])
def test_a_large_already_managed_import_error_is_skipped_not_failed(tmp_path, ci):
    # The state listing misses the address (a concurrent import landed it), so
    # the import runs and Terraform reports it already managed, followed by far
    # more output than a pipe holds. grep -q stops reading at the first match.
    data_access = _layer(tmp_path, DETERMINISTIC, abac=(
        'tag_assignments = [{ entity_type = "tables", entity_name = "cat.sch.tbl", '
        'tag_key = "owner", tag_value = "fin" }]\n'))
    runner = tmp_path / "import-runner"
    runner.write_text(f'''#!{sys.executable}
import sys
if sys.argv[3] == "import":
    print("Error: Resource already managed by Terraform", file=sys.stderr)
    for i in range(20000):
        print(f"  detail line {{i}} " + "x" * 100, file=sys.stderr)
    sys.exit(1)
''')
    runner.chmod(0o755)
    record = _write_record(tmp_path / "imports.jsonl", [
        {"kind": "import", "layer": "data_access", "address": TAG_IMPORT,
         "id": "tables,cat.sch.tbl,owner"},
    ])
    env = os.environ | {"TERRAFORM_RUNNER": str(runner)} | ({"CI": "true"} if ci else {})

    result = subprocess.run(["bash", SCRIPT, "--skip-treatment-tags", f"--apply-recorded={record}"], cwd=data_access,
                            env=env, text=True, capture_output=True)

    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-2000:]
    assert f"Already managed: {TAG_IMPORT} (skipping)" in result.stdout
    assert "Failed to import" not in result.stdout


def _state_bytes(data_access):
    path = data_access / "terraform.tfstate"
    return path.read_bytes() if path.exists() else None


def test_record_mode_changes_nothing_live_or_in_state(tmp_path):
    """release-plan: the stale tag and the importable one are recorded, never cleared or imported."""
    data_access = _layer(tmp_path, 'governance_mode = "deterministic"\nsql_warehouse_id = "wh"\n')
    fake = fake_uc.install(tmp_path, LIVE)
    record = tmp_path / "imports.jsonl"
    result = _run(data_access, fake, "--skip-treatment-tags", f"--record={record}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert fake_uc.sql_writes(fake) == [] and fake_uc.runner_imports(fake) == []
    assert _state_bytes(data_access) is None
    entries = [json.loads(line) for line in record.read_text().splitlines()]
    assert entries == [
        {"kind": "cleanup", "layer": "data_access", "entity_type": "tables", "entity_name": "cat.sch.tbl",
         "tag_key": "sensitivity", "statement": "ALTER TABLE `cat`.`sch`.`tbl` UNSET TAGS ('sensitivity')",
         "warehouse_id": "wh"},
        {"kind": "import", "layer": "data_access",
         "address": 'module.data_access.databricks_entity_tag_assignment.assignments["tables|cat.sch.tbl|owner|fin"]',
         "id": "tables,cat.sch.tbl,owner"},
    ]


@pytest.mark.parametrize("mode", ["record", "import"])
def test_an_import_already_in_state_is_skipped(tmp_path, mode):
    # The runner's alias is state-list; "state list" is refused (and the
    # refusal, behind 2>/dev/null, read as "not in state").
    data_access = _layer(tmp_path, DETERMINISTIC)
    fake = fake_uc.install(tmp_path, LIVE)
    (data_access / "terraform.tfstate").write_text(json.dumps({"version": 4, "resources": [
        {"module": "module.data_access", "mode": "managed", "type": "databricks_entity_tag_assignment",
         "name": "assignments", "instances": [{"index_key": "tables|cat.sch.tbl|owner|fin",
                                               "attributes": {"id": "tables,cat.sch.tbl,owner"}}]}]}))
    record = tmp_path / "imports.jsonl"
    args = [f"--record={record}"] if mode == "record" else []
    result = _run(data_access, fake | {"LAYER_ENV_DIR": str(data_access)}, "--skip-treatment-tags", *args)
    assert result.returncode == 0, result.stdout + result.stderr
    assert TAG_IMPORT not in [argv[3] for argv in fake_uc.runner_imports(fake)]
    assert {e["argv"][2] for e in fake_uc.events(fake) if e["src"] == "runner"} <= {"state-list", "import"}
    if mode == "record":
        assert [json.loads(line)["kind"] for line in record.read_text().splitlines()] == ["cleanup"]
    else:
        assert f"Already in state: {TAG_IMPORT}" in result.stdout


def test_record_mode_refuses_what_it_cannot_verify(tmp_path):
    abac = ('fgac_policies = [{ name = "mask", catalog = "cat", policy_type = "POLICY_TYPE_COLUMN_MASK" }]\n')
    data_access = _layer(tmp_path, 'governance_mode = "deterministic"\n', abac)
    fake = fake_uc.install(tmp_path, LIVE | {"rest_down": True})
    result = subprocess.run(["bash", SCRIPT, "--fgac-only", "--skip-treatment-tags",
                             f"--record={tmp_path / 'imports.jsonl'}"], cwd=data_access,
                            env=os.environ | fake, text=True, capture_output=True)
    assert result.returncode != 0
    assert "records only imports it verified exist" in result.stderr
    assert (tmp_path / "imports.jsonl").read_text() == ""


TAG_IMPORT = 'module.data_access.databricks_entity_tag_assignment.assignments["tables|cat.sch.tbl|owner|fin"]'
CLEANUP = {"kind": "cleanup", "layer": "data_access", "entity_type": "tables", "entity_name": "cat.sch.tbl",
           "tag_key": "sensitivity", "statement": "ALTER TABLE `cat`.`sch`.`tbl` UNSET TAGS ('sensitivity')",
           "warehouse_id": "wh"}
DETERMINISTIC = 'governance_mode = "deterministic"\nsql_warehouse_id = "wh"\n'


def _write_record(path, entries):
    path.write_text("".join(json.dumps(e) + "\n" for e in entries))
    return path


def test_apply_recorded_runs_exactly_the_layers_recorded_entries(tmp_path):
    data_access = _layer(tmp_path, DETERMINISTIC)
    fake = fake_uc.install(tmp_path, LIVE)
    record = _write_record(tmp_path / "imports.jsonl", [
        CLEANUP,
        {"kind": "import", "layer": "data_access", "address": TAG_IMPORT, "id": "tables,cat.sch.tbl,owner"},
        {"kind": "import", "layer": "workspace", "address": "module.workspace.databricks_sql_endpoint.warehouse[0]",
         "id": "w-id"},
    ])
    result = _run(data_access, fake, "--skip-treatment-tags", f"--apply-recorded={record}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert fake_uc.sql_writes(fake) == ["ALTER TABLE `cat`.`sch`.`tbl` UNSET TAGS ('sensitivity')"]
    assert [e["warehouse_id"] for e in fake_uc.events(fake) if e["src"] == "sql"] == ["wh"]
    assert [argv[3:] for argv in fake_uc.runner_imports(fake)] == [[TAG_IMPORT, "tables,cat.sch.tbl,owner"]]
    # A recorded import that fails fails the release (it was verified to exist).
    failing = _run(data_access, fake | {"FAKE_UC_FAIL_IMPORT": "1"}, "--skip-treatment-tags",
                   f"--apply-recorded={record}")
    assert failing.returncode == 1 and "recorded import(s) failed" in failing.stderr


def test_what_release_plan_records_release_runs(tmp_path):
    """The recorder's own output passes the check, and release runs the rebuilt UNSET and the import."""
    data_access = _layer(tmp_path, DETERMINISTIC)
    record = tmp_path / "imports.jsonl"
    recorded = _run(data_access, fake_uc.install(tmp_path / "plan", LIVE), "--skip-treatment-tags",
                    f"--record={record}")
    assert recorded.returncode == 0, recorded.stdout + recorded.stderr
    assert [json.loads(line) for line in record.read_text().splitlines()][0] == CLEANUP
    fake = fake_uc.install(tmp_path / "release", LIVE)
    result = _run(data_access, fake, "--skip-treatment-tags", f"--apply-recorded={record}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert fake_uc.sql_writes(fake) == [CLEANUP["statement"]]
    assert [argv[3] for argv in fake_uc.runner_imports(fake)] == [TAG_IMPORT]


def test_record_mode_needs_the_envs_own_warehouse_for_a_cleanup(tmp_path):
    """No sql_warehouse_id: the recorder doesn't pick some running warehouse release would then refuse."""
    data_access = _layer(tmp_path, 'governance_mode = "deterministic"\n')
    fake = fake_uc.install(tmp_path, LIVE)
    result = _run(data_access, fake, "--skip-treatment-tags", f"--record={tmp_path / 'imports.jsonl'}")
    assert result.returncode != 0 and "configures no SQL warehouse" in result.stderr
    assert fake_uc.sql_writes(fake) == [] and fake_uc.runner_imports(fake) == []


FORGED = {
    "treatment-cleanup": CLEANUP | {"entity_type": "columns", "entity_name": "cat.sch.tbl.ssn", "tag_key": "gr_treatment",
                                    "statement": "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `ssn` UNSET TAGS ('gr_treatment')"},
    "arbitrary-sql": CLEANUP | {"statement": "GRANT SELECT ON CATALOG cat TO `account users`"},
    "sql-in-identifier": CLEANUP | {"entity_name": "cat.sch.tbl` SET OWNER TO `x",
                                    "statement": "ALTER TABLE cat.sch.tbl` SET OWNER TO `x UNSET TAGS ('sensitivity')"},
    "foreign-warehouse": CLEANUP | {"warehouse_id": "someone-elses"},
    "extra-field": CLEANUP | {"note": "x"},
    "unknown-layer": {"kind": "import", "layer": "account", "address": "module.account.databricks_group.groups[\"g\"]",
                      "id": "1"},
    "unsupported-address": {"kind": "import", "layer": "data_access",
                            "address": 'module.data_access.databricks_grant.anything["cat|g"]', "id": "catalog/cat/g"},
    "treatment-import": {"kind": "import", "layer": "data_access",
                         "address": 'module.data_access.databricks_entity_tag_assignment.treatment["columns|cat.sch.tbl.ssn|gr_treatment"]',
                         "id": "columns,cat.sch.tbl.ssn,gr_treatment"},
    "treatment-cleanup-upper": CLEANUP | {"entity_type": "columns", "entity_name": "cat.sch.tbl.ssn", "tag_key": "GR_TREATMENT",
                                          "statement": "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `ssn` UNSET TAGS ('GR_TREATMENT')"},
    "treatment-tag-import-mixed-case": {
        "kind": "import", "layer": "data_access",
        "address": 'module.data_access.databricks_entity_tag_assignment.assignments["columns|cat.sch.tbl.ssn|Gr_Treatment|redact"]',
        "id": "columns,cat.sch.tbl.ssn,Gr_Treatment"},
    "mismatched-id": {"kind": "import", "layer": "data_access", "address": TAG_IMPORT, "id": "tables,cat.sch.other,owner"},
    "missing-field": {"kind": "import", "layer": "data_access", "address": TAG_IMPORT},
}


@pytest.mark.parametrize("forged", FORGED, ids=list(FORGED))
def test_apply_recorded_refuses_an_entry_the_recorder_never_writes_before_any_call(tmp_path, forged):
    """Even a hash-matching record: one bad entry and nothing runs, not the good entries before it either."""
    data_access = _layer(tmp_path, DETERMINISTIC)
    fake = fake_uc.install(tmp_path, LIVE)
    good = {"kind": "import", "layer": "data_access", "address": TAG_IMPORT, "id": "tables,cat.sch.tbl,owner"}
    record = _write_record(tmp_path / "imports.jsonl", [CLEANUP, good, FORGED[forged]])
    result = _run(data_access, fake, "--skip-treatment-tags", f"--apply-recorded={record}")
    assert result.returncode == 3, result.stdout + result.stderr
    assert "entry 3:" in result.stderr and "never records" in result.stderr
    assert fake_uc.events(fake) == []  # no SQL (lookup or write), no runner call


def test_the_check_accepts_every_shape_the_recorder_writes(tmp_path):
    from scripts import import_record
    (tmp_path / "env.auto.tfvars").write_text('sql_warehouse_id = "wh"\n')
    imports = [
        ('module.data_access.databricks_policy_info.policies["mask_ssn"]', "CATALOG,cat,cat_mask_ssn"),
        (TAG_IMPORT, "tables,cat.sch.tbl,owner"),
        ('module.data_access.databricks_entity_tag_assignment.assignments["columns|cat.sch.tbl.ssn|pii|yes"]',
         "columns,cat.sch.tbl.ssn,pii"),
        ('module.data_access.databricks_grant.catalog_access["cat|Finance Analysts"]', "catalog/cat/Finance Analysts"),
        ('module.data_access.databricks_grant.terraform_sp_manage_catalog["cat"]', "catalog/cat/1234-abcd"),
        ("module.data_access.databricks_sql_endpoint.warehouse[0]", "abc123"),
        ('module.data_access.databricks_data_classification_catalog_config.classification["cat"]', "catalogs/cat/config"),
    ]
    entries = [{"kind": "import", "layer": "data_access", "address": a, "id": i} for a, i in imports]
    entries += [{"kind": "import", "layer": "workspace", "address": "module.workspace.databricks_sql_endpoint.warehouse[0]",
                 "id": "abc123"}, import_record.cleanup_entry("columns", "cat.sch.tbl.ssn", "pii", "wh")]
    assert import_record.validate(entries, tmp_path) == entries
    assert entries[-1]["statement"] == "ALTER TABLE `cat`.`sch`.`tbl` ALTER COLUMN `ssn` UNSET TAGS ('pii')"


def test_failed_optional_import_continues_and_exits_nonzero_for_hard_failure(tmp_path):
    data_access = tmp_path / "env" / "data_access"
    data_access.mkdir(parents=True)
    (tmp_path / "env" / "env.auto.tfvars").write_text('governance_mode = "legacy"\n')
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

    assert result.returncode == 1
    assert "optional-import-failed" in result.stderr
    assert "Failed to import" in result.stdout
    assert "not treating this as a missing Databricks object" in result.stdout
    assert "import(s) failed because Terraform could not run the import walk" in result.stderr
    log = (tmp_path / "runner.log").read_text()
    assert "first" in log
    assert "second" in log


@pytest.mark.parametrize("hard_error", [
    "Error: Invalid for_each argument",
    "Error: Module output value precondition failed",
])
def test_a_configuration_error_is_not_blamed_on_the_object(tmp_path, hard_error):
    """Terraform config and precondition failures happen before object lookup."""
    data_access = tmp_path / "env" / "data_access"
    data_access.mkdir(parents=True)
    (tmp_path / "env" / "env.auto.tfvars").write_text('governance_mode = "legacy"\n')
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
        f"if [[ \"$*\" == *first* ]]; then echo {hard_error!r}; exit 1; fi\n"
        "echo 'Error: Cannot import non-existent remote object'; exit 1\n"
    )
    runner.chmod(0o755)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "python3").symlink_to(sys.executable)
    env = os.environ | {"TERRAFORM_RUNNER": str(runner), "PATH": f"{fake_bin}:{os.environ['PATH']}"}
    result = subprocess.run(["bash", SCRIPT, "--tag-assignments-only"], cwd=data_access, env=env,
                            text=True, capture_output=True)

    assert result.returncode == 1, result.stdout + result.stderr
    failed = [line for line in result.stdout.splitlines() if "Failed to import" in line]
    assert len(failed) == 2, result.stdout
    first, second = (next(line for line in failed if name in line) for name in ("first", "second"))
    assert "couldn't evaluate the configuration" in first and "may not exist" not in first
    assert second.endswith("(may not exist in Databricks)")
    assert hard_error in result.stderr
    assert "import(s) failed because Terraform could not run the import walk" in result.stderr


@pytest.mark.parametrize(("terraform_output", "expected"), [
    ("+ terraform import module.data_access.databricks_policy_info.policies[dev404]\n"
     "Error: failed to configure provider: authentication failed\n",
     "authentication failed"),
    ("Error: PERMISSION_DENIED: cannot read policy t_a404f\n", "PERMISSION_DENIED"),
    ("Error: Configuration for import target does not exist\n", "Configuration for import target does not exist"),
])
def test_only_provider_missing_object_errors_are_soft_import_misses(tmp_path, terraform_output, expected):
    data_access = tmp_path / "env" / "data_access"
    data_access.mkdir(parents=True)
    (tmp_path / "env" / "env.auto.tfvars").write_text('governance_mode = "legacy"\n')
    (data_access / "abac.auto.tfvars").write_text(
        'tag_assignments = [{ entity_type = "tables", entity_name = "cat.sch.probe", '
        'tag_key = "k", tag_value = "v" }]\n'
    )
    runner = tmp_path / "terraform-runner"
    runner.write_text(
        "#!/usr/bin/env bash\n"
        "if [ \"$3\" = state ]; then exit 0; fi\n"
        f"printf '%b' {terraform_output!r}; exit 1\n"
    )
    runner.chmod(0o755)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "python3").symlink_to(sys.executable)
    env = os.environ | {"TERRAFORM_RUNNER": str(runner), "PATH": f"{fake_bin}:{os.environ['PATH']}"}

    result = subprocess.run(["bash", SCRIPT, "--tag-assignments-only"], cwd=data_access, env=env,
                            text=True, capture_output=True)

    assert result.returncode == 1, result.stdout + result.stderr
    assert expected in result.stderr
    assert "may not exist in Databricks" not in result.stdout
    assert "not treating this as a missing Databricks object" in result.stdout


@pytest.mark.parametrize("terraform_output", [
    "Error: Cannot import non-existent remote object\n",
    "Error: RESOURCE_DOES_NOT_EXIST: policy missing\n",
    "Error: remote object does not exist\n",
])
def test_provider_missing_object_errors_remain_soft_import_misses(tmp_path, terraform_output):
    data_access = tmp_path / "env" / "data_access"
    data_access.mkdir(parents=True)
    (tmp_path / "env" / "env.auto.tfvars").write_text('governance_mode = "legacy"\n')
    (data_access / "abac.auto.tfvars").write_text(
        'tag_assignments = [{ entity_type = "tables", entity_name = "cat.sch.missing", '
        'tag_key = "k", tag_value = "v" }]\n'
    )
    runner = tmp_path / "terraform-runner"
    runner.write_text(
        "#!/usr/bin/env bash\n"
        "if [ \"$3\" = state ]; then exit 0; fi\n"
        f"printf '%b' {terraform_output!r}; exit 1\n"
    )
    runner.chmod(0o755)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "python3").symlink_to(sys.executable)
    env = os.environ | {"TERRAFORM_RUNNER": str(runner), "PATH": f"{fake_bin}:{os.environ['PATH']}"}

    result = subprocess.run(["bash", SCRIPT, "--tag-assignments-only"], cwd=data_access, env=env,
                            text=True, capture_output=True)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "may not exist in Databricks" in result.stdout


def test_a_missing_optional_import_still_does_not_fail_make_import(tmp_path):
    data_access = tmp_path / "env" / "data_access"
    data_access.mkdir(parents=True)
    (tmp_path / "env" / "env.auto.tfvars").write_text('governance_mode = "legacy"\n')
    (data_access / "abac.auto.tfvars").write_text(
        'tag_assignments = [{ entity_type = "tables", entity_name = "cat.sch.missing", '
        'tag_key = "k", tag_value = "v" }]\n'
    )
    runner = tmp_path / "terraform-runner"
    runner.write_text(
        "#!/usr/bin/env bash\n"
        "if [ \"$3\" = state ]; then exit 0; fi\n"
        "echo 'Error: Cannot import non-existent remote object'; exit 1\n"
    )
    runner.chmod(0o755)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "python3").symlink_to(sys.executable)
    env = os.environ | {"TERRAFORM_RUNNER": str(runner), "PATH": f"{fake_bin}:{os.environ['PATH']}"}

    result = subprocess.run(["bash", SCRIPT, "--tag-assignments-only"], cwd=data_access, env=env,
                            text=True, capture_output=True)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "may not exist in Databricks" in result.stdout


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


def test_run_by_hand_the_runner_gets_envs_dir_from_the_env_dir(tmp_path):
    """make exports ENVS_DIR; by hand terraform_layer.sh would default to shared/envs (the wrong state)."""
    envs = tmp_path / "azure" / "envs"
    data_access = envs / "dev" / "data_access"
    data_access.mkdir(parents=True)
    (envs / "dev" / "env.auto.tfvars").write_text('sql_warehouse_id = "wh"\n')
    (data_access / "abac.auto.tfvars").write_text(STALE_TREATMENT)
    fake = fake_uc.install(tmp_path, LIVE)
    log = tmp_path / "envs_dir.log"
    wrapper = tmp_path / "wrapper.sh"
    wrapper.write_text(f'#!/bin/sh\necho "$ENVS_DIR" >> {log}\nexec "{fake["TERRAFORM_RUNNER"]}" "$@"\n')
    wrapper.chmod(0o755)
    environ = {k: v for k, v in os.environ.items() if k != "ENVS_DIR"} | fake | {"TERRAFORM_RUNNER": str(wrapper)}
    result = subprocess.run(["bash", SCRIPT, "--tag-assignments-only"], cwd=data_access, env=environ,
                            text=True, capture_output=True)
    assert result.returncode == 0, result.stdout + result.stderr
    seen = set(log.read_text().split())
    assert seen == {str(envs)}, seen
