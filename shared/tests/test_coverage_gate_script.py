"""scripts/coverage_gate.py: the data_access coverage gate Terraform enforces.

Unit tests use a stub layer runner (terraform console) and a stub validator.
The real-Terraform tests at the bottom drive the real layer runner and
`terraform plan` (offline: mock credentials, no state refresh) to show a raw
single-layer run can't grant business SELECT without a current pass.
"""

import base64
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED))

from scripts import coverage_gate as cg  # noqa: E402

RUNNER = SHARED / "scripts" / "terraform_layer.sh"
TABLE = "cat.sch.customers"


def _encoded(inputs: dict) -> str:
    return base64.b64encode(json.dumps(inputs).encode()).decode()


def _inputs(**overrides):
    inputs = {
        "fingerprint": "fp-1",
        "business_access_enabled": True,
        "grant_tables": [TABLE],
        "acknowledged_columns": [],
    }
    inputs.update(overrides)
    return inputs


@pytest.fixture
def env_dir(tmp_path):
    env = tmp_path / "prod"
    (env / "data_access").mkdir(parents=True)
    (env / "data_access" / "abac.auto.tfvars").write_text("tag_assignments = []\n")
    (env / "data_access" / "masking_functions.sql").write_text("-- masks\n")
    (env / "env.auto.tfvars").write_text(f'uc_tables = ["{TABLE}"]\n')
    (env / "auth.auto.tfvars").write_text('databricks_workspace_host = "https://example.invalid"\n')
    _record_refresh(env)
    return env


def _record_refresh(env, mode="ddl"):
    """What derive_assignments.py writes after re-reading live UC."""
    (env / "generated").mkdir(exist_ok=True)
    cg.write_refresh_record(
        env / cg.REFRESH_RELPATH, mode=mode, ddl_path=env / "ddl" / "_fetched.sql",
        config_path=env / "data_access" / "abac.auto.tfvars" if mode == "full" else None,
    )


@pytest.fixture
def stub_runner(tmp_path):
    """Runner answering `console` with the queued inputs (last one repeats)."""
    queue = tmp_path / "console-queue"
    log = tmp_path / "runner.log"
    runner = tmp_path / "runner"
    runner.write_text(
        "#!/bin/sh\n"
        f'printf "%s|%s\\n" "$LAYER_ENV_DIR" "$*" >> "{log}"\n'
        'echo "+ terraform init (stub)"\n'
        f'first=$(head -n 1 "{queue}")\n'
        f'if [ "$(wc -l < "{queue}")" -gt 1 ]; then tail -n +2 "{queue}" > "{queue}.next"; mv "{queue}.next" "{queue}"; fi\n'
        'echo "+ terraform console"\n'
        'echo "$first"\n'
    )
    runner.chmod(0o755)

    def queue_inputs(*answers):
        queue.write_text("".join(f'"{_encoded(a)}"\n' for a in answers))
        return runner, log

    return queue_inputs


@pytest.fixture
def stub_validator(tmp_path, monkeypatch):
    record = tmp_path / "validator.json"
    validator = tmp_path / "validator.py"

    def configure(rc):
        validator.write_text(
            "import json, sys\n"
            "args = sys.argv[1:]\n"
            "context = json.load(open(args[args.index('--exposure-context') + 1]))\n"
            f"json.dump({{'args': args, 'context': context}}, open({str(record)!r}, 'w'))\n"
            f"raise SystemExit({rc})\n"
        )
        monkeypatch.setattr(cg, "VALIDATOR", validator)
        return record

    return configure


def _gate_file(env_dir):
    return env_dir / "data_access" / ".coverage_gate.json"


def _state(env_dir, tables):
    (env_dir / "data_access" / "terraform.tfstate").write_text(json.dumps({
        "version": 4,
        "resources": [{
            "module": "module.data_access", "mode": "managed",
            "type": "databricks_grant", "name": "table_access",
            "instances": [{"index_key": f"{t}|analysts"} for t in tables],
        }],
    }))


# ── needs-derive ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "env, flags, generated, expected",
    [
        ("business_access_enabled = true\nenable_classification = true\n", "", True, "full"),
        ("enable_classification = true\n", "-var=business_access_enabled=true", True, "full"),
        ("enable_classification = true\n", "-var business_access_enabled=true", True, "full"),
        ("business_access_enabled = true\nenable_classification = true\n",
         "-var=business_access_enabled=false", True, "none"),
        ("enable_classification = true\n", "", True, "none"),
        # Opening access without native classification still re-reads the DDL.
        ("business_access_enabled = true\n", "", True, "ddl"),
        ("business_access_enabled = true\nenable_classification = true\n", "", False, "ddl"),
    ],
)
def test_live_refresh_mode_before_opening_access(tmp_path, env, flags, generated, expected):
    (tmp_path / "env.auto.tfvars").write_text(env)
    if generated:
        (tmp_path / "generated").mkdir()
        (tmp_path / "generated" / "abac.auto.tfvars").write_text("tag_assignments = []\n")
    assert cg.needs_derive(tmp_path, flags)[0] == expected


def test_needs_derive_cli_accepts_the_flags_make_passes(tmp_path, capsys):
    (tmp_path / "env.auto.tfvars").write_text("enable_classification = true\n")
    (tmp_path / "generated").mkdir()
    (tmp_path / "generated" / "abac.auto.tfvars").write_text("tag_assignments = []\n")
    assert cg.main(["needs-derive", "--env-dir", str(tmp_path),
                    "--apply-flags=-var=business_access_enabled=true"]) == 0
    assert capsys.readouterr().out == "full\n"


def test_needs_derive_fails_on_unparseable_env(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text("business_access_enabled = = true\n")
    assert cg.main(["needs-derive", "--env-dir", str(tmp_path)]) == 2


def test_console_gets_only_variable_flags():
    assert cg.console_flags(
        "-var=business_access_enabled=true -parallelism=1 -target=x -var-file=a.tfvars -var k=v"
    ) == ["-var=business_access_enabled=true", "-var-file=a.tfvars", "-var", "k=v"]


# ── granted tables ────────────────────────────────────────────────────────────


def test_no_state_means_nothing_is_granted(env_dir):
    assert cg.granted_tables(env_dir / "data_access") == set()


def test_granted_tables_come_from_table_access_instances(env_dir):
    _state(env_dir, [TABLE, "Cat.Sch.Orders"])
    assert cg.granted_tables(env_dir / "data_access") == {TABLE, "cat.sch.orders"}


def test_unreadable_state_fails_closed(env_dir):
    (env_dir / "data_access" / "terraform.tfstate").write_text("{truncated")
    with pytest.raises(cg.GateError, match="cannot read data_access state"):
        cg.granted_tables(env_dir / "data_access")


# ── run ───────────────────────────────────────────────────────────────────────


def test_closed_gate_needs_no_gate_run(env_dir, stub_runner, stub_validator):
    runner, _log = stub_runner(_inputs(business_access_enabled=False))
    record = stub_validator(1)
    assert cg.run_gate(env_dir, "prod", runner, "", False) == 0
    assert not record.exists()
    assert not _gate_file(env_dir).exists()


def test_missing_data_access_config_skips(tmp_path, stub_runner):
    runner, log = stub_runner(_inputs())
    assert cg.run_gate(tmp_path, "prod", runner, "", False) == 0
    assert not log.exists()


def test_pass_records_terraforms_fingerprint_and_first_exposure(env_dir, stub_runner, stub_validator):
    _state(env_dir, ["cat.sch.orders"])
    runner, log = stub_runner(_inputs(
        grant_tables=[TABLE, "cat.sch.orders"], acknowledged_columns=["cat.sch.customers.nickname"],
    ))
    record = stub_validator(0)
    flags = "-var=business_access_enabled=true -parallelism=1"
    assert cg.run_gate(env_dir, "prod", runner, flags, False) == 0

    gate = json.loads(_gate_file(env_dir).read_text())
    assert gate["status"] == "pass"
    assert gate["fingerprint"] == "fp-1"
    refresh = json.loads((env_dir / cg.REFRESH_RELPATH).read_text())
    assert gate["refreshed_at"] == refresh["refreshed_at"]
    assert gate["first_exposure_tables"] == [TABLE]
    assert gate["granted_tables"] == ["cat.sch.orders"]
    seen = json.loads(record.read_text())
    assert seen["context"]["first_exposure_tables"] == [TABLE]
    assert seen["context"]["acknowledged_columns"] == ["cat.sch.customers.nickname"]
    assert seen["context"]["acknowledge_file"] == str(env_dir / "env.auto.tfvars")
    assert seen["args"][:2] == ["--coverage-gate", str(env_dir / "data_access" / "abac.auto.tfvars")]
    assert str(env_dir / "ddl" / "_fetched.sql") in seen["args"]
    # Both console reads use the apply's -var flags (and only those).
    calls = log.read_text().splitlines()
    assert calls == [f"{env_dir / 'data_access'}|data_access prod console -var=business_access_enabled=true"] * 2


def test_validation_failure_records_a_failed_gate(env_dir, stub_runner, stub_validator):
    runner, _log = stub_runner(_inputs())
    stub_validator(1)
    assert cg.run_gate(env_dir, "prod", runner, "", False) == 1
    gate = json.loads(_gate_file(env_dir).read_text())
    assert gate["status"] == "fail"
    assert gate["fingerprint"] == "fp-1"


def test_inputs_changing_during_the_gate_fail_it(env_dir, stub_runner, stub_validator):
    runner, _log = stub_runner(_inputs(), _inputs(fingerprint="fp-2"))
    stub_validator(0)
    assert cg.run_gate(env_dir, "prod", runner, "", False) == 1
    gate = json.loads(_gate_file(env_dir).read_text())
    assert gate["status"] == "fail"
    assert gate["reason"] == "inputs changed while the gate ran"


def test_pass_requires_a_live_refresh(env_dir, stub_runner, stub_validator):
    (env_dir / cg.REFRESH_RELPATH).unlink()
    runner, _log = stub_runner(_inputs())
    stub_validator(0)
    assert cg.run_gate(env_dir, "prod", runner, "", False) == 1
    gate = json.loads(_gate_file(env_dir).read_text())
    assert gate["status"] == "fail"
    assert "no live refresh" in gate["reason"]
    assert "refreshed_at" not in gate


def test_refresh_must_match_the_ddl_being_gated(env_dir):
    (env_dir / "ddl").mkdir()
    (env_dir / "ddl" / "_fetched.sql").write_text("CREATE TABLE cat.sch.customers (\n  id BIGINT\n);\n")
    _record_refresh(env_dir)
    tfvars = env_dir / "data_access" / "abac.auto.tfvars"
    assert cg.live_refresh(env_dir, tfvars)[0] is not None
    # A DDL snapshot nobody re-read from UC (edited, copied, stale) is not live.
    (env_dir / "ddl" / "_fetched.sql").write_text("CREATE TABLE cat.sch.customers (\n  ssn STRING\n);\n")
    refreshed_at, reason = cg.live_refresh(env_dir, tfvars)
    assert refreshed_at is None
    assert "changed since the last live refresh" in reason


def test_full_refresh_must_match_the_split_tags(env_dir):
    tfvars = env_dir / "data_access" / "abac.auto.tfvars"
    tfvars.write_text(
        'tag_assignments = [{ entity_type = "columns", entity_name = "cat.sch.customers.email", '
        'tag_key = "gr_treatment", tag_value = "email_partial" }]\n'
    )
    _record_refresh(env_dir, mode="full")
    assert cg.live_refresh(env_dir, tfvars) == (
        json.loads((env_dir / cg.REFRESH_RELPATH).read_text())["refreshed_at"], "full",
    )
    # A live class.* tag changed (or the split predates the refresh).
    tfvars.write_text("tag_assignments = []\n")
    refreshed_at, reason = cg.live_refresh(env_dir, tfvars)
    assert refreshed_at is None
    assert "tag_assignments don't match the last live refresh" in reason


@pytest.mark.parametrize("record", ["{truncated", '{"mode": "full"}', '{"mode": "later", "refreshed_at": "2026-01-01T00:00:00Z"}',
                                    '{"mode": "ddl", "refreshed_at": "yesterday"}'])
def test_malformed_refresh_records_fail_closed(env_dir, record):
    (env_dir / cg.REFRESH_RELPATH).write_text(record)
    assert cg.live_refresh(env_dir, env_dir / "data_access" / "abac.auto.tfvars")[0] is None


def test_console_errors_fail_the_gate(env_dir, tmp_path):
    runner = tmp_path / "broken-runner"
    runner.write_text("#!/bin/sh\necho 'Error: Invalid value' >&2\nexit 1\n")
    runner.chmod(0o755)
    code = cg.main(["run", "--env-dir", str(env_dir), "--env-name", "prod", "--runner", str(runner)])
    assert code == 2
    assert not _gate_file(env_dir).exists()


def test_garbled_console_output_fails_the_gate(env_dir, tmp_path):
    runner = tmp_path / "chatty-runner"
    runner.write_text("#!/bin/sh\necho '(known after apply)'\n")
    runner.chmod(0o755)
    with pytest.raises(cg.GateError, match="unexpected terraform console output"):
        cg.query_inputs(runner, "prod", env_dir / "data_access", [])


# ── Makefile wiring ───────────────────────────────────────────────────────────


def _dry_run(target, *args):
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL")}
    result = subprocess.run(
        ["make", "-n", target, *args], cwd=SHARED.parent / "aws",
        text=True, capture_output=True, env=env,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


@pytest.mark.parametrize("target", ["apply", "apply-governance"])
def test_apply_paths_derive_before_promote_unless_already_derived(target):
    recipe = _dry_run(target, "ENV=dev")
    assert recipe.index("_derive-before-exposure") < recipe.index("promote;")
    assert 'if [ -z "" ]; then' in recipe
    assert 'if [ -z "1" ]; then' in _dry_run(target, "ENV=dev", "_EXPOSURE_DERIVED=1")


def test_data_access_layer_gates_before_plan_and_apply():
    source = (SHARED / "Makefile.shared").read_text()
    for target, command in (("_apply-layer:", "apply -parallelism=1"), ("_plan-layer:", '"$$target_env" plan')):
        body = source[source.index(target):]
        body = body[:body.index("\n\n")]
        assert body.index("$(_COVERAGE_GATE) run") < body.index(command), target
    apply_layer = source[source.index("_apply-layer:"):]
    assert '--apply-flags="$$apply_flags"' in apply_layer[:apply_layer.index("\n\n")]


# ── real Terraform: a raw single-layer run can't bypass the gate ─────────────

needs_terraform = pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")

ACCOUNT_ABAC = """
groups = { analysts = {} }
tag_policies = [
  { key = "gr_treatment", description = "treatments", values = ["email_partial"] },
]
"""

DATA_ACCESS_ABAC = f"""
groups = {{ analysts = {{}} }}
tag_assignments = [
  {{ entity_type = "columns", entity_name = "{TABLE}.email", tag_key = "gr_treatment", tag_value = "email_partial" }},
]
fgac_policies = [
  {{
    name             = "gr_mask_cat_email_partial"
    policy_type      = "POLICY_TYPE_COLUMN_MASK"
    catalog          = "cat"
    to_principals    = ["analysts"]
    comment          = "GenieRails treatment email_partial"
    match_condition  = "hasTagValue('gr_treatment', 'email_partial')"
    match_alias      = "gr_treatment_email_partial"
    function_name    = "mask_email"
    function_catalog = "cat"
    function_schema  = "sch"
  }},
]
"""

DDL = f"CREATE TABLE {TABLE} (\n  id BIGINT,\n  email STRING\n);\n"


@pytest.fixture(scope="module")
def plugin_cache(tmp_path_factory):
    return tmp_path_factory.mktemp("tf-plugin-cache")


@pytest.fixture
def live_like_env(tmp_path, plugin_cache, monkeypatch):
    envs = tmp_path / "aws" / "envs"
    env = envs / "prod"
    layer = env / "data_access"
    layer.mkdir(parents=True)
    (envs / "account").mkdir()
    (envs / "account" / "abac.auto.tfvars").write_text(ACCOUNT_ABAC)
    (env / "ddl").mkdir()
    (env / "ddl" / "_fetched.sql").write_text(DDL)
    (env / "auth.auto.tfvars").write_text(
        'databricks_account_id = "account"\n'
        'databricks_client_id = "service-principal"\n'
        'databricks_client_secret = "not-a-secret"\n'
        'databricks_workspace_id = "123"\n'
        'databricks_workspace_host = "https://example.invalid"\n'
    )
    (env / "env.auto.tfvars").write_text(
        f'uc_tables = ["{TABLE}"]\nsql_warehouse_id = "warehouse"\nbusiness_access_enabled = true\n'
    )
    os.symlink("../auth.auto.tfvars", layer / "auth.auto.tfvars")
    os.symlink("../env.auto.tfvars", layer / "env.auto.tfvars")
    (layer / "abac.auto.tfvars").write_text(DATA_ACCESS_ABAC)
    (layer / "masking_functions.sql").write_text(
        "CREATE OR REPLACE FUNCTION mask_email(email STRING)\nRETURNS STRING\nRETURN '***';\n"
    )
    monkeypatch.setenv("TF_PLUGIN_CACHE_DIR", str(plugin_cache))
    monkeypatch.setenv("TF_IN_AUTOMATION", "1")
    _record_refresh(env)
    return env


def _raw_plan(env, *flags):
    """terraform_layer.sh directly, the way a user would bypass make."""
    return subprocess.run(
        [str(RUNNER), "data_access", "prod", "plan", "-refresh=false", "-lock=false",
         "-input=false", "-no-color", *flags],
        env={**os.environ, "LAYER_ENV_DIR": str(env / "data_access")},
        text=True, capture_output=True,
    )


def _gate(env, *flags):
    return subprocess.run(
        [sys.executable, str(SHARED / "scripts" / "coverage_gate.py"), "run",
         "--env-dir", str(env), "--env-name", "prod", *flags],
        text=True, capture_output=True,
    )


@needs_terraform
def test_raw_layer_plan_cannot_grant_without_a_current_pass(live_like_env):
    env = live_like_env
    missing = _raw_plan(env)
    assert missing.returncode != 0
    assert "Coverage gate missing" in missing.stderr
    assert "Business SELECT grants are blocked" in missing.stderr

    gated = _gate(env)
    assert gated.returncode == 0, gated.stdout + gated.stderr
    result = json.loads(_gate_file(env).read_text())
    assert result["status"] == "pass"
    assert result["first_exposure_tables"] == [TABLE]
    planned = _raw_plan(env)
    assert planned.returncode == 0, planned.stdout + planned.stderr
    assert f'databricks_grant.table_access["{TABLE}|analysts"] will be created' in planned.stdout

    # -var overrides change what Terraform would apply, so the pass is stale.
    override = _raw_plan(env, "-var=tag_assignments=[]")
    assert override.returncode != 0
    assert "Coverage gate stale" in override.stderr

    # So does editing the config after the gate.
    abac = env / "data_access" / "abac.auto.tfvars"
    abac.write_text(DATA_ACCESS_ABAC.replace("email_partial\" }", "email_partial\" }") + "\n# comment only\n")
    assert _raw_plan(env).returncode == 0, "a comment-only edit must not invalidate the gate"
    abac.write_text(DATA_ACCESS_ABAC.replace('to_principals    = ["analysts"]', "to_principals    = []"))
    edited = _raw_plan(env)
    assert edited.returncode != 0
    assert "Coverage gate stale" in edited.stderr


@needs_terraform
def test_first_exposure_failure_blocks_the_plan_until_acknowledged(live_like_env):
    env = live_like_env
    (env / "ddl" / "_fetched.sql").write_text(DDL.replace("email STRING", "email STRING,\n  ssn STRING"))
    _record_refresh(env)  # the live refresh read the new column
    failed = _gate(env)
    assert failed.returncode == 1
    assert "first exposure blocked" in failed.stdout
    assert f"{TABLE}.ssn (looks like: ssn)" in failed.stdout
    assert json.loads(_gate_file(env).read_text())["status"] == "fail"
    blocked = _raw_plan(env)
    assert blocked.returncode != 0
    assert "Coverage gate failed" in blocked.stderr

    with (env / "env.auto.tfvars").open("a") as handle:
        handle.write(f'coverage_acknowledged_columns = ["{TABLE}.ssn"]\n')
    # The acknowledgement is itself a gate input: the failed result is now
    # stale, and the re-run passes.
    assert "Coverage gate stale" in _raw_plan(env).stderr
    acknowledged = _gate(env)
    assert acknowledged.returncode == 0, acknowledged.stdout + acknowledged.stderr
    assert _raw_plan(env).returncode == 0


@needs_terraform
def test_already_granted_table_keeps_the_warning(live_like_env):
    env = live_like_env
    (env / "ddl" / "_fetched.sql").write_text(DDL.replace("email STRING", "email STRING,\n  ssn STRING"))
    _record_refresh(env)  # the live refresh read the new column
    _state(env, [TABLE])
    granted = _gate(env, "--verbose")
    assert granted.returncode == 0, granted.stdout + granted.stderr
    assert "COVERAGE GATE (non-blocking)" in granted.stdout
    assert f"{TABLE}.ssn" in granted.stdout
    assert json.loads(_gate_file(env).read_text())["first_exposure_tables"] == []


@needs_terraform
def test_gate_honours_the_apply_flags_like_terraform(live_like_env):
    env = live_like_env
    closed = _gate(env, "--apply-flags=-var=business_access_enabled=false")
    assert closed.returncode == 0
    assert "not required" in closed.stdout
    assert not _gate_file(env).exists()
    assert _raw_plan(env, "-var=business_access_enabled=false").returncode == 0


def _apply_layer(env_dir, runner, apply_flags):
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "APPLY_FLAGS")}
    return subprocess.run(
        ["make", "--no-print-directory", "_apply-layer", "LAYER=data_access", "TARGET_ENV=prod",
         f"LAYER_ENV_DIR={env_dir / 'data_access'}", f"ROOT_RUNNER={runner}",
         f"APPLY_FLAGS={apply_flags}"],
        cwd=SHARED.parent / "aws", text=True, capture_output=True, env=env,
    )


def test_make_never_applies_data_access_when_the_gate_fails(env_dir, stub_runner):
    # Open gate; the bare fixture config fails validation.
    runner, log = stub_runner(_inputs())
    result = _apply_layer(env_dir, runner, "-var=business_access_enabled=true")
    assert result.returncode != 0
    assert "coverage gate FAILED" in result.stderr
    calls = log.read_text().splitlines()
    assert calls and all(" console " in f" {call.split('|', 1)[1]} " for call in calls), calls
    assert json.loads(_gate_file(env_dir).read_text())["status"] == "fail"


def test_make_applies_data_access_after_the_gate(env_dir, stub_runner):
    runner, log = stub_runner(_inputs(business_access_enabled=False))
    result = _apply_layer(env_dir, runner, "")
    assert result.returncode == 0, result.stdout + result.stderr
    commands = [call.split("|", 1)[1].split()[2] for call in log.read_text().splitlines()]
    assert commands[0] == "console"
    assert "apply" in commands


def test_ddl_change_reapplies_data_access_so_genie_sees_the_new_gate(env_dir, stub_runner):
    # The DDL is a gate input: if a DDL-only change skipped the apply, the gate
    # result would move on while the state keeps the old fingerprint, and the
    # workspace layer would block CAN_RUN with nothing left to apply.
    runner, log = stub_runner(_inputs(business_access_enabled=False))
    (env_dir / "ddl").mkdir()
    ddl = env_dir / "ddl" / "_fetched.sql"
    ddl.write_text("CREATE TABLE cat.sch.customers (\n  id BIGINT\n);\n")

    def applies():
        result = _apply_layer(env_dir, runner, "")
        assert result.returncode == 0, result.stdout + result.stderr
        return "inputs unchanged" not in result.stdout

    assert applies()
    assert not applies()
    ddl.write_text("CREATE TABLE cat.sch.customers (\n  id BIGINT,\n  email STRING\n);\n")
    assert applies()


@needs_terraform
@pytest.mark.parametrize("config", [
    "roots/data_access", "roots/workspace", "modules/data_access", "modules/workspace",
    "modules/coverage_gate_check",
])
def test_terraform_test_suites_pass(config, tmp_path, plugin_cache):
    """The gate's Terraform-native tests (and the existing ones) stay green."""
    directory = SHARED / config
    env = {**os.environ, "TF_DATA_DIR": str(tmp_path / ".terraform"),
           "TF_PLUGIN_CACHE_DIR": str(plugin_cache), "TF_IN_AUTOMATION": "1"}
    init = subprocess.run(["terraform", "init", "-backend=false", "-input=false"],
                          cwd=directory, env=env, text=True, capture_output=True)
    assert init.returncode == 0, init.stdout + init.stderr
    result = subprocess.run(["terraform", "test", "-no-color"],
                            cwd=directory, env=env, text=True, capture_output=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "0 failed" in result.stdout


# ── make plan re-reads live UC before it gates (review blocker 1) ────────────

FAKE_DERIVE = '''\
"""Stand-in for derive_assignments.py: "live UC" is the file $LIVE_DDL."""
import argparse, os, shutil, sys
from pathlib import Path
sys.path.insert(0, {shared!r})
from scripts.coverage_gate import write_refresh_record
parser = argparse.ArgumentParser()
for flag in ("--auth-file", "--env-file", "--config", "--write-ddl", "--refresh-record"):
    parser.add_argument(flag)
parser.add_argument("--ddl-only", action="store_true")
args = parser.parse_args()
with open(os.environ["DERIVE_LOG"], "a") as log:
    log.write(" ".join(sys.argv[1:]) + "\\n")
Path(args.refresh_record).unlink(missing_ok=True)
if os.environ.get("LIVE_UC_DOWN"):
    print("ERROR: Could not fetch DDL for the governed footprint", file=sys.stderr)
    raise SystemExit(1)
shutil.copy(os.environ["LIVE_DDL"], args.write_ddl)
write_refresh_record(Path(args.refresh_record), mode="ddl" if args.ddl_only else "full",
                     ddl_path=Path(args.write_ddl), config_path=None if args.ddl_only else Path(args.config))
'''


@pytest.fixture
def live_uc(live_like_env, tmp_path, monkeypatch):
    """make plan against live_like_env, with a scriptable live UC."""
    fake = tmp_path / "fake_derive.py"
    fake.write_text(FAKE_DERIVE.format(shared=str(SHARED)))
    live_ddl = tmp_path / "live.sql"
    live_ddl.write_text(DDL)
    monkeypatch.setenv("LIVE_DDL", str(live_ddl))
    monkeypatch.setenv("DERIVE_LOG", str(tmp_path / "derive.log"))
    cloud_root = live_like_env.parents[1]

    def plan():
        env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "APPLY_FLAGS")}
        env["TF_CLI_ARGS_plan"] = "-no-color"
        return subprocess.run(
            ["make", "--no-print-directory", "plan", "ENV=prod", f"CLOUD_ROOT={cloud_root}",
             f"SHARED_ROOT={SHARED}", f"DERIVE_ASSIGNMENTS_SCRIPT={fake}"],
            cwd=SHARED.parent / "aws", text=True, capture_output=True, env=env,
        )

    return live_like_env, live_ddl, plan


@needs_terraform
def test_make_plan_refreshes_live_metadata_and_blocks_a_new_untagged_column(live_uc):
    env, live_ddl, plan = live_uc
    first = plan()
    assert first.returncode == 0, first.stdout + first.stderr
    assert "=== Refresh Live DDL (prod) ===" in first.stdout
    assert f'databricks_grant.table_access["{TABLE}|analysts"] will be created' in first.stdout
    passed = json.loads(_gate_file(env).read_text())
    assert passed["status"] == "pass" and passed["refreshed_at"]

    # Someone adds an untagged ssn column in UC. The local snapshot still
    # says email only, and it passed a moment ago.
    live_ddl.write_text(DDL.replace("email STRING", "email STRING,\n  ssn STRING"))
    second = plan()
    assert second.returncode != 0
    assert "first exposure blocked" in second.stdout
    assert f"{TABLE}.ssn (looks like: ssn)" in second.stdout
    assert "=== Terraform Plan (data_access:prod) ===" not in second.stdout
    assert "ssn STRING" in (env / "ddl" / "_fetched.sql").read_text()
    assert json.loads(_gate_file(env).read_text())["status"] == "fail"
    # ... and raw Terraform can't fall back on the earlier pass.
    raw = _raw_plan(env)
    assert raw.returncode != 0
    assert "Coverage gate failed" in raw.stderr


@needs_terraform
def test_failed_live_refresh_leaves_no_usable_pass(live_uc, monkeypatch):
    env, _live_ddl, plan = live_uc
    assert plan().returncode == 0
    assert _raw_plan(env).returncode == 0

    monkeypatch.setenv("LIVE_UC_DOWN", "1")
    down = plan()
    assert down.returncode != 0
    assert "Could not fetch DDL" in down.stderr
    assert "=== Terraform Plan (data_access:prod) ===" not in down.stdout
    assert not (env / cg.REFRESH_RELPATH).exists()
    result = json.loads(_gate_file(env).read_text())
    assert result["status"] == "fail" and "refreshed_at" not in result
    raw = _raw_plan(env)
    assert raw.returncode != 0
    assert "Coverage gate failed" in raw.stderr


@needs_terraform
def test_gate_run_without_a_refresh_for_the_current_ddl_fails_closed(live_like_env):
    env = live_like_env
    # The DDL snapshot changes without anyone re-reading UC.
    (env / "ddl" / "_fetched.sql").write_text(DDL.replace("id BIGINT", "id BIGINT,\n  note STRING"))
    gated = _gate(env)
    assert gated.returncode == 1
    assert "changed since the last live refresh" in gated.stderr
    assert json.loads(_gate_file(env).read_text())["status"] == "fail"


# ── Genie CAN_RUN needs the space's grants in data_access state (blocker 2) ──

_WS_STATE = (
    'jsonencode({{ version = 4, outputs = {{ coverage_gate = {{ value = {{ business_access_enabled = true, '
    'fingerprint = "applied", status = "pass", max_age = "6h", table_grant_count = {count} }} }}, '
    'table_grant_resource_keys = {{ value = {keys} }} }} }})'
)

_WS_TEST = '''
mock_provider "databricks" {{
  alias = "account"
}}
mock_provider "databricks" {{
  alias = "workspace"
}}
mock_provider "null" {{}}

override_data {{
  target = module.workspace.data.databricks_group.existing
  values = {{ id = 123 }}
}}

run "state" {{
  module {{
    source = "../data_access/tests/file_writer"
  }}
  variables {{
    files = {{
      "{env}/data_access/terraform.tfstate"   = {state}
      "{env}/data_access/.coverage_gate.json" = jsonencode({{ status = "pass", fingerprint = "applied", refreshed_at = "{refreshed}" }})
    }}
  }}
}}

run "can_run" {{
  command = plan
  variables {{
    env_dir                   = "{env}"
    databricks_account_id     = "account"
    databricks_client_id      = "service-principal"
    databricks_client_secret  = "secret"
    databricks_workspace_id   = "123"
    databricks_workspace_host = "https://example.invalid"
    sql_warehouse_id          = "warehouse"
    business_access_enabled   = true
    groups                    = {{ analysts = {{}} }}
    genie_spaces              = [{{ name = "Sales", genie_space_id = "space-1", uc_tables = ["cat.sch.customers"] }}]
    genie_space_configs       = {{ Sales = {{ acl_groups = {acl} }} }}
  }}
}}
'''


@needs_terraform
@pytest.mark.parametrize(
    "count, keys, acl, refused, refreshed",
    [
        # Matching pass, but the apply left zero table grants.
        (0, "[]", '["analysts"]', "the data_access layer has no business table grants in place", "@NOW@"),
        (0, "[]", "[]", None, "@NOW@"),
        # Grants exist, but not for this space's table and group.
        (2, '["cat.sch.customers|auditors", "cat.sch.orders|analysts"]', '["analysts"]',
         "lacks the SELECT grants its CAN_RUN groups need (cat.sch.customers|analysts)", "@NOW@"),
        (1, '["cat.sch.customers|analysts"]', '["analysts"]', None, "@NOW@"),
        # Everything in place, but the live refresh behind the pass is old
        # (raw workspace terraform, or apply-genie without its refresh).
        (1, '["cat.sch.customers|analysts"]', '["analysts"]',
         "older than coverage_gate_max_age (6h)", "2000-01-01T00:00:00Z"),
        (1, '["cat.sch.customers|analysts"]', "[]", None, "2000-01-01T00:00:00Z"),
        (1, '["cat.sch.customers|analysts"]', '["analysts"]',
         "records no live refresh", "2999-01-01T00:00:00Z"),
    ],
)
def test_workspace_root_refuses_can_run_without_the_spaces_grants(
        tmp_path, plugin_cache, count, keys, acl, refused, refreshed):
    root = SHARED / "roots" / "workspace"
    name = f"refusal-{tmp_path.name}"
    test_dir = root / "tests" / ".tmp" / name
    test_dir.mkdir(parents=True)
    try:
        (test_dir / "can_run.tftest.hcl").write_text(_WS_TEST.format(
            env=f"tests/.tmp/{name}/env", state=_WS_STATE.format(count=count, keys=keys), acl=acl,
            refreshed=refreshed,
        ))
        env = {**os.environ, "TF_DATA_DIR": str(tmp_path / ".terraform"),
               "TF_PLUGIN_CACHE_DIR": str(plugin_cache), "TF_IN_AUTOMATION": "1"}
        relative = f"tests/.tmp/{name}"
        init = subprocess.run(["terraform", "init", "-backend=false", f"-test-directory={relative}"],
                              cwd=root, env=env, text=True, capture_output=True)
        assert init.returncode == 0, init.stdout + init.stderr
        result = subprocess.run(["terraform", "test", "-no-color", f"-test-directory={relative}"],
                                cwd=root, env=env, text=True, capture_output=True)
        output = " ".join((result.stdout + result.stderr).split())
        if refused:
            assert result.returncode != 0
            assert "Resource precondition failed" in output
            assert "Genie CAN_RUN for Sales is blocked" in output
            assert refused in output
        else:
            assert result.returncode == 0, output
    finally:
        shutil.rmtree(test_dir, ignore_errors=True)


def test_max_age_ceiling_is_the_same_everywhere():
    """The shared check is the authority; the variable validation only fails early."""
    check = (SHARED / "modules" / "coverage_gate_check" / "main.tf").read_text()
    variables = (SHARED / "modules" / "data_access" / "variables.tf").read_text()
    assert 'max_age_ceiling = "24h"' in check
    validation = variables[variables.index('variable "coverage_gate_max_age"'):]
    validation = validation[:validation.index("\n}\n")]
    assert 'timeadd("2000-01-01T00:00:00Z", "24h")' in validation
    assert "at most 24h" in validation
    # Both layers judge results with the shared module, not their own copy.
    for path in ("modules/data_access/main.tf", "roots/workspace/main.tf"):
        source = (SHARED / path).read_text()
        assert 'modules/coverage_gate_check"' in source or '"../coverage_gate_check"' in source, path
        assert "plantimestamp()" not in source, path


def test_apply_genie_refreshes_and_regates_before_the_workspace_apply():
    source = (SHARED / "Makefile.shared").read_text()
    body = source[source.index("\napply-genie:"):]
    body = body[:body.index("\n\n")]
    derive = body.index("_derive-before-exposure")
    gate = body.index("$(_COVERAGE_GATE) run")
    workspace = body.index("_apply-layer LAYER=workspace")
    assert derive < gate < workspace
    assert '--apply-flags="$(APPLY_FLAGS)"' in body
    assert "LAYER=data_access" not in body


def test_make_apply_genie_refreshes_and_never_opens_can_run_on_a_failed_gate(env_dir, stub_runner, tmp_path, monkeypatch):
    (env_dir / "env.auto.tfvars").write_text(f'uc_tables = ["{TABLE}"]\nbusiness_access_enabled = true\n')
    (env_dir / "abac.auto.tfvars").write_text("# workspace layer present\n")
    fake = tmp_path / "fake_derive.py"
    fake.write_text(FAKE_DERIVE.format(shared=str(SHARED)))
    live_ddl = tmp_path / "live.sql"
    live_ddl.write_text(DDL)
    monkeypatch.setenv("LIVE_DDL", str(live_ddl))
    monkeypatch.setenv("DERIVE_LOG", str(tmp_path / "derive.log"))
    runner, log = stub_runner(_inputs())
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "APPLY_FLAGS")}
    result = subprocess.run(
        ["make", "--no-print-directory", "apply-genie", "ENV=prod", f"ENV_DIR={env_dir}",
         f"ACCOUNT_ENV_DIR={tmp_path / 'account'}", f"ROOT_RUNNER={runner}",
         f"DERIVE_ASSIGNMENTS_SCRIPT={fake}"],
        cwd=SHARED.parent / "aws", text=True, capture_output=True, env=env,
    )
    assert result.returncode != 0
    # The live refresh ran (DDL-only: no native classification here) ...
    assert "--ddl-only" in (tmp_path / "derive.log").read_text()
    # ... then the gate, which fails on the bare fixture config ...
    assert "coverage gate FAILED for data_access:prod" in result.stderr
    # ... so the workspace layer was never planned or applied.
    calls = [call.split("|", 1)[1] for call in log.read_text().splitlines()]
    assert calls and all(call.startswith("data_access prod console") for call in calls), calls
