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
    return env


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
        ("business_access_enabled = true\nenable_classification = true\n", "", True, True),
        ("enable_classification = true\n", "-var=business_access_enabled=true", True, True),
        ("enable_classification = true\n", "-var business_access_enabled=true", True, True),
        ("business_access_enabled = true\nenable_classification = true\n",
         "-var=business_access_enabled=false", True, False),
        ("enable_classification = true\n", "", True, False),
        ("business_access_enabled = true\n", "", True, False),
        ("business_access_enabled = true\nenable_classification = true\n", "", False, False),
    ],
)
def test_needs_derive_only_when_opening_access_with_native_classification(
        tmp_path, env, flags, generated, expected):
    (tmp_path / "env.auto.tfvars").write_text(env)
    if generated:
        (tmp_path / "generated").mkdir()
        (tmp_path / "generated" / "abac.auto.tfvars").write_text("tag_assignments = []\n")
    assert cg.needs_derive(tmp_path, flags)[0] is expected


def test_needs_derive_cli_accepts_the_flags_make_passes(tmp_path, capsys):
    (tmp_path / "env.auto.tfvars").write_text("enable_classification = true\n")
    (tmp_path / "generated").mkdir()
    (tmp_path / "generated" / "abac.auto.tfvars").write_text("tag_assignments = []\n")
    assert cg.main(["needs-derive", "--env-dir", str(tmp_path),
                    "--apply-flags=-var=business_access_enabled=true"]) == 0
    assert capsys.readouterr().out == "yes\n"


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
