"""make wiring of staged policy changes: deterministic data_access applies go through the controller."""

import json
import shutil
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

from tests import script_stubs

from scripts import environment_lock

ROOT = Path(__file__).parents[2]


def _env(staged=None, gate=None, sticky=None):
    environ = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "APPLY_FLAGS")}
    if staged:
        # Stubbed below make: the controller, gate and sticky helpers can't be overridden.
        bin_dir = script_stubs.install(Path(staged).parent / "stub-bin", {
            "staged_policy_apply.py": str(staged), "coverage_gate.py": str(gate), "sticky_governance.py": str(sticky)})
        environ["PATH"] = f"{bin_dir}:{environ['PATH']}"
    return environ


def _stub(path: Path, log: Path, rc: int = 0) -> Path:
    path.write_text(f"#!{sys.executable}\nimport json, os, sys\n"
                    f"open({str(log)!r}, 'a').write(json.dumps([os.path.basename(sys.argv[0])] + sys.argv[1:]) + '\\n')\n"
                    f"sys.exit({rc})\n")
    path.chmod(0o755)
    return path


def _env_dir(tmp_path: Path, mode: str) -> Path:
    env_dir = tmp_path / "envs" / "dev"
    (env_dir / "data_access").mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text(f'governance_mode = "{mode}"\n' if mode else 'genie_space_id = "x"\n')
    (env_dir / "data_access" / "abac.auto.tfvars").write_text("fgac_policies = []\n")
    return env_dir


def _apply_layer(tmp_path: Path, mode: str, *extra: str, cloud: str = "aws"):
    env_dir = _env_dir(tmp_path, mode)
    log = tmp_path / "calls"
    stubs = {name: _stub(tmp_path / name, log) for name in ("runner", "staged", "gate", "sticky", "make")}
    with _held_as_rehearse(env_dir):
        result = _run_apply_layer(env_dir, stubs, extra, cloud)
    calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
    return result, calls, env_dir


@contextmanager
def _held_as_rehearse(env_dir: Path):
    """_apply-layer runs inside make rehearse, under its env lock (release_gate.py apply-guard)."""
    ok, message, nonce = environment_lock.acquire(env_dir, os.getpid(), "rehearse")
    assert ok, message
    os.environ[environment_lock.nonce_var(env_dir)] = nonce  # as make's _ENV_LOCK exports it
    try:
        yield
    finally:
        os.environ.pop(environment_lock.nonce_var(env_dir), None)
        environment_lock.release_lock(env_dir, os.getpid())


def _run_apply_layer(env_dir, stubs, extra, cloud):
    return subprocess.run(
        ["make", "--no-print-directory", "_apply-layer", "LAYER=data_access", "TARGET_ENV=dev", "ENV=dev",
         f"LAYER_ENV_DIR={env_dir / 'data_access'}", f"ENV_DIR={env_dir}", f"ROOT_RUNNER={stubs['runner']}",
         f"MAKE={stubs['make']}", "IMPORT_EXISTING_SCRIPT=true", "FORCE_APPLY=1", *extra],
        cwd=ROOT / cloud, text=True, capture_output=True, env=_env(stubs["staged"], stubs["gate"], stubs["sticky"]))


def _ci_allowed_stubs(tmp_path: Path) -> dict[str, str]:
    apply = tmp_path / "ci-apply"
    apply.write_text("#!/bin/sh\necho genierails-ci-guard:v1:allowed\n")
    apply.chmod(0o755)
    status = tmp_path / "ci-status"
    status.write_text("#!/bin/sh\necho genierails-ci-status:v1:not-ci\n")
    status.chmod(0o755)
    return {"ci_guard.py apply": str(apply), "ci_guard.py ci-status": str(status),
            "genie_space_placeholder.py": "true", "scheduled_governance_scope.py": "true",
            "auth_tfvars.py": "true"}


def test_make_import_returns_the_data_access_failure_after_workspace_import_runs(tmp_path):
    shared = tmp_path / "shared"
    (shared / "scripts").mkdir(parents=True)
    (shared / "Makefile.shared").write_text((ROOT / "shared" / "Makefile.shared").read_text())
    (shared / "env.auto.tfvars.example").write_text((ROOT / "shared" / "env.auto.tfvars.example").read_text())
    (shared / "auth.auto.tfvars.example").write_text((ROOT / "shared" / "auth.auto.tfvars.example").read_text())
    log = tmp_path / "imports.jsonl"
    importer = shared / "scripts" / "import_existing.sh"
    importer.write_text(f"#!/bin/sh\n"
                        f"printf '%s\\n' \"$PWD\" >> {str(log)!r}\n"
                        "case \"$PWD\" in */data_access) exit 1 ;; *) exit 0 ;; esac\n")
    importer.chmod(0o755)
    env_dir = tmp_path / "envs" / "dev"
    (env_dir / "data_access").mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text('governance_mode = "legacy"\n')
    (tmp_path / "envs" / "account").mkdir(parents=True)
    bin_dir = script_stubs.install(tmp_path / "stub-bin", _ci_allowed_stubs(tmp_path))
    env = script_stubs.without_ci(os.environ) | {"PATH": f"{bin_dir}:{os.environ['PATH']}"}

    result = subprocess.run(
        ["make", "--no-print-directory", "import", "ENV=dev", f"ENV_DIR={env_dir}",
         f"ACCOUNT_ENV_DIR={tmp_path / 'envs' / 'account'}", f"SHARED_ROOT={shared}"],
        cwd=ROOT / "aws", text=True, capture_output=True, env=env)

    assert result.returncode != 0, result.stdout + result.stderr
    calls = log.read_text().splitlines()
    assert calls == [str(env_dir / "data_access"), str(env_dir)]


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_deterministic_data_access_apply_runs_only_through_the_staged_controller(tmp_path, cloud):
    result, calls, env_dir = _apply_layer(tmp_path, "deterministic", cloud=cloud)
    assert result.returncode == 0, result.stdout + result.stderr
    staged = [c for c in calls if c[0] == "staged"]
    assert len(staged) == 1 and staged[0][1:4] == ["run", "--env-dir", str(env_dir)]
    verify = staged[0][staged[0].index("--verify-access-cmd") + 1]
    assert "_verify-access-gate" in verify and "VERIFY_REQUIRE_MASKS=1" in verify
    assert 'VERIFY_FROM_TFVARS="$GENIERAILS_VERIFY_TFVARS"' in verify
    assert 'VERIFY_ENV_FILE="$GENIERAILS_VERIFY_ENV_FILE"' in verify
    # Terraform never applies the data_access layer in one shot.
    assert not [c for c in calls if c[0] == "runner" and "apply" in c]


def test_the_staged_controller_gets_the_verify_spec_to_check_before_create(tmp_path):
    result, calls, _ = _apply_layer(tmp_path, "deterministic", "VERIFY_SPEC=/specs/s.json", "VERIFY_KEY_COLUMN=id",
                                    "ACCOUNT_ENV_DIR=/acct")
    assert result.returncode == 0, result.stdout + result.stderr
    staged = [c for c in calls if c[0] == "staged"][0]
    flags = {staged[i]: staged[i + 1] for i in range(len(staged) - 1) if staged[i].startswith("--")}
    assert flags["--verify-spec"] == "/specs/s.json" and flags["--verify-key"] == "id"
    assert flags["--account-tfvars"] == "/acct/abac.auto.tfvars"


def test_every_staged_controller_apply_and_plan_gets_the_verify_spec():
    makefile = (ROOT / "shared" / "Makefile.shared").read_text()
    for command in ("run", "apply-saved", "plan"):
        lines = [line for line in makefile.splitlines() if f"$(_STAGED_POLICY) {command} " in line]
        assert lines and all("$(_STAGED_SPEC_FLAGS)" in line for line in lines), (command, lines)


def test_legacy_data_access_apply_is_unchanged(tmp_path):
    for mode in ("legacy", ""):
        result, calls, _ = _apply_layer(tmp_path / (mode or "unset"), mode)
        assert result.returncode == 0, result.stdout + result.stderr
        assert not [c for c in calls if c[0] == "staged"]
        assert ["runner", "data_access", "dev", "apply", "-parallelism=1", "-auto-approve"] in calls


def test_deterministic_apply_refuses_apply_flags(tmp_path):
    result, calls, _ = _apply_layer(tmp_path, "deterministic", "APPLY_FLAGS=-target=x")
    assert result.returncode != 0
    assert "APPLY_FLAGS" in result.stderr and not [c for c in calls if c[0] in ("staged", "runner")]


def test_a_failed_staged_apply_fails_the_layer(tmp_path):
    env_dir = _env_dir(tmp_path, "deterministic")
    log = tmp_path / "calls"
    stubs = {name: _stub(tmp_path / name, log) for name in ("runner", "gate", "sticky", "make")}
    staged = _stub(tmp_path / "staged", log, rc=1)
    with _held_as_rehearse(env_dir):
        result = subprocess.run(
            ["make", "--no-print-directory", "_apply-layer", "LAYER=data_access", "TARGET_ENV=dev", "ENV=dev",
             f"LAYER_ENV_DIR={env_dir / 'data_access'}", f"ENV_DIR={env_dir}", f"ROOT_RUNNER={stubs['runner']}",
             f"MAKE={stubs['make']}", "IMPORT_EXISTING_SCRIPT=true"],
            cwd=ROOT / "aws", text=True, capture_output=True, env=_env(staged, stubs["gate"], stubs["sticky"]))
    assert result.returncode != 0
    assert not (env_dir / "data_access" / ".data_access.apply.sha").exists()


def test_data_access_apply_stops_on_any_import_failure(tmp_path):
    env_dir = _env_dir(tmp_path, "deterministic")
    log = tmp_path / "calls"
    stubs = {name: _stub(tmp_path / name, log) for name in ("runner", "staged", "gate", "sticky", "make")}
    importer = _stub(tmp_path / "import-existing", log, rc=1)
    with _held_as_rehearse(env_dir):
        result = subprocess.run(
            ["make", "--no-print-directory", "_apply-layer", "LAYER=data_access", "TARGET_ENV=dev", "ENV=dev",
             f"LAYER_ENV_DIR={env_dir / 'data_access'}", f"ENV_DIR={env_dir}", f"ROOT_RUNNER={stubs['runner']}",
             f"MAKE={stubs['make']}", f"IMPORT_EXISTING_SCRIPT={importer}", "FORCE_APPLY=1"],
            cwd=ROOT / "aws", text=True, capture_output=True, env=_env(stubs["staged"], stubs["gate"], stubs["sticky"]))

    assert result.returncode != 0
    assert "data_access import failed" in result.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    assert [c[0] for c in calls].count("import-existing") == 1
    assert not [c for c in calls if c[0] == "staged"]


@pytest.mark.parametrize("cloud", ["aws", "azure"])
@pytest.mark.parametrize("target, env", [("rehearse", "dev"), ("release", "prod")])
def test_rehearse_and_release_confirm_only_after_their_own_verification(cloud, target, env):
    result = subprocess.run(["make", "-n", target, f"ENV={env}"], cwd=ROOT / cloud, text=True,
                            capture_output=True, env=_env())
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "_STAGED_CONFIRM=0" in out
    apply_at = out.index("_STAGED_CONFIRM=0")
    verify_at = out.index("_verify-access-gate ENV=", apply_at)
    confirm_at = out.index("staged_policy_apply.py\" confirm", verify_at)
    assert apply_at < verify_at < confirm_at


def test_apply_governance_and_maintain_confirm_after_the_data_access_apply():
    text = (ROOT / "shared" / "Makefile.shared").read_text()
    recipe = text[text.index("\napply-governance:"):text.index("\npolicy-status:")]
    assert recipe.index("_apply-layer LAYER=data_access") < recipe.index("$(_STAGED_CONFIRM_RUN)")
    maintain = text[text.index("\nmaintain:"):text.index("\nvalidate:")]
    assert "$(MAKE) apply-governance" in maintain and "_STAGED_CONFIRM=0" not in maintain


def test_plan_previews_the_phases_and_destroy_is_not_guarded():
    text = (ROOT / "shared" / "Makefile.shared").read_text()
    plan = text[text.index("\n_plan-layer:"):text.index("\nplan:")]
    assert "describe --env-dir" in plan and "-var=policy_stage=preview" in plan
    destroy = text[text.index("\n_destroy-layer:"):text.index("\ndestroy:")]
    assert "-var=policy_stage=destroy" in destroy


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_policy_status_target_is_read_only(tmp_path, cloud):
    env_dir = _env_dir(tmp_path, "deterministic")
    result = subprocess.run(["make", "--no-print-directory", "policy-status", "ENV=dev", f"ENV_DIR={env_dir}"],
                            cwd=ROOT / cloud, text=True, capture_output=True, env=_env())
    assert result.returncode == 0, result.stdout + result.stderr
    assert "no policy journal yet" in result.stdout
    assert sorted(p.name for p in env_dir.rglob("*")) == ["abac.auto.tfvars", "data_access", "env.auto.tfvars"]


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_terraform_refuses_a_one_shot_policy_change_outside_the_staged_phases(tmp_path):
    from tests.terraform_helpers import shared_copy, tf, tf_env, tf_init

    root = shared_copy(tmp_path) / "roots" / "data_access"
    env = tf_env(tmp_path)
    tf_init(root, env=env)
    result = tf(root, "test", "-no-color", "-filter=tests/staged_policy_guard.tftest.hcl", env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    for run in ("one_shot_retag_is_refused", "one_shot_tier_move_is_refused", "create_may_not_remove_a_live_policy",
                "import_walk_allows_drifted_principals", "tighten_may_not_remove_coverage",
                "legacy_mode_is_never_guarded", "switch_retags_under_the_controller",
                "rebind_variables_outside_rebind_are_ignored", "rebind_variables_in_rebind"):
        assert f'run "{run}"... pass' in result.stdout, result.stdout
