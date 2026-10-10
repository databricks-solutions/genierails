import os
import re
import signal
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
APPLYING_TARGETS = (
    "enable-classification", "rehearse", "release", "maintain", "apply",
    "apply-governance", "apply-genie", "_apply-layer", "integration-test",
    "test-champion", "test-all", "test-ci", "test-ci-parallel",
    "destroy", "destroy-governance", "destroy-genie", "_destroy-layer",
    "import", "migrate-state",
)
CI_MARKERS = ("CI", "TF_BUILD", "JENKINS_URL", "GITLAB_CI", "BUILDKITE", "CIRCLECI")


def _clean_env(**updates):
    env = {key: value for key, value in os.environ.items() if key not in (*CI_MARKERS, "GENIERAILS_ALLOW_CI_APPLY")}
    env.update(updates)
    return env


def _run_guarded_make(cloud, target, *, env, timeout=10):
    proc = subprocess.Popen(
        ["make", "-f", str(ROOT / cloud / "Makefile"), target, "ENV=dev"],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
        pytest.fail(f"timed out and killed process group for {cloud} make {target}")
    return subprocess.CompletedProcess(proc.args, proc.returncode, stdout, stderr)


@pytest.mark.parametrize("cloud", ["aws", "azure"])
@pytest.mark.parametrize("target", APPLYING_TARGETS)
def test_every_applying_target_refuses_in_ci(cloud, target):
    proc = _run_guarded_make(cloud, target, env=_clean_env(CI="true"))
    output = proc.stdout + proc.stderr
    assert proc.returncode != 0
    assert "GenieRails v1 runs Terraform applies on the deployment machine" in output


@pytest.mark.parametrize("cloud", ["aws", "azure"])
@pytest.mark.parametrize("target", ["plan", "validate", "test-unit", "coverage-gate", "audit-schema"])
def test_read_only_targets_have_no_ci_refusal(cloud, target):
    proc = subprocess.run(
        ["make", "-n", "-f", str(ROOT / cloud / "Makefile"), target, "ENV=dev", "CI=true"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert "applying targets cannot run in CI" not in proc.stdout + proc.stderr


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_throwaway_integration_workflow_has_explicit_ci_apply_opt_out(cloud):
    proc = _run_guarded_make(
        cloud,
        "_guard-not-ci-apply",
        env=_clean_env(CI="true", GENIERAILS_ALLOW_CI_APPLY="1"),
    )
    assert proc.returncode == 0


@pytest.mark.parametrize(("marker", "value"), [
    ("CI", "true"), ("CI", "1"), ("CI", "yes"), ("CI", "TrUe"),
    ("TF_BUILD", "True"), ("JENKINS_URL", "https://jenkins.example"),
    ("GITLAB_CI", "true"), ("BUILDKITE", "true"), ("CIRCLECI", "true"),
])
def test_guard_detects_common_ci_markers(marker, value):
    proc = _run_guarded_make("aws", "_guard-not-ci-apply", env=_clean_env(**{marker: value}))
    assert proc.returncode != 0
    assert "GenieRails v1 runs Terraform applies on the deployment machine" in proc.stderr


def test_ci_workflow_scopes_apply_opt_out_to_integration_steps():
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert workflow.count('GENIERAILS_ALLOW_CI_APPLY: "1"') == 2
    unit_job = workflow[workflow.index("  unit-tests:"):workflow.index("  validation:")]
    assert "GENIERAILS_ALLOW_CI_APPLY" not in unit_job


def test_shipped_workflows_do_not_run_guarded_targets_without_opt_out():
    workflow_paths = sorted(
        path
        for cloud in ("aws", "azure")
        for suffix in ("*.yml", "*.yaml")
        for path in (ROOT / cloud / ".github").rglob(suffix)
    )
    assert workflow_paths
    for path in workflow_paths:
        text = path.read_text()
        steps = re.split(r"(?m)^      - ", text)[1:]
        executed_targets = set()
        for step in steps:
            targets = re.findall(r"\bmake\s+(?:--no-print-directory\s+)?([A-Za-z_][\w-]*)", step)
            executed_targets.update(targets)
            opted_out = bool(re.search(r"GENIERAILS_ALLOW_CI_APPLY:\s*['\"]?1['\"]?", step))
            guarded = set(targets) & set(APPLYING_TARGETS)
            assert not guarded or opted_out, f"{path} runs guarded targets without opt-out: {sorted(guarded)}"
        if path.name == "deploy.yml":
            assert "GENIERAILS_ALLOW_CI_APPLY" not in text
            assert not (executed_targets & set(APPLYING_TARGETS))


def test_workspace_guard_runs_env_validator_and_rejects_bad_tfvars(tmp_path):
    cloud_root = tmp_path / "aws"
    env_dir = cloud_root / "envs" / "dev"
    env_dir.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text('governance_mode = "future"\n')
    proc = subprocess.run(
        [
            "make", "-f", str(ROOT / "aws" / "Makefile"),
            "_guard-workspace-config", "ENV=dev",
            f"CLOUD_ROOT={cloud_root}", f"ENV_DIR={env_dir}",
            f"SHARED_ROOT={ROOT / 'shared'}",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert proc.returncode != 0
    assert "env config validation: governance_mode must be legacy or deterministic" in proc.stderr


def test_env_validator_cli_uses_nonzero_exit_and_stderr(tmp_path):
    env_file = tmp_path / "env.auto.tfvars"
    env_file.write_text('treatment_versions = { ssn = { partial = ["bad"] } }\n')
    proc = subprocess.run(
        [sys.executable, str(ROOT / "shared" / "scripts" / "validate_env_config.py"), str(env_file)],
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert proc.returncode == 1
    assert proc.stdout == ""
    assert "treatment_versions" in proc.stderr
    assert "Traceback" not in proc.stderr
