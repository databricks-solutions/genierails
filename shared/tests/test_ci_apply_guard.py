import os
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


@pytest.mark.parametrize("cloud", ["aws", "azure"])
@pytest.mark.parametrize("target", APPLYING_TARGETS)
def test_every_applying_target_refuses_in_ci(cloud, target):
    env = {**os.environ, "CI": "true"}
    proc = subprocess.run(
        ["make", "-f", str(ROOT / cloud / "Makefile"), target, "ENV=dev"],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
    )
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
    assert "applying targets cannot run with CI=true" not in proc.stdout + proc.stderr


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_throwaway_integration_workflow_has_explicit_ci_apply_opt_out(cloud):
    proc = subprocess.run(
        [
            "make", "-f", str(ROOT / cloud / "Makefile"),
            "_guard-not-ci-apply", "ENV=dev", "CI=true",
            "GENIERAILS_ALLOW_CI_APPLY=1",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert proc.returncode == 0


def test_ci_workflow_scopes_apply_opt_out_to_integration_steps():
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert workflow.count('GENIERAILS_ALLOW_CI_APPLY: "1"') == 2
    unit_job = workflow[workflow.index("  unit-tests:"):workflow.index("  validation:")]
    assert "GENIERAILS_ALLOW_CI_APPLY" not in unit_job


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
