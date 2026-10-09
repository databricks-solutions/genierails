import os
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
APPLYING_TARGETS = (
    "enable-classification", "rehearse", "release", "maintain", "apply",
    "apply-governance", "apply-genie", "_apply-layer", "integration-test",
    "test-champion", "test-all", "test-ci", "test-ci-parallel",
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
