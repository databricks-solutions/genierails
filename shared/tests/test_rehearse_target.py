"""Regression tests for the Makefile rehearsal pipeline."""

import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[2]
CLOUD_ROOT = ROOT / "aws"


def test_rehearse_dry_run_is_ordered_and_does_not_toggle_exposure_gate():
    result = subprocess.run(
        [
            "make",
            "-n",
            "rehearse",
            "ENV=dev",
            "VERIFY_KEY_COLUMN=customer_id",
        ],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    output = result.stdout + result.stderr
    invocations = [
        'make coverage-gate ENV="dev"',
        'make validate-generated ENV="dev"',
        'make apply ENV="dev"',
        'make verify-access ENV="dev" VERIFY_KEY_COLUMN="customer_id"',
    ]
    positions = [output.index(invocation) for invocation in invocations]

    assert positions == sorted(positions)
    assert output.count("business_access_enabled") == 1
    assert "requires business_access_enabled=true" in output
    assert not any(
        mutation in output
        for mutation in ("sed ", "perl ", "env.auto.tfvars >>", "env.auto.tfvars >")
    )


def test_rehearse_requires_verify_key_column():
    result = subprocess.run(
        ["make", "rehearse", "ENV=dev"],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0
    assert "ERROR: VERIFY_KEY_COLUMN is required for rehearse." in result.stdout
