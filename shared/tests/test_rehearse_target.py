"""Regression tests for the Makefile rehearsal pipeline."""

import subprocess
import shlex
from pathlib import Path


ROOT = Path(__file__).parents[2]
CLOUD_ROOT = ROOT / "aws"


def _recording_stub(tmp_path):
    log = tmp_path / "recursive-make.log"
    stub = tmp_path / "record-successful-make"
    stub.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> \"{log}\"\n"
        "exit 0\n"
    )
    stub.chmod(0o755)
    return stub, log


def _recorded_calls(log):
    return [shlex.split(line) for line in log.read_text().splitlines()]


def test_rehearse_stops_after_first_failing_stage(tmp_path):
    log = tmp_path / "recursive-make.log"
    stub = tmp_path / "record-failing-make"
    stub.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$1\" >> \"{log}\"\n"
        "exit 1\n"
    )
    stub.chmod(0o755)

    result = subprocess.run(
        [
            "make",
            "rehearse",
            "ENV=dev",
            "VERIFY_KEY_COLUMN=customer_id",
            f"MAKE={stub}",
        ],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0
    assert log.read_text().splitlines() == ["coverage-gate"]


def test_rehearse_with_key_is_ordered_and_does_not_toggle_exposure_gate(tmp_path):
    stub, log = _recording_stub(tmp_path)
    result = subprocess.run(
        [
            "make",
            "rehearse",
            "ENV=dev",
            "VERIFY_KEY_COLUMN=  customer_id  ",
            f"MAKE={stub}",
        ],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    output = result.stdout + result.stderr
    assert _recorded_calls(log) == [
        ["coverage-gate", "ENV=dev"],
        ["validate-generated", "ENV=dev"],
        ["apply", "ENV=dev"],
        ["verify-access", "ENV=dev", "VERIFY_KEY_COLUMN=customer_id"],
    ]
    assert output.count("business_access_enabled") == 1
    assert "requires business_access_enabled=true" in output
    assert not any(
        mutation in output
        for mutation in ("sed ", "perl ", "env.auto.tfvars >>", "env.auto.tfvars >")
    )


def test_rehearse_without_key_runs_apply_then_recommends_live_verification(tmp_path):
    stub, log = _recording_stub(tmp_path)
    result = subprocess.run(
        ["make", "rehearse", "ENV=dev", f"MAKE={stub}"],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    output = result.stdout + result.stderr
    assert _recorded_calls(log) == [
        ["coverage-gate", "ENV=dev"],
        ["validate-generated", "ENV=dev"],
        ["apply", "ENV=dev"],
    ]
    assert (
        "rehearse: skipped verify-access — pass VERIFY_KEY_COLUMN=<col> "
        "to prove masking live (recommended)"
    ) in output
