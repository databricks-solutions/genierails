"""Regression tests for the Makefile rehearsal pipeline."""

import os
import shlex
import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[2]
CLOUD_ROOT = ROOT / "aws"


def _clean_env():
    return {
        key: value
        for key, value in os.environ.items()
        if key not in ("VERIFY_KEY_COLUMN", "MAKEFLAGS", "MAKELEVEL")
    }


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
        env=_clean_env(),
    )

    assert result.returncode != 0
    assert log.read_text().splitlines() == ["coverage-gate"]


def test_rehearse_rejects_prod_before_any_recursive_make_call(tmp_path):
    stub, log = _recording_stub(tmp_path)
    result = subprocess.run(
        ["make", "rehearse", "ENV=prod", f"MAKE={stub}"],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
        env=_clean_env(),
    )

    assert result.returncode != 0
    assert not log.exists()
    output = result.stdout + result.stderr
    assert "rehearse: ENV=prod is not allowed" in output
    assert "use make certify" in output
    assert "business_access_enabled=true" in output
    assert "Phase 5" in output


def test_rehearse_with_key_opens_exposure_gate_for_its_apply_only(tmp_path):
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
        env=_clean_env(),
    )

    assert result.returncode == 0, result.stdout + result.stderr
    output = result.stdout + result.stderr
    assert _recorded_calls(log) == [
        ["coverage-gate", "ENV=dev"],
        ["validate-generated", "ENV=dev"],
        [
            "apply",
            "ENV=dev",
            "APPLY_FLAGS=-var=business_access_enabled=true",
        ],
        ["verify-access", "ENV=dev", "VERIFY_KEY_COLUMN=customer_id"],
    ]
    assert not any(
        mutation in output
        for mutation in ("sed ", "perl ", "env.auto.tfvars >>", "env.auto.tfvars >")
    )


def test_rehearse_without_key_runs_key_independent_live_verification(tmp_path):
    stub, log = _recording_stub(tmp_path)
    result = subprocess.run(
        ["make", "rehearse", "ENV=dev", f"MAKE={stub}"],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
        env=_clean_env(),
    )

    assert result.returncode == 0, result.stdout + result.stderr
    output = result.stdout + result.stderr
    assert _recorded_calls(log) == [
        ["coverage-gate", "ENV=dev"],
        ["validate-generated", "ENV=dev"],
        [
            "apply",
            "ENV=dev",
            "APPLY_FLAGS=-var=business_access_enabled=true",
        ],
        ["verify-access", "ENV=dev"],
    ]
    assert "skipped verify-access" not in output
