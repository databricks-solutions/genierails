"""Regression tests for the Makefile certification pipeline."""

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


def test_certify_is_ordered_and_enforcement_only(tmp_path):
    stub, log = _recording_stub(tmp_path)
    result = subprocess.run(
        ["make", "certify", "ENV=prod", f"ENV_DIR={tmp_path / 'prod'}", f"MAKE={stub}"],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
        env=_clean_env(),
    )

    assert result.returncode == 0, result.stdout + result.stderr
    calls = _recorded_calls(log)
    assert calls == [
        ["derive-assignments", "ENV=prod"],
        ["coverage-gate", "ENV=prod"],
        ["validate-generated", "ENV=prod"],
        ["apply-governance", "ENV=prod"],
        ["audit-rulebook", "ENV=prod"],
    ]
    assert not any(
        call[0] in ("apply", "apply-genie", "verify-access") for call in calls
    )
    assert not any("business_access_enabled" in arg for call in calls for arg in call)


def test_certify_stops_after_first_failing_stage(tmp_path):
    log = tmp_path / "recursive-make.log"
    stub = tmp_path / "record-failing-make"
    stub.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> \"{log}\"\n"
        "exit 1\n"
    )
    stub.chmod(0o755)

    result = subprocess.run(
        ["make", "certify", "ENV=prod", f"ENV_DIR={tmp_path / 'prod'}", f"MAKE={stub}"],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
        env=_clean_env(),
    )

    assert result.returncode != 0
    assert _recorded_calls(log) == [["derive-assignments", "ENV=prod"]]
