"""Regression tests for GNU Make dry-run safety in mixed recursive recipes."""

import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[2]
CLOUD_ROOT = ROOT / "aws"


def _clean_env():
    return {
        key: value
        for key, value in os.environ.items()
        if key not in ("MAKEFLAGS", "MAKELEVEL")
    }


def test_apply_layer_dry_run_prints_but_does_not_execute_side_effects(tmp_path):
    env_dir = tmp_path / "env"
    env_dir.mkdir()
    (env_dir / "abac.auto.tfvars").write_text("# present\n")

    runner_log = tmp_path / "runner.log"
    runner = tmp_path / "record-runner"
    runner.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> \"{runner_log}\"\n"
    )
    runner.chmod(0o755)

    result = subprocess.run(
        [
            "make",
            "-n",
            "_apply-layer",
            "LAYER=test",
            "TARGET_ENV=dev",
            f"LAYER_ENV_DIR={env_dir}",
            f"ROOT_RUNNER={runner}",
        ],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
        env=_clean_env(),
    )

    assert result.returncode == 0, result.stdout + result.stderr
    output = result.stdout + result.stderr
    assert "apply -parallelism=1 -auto-approve" in output
    assert "current_fingerprint" in output
    assert not runner_log.exists()
    assert not (env_dir / ".test.apply.sha").exists()


def test_apply_dry_run_does_not_reach_configured_account_layer(tmp_path):
    account_dir = tmp_path / "account"
    account_dir.mkdir()
    (account_dir / "abac.auto.tfvars").write_text("# present\n")

    runner_log = tmp_path / "runner.log"
    runner = tmp_path / "record-runner"
    runner.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> \"{runner_log}\"\n"
    )
    runner.chmod(0o755)

    result = subprocess.run(
        [
            "make",
            "-n",
            "apply",
            "ENV=account",
            f"ACCOUNT_ENV_DIR={account_dir}",
            f"ROOT_RUNNER={runner}",
        ],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
        env=_clean_env(),
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "_apply-layer LAYER=account" in result.stdout
    assert not runner_log.exists()
    assert not (account_dir / ".account.apply.sha").exists()


def test_apply_layer_real_path_keeps_command_and_fingerprint_behavior(tmp_path):
    env_dir = tmp_path / "env"
    env_dir.mkdir()
    (env_dir / "abac.auto.tfvars").write_text("# present\n")

    runner_log = tmp_path / "runner.log"
    runner = tmp_path / "record-runner"
    runner.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> \"{runner_log}\"\n"
    )
    runner.chmod(0o755)

    result = subprocess.run(
        [
            "make",
            "--no-print-directory",
            "_apply-layer",
            "LAYER=test",
            "TARGET_ENV=dev",
            f"LAYER_ENV_DIR={env_dir}",
            f"ROOT_RUNNER={runner}",
        ],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
        env=_clean_env(),
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert runner_log.read_text().splitlines() == [
        "test dev apply -parallelism=1 -auto-approve"
    ]
    assert (env_dir / ".test.apply.sha").read_text().strip()
