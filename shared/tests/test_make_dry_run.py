"""Regression tests for GNU Make dry-run safety in mixed recursive recipes."""

import os
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[2]
CLOUD_ROOT = ROOT / "aws"


def test_offline_evidence_does_not_resolve_a_terraform_warehouse():
    makefile = (ROOT / "shared" / "Makefile.shared").read_text()
    recipe = makefile[makefile.index("evidence:"):makefile.index("audit-rulebook:")]
    assert "GENIERAILS_EVIDENCE_INTEGRATION" in recipe
    assert 'python3 "$(SHARED_ROOT)/evidence_report.py";' in recipe
    assert '--warehouse-id "$$warehouse_id"' in recipe


def _clean_env():
    return {
        key: value
        for key, value in os.environ.items()
        if key not in ("GNUMAKEFLAGS", "MAKEFLAGS", "MAKELEVEL")
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


def test_discovered_agent_attribution_changes_apply_fingerprint(tmp_path):
    env_dir = tmp_path / "env"
    env_dir.mkdir()
    (env_dir / "abac.auto.tfvars").write_text("# present\n")
    discovered = env_dir / "discovered_uc_tables.auto.tfvars"
    discovered.write_text(
        'discovered_uc_tables = ["cat.sch.tbl"]\n'
        'discovered_table_agents = { "cat.sch.tbl" = ["Agent A", "Old A"] }\n'
    )
    runner_log = tmp_path / "runner.log"
    runner = tmp_path / "record-runner"
    runner.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> \"{runner_log}\"\n"
    )
    runner.chmod(0o755)
    args = [
        "make", "--no-print-directory", "_apply-layer", "LAYER=test",
        "TARGET_ENV=dev", f"LAYER_ENV_DIR={env_dir}", f"ROOT_RUNNER={runner}",
    ]

    first = subprocess.run(
        args, cwd=CLOUD_ROOT, text=True, capture_output=True, env=_clean_env()
    )
    first_fingerprint = (env_dir / ".test.apply.sha").read_text()
    discovered.write_text(
        'discovered_uc_tables = ["cat.sch.tbl"]\n'
        'discovered_table_agents = { "cat.sch.tbl" = ["Agent A"] }\n'
    )
    second = subprocess.run(
        args, cwd=CLOUD_ROOT, text=True, capture_output=True, env=_clean_env()
    )
    second_fingerprint = (env_dir / ".test.apply.sha").read_text()

    assert first.returncode == 0, first.stdout + first.stderr
    assert second.returncode == 0, second.stdout + second.stderr
    assert first_fingerprint != second_fingerprint
    assert runner_log.read_text().splitlines() == [
        "test dev apply -parallelism=1 -auto-approve",
        "test dev apply -parallelism=1 -auto-approve",
    ]


def test_rehearsal_apply_flag_follows_tfvars_and_changes_fingerprint(tmp_path):
    env_dir = tmp_path / "env"
    env_dir.mkdir()
    (env_dir / "abac.auto.tfvars").write_text("# present\n")
    (env_dir / "env.auto.tfvars").write_text(
        "business_access_enabled = false\n"
    )

    runner_log = tmp_path / "runner.log"
    runner = tmp_path / "record-runner"
    runner.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> \"{runner_log}\"\n"
    )
    runner.chmod(0o755)

    base_args = [
        "make",
        "--no-print-directory",
        "_apply-layer",
        "LAYER=workspace",
        "TARGET_ENV=dev",
        f"LAYER_ENV_DIR={env_dir}",
        f"ROOT_RUNNER={runner}",
    ]
    rehearsed = subprocess.run(
        [*base_args, "APPLY_FLAGS=-var=business_access_enabled=true"],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
        env=_clean_env(),
    )
    normal = subprocess.run(
        base_args,
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
        env=_clean_env(),
    )

    assert rehearsed.returncode == 0, rehearsed.stdout + rehearsed.stderr
    assert normal.returncode == 0, normal.stdout + normal.stderr
    assert runner_log.read_text().splitlines() == [
        "workspace dev apply -parallelism=1 -auto-approve "
        "-var=business_access_enabled=true",
        "workspace dev apply -parallelism=1 -auto-approve",
    ]


# -O/--output-sync (and GNUMAKEFLAGS) need GNU Make 4+; Apple's make is 3.81.
# conftest.py runs these with gmake when `make` is older, else skips them.
@pytest.mark.gnu_make
def test_apply_layer_output_sync_is_not_mistaken_for_dry_run(tmp_path):
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
            "-Oline",
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


def _run_recorded_apply(tmp_path, make_args, extra_env=None):
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
    env = _clean_env()
    env.update(extra_env or {})
    result = subprocess.run(
        [
            "make",
            *make_args,
            "_apply-layer",
            "LAYER=test",
            "TARGET_ENV=dev",
            f"LAYER_ENV_DIR={env_dir}",
            f"ROOT_RUNNER={runner}",
        ],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
        env=env,
    )
    return result, runner_log, env_dir / ".test.apply.sha"


@pytest.mark.parametrize(
    ("make_args", "extra_env"),
    [
        ([], {}),
        pytest.param(["-Oline"], {}, marks=pytest.mark.gnu_make),
        pytest.param(["-Onone"], {}, marks=pytest.mark.gnu_make),
        pytest.param(["--output-sync=line"], {}, marks=pytest.mark.gnu_make),
        pytest.param(["--no-print-directory", "-Oline"], {}, marks=pytest.mark.gnu_make),
        (["-I/tmp/nn"], {}),
        (["-j8", "-I/tmp/nn"], {}),
        pytest.param([], {"GNUMAKEFLAGS": "-Oline"}, marks=pytest.mark.gnu_make),
        (["ENV=dev"], {}),
        (["-j2"], {}),
        (["-B"], {}),
        (["FOO=nnn"], {}),
    ],
)
def test_real_make_options_execute_apply_layer(tmp_path, make_args, extra_env):
    result, runner_log, fingerprint = _run_recorded_apply(
        tmp_path, make_args, extra_env
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert runner_log.read_text().splitlines() == [
        "test dev apply -parallelism=1 -auto-approve"
    ]
    assert fingerprint.read_text().strip()


@pytest.mark.parametrize(
    "make_args",
    [
        ["-n"],
        ["--dry-run"],
        ["--recon"],
        ["--just-print"],
        ["-n", "-j2"],
        ["-kn"],
        ["-s", "-n"],
    ],
)
def test_dry_run_options_do_not_execute_apply_layer(tmp_path, make_args):
    result, runner_log, fingerprint = _run_recorded_apply(tmp_path, make_args)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "apply -parallelism=1 -auto-approve" in result.stdout
    assert not runner_log.exists()
    assert not fingerprint.exists()
