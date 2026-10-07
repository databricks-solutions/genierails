"""Regression tests for Terraform CLI argument precedence in the layer runner."""

import os
import shlex
import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[2]
RUNNER = ROOT / "shared" / "scripts" / "terraform_layer.sh"


def test_caller_var_override_follows_repo_tfvars(tmp_path):
    env_dir = tmp_path / "env"
    env_dir.mkdir()
    tfvars = env_dir / "env.auto.tfvars"
    tfvars.write_text('coverage_gate_max_age = "6h"\n')

    log = tmp_path / "terraform.log"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    terraform = bin_dir / "terraform"
    terraform.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> \"{log}\"\n"
    )
    terraform.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    env["LAYER_ENV_DIR"] = str(env_dir)
    result = subprocess.run(
        [
            RUNNER,
            "workspace",
            "dev",
            "apply",
            "-auto-approve",
            "-var=coverage_gate_max_age=1h",
        ],
        text=True,
        capture_output=True,
        env=env,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    apply_args = shlex.split(log.read_text().splitlines()[1])
    assert apply_args.index(f"-var-file={tfvars}") < apply_args.index(
        "-var=coverage_gate_max_age=1h"
    )


def test_data_access_runner_loads_discovered_table_facts(tmp_path):
    env_dir = tmp_path / "data_access"
    env_dir.mkdir()
    discovered = env_dir / "discovered_uc_tables.auto.tfvars"
    discovered.write_text('discovered_uc_tables = ["main.agent.orders"]\n')

    log = tmp_path / "terraform.log"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    terraform = bin_dir / "terraform"
    terraform.write_text("#!/bin/sh\n" f"printf '%s\\n' \"$*\" >> \"{log}\"\n")
    terraform.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    env["LAYER_ENV_DIR"] = str(env_dir)
    result = subprocess.run(
        [RUNNER, "data_access", "dev", "plan"],
        text=True, capture_output=True, env=env,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    plan_args = shlex.split(log.read_text().splitlines()[1])
    assert f"-var-file={discovered}" in plan_args


def _runner_with_stale_lock(tmp_path, init_error):
    """A copy of the runner whose workspace root has a lock file, and a fake
    terraform whose read-only init fails with init_error."""
    scripts = tmp_path / "project" / "scripts"
    scripts.mkdir(parents=True)
    runner = scripts / RUNNER.name
    runner.write_text(RUNNER.read_text())
    runner.chmod(0o755)
    root = tmp_path / "project" / "roots" / "workspace"
    root.mkdir(parents=True)
    (root / ".terraform.lock.hcl").write_text(
        'provider "registry.terraform.io/hashicorp/null" {\n  version     = "3.3.2"\n}\n')

    log = tmp_path / "terraform.log"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    terraform = bin_dir / "terraform"
    terraform.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> \"{log}\"\n"
        'case "$*" in *-lockfile=readonly*)\n'
        f"  echo '{init_error}' >&2; exit 1;;\n"
        # The writable init records the missing provider.
        "init*) printf 'provider \"registry.terraform.io/hashicorp/time\" {\\n  version     = \"0.13.1\"\\n}\\n' >> .terraform.lock.hcl;;\n"
        "esac\n"
    )
    terraform.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    env["LAYER_ENV_DIR"] = str(tmp_path / "env")
    return runner, env, log


def test_stale_lock_file_is_updated_instead_of_failing(tmp_path):
    # Upgrading adds a provider the locally generated lock file lacks (e.g.
    # hashicorp/time via a tests/ module); a read-only init must not wedge
    # every plan/apply on an existing install.
    runner, env, log = _runner_with_stale_lock(
        tmp_path, "Error: Provider dependency changes detected")
    result = subprocess.run([runner, "workspace", "dev", "plan"],
                            text=True, capture_output=True, env=env)

    assert result.returncode == 0, result.stdout + result.stderr
    calls = [shlex.split(line) for line in log.read_text().splitlines()]
    assert [c[0] for c in calls] == ["init", "init", "plan"]
    assert "-lockfile=readonly" in calls[0]
    assert "-lockfile=readonly" not in calls[1]
    assert "lacks providers" in result.stderr
    # Only what the writable init added is reported.
    assert "recorded registry.terraform.io/hashicorp/time 0.13.1" in result.stderr
    assert "recorded registry.terraform.io/hashicorp/null" not in result.stderr


def test_other_init_failures_still_fail(tmp_path):
    runner, env, log = _runner_with_stale_lock(tmp_path, "Error: Failed to query available provider packages")
    result = subprocess.run([runner, "workspace", "dev", "plan"],
                            text=True, capture_output=True, env=env)

    assert result.returncode != 0
    assert "Failed to query available provider packages" in result.stderr
    assert [shlex.split(line)[0] for line in log.read_text().splitlines()] == ["init"]
    assert not (tmp_path / "project" / "roots" / "workspace" / ".terraform-init.lock.d").exists()
