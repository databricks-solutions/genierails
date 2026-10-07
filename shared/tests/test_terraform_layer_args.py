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
