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
    tfvars.write_text("business_access_enabled = false\n")

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
            "-var=business_access_enabled=true",
        ],
        text=True,
        capture_output=True,
        env=env,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    apply_args = shlex.split(log.read_text().splitlines()[1])
    assert apply_args.index(f"-var-file={tfvars}") < apply_args.index(
        "-var=business_access_enabled=true"
    )
