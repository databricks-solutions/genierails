"""Regression tests for Terraform CLI argument precedence in the layer runner."""

import os
import signal
import shlex
import subprocess
import time
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


def _locking_runner(tmp_path):
    runner, env, log = _runner_with_stale_lock(tmp_path, "unused")
    terraform = Path(env["PATH"].split(os.pathsep)[0]) / "terraform"
    terraform.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> \"{log}\"\n"
        'if [ "$1" = init ]; then sleep "${FAKE_INIT_SLEEP:-0}"; fi\n'
    )
    return runner, env


def test_killed_init_holder_is_reclaimed(tmp_path):
    runner, env = _locking_runner(tmp_path)
    holder_env = {**env, "FAKE_INIT_SLEEP": "30"}
    holder = subprocess.Popen([runner, "workspace", "dev", "plan"], env=holder_env,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                              start_new_session=True)
    owner = tmp_path / "project/roots/workspace/.terraform-init.lock.d/owner"
    for _ in range(100):
        if owner.exists():
            break
        time.sleep(0.02)
    assert owner.exists()
    os.killpg(holder.pid, signal.SIGKILL)
    holder.wait(timeout=5)
    result = subprocess.run([runner, "workspace", "dev", "plan"], env=env,
                            text=True, capture_output=True, timeout=5)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "reclaiming stale Terraform init lock" in result.stderr


def test_live_init_holder_is_never_stolen_and_wait_times_out(tmp_path):
    runner, env = _locking_runner(tmp_path)
    holder = subprocess.Popen([runner, "workspace", "dev", "plan"],
                              env={**env, "FAKE_INIT_SLEEP": "30"},
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                              start_new_session=True)
    owner = tmp_path / "project/roots/workspace/.terraform-init.lock.d/owner"
    for _ in range(100):
        if owner.exists():
            break
        time.sleep(0.02)
    result = subprocess.run([runner, "workspace", "dev", "plan"],
                            env={**env, "INIT_LOCK_TIMEOUT_SECONDS": "1"},
                            text=True, capture_output=True, timeout=5)
    assert result.returncode != 0
    assert "Timed out after 1s" in result.stderr
    assert str(owner.parent) in result.stderr
    assert "rm -rf" in result.stderr
    assert holder.poll() is None
    os.killpg(holder.pid, signal.SIGTERM)
    holder.wait(timeout=5)


def test_concurrent_waiters_atomically_reclaim_one_dead_owner(tmp_path):
    runner, env = _locking_runner(tmp_path)
    bin_dir = Path(env["PATH"].split(os.pathsep)[0])
    guard = tmp_path / "init-active"
    overlap = tmp_path / "init-overlap"
    terraform = bin_dir / "terraform"
    terraform.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = init ]; then\n'
        f'  if ! mkdir "{guard}" 2>/dev/null; then touch "{overlap}"; fi\n'
        "  sleep 0.02\n"
        f'  rmdir "{guard}" 2>/dev/null || true\n'
        "fi\n"
    )
    lock = tmp_path / "project/roots/workspace/.terraform-init.lock.d"
    for _trial in range(20):
        lock.mkdir()
        (lock / "owner").write_text(
            f"pid=99999999\nhost={subprocess.check_output(['hostname'], text=True).strip()}\n"
            "started_at=2000-01-01T00:00:00Z\n"
        )
        runners = [
            subprocess.Popen([runner, "workspace", f"dev-{index}", "plan"], env=env,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            for index in range(6)
        ]
        results = [process.communicate(timeout=15) + (process.returncode,) for process in runners]
        assert all(returncode == 0 for _stdout, _stderr, returncode in results), results
        assert not overlap.exists(), results
        assert not lock.exists()
