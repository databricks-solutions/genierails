"""aws/Makefile and azure/Makefile work through make -f from any directory.

The wrappers used to set CLOUD_ROOT from the cwd, so make -f <path>/aws/Makefile
run elsewhere looked for <cwd>/../shared/Makefile.shared and failed. They now
default CLOUD_ROOT to their own dir and point recursive $(MAKE) calls back at
themselves when run from another dir; a command-line CLOUD_ROOT still wins.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
REPO = SHARED.parent

pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")


def _clean_env():
    return {k: v for k, v in os.environ.items()
            if k not in ("MAKEFLAGS", "MAKELEVEL", "MAKEFILES", "GENIERAILS_CLOUD_DIR", "ENV", "ENV_DIR",
                         "CLOUD_ROOT", "SHARED_ROOT", "SOURCE_ENV", "DEST_ENV")}


def _make(cwd, makefile, *args):
    return subprocess.run(["make", "--no-print-directory", "-f", str(makefile), *args], cwd=cwd, text=True,
                          capture_output=True, timeout=120, env=_clean_env())


@pytest.fixture(params=["aws", "azure"])
def wrapper(request, tmp_path):
    """A copy of the real wrapper beside a link to shared/, so setup writes under tmp_path."""
    cloud = tmp_path / "checkout" / request.param
    cloud.mkdir(parents=True)
    shutil.copy(REPO / request.param / "Makefile", cloud / "Makefile")
    (tmp_path / "checkout/shared").symlink_to(SHARED)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    return cloud, elsewhere


def test_setup_through_make_f_from_another_dir(wrapper):
    cloud, elsewhere = wrapper
    (cloud / "envs/dev").mkdir(parents=True)
    (cloud / "envs/dev/main.tf").write_text("# stale\n")

    result = _make(elsewhere, cloud / "Makefile", "setup", "ENV=dev")

    assert result.returncode == 0, result.stdout + result.stderr
    assert (cloud / "envs/dev/env.auto.tfvars").is_file()
    assert (cloud / "envs/account/env.auto.tfvars").is_file()
    assert os.readlink(cloud / "envs/dev/data_access/env.auto.tfvars") == "../env.auto.tfvars"
    assert not (cloud / "envs/dev/main.tf").exists()
    assert list(elsewhere.iterdir()) == []


@pytest.mark.parametrize("cloud_name", ["aws", "azure"])
def test_help_and_release_dry_run_through_make_f_from_another_dir(tmp_path, cloud_name):
    makefile = REPO / cloud_name / "Makefile"

    help_result = _make(tmp_path, makefile, "help")
    release = _make(tmp_path, makefile, "-n", "release", "ENV=prod")

    assert help_result.returncode == 0, help_result.stdout + help_result.stderr
    assert "setup" in help_result.stdout and "release" in help_result.stdout
    assert release.returncode == 0, release.stdout + release.stderr
    assert "make verify-access ENV=prod" in release.stdout
    assert list(tmp_path.iterdir()) == []
    assert not (REPO / cloud_name / "envs").exists()


def test_run_from_its_own_dir_make_is_unchanged(wrapper):
    cloud, _ = wrapper
    (cloud / "Probe.mk").write_text("probe:\n\t@echo 'MAKE=[$(MAKE)] ROOT=[$(CLOUD_ROOT)]'\n")

    here = subprocess.run(["make", "--no-print-directory", "-f", "Makefile", "-f", "Probe.mk", "probe"],
                          cwd=cloud, text=True, capture_output=True, timeout=60, env=_clean_env())

    assert here.returncode == 0, here.stdout + here.stderr
    assert "-f" not in here.stdout.split("MAKE=[")[1].split("]")[0]
    assert f"ROOT=[{cloud.resolve()}]" in here.stdout


def test_command_line_cloud_root_still_wins(wrapper, tmp_path):
    cloud, elsewhere = wrapper
    other = tmp_path / "other_root"
    other.mkdir()

    result = _make(elsewhere, cloud / "Makefile", "setup", "ENV=dev", f"CLOUD_ROOT={other}",
                   f"SHARED_ROOT={SHARED}")

    assert result.returncode == 0, result.stdout + result.stderr
    assert (other / "envs/dev/env.auto.tfvars").is_file()
    assert not (cloud / "envs").exists()
