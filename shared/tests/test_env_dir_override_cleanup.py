"""Redirected env dirs are scaffolded but never cleaned.

_prepare-env removes leftovers of the retired copied-root layout (*.tf, *.py,
ABAC_PROMPT.md, scripts/, and ddl/ generated/ data_access/ under the account
env) by glob. ENV_DIR, ACCOUNT_ENV_DIR, SOURCE_ENV_DIR and DEST_ENV_DIR can
point anywhere, and `make setup ENV=dev ENV_DIR=/tmp/victim` used to delete the
victim's own keep.tf, keep.py and scripts/. The cleanup now runs only when
the env dir's physical path is the checkout's own envs/<name>, anchored to the
including Makefile's dir rather than the overridable CLOUD_ROOT, and make clean
refuses anywhere else. Symlinked envs/ or envs/<name> and an overridden
CLOUD_ROOT are covered too.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
MAKEFILE = SHARED / "Makefile.shared"

pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")

USER_FILES = {"keep.tf": "# mine\n", "keep.py": "# mine\n", "scripts/keep": "# mine\n",
              "ABAC_PROMPT.md": "# mine\n", "terraform.tfstate": "{}\n", ".terraform/keep": "x\n",
              "ddl/keep.sql": "-- mine\n", "generated/keep": "x\n", "data_access/keep": "x\n"}
LEGACY = ["main.tf", "helper.py", "ABAC_PROMPT.md", "scripts/old.sh"]

DEV_ENV = 'genie_spaces = [\n  { genie_space_id = "s1", uc_tables = ["paycat.s.t"] },\n]\n'
DEV_GENERATED = '''
groups = { pay_group = {} }
fgac_policies = [
  { name = "pay" catalog = "paycat" to_principals = ["pay_group"] },
]
genie_space_configs = {
  Payments = { title = "Payments" }
}
genie_space_id_to_name = { s1 = "Payments" }
'''


def _clean_env(**extra):
    env = {k: v for k, v in os.environ.items()
           if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "ENV_DIR", "ACCOUNT_ENV_DIR", "SOURCE_ENV",
                        "SOURCE_ENV_DIR", "DEST_ENV", "DEST_ENV_DIR", "DEST_CATALOG_MAP")}
    env.update(extra)
    return env


@pytest.fixture
def cloud(tmp_path):
    # Like aws/Makefile, the including Makefile sits in the cloud dir itself.
    root = tmp_path / "cloud"
    (root / "envs").mkdir(parents=True)
    (root / "Makefile").write_text(
        f"SHARED_ROOT := {SHARED}\nCLOUD_ROOT := {root}\nCLOUD := aws\ninclude {MAKEFILE}\n")
    # validate_abac.py needs the Databricks SDK; promote's split is what matters here.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "python3").write_text(
        "#!/bin/sh\ncase \"$1\" in *validate_abac.py) exit 0;; esac\n"
        f"exec {sys.executable} \"$@\"\n")
    (bin_dir / "python3").chmod(0o755)

    def run(*args, cwd=root):
        return subprocess.run(["make", "--no-print-directory", *args], cwd=cwd, text=True,
                              capture_output=True, timeout=120,
                              env=_clean_env(PATH=f"{bin_dir}:{os.environ['PATH']}"))

    run.root = root
    return run


def _victim(path: Path) -> Path:
    for rel, text in USER_FILES.items():
        (path / rel).parent.mkdir(parents=True, exist_ok=True)
        (path / rel).write_text(text)
    return path


def _assert_intact(path: Path):
    assert {rel: (path / rel).read_text() for rel in USER_FILES if (path / rel).is_file()} == USER_FILES


@pytest.mark.parametrize("env_name", ["dev", "account"])
def test_setup_never_cleans_a_redirected_env_dir(cloud, tmp_path, env_name):
    victim = _victim(tmp_path / "victim")

    result = cloud("setup", f"ENV={env_name}", f"ENV_DIR={victim}")

    assert result.returncode == 0, result.stdout + result.stderr
    _assert_intact(victim)
    assert (victim / "env.auto.tfvars").is_file()


def test_setup_never_cleans_a_redirected_account_env_dir(cloud, tmp_path):
    victim = _victim(tmp_path / "victim")

    result = cloud("setup", "ENV=dev", f"ACCOUNT_ENV_DIR={victim}")

    assert result.returncode == 0, result.stdout + result.stderr
    _assert_intact(victim)
    assert (victim / "auth.auto.tfvars").is_symlink()
    assert (cloud.root / "envs/dev/env.auto.tfvars").is_file()


def test_promote_never_cleans_redirected_source_or_dest_dirs(cloud, tmp_path):
    source = _victim(tmp_path / "source")
    dest = _victim(tmp_path / "dest")
    (source / "env.auto.tfvars").write_text(DEV_ENV)
    (source / "generated/abac.auto.tfvars").write_text(DEV_GENERATED)
    (source / "generated/masking_functions.sql").write_text("-- none\n")

    result = cloud("promote", "SOURCE_ENV=dev", f"SOURCE_ENV_DIR={source}", "DEST_ENV=prod",
                   f"DEST_ENV_DIR={dest}", "DEST_CATALOG_MAP=paycat=ppay")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Promote complete: dev -> prod" in result.stdout
    _assert_intact(source)
    _assert_intact(dest)
    assert (dest / "generated/abac.auto.tfvars").is_file()


def test_clean_refuses_a_redirected_env_dir(cloud, tmp_path):
    victim = _victim(tmp_path / "victim")
    (victim / "generated/abac.auto.tfvars").write_text("groups = {}\n")

    result = cloud("clean", "ENV=dev", f"ENV_DIR={victim}")

    assert result.returncode != 0
    assert f"make clean only cleans this checkout's {cloud.root}/envs/dev" in result.stdout
    _assert_intact(victim)
    assert (victim / "generated/abac.auto.tfvars").is_file()


def test_setup_still_removes_legacy_files_from_its_own_env_dirs(cloud):
    dev, account = cloud.root / "envs/dev", cloud.root / "envs/account"
    for env_dir in (dev, account):
        for rel in LEGACY:
            (env_dir / rel).parent.mkdir(parents=True, exist_ok=True)
            (env_dir / rel).write_text("# stale\n")
    for sub in ("ddl", "generated", "data_access"):
        (account / sub).mkdir()
        (account / sub / "stale").write_text("x\n")
    (dev / "auth.auto.tfvars").write_text("# keep\n")

    result = cloud("setup", "ENV=dev")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Not removing" not in result.stdout
    for env_dir in (dev, account):
        assert not any((env_dir / rel).exists() for rel in LEGACY), env_dir
        assert not (env_dir / "scripts").exists()
    assert not any((account / sub).exists() for sub in ("ddl", "generated", "data_access"))
    assert (dev / "auth.auto.tfvars").read_text() == "# keep\n"
    assert (dev / "env.auto.tfvars").is_file() and (dev / "ddl").is_dir() and (dev / "generated").is_dir()
    assert os.readlink(dev / "data_access/env.auto.tfvars") == "../env.auto.tfvars"
    assert os.readlink(account / "auth.auto.tfvars") == "../dev/auth.auto.tfvars"


def test_setup_does_not_clean_through_a_symlinked_env_dir(cloud, tmp_path):
    target = _victim(tmp_path / "linked")
    (cloud.root / "envs/dev").symlink_to(target)

    result = cloud("setup", "ENV=dev")

    assert result.returncode == 0, result.stdout + result.stderr
    _assert_intact(target)
    assert f"Not removing old-layout files (*.tf, *.py, scripts/, ...) from {cloud.root}/envs/dev" in result.stdout

    clean = cloud("clean", "ENV=dev")
    assert clean.returncode != 0
    assert "make clean only cleans this checkout's" in clean.stdout
    _assert_intact(target)


def test_setup_and_clean_do_not_follow_a_symlinked_envs_dir(cloud, tmp_path):
    victim_envs = tmp_path / "victim_envs"
    for name in ("dev", "account"):
        _victim(victim_envs / name)
    (cloud.root / "envs").rmdir()
    (cloud.root / "envs").symlink_to(victim_envs)

    result = cloud("setup", "ENV=dev")

    assert result.returncode == 0, result.stdout + result.stderr
    _assert_intact(victim_envs / "dev")
    _assert_intact(victim_envs / "account")
    assert (victim_envs / "dev/env.auto.tfvars").is_file()

    clean = cloud("clean", "ENV=dev")
    assert clean.returncode != 0
    assert "make clean only cleans this checkout's" in clean.stdout
    _assert_intact(victim_envs / "dev")


@pytest.mark.parametrize("sub", ["generated", "data_access"])
def test_clean_does_not_follow_a_symlinked_subdir(cloud, tmp_path, sub):
    assert cloud("setup", "ENV=dev").returncode == 0
    dev = cloud.root / "envs/dev"
    victim = _victim(tmp_path / "victim")
    shutil.rmtree(dev / sub)
    (dev / sub).symlink_to(victim)

    result = cloud("clean", "ENV=dev")

    assert result.returncode != 0
    _assert_intact(victim)


def test_overridden_cloud_root_is_never_cleaned(cloud, tmp_path):
    victim = tmp_path / "victim"
    for name in ("dev", "account"):
        _victim(victim / "envs" / name)

    result = cloud("setup", "ENV=dev", f"CLOUD_ROOT={victim}")

    assert result.returncode == 0, result.stdout + result.stderr
    _assert_intact(victim / "envs/dev")
    _assert_intact(victim / "envs/account")
    assert (victim / "envs/dev/env.auto.tfvars").is_file()

    clean = cloud("clean", "ENV=dev", f"CLOUD_ROOT={victim}")
    assert clean.returncode != 0
    _assert_intact(victim / "envs/dev")


def test_cleanup_still_runs_when_the_checkout_is_reached_through_a_symlink(cloud, tmp_path):
    # Symlinks above the cloud dir (e.g. a checkout opened via a linked path) are fine.
    dev = cloud.root / "envs/dev"
    dev.mkdir()
    (dev / "main.tf").write_text("# stale\n")
    (tmp_path / "alias").symlink_to(cloud.root)

    result = cloud("setup", "ENV=dev", cwd=tmp_path / "alias")

    assert result.returncode == 0, result.stdout + result.stderr
    assert not (dev / "main.tf").exists()
    assert "Not removing" not in result.stdout


def test_clean_still_cleans_its_own_env_dir(cloud):
    assert cloud("setup", "ENV=dev").returncode == 0
    dev = cloud.root / "envs/dev"
    (dev / "generated/abac.auto.tfvars").write_text("groups = {}\n")
    (dev / "terraform.tfstate").write_text("{}\n")

    result = cloud("clean", "ENV=dev")

    assert result.returncode == 0, result.stdout + result.stderr
    assert not (dev / "generated/abac.auto.tfvars").exists()
    assert not (dev / "terraform.tfstate").exists()
    assert (dev / "env.auto.tfvars").is_file()
