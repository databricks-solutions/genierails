"""make refuses an env name that isn't a plain directory name under envs/.

Recipes loop over env names word by word, so one mis-quoted argument such as
"ENV=prod VERIFY_KEY_COLUMN=customer_id" (make then reads ENV as two words)
scaffolded a stray envs/VERIFY_KEY_COLUMN=customer_id and ran the env cleanup
in it, and a name with "/" or ".." would reach outside envs/. The check runs
when make reads Makefile.shared, before any recipe, with the rule promote-to
already applied (saved_settings.ENV_NAME).
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
MAKEFILE = SHARED / "Makefile.shared"
sys.path.insert(0, str(SHARED / "scripts"))

from saved_settings import ENV_NAME  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")

VALID = ["dev", "prod", "account", "data_access", "bu_fin", "bu2", "import_noabac_prod", "genie-only"]
INVALID = ["prod VERIFY_KEY_COLUMN=customer_id", "dev prod", "Prod", "PROD", "1dev", "_dev",
           "-x", "dev/", "../envs/prod", "/tmp/dev", "..", ".", "dev.prod", "a,b", "a%b", "dé", ""]


def _clean_env():
    return {k: v for k, v in os.environ.items()
            if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "FROM", "SOURCE_ENV", "DEST_ENV", "TARGET_ENV")}


@pytest.fixture
def cloud(tmp_path):
    root = tmp_path / "cloud"
    (root / "envs").mkdir(parents=True)
    (tmp_path / "Makefile").write_text(
        f"SHARED_ROOT := {SHARED}\nCLOUD_ROOT := {root}\nCLOUD := aws\ninclude {MAKEFILE}\n")

    def run(*args):
        return subprocess.run(["make", "--no-print-directory", *args], cwd=tmp_path, text=True,
                              capture_output=True, env=_clean_env(), timeout=120)

    run.root = root
    return run


def _entries(root: Path) -> set[str]:
    return {str(p.relative_to(root)) for p in root.rglob("*")}


@pytest.mark.parametrize("name", VALID + INVALID)
def test_make_and_promote_to_agree_on_env_names(cloud, name):
    result = cloud("-n", "help", f"ENV={name}")
    accepted = bool(ENV_NAME.fullmatch(name))
    assert (result.returncode == 0) == accepted, result.stdout + result.stderr
    if not accepted:
        assert f"ENV='{name}' is not an env name" in result.stderr


@pytest.mark.parametrize("variable", ["FROM", "SOURCE_ENV", "DEST_ENV", "TARGET_ENV"])
@pytest.mark.parametrize("name", ["dev x", "../envs/dev", "Dev"])
def test_other_env_name_variables_are_checked_too(cloud, variable, name):
    result = cloud("-n", "help", "ENV=prod", f"{variable}={name}")
    assert result.returncode != 0
    assert f"{variable}='{name}' is not an env name" in result.stderr


@pytest.mark.parametrize("variable", ["FROM", "SOURCE_ENV", "DEST_ENV", "TARGET_ENV"])
def test_unset_or_empty_optional_names_pass(cloud, variable):
    assert cloud("-n", "help", "ENV=prod", f"{variable}=").returncode == 0


def test_a_mis_quoted_argument_scaffolds_nothing(cloud):
    # The reproduction: one argument holding two assignments.
    before = _entries(cloud.root)
    result = cloud("setup", "ENV=prod VERIFY_KEY_COLUMN=customer_id")
    assert result.returncode != 0
    assert "Pass each make argument separately" in result.stderr
    assert _entries(cloud.root) == before
    assert not (cloud.root / "envs" / "VERIFY_KEY_COLUMN=customer_id").exists()


def test_a_path_cannot_reach_outside_envs(cloud):
    outside = cloud.root / "keep.tf"
    outside.write_text("# must survive\n")
    result = cloud("setup", "ENV=dev ..")
    assert result.returncode != 0
    assert outside.read_text() == "# must survive\n"
    assert not (cloud.root / "envs" / "dev").exists()


def test_a_valid_setup_still_scaffolds_its_env(cloud):
    result = cloud("setup", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    assert (cloud.root / "envs" / "dev" / "env.auto.tfvars").is_file()
    assert {p.name for p in (cloud.root / "envs").iterdir()} == {"account", "dev"}
