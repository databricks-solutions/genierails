"""Shared pytest fixtures for the unit test suite.

No Databricks connection, LLM call, or Terraform is required.
"""
import os
import re
import shutil
import subprocess
import tempfile

import pytest
import hcl2
from pathlib import Path


def _gnu_make_version(binary: str) -> tuple[int, ...] | None:
    try:
        out = subprocess.run([binary, "--version"], text=True, capture_output=True, timeout=30).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    m = re.match(r"GNU Make (\d+)\.(\d+)", out)
    return tuple(int(part) for part in m.groups()) if m else None


def _find_gnu_make() -> str | None:
    """`make` if it is GNU Make 4+, else `gmake` (Homebrew on macOS), else None.

    Apple's make is GNU Make 3.81, which rejects options such as -Oline.
    """
    for binary in ("make", "gmake"):
        path = shutil.which(binary)
        if path and (_gnu_make_version(path) or (0,)) >= (4,):
            return path
    return None


GNU_MAKE = _find_gnu_make()


@pytest.fixture(autouse=True)
def _unit_tests_are_not_ci_apply_jobs(monkeypatch):
    """Keep GitHub's ambient CI marker from changing make-driven unit tests.

    Guard tests opt back in explicitly for the subprocess they are checking.
    """
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("GENIERAILS_ALLOW_CI_APPLY", raising=False)


def pytest_configure(config):
    config.addinivalue_line("markers", "gnu_make: needs GNU Make 4+ options (e.g. -Oline) from `make`")
    # Tests and recipes call plain `make`; when that is Apple's 3.81 but gmake
    # is installed, put a `make` -> gmake shim first on PATH for the session.
    if GNU_MAKE and Path(GNU_MAKE).name != "make":
        shim = Path(tempfile.mkdtemp(prefix="genierails-gnu-make-"))
        (shim / "make").symlink_to(GNU_MAKE)
        os.environ["PATH"] = f"{shim}{os.pathsep}{os.environ.get('PATH', '')}"
        config.add_cleanup(lambda: shutil.rmtree(shim, ignore_errors=True))


def pytest_runtest_setup(item):
    if item.get_closest_marker("gnu_make") and GNU_MAKE is None:
        message = "needs GNU Make 4+ (`make --version`); on macOS: brew install make, which provides gmake"
        # CI must never pass because the make-driven tests quietly didn't run.
        if os.environ.get("REQUIRE_TERRAFORM_TESTS") == "1":
            pytest.fail(f"REQUIRE_TERRAFORM_TESTS=1 but this test {message}")
        pytest.skip(message)


@pytest.fixture
def tmp_tfvars(tmp_path):
    """Return a factory that writes content to a temp .tfvars file."""
    def _write(content: str) -> Path:
        p = tmp_path / "abac.auto.tfvars"
        p.write_text(content)
        return p
    return _write


@pytest.fixture
def tmp_sql(tmp_path):
    """Return a factory that writes content to a temp .sql file."""
    def _write(content: str) -> Path:
        p = tmp_path / "masking_functions.sql"
        p.write_text(content)
        return p
    return _write


def assert_valid_hcl(path: Path) -> dict:
    """Parse path with hcl2 and return the config dict.

    Raises AssertionError with a clear message if the file is invalid HCL.
    """
    try:
        return hcl2.loads(path.read_text())
    except Exception as exc:
        raise AssertionError(
            f"File is not valid HCL after autofix:\n"
            f"  file: {path}\n"
            f"  error: {exc}\n\n"
            f"Content:\n{path.read_text()}"
        ) from exc


def pytest_collection_modifyitems(config, items):
    """With REQUIRE_TERRAFORM_TESTS=1 (CI), a missing terraform fails instead of skipping.

    The coverage-gate enforcement tests skip without terraform; CI installs it,
    and must never pass because they quietly didn't run.
    """
    if os.environ.get("REQUIRE_TERRAFORM_TESTS") == "1" and shutil.which("terraform") is None:
        raise pytest.UsageError("REQUIRE_TERRAFORM_TESTS=1 but terraform is not on PATH")
