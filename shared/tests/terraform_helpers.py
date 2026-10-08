"""Run Terraform from tests without touching the source tree.

terraform init/test/plan in a real root writes .terraform.lock.hcl and the
tftest file writers write tests/.tmp there, so every test that drives
Terraform runs it in a copy of shared/ under tmp_path (shared_copy). tf()
bounds each call with a timeout; tf_init() skips when the providers can't be
downloaded (offline), but fails under REQUIRE_TERRAFORM_TESTS=1 (CI), so the
tests can never pass there without having run.
"""

from __future__ import annotations

import atexit
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
TIMEOUT = 900
OFFLINE_MARKERS = (
    "Failed to query available provider packages",
    "could not connect",
    "Failed to install provider",
    "no such host",
    "i/o timeout",
)
# Destroy-time provisioners (terraform test teardown) run
# ../../scripts/genie_space.sh; in a copy that is this no-op, so nothing ever
# reaches Databricks. GENIE_STUB_LOG, when set, records each call.
GENIE_STUB = """#!/bin/sh
if [ -n "${GENIE_STUB_LOG:-}" ]; then
  echo "$* space=${GENIE_SPACE_OBJECT_ID:-} id=${GENIE_ID_BASENAME:-} revoke=${GENIE_REVOKE_GROUPS_CSV:-}" >> "$GENIE_STUB_LOG"
fi
exit 0
"""
_IGNORED = {".terraform", ".tmp", ".terraform.lock.hcl", "__pycache__", ".pytest_cache"}


def required() -> bool:
    return os.environ.get("REQUIRE_TERRAFORM_TESTS") == "1"


def _ignore(directory: str, names: list[str]) -> set[str]:
    ignored = {name for name in names if name in _IGNORED or ".tfstate" in name}
    if Path(directory) == SHARED:
        ignored |= {"tests"} & set(names)  # the python tests; module tftests are kept
    return ignored


def shared_copy(tmp_path: Path, *, stub_genie: bool = True) -> Path:
    """A copy of shared/ (code, modules, roots, scripts, examples) to run Terraform in."""
    dest = tmp_path / "shared"
    shutil.copytree(SHARED, dest, ignore=_ignore)
    if stub_genie:
        stub = dest / "scripts" / "genie_space.sh"
        stub.write_text(GENIE_STUB)
        stub.chmod(0o755)
    return dest


_plugin_cache: Path | None = None


def session_plugin_cache() -> Path:
    """One provider cache for the whole run (each copy would download them again)."""
    global _plugin_cache
    if _plugin_cache is None:
        _plugin_cache = Path(tempfile.mkdtemp(prefix="genierails-tf-plugins-"))
        atexit.register(shutil.rmtree, _plugin_cache, True)
    return _plugin_cache


def tf_env(tmp_path: Path, plugin_cache: Path | None = None, **extra: str) -> dict:
    env = {
        **os.environ,
        "TF_DATA_DIR": str(tmp_path / ".terraform"),
        "TF_IN_AUTOMATION": "1",
        "GENIERAILS_TERRAFORM_TEST": "1",
        **extra,
    }
    if plugin_cache is not None:
        env["TF_PLUGIN_CACHE_DIR"] = str(plugin_cache)
    env.setdefault("TF_PLUGIN_CACHE_DIR", str(session_plugin_cache()))
    return env


def tf(cwd: Path, *args: str, env: dict, timeout: int = TIMEOUT) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(["terraform", *args], cwd=cwd, text=True,
                              capture_output=True, env=env, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        pytest.fail(f"terraform {' '.join(args)} in {cwd} timed out after {timeout}s\n"
                    f"{exc.stdout or ''}{exc.stderr or ''}")


def skip_if_providers_unavailable(init: subprocess.CompletedProcess) -> None:
    if init.returncode == 0:
        return
    output = init.stdout + init.stderr
    if not required() and any(marker in output for marker in OFFLINE_MARKERS):
        pytest.skip("Terraform providers not downloadable (offline)")
    raise AssertionError(output)


def tf_init(cwd: Path, *args: str, env: dict) -> subprocess.CompletedProcess:
    init = tf(cwd, "init", "-backend=false", "-input=false", *args, env=env)
    skip_if_providers_unavailable(init)
    return init
