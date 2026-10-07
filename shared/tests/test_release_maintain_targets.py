"""Regression tests for unified release, governance maintenance, and locking."""

import json
import os
import shlex
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
CLOUD_ROOT = ROOT / "aws"
sys.path.insert(0, str(ROOT / "shared"))
from scripts import environment_lock as lock  # noqa: E402


def _env():
    return {k: v for k, v in os.environ.items() if k not in ("VERIFY_KEY_COLUMN", "APPLY_FLAGS", "MAKEFLAGS", "MAKELEVEL")}


def _env_dir(tmp_path, gate="false"):
    path = tmp_path / "prod"
    (path / "generated").mkdir(parents=True)
    (path / "env.auto.tfvars").write_text(f"business_access_enabled = {gate}\n")
    return path


def _stub(tmp_path, fail=None, sleep=0):
    log = tmp_path / "calls"
    stub = tmp_path / "make-stub"
    stub.write_text("#!/bin/sh\n" + f"printf '%s\\n' \"$*\" >> '{log}'\n" +
                    (f"sleep {sleep}\n" if sleep else "") +
                    (f"[ \"$1\" = \"{fail}\" ] && exit 1\n" if fail else "") + "exit 0\n")
    stub.chmod(0o755)
    audit = tmp_path / "audit.py"
    audit.write_text(f"open({str(log)!r}, 'a').write('audit-schema ENV=prod\\n')\n")
    return stub, log, audit


def _make(target, env_dir, stub, audit, *extra):
    return subprocess.run(["make", target, "ENV=prod", f"ENV_DIR={env_dir}", f"MAKE={stub}",
                           f"AUDIT_SCHEMA_SCRIPT={audit}", *extra], cwd=CLOUD_ROOT,
                          text=True, capture_output=True, env=_env())


def _calls(log):
    return [shlex.split(x) for x in log.read_text().splitlines()] if log.exists() else []


def test_release_runs_unified_pipeline_in_order_and_writes_no_receipt(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path)
    result = _make("release", env_dir, stub, audit, "VERIFY_KEY_COLUMN= customer_id ")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _calls(log) == [
        ["--no-print-directory", "_guard-genie-placeholder", "ENV=prod"],
        ["derive-assignments", "ENV=prod"],
        ["validate-generated", "ENV=prod"],
        ["coverage-gate", "ENV=prod"],
        ["apply", "ENV=prod", "APPLY_FLAGS=-var=business_access_enabled=true", "_EXPOSURE_DERIVED=1"],
        ["verify-access", "ENV=prod", "VERIFY_KEY_COLUMN=customer_id"],
    ]
    assert "business_access_enabled = true" in (env_dir / "env.auto.tfvars").read_text()
    assert not list(env_dir.rglob(".certified*"))
    assert not (env_dir / "generated/.governance.lock").exists()


def test_release_coverage_failure_never_applies(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path, fail="coverage-gate")
    result = _make("release", env_dir, stub, audit)
    assert result.returncode != 0
    assert [c[0] if c[0] != "--no-print-directory" else c[1] for c in _calls(log)] == [
        "_guard-genie-placeholder", "derive-assignments", "validate-generated", "coverage-gate"]
    assert "business_access_enabled = false" in (env_dir / "env.auto.tfvars").read_text()
    assert not list(env_dir.rglob(".certified*"))


def test_release_verify_failure_prints_rollback_guidance(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, _log, audit = _stub(tmp_path, fail="verify-access")
    result = _make("release", env_dir, stub, audit)
    assert result.returncode != 0
    assert "PARTIALLY OPENED" in result.stderr
    assert "set business_access_enabled = false" in result.stderr
    assert "make apply ENV=prod" in result.stderr


def test_maintain_remains_governance_only_and_writes_no_receipt(tmp_path):
    env_dir = _env_dir(tmp_path, "true")
    stub, log, audit = _stub(tmp_path)
    result = _make("maintain", env_dir, stub, audit)
    assert result.returncode == 0, result.stdout + result.stderr
    names = [c[1] if c[0] == "--no-print-directory" else c[0] for c in _calls(log)]
    names = [name for name in names if name != "_guarded-bootstrap"]
    assert names == ["_guard-genie-placeholder", "audit-schema", "derive-assignments", "coverage-gate",
                     "validate-generated", "apply-governance", "audit-rulebook"]
    assert "apply" not in names and "verify-access" not in names
    assert not list(env_dir.rglob(".certified*"))


def test_lock_is_exclusive_and_stale_lock_is_reclaimed(tmp_path):
    env_dir = _env_dir(tmp_path)
    assert lock.acquire_lock(env_dir, os.getpid(), "release")[0]
    ok, message = lock.acquire_lock(env_dir, os.getpid() + 1, "maintain")
    assert not ok and "held by make release" in message
    lock.release_lock(env_dir, os.getpid())
    dead = subprocess.Popen(["true"]); dead.wait()
    path = env_dir / "generated/.governance.lock"
    path.write_text(json.dumps({"pid": dead.pid, "host": socket.gethostname(), "owner": "release"}))
    ok, message = lock.acquire_lock(env_dir, os.getpid(), "maintain")
    assert ok and "stale lock" in message
    lock.release_lock(env_dir, os.getpid())


def test_release_interrupt_cleans_lock(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, _log, audit = _stub(tmp_path, sleep=10)
    proc = subprocess.Popen(["make", "release", "ENV=prod", f"ENV_DIR={env_dir}", f"MAKE={stub}",
                             f"AUDIT_SCHEMA_SCRIPT={audit}"], cwd=CLOUD_ROOT, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=_env(), start_new_session=True)
    lock_path = env_dir / "generated/.governance.lock"
    for _ in range(100):
        if lock_path.exists(): break
        time.sleep(.02)
    os.killpg(proc.pid, signal.SIGTERM)
    proc.communicate(timeout=5)
    assert not lock_path.exists()


def test_release_rejects_account_env(tmp_path):
    stub, log, audit = _stub(tmp_path)
    result = subprocess.run(["make", "release", "ENV=account", f"MAKE={stub}"], cwd=CLOUD_ROOT,
                            text=True, capture_output=True, env=_env())
    assert result.returncode != 0 and not _calls(log)
