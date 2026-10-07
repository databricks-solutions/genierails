"""Regression tests for unified release, governance maintenance, and locking."""

import json
import os
import shlex
import signal
import socket
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
CLOUD_ROOT = ROOT / "aws"
sys.path.insert(0, str(ROOT / "shared"))
from scripts import environment_lock as lock  # noqa: E402
from scripts import release_helpers as release_helpers  # noqa: E402


def _env():
    return {k: v for k, v in os.environ.items() if k not in ("VERIFY_KEY_COLUMN", "APPLY_FLAGS", "MAKEFLAGS", "MAKELEVEL")}


def _env_dir(tmp_path, gate="false"):
    path = tmp_path / "prod"
    (path / "generated").mkdir(parents=True)
    (path / "env.auto.tfvars").write_text(f"business_access_enabled = {gate}\n")
    return path


def _stub(tmp_path, fail=None, sleep=0, on=None):
    log = tmp_path / "calls"
    stub = tmp_path / "make-stub"
    body = ["#!/bin/sh", f"printf '%s\\n' \"$*\" >> '{log}'"]
    if sleep:
        body.append(f"sleep {sleep}")
    for target, action in (on or {}).items():
        body.append(f'if [ "$1" = "{target}" ]; then {action}; fi')
    if fail:
        body.append(f'[ "$1" = "{fail}" ] && exit 1')
    body.append("exit 0")
    stub.write_text("\n".join(body) + "\n")
    stub.chmod(0o755)
    audit = tmp_path / "audit.py"
    audit_rc = 1 if fail == "audit-schema" else 0
    if "audit-schema" in (on or {}):
        audit_rc = 2 if "exit 2" in on["audit-schema"] else audit_rc
    audit.write_text(
        f"open({str(log)!r}, 'a').write('audit-schema ENV=prod\\n')\n"
        f"raise SystemExit({audit_rc})\n"
    )
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
        ["derive-assignments", "ENV=prod"],
        ["validate-generated", "ENV=prod"],
        ["coverage-gate", "ENV=prod"],
        ["--no-print-directory", "promote", "ENV=prod"],
        ["audit-rulebook", "ENV=prod"],
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
        "derive-assignments", "validate-generated", "coverage-gate"]
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
    assert names == ["audit-schema", "derive-assignments", "coverage-gate",
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


def test_rulebook_drift_blocks_release_before_access_apply(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path, fail="audit-rulebook")
    result = _make("release", env_dir, stub, audit)
    assert result.returncode != 0
    names = [c[1] if c[0] == "--no-print-directory" else c[0] for c in _calls(log)]
    assert names == ["derive-assignments", "validate-generated", "coverage-gate", "promote", "audit-rulebook"]
    assert "reported drift" in result.stderr
    assert "no new or wider business access was applied" in result.stderr
    assert "business_access_enabled = false" in (env_dir / "env.auto.tfvars").read_text()


def test_rulebook_audit_error_is_not_described_as_drift(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path, on={"audit-rulebook": "exit 2"})
    result = _make("release", env_dir, stub, audit)
    assert result.returncode == 2
    assert "failed (error, not drift)" in result.stderr
    assert "reported drift" not in result.stderr
    assert not any(c[0] == "apply" for c in _calls(log))


def test_release_failed_apply_prints_rollback_and_does_not_persist(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path, fail="apply")
    result = _make("release", env_dir, stub, audit)
    assert result.returncode != 0
    assert [c[0] for c in _calls(log)][-1] == "apply"
    assert not release_helpers.gate_open(env_dir)
    assert "PARTIALLY OPENED" in result.stderr
    assert "still has business_access_enabled = false" in result.stderr


def test_release_without_key_still_calls_verify_access(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path)
    result = _make("release", env_dir, stub, audit)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _calls(log)[-1] == ["verify-access", "ENV=prod"]


@pytest.mark.parametrize("target", ["release", "certify", "maintain"])
def test_placeholder_genie_space_id_refuses_before_lock(tmp_path, target):
    env_dir = _env_dir(tmp_path)
    (env_dir / "generated").rmdir()
    (env_dir / "env.auto.tfvars").write_text(
        'business_access_enabled = false\n'
        'genie_spaces = [{ name = "Finance", genie_space_id = "<your-genie-space-id>" }]\n'
    )
    stub, log, audit = _stub(tmp_path)
    result = _make(target, env_dir, stub, audit)
    assert result.returncode != 0
    assert "<your-genie-space-id>" in result.stdout + result.stderr
    assert not _calls(log)
    assert not (env_dir / "generated").exists()


def test_placeholder_error_is_not_hidden_by_a_held_lock(tmp_path):
    env_dir = _env_dir(tmp_path)
    (env_dir / "env.auto.tfvars").write_text(
        'genie_spaces = [{ genie_space_id = "<your-genie-space-id>" }]\n'
    )
    assert lock.acquire_lock(env_dir, os.getpid(), "maintain")[0]
    stub, _log, audit = _stub(tmp_path)
    result = _make("release", env_dir, stub, audit)
    assert result.returncode != 0
    assert "<your-genie-space-id>" in result.stdout + result.stderr
    assert "is locked" not in result.stderr
    lock.release_lock(env_dir, os.getpid())


@pytest.mark.parametrize("target", ["release", "certify", "maintain"])
def test_targets_refuse_while_env_lock_is_held(tmp_path, target):
    env_dir = _env_dir(tmp_path)
    assert lock.acquire_lock(env_dir, os.getpid(), "maintain")[0]
    stub, log, audit = _stub(tmp_path)
    if target == "certify":
        result = subprocess.run(
            ["make", target, "ENV=prod", f"ENV_DIR={env_dir}",
             f"AUDIT_SCHEMA_SCRIPT={audit}"], cwd=CLOUD_ROOT,
            text=True, capture_output=True, env=_env(),
        )
    else:
        result = _make(target, env_dir, stub, audit)
    assert result.returncode != 0
    assert not _calls(log)
    assert "held by make maintain" in result.stderr
    assert (env_dir / lock.LOCK_RELPATH).exists()
    lock.release_lock(env_dir, os.getpid())


def test_targets_take_over_stale_lock(tmp_path):
    env_dir = _env_dir(tmp_path)
    dead = subprocess.Popen(["true"]); dead.wait()
    (env_dir / lock.LOCK_RELPATH).write_text(json.dumps(
        {"pid": dead.pid, "host": socket.gethostname(), "owner": "release"}))
    stub, _log, audit = _stub(tmp_path)
    result = _make("release", env_dir, stub, audit)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "stale lock" in result.stdout
    assert not (env_dir / lock.LOCK_RELPATH).exists()


def test_parallel_make_maintain_release_serialises_on_lock(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path, sleep=1)
    result = subprocess.run(
        ["make", "-j2", "maintain", "release", "ENV=prod", f"ENV_DIR={env_dir}",
         f"MAKE={stub}", f"AUDIT_SCHEMA_SCRIPT={audit}"], cwd=CLOUD_ROOT,
        text=True, capture_output=True, env=_env(),
    )
    assert result.returncode != 0
    assert "held by make" in result.stderr
    assert _calls(log)
    assert not (env_dir / lock.LOCK_RELPATH).exists()


@pytest.mark.parametrize("content", ["", "not json", "[]", '{"pid":"12","host":"x"}', '{"pid":12}'])
def test_malformed_lock_is_treated_as_held(tmp_path, content):
    env_dir = _env_dir(tmp_path)
    path = env_dir / lock.LOCK_RELPATH
    path.write_text(content)
    stub, log, audit = _stub(tmp_path)
    result = _make("release", env_dir, stub, audit)
    assert result.returncode != 0 and not _calls(log)
    assert "cannot determine the owner" in result.stderr
    assert path.read_text() == content


def test_other_host_lock_is_held_even_with_dead_pid(tmp_path):
    env_dir = _env_dir(tmp_path)
    dead = subprocess.Popen(["true"]); dead.wait()
    path = env_dir / lock.LOCK_RELPATH
    path.write_text(json.dumps({"pid": dead.pid, "host": "other-host", "owner": "release"}))
    ok, message = lock.acquire_lock(env_dir, os.getpid(), "maintain")
    assert not ok and "other-host" in message and path.exists()


def test_dry_run_creates_no_lock_or_env_dirs(tmp_path):
    env_dir = tmp_path / "never"
    for target in ("release", "maintain", "certify"):
        result = subprocess.run(
            ["make", "-n", target, "ENV=prod", f"ENV_DIR={env_dir}",
             f"ACCOUNT_ENV_DIR={tmp_path / 'account'}"], cwd=CLOUD_ROOT,
            text=True, capture_output=True, env=_env(),
        )
        assert result.returncode == 0
    assert not env_dir.exists()


def _default_signals():
    for sig in (signal.SIGINT, signal.SIGHUP, signal.SIGTERM):
        signal.signal(sig, signal.SIG_DFL)


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT, signal.SIGHUP])
@pytest.mark.parametrize("target", ["release", "maintain"])
def test_signal_mid_run_releases_lock(tmp_path, target, sig):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path, on={"derive-assignments": "exec sleep 30"})
    proc = subprocess.Popen(
        ["make", target, "ENV=prod", f"ENV_DIR={env_dir}", f"MAKE={stub}",
         f"AUDIT_SCHEMA_SCRIPT={audit}"], cwd=CLOUD_ROOT, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=_env(), start_new_session=True,
        preexec_fn=_default_signals,
    )
    path = env_dir / lock.LOCK_RELPATH
    try:
        deadline = time.time() + 10
        while not (log.exists() and "derive-assignments" in log.read_text()):
            assert time.time() < deadline
            assert proc.poll() is None
            time.sleep(.05)
        assert path.exists(), "the lock must exist before the signal"
        os.killpg(proc.pid, sig)
        proc.wait(timeout=10)
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
    deadline = time.time() + 5
    while path.exists() and time.time() < deadline:
        time.sleep(.05)
    assert proc.returncode != 0
    assert not path.exists()


def test_maintain_audit_drift_points_to_native_classification(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, _log, audit = _stub(tmp_path, fail="audit-schema")
    result = _make("maintain", env_dir, stub, audit)
    assert result.returncode != 0
    assert "native UC classification" in result.stderr and "never LLM-tags" in result.stderr


def test_maintain_audit_error_is_not_described_as_drift(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, _log, audit = _stub(tmp_path, on={"audit-schema": "exit 2"})
    result = _make("maintain", env_dir, stub, audit)
    assert result.returncode == 2
    assert "failed (error, not drift)" in result.stderr
    assert "reported drift" not in result.stderr


@pytest.mark.parametrize("audit_rc, expected", [(1, "reported drift"), (2, "error, not drift")])
def test_maintain_preserves_direct_audit_exit_code_with_real_nested_make(tmp_path, audit_rc, expected):
    env_dir = _env_dir(tmp_path)
    audit = tmp_path / "direct-audit.py"
    audit.write_text(f"raise SystemExit({audit_rc})\n")
    result = subprocess.run(
        ["make", "maintain", "ENV=prod", f"ENV_DIR={env_dir}",
         f"ACCOUNT_ENV_DIR={tmp_path / 'account'}", f"AUDIT_SCHEMA_SCRIPT={audit}"],
        cwd=CLOUD_ROOT, text=True, capture_output=True, env=_env(),
    )
    assert result.returncode != 0 and expected in result.stderr


def test_persist_gate_replaces_existing_line_or_appends(tmp_path):
    env_dir = _env_dir(tmp_path)
    assert lock.acquire_lock(env_dir, os.getpid(), "release")[0]
    release_helpers.open_gate(env_dir, os.getpid())
    assert (env_dir / "env.auto.tfvars").read_text().count("business_access_enabled") == 1
    (env_dir / "env.auto.tfvars").write_text('uc_tables = ["a.b.c"]')
    release_helpers.open_gate(env_dir, os.getpid())
    assert (env_dir / "env.auto.tfvars").read_text() == 'uc_tables = ["a.b.c"]\nbusiness_access_enabled = true\n'
    lock.release_lock(env_dir, os.getpid())


def test_persist_gate_is_atomic_keeps_mode_and_symlink(tmp_path, monkeypatch):
    env_dir = _env_dir(tmp_path)
    real = tmp_path / "real.tfvars"
    real.write_text("business_access_enabled = false\n"); real.chmod(0o600)
    (env_dir / "env.auto.tfvars").unlink(); (env_dir / "env.auto.tfvars").symlink_to(real)
    assert lock.acquire_lock(env_dir, os.getpid(), "release")[0]
    replaced = []
    original = os.replace
    monkeypatch.setattr(release_helpers.os, "replace", lambda src, dst: (replaced.append(Path(dst)), original(src, dst)))
    release_helpers.open_gate(env_dir, os.getpid())
    assert replaced == [real.resolve()] and (env_dir / "env.auto.tfvars").is_symlink()
    assert stat.S_IMODE(real.stat().st_mode) == 0o600
    lock.release_lock(env_dir, os.getpid())


def test_failed_atomic_write_leaves_original_intact(tmp_path, monkeypatch):
    env_dir = _env_dir(tmp_path)
    before = (env_dir / "env.auto.tfvars").read_text()
    assert lock.acquire_lock(env_dir, os.getpid(), "release")[0]
    monkeypatch.setattr(release_helpers.os, "replace", lambda *_: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        release_helpers.open_gate(env_dir, os.getpid())
    assert (env_dir / "env.auto.tfvars").read_text() == before
    assert not list(env_dir.glob(".env.auto.tfvars.*.tmp"))
    lock.release_lock(env_dir, os.getpid())


def test_gate_open_parses_inline_comment(tmp_path):
    env_dir = _env_dir(tmp_path)
    (env_dir / "env.auto.tfvars").write_text("business_access_enabled = true # note\n")
    assert release_helpers.gate_open(env_dir)


def test_old_receipts_are_removed_by_release_and_maintain(tmp_path):
    for target in ("release", "maintain"):
        env_dir = _env_dir(tmp_path / target)
        for name in (".certified.json", ".certified.pending.json"):
            (env_dir / "generated" / name).write_text("old")
        (env_dir / "generated" / ".keep").write_text("keep")
        stub, _log, audit = _stub(tmp_path / target)
        result = _make(target, env_dir, stub, audit)
        assert result.returncode == 0, result.stdout + result.stderr
        assert not list(env_dir.glob("generated/.certified*"))
        assert (env_dir / "generated/.keep").exists()


def test_generate_delta_help_is_marked_legacy():
    result = subprocess.run(["make", "help"], cwd=CLOUD_ROOT, text=True, capture_output=True, env=_env())
    assert "[Legacy]" in next(line for line in result.stdout.splitlines() if "generate-delta" in line)
