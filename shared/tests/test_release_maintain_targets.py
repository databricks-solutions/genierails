"""Regression tests for unified release, governance maintenance, and locking."""

import json
import os
import re
import shlex
import shutil
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


# An already-released prod env still carries the retired flag; release and
# maintain must leave the file exactly as they found it.
RELEASED_ENV_FILE = "business_access_enabled = true\n"


def _env_dir(tmp_path, env_file=RELEASED_ENV_FILE):
    path = tmp_path / "prod"
    (path / "generated").mkdir(parents=True)
    (path / "env.auto.tfvars").write_text(env_file)
    return path


def _same_env_promote(env_dir):
    return ["--no-print-directory", "promote", "ENV=prod", f"ENV_DIR={env_dir}",
            "SOURCE_ENV=prod", f"SOURCE_ENV_DIR={env_dir}", "DEST_ENV=", "DEST_ENV_DIR=",
            "DEST_CATALOG_MAP="]


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
        ["--no-print-directory", "_verify-access-keys-readonly", "ENV=prod", "VERIFY_KEY_COLUMN=customer_id"],
        ["derive-assignments", "ENV=prod"],
        ["validate-generated", "ENV=prod"],
        ["coverage-gate", "ENV=prod"],
        _same_env_promote(env_dir),
        ["verify-access-keys", "ENV=prod", "VERIFY_KEY_COLUMN=customer_id"],
        ["audit-rulebook", "ENV=prod"],
        ["apply", "ENV=prod", "APPLY_FLAGS=", "_EXPOSURE_DERIVED=1"],
        ["verify-access", "ENV=prod", "VERIFY_REQUIRE_MASKS=1", "VERIFY_KEY_COLUMN=customer_id"],
    ]
    # No gate to open or persist: release neither passes nor writes it. The
    # stubbed verify-access proves no mask check, so the key isn't saved either.
    assert "business_access_enabled" not in log.read_text()
    assert (env_dir / "env.auto.tfvars").read_text() == RELEASED_ENV_FILE
    assert not list(env_dir.rglob(".certified*"))
    assert not (env_dir / "generated/.governance.lock").exists()


def test_release_coverage_failure_never_applies(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path, fail="coverage-gate")
    result = _make("release", env_dir, stub, audit)
    assert result.returncode != 0
    assert [c[0] if c[0] != "--no-print-directory" else c[1] for c in _calls(log)] == [
        "_verify-access-keys-readonly", "derive-assignments", "validate-generated", "coverage-gate"]
    assert (env_dir / "env.auto.tfvars").read_text() == RELEASED_ENV_FILE
    assert not list(env_dir.rglob(".certified*"))


def test_release_verify_failure_prints_rollback_guidance(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, _log, audit = _stub(tmp_path, fail="verify-access")
    result = _make("release", env_dir, stub, audit)
    assert result.returncode != 0
    assert "verify-access failed" in result.stderr
    assert "PARTLY APPLIED" in result.stderr
    # Withdrawing access means removing groups/ACL entries; the retired flag
    # does nothing, so the hint must not point at it.
    assert "remove the groups" in result.stderr
    assert "business_access_enabled" not in result.stderr
    assert "make apply ENV=prod" in result.stderr


def test_maintain_remains_governance_only_and_writes_no_receipt(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path)
    result = _make("maintain", env_dir, stub, audit)
    assert result.returncode == 0, result.stdout + result.stderr
    names = [c[1] if c[0] == "--no-print-directory" else c[0] for c in _calls(log)]
    names = [name for name in names if name != "_guarded-bootstrap"]
    # Like release, the rulebook is audited before anything is applied.
    assert names == ["audit-schema", "derive-assignments", "coverage-gate",
                     "validate-generated", "audit-rulebook", "apply-governance"]
    assert "apply" not in names and "verify-access" not in names
    assert not list(env_dir.rglob(".certified*"))


@pytest.mark.parametrize("audit_rc, says", [(1, "reported drift"), (2, "failed (error, not drift)")])
def test_maintain_rulebook_drift_or_error_stops_before_apply(tmp_path, audit_rc, says):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path, on={"audit-rulebook": f"exit {audit_rc}"})
    result = _make("maintain", env_dir, stub, audit)
    assert result.returncode != 0
    names = [c[1] if c[0] == "--no-print-directory" else c[0] for c in _calls(log)]
    assert names[-1] == "audit-rulebook" and "apply-governance" not in names
    assert f"maintain: audit-rulebook {says}" in result.stderr
    assert "governance was not applied" in result.stderr


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
    assert names == ["_verify-access-keys-readonly", "derive-assignments", "validate-generated",
                     "coverage-gate", "promote", "verify-access-keys", "audit-rulebook"]
    assert "reported drift" in result.stderr
    assert "no new or wider business access was applied" in result.stderr
    assert (env_dir / "env.auto.tfvars").read_text() == RELEASED_ENV_FILE


def test_rulebook_audit_error_is_not_described_as_drift(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path, on={"audit-rulebook": "exit 2"})
    result = _make("release", env_dir, stub, audit)
    assert result.returncode == 2
    assert "failed (error, not drift)" in result.stderr
    assert "reported drift" not in result.stderr
    assert not any(c[0] == "apply" for c in _calls(log))


def test_release_failed_apply_prints_withdrawal_guidance_and_writes_nothing(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path, fail="apply")
    result = _make("release", env_dir, stub, audit)
    assert result.returncode != 0
    assert [c[0] for c in _calls(log)][-1] == "apply"
    assert (env_dir / "env.auto.tfvars").read_text() == RELEASED_ENV_FILE
    assert "apply failed" in result.stderr and "PARTLY APPLIED" in result.stderr
    assert "remove the groups" in result.stderr
    assert "business_access_enabled" not in result.stderr


def test_release_promote_stays_same_env_when_dest_env_leaks_from_the_shell(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path)
    leaked = {**_env(), "DEST_ENV": "dev", "SOURCE_ENV": "dev", "DEST_CATALOG_MAP": "a=b",
              "SOURCE_ENV_DIR": str(tmp_path / "dev"), "DEST_ENV_DIR": str(tmp_path / "dev")}
    result = subprocess.run(["make", "release", "ENV=prod", f"ENV_DIR={env_dir}", f"MAKE={stub}",
                             f"AUDIT_SCHEMA_SCRIPT={audit}"], cwd=CLOUD_ROOT,
                            text=True, capture_output=True, env=leaked)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _same_env_promote(env_dir) in _calls(log)


def test_same_env_promote_arguments_beat_leaked_shell_variables(tmp_path):
    """The arguments release/apply pass to promote win over a leaked shell
    environment (and over the outer make's command line), so promote takes
    its same-env branch."""
    env_dir = tmp_path / "prod"
    leaked = {**_env(), "DEST_ENV": "dev", "SOURCE_ENV": "dev", "DEST_CATALOG_MAP": "a=b",
              "SOURCE_ENV_DIR": str(tmp_path / "dev"), "DEST_ENV_DIR": str(tmp_path / "dev")}
    args = [a for a in _same_env_promote(env_dir) if a not in ("--no-print-directory", "promote")]
    show = tmp_path / "show.mk"
    show.write_text('_show-promote-vars: ; @echo "DEST_ENV=[$(DEST_ENV)] SOURCE_ENV=[$(SOURCE_ENV)] '
                    'SOURCE_ENV_DIR=[$(SOURCE_ENV_DIR)] DEST_ENV_DIR=[$(DEST_ENV_DIR)] MAP=[$(DEST_CATALOG_MAP)]"\n')
    result = subprocess.run(["make", "--no-print-directory", "-f", "Makefile", "-f", str(show),
                             "_show-promote-vars", *args], cwd=CLOUD_ROOT,
                            text=True, capture_output=True, env=leaked)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == (
        f"DEST_ENV=[] SOURCE_ENV=[prod] SOURCE_ENV_DIR=[{env_dir}] DEST_ENV_DIR=[] MAP=[]")


def test_every_in_recipe_same_env_promote_clears_cross_env_variables():
    makefile = (ROOT / "shared/Makefile.shared").read_text()
    definition = makefile[makefile.index("_SAME_ENV_PROMOTE = "):]
    definition = definition[:definition.index("\n\n")]
    for setting in ('SOURCE_ENV="$(ENV)"', 'DEST_ENV= ', "DEST_ENV_DIR= ", "DEST_CATALOG_MAP="):
        assert setting in definition
    # release, apply, apply-governance and plan's PROMOTE_AFTER all use it;
    # the only raw promote calls left are the explicit cross-env test targets
    # and promote-to, which passes every cross-env variable explicitly.
    raw = [line.strip() for line in makefile.splitlines()
           if re.search(r"\$\(MAKE\)[^\n]*\bpromote\b", line) and "_SAME_ENV_PROMOTE =" not in line]
    assert raw == [
        '$(MAKE) --no-print-directory promote ENV="$$src" ENV_DIR="$$src_dir" SOURCE_ENV="$$src" SOURCE_ENV_DIR="$$src_dir" \\',
        "$(MAKE) --no-print-directory promote \\",
    ], raw
    to = makefile[makefile.index("\npromote-to:"):]
    to = to[:to.index("\n\n")]
    assert 'DEST_ENV="$(ENV)" DEST_ENV_DIR="$$dest_dir" DEST_CATALOG_MAP="$$map"' in to
    assert makefile.count("$(_SAME_ENV_PROMOTE)") == 4


def test_release_without_key_still_calls_verify_access(tmp_path):
    # No column masks to pair rows for, so no key is needed; verify-access
    # still runs, and strictly (test_release_mask_proof covers masks).
    env_dir = _env_dir(tmp_path)
    stub, log, audit = _stub(tmp_path)
    result = _make("release", env_dir, stub, audit)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _calls(log)[-1] == ["verify-access", "ENV=prod", "VERIFY_REQUIRE_MASKS=1"]


@pytest.mark.parametrize("target", ["release", "maintain"])
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


@pytest.mark.parametrize("target", ["release", "maintain"])
def test_targets_refuse_while_env_lock_is_held(tmp_path, target):
    env_dir = _env_dir(tmp_path)
    assert lock.acquire_lock(env_dir, os.getpid(), "maintain")[0]
    stub, log, audit = _stub(tmp_path)
    result = _make(target, env_dir, stub, audit)
    assert result.returncode != 0
    # Only release's read-only key check runs before the lock; nothing after it.
    assert _calls(log) == ([["--no-print-directory", "_verify-access-keys-readonly", "ENV=prod"]]
                           if target == "release" else [])
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
    assert result.returncode != 0
    assert _calls(log) == [["--no-print-directory", "_verify-access-keys-readonly", "ENV=prod"]]
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


def _guarding_stub(tmp_path):
    """MAKE stand-in: records every nested call and, where the real apply /
    apply-governance would run _guard-workspace-config, runs exactly that real
    target with the arguments the nested apply received (so APPLY_FLAGS= is
    in force there). Everything else is a no-op, so nothing live runs."""
    log = tmp_path / "calls"
    stub = tmp_path / "guarding-make"
    stub.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> '{log}'\n"
        'case "$1" in apply|apply-governance) shift; '
        f'exec {shutil.which("make")} --no-print-directory _guard-workspace-config "$@" ;; esac\n'
        "exit 0\n"
    )
    stub.chmod(0o755)
    audit = tmp_path / "audit.py"
    audit.write_text("raise SystemExit(0)\n")
    return stub, log, audit


def _run_with_flags(tmp_path, target, env_name, apply_flags, env_file='sql_warehouse_id = ""\n'):
    env_dir = tmp_path / env_name
    (env_dir / "generated").mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text(env_file)
    stub, log, audit = _guarding_stub(tmp_path)
    result = subprocess.run(
        ["make", target, f"ENV={env_name}", f"ENV_DIR={env_dir}", f"MAKE={stub}",
         f"AUDIT_SCHEMA_SCRIPT={audit}", f"APPLY_FLAGS={apply_flags}"],
        cwd=CLOUD_ROOT, text=True, capture_output=True, env=_env(),
    )
    assert result.returncode == 0, result.stdout + result.stderr
    warnings = [line for line in (result.stdout + result.stderr).splitlines()
                if line.startswith("WARNING: business_access_enabled")]
    return _calls(log), warnings


@pytest.mark.parametrize("flags", ["-var=business_access_enabled=false", "-var business_access_enabled=false",
                                   "-var=business_access_enabled=true"])
@pytest.mark.parametrize("target, env_name, nested", [
    ("release", "prod", "apply"),
    ("rehearse", "dev", "apply"),
    ("maintain", "prod", "apply-governance"),
])
def test_retired_flag_in_caller_apply_flags_warns_once_though_nested_apply_clears_them(
        tmp_path, flags, target, env_name, nested):
    calls, warnings = _run_with_flags(tmp_path, target, env_name, flags)
    assert len(warnings) == 1, warnings
    assert "(APPLY_FLAGS)" in warnings[0]
    # What reaches Terraform is unchanged: the nested apply still gets empty flags.
    nested_calls = [call for call in calls if call[0] == nested]
    assert nested_calls == [[nested, f"ENV={env_name}", "APPLY_FLAGS=", "_EXPOSURE_DERIVED=1"]]
    assert not any(f"APPLY_FLAGS={flags}" in call for call in calls)


@pytest.mark.parametrize("target, env_name", [("release", "prod"), ("rehearse", "dev")])
def test_retired_flag_in_file_and_caller_flags_warns_exactly_once(tmp_path, target, env_name):
    _calls_, warnings = _run_with_flags(tmp_path, target, env_name, "-var business_access_enabled=false",
                                        env_file="business_access_enabled = true\n")
    assert len(warnings) == 1, warnings
    assert f"envs/{env_name}/env.auto.tfvars" in warnings[0] or "env.auto.tfvars" in warnings[0]
    assert "APPLY_FLAGS" in warnings[0]


@pytest.mark.parametrize("target, env_name", [("release", "prod"), ("rehearse", "dev"), ("maintain", "prod")])
def test_no_retired_flag_means_no_warning_through_nested_apply(tmp_path, target, env_name):
    _calls_, warnings = _run_with_flags(tmp_path, target, env_name, "-var=coverage_gate_max_age=6h")
    assert warnings == []


