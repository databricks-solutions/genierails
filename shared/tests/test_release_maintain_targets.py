"""Regression tests for the certification receipt, `make release`, and `make maintain`."""

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

from scripts import certification_receipt as cr  # noqa: E402


def _clean_env():
    return {
        key: value
        for key, value in os.environ.items()
        if key not in ("VERIFY_KEY_COLUMN", "APPLY_FLAGS", "MAKEFLAGS", "MAKELEVEL")
    }


def _stub(tmp_path, fail_on=None, on=None, sleep=None):
    """Recursive-make stub: logs its args, then runs on[$1] (shell) if given.

    Exits 1 when its first arg is fail_on; sleeps `sleep` seconds per call.
    """
    log = tmp_path / "recursive-make.log"
    stub = tmp_path / "record-make"
    body = [
        "#!/bin/sh",
        'case "$*" in *"_guarded-bootstrap _guard-workspace-target"*) exit 0;; esac',
        f"printf '%s\\n' \"$*\" >> \"{log}\"",
    ]
    if sleep:
        body.append(f"sleep {sleep}")
    for target, action in (on or {}).items():
        body.append(f'if [ "$1" = "{target}" ]; then {action}; fi')
    if fail_on:
        body.append(f'[ "$1" = "{fail_on}" ] && exit 1')
    body.append("exit 0")
    stub.write_text("\n".join(body) + "\n")
    stub.chmod(0o755)
    audit_rc = 1 if fail_on == "audit-schema" else 0
    if "audit-schema" in (on or {}):
        audit_rc = 2 if "exit 2" in on["audit-schema"] else audit_rc
    audit = tmp_path / "fake-audit-schema.py"
    audit.write_text(
        "#!/usr/bin/env python3\n"
        f"with open({str(log)!r}, 'a') as handle:\n"
        "    handle.write('audit-schema ENV=prod\\n')\n"
        f"raise SystemExit({audit_rc})\n"
    )
    audit.chmod(0o755)
    return stub, log


def _calls(log):
    if not log.exists():
        return []
    return [shlex.split(line) for line in log.read_text().splitlines()]


def _env_dir(tmp_path, gate="false"):
    env_dir = tmp_path / "prod"
    (env_dir / "generated").mkdir(parents=True)
    (env_dir / "generated" / "abac.auto.tfvars").write_text('tag_assignments = []\n')
    (env_dir / "generated" / "masking_functions.sql").write_text("-- masks\n")
    (env_dir / "env.auto.tfvars").write_text(
        f'uc_tables = ["prod.finance.customers"]\nbusiness_access_enabled = {gate}\n'
    )
    return env_dir


def _make(target, env_dir, stub, *extra):
    return subprocess.run(
        ["make", target, "ENV=prod", f"ENV_DIR={env_dir}",
         f"ACCOUNT_ENV_DIR={env_dir.parent / 'account'}", f"MAKE={stub}",
         f"AUDIT_SCHEMA_SCRIPT={stub.parent / 'fake-audit-schema.py'}", *extra],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
        env=_clean_env(),
    )


def _receipt(env_dir):
    return env_dir / "generated" / ".certified.json"


def _lock(env_dir):
    return env_dir / "generated" / ".governance.lock"


def _mutate_abac(env_dir):
    return f"echo 'tag_assignments = [9]' > '{env_dir / 'generated' / 'abac.auto.tfvars'}'"


def _dead_pid():
    proc = subprocess.Popen(["true"])
    proc.wait()
    return proc.pid


# ── receipt helper ────────────────────────────────────────────────────────────


def test_fingerprint_is_stable_and_ignores_exposure_gate(tmp_path):
    env_dir = _env_dir(tmp_path)
    first = cr.fingerprint(cr.compute_components(env_dir))
    assert cr.fingerprint(cr.compute_components(env_dir)) == first

    cr.persist_gate_open(env_dir)
    assert cr.gate_open(env_dir)
    assert cr.fingerprint(cr.compute_components(env_dir)) == first


@pytest.mark.parametrize(
    "mutate, component",
    [
        (lambda d: (d / "generated" / "abac.auto.tfvars").write_text("tag_assignments = [1]\n"),
         "generated/abac.auto.tfvars"),
        (lambda d: (d / "generated" / "masking_functions.sql").write_text("-- other\n"),
         "generated/masking_functions.sql"),
        (lambda d: (d / "env.auto.tfvars").write_text(
            'uc_tables = ["prod.finance.customers", "prod.finance.accounts"]\n'
            "business_access_enabled = false\n"), "footprint"),
        (lambda d: (d / "data_access").mkdir() or (
            d / "data_access" / "discovered_uc_tables.auto.tfvars").write_text(
            'discovered_uc_tables = ["prod.hr.staff"]\n'), "footprint"),
    ],
)
def test_receipt_goes_stale_when_certified_inputs_change(tmp_path, mutate, component):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    assert cr.check_receipt(env_dir)[0]

    mutate(env_dir)
    current, reason = cr.check_receipt(env_dir)
    assert not current
    assert "config changed since certification" in reason
    assert component in reason


def test_dirty_repo_enforcement_input_makes_receipt_stale(tmp_path, monkeypatch):
    repo = tmp_path / "shared"
    (repo / "tests").mkdir(parents=True)
    (repo / "treatment_config.json").write_text('{"treatments": []}\n')
    (repo / "validate_abac.py").write_text("# gate\n")
    (repo / "README.md").write_text("docs\n")
    (repo / "tests" / "test_x.py").write_text("# test\n")
    monkeypatch.setattr(cr, "REPO_INPUT_ROOT", repo)
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")

    # Docs and tests are not enforcement inputs.
    (repo / "README.md").write_text("more docs\n")
    (repo / "tests" / "test_x.py").write_text("# changed\n")
    assert cr.check_receipt(env_dir)[0]

    # An uncommitted edit to an enforcement input invalidates the receipt.
    (repo / "treatment_config.json").write_text('{"treatments": [1]}\n')
    current, reason = cr.check_receipt(env_dir)
    assert not current
    assert "repo:treatment_config.json" in reason


def test_run_logs_written_after_certify_do_not_invalidate_receipt(tmp_path, monkeypatch):
    # run_parallel_tests.py writes shared/scripts/logs/<suite>/<scenario>.log as
    # each scenario finishes, i.e. during another scenario's certify -> release.
    repo = tmp_path / "shared"
    (repo / "scripts").mkdir(parents=True)
    (repo / "validate_abac.py").write_text("# gate\n")
    monkeypatch.setattr(cr, "REPO_INPUT_ROOT", repo)
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")

    log_dir = repo / "scripts" / "logs" / "aws_20261006_101500"
    log_dir.mkdir(parents=True)
    (log_dir / "quickstart.abc123.log").write_text("# Scenario: quickstart\n")
    (log_dir / "promote.provision.log").write_text("# Provision FAILED\n")
    assert cr.check_receipt(env_dir)[0]


@pytest.mark.parametrize("rel", [
    "modules/x/logs/a.tf",            # an input under a differently located logs/
    "modules/x/logs/policy.sql",
    "logs/a.tf",
    "scripts/debug.log",              # a .log outside scripts/logs/
    "modules/x/apply.log",
    "scripts/logs/aws_1/notes.tf",    # non-.log file inside the run-log dir
])
def test_only_run_log_files_under_scripts_logs_are_excluded(tmp_path, monkeypatch, rel):
    repo = tmp_path / "shared"
    (repo / "scripts").mkdir(parents=True)
    (repo / "validate_abac.py").write_text("# gate\n")
    target = repo / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("x = 1\n")
    monkeypatch.setattr(cr, "REPO_INPUT_ROOT", repo)
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")

    target.write_text("x = 2\n")
    current, reason = cr.check_receipt(env_dir)
    assert not current
    assert f"repo:{rel}" in reason


def test_real_repo_inputs_cover_gate_code_and_registries():
    names = {p.relative_to(cr.SHARED_ROOT).as_posix() for p in cr.repo_input_files()}
    assert {
        "validate_abac.py",
        "treatment_config.json",
        "treatment_derivation.py",
        "tag_vocabulary_registry.json",
        "Makefile.shared",
        "scripts/derive_assignments.py",
        "scripts/footprint.py",
    } <= names
    assert not any(n.startswith(("tests/", "docs/", "examples/")) for n in names)


def test_symlinked_inputs_are_hashed_by_target_content(tmp_path):
    env_dir = _env_dir(tmp_path)
    real = tmp_path / "real-env.auto.tfvars"
    real.write_text((env_dir / "env.auto.tfvars").read_text())
    (env_dir / "env.auto.tfvars").unlink()
    (env_dir / "env.auto.tfvars").symlink_to(real)
    cr.write_receipt(env_dir, "certify")

    real.write_text('uc_tables = ["prod.other.t"]\nbusiness_access_enabled = false\n')
    assert not cr.check_receipt(env_dir)[0]


def test_receipt_records_git_commit_and_inputs(tmp_path):
    env_dir = _env_dir(tmp_path)
    receipt = json.loads(cr.write_receipt(env_dir, "maintain").read_text())
    assert receipt["certified_by"] == "maintain"
    assert "git_commit" in receipt
    assert set(receipt["components"]) >= {
        "env:generated/abac.auto.tfvars",
        "env:generated/masking_functions.sql",
        "env:env.auto.tfvars",
        "footprint",
        "repo:validate_abac.py",
    }


@pytest.mark.parametrize(
    "content, expected",
    [
        ("not json {", "malformed"),
        ("[1, 2]", "malformed"),
        ('{"version": 1, "fingerprint": "x", "components": {}}', "malformed"),
        ('{"version": 3, "fingerprint": 5, "components": {}}', "malformed"),
    ],
)
def test_malformed_receipt_is_rejected(tmp_path, content, expected):
    env_dir = _env_dir(tmp_path)
    _receipt(env_dir).write_text(content)
    current, reason = cr.check_receipt(env_dir)
    assert not current
    assert expected in reason


def test_forged_receipt_is_rejected(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    receipt = json.loads(_receipt(env_dir).read_text())

    # Hand-edited fingerprint to match a changed config: no longer self-consistent.
    (env_dir / "generated" / "abac.auto.tfvars").write_text("tag_assignments = [3]\n")
    receipt["fingerprint"] = cr.fingerprint(cr.compute_components(env_dir))
    _receipt(env_dir).write_text(json.dumps(receipt))
    current, reason = cr.check_receipt(env_dir)
    assert not current
    assert "not self-consistent" in reason


def test_persist_gate_replaces_existing_line_or_appends(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.persist_gate_open(env_dir)
    text = (env_dir / "env.auto.tfvars").read_text()
    assert text.count("business_access_enabled") == 1
    assert "business_access_enabled = true" in text

    (env_dir / "env.auto.tfvars").write_text('uc_tables = ["a.b.c"]')
    cr.persist_gate_open(env_dir)
    assert (env_dir / "env.auto.tfvars").read_text() == (
        'uc_tables = ["a.b.c"]\nbusiness_access_enabled = true\n'
    )


def test_persist_gate_is_atomic_keeps_mode_and_symlink(tmp_path, monkeypatch):
    env_dir = _env_dir(tmp_path)
    real = tmp_path / "real-env.auto.tfvars"
    real.write_text("business_access_enabled = false\n")
    real.chmod(0o600)
    (env_dir / "env.auto.tfvars").unlink()
    (env_dir / "env.auto.tfvars").symlink_to(real)

    replaced = []
    original_replace = os.replace
    monkeypatch.setattr(
        cr.os, "replace", lambda src, dst: (replaced.append(Path(dst)), original_replace(src, dst))
    )
    cr.persist_gate_open(env_dir)

    assert replaced == [real.resolve()]
    assert (env_dir / "env.auto.tfvars").is_symlink()
    assert stat.S_IMODE(real.stat().st_mode) == 0o600
    assert real.read_text() == "business_access_enabled = true\n"
    assert not list(tmp_path.glob(".real-env.auto.tfvars.*.tmp"))


def test_failed_atomic_write_leaves_original_intact(tmp_path, monkeypatch):
    env_dir = _env_dir(tmp_path)
    before = (env_dir / "env.auto.tfvars").read_text()

    def boom(*_args):
        raise OSError("disk full")

    monkeypatch.setattr(cr.os, "replace", boom)
    with pytest.raises(OSError):
        cr.persist_gate_open(env_dir)
    assert (env_dir / "env.auto.tfvars").read_text() == before
    assert not list(env_dir.glob(".env.auto.tfvars.*.tmp"))


def test_warn_only_when_gate_open_without_current_receipt(tmp_path, capsys):
    env_dir = _env_dir(tmp_path, gate="true")
    assert cr.main(["warn", str(env_dir), "--env", "prod"]) == 0
    assert "WARNING" in capsys.readouterr().err

    cr.write_receipt(env_dir, "certify")
    assert cr.main(["warn", str(env_dir), "--env", "prod"]) == 0
    assert capsys.readouterr().err == ""

    closed = _env_dir(tmp_path / "closed")
    assert cr.main(["warn", str(closed), "--env", "prod"]) == 0
    assert capsys.readouterr().err == ""


def test_lock_is_exclusive_and_released_only_by_owner(tmp_path):
    env_dir = _env_dir(tmp_path)
    me = os.getpid()
    assert cr.acquire_lock(env_dir, me, "certify")[0]
    held, message = cr.acquire_lock(env_dir, me + 1, "release")
    assert not held
    assert "held by make certify" in message

    cr.release_lock(env_dir, me + 1)  # not the owner: no-op
    assert _lock(env_dir).exists()
    cr.release_lock(env_dir, me)
    assert not _lock(env_dir).exists()


def test_stale_lock_from_dead_pid_is_taken_over(tmp_path):
    env_dir = _env_dir(tmp_path)
    _lock(env_dir).write_text(json.dumps(
        {"pid": _dead_pid(), "host": socket.gethostname(), "owner": "maintain"}
    ))
    ok, message = cr.acquire_lock(env_dir, os.getpid(), "release")
    assert ok
    assert "stale lock" in message


# ── make certify ──────────────────────────────────────────────────────────────


def test_certify_success_writes_receipt_and_releases_lock(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, _log = _stub(tmp_path)
    result = _make("certify", env_dir, stub)
    assert result.returncode == 0, result.stdout + result.stderr
    receipt = json.loads(_receipt(env_dir).read_text())
    assert receipt["certified_by"] == "certify"
    assert cr.check_receipt(env_dir)[0]
    assert not _lock(env_dir).exists()
    assert not (env_dir / "generated" / ".certified.pending.json").exists()


@pytest.mark.parametrize("failing", ["derive-assignments", "coverage-gate", "audit-rulebook"])
def test_failed_certify_deletes_stale_receipt_and_writes_none(tmp_path, failing):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    stub, _log = _stub(tmp_path, fail_on=failing)
    result = _make("certify", env_dir, stub)
    assert result.returncode != 0
    assert not _receipt(env_dir).exists()
    assert not _lock(env_dir).exists()


@pytest.mark.parametrize("target", ["certify", "maintain"])
def test_mutation_after_gate_blocks_receipt(tmp_path, target):
    env_dir = _env_dir(tmp_path)
    stub, log = _stub(tmp_path, on={"audit-rulebook": _mutate_abac(env_dir)})
    result = _make(target, env_dir, stub)
    assert result.returncode != 0
    assert "inputs changed after the coverage gate ran" in result.stderr
    assert "generated/abac.auto.tfvars" in result.stderr
    assert f"re-run make {target} ENV=prod" in result.stderr
    assert not _receipt(env_dir).exists()


@pytest.mark.parametrize("target", ["certify", "maintain"])
def test_mutation_during_gate_stops_before_apply(tmp_path, target):
    env_dir = _env_dir(tmp_path)
    stub, log = _stub(tmp_path, on={"coverage-gate": _mutate_abac(env_dir)})
    result = _make(target, env_dir, stub)
    assert result.returncode != 0
    assert "apply-governance" not in [c[0] for c in _calls(log)]
    assert not _receipt(env_dir).exists()


# ── make release ──────────────────────────────────────────────────────────────


def test_release_refuses_without_receipt(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log = _stub(tmp_path)
    result = _make("release", env_dir, stub)
    assert result.returncode != 0
    assert _calls(log) == []
    assert "no certification receipt" in result.stderr
    assert "make certify ENV=prod" in result.stderr
    assert "business_access_enabled = false" in (env_dir / "env.auto.tfvars").read_text()
    assert not _lock(env_dir).exists()


def test_release_refuses_stale_receipt(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    (env_dir / "generated" / "abac.auto.tfvars").write_text("tag_assignments = [2]\n")
    stub, log = _stub(tmp_path)
    result = _make("release", env_dir, stub)
    assert result.returncode != 0
    assert _calls(log) == []
    assert "config changed since certification" in result.stderr
    assert "re-run make certify ENV=prod" in result.stderr
    assert "business_access_enabled = false" in (env_dir / "env.auto.tfvars").read_text()


def test_release_refuses_forged_receipt(tmp_path):
    env_dir = _env_dir(tmp_path)
    _receipt(env_dir).write_text(json.dumps({
        "version": cr.RECEIPT_VERSION, "env": "prod", "certified_by": "certify",
        "gated_at": "2026-10-06T00:00:00+00:00", "certified_at": "2026-10-06T00:00:00+00:00",
        "fingerprint": "0" * 64, "components": {},
    }))
    stub, log = _stub(tmp_path)
    result = _make("release", env_dir, stub)
    assert result.returncode != 0
    assert _calls(log) == []
    assert "not self-consistent" in result.stderr


def test_release_forces_gate_for_apply_then_persists_and_verifies(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    stub, log = _stub(tmp_path)
    result = _make("release", env_dir, stub, "VERIFY_KEY_COLUMN= customer_id ")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _calls(log) == [
        ["_derive-before-exposure", "ENV=prod", "APPLY_FLAGS=-var=business_access_enabled=true"],
        ["apply", "ENV=prod", "APPLY_FLAGS=-var=business_access_enabled=true",
         "_EXPOSURE_DERIVED=1"],
        ["verify-access", "ENV=prod", "VERIFY_KEY_COLUMN=customer_id"],
    ]
    assert cr.gate_open(env_dir)
    # Opening the gate must not invalidate the certification.
    assert cr.check_receipt(env_dir)[0]
    assert "Release complete (prod)" in result.stdout
    assert not _lock(env_dir).exists()


def test_release_without_key_still_calls_verify_access(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    stub, log = _stub(tmp_path)
    result = _make("release", env_dir, stub)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _calls(log)[-1] == ["verify-access", "ENV=prod"]


def test_release_failed_apply_prints_rollback_and_does_not_persist(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    stub, log = _stub(tmp_path, fail_on="apply")
    result = _make("release", env_dir, stub)
    assert result.returncode != 0
    assert [c[0] for c in _calls(log)] == ["_derive-before-exposure", "apply"]
    assert not cr.gate_open(env_dir)
    assert "PARTIALLY OPENED" in result.stderr
    assert "still has business_access_enabled = false, so run: make apply ENV=prod" in result.stderr


def test_release_mutation_during_apply_is_not_persisted(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    stub, log = _stub(tmp_path, on={"apply": _mutate_abac(env_dir)})
    result = _make("release", env_dir, stub)
    assert result.returncode != 0
    assert [c[0] for c in _calls(log)] == ["_derive-before-exposure", "apply"]
    assert not cr.gate_open(env_dir)
    assert "config changed since certification" in result.stderr
    assert "PARTIALLY OPENED" in result.stderr


def test_release_verify_failure_explains_how_to_close(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    stub, _log = _stub(tmp_path, fail_on="verify-access")
    result = _make("release", env_dir, stub)
    assert result.returncode != 0
    assert cr.gate_open(env_dir)
    assert "set business_access_enabled = false" in result.stderr
    assert "make apply ENV=prod" in result.stderr


def test_release_rejects_account_env(tmp_path):
    stub, log = _stub(tmp_path)
    result = subprocess.run(
        ["make", "release", "ENV=account", f"MAKE={stub}"],
        cwd=CLOUD_ROOT, text=True, capture_output=True, env=_clean_env(),
    )
    assert result.returncode != 0
    assert _calls(log) == []


# ── locking across targets ────────────────────────────────────────────────────


@pytest.mark.parametrize("target", ["release", "certify", "maintain"])
def test_targets_refuse_while_env_lock_is_held(tmp_path, target):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    assert cr.acquire_lock(env_dir, os.getpid(), "maintain")[0]
    stub, log = _stub(tmp_path)
    result = _make(target, env_dir, stub)
    assert result.returncode != 0
    assert _calls(log) == []
    assert "prod is locked" in result.stderr
    assert "held by make maintain" in result.stderr
    # The refused target must not have touched the holder's lock or receipt.
    assert _lock(env_dir).exists()
    assert _receipt(env_dir).exists()


def test_targets_take_over_stale_lock(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    _lock(env_dir).write_text(json.dumps(
        {"pid": _dead_pid(), "host": socket.gethostname(), "owner": "certify"}
    ))
    stub, _log = _stub(tmp_path)
    result = _make("release", env_dir, stub)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "stale lock" in result.stdout
    assert not _lock(env_dir).exists()


def test_parallel_make_maintain_release_serialises_on_lock(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    stub, log = _stub(tmp_path, sleep=1)
    result = subprocess.run(
        ["make", "-j2", "maintain", "release", "ENV=prod",
         f"ENV_DIR={env_dir}", f"MAKE={stub}",
         f"AUDIT_SCHEMA_SCRIPT={stub.parent / 'fake-audit-schema.py'}"],
        cwd=CLOUD_ROOT, text=True, capture_output=True, env=_clean_env(),
    )
    assert result.returncode != 0
    assert "prod is locked" in result.stderr
    calls = [c[0] for c in _calls(log)]
    # Exactly one pipeline ran; the other was refused before any stage.
    assert calls in (
        ["audit-schema", "derive-assignments", "coverage-gate", "validate-generated",
         "apply-governance", "audit-rulebook"],
        ["_derive-before-exposure", "apply", "verify-access"],
    )
    assert not _lock(env_dir).exists()


# ── make maintain ─────────────────────────────────────────────────────────────


def test_maintain_is_ordered_governance_only_and_refreshes_receipt(tmp_path):
    env_dir = _env_dir(tmp_path, gate="true")
    before = (env_dir / "env.auto.tfvars").read_text()
    stub, log = _stub(tmp_path)
    result = _make("maintain", env_dir, stub, "APPLY_FLAGS=-var=business_access_enabled=false")
    assert result.returncode == 0, result.stdout + result.stderr
    calls = _calls(log)
    assert calls == [
        ["audit-schema", "ENV=prod"],
        ["derive-assignments", "ENV=prod"],
        ["coverage-gate", "ENV=prod"],
        ["validate-generated", "ENV=prod"],
        ["apply-governance", "ENV=prod", "APPLY_FLAGS=", "_EXPOSURE_DERIVED=1"],
        ["audit-rulebook", "ENV=prod"],
    ]
    assert not any(c[0] in ("apply", "apply-genie", "generate-delta", "generate") for c in calls)
    assert not any("business_access_enabled" in arg for c in calls for arg in c)
    assert (env_dir / "env.auto.tfvars").read_text() == before
    receipt = json.loads(_receipt(env_dir).read_text())
    assert receipt["certified_by"] == "maintain"
    assert not _lock(env_dir).exists()


@pytest.mark.parametrize(
    "failing, expected",
    [
        ("audit-schema", ["audit-schema"]),
        ("derive-assignments", ["audit-schema", "derive-assignments"]),
        ("coverage-gate", ["audit-schema", "derive-assignments", "coverage-gate"]),
        ("audit-rulebook", ["audit-schema", "derive-assignments", "coverage-gate",
                            "validate-generated", "apply-governance", "audit-rulebook"]),
    ],
)
def test_failed_maintain_invalidates_existing_receipt(tmp_path, failing, expected):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    stub, log = _stub(tmp_path, fail_on=failing)
    result = _make("maintain", env_dir, stub)
    assert result.returncode != 0
    assert [c[0] for c in _calls(log)] == expected
    assert not _receipt(env_dir).exists()
    assert not _lock(env_dir).exists()


def test_maintain_audit_drift_points_to_native_classification(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log = _stub(tmp_path, fail_on="audit-schema")
    result = _make("maintain", env_dir, stub)
    assert result.returncode != 0
    assert [c[0] for c in _calls(log)] == ["audit-schema"]
    assert "native UC classification" in result.stderr
    assert "never LLM-tags" in result.stderr


def test_maintain_audit_error_is_not_described_as_drift(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, log = _stub(tmp_path, on={"audit-schema": "exit 2"})
    result = _make("maintain", env_dir, stub)
    assert result.returncode == 2
    assert [c[0] for c in _calls(log)] == ["audit-schema"]
    assert "audit-schema failed (error, not drift)" in result.stderr
    assert "reported drift" not in result.stderr
    assert "native UC classification" not in result.stderr
    assert "LLM-tags" not in result.stderr


@pytest.mark.parametrize(
    "audit_rc, expected, unexpected",
    [
        (1, "audit-schema reported drift", "error, not drift"),
        (2, "audit-schema failed (error, not drift)", "native UC classification"),
    ],
)
def test_maintain_preserves_direct_audit_exit_code_with_real_nested_make(
    tmp_path, audit_rc, expected, unexpected,
):
    env_dir = _env_dir(tmp_path)
    audit = tmp_path / "direct-audit.py"
    audit.write_text(f"raise SystemExit({audit_rc})\n")
    result = subprocess.run(
        [
            "make", "maintain", "ENV=prod", f"ENV_DIR={env_dir}",
            f"ACCOUNT_ENV_DIR={env_dir.parent / 'account'}",
            f"AUDIT_SCHEMA_SCRIPT={audit}",
        ],
        cwd=CLOUD_ROOT,
        text=True,
        capture_output=True,
        env=_clean_env(),
    )
    assert result.returncode != 0
    assert expected in result.stderr
    assert unexpected not in result.stderr


# ── make apply warning ────────────────────────────────────────────────────────


def test_apply_recipe_warns_on_uncertified_exposure(tmp_path):
    result = subprocess.run(
        ["make", "-n", "apply", "ENV=prod", f"ENV_DIR={tmp_path}",
         f"ACCOUNT_ENV_DIR={tmp_path / 'account'}"],
        cwd=CLOUD_ROOT, text=True, capture_output=True, env=_clean_env(),
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.count(f'" warn "{tmp_path}" --env "prod"') == 2


def test_generate_delta_help_is_marked_legacy():
    result = subprocess.run(
        ["make", "help"], cwd=CLOUD_ROOT, text=True, capture_output=True, env=_clean_env(),
    )
    line = next(l for l in result.stdout.splitlines() if "generate-delta" in l)
    assert "[Legacy]" in line


# ── follow-up review: input coverage ──────────────────────────────────────────


def test_repo_inputs_follow_directory_symlinks_cycle_safely(tmp_path, monkeypatch):
    repo = tmp_path / "shared"
    real_overlays = tmp_path / "overlays-real"
    (real_overlays / "anz").mkdir(parents=True)
    (real_overlays / "anz" / "patterns.json").write_text('{"tfn": 1}\n')
    repo.mkdir()
    (repo / "validate_abac.py").write_text("# gate\n")
    (repo / "countries").symlink_to(real_overlays, target_is_directory=True)
    (real_overlays / "loop").symlink_to(real_overlays, target_is_directory=True)
    monkeypatch.setattr(cr, "REPO_INPUT_ROOT", repo)

    names = {p.relative_to(repo).as_posix() for p in cr.repo_input_files()}
    assert "countries/anz/patterns.json" in names
    assert not any("loop" in n for n in names)

    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    (real_overlays / "anz" / "patterns.json").write_text('{"tfn": 2}\n')
    current, reason = cr.check_receipt(env_dir)
    assert not current
    assert "repo:countries/anz/patterns.json" in reason


@pytest.mark.parametrize("rel", [
    "extra.auto.tfvars",
    "generated/spaces/finance.auto.tfvars",
    "data_access/overrides.auto.tfvars.json",
    "generated/space_config.yaml",
    "override.tf",
])
def test_new_env_input_file_invalidates_receipt(tmp_path, rel):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    (env_dir / rel).parent.mkdir(parents=True, exist_ok=True)
    (env_dir / rel).write_text("x = 1\n")
    current, reason = cr.check_receipt(env_dir)
    assert not current
    assert f"env:{rel}" in reason


def test_account_layer_input_change_invalidates_receipt(tmp_path):
    env_dir = _env_dir(tmp_path)
    account = tmp_path / "account"
    account.mkdir()
    (account / "env.auto.tfvars").write_text("manage_groups = false\n")
    cr.write_receipt(env_dir, "certify")
    (account / "env.auto.tfvars").write_text("manage_groups = true\n")
    current, reason = cr.check_receipt(env_dir)
    assert not current
    assert "account:env.auto.tfvars" in reason


def test_volatile_and_derived_env_files_do_not_invalidate_receipt(tmp_path):
    env_dir = _env_dir(tmp_path)
    account = tmp_path / "account"
    (env_dir / "data_access").mkdir()
    account.mkdir()
    cr.write_receipt(env_dir, "certify")

    for path, text in {
        env_dir / "terraform.tfstate": "{}",
        env_dir / "terraform.tfstate.backup": "{}",
        env_dir / "data_access" / "terraform.tfstate": "{}",
        env_dir / ".terraform" / "providers.tf": "x",
        env_dir / ".terraform.lock.hcl": "x",
        env_dir / "data_access" / ".data_access.apply.sha": "x",
        env_dir / "apply.log": "x",
        env_dir / "generated" / "generated_response.md": "x",
        # promote / _prepare-classification outputs, regenerated before every apply
        env_dir / "abac.auto.tfvars": "x = 1",
        env_dir / "data_access" / "abac.auto.tfvars": "x = 1",
        env_dir / "data_access" / "masking_functions.sql": "-- x",
        env_dir / "data_access" / "classification.auto.tfvars": "x = 1",
        env_dir / "generated" / "genie_space_derived_acl_groups.auto.tfvars": "x = 1",
        account / "abac.auto.tfvars": "x = 1",
        account / "terraform.tfstate": "{}",
    }.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    assert cr.check_receipt(env_dir)[0]


def test_data_access_env_symlink_is_hashed_without_gate(tmp_path):
    env_dir = _env_dir(tmp_path)
    (env_dir / "data_access").mkdir()
    (env_dir / "data_access" / "env.auto.tfvars").symlink_to("../env.auto.tfvars")
    cr.write_receipt(env_dir, "certify")
    cr.persist_gate_open(env_dir)
    assert cr.check_receipt(env_dir)[0]
    assert (env_dir / "data_access" / "env.auto.tfvars").is_symlink()


def test_certify_tolerates_promote_rewriting_derived_files(tmp_path):
    env_dir = _env_dir(tmp_path)
    split = env_dir / "data_access" / "abac.auto.tfvars"
    stub, _log = _stub(tmp_path, on={
        "apply-governance": f"mkdir -p '{split.parent}'; echo 'x = 2' > '{split}'"
    })
    result = _make("certify", env_dir, stub)
    assert result.returncode == 0, result.stdout + result.stderr
    assert cr.check_receipt(env_dir)[0]


# ── follow-up review: receipt metadata ────────────────────────────────────────


@pytest.mark.parametrize("field, value, expected", [
    ("env", "dev", "is for env 'dev', not 'prod'"),
    ("certified_by", 7, "bad metadata"),
    ("certified_by", "rehearse", "bad metadata"),
    ("certified_at", "yesterday", "bad metadata"),
    ("gated_at", None, "bad metadata"),
])
def test_receipt_metadata_is_validated(tmp_path, field, value, expected):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    receipt = json.loads(_receipt(env_dir).read_text())
    receipt[field] = value
    _receipt(env_dir).write_text(json.dumps(receipt))
    current, reason = cr.check_receipt(env_dir, "prod")
    assert not current
    assert expected in reason


# ── follow-up review: bound verify-then-write gate ────────────────────────────


def test_open_gate_requires_lock_ownership(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    with pytest.raises(cr.ReceiptError, match="does not hold"):
        cr.release_open_gate(env_dir, os.getpid(), "prod")
    assert not cr.gate_open(env_dir)


def test_open_gate_refuses_stale_receipt_without_writing(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    assert cr.acquire_lock(env_dir, os.getpid(), "release")[0]
    (env_dir / "generated" / "abac.auto.tfvars").write_text("tag_assignments = [4]\n")
    with pytest.raises(cr.ReceiptError, match="gate was NOT persisted"):
        cr.release_open_gate(env_dir, os.getpid(), "prod")
    assert not cr.gate_open(env_dir)


def test_open_gate_reverifies_after_write(tmp_path, monkeypatch):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    assert cr.acquire_lock(env_dir, os.getpid(), "release")[0]
    original = cr.persist_gate_open

    def write_and_race(d):
        original(d)
        (d / "generated" / "abac.auto.tfvars").write_text("tag_assignments = [5]\n")

    monkeypatch.setattr(cr, "persist_gate_open", write_and_race)
    with pytest.raises(cr.ReceiptError, match="after persisting business_access_enabled"):
        cr.release_open_gate(env_dir, os.getpid(), "prod")


def test_open_gate_happy_path_under_lock(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    assert cr.acquire_lock(env_dir, os.getpid(), "release")[0]
    cr.release_open_gate(env_dir, os.getpid(), "prod")
    assert cr.gate_open(env_dir)
    assert cr.check_receipt(env_dir)[0]


# ── follow-up review: lock robustness ─────────────────────────────────────────


@pytest.mark.parametrize("content", [
    "",
    "not json",
    "[1]",
    json.dumps({"host": socket.gethostname(), "owner": "certify"}),
    json.dumps({"pid": "123", "host": socket.gethostname()}),
    json.dumps({"pid": 123}),
])
def test_malformed_lock_is_treated_as_held(tmp_path, content):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    _lock(env_dir).write_text(content)
    stub, log = _stub(tmp_path)
    result = _make("release", env_dir, stub)
    assert result.returncode != 0
    assert _calls(log) == []
    assert "cannot determine the owner" in result.stderr
    assert str(_lock(env_dir)) in result.stderr
    assert "delete" in result.stderr and "manually" in result.stderr
    assert _lock(env_dir).read_text() == content


def test_other_host_lock_is_held_even_with_dead_pid(tmp_path):
    env_dir = _env_dir(tmp_path)
    _lock(env_dir).write_text(json.dumps(
        {"pid": _dead_pid(), "host": "some-other-host", "owner": "certify"}
    ))
    ok, message = cr.acquire_lock(env_dir, os.getpid(), "release")
    assert not ok
    assert "some-other-host" in message
    assert _lock(env_dir).exists()


def test_dry_run_creates_no_lock_or_env_dirs(tmp_path):
    env_dir = tmp_path / "never"
    for target in ("release", "maintain", "certify"):
        result = subprocess.run(
            ["make", "--dry-run", target, "ENV=prod", f"ENV_DIR={env_dir}",
             f"ACCOUNT_ENV_DIR={tmp_path / 'account'}"],
            cwd=CLOUD_ROOT, text=True, capture_output=True, env=_clean_env(),
        )
        assert result.returncode == 0, result.stdout + result.stderr
    assert not env_dir.exists()


# maintain runs the audit script directly (not via $(MAKE)), so its first stub
# stage after the lock is derive-assignments.
_SLOW_STAGE = {"release": "apply", "certify": "derive-assignments", "maintain": "derive-assignments"}


def _default_signals():
    # A suite launched in the background (INT ignored) or under nohup (HUP
    # ignored) passes SIG_IGN down, and a shell cannot trap a signal ignored
    # on entry, so the signal would never land. Model an interactive terminal.
    for sig in (signal.SIGINT, signal.SIGHUP, signal.SIGTERM):
        signal.signal(sig, signal.SIG_DFL)


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT, signal.SIGHUP])
@pytest.mark.parametrize("target", ["release", "certify", "maintain"])
def test_signal_mid_run_releases_lock(tmp_path, target, sig):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    stub, log = _stub(tmp_path, on={_SLOW_STAGE[target]: "exec sleep 30"})
    proc = subprocess.Popen(
        ["make", target, "ENV=prod", f"ENV_DIR={env_dir}",
         f"ACCOUNT_ENV_DIR={tmp_path / 'account'}", f"MAKE={stub}",
         f"AUDIT_SCHEMA_SCRIPT={stub.parent / 'fake-audit-schema.py'}"],
        cwd=CLOUD_ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=_clean_env(), start_new_session=True, preexec_fn=_default_signals,
    )
    try:
        deadline = time.time() + 15
        while not (log.exists() and _SLOW_STAGE[target] in log.read_text()):
            assert time.time() < deadline, "slow stage never started"
            assert proc.poll() is None, proc.communicate()
            time.sleep(0.05)
        assert _lock(env_dir).exists()
        os.killpg(proc.pid, sig)
    finally:
        if proc.poll() is None:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)
    # The recipe shell's trap runs the unlock after make itself has gone.
    deadline = time.time() + 10
    while _lock(env_dir).exists() and time.time() < deadline:
        time.sleep(0.05)
    assert not _lock(env_dir).exists()
    assert proc.returncode != 0
    assert not cr.gate_open(env_dir)


# ── merge with main: placeholder guard + access_tier_groups coverage ──────────


def test_access_tier_groups_change_invalidates_receipt(tmp_path):
    env_dir = _env_dir(tmp_path)
    env_file = env_dir / "env.auto.tfvars"
    env_file.write_text(env_file.read_text() + 'access_tier_groups = ["analysts"]\n')
    cr.write_receipt(env_dir, "certify")
    env_file.write_text(env_file.read_text().replace('["analysts"]', '["analysts", "admins"]'))
    current, reason = cr.check_receipt(env_dir)
    assert not current
    assert "env:env.auto.tfvars" in reason


@pytest.mark.parametrize("target", ["release", "certify", "maintain"])
def test_placeholder_genie_space_id_refuses_before_lock(tmp_path, target):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    env_file = env_dir / "env.auto.tfvars"
    env_file.write_text(
        env_file.read_text()
        + 'genie_spaces = [{ name = "Finance", genie_space_id = "<your-genie-space-id>" }]\n'
    )
    stub, log = _stub(tmp_path)
    result = _make(target, env_dir, stub)
    assert result.returncode != 0
    assert "<your-genie-space-id>" in result.stdout + result.stderr
    assert _calls(log) == []
    assert not _lock(env_dir).exists()
    assert "PARTIALLY OPENED" not in result.stderr
