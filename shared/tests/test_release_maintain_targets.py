"""Regression tests for the certification receipt, `make release`, and `make maintain`."""

import json
import os
import shlex
import socket
import stat
import subprocess
import sys
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
    body = ["#!/bin/sh", f"printf '%s\\n' \"$*\" >> \"{log}\""]
    if sleep:
        body.append(f"sleep {sleep}")
    for target, action in (on or {}).items():
        body.append(f'if [ "$1" = "{target}" ]; then {action}; fi')
    if fail_on:
        body.append(f'[ "$1" = "{fail_on}" ] && exit 1')
    body.append("exit 0")
    stub.write_text("\n".join(body) + "\n")
    stub.chmod(0o755)
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
        ["make", target, "ENV=prod", f"ENV_DIR={env_dir}", f"MAKE={stub}", *extra],
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
        "generated/abac.auto.tfvars",
        "generated/masking_functions.sql",
        "ddl/_fetched.sql",
        "data_access/discovered_uc_tables.auto.tfvars",
        "footprint",
        "env.auto.tfvars",
        "repo:validate_abac.py",
    }


@pytest.mark.parametrize(
    "content, expected",
    [
        ("not json {", "malformed"),
        ("[1, 2]", "malformed"),
        ('{"version": 1, "fingerprint": "x", "components": {}}', "malformed"),
        ('{"version": 2, "fingerprint": 5, "components": {}}', "malformed"),
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
    _receipt(env_dir).write_text(json.dumps(
        {"version": 2, "fingerprint": "0" * 64, "components": {}}
    ))
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
        ["apply", "ENV=prod", "APPLY_FLAGS=-var=business_access_enabled=true"],
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
    assert [c[0] for c in _calls(log)] == ["apply"]
    assert not cr.gate_open(env_dir)
    assert "PARTIALLY OPENED" in result.stderr
    assert "still has business_access_enabled = false, so run: make apply ENV=prod" in result.stderr


def test_release_mutation_during_apply_is_not_persisted(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    stub, log = _stub(tmp_path, on={"apply": _mutate_abac(env_dir)})
    result = _make("release", env_dir, stub)
    assert result.returncode != 0
    assert [c[0] for c in _calls(log)] == ["apply"]
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
         f"ENV_DIR={env_dir}", f"MAKE={stub}"],
        cwd=CLOUD_ROOT, text=True, capture_output=True, env=_clean_env(),
    )
    assert result.returncode != 0
    assert "prod is locked" in result.stderr
    calls = [c[0] for c in _calls(log)]
    # Exactly one pipeline ran; the other was refused before any stage.
    assert calls in (
        ["audit-schema", "derive-assignments", "coverage-gate", "validate-generated",
         "apply-governance", "audit-rulebook"],
        ["apply", "verify-access"],
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
        ["apply-governance", "ENV=prod", "APPLY_FLAGS="],
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


# ── make apply warning ────────────────────────────────────────────────────────


def test_apply_recipe_warns_on_uncertified_exposure(tmp_path):
    result = subprocess.run(
        ["make", "-n", "apply", "ENV=prod", f"ENV_DIR={tmp_path}",
         f"ACCOUNT_ENV_DIR={tmp_path / 'account'}"],
        cwd=CLOUD_ROOT, text=True, capture_output=True, env=_clean_env(),
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.count(f'certification_receipt.py" warn "{tmp_path}"') == 2


def test_generate_delta_help_is_marked_legacy():
    result = subprocess.run(
        ["make", "help"], cwd=CLOUD_ROOT, text=True, capture_output=True, env=_clean_env(),
    )
    line = next(l for l in result.stdout.splitlines() if "generate-delta" in l)
    assert "[Legacy]" in line
