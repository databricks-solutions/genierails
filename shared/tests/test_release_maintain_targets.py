"""Regression tests for the certification receipt, `make release`, and `make maintain`."""

import json
import os
import shlex
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


def _stub(tmp_path, fail_on=None):
    """Recursive-make stub: logs its args; exits 1 when its first arg is fail_on."""
    log = tmp_path / "recursive-make.log"
    stub = tmp_path / "record-make"
    fail = f'[ "$1" = "{fail_on}" ] && exit 1\n' if fail_on else ""
    stub.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> \"{log}\"\n"
        f"{fail}"
        "exit 0\n"
    )
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


def test_receipt_records_git_commit_and_inputs(tmp_path):
    env_dir = _env_dir(tmp_path)
    receipt = json.loads(cr.write_receipt(env_dir, "maintain").read_text())
    assert receipt["certified_by"] == "maintain"
    assert receipt["git_commit"] == receipt["components"]["git_commit"]
    assert set(receipt["components"]) >= {
        "generated/abac.auto.tfvars",
        "generated/masking_functions.sql",
        "ddl/_fetched.sql",
        "footprint",
        "env.auto.tfvars",
    }


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


# ── make certify ──────────────────────────────────────────────────────────────


def test_certify_success_writes_receipt(tmp_path):
    env_dir = _env_dir(tmp_path)
    stub, _log = _stub(tmp_path)
    result = _make("certify", env_dir, stub)
    assert result.returncode == 0, result.stdout + result.stderr
    receipt = json.loads((env_dir / "generated" / ".certified.json").read_text())
    assert receipt["certified_by"] == "certify"
    assert cr.check_receipt(env_dir)[0]


@pytest.mark.parametrize("failing", ["derive-assignments", "coverage-gate", "audit-rulebook"])
def test_failed_certify_deletes_stale_receipt_and_writes_none(tmp_path, failing):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    stub, _log = _stub(tmp_path, fail_on=failing)
    result = _make("certify", env_dir, stub)
    assert result.returncode != 0
    assert not (env_dir / "generated" / ".certified.json").exists()


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


def test_release_without_key_still_calls_verify_access(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    stub, log = _stub(tmp_path)
    result = _make("release", env_dir, stub)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _calls(log)[-1] == ["verify-access", "ENV=prod"]


def test_release_failed_apply_does_not_persist_gate(tmp_path):
    env_dir = _env_dir(tmp_path)
    cr.write_receipt(env_dir, "certify")
    stub, log = _stub(tmp_path, fail_on="apply")
    result = _make("release", env_dir, stub)
    assert result.returncode != 0
    assert [c[0] for c in _calls(log)] == ["apply"]
    assert not cr.gate_open(env_dir)


def test_release_rejects_account_env(tmp_path):
    stub, log = _stub(tmp_path)
    result = subprocess.run(
        ["make", "release", "ENV=account", f"MAKE={stub}"],
        cwd=CLOUD_ROOT, text=True, capture_output=True, env=_clean_env(),
    )
    assert result.returncode != 0
    assert _calls(log) == []


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
    receipt = json.loads((env_dir / "generated" / ".certified.json").read_text())
    assert receipt["certified_by"] == "maintain"


@pytest.mark.parametrize(
    "failing, expected",
    [
        ("derive-assignments", ["audit-schema", "derive-assignments"]),
        ("coverage-gate", ["audit-schema", "derive-assignments", "coverage-gate"]),
    ],
)
def test_maintain_stops_at_first_failure_without_receipt_refresh(tmp_path, failing, expected):
    env_dir = _env_dir(tmp_path)
    stub, log = _stub(tmp_path, fail_on=failing)
    result = _make("maintain", env_dir, stub)
    assert result.returncode != 0
    assert [c[0] for c in _calls(log)] == expected
    assert not (env_dir / "generated" / ".certified.json").exists()


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
