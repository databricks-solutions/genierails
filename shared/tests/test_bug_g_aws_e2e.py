"""AWS-shaped Bug G staged-policy regressions using the real spec builder."""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import generate_abac
from governance_policies import build_model
from scripts import policy_generations as pg
from scripts import staged_policy_apply as spa
from tests import test_staged_policy_apply as base
from tests.test_staged_policy_apply import (
    Harness,
    TIERS,
    _assert_live_generated_principals,
    _remove_sql_hash,
    _remove_treatment_tag_state,
    _replace_deleted_principals_with_unknown,
    live_state,
)
from verify_effective_access import DEFAULT_ADMIN_TIER, load_spec_from_tfvars


CLASSES = {
    "card": ["credit_card"],
    "cvv": ["card_security_code"],
    "email": ["email_address"],
    "phone": ["phone_number"],
    "ssn": ["us_ssn"],
    "acct": ["bank_account_number"],
    "email2": ["email_address"],
    "phone2": ["phone_number"],
    "ssn2": ["us_ssn"],
}
CVV_CHECKS = {"cat.sch.t.cvv", "cat.sch.t.acct"}


def _write_env(env_dir: Path) -> dict:
    env_dir.mkdir(parents=True, exist_ok=True)
    settings = {"access_tier_groups": list(TIERS), "treatment_versions": {},
                "raw_exempt_principals": []}
    columns = {f"cat.sch.t.{name}": classes for name, classes in CLASSES.items()}
    model = build_model(columns, {column: "STRING" for column in columns}, settings)
    generated = env_dir / "generated"
    generated.mkdir()
    tfvars = generated / "abac.auto.tfvars"
    sql = generated / "masking_functions.sql"
    tfvars.write_text("tag_policies = [\n]\ntag_assignments = [\n]\nfgac_policies = [\n]\n")
    sql.write_text("")
    generate_abac.write_deterministic_model(tfvars, sql, model)
    (env_dir / "env.auto.tfvars").write_text(
        'governance_mode = "deterministic"\n'
        f"access_tier_groups = {json.dumps(TIERS)}\n"
        "raw_exempt_principals = []\n")
    layer = env_dir / "data_access"
    layer.mkdir()
    (layer / tfvars.name).write_text(tfvars.read_text())
    (layer / sql.name).write_text(sql.read_text())
    return pg.desired_generation(env_dir)


class ResolvingHarness(Harness):
    """Resolve every derived membership as the live verifier does before querying."""

    real_groups = set(TIERS) | {DEFAULT_ADMIN_TIER}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.validate_real_specs = True
        self.resolve_failures: list[str] = []

    def run(self, argv, env=None, check=False):
        if argv[:2] == ["/bin/sh", "-c"]:
            try:
                spec = load_spec_from_tfvars(
                    Path(env["GENIERAILS_VERIFY_TFVARS"]),
                    env_file=Path(env["GENIERAILS_VERIFY_ENV_FILE"]), key_column="id")
            except ValueError as exc:
                stage = pg.load_stage_record(self.env_dir)["stage"]
                self.calls.append(f"verify-access-refused:{stage}")
                self.resolve_failures.append(str(exc))
                return SimpleNamespace(returncode=2)
            unknown = sorted(
                membership
                for principal in spec.principals
                for membership in spec.principal_memberships.get(principal, (principal,))
                if membership not in self.real_groups
            )
            if unknown:
                stage = pg.load_stage_record(self.env_dir)["stage"]
                self.calls.append(f"verify-access-resolve-failed:{stage}")
                self.resolve_failures.append(
                    f"Tier group {unknown[0]!r} resolved to 0 exact account-group matches")
                return SimpleNamespace(returncode=2)
        return super().run(argv, env=env, check=check)


def _setup(env_dir, monkeypatch, tmp_path, *, with_existing_journal):
    harness = ResolvingHarness(env_dir, monkeypatch, tmp_path)
    desired = _write_env(env_dir)
    assert harness.apply() == 0
    assert harness.controller().confirm() == 0
    harness.calls.clear()
    harness.verify_specs.clear()
    assert len(base.live_policies(live_state(env_dir))) >= 9
    _replace_deleted_principals_with_unknown(env_dir)
    _remove_sql_hash(env_dir)
    _remove_treatment_tag_state(env_dir)
    harness.live_treatment_tags = dict(desired["tags"])
    if not with_existing_journal:
        spa.journal_path(env_dir).unlink()
    return harness, desired


def _assert_created_proof(harness):
    assert harness.verify_specs, harness.calls
    spec = harness.verify_specs[0]
    checks = {f"{check.table}.{check.column}": check for check in spec.column_masks}
    assert set(checks) == CVV_CHECKS
    for check in checks.values():
        assert set(check.masked_principals) == set(TIERS)
        assert check.unmasked_principals == ()
    assert pg.UNKNOWN not in {principal.lower() for principal in spec.principals}
    assert len(spec.mask_config["fgac_policies"]) == 2
    assert not any(
        pg.UNKNOWN in (policy.get("to_principals") or []) + (policy.get("except_principals") or [])
        for policy in spec.mask_config["fgac_policies"]
    )


@pytest.mark.parametrize("with_existing_journal", [False, True])
def test_aws_shape_full_rehearse_reaches_grant(tmp_path, monkeypatch, with_existing_journal):
    env_dir = tmp_path / "envs" / "dev"
    harness, desired = _setup(
        env_dir, monkeypatch, tmp_path, with_existing_journal=with_existing_journal)

    assert harness.apply() == 0, (harness.calls, harness.resolve_failures)

    assert not harness.resolve_failures
    for phase in ("verify-access:create", "tighten", "inventory:tighten", "switch",
                  "verify-access:switch", "grant"):
        assert phase in harness.calls
    _assert_created_proof(harness)
    switched = harness.verify_specs[-1]
    assert {f"{check.table}.{check.column}" for check in switched.column_masks} == set(desired["tags"])
    assert not harness.invariant_problems
    _assert_live_generated_principals(env_dir, desired)


def test_aws_shape_resumes_post_create_journal_and_reaches_grant(tmp_path, monkeypatch):
    env_dir = tmp_path / "envs" / "dev"
    harness, desired = _setup(env_dir, monkeypatch, tmp_path, with_existing_journal=False)
    harness.fail_verify = {"create"}
    assert harness.apply() == 1
    transition = spa.load_journal(env_dir)["transition"]
    assert transition["completed"] == ["create"]
    assert transition["failed_phase"] == "verify-created"
    harness.calls.clear()
    harness.verify_specs.clear()
    harness.resolve_failures.clear()

    assert harness.apply() == 0

    assert "create" not in harness.calls
    assert harness.calls.index("verify-access:create") < harness.calls.index("tighten")
    assert harness.calls.index("tighten") < harness.calls.index("switch")
    _assert_created_proof(harness)
    assert not harness.invariant_problems
    _assert_live_generated_principals(env_dir, desired)


def _forge_history(journal):
    journal["history"] = [entry for entry in journal.get("history") or []
                          if entry.get("reason") != "principal-only drift adopted from state"]


def _forge_marker_fp(journal):
    journal["adopted_unverified"]["fingerprint"] = "deadbeefdeadbeef"


def _forge_marker_missing(journal):
    journal.pop("adopted_unverified", None)


def _forge_marker_restore(journal):
    journal["adopted_unverified"]["restore_from"] = "deadbeefdeadbeef"


def _forge_history_to(journal):
    for entry in journal.get("history") or []:
        if entry.get("reason") == "principal-only drift adopted from state":
            entry["to"] = "deadbeefdeadbeef"


@pytest.mark.parametrize("forge", [
    "history", "marker_fp", "marker_missing", "marker_restore", "history_to",
    "snapshot_edit", "snapshot_edit_cvv_unknown",
])
def test_forged_adoption_never_drops_policies(tmp_path, monkeypatch, forge):
    env_dir = tmp_path / "envs" / "dev"
    harness, desired = _setup(env_dir, monkeypatch, tmp_path, with_existing_journal=False)
    harness.fail_verify = {"create"}
    assert harness.apply() == 1
    journal = spa.load_journal(env_dir)
    transition = journal["transition"]
    assert transition["failed_phase"] == "verify-created"
    adopted = transition["adopted_from_state"]
    if forge.startswith("snapshot_edit"):
        snapshot_path = pg.ledger_dir(env_dir) / f"{adopted}.json"
        snapshot = json.loads(snapshot_path.read_text())
        for policy in snapshot["fgac_policies"]:
            if "card_security_code" in policy.get("match_condition", ""):
                if forge == "snapshot_edit_cvv_unknown":
                    policy["except_principals"] = [pg.UNKNOWN]
                else:
                    policy["comment"] = "edited"
        snapshot_path.write_text(json.dumps(snapshot))
    else:
        forgeries = {
            "history": _forge_history,
            "marker_fp": _forge_marker_fp,
            "marker_missing": _forge_marker_missing,
            "marker_restore": _forge_marker_restore,
            "history_to": _forge_history_to,
        }
        forgeries[forge](journal)
        spa.save_journal(env_dir, journal)
    harness.calls.clear()
    harness.verify_specs.clear()
    harness.resolve_failures.clear()
    harness.fail_verify = set()

    try:
        result = harness.apply()
    except spa.Refused:
        result = 2

    assert result != 0
    for spec in harness.verify_specs:
        assert len(spec.mask_config["fgac_policies"]) == len(desired["fgac_policies"])
