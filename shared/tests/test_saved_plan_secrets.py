"""Saved Terraform plans carry the deployer's client secret: deleted once applied and recorded, kept otherwise.

release-plan saves a plan per root (account, workspace, each staged data_access
phase) in its manifest dir, and policy-abandon / policy-rebind save theirs in
the env's policy ledger. Each is deleted only after its apply succeeded and
the release receipt, phase record or journal entry was written; a failed or
unrecorded apply keeps it, owner-only. (make account-plan / apply-account:
tests/test_release_gate_make.py.)
"""

from __future__ import annotations

import json
import os
import stat
import sys
import time
from pathlib import Path

import pytest

from scripts import policy_generations as pg
from scripts import release_gate as rg
from scripts import staged_policy_apply as spa
from tests.test_policy_rebind import CI_MARKERS, deleted_principals
from tests.test_release_gate import Repo
from tests.test_release_gate_make import CLASSIFICATION_IMPORT, ROOT, Make, _planned
from tests.test_staged_policy_apply import (
    Harness, _new_value_column, _retag_live, _rolled_back_switch, released, write_env,
)

SECRET = "fake-deployer-client-secret-must-not-persist"


@pytest.fixture
def repo(tmp_path, monkeypatch):
    return Repo(tmp_path, monkeypatch)


@pytest.fixture
def make(tmp_path, repo):
    return Make(tmp_path, repo)


def _plans(manifest: Path) -> list[Path]:
    """The saved plans a release manifest names (account, workspace, each data_access phase)."""
    plans = json.loads(manifest.read_text())["plans"]
    return [manifest.parent / plans[root]["file"] for root in ("account", "workspace") if plans[root].get("file")] + [
        manifest.parent / "data_access" / step["file"] for step in plans["data_access"].get("steps") or []
        if step.get("file")]


def _with_secret(manifest: Path) -> list[Path]:
    """Make each saved plan carry the deployer secret, as a real one does, keeping the manifest consistent."""
    data = json.loads(manifest.read_text())
    plans = _plans(manifest)
    for path in plans:
        os.chmod(path, 0o600)
        path.write_text(f"{path.name}: databricks_client_secret = {SECRET}")
        os.chmod(path, 0o400)
    for root in ("account", "workspace"):
        if data["plans"][root].get("file"):
            data["plans"][root]["sha256"] = rg.sha256_file(manifest.parent / data["plans"][root]["file"])
    for step in data["plans"]["data_access"].get("steps") or []:
        if step.get("file"):
            step["sha256"] = rg.sha256_file(manifest.parent / "data_access" / step["file"])
    os.chmod(manifest, 0o600)
    manifest.write_text(json.dumps(data))
    os.chmod(manifest, 0o400)
    assert plans
    return plans


def _secret_files(repo: Repo) -> list[Path]:
    """Every file in the checkout and the state dir that still holds the secret."""
    found = []
    for base in (repo.root, repo.state):
        for path in base.rglob("*"):
            if ".git" not in path.parts and path.is_file() and SECRET in path.read_text(errors="ignore"):
                found.append(path)
    return found


def _owner_only(path: Path) -> bool:
    return not stat.S_IMODE(path.stat().st_mode) & 0o077


def _plan_modes(h, *, fail: bool = False, chmod: int | None = None) -> dict[str, int]:
    """Record each saved plan's mode as its hook returns (before the controller acts on the result).

    ``fail`` makes every planning hook return 1 after it wrote its plan, as one refusing its plan does;
    ``chmod`` makes the hook set the plan's mode itself (a writer that ignores the umask).
    """
    modes = {}
    real_run = h.run

    def run(argv, env=None, check=False):
        result = real_run(argv, env=env, check=check)
        out = (env or {}).get(pg.PLAN_OUT_ENV)
        if out:
            if chmod is not None and Path(out).exists():
                os.chmod(out, chmod)
            if Path(out).exists():
                modes[Path(out).name] = stat.S_IMODE(Path(out).stat().st_mode)
            if fail:
                result.returncode = 1
        return result

    h.run = run
    return modes


# -- make release ---------------------------------------------------------------------
def test_a_release_deletes_its_saved_plans_after_the_receipt(make, repo):
    manifest = _planned(make)
    plans = _with_secret(manifest)
    assert len(plans) == 3 and _secret_files(repo)  # account, workspace, data_access grant
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert rg.last_release(repo.cloud, "prod")["manifest"]["path"] == str(manifest)
    assert not [p for p in plans if p.exists()]
    assert not _secret_files(repo)
    assert SECRET not in result.stdout + result.stderr
    # The manifest and the non-secret records stay, with each deleted plan's hash.
    assert manifest.is_file() and (manifest.parent / "data_access" / "phases.json").is_file()
    # Re-releasing it is refused as released, not as missing its plans.
    make.reset()
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}")
    assert result.returncode != 0 and "was already released" in result.stderr
    assert "changed or is missing" not in result.stderr
    assert not [c for c in make.calls() if c[0] in ("staged", "runner")]


def test_a_new_release_plan_deletes_only_the_superseded_plans_and_keeps_audit_records(make, repo):
    older = _planned(make)
    old_plans = _with_secret(older)
    receipt = older.parent / "approval-receipt.json"
    os.chmod(older.parent, 0o700)
    receipt.write_text('{"approved": true}\n')
    os.chmod(receipt, 0o400)
    time.sleep(1.1)  # release manifest ids have one-second resolution

    result = make("release-plan", "prod")
    assert result.returncode == 0, result.stdout + result.stderr
    current = Path(result.stdout.split("make release ENV=prod MANIFEST=")[-1].strip())
    current_plans = _plans(current)

    assert not [path for path in old_plans if path.exists()]
    assert current_plans and all(path.is_file() for path in current_plans)
    assert older.is_file() and receipt.read_text() == '{"approved": true}\n'
    assert json.loads(older.read_text())["plans"]["workspace"]["sha256"]
    assert not _secret_files(repo)

    make.reset()
    refused = make("_release-manifest", "prod", f"MANIFEST={older}")
    assert refused.returncode != 0
    assert "was superseded by a newer make release-plan" in refused.stderr
    assert "changed or is missing" not in refused.stderr
    assert not [call for call in make.calls() if call[0] in ("staged", "runner")]


@pytest.mark.parametrize("kind", ["staged-phase", "imports"])
def test_a_phase_manifest_deletes_its_saved_plans_after_its_record(make, repo, kind):
    extra = ()
    if kind == "imports":
        make.imports.write_text(json.dumps(CLASSIFICATION_IMPORT) + "\n")
        (repo.prod / "data_access").mkdir(exist_ok=True)
        extra = (f"IMPORT_EXISTING_SCRIPT={make._recorder('importer')}",)
    manifest = _planned(make)
    if kind == "staged-phase":
        make.exit_code("staged", 10)
    plans = _with_secret(manifest)
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", *extra)
    assert result.returncode == 0, result.stdout + result.stderr
    assert [r["id"] for r in rg.phase_records(repo.cloud, "prod", repo.head())]
    assert not [p for p in plans if p.exists()] and not _secret_files(repo)


@pytest.mark.parametrize("failing", ["staged", "runner"])
def test_a_failed_release_keeps_its_saved_plans_owner_only(make, repo, failing):
    manifest = _planned(make)
    plans = _with_secret(manifest)
    make.exit_code(failing, 1)  # the saved data_access phases, or the saved workspace plan
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}")
    assert result.returncode != 0
    assert not rg.release_receipts(repo.cloud, "prod")
    assert all(p.is_file() and _owner_only(p) for p in plans)
    assert SECRET not in result.stdout + result.stderr


def test_an_unreleased_manifest_keeps_its_saved_plans_owner_only(make, repo):
    manifest = _planned(make)
    plans = _with_secret(manifest)
    assert all(p.is_file() and _owner_only(p) for p in plans)
    with pytest.raises(rg.Refused, match="no release receipt or phase record"):
        rg.drop_saved_plans(repo.prod, "prod", manifest)
    assert all(p.is_file() for p in plans)


@pytest.mark.parametrize("kind", ["release", "phase"])
def test_a_failed_receipt_write_keeps_the_saved_plans(make, repo, kind):
    manifest = _planned(make)
    plans = _with_secret(manifest)
    if kind == "phase":
        make.exit_code("staged", 10)
        blocked = rg.env_state(repo.cloud, "prod") / "receipts" / "phases"
    else:
        blocked = rg.release_dir(repo.cloud, "prod")
    blocked.parent.mkdir(parents=True, exist_ok=True)
    blocked.write_text("blocks the receipt directory")
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}")
    assert result.returncode != 0
    assert not rg.release_receipts(repo.cloud, "prod")
    assert not rg.released_by(repo.cloud, "prod", json.loads(manifest.read_text())["id"])
    assert all(p.is_file() for p in plans)


def test_a_plan_that_cant_be_deleted_is_reported_and_the_release_still_succeeds(make, repo):
    manifest = _planned(make)
    plans = _with_secret(manifest)
    data_access = manifest.parent / "data_access"
    os.chmod(data_access, 0o500)  # its plan can't be unlinked
    try:
        result = make("_release-manifest", "prod", f"MANIFEST={manifest}")
    finally:
        os.chmod(data_access, 0o700)
    assert result.returncode == 0, result.stdout + result.stderr
    assert rg.last_release(repo.cloud, "prod")
    left = [p for p in plans if p.exists()]
    assert left == [data_access / "00-grant.tfplan"]
    assert f"saved plan {left[0]} contains credentials and could not be deleted" in result.stderr
    assert "delete it." in result.stderr and SECRET not in result.stderr


# -- policy-abandon / policy-rebind ----------------------------------------------------
@pytest.fixture
def harness(tmp_path, monkeypatch):
    for marker in CI_MARKERS:
        monkeypatch.delenv(marker, raising=False)
    env_dir = tmp_path / "envs" / "dev"
    env_dir.mkdir(parents=True)
    h = Harness(env_dir, monkeypatch, tmp_path)
    yield h
    assert not h.invariant_problems, h.invariant_problems


def _abandonable(h) -> Path:
    _rolled_back_switch(h)
    write_env(h.env_dir, versions={"email_partial": {"partial": "redacted"}},
              tiers=["raw_v2", "regional_v2", "masked_v2"], exempt=("new-deployer-sp",))
    h.calls.clear()
    return pg.ledger_dir(h.env_dir) / "abandon" / f"{h.journal()['transition']['run_id']}.tfplan"


def test_abandon_deletes_its_saved_plan_after_the_journal_records_it(harness):
    plan = _abandonable(harness)
    seen = {}
    controller = harness.controller()
    real_save = controller._save

    def save(journal):
        if journal.get("transition") is None:
            seen["plan_at_final_save"] = plan.exists()
        real_save(journal)

    controller._save = save
    assert controller.abandon() == 0
    assert harness.calls == ["abandon:plan", "inventory:restore", "abandon:saved", "inventory:abandon"]
    assert harness.journal()["transition"] is None
    assert seen["plan_at_final_save"] is True  # the journal first, then the delete
    assert not plan.exists()


def test_a_failed_abandon_keeps_its_saved_plan_owner_only(harness):
    plan = _abandonable(harness)
    real_run = harness.run

    def run(argv, env=None, check=False):
        result = real_run(argv, env=env, check=check)
        if harness.calls and harness.calls[-1] == "abandon:saved":
            result.returncode = 1
        return result

    harness.run = run
    assert harness.controller().abandon() == 1
    assert harness.journal()["transition"]["status"] == "failed"
    assert plan.is_file() and _owner_only(plan) and _owner_only(plan.parent)


def test_an_abandon_whose_journal_write_fails_keeps_its_saved_plan(harness):
    plan = _abandonable(harness)
    controller = harness.controller()
    real_save = controller._save

    def save(journal):
        if journal.get("transition") is None:
            raise OSError("disk full")
        real_save(journal)

    controller._save = save
    with pytest.raises(OSError, match="disk full"):
        controller.abandon()
    assert plan.is_file()


def _rebind_plans(h) -> list[Path]:
    return sorted((pg.ledger_dir(h.env_dir) / "rebind").glob("*.tfplan"))


def test_rebind_deletes_its_saved_plans_and_held_copy_after_the_journal_records_it(harness):
    deleted_principals(harness)
    assert harness.controller().rebind() == 0
    assert harness.journal()["transition"]["rebound"]
    assert _rebind_plans(harness) == []


def test_a_failed_rebind_keeps_its_saved_plans_owner_only(harness):
    deleted_principals(harness)
    harness.agent_coverage_rc = 1  # after the protect apply
    assert harness.controller().rebind() == 1
    t = harness.journal()["transition"]
    (record,) = t["rebind_held"]
    plans = _rebind_plans(harness)
    names = {p.name for p in plans}
    # The held-back record's plan copy is what the re-run validates the held grants against.
    assert record["plan"] in names and f"{t['run_id']}-rebind-protect.tfplan" in names
    assert all(_owner_only(p) for p in plans) and _owner_only(plans[0].parent)
    # The re-run finishes and then deletes them; the abandon after it deletes its own.
    harness.agent_coverage_rc = 0
    harness.calls.clear()
    assert harness.controller().rebind() == 0
    assert _rebind_plans(harness) == []
    assert harness.controller().abandon() == 0
    assert not list((pg.ledger_dir(harness.env_dir) / "abandon").glob("*.tfplan"))


# -- owner-only from creation, whatever the plan writer returns -------------------------
def test_a_refused_abandon_plan_is_owner_only_when_written_and_then_deleted(harness):
    old, failed = _rolled_back_switch(harness)
    column, value = _new_value_column(old, failed)
    _retag_live(harness.env_dir, column, value)  # the abandon hook plans, then refuses its plan
    harness.live_check = "real"
    run_id = harness.journal()["transition"]["run_id"]
    harness.calls.clear()
    modes = _plan_modes(harness)
    with pytest.raises(spa.Refused, match="abandon plan was refused"):
        harness.controller().abandon()
    assert harness.calls == ["abandon:plan"]
    assert modes == {f"{run_id}.tfplan": 0o600}  # written, and owner-only as the hook returned
    assert not list((pg.ledger_dir(harness.env_dir) / "abandon").glob("*.tfplan"))


def test_a_refused_rebind_plan_is_owner_only_when_written_and_then_deleted(harness):
    old, failed, _ = deleted_principals(harness)
    column = next(iter(old["tags"]))
    _retag_live(harness.env_dir, column, failed["tags"][column])  # the protect plan is refused
    harness.live_check = "real"
    run_id = harness.journal()["transition"]["run_id"]
    modes = _plan_modes(harness)
    with pytest.raises(spa.Refused, match="rebind-protect plan was refused"):
        harness.controller().rebind()
    assert harness.calls == ["rebind-protect:plan"]
    assert modes == {f"{run_id}-rebind-protect.tfplan": 0o600}
    assert _rebind_plans(harness) == []


def test_a_saved_phase_plan_is_owner_only_even_when_its_hook_fails(harness, tmp_path):
    released(harness)
    write_env(harness.env_dir, versions={"email_partial": {"partial": "redacted"}})
    modes = _plan_modes(harness, fail=True)
    out = tmp_path / "plan"
    assert harness.controller().plan(out) == 1
    assert modes and set(modes.values()) == {0o600}
    assert all(_owner_only(p) for p in out.glob("*.tfplan"))


def test_a_saved_phase_plan_a_writer_left_world_readable_is_made_owner_only(harness, tmp_path):
    released(harness)
    write_env(harness.env_dir, versions={"email_partial": {"partial": "redacted"}})
    _plan_modes(harness, fail=True, chmod=0o644)
    out = tmp_path / "plan"
    assert harness.controller().plan(out) == 1
    plans = list(out.glob("*.tfplan"))
    assert plans and all(_owner_only(p) for p in plans)


def test_every_makefile_plan_writer_runs_under_umask_077():
    """Each ``terraform plan -out=`` in the recipes writes the plan owner-only from creation."""
    text = (ROOT / "shared" / "Makefile.shared").read_text()
    writers = [line for line in text.splitlines() if " plan " in line and "-out=" in line]
    assert len(writers) == 5
    assert all("(umask 077; " in line for line in writers), [w for w in writers if "(umask 077; " not in w]
    check_workspace = next(line for line in writers if '-out="$$plan_dir/workspace.tfplan"' in line
                           and '"$$target_env" plan $$stage_flag' in line)
    assert 'LAYER_ENV_DIR="$(LAYER_ENV_DIR)" "$(ROOT_RUNNER)" "$$layer" "$$target_env" plan' in check_workspace


MODE_RUNNER = """#!{python}
import os, stat, sys
from pathlib import Path
args = sys.argv[1:]
out = next((a.split("=", 1)[1] for a in args if a.startswith("-out=")), "")
if len(args) >= 3 and args[2] == "plan" and out:
    Path(out).write_text("databricks_client_secret = {secret}")
    Path({log!r}).write_text(oct(stat.S_IMODE(os.stat(out).st_mode)))
    sys.exit(1)  # planning failed after the plan was written
sys.exit(0)
"""


def test_a_failed_account_plan_wrote_its_plan_owner_only(tmp_path, repo):
    log = tmp_path / "plan-mode.txt"
    runner = tmp_path / "mode-runner"
    runner.write_text(MODE_RUNNER.format(python=sys.executable, secret=SECRET, log=str(log)))
    runner.chmod(0o755)
    result = Make(tmp_path, repo)("account-plan", "account", root_runner=runner)
    assert result.returncode != 0 and "planning failed" in result.stderr
    assert log.read_text() == "0o600"
    assert not _secret_files(repo)  # the failed plan's dir is removed


NOOP_RUNNER = """#!{python}
import json, sys
from pathlib import Path
args = sys.argv[1:]
if len(args) >= 3 and args[2] == "plan":
    out = next(a.split("=", 1)[1] for a in args if a.startswith("-out="))
    Path(out).write_text("databricks_client_secret = {secret}")
elif len(args) >= 3 and args[2] == "show":
    print(json.dumps({{"resource_changes": []}}))
sys.exit(0)
"""


def test_a_noop_account_plan_records_the_result_without_keeping_its_saved_plan(tmp_path, repo):
    runner = tmp_path / "noop-runner"
    runner.write_text(NOOP_RUNNER.format(python=sys.executable, secret=SECRET))
    runner.chmod(0o755)

    make = Make(tmp_path, repo)
    result = make("account-plan", "account", root_runner=runner)

    assert result.returncode == 0, result.stdout + result.stderr
    manifest = Path(next(line.split(":", 1)[1].strip() for line in result.stdout.splitlines()
                         if "Manifest (immutable):" in line))
    data = json.loads(manifest.read_text())
    assert data["plan"] == {"changes": False, "summary": {"changes": False, "resource_changes": {}}}
    assert not (manifest.parent / "account.tfplan").exists()
    assert not _secret_files(repo)

    apply_result = make("apply-account", "account")
    assert apply_result.returncode != 0
    assert "no unreceipted account manifest" in apply_result.stderr


def test_a_new_account_plan_deletes_the_older_unapplied_plan_but_keeps_its_manifest(tmp_path, repo):
    make = Make(tmp_path, repo)
    first = make("account-plan", "account")
    assert first.returncode == 0, first.stdout + first.stderr
    older = Path(first.stdout.split("Manifest (immutable): ")[-1].splitlines()[0])
    old_plan = older.parent / "account.tfplan"
    assert old_plan.is_file()

    second = make("account-plan", "account")
    assert second.returncode == 0, second.stdout + second.stderr
    current = Path(second.stdout.split("Manifest (immutable): ")[-1].splitlines()[0])

    assert current != older and (current.parent / "account.tfplan").is_file()
    assert older.is_file() and not old_plan.exists()
    assert json.loads(older.read_text())["plan"]["sha256"]

    # Cleanup is strictly backward: treating the older manifest as current
    # must never delete the newer plan.
    rg.drop_superseded_saved_plans(repo.cloud / "envs" / "account", "account", older)
    assert (current.parent / "account.tfplan").is_file()


def test_superseded_plan_delete_failure_warns_and_does_not_fail(tmp_path, repo, monkeypatch, capsys):
    make = Make(tmp_path, repo)
    first = make("account-plan", "account")
    older = Path(first.stdout.split("Manifest (immutable): ")[-1].splitlines()[0])
    old_plan = older.parent / "account.tfplan"
    real_unlink = Path.unlink

    def refuse_old_plan(path, *args, **kwargs):
        if path == old_plan:
            raise PermissionError("detail must not be printed")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", refuse_old_plan)
    second = make("account-plan", "account")
    assert second.returncode == 0, second.stdout + second.stderr
    # The subprocess cannot see the monkeypatch, so exercise the cleanup's
    # error boundary directly against its completed current manifest.
    current = Path(second.stdout.split("Manifest (immutable): ")[-1].splitlines()[0])
    old_plan.write_text("credential-bearing plan")
    rg.drop_superseded_saved_plans(repo.cloud / "envs" / "account", "account", current)
    warning = capsys.readouterr().err
    assert str(old_plan) in warning and "contains credentials" in warning and "delete it by hand" in warning
    assert "detail must not be printed" not in warning and old_plan.is_file()


def test_malformed_old_manifest_cleanup_warns_path_only_and_account_plan_succeeds(tmp_path, repo):
    make = Make(tmp_path, repo)
    first = make("account-plan", "account")
    assert first.returncode == 0, first.stdout + first.stderr
    older = Path(first.stdout.split("Manifest (immutable): ")[-1].splitlines()[0])
    malformed = json.loads(older.read_text())
    malformed["plan"] = "malformed-secret-marker"
    os.chmod(older, 0o600)
    older.write_text(json.dumps(malformed))
    os.chmod(older, 0o400)

    second = make("account-plan", "account")
    assert second.returncode == 0, second.stdout + second.stderr
    assert f"superseded saved-plan cleanup failed for {older}" in second.stderr
    assert "malformed-secret-marker" not in second.stdout + second.stderr


def test_a_newer_noop_account_plan_supersedes_an_older_pending_one(tmp_path, repo):
    make = Make(tmp_path, repo)
    first = make("account-plan", "account")
    assert first.returncode == 0, first.stdout + first.stderr
    older = Path(first.stdout.split("Manifest (immutable): ")[-1].splitlines()[0])
    assert (older.parent / "account.tfplan").is_file()
    runner = tmp_path / "noop-runner"
    runner.write_text(NOOP_RUNNER.format(python=sys.executable, secret=SECRET))
    runner.chmod(0o755)

    second = make("account-plan", "account", root_runner=runner)
    assert second.returncode == 0, second.stdout + second.stderr
    noop = Path(second.stdout.split("Manifest (immutable): ")[-1].splitlines()[0])

    assert noop != older and older.is_file() and not _secret_files(repo)
    selected = make("apply-account", "account")
    assert selected.returncode != 0 and "no unreceipted account manifest" in selected.stderr
    with pytest.raises(rg.Refused, match="superseded by a newer account-plan"):
        rg.account_manifest_check(repo.cloud / "envs" / "account", "account", older)
    with pytest.raises(rg.Refused, match="records no account changes"):
        rg.account_manifest_check(repo.cloud / "envs" / "account", "account", noop)
