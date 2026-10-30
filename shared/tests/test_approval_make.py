"""make wiring of APPROVAL= (scripts/approval.py) in _apply-account-locked and _release-manifest.

APPROVAL=<record> is private to GenieRails' own interactive flows: it counts
only as a command-line variable of a nested make, is checked under the lock
before anything applies, and lands in the receipt. The public apply-account and
release clear it, so typing it gains nothing, and without it every receipt says
{"via": "external"}. Real make and release_gate.py run against the
test_release_gate_make harness (stubs for terraform and the live checks).
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

from scripts import approval
from scripts import environment_lock
from scripts import release_gate as rg
from tests.pty_harness import pty_run
from tests.test_approval import QUESTION, RUN, Tty, _environ, _forge, approval_run  # noqa: F401 (fixture)
from tests.test_release_gate_make import Make, _account_planned, _planned, make, repo  # noqa: F401 (fixtures)

EXTERNAL = {"via": "external"}
NESTED = {"MAKELEVEL": "1"}  # what a recipe of the top-level make (the interactive flows) runs under


def _account_dir(repo) -> Path:
    return repo.cloud / "envs" / "account"


def _record(env_dir: Path, env: str, manifest: Path) -> Path:
    return approval.record(env_dir, env, manifest, QUESTION, run=RUN, environ=NESTED, stdin=Tty(), stdout=Tty())


def _unlocked(env_dir: Path) -> bool:
    return not environment_lock.lock_path(env_dir).exists()


@pytest.fixture
def account_make(tmp_path, repo):
    return Make(tmp_path, repo, passthrough=("_apply-account-locked",))


def _account_receipt(repo) -> dict:
    [receipt] = rg.account_receipts(repo.cloud, "account")
    return receipt


# -- account -------------------------------------------------------------------------

def test_primitive_apply_account_records_an_external_approval(account_make, repo):
    _account_planned(account_make)
    result = account_make("apply-account", "account")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _account_receipt(repo)["approval"] == EXTERNAL
    assert "Approval record" not in result.stdout


def test_apply_account_ignores_an_approval_typed_on_its_command_line(account_make, repo):
    manifest = _account_planned(account_make)
    record = _record(_account_dir(repo), "account", manifest)
    result = account_make("apply-account", "account", f"APPROVAL={record}")
    assert result.returncode == 0, result.stdout + result.stderr
    nested = [c for c in account_make.calls() if "_apply-account-locked" in c]
    assert nested and nested[0][-1] == "APPROVAL="  # cleared for the nested make
    assert _account_receipt(repo)["approval"] == EXTERNAL


def test_a_nested_apply_with_a_terminal_record_embeds_it(account_make, repo):
    manifest = _account_planned(account_make)
    record = _record(_account_dir(repo), "account", manifest)
    result = account_make("_apply-account-locked", "account", f"MANIFEST={manifest}", f"APPROVAL={record}",
                          environ_extra=NESTED)
    assert result.returncode == 0, result.stdout + result.stderr
    out = result.stdout
    assert out.index("environment lock acquired (account, apply-account)") \
        < out.index(f"Approval record {record}: approves this manifest.") < out.index("=== Apply saved plan (account) ===")
    approved = _account_receipt(repo)["approval"]
    assert approved["via"] == "terminal" and approved["record"] == str(record)
    assert approved["manifest_sha256"] == rg.sha256_file(manifest)


def test_a_record_approved_on_a_real_terminal_lands_in_the_receipt(account_make, repo, tmp_path):
    manifest = _account_planned(account_make)
    answer = tmp_path / "answer"
    answer.touch()
    rc, shown = pty_run([sys.executable, approval.__file__, "approve", "--env-dir", str(_account_dir(repo)),
                         "--env", "account", "--manifest", str(manifest), "--question", QUESTION,
                         "--answer-file", str(answer)], "y", _environ(MAKELEVEL="1") | repo.env)
    assert rc == 0, shown
    record = Path(answer.read_text().strip().split(approval.APPROVED + ":", 1)[1])
    result = account_make("_apply-account-locked", "account", f"MANIFEST={manifest}", f"APPROVAL={record}",
                          environ_extra=NESTED)
    assert result.returncode == 0, result.stdout + result.stderr
    approved = _account_receipt(repo)["approval"]
    assert approved["via"] == "terminal" and approved["tty"].startswith("/dev/") and approved["user"]


@pytest.mark.parametrize("how", ["host", "sha", "other-manifest", "missing", "outside", "release-record"])
def test_a_nested_apply_refuses_a_record_that_does_not_approve_the_manifest_before_sync_tags(
        account_make, repo, tmp_path, how):
    manifest = _account_planned(account_make)
    record = _record(_account_dir(repo), "account", manifest)
    if how == "host":
        record = _forge(record, host="someone-elses-laptop")
    elif how == "sha":
        record = _forge(record, manifest_sha256="0" * 64)
    elif how == "other-manifest":
        record = _forge(record, manifest_id="20300101T000000-000000000000")
    elif how == "missing":
        record = record.with_name("missing.json")
    elif how == "outside":
        record = tmp_path / "copy.json"
        record.write_bytes(_record(_account_dir(repo), "account", manifest).read_bytes())
    else:
        record = record.parents[2] / "prod" / "approvals" / record.name
        record.parent.mkdir(parents=True)
        record.write_bytes(b"{}")
    account_make.reset()
    result = account_make("_apply-account-locked", "account", f"MANIFEST={manifest}", f"APPROVAL={record}",
                          environ_extra=NESTED)
    assert result.returncode != 0
    assert "apply-account: the approval record doesn't approve this manifest (above); nothing was applied." \
        in result.stderr
    assert "sync-tags" not in account_make.names()
    assert not [c for c in account_make.calls() if c[:1] == ["runner"] and "apply-plan" in c]
    assert not rg.account_receipts(repo.cloud, "account")
    assert (manifest.parent / "account.tfplan").exists()  # the approved plan stays for a real approval
    assert _unlocked(_account_dir(repo))


@pytest.mark.parametrize("how", ["top-level", "environment"])
def test_approval_counts_only_as_a_nested_command_line_variable(account_make, repo, how):
    manifest = _account_planned(account_make)
    record = _record(_account_dir(repo), "account", manifest)
    if how == "top-level":  # make _apply-account-locked APPROVAL=... typed by hand: MAKELEVEL 0
        result = account_make("_apply-account-locked", "account", f"MANIFEST={manifest}", f"APPROVAL={record}")
    else:
        result = account_make("_apply-account-locked", "account", f"MANIFEST={manifest}",
                              environ_extra=NESTED | {"APPROVAL": str(record)})
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Approval record" not in result.stdout
    assert _account_receipt(repo)["approval"] == EXTERNAL


def test_a_bad_approval_typed_on_apply_accounts_command_line_changes_nothing(account_make, repo, tmp_path):
    _account_planned(account_make)
    result = account_make("apply-account", "account", f"APPROVAL={tmp_path / 'forged.json'}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _account_receipt(repo)["approval"] == EXTERNAL


# -- release -------------------------------------------------------------------------

def _phases(repo) -> list[dict]:
    return rg.phase_records(repo.cloud, "prod", repo.head())


def test_primitive_release_records_external_approvals(make, repo):
    manifest = _planned(make)
    make.exit_code("staged", 10)
    assert make("_release-manifest", "prod", f"MANIFEST={manifest}").returncode == 0
    assert [p["approval"] for p in _phases(repo)] == [EXTERNAL]
    make.exit_code("staged", 0)
    time.sleep(1.1)  # manifest ids are per second
    final = _planned(make)
    result = make("_release-manifest", "prod", f"MANIFEST={final}")
    assert result.returncode == 0, result.stdout + result.stderr
    receipt = rg.last_release(repo.cloud, "prod")
    assert receipt["approval"] == EXTERNAL
    assert receipt["phase_approvals"] == {json.loads(manifest.read_text())["id"]: EXTERNAL}


def test_a_nested_release_with_terminal_records_puts_every_round_on_the_receipt(make, repo):
    manifest = _planned(make)
    make.exit_code("staged", 10)
    phase_record = _record(repo.prod, "prod", manifest)
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", f"APPROVAL={phase_record}",
                  environ_extra=NESTED)
    assert result.returncode == 0, result.stdout + result.stderr
    assert [p["approval"]["record"] for p in _phases(repo)] == [str(phase_record)]
    make.exit_code("staged", 0)
    time.sleep(1.1)  # manifest ids are per second
    final = _planned(make)
    final_record = _record(repo.prod, "prod", final)
    result = make("_release-manifest", "prod", f"MANIFEST={final}", f"APPROVAL={final_record}", environ_extra=NESTED)
    assert result.returncode == 0, result.stdout + result.stderr
    out = result.stdout
    assert out.index("live snapshot match.") < out.index(f"Approval record {final_record}: approves this manifest.") \
        < out.index("=== Apply saved plan (workspace:prod) ===")
    receipt = rg.last_release(repo.cloud, "prod")
    assert receipt["approval"]["record"] == str(final_record)
    assert receipt["phase_approvals"][json.loads(manifest.read_text())["id"]]["record"] == str(phase_record)


@pytest.mark.parametrize("phase", [False, True])
def test_a_nested_release_refuses_a_bad_record_before_importing_or_applying(make, repo, phase):
    manifest = _planned(make)
    if phase:
        make.exit_code("staged", 10)
    record = _forge(_record(repo.prod, "prod", manifest), manifest_sha256="0" * 64)
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", f"APPROVAL={record}", environ_extra=NESTED)
    assert result.returncode != 0
    assert "release: the approval record doesn't approve this manifest (above); nothing was applied." in result.stderr
    assert not [c for c in make.calls() if c[0] in ("staged", "runner")]
    assert "_sync-tags-add-only-locked" not in make.names()
    assert not _phases(repo) and not rg.release_receipts(repo.cloud, "prod")
    assert _unlocked(repo.prod)


def test_release_ignores_an_approval_typed_on_its_command_line(tmp_path, repo):
    make = Make(tmp_path, repo, passthrough=("_release-manifest",))
    manifest = _planned(make)
    record = _record(repo.prod, "prod", manifest)
    result = make("release", "prod", f"MANIFEST={manifest}", f"APPROVAL={record}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Approval record" not in result.stdout
    assert rg.last_release(repo.cloud, "prod")["approval"] == EXTERNAL


def test_without_approval_the_recipes_print_exactly_what_they_did(make, repo):
    """No APPROVAL: nothing new is printed (the only difference is the receipt's new field)."""
    manifest = _planned(make)
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}")
    assert result.returncode == 0
    output = result.stdout + result.stderr
    assert "Approval record" not in output and "approval record" not in output and "approval check" not in output
