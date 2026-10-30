"""make wiring of APPROVAL= (scripts/approval.py) in _apply-account-locked and _release-manifest.

APPROVAL=<record> is private to GenieRails' own interactive flows: it counts
only as a command-line variable of a nested make, is checked under the lock
before anything applies, and lands in the receipt. The public apply-account and
release clear it, so typing it gains nothing, and without it every receipt says
{"via": "external"}. A record counts only inside the run whose prompt wrote it
(its nonce and asking shell): a nested make started from outside that run
(MAKELEVEL or MAKEFLAGS set by hand) refuses it. Real make and release_gate.py
run against the test_release_gate_make harness (stubs for terraform and the
live checks); CONSUMER is a test-only asking flow (approve, then the nested
make), run on a pseudo-terminal.
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
from tests import script_stubs
from tests.pty_harness import CTRL_D
from tests.test_approval import NONCE, QUESTION, RUN, Tty, _forge, this_run  # noqa: F401 (autouse fixture)
from tests.test_rehearse_account_approval import pty_make
from tests.test_release_gate_make import ROOT, Make, _account_planned, _planned, make, repo  # noqa: F401 (fixtures)

EXTERNAL = {"via": "external"}
# What a recipe of the top-level make runs under, typed by hand: MAKELEVEL, and even this run's nonce.
NESTED = {"MAKELEVEL": "1", approval.RUN_ENV: NONCE}
CONSUMER = """include {root}/aws/Makefile
_test-approve-then:
\t@GENIERAILS_APPROVAL_RUN="$$(od -An -N16 -tx1 /dev/urandom | tr -d ' \\n')"; export GENIERAILS_APPROVAL_RUN; \\
\tanswer="$$(mktemp)"; \\
\tshown="$$(mktemp)"; python3 "$(SHARED_ROOT)/scripts/approval.py" show --env-dir "$(ENV_DIR)" --env "$(ENV)" --manifest "$(TEST_MANIFEST)" --answer-file "$$shown" || exit 1; \\
\tdigest="$$(sed 's/^genierails-approval:v1:digest://' "$$shown")"; rm -f "$$shown"; \\
\tpython3 "$(SHARED_ROOT)/scripts/approval.py" approve --env-dir "$(ENV_DIR)" --env "$(ENV)" --manifest "$(TEST_MANIFEST)" --question "{question}" --expect-digest "$$digest" --answer-file "$$answer" || exit 1; \\
\trecord="$$(sed 's/^genierails-approval:v1:approved://' "$$answer")"; rm -f "$$answer"; \\
\t$(BETWEEN) \\
\t$(MAKE) --no-print-directory $(THEN) ENV="$(ENV)" ENV_DIR="$(ENV_DIR)" MANIFEST="$(TEST_MANIFEST)" APPROVAL="$$record"
"""


def _approve_then(make: Make, env: str, then: str, manifest: Path, answer: str = "y", between: str = "",
                  **kwargs):
    """On a pty, the test asking flow: show and approve ``manifest``, run ``between``, then
    ``make <then> MANIFEST= APPROVAL=<record>``."""
    consumer = make.tmp / "consumer.mk"
    consumer.write_text(CONSUMER.format(root=ROOT, question=QUESTION))
    return pty_make(make, "_test-approve-then", env, "-f", str(consumer), f"THEN={then}",
                    f"TEST_MANIFEST={manifest}", f"BETWEEN={between}", answer=answer, **kwargs)


MUTATE = """import json, os, sys
path = sys.argv[1]
os.chmod(path, 0o600)
data = json.load(open(path))
data["host"] = "mutated-" + sys.argv[2]
open(path, "w").write(json.dumps(data, indent=2))
os.chmod(path, 0o400)
"""
# release_gate.py account-manifest-check, mutating the manifest when _apply-account-locked reads its
# removals under the lock (after the approval check), then the real command.
CHECK_THEN_MUTATE = """import os, subprocess, sys
args = sys.argv[1:]
if "--field" in args and args[args.index("--field") + 1] == "tag_value_removal.allowed_arg":
    subprocess.run([sys.executable, {mutate!r}, args[args.index("--manifest") + 1], "under-the-lock"], check=True)
os.execv(sys.executable, [sys.executable, {gate!r}, "account-manifest-check", *args])
"""


def _mutator(make: Make) -> Path:
    path = make.tmp / "mutate.py"
    path.write_text(MUTATE)
    return path


def _not_applied(make: Make, repo) -> None:
    assert "sync-tags" not in make.names()
    assert not [c for c in make.calls() if c[:1] == ["runner"] and "apply-plan" in c]
    assert not rg.account_receipts(repo.cloud, "account") and _unlocked(_account_dir(repo))


def test_a_manifest_changed_after_approval_is_refused_under_the_lock_before_anything_applies(account_make, repo):
    manifest = _account_planned(account_make)
    mutate = _mutator(account_make)
    rc, shown = _approve_then(account_make, "account", "_apply-account-locked", manifest,
                              between=f"{sys.executable} {mutate} {manifest} after-approval;")
    assert rc != 0, shown
    lock = shown.index("environment lock acquired (account, apply-account)")
    assert shown.index("manifest_sha256 differ") > lock
    assert "apply-account: the approval record doesn't approve this manifest (above); nothing was applied." in shown
    _not_applied(account_make, repo)


def test_a_manifest_changed_under_the_lock_after_its_check_never_reaches_sync_tags_or_terraform(account_make, repo):
    manifest = _account_planned(account_make)
    stub = account_make.tmp / "check-then-mutate.py"
    stub.write_text(CHECK_THEN_MUTATE.format(mutate=str(_mutator(account_make)), gate=rg.__file__))
    rc, shown = _approve_then(account_make, "account", "_apply-account-locked", manifest,
                              stubs={"release_gate.py account-manifest-check": script_stubs.python_file(stub)})
    assert rc != 0, shown
    assert shown.index(f"Approval record") < shown.index("changed after it was shown")
    assert "apply-account: the manifest is not the one approved (above); nothing more was applied." in shown
    assert json.loads(manifest.read_text())["host"] == "mutated-under-the-lock"
    _not_applied(account_make, repo)
    assert (manifest.parent / "account.tfplan").exists()


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


def test_a_record_approved_on_a_real_terminal_in_this_run_lands_in_the_receipt(account_make, repo):
    manifest = _account_planned(account_make)
    rc, shown = _approve_then(account_make, "account", "_apply-account-locked", manifest)
    assert rc == 0, shown
    [record] = sorted(approval.approvals_dir(_account_dir(repo), "account").iterdir())
    assert shown.index("environment lock acquired (account, apply-account)") \
        < shown.index(f"Approval record {record}: approves this manifest.") \
        < shown.index("=== Apply saved plan (account) ===")
    approved = _account_receipt(repo)["approval"]
    assert approved["via"] == "terminal" and approved["record"] == str(record) and approved["user"]
    assert approved["tty"].startswith("/dev/") and approved["manifest_sha256"] == rg.sha256_file(manifest)


def test_no_at_the_asking_flow_applies_nothing(account_make, repo):
    manifest = _account_planned(account_make)
    for answer in ("n", "", CTRL_D):
        rc, shown = _approve_then(account_make, "account", "_apply-account-locked", manifest, answer)
        assert rc != 0, shown
    assert "_apply-account-locked" not in account_make.names() and not rg.account_receipts(repo.cloud, "account")


@pytest.mark.parametrize("how", ["makelevel", "makeflags"])
def test_a_nested_make_forged_from_outside_the_run_refuses_a_record_even_with_its_nonce(account_make, repo, how):
    """MAKELEVEL=1 (or APPROVAL in MAKEFLAGS) typed by hand, with a well-formed 0600 record and this run's nonce."""
    manifest = _account_planned(account_make)
    record = _record(_account_dir(repo), "account", manifest)  # perfect but for its asking shell
    account_make.reset()
    if how == "makelevel":
        result = account_make("_apply-account-locked", "account", f"MANIFEST={manifest}", f"APPROVAL={record}",
                              environ_extra=NESTED)
    else:
        result = account_make("_apply-account-locked", "account", f"MANIFEST={manifest}",
                              environ_extra=NESTED | {"MAKEFLAGS": f"APPROVAL={record}"})
    assert result.returncode != 0, result.stdout + result.stderr
    assert "was not written by this run's own prompt (its asker is not running above" in result.stderr
    assert "apply-account: the approval record doesn't approve this manifest (above); nothing was applied." \
        in result.stderr
    assert "sync-tags" not in account_make.names()
    assert not [c for c in account_make.calls() if c[:1] == ["runner"] and "apply-plan" in c]
    assert not rg.account_receipts(repo.cloud, "account") and _unlocked(_account_dir(repo))


@pytest.mark.parametrize("how", ["makelevel", "makeflags"])
def test_without_a_run_nonce_a_forged_nested_make_ignores_approval_and_records_external(account_make, repo,
                                                                                          monkeypatch, how):
    """MAKELEVEL=1 (or APPROVAL in MAKEFLAGS) by hand, no GENIERAILS_APPROVAL_RUN: APPROVAL= is ignored."""
    manifest = _account_planned(account_make)
    record = _record(_account_dir(repo), "account", manifest)
    monkeypatch.delenv(approval.RUN_ENV)
    extra = (f"APPROVAL={record}",) if how == "makelevel" else ()
    environ = {"MAKELEVEL": "1"} | ({"MAKEFLAGS": f"APPROVAL={record}"} if how == "makeflags" else {})
    result = account_make("_apply-account-locked", "account", f"MANIFEST={manifest}", *extra, environ_extra=environ)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Approval record" not in result.stdout
    assert _account_receipt(repo)["approval"] == EXTERNAL


@pytest.mark.parametrize("how", ["0644", "symlink", "hand-written"])
def test_a_nested_make_refuses_a_record_this_run_never_wrote(account_make, repo, how):
    manifest = _account_planned(account_make)
    rc, shown = _approve_then(account_make, "account", "_apply-account-locked", manifest, answer="n")
    assert rc != 0
    record = _record(_account_dir(repo), "account", manifest)
    if how == "0644":
        record.chmod(0o644)
    elif how == "symlink":
        link = record.with_name("link.json")
        link.symlink_to(record)
        record = link
    else:
        record = _forge(record, run_sha256="0" * 64)
    account_make.reset()
    result = account_make("_apply-account-locked", "account", f"MANIFEST={manifest}", f"APPROVAL={record}",
                          environ_extra=NESTED)
    assert result.returncode != 0
    assert "the approval record doesn't approve this manifest (above); nothing was applied." in result.stderr
    assert not rg.account_receipts(repo.cloud, "account") and "sync-tags" not in account_make.names()


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


def _records(repo) -> list[Path]:
    directory = approval.approvals_dir(repo.prod, "prod")
    return sorted(directory.iterdir()) if directory.is_dir() else []


def test_a_nested_release_with_terminal_records_puts_every_round_on_the_receipt(tmp_path, repo):
    make = Make(tmp_path, repo, passthrough=("_release-manifest",))
    manifest = _planned(make)
    make.exit_code("staged", 10)
    rc, shown = _approve_then(make, "prod", "_release-manifest", manifest)
    assert rc == 0, shown
    [phase_record] = _records(repo)
    assert [p["approval"]["record"] for p in _phases(repo)] == [str(phase_record)]
    make.exit_code("staged", 0)
    time.sleep(1.1)  # manifest ids are per second
    final = _planned(make)
    rc, shown = _approve_then(make, "prod", "_release-manifest", final)
    assert rc == 0, shown
    [final_record] = [r for r in _records(repo) if r != phase_record]
    assert shown.index("live snapshot match.") < shown.index(f"Approval record {final_record}: approves this manifest.") \
        < shown.index("=== Apply saved plan (workspace:prod) ===")
    receipt = rg.last_release(repo.cloud, "prod")
    assert receipt["approval"]["record"] == str(final_record)
    assert receipt["phase_approvals"][json.loads(manifest.read_text())["id"]]["record"] == str(phase_record)


def test_a_nested_release_forged_from_outside_the_run_refuses_a_record_even_with_its_nonce(make, repo):
    manifest = _planned(make)
    record = _record(repo.prod, "prod", manifest)
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", f"APPROVAL={record}", environ_extra=NESTED)
    assert result.returncode != 0
    assert "was not written by this run's own prompt (its asker is not running above" in result.stderr
    assert not [c for c in make.calls() if c[0] in ("staged", "runner")]
    assert not _phases(repo) and not rg.release_receipts(repo.cloud, "prod") and _unlocked(repo.prod)


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
