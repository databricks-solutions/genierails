"""The receipts' additive ``approval`` field (scripts/approval.py), and that receipts written before it still count.

Account receipts, phase records and release receipts record who approved the
manifest they applied: the checked record of a y typed at a terminal, or
``{"via": "external"}`` when it was approved outside GenieRails (the
primitives). A given record that doesn't approve the manifest refuses before
anything is written. The validators (_phase_problem, _receipt_problem) and
maintain-check stay as strict as before and ignore the new fields.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import approval
from scripts import release_gate as rg
from tests.test_approval import QUESTION, THIS_RUN, _forge, _record, this_run  # noqa: F401 (autouse fixture)
from tests.test_release_gate import (  # noqa: F401 (fixtures)
    _second_manifest, account_manifest, manifest, repo,
)

EXTERNAL = {"via": "external"}
GATE = Path(rg.__file__)


def _account_dir(repo) -> Path:
    return repo.cloud / "envs" / "account"


def _id(path: Path) -> str:
    return json.loads(Path(path).read_text())["id"]


def _receipts(directory: Path) -> list[Path]:
    return sorted(directory.glob("*.json")) if directory.is_dir() else []


# -- account receipts --------------------------------------------------------------

def test_an_account_receipt_without_a_record_says_external(repo, account_manifest):
    receipt = json.loads(rg.account_receipt(_account_dir(repo), "account", account_manifest).read_text())
    assert receipt["approval"] == EXTERNAL


def test_an_account_receipt_embeds_the_checked_terminal_record(repo, account_manifest):
    record = _record(repo, account_manifest, env="account")
    path = rg.account_receipt(_account_dir(repo), "account", account_manifest, record)
    approved = json.loads(path.read_text())["approval"]
    assert approved["via"] == "terminal" and approved["manifest_sha256"] == rg.sha256_file(account_manifest)
    assert approved["record"] == str(record) and approved["record_sha256"] == rg.sha256_file(record)
    assert approved == approval.check(_account_dir(repo), "account", account_manifest, record)


@pytest.mark.parametrize("field,value", [("manifest_sha256", "0" * 64), ("host", "elsewhere"),
                                         ("manifest_id", "other")])
def test_an_account_receipt_refuses_a_record_that_does_not_approve_its_manifest_and_writes_nothing(
        repo, account_manifest, field, value):
    forged = _forge(_record(repo, account_manifest, env="account"), **{field: value})
    with pytest.raises(rg.Refused, match="doesn't approve"):
        rg.account_receipt(_account_dir(repo), "account", account_manifest, forged)
    assert not rg.account_receipts(repo.cloud, "account")
    # Single use is unchanged: the manifest is still unreceipted.
    assert not rg.account_manifest_used(repo.cloud, "account", _id(account_manifest))


def test_an_account_receipt_refuses_a_release_manifests_record(repo, account_manifest, manifest):
    record = _record(repo, manifest)
    with pytest.raises(rg.Refused, match="is not in"):
        rg.account_receipt(_account_dir(repo), "account", account_manifest, record)
    assert not rg.account_receipts(repo.cloud, "account")


def test_an_account_receipt_written_before_approvals_still_makes_its_manifest_single_use(repo, account_manifest):
    receipt = json.loads(rg.account_receipt(_account_dir(repo), "account", account_manifest).read_text())
    for path in _receipts(rg.env_state(repo.cloud, "account") / "receipts" / "account"):
        path.unlink()
    old = {k: v for k, v in receipt.items() if k != "approval"}
    rg._write_once(rg.env_state(repo.cloud, "account") / "receipts" / "account" / "old.json", old)
    assert rg.account_manifest_used(repo.cloud, "account", _id(account_manifest))
    with pytest.raises(rg.Refused, match="single-use"):
        rg.account_manifest_check(_account_dir(repo), "account", account_manifest)


# -- phase records and release receipts -----------------------------------------------

def _phases_dir(repo) -> Path:
    return rg.env_state(repo.cloud, "prod") / "receipts" / "phases"


def test_a_phase_record_and_a_release_receipt_without_a_record_say_external(repo, manifest):
    record = json.loads(rg.phase_record(repo.prod, "prod", manifest).read_text())
    assert record["approval"] == EXTERNAL
    receipt = json.loads(rg.release_receipt(repo.prod, "prod", _second_manifest(repo)).read_text())
    assert receipt["approval"] == EXTERNAL
    assert receipt["phase_manifests"] == [_id(manifest)]
    assert receipt["phase_approvals"] == {_id(manifest): EXTERNAL}


def test_every_round_approved_at_the_terminal_is_on_the_final_receipt(repo, manifest):
    phase_approval = _record(repo, manifest, round_no=1)
    rg.phase_record(repo.prod, "prod", manifest, phase_approval)
    final = _second_manifest(repo)
    final_approval = _record(repo, final, round_no=2)
    receipt = json.loads(rg.release_receipt(repo.prod, "prod", final, final_approval).read_text())
    assert receipt["approval"]["via"] == "terminal" and receipt["approval"]["manifest_id"] == _id(final)
    assert receipt["approval"]["round"] == 2
    assert set(receipt["phase_approvals"]) == set(receipt["phase_manifests"]) == {_id(manifest)}
    linked = receipt["phase_approvals"][_id(manifest)]
    assert linked["via"] == "terminal" and linked["manifest_sha256"] == rg.sha256_file(manifest)
    assert linked == approval.check(repo.prod, "prod", manifest, phase_approval)
    assert rg.maintain_check(repo.prod, "prod")["commit"] == repo.head()


@pytest.mark.parametrize("writer", ["phase-record", "release-receipt"])
def test_a_record_for_another_manifest_refuses_before_anything_is_written(repo, manifest, writer):
    other = _record(repo, manifest)
    target = _second_manifest(repo)
    with pytest.raises(rg.Refused, match="doesn't approve"):
        (rg.phase_record if writer == "phase-record" else rg.release_receipt)(repo.prod, "prod", target, other)
    assert not _receipts(_phases_dir(repo)) and not rg.release_receipts(repo.cloud, "prod")


def _strip(path: Path, *keys: str) -> Path:
    """The file as main wrote it: the same record without the new fields (write-once, so a new file)."""
    data = {k: v for k, v in json.loads(path.read_text()).items() if k not in keys}
    path.unlink()
    return rg._write_once(path, data)


def test_phase_records_written_before_approvals_still_verify_and_link_as_external(repo, manifest):
    old = _strip(rg.phase_record(repo.prod, "prod", manifest), "approval")
    assert rg._phase_problem(repo.cloud, "prod", old, json.loads(old.read_text())) == ""
    receipt = json.loads(rg.release_receipt(repo.prod, "prod", _second_manifest(repo)).read_text())
    assert receipt["phase_manifests"] == [_id(manifest)] and not receipt["phase_manifests_unlinked"]
    assert receipt["phase_approvals"] == {_id(manifest): EXTERNAL}


def test_release_receipts_written_before_approvals_still_verify_account_for_their_phases_and_gate_maintain(
        repo, manifest):
    rg.phase_record(repo.prod, "prod", manifest)
    first = _strip(rg.release_receipt(repo.prod, "prod", _second_manifest(repo)), "approval", "phase_approvals")
    assert rg._receipt_problem(repo.cloud, "prod", json.loads(first.read_text())) == ""
    assert rg.maintain_check(repo.prod, "prod")["commit"] == repo.head()
    # The next release: the old receipt still accounts for the old phase, which never links twice.
    third = _second_manifest(repo)
    receipt = json.loads(rg.release_receipt(repo.prod, "prod", third).read_text())
    assert receipt["phase_manifests"] == [] and receipt["phase_approvals"] == {}
    assert [e["id"] for e in receipt["phase_manifests_earlier"]] == [_id(manifest)]
    assert not receipt["release_receipts_ignored"]


def test_the_validators_stay_strict(repo, manifest):
    """The new fields make nothing count that didn't: a tampered manifest still unlinks its phase record."""
    rg.phase_record(repo.prod, "prod", manifest, _record(repo, manifest))
    manifest.chmod(0o600)
    manifest.write_text(manifest.read_text().replace('"final": true', '"final": false'))
    record_path = _phases_dir(repo) / f"{_id(manifest)}.json"
    assert "doesn't match the hash" in rg._phase_problem(repo.cloud, "prod", record_path,
                                                         json.loads(record_path.read_text()))
    assert "is missing or doesn't match" in rg._receipt_problem(
        repo.cloud, "prod", {"kind": "release", "version": 1, "env": "prod", "cloud": "aws",
                             "manifest": {"id": _id(manifest), "sha256": "0" * 64}, "approval": EXTERNAL})


# -- the CLI flags -------------------------------------------------------------------

# release_gate.py's CLI as this test's asking run (the records' process chain, as in-process):
# tests/test_approval_make.py runs the real chain under make.
AS_THIS_RUN = ("import runpy, sys; from scripts import approval; approval._chain = lambda pid: {chain!r}; "
               "sys.argv = sys.argv[1:]; runpy.run_path(sys.argv[0], run_name='__main__')")


def _gate(repo, *args):
    return subprocess.run([sys.executable, "-c", AS_THIS_RUN.format(chain=THIS_RUN), str(GATE), *args],
                          capture_output=True, text=True, env=os.environ | repo.env | {"PYTHONPATH": str(GATE.parents[1])})


def test_receipt_commands_take_approval_and_refuse_a_bad_one_with_exit_1(repo, manifest, account_manifest):
    record = _record(repo, account_manifest, env="account")
    bad = _gate(repo, "account-receipt", "--env-dir", str(_account_dir(repo)), "--env", "account",
                "--manifest", str(account_manifest), "--approval", str(_forge(record, host="elsewhere")))
    assert bad.returncode == 1 and "doesn't approve" in bad.stderr and not rg.account_receipts(repo.cloud, "account")
    good = _gate(repo, "account-receipt", "--env-dir", str(_account_dir(repo)), "--env", "account",
                 "--manifest", str(account_manifest), "--approval", str(record))
    assert good.returncode == 0, good.stderr
    assert rg.account_receipts(repo.cloud, "account")[-1]["approval"]["via"] == "terminal"
    phase = _gate(repo, "phase-record", "--env-dir", str(repo.prod), "--env", "prod", "--manifest", str(manifest),
                  "--approval", str(_record(repo, manifest)))
    assert phase.returncode == 0, phase.stderr
    final = _second_manifest(repo)
    bad = _gate(repo, "release-receipt", "--env-dir", str(repo.prod), "--env", "prod", "--manifest", str(final),
                "--approval", str(record))
    assert bad.returncode == 1 and not rg.release_receipts(repo.cloud, "prod")
    good = _gate(repo, "release-receipt", "--env-dir", str(repo.prod), "--env", "prod", "--manifest", str(final))
    assert good.returncode == 0, good.stderr
    receipt = rg.release_receipts(repo.cloud, "prod")[-1]
    assert receipt["approval"] == EXTERNAL and receipt["phase_approvals"][_id(manifest)]["via"] == "terminal"


def test_question_is_recorded_as_shown(repo, manifest):
    data = json.loads(_record(repo, manifest).read_text())
    assert data["question"] == f"{QUESTION} [y/N] "
