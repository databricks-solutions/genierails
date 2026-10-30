"""scripts/approval.py: who may be asked, what counts as yes, and the write-once approval records.

The CLI runs for real on a pseudo-terminal (tests/pty_harness.py) for the y/N
answers; the record and check rules use the release-gate Repo and manifests.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import approval
from scripts import release_gate as rg
from tests.pty_harness import CTRL_C, CTRL_D, pty_run
from tests.script_stubs import without_ci
from tests.test_release_gate import (  # noqa: F401 (fixtures)
    _account_plan_dir, _second_manifest, account_manifest, manifest, repo,
)

SCRIPT = Path(approval.__file__)
QUESTION = "Approve and apply this account manifest?"


class Tty:
    def __init__(self, tty: bool = True):
        self.tty = tty

    def isatty(self) -> bool:
        return self.tty


NONCE = "ab" * 16
# The asking flow's process chain, as approval._chain sees it in-process: its recipe
# shell directly under the top-level make (tests/test_approval_make.py and
# tests/test_rehearse_account_approval.py use real ones).
THIS_RUN = [(4242, "sh"), (4241, "make"), (4240, "zsh")]
ON_A_TERMINAL = {"MAKELEVEL": "1", approval.RUN_ENV: NONCE}


@pytest.fixture(autouse=True)
def this_run(monkeypatch):
    """In-process record and check run as this run's asking flow: its nonce, its process chain."""
    monkeypatch.setenv(approval.RUN_ENV, NONCE)
    monkeypatch.setattr(approval, "_chain", lambda pid: list(THIS_RUN))


def _terminal(**environ):
    return {"environ": ON_A_TERMINAL | environ, "stdin": Tty(), "stdout": Tty()}


def _environ(**extra) -> dict:
    env = {k: v for k, v in without_ci(os.environ).items()
           if k not in ("MAKELEVEL", "MAKEFLAGS", "MFLAGS", approval.INTERACTIVE_ENV, approval.RUN_ENV)}
    return env | extra


def under_make(tmp_path: Path, *commands: list[str]) -> list[str]:
    """``commands`` (each an argv), in order, as one recipe of a top-level make: how an asking flow runs them."""
    makefile = tmp_path / "consumer.mk"
    makefile.write_text("run:\n\t@$(CMD); exit $$?\n")
    recipe = " && ".join(shlex.join(command) for command in commands)
    return ["make", "--no-print-directory", "-s", "-f", str(makefile), "run", f"CMD={recipe}"]


def _approvals(env_dir: Path, env: str) -> list[Path]:
    directory = approval.approvals_dir(env_dir, env)
    return sorted(directory.iterdir()) if directory.is_dir() else []


# -- who can be asked ---------------------------------------------------------------

def test_interactive_only_for_the_top_level_make_outside_ci_on_a_terminal():
    assert approval.interactive(**_terminal())


@pytest.mark.parametrize("environ,stdin,stdout", [
    ({"MAKELEVEL": None}, True, True),                    # not run from make at all
    ({"MAKELEVEL": "0"}, True, True),
    ({"MAKELEVEL": "2"}, True, True),                     # a nested make (e.g. run_integration_tests)
    ({"MAKELEVEL": " 1"}, True, True),
    ({"CI": "true"}, True, True),
    ({"CI": "1"}, True, True),
    ({"TF_BUILD": "True"}, True, True),
    ({"GITLAB_CI": "x"}, True, True),
    ({"CI": "true", "GENIERAILS_ALLOW_CI_APPLY": "1"}, True, True),  # an integration workflow never prompts
    ({"GENIERAILS_ALLOW_CI_APPLY": "1"}, True, True),
    ({"GENIERAILS_INTERACTIVE": "0"}, True, True),        # switches prompting off
    ({}, False, True),                                    # printf 'y\n' | make ...
    ({}, True, False),                                    # make ... | tee log, or captured output
    ({}, False, False),
    ({"GENIERAILS_INTERACTIVE": "1"}, False, True),       # no value switches prompting on
    ({"GENIERAILS_INTERACTIVE": "1"}, True, False),
    ({"GENIERAILS_INTERACTIVE": "yes", "MAKELEVEL": None}, True, True),
])
def test_not_interactive_when_any_condition_fails(environ, stdin, stdout):
    env = {k: v for k, v in (ON_A_TERMINAL | environ).items() if v is not None}
    assert not approval.interactive(env, Tty(stdin), Tty(stdout))


@pytest.mark.parametrize("failure", ["import", "raise", "garbled"])
def test_a_broken_ci_guard_counts_as_ci(monkeypatch, failure):
    if failure == "import":
        import scripts

        monkeypatch.setitem(sys.modules, "scripts.ci_guard", None)
        monkeypatch.delattr(scripts, "ci_guard", raising=False)
    else:
        from scripts import ci_guard

        def boom(*args, **kwargs):
            raise RuntimeError("broken")
        monkeypatch.setattr(ci_guard, "status_line", boom if failure == "raise" else lambda *a, **k: "")
    assert not approval.interactive(**_terminal())


def test_streams_that_cannot_say_are_not_terminals():
    class Closed:
        def isatty(self):
            raise ValueError("I/O operation on closed file")
    assert not approval.interactive(ON_A_TERMINAL, Closed(), Tty())
    assert not approval.interactive(ON_A_TERMINAL, object(), Tty())


# -- what counts as yes -------------------------------------------------------------

@pytest.mark.parametrize("answer", ["y", "Y", "yes", "YES", "Yes", " y ", "\tyes\n"])
def test_only_y_or_yes_is_yes(answer):
    assert approval.is_yes(answer)
    assert approval.ask(QUESTION, read=lambda _prompt: answer, **_terminal()) is True


@pytest.mark.parametrize("answer", ["", " ", "n", "N", "no", "Y es", "yy", "ye", "yes!", "y y", "true", "1",
                                    "ok", "sure", "yes please", "\x1b[A", "oui"])
def test_anything_else_is_no(answer):
    assert not approval.is_yes(answer)
    assert approval.ask(QUESTION, read=lambda _prompt: answer, **_terminal()) is False


def test_eof_is_no():
    def eof(_prompt):
        raise EOFError
    assert approval.ask(QUESTION, read=eof, **_terminal()) is False


def test_ctrl_c_exits_130():
    def interrupt(_prompt):
        raise KeyboardInterrupt
    with pytest.raises(SystemExit) as exc:
        approval.ask(QUESTION, read=interrupt, **_terminal())
    assert exc.value.code == 130


@pytest.mark.parametrize("environ,stdin,stdout", [({"CI": "true"}, True, True), ({}, False, True),
                                                  ({}, True, False), ({"MAKELEVEL": "0"}, True, True)])
def test_ask_refuses_without_a_person_at_a_terminal_and_reads_nothing(environ, stdin, stdout):
    def read(_prompt):
        raise AssertionError("read the answer")
    with pytest.raises(approval.Refused, match="no person at a terminal"):
        approval.ask(QUESTION, read=read, environ=ON_A_TERMINAL | environ, stdin=Tty(stdin), stdout=Tty(stdout))


def test_the_prompt_shown_ends_in_y_n_once():
    assert approval.prompt(QUESTION) == f"{QUESTION} [y/N] "
    assert approval.prompt(f"{QUESTION} [y/N]") == f"{QUESTION} [y/N] "


# -- the write-once record ----------------------------------------------------------

def _record(repo, manifest_path, env="prod", **kwargs):
    env_dir = repo.prod if env == "prod" else repo.cloud / "envs" / env
    return approval.record(env_dir, env, manifest_path, QUESTION, **(_terminal() | kwargs))


def test_record_is_written_once_0600_in_a_0700_dir_and_says_who_approved(repo, manifest):
    path = _record(repo, manifest, round_no=2)
    data = json.loads(path.read_text())
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert path.parent == approval.approvals_dir(repo.prod, "prod") == repo.state / "aws" / "prod" / "approvals"
    m = json.loads(manifest.read_text())
    assert path.name.startswith(f"{m['id']}-") and path.suffix == ".json"
    assert data | {"user": "", "git_user": {}, "tty": "", "approved_at": ""} == {
        "kind": "approval", "version": 1, "env": "prod", "manifest_kind": "release-manifest",
        "manifest_id": m["id"], "manifest_path": str(manifest.resolve()), "manifest_sha256": rg.sha256_file(manifest),
        "commit": repo.head(), "via": "terminal", "user": "", "git_user": {}, "host": data["host"], "tty": "",
        "approved_at": "", "question": f"{QUESTION} [y/N] ", "round": 2,
        "run_sha256": approval.run_sha256(NONCE), "asker": {"pid": 4242, "make_pid": 4241}}
    assert data["user"] and data["approved_at"] >= m["created_at"]
    assert set(data["git_user"]) == {"name", "email"}


def test_an_account_manifest_record(repo, account_manifest):
    path = _record(repo, account_manifest, env="account")
    data = json.loads(path.read_text())
    assert data["manifest_kind"] == "account-manifest" and data["env"] == "account" and data["round"] is None
    assert path.parent == repo.state / "aws" / "account" / "approvals"


def test_a_second_y_gets_its_own_record(repo, manifest):
    first, second = _record(repo, manifest), _record(repo, manifest)
    assert first != second and len(_approvals(repo.prod, "prod")) == 2


def test_record_never_overwrites(repo, manifest, monkeypatch):
    monkeypatch.setattr(rg, "_now", lambda: "2030-01-01T00:00:00.000000Z")
    first = _record(repo, manifest)
    before = first.read_bytes()
    with pytest.raises(approval.Refused, match="never overwritten"):
        _record(repo, manifest)
    assert first.read_bytes() == before


def test_record_never_writes_through_a_symlink(repo, manifest, monkeypatch, tmp_path):
    monkeypatch.setattr(rg, "_now", lambda: "2030-01-01T00:00:00.000000Z")
    directory = approval.approvals_dir(repo.prod, "prod")
    directory.mkdir(parents=True, mode=0o700)
    target = tmp_path / "elsewhere.json"
    target.write_text("theirs\n")
    m = json.loads(manifest.read_text())
    (directory / f"{m['id']}-2030-01-01T000000000000Z.json").symlink_to(target)
    with pytest.raises(approval.Refused, match="never overwritten"):
        _record(repo, manifest)
    assert target.read_text() == "theirs\n"
    dangling = tmp_path / "missing.json"
    (directory / f"{m['id']}-2030-01-01T000000000000Z.json").unlink()
    (directory / f"{m['id']}-2030-01-01T000000000000Z.json").symlink_to(dangling)
    with pytest.raises(approval.Refused, match="never overwritten"):
        _record(repo, manifest)
    assert not dangling.exists()


def test_record_refuses_a_symlinked_approvals_dir(repo, manifest, tmp_path):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    directory = approval.approvals_dir(repo.prod, "prod")
    directory.parent.mkdir(parents=True, exist_ok=True)
    directory.symlink_to(elsewhere)
    with pytest.raises(approval.Refused, match="not a directory"):
        _record(repo, manifest)
    assert not list(elsewhere.iterdir())


@pytest.mark.parametrize("environ,stdin,stdout", [({"CI": "true"}, True, True), ({}, False, True),
                                                  ({}, True, False), ({"MAKELEVEL": "2"}, True, True),
                                                  ({"GENIERAILS_INTERACTIVE": "0"}, True, True)])
def test_record_refuses_without_a_person_at_a_terminal(repo, manifest, environ, stdin, stdout):
    with pytest.raises(approval.Refused, match="no person at a terminal"):
        approval.record(repo.prod, "prod", manifest, QUESTION, environ=ON_A_TERMINAL | environ, stdin=Tty(stdin),
                        stdout=Tty(stdout))
    assert not _approvals(repo.prod, "prod")


def test_record_refuses_something_that_is_not_a_manifest(repo, tmp_path):
    other = tmp_path / "x.json"
    other.write_text(json.dumps({"kind": "release"}))
    with pytest.raises(approval.Refused, match="not an account or release manifest"):
        _record(repo, other)


# -- check: the record approves this manifest as it is now ------------------------------

def test_check_returns_the_record_to_embed(repo, manifest):
    path = _record(repo, manifest)
    checked = approval.check(repo.prod, "prod", manifest, path)
    assert checked == json.loads(path.read_text()) | {"record": str(path), "record_sha256": rg.sha256_file(path)}
    assert approval.embed(repo.prod, "prod", manifest, None) == {"via": "external"}


def _forge(path: Path, mode: int = 0o600, **changes) -> Path:
    """A copy of the record with ``changes``, written by hand (0600 unless ``mode``) beside it."""
    data = json.loads(path.read_text()) | changes
    forged = path.with_name(path.stem + "-forged.json")
    forged.write_text(json.dumps(data))
    forged.chmod(mode)
    return forged


@pytest.mark.parametrize("field,value,why", [
    ("manifest_sha256", "0" * 64, "manifest_sha256 differ"),
    ("manifest_id", "20300101T000000-000000000000", "manifest_id differ"),
    ("commit", "0" * 40, "commit differ"),
    ("host", "someone-elses-laptop", "host differ"),
    ("env", "dev", "env differ"),
    ("via", "external", "via differ"),
    ("manifest_kind", "account-manifest", "manifest_kind differ"),
    ("approved_at", "2000-01-01T00:00:00.000000Z", "predates the manifest"),
    ("approved_at", None, "predates the manifest"),
    ("kind", "release", "not a version 1 approval record"),
    ("version", 2, "not a version 1 approval record"),
])
def test_check_refuses_a_record_that_does_not_approve_this_manifest(repo, manifest, field, value, why):
    forged = _forge(_record(repo, manifest), **{field: value})
    with pytest.raises(approval.Refused, match=re.escape(why)):
        approval.check(repo.prod, "prod", manifest, forged)


def test_check_refuses_a_record_for_another_manifest(repo, manifest):
    record = _record(repo, manifest)
    second = _second_manifest(repo)
    with pytest.raises(approval.Refused, match="manifest_id, manifest_sha256 differ"):
        approval.check(repo.prod, "prod", second, record)


def test_check_refuses_once_the_manifest_changed(repo, manifest):
    record = _record(repo, manifest)
    manifest.chmod(0o600)
    manifest.write_text(manifest.read_text().replace('"final": true', '"final": false'))
    with pytest.raises(approval.Refused, match="manifest_sha256 differ"):
        approval.check(repo.prod, "prod", manifest, record)


def test_check_refuses_a_symlink_a_relative_path_or_a_record_outside_the_approvals_dir(repo, manifest, tmp_path):
    record = _record(repo, manifest)
    link = record.with_name("link.json")
    link.symlink_to(record)
    with pytest.raises(approval.Refused, match="not a regular file"):
        approval.check(repo.prod, "prod", manifest, link)
    outside = tmp_path / "copy.json"
    outside.write_bytes(record.read_bytes())
    with pytest.raises(approval.Refused, match="is not in"):
        approval.check(repo.prod, "prod", manifest, outside)
    with pytest.raises(approval.Refused, match="not an absolute path"):
        approval.check(repo.prod, "prod", manifest, Path(record.name))
    with pytest.raises(approval.Refused, match="can't be read"):
        approval.check(repo.prod, "prod", manifest, record.with_name("missing.json"))


def test_check_refuses_a_record_of_another_env(repo, manifest, account_manifest):
    record = _record(repo, account_manifest, env="account")
    with pytest.raises(approval.Refused, match="is not in"):
        approval.check(repo.prod, "prod", manifest, record)


# -- check: only a record this run's own prompt wrote counts ----------------------------

def test_check_refuses_a_hand_written_record_without_this_runs_nonce(repo, manifest):
    genuine = json.loads(_record(repo, manifest).read_text())
    for nonce in ({"run_sha256": None}, {"run_sha256": "0" * 64}):
        with pytest.raises(approval.Refused, match="was not written by this run's own prompt$"):
            approval.check(repo.prod, "prod", manifest, _forge(_record(repo, manifest), **nonce))
    hand = approval.approvals_dir(repo.prod, "prod") / "by-hand.json"
    hand.write_text(json.dumps({k: v for k, v in genuine.items() if k not in ("run_sha256", "asker")}))
    hand.chmod(0o600)
    with pytest.raises(approval.Refused, match="was not written by this run's own prompt$"):
        approval.check(repo.prod, "prod", manifest, hand)


def test_check_refuses_a_record_from_another_run(repo, manifest, monkeypatch):
    record = _record(repo, manifest)
    monkeypatch.setenv(approval.RUN_ENV, approval.new_run())  # the next run's own nonce
    with pytest.raises(approval.Refused, match="was not written by this run's own prompt$"):
        approval.check(repo.prod, "prod", manifest, record)
    with pytest.raises(approval.Refused, match="was not written by this run's own prompt$"):
        approval.check(repo.prod, "prod", manifest, record, run=approval.new_run())
    assert approval.check(repo.prod, "prod", manifest, record, run=NONCE)["via"] == "terminal"
    monkeypatch.delenv(approval.RUN_ENV)  # a run that never asked
    with pytest.raises(approval.Refused, match=re.escape("no approval run (GENIERAILS_APPROVAL_RUN)")):
        approval.check(repo.prod, "prod", manifest, record)


@pytest.mark.parametrize("mode", [0o644, 0o640, 0o604, 0o400, 0o700])
def test_check_refuses_a_record_that_is_not_0600(repo, manifest, mode):
    forged = _forge(_record(repo, manifest), mode=mode)
    with pytest.raises(approval.Refused, match=f"is mode {mode:o}, not 600"):
        approval.check(repo.prod, "prod", manifest, forged)


def test_check_refuses_a_record_owned_by_another_user(repo, manifest, monkeypatch):
    record = _record(repo, manifest)
    info = os.lstat(record)
    real_fstat = os.fstat
    monkeypatch.setattr(approval.os, "fstat", lambda fd: os.stat_result(
        (*real_fstat(fd)[:4], info.st_uid + 1, *real_fstat(fd)[5:])))
    with pytest.raises(approval.Refused, match=f"is owned by uid {info.st_uid + 1}, not this user"):
        approval.check(repo.prod, "prod", manifest, record)


def test_check_refuses_an_approvals_dir_owned_by_another_user_or_a_symlink(repo, manifest, monkeypatch, tmp_path):
    record = _record(repo, manifest)
    monkeypatch.setattr(approval.os, "getuid", lambda: os.lstat(record).st_uid + 1)
    with pytest.raises(approval.Refused, match="is a symlink or not owned by this user"):
        approval.check(repo.prod, "prod", manifest, record)
    monkeypatch.undo()
    monkeypatch.setenv(approval.RUN_ENV, NONCE)
    monkeypatch.setattr(approval, "_chain", lambda pid: list(THIS_RUN))
    directory = record.parent
    moved = tmp_path / "moved-approvals"
    directory.rename(moved)
    directory.symlink_to(moved)
    with pytest.raises(approval.Refused, match="is a symlink or not owned by this user|is not in"):
        approval.check(repo.prod, "prod", manifest, directory / record.name)


def test_check_refuses_a_symlink_to_a_genuine_record(repo, manifest):
    record = _record(repo, manifest)
    link = record.with_name("link.json")
    link.symlink_to(record)
    with pytest.raises(approval.Refused, match="not a regular file"):
        approval.check(repo.prod, "prod", manifest, link)


@pytest.mark.parametrize("chain", [
    [(9, "sh"), (8, "make"), (7, "zsh")],                    # another run's make
    [(4242, "sh"), (4241, "zsh"), (1, "launchd")],           # its pid reused, not under a make
    [(4242, "sh"), (4000, "make"), (1, "launchd")],          # under another make
    [(4241, "make"), (4242, "sh")],                          # the make below it
    [(4242, "sh")],
    [],
])
def test_check_refuses_a_record_whose_asker_is_not_running_above_it_under_the_same_make(repo, manifest,
                                                                                     monkeypatch, chain):
    record = _record(repo, manifest)
    monkeypatch.setattr(approval, "_chain", lambda pid: list(chain))
    with pytest.raises(approval.Refused, match="its asker is not running above this process under the same make"):
        approval.check(repo.prod, "prod", manifest, record)


@pytest.mark.parametrize("asker", [None, {}, {"pid": 4242}, {"pid": "4242", "make_pid": 4241}, [4242, 4241]])
def test_check_refuses_a_record_with_a_malformed_asker(repo, manifest, asker):
    with pytest.raises(approval.Refused, match="was not written by this run's own prompt"):
        approval.check(repo.prod, "prod", manifest, _forge(_record(repo, manifest), asker=asker))


def test_nested_makes_shells_and_the_flows_own_process_between_the_check_and_the_asker_are_fine(
        repo, manifest, monkeypatch):
    record = _record(repo, manifest)
    nested = [(30, "python3"), (29, "bash"), (28, "sh"), (27, "gmake"), (26, "python3"), (4242, "sh"),
              (4241, "make")]  # e.g. scripts/release_loop.py running the nested makes
    monkeypatch.setattr(approval, "_chain", lambda pid: nested[1:])  # the check's parent and up
    assert approval.check(repo.prod, "prod", manifest, record)["via"] == "terminal"


def test_record_refuses_without_a_nonce_or_an_asking_make_recipe(repo, manifest, monkeypatch):
    for environ in ({"MAKELEVEL": "1"}, {"MAKELEVEL": "1", approval.RUN_ENV: "ab"},
                    {"MAKELEVEL": "1", approval.RUN_ENV: "AB" * 16}):
        with pytest.raises(approval.Refused, match=re.escape("no approval run (GENIERAILS_APPROVAL_RUN)")):
            approval.record(repo.prod, "prod", manifest, QUESTION, environ=environ, stdin=Tty(), stdout=Tty())
    with pytest.raises(approval.Refused, match=re.escape("no approval run (GENIERAILS_APPROVAL_RUN)")):
        approval.record(repo.prod, "prod", manifest, QUESTION, run="", **_terminal())
    for chain in ([(10, "sh"), (9, "zsh"), (1, "launchd")], [(10, "python3")], []):
        monkeypatch.setattr(approval, "_chain", lambda pid, c=chain: list(c))
        with pytest.raises(approval.Refused, match="didn't run in a make recipe"):
            _record(repo, manifest)
    assert not _approvals(repo.prod, "prod")


def test_the_asker_is_the_nearest_process_directly_under_a_make():
    assert approval._asker([(12, "python3"), (11, "bash"), (10, "make"), (9, "zsh")]) == {"pid": 11, "make_pid": 10}
    assert approval._asker([(12, "sh"), (10, "gnumake")]) == {"pid": 12, "make_pid": 10}
    # A recipe make ran without a shell (one simple command), or release_loop.py recording in-process.
    assert approval._asker([(20, "python3"), (19, "make"), (1, "launchd")]) == {"pid": 20, "make_pid": 19}


def test_the_run_nonce_is_32_lowercase_hex_and_fresh_each_time():
    first, second = approval.new_run(), approval.new_run()
    assert first != second and approval.run_of({approval.RUN_ENV: first}) == first
    assert approval.run_sha256(first) == __import__("hashlib").sha256(first.encode()).hexdigest()


def test_the_real_process_chain_names_this_process_and_its_parents(monkeypatch):
    monkeypatch.undo()
    chain = approval._chain(os.getpid())
    assert chain[0][0] == os.getpid() and chain[0][1].startswith("python")
    assert len(chain) >= 2 and all("/" not in name for _, name in chain)


# -- tag-value removals: the same y/N, louder ---------------------------------------------

def test_an_account_manifest_that_removes_values_names_each_one_above_a_louder_question():
    manifest = {"kind": "account-manifest", "tag_value_removal": {"removed": ["gr_treatment:old", "pii:ssn_v1"]}}
    lines, question = approval.removal_warning(manifest, QUESTION)
    assert lines[1] == ("!! WARNING: approving this account manifest REMOVES 2 tag value(s) from every workspace "
                        "on the metastore:")
    assert lines[2:4] == ["!!   gr_treatment:old", "!!   pii:ssn_v1"]
    assert question == "Approve and apply this account manifest, INCLUDING REMOVING THE 2 TAG VALUE(S) ABOVE?"
    assert approval.prompt(question).endswith("ABOVE? [y/N] ")


@pytest.mark.parametrize("manifest", [
    {"kind": "account-manifest", "tag_value_removal": {"removed": []}},
    {"kind": "account-manifest"},
    {"kind": "release-manifest", "tag_value_removal": {"removed": ["x:y"]}},
])
def test_no_removals_no_warning(manifest):
    assert approval.removal_warning(manifest, QUESTION) == ([], QUESTION)


def test_approve_shows_the_removal_warning_right_above_the_prompt_and_records_the_louder_question(
        repo, account_manifest, tmp_path):
    env_dir = repo.cloud / "envs" / "account"
    args = ["approve", "--env-dir", str(env_dir), "--env", "account", "--manifest", str(account_manifest),
            "--question", QUESTION, "--expect-digest", rg.sha256_file(account_manifest)]
    rc, line, shown = _pty_under_make(args, "y", _environ() | repo.env | {approval.RUN_ENV: NONCE}, tmp_path)
    assert rc == 0, shown
    warning = shown.index("!! WARNING: approving this account manifest REMOVES 1 tag value(s)")
    assert warning < shown.index("!!   gr_treatment:old") < shown.index(
        "Approve and apply this account manifest, INCLUDING REMOVING THE 1 TAG VALUE(S) ABOVE? [y/N] ")
    [record] = _approvals(env_dir, "account")
    assert json.loads(record.read_text())["question"] == (
        "Approve and apply this account manifest, INCLUDING REMOVING THE 1 TAG VALUE(S) ABOVE? [y/N] ")


# -- the bytes approved are the bytes shown -------------------------------------------------

def _mutate(manifest: Path, **changes) -> None:
    manifest.chmod(0o600)
    manifest.write_text(json.dumps(json.loads(manifest.read_text()) | changes, indent=2))
    manifest.chmod(0o400)


def test_pin_reads_once_and_refuses_another_digest(repo, manifest):
    data, digest = approval.pin(manifest, "prod")
    assert digest == rg.sha256_file(manifest) and data == json.loads(manifest.read_text())
    assert approval.pin(manifest, "prod", digest)[1] == digest
    with pytest.raises(approval.Refused, match="changed after it was shown"):
        approval.pin(manifest, "prod", "0" * 64)
    with pytest.raises(approval.Refused, match="is not a sha256 digest"):
        approval.require_digest(manifest, "abc")
    with pytest.raises(approval.Refused, match="is a manifest for prod, not dev"):
        approval.pin(manifest, "dev")
    link = manifest.with_name("link.json")
    link.symlink_to(manifest)
    with pytest.raises(approval.Refused, match="can't be read"):
        approval.pin(link, "prod")


def test_record_with_a_digest_pins_it_and_refuses_a_changed_manifest(repo, manifest):
    digest = rg.sha256_file(manifest)
    record = _record(repo, manifest, expect_digest=digest)
    assert json.loads(record.read_text())["manifest_sha256"] == digest
    _mutate(manifest, host="mutated-after-display")
    with pytest.raises(approval.Refused, match="changed after it was shown"):
        _record(repo, manifest, expect_digest=digest)
    assert _approvals(repo.prod, "prod") == [record]
    with pytest.raises(approval.Refused, match="manifest_sha256 differ"):  # and under the apply's lock
        approval.check(repo.prod, "prod", manifest, record)


def test_show_prints_the_summary_and_the_digest_of_the_bytes_it_read(repo, account_manifest, tmp_path):
    env_dir = repo.cloud / "envs" / "account"
    (tmp_path / "answer").touch()
    rc, out, err = _cli("show", "--env-dir", str(env_dir), "--env", "account", "--manifest", str(account_manifest),
                        "--answer-file", str(tmp_path / "answer"), env=_environ() | repo.env)
    digest = rg.sha256_file(account_manifest)
    assert rc == 0, err
    assert (tmp_path / "answer").read_text() == f"{approval.DIGEST}:{digest}\n"
    assert f"  Manifest: {account_manifest.resolve()}" in out and "!! TAG VALUES THIS MANIFEST REMOVES" in out
    assert out.rstrip().endswith(f"  Manifest sha256: {digest} (approving approves exactly these bytes)")


def test_digest_check_against_a_digest_or_the_records(repo, manifest):
    digest = rg.sha256_file(manifest)
    record = _record(repo, manifest, expect_digest=digest)
    env = _environ() | repo.env
    for how in (("--expect-digest", digest), ("--approval", str(record))):
        assert _cli("digest-check", "--manifest", str(manifest), *how, env=env) == (0, approval.VALID + "\n", "")
    _mutate(manifest, host="mutated-after-approval")
    for how in (("--expect-digest", digest), ("--approval", str(record))):
        rc, out, err = _cli("digest-check", "--manifest", str(manifest), *how, env=env)
        assert (rc, out) == (1, approval.INVALID + "\n") and "changed after it was shown" in err


def test_approve_refuses_before_asking_when_the_manifest_is_not_the_one_shown(repo, manifest, tmp_path):
    args = ["approve", "--env-dir", str(repo.prod), "--env", "prod", "--manifest", str(manifest),
            "--question", QUESTION, "--expect-digest", "0" * 64]
    rc, line, shown = _pty_under_make(args, "y", _environ() | repo.env | {approval.RUN_ENV: NONCE}, tmp_path)
    assert line == approval.REFUSED + "\n" and "changed after it was shown" in shown, shown
    assert "[y/N]" not in shown and not _approvals(repo.prod, "prod")


def test_approve_refuses_a_manifest_changed_while_the_question_was_up_and_records_nothing(repo, manifest,
                                                                                        tmp_path):
    """Codex's repro on approve itself: the file changes between the prompt and the y."""
    args = ["approve", "--env-dir", str(repo.prod), "--env", "prod", "--manifest", str(manifest),
            "--question", QUESTION, "--expect-digest", rg.sha256_file(manifest)]
    answer_file = tmp_path / "answer"
    answer_file.touch()
    rc, shown = pty_run(under_make(tmp_path, [sys.executable, str(SCRIPT), *args, "--answer-file", str(answer_file)]),
                        "y", _environ() | repo.env | {approval.RUN_ENV: NONCE},
                        before_answer=lambda: _mutate(manifest, host="mutated-at-the-prompt"))
    assert rc != 0 and answer_file.read_text() == approval.REFUSED + "\n", shown
    assert "changed after it was shown" in shown and not _approvals(repo.prod, "prod")


# -- the CLI: one exact versioned answer ----------------------------------------------

def _cli(*args, env=None, stdin=subprocess.DEVNULL):
    result = subprocess.run([sys.executable, str(SCRIPT), *args], env=env or _environ(MAKELEVEL="1"),
                            stdin=stdin, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


def test_the_answers_are_the_documented_lines():
    assert approval.ANSWERS == {
        "interactive": {(0, "genierails-approval:v1:interactive"), (0, "genierails-approval:v1:not-interactive")},
        "ask": {(0, "genierails-approval:v1:yes"), (1, "genierails-approval:v1:no"),
                (1, "genierails-approval:v1:refused")},
        "approve": {(0, "genierails-approval:v1:approved"), (1, "genierails-approval:v1:no"),
                    (1, "genierails-approval:v1:refused")},
        "check": {(0, "genierails-approval:v1:valid"), (1, "genierails-approval:v1:invalid")},
        "show": {(0, "genierails-approval:v1:digest")},
        "digest-check": {(0, "genierails-approval:v1:valid"), (1, "genierails-approval:v1:invalid")}}


def test_the_makefile_accepts_only_the_exact_check_answers():
    text = (Path(approval.__file__).parents[1] / "Makefile.shared").read_text()
    block = text.split("override _APPROVAL_CHECK = ", 1)[1].split("\noverride ", 1)[0]
    pairs = set(re.findall(r'"(\d+):(genierails-approval:v\d+:[a-z-]+)"\)', block))
    assert {(int(rc), line) for rc, line in pairs} == approval.ANSWERS["check"]
    assert '*) echo "ERROR: the approval check' in block and "exit 1 ;;" in block
    assert not re.search(r"\bif !? *python3 \"\$\(SHARED_ROOT\)/scripts/approval\.py\"", text)


def test_cli_off_a_terminal(repo, manifest):
    assert _cli("interactive") == (0, approval.NOT_INTERACTIVE + "\n", "")
    rc, out, err = _cli("ask", "--question", QUESTION)
    assert (rc, out) == (1, approval.REFUSED + "\n") and "no person at a terminal" in err
    rc, out, err = _cli("approve", "--env-dir", str(repo.prod), "--env", "prod", "--manifest", str(manifest),
                        "--question", QUESTION, "--expect-digest", rg.sha256_file(manifest), env=_environ(MAKELEVEL="1") | repo.env)
    assert (rc, out) == (1, approval.REFUSED + "\n") and not _approvals(repo.prod, "prod")


def test_cli_check(repo, manifest, tmp_path):
    record = _record(repo, manifest)  # this test's in-process run, so no other process's
    args = ("check", "--env-dir", str(repo.prod), "--env", "prod", "--manifest", str(manifest))
    env = _environ() | repo.env | {approval.RUN_ENV: NONCE}
    rc, out, err = _cli(*args, "--approval", str(_forge(record, host="elsewhere")), env=env)
    assert (rc, out) == (1, approval.INVALID + "\n") and "host differ" in err
    rc, out, err = _cli(*args, "--approval", str(record), env=env)
    assert (rc, out) == (1, approval.INVALID + "\n") and "was not written by this run's own prompt" in err


def test_cli_check_is_valid_only_inside_the_run_that_asked(repo, manifest, tmp_path):
    approve = [sys.executable, str(SCRIPT), "approve", "--env-dir", str(repo.prod), "--env", "prod", "--manifest",
               str(manifest), "--question", QUESTION, "--expect-digest", rg.sha256_file(manifest), "--answer-file",
               str(tmp_path / "answer")]
    check = [sys.executable, str(SCRIPT), "check", "--env-dir", str(repo.prod), "--env", "prod", "--manifest",
             str(manifest), "--approval"]
    (tmp_path / "answer").touch()
    script = tmp_path / "approve-then-check.sh"
    script.write_text(f'{shlex.join(approve)} || exit 1\n'
                      f'record="$(sed "s/^{approval.APPROVED}://" {tmp_path / "answer"})"\n'
                      f'{shlex.join(check)} "$record"\n')
    env = _environ() | repo.env | {approval.RUN_ENV: NONCE}
    rc, shown = pty_run(under_make(tmp_path, ["sh", str(script)]), "y", env)
    assert rc == 0 and shown.rstrip().endswith(approval.VALID), shown
    [record] = _approvals(repo.prod, "prod")
    # The same record, checked by another make run (same nonce even): not this run's prompt.
    rc, shown = pty_run(under_make(tmp_path, [*check, str(record)]), None, env)
    assert rc != 0 and "its asker is not running above this process" in shown, shown
    assert shown.rstrip().splitlines()[-2] == approval.INVALID


def test_cli_an_internal_error_answers_nothing(repo, tmp_path):
    rc, out, err = _cli("check", "--env-dir", str(repo.prod), "--env", "prod", "--manifest",
                        str(tmp_path / "none.json"), "--approval", str(tmp_path / "none.json"),
                        env=_environ() | repo.env)
    assert rc == 1 and out == approval.INVALID + "\n"  # an unreadable record is invalid, not an error
    (tmp_path / "answer").write_text("stale\n")
    rc, out, err = _cli("interactive", "--answer-file", str(tmp_path / "answer"))
    assert (rc, out) == (3, "") and "nothing was approved" in err


def _pty(args, answer, env, tmp_path):
    answer_file = tmp_path / "answer"
    answer_file.unlink(missing_ok=True)
    answer_file.touch()
    rc, shown = pty_run([sys.executable, str(SCRIPT), *args, "--answer-file", str(answer_file)], answer, env)
    return rc, answer_file.read_text(), shown


@pytest.mark.parametrize("answer,expected", [("y", (0, approval.YES)), ("YES", (0, approval.YES)),
                                             ("n", (1, approval.NO)), ("", (1, approval.NO)),
                                             ("Y es", (1, approval.NO)), ("yy", (1, approval.NO)),
                                             (CTRL_D, (1, approval.NO))])
def test_ask_on_a_terminal(tmp_path, answer, expected):
    rc, line, shown = _pty(["ask", "--question", QUESTION], answer, _environ(MAKELEVEL="1"), tmp_path)
    assert (rc, line) == (expected[0], expected[1] + "\n"), shown
    assert f"{QUESTION} [y/N] " in shown


def _pty_under_make(args, answer, env, tmp_path):
    answer_file = tmp_path / "answer"
    answer_file.unlink(missing_ok=True)
    answer_file.touch()
    rc, shown = pty_run(under_make(tmp_path, [sys.executable, str(SCRIPT), *args, "--answer-file",
                                              str(answer_file)]), answer, env)
    return rc, answer_file.read_text(), shown


def test_ctrl_c_at_the_terminal_exits_130_answers_nothing_and_writes_no_record(repo, manifest, tmp_path):
    args = ["approve", "--env-dir", str(repo.prod), "--env", "prod", "--manifest", str(manifest),
            "--question", QUESTION, "--expect-digest", rg.sha256_file(manifest)]
    rc, line, shown = _pty_under_make(args, CTRL_C, _environ() | repo.env | {approval.RUN_ENV: NONCE}, tmp_path)
    assert rc != 0 and line == "", shown
    assert not _approvals(repo.prod, "prod")


def test_approve_on_a_terminal_writes_the_record_only_on_y(repo, manifest, tmp_path, monkeypatch):
    args = ["approve", "--env-dir", str(repo.prod), "--env", "prod", "--manifest", str(manifest),
            "--question", QUESTION, "--expect-digest", rg.sha256_file(manifest), "--round", "3"]
    env = _environ() | repo.env | {approval.RUN_ENV: NONCE}
    for answer in ("n", "", CTRL_D, "Y es"):
        rc, line, shown = _pty_under_make(args, answer, env, tmp_path)
        assert (rc, line) == (2, approval.NO + "\n"), shown  # make's own exit code for a failing recipe
    assert not _approvals(repo.prod, "prod")
    rc, line, shown = _pty_under_make(args, "y", env, tmp_path)
    [record] = _approvals(repo.prod, "prod")
    assert (rc, line) == (0, f"{approval.APPROVED}:{record}\n"), shown
    data = json.loads(record.read_text())
    assert data["via"] == "terminal" and data["round"] == 3 and data["tty"].startswith("/dev/")
    assert data["run_sha256"] == approval.run_sha256(NONCE)
    # The recipe that asked (the shell running it), directly under the top-level make.
    asker = data["asker"]
    assert set(asker) == {"pid", "make_pid"} and asker["pid"] != asker["make_pid"]
    monkeypatch.setattr(approval, "_chain", lambda pid: [(asker["pid"], "sh"), (asker["make_pid"], "make")])
    assert approval.check(repo.prod, "prod", manifest, record)["manifest_sha256"] == rg.sha256_file(manifest)


def test_approve_refuses_before_asking_without_a_nonce_or_outside_a_make_recipe(repo, manifest, tmp_path):
    args = ["approve", "--env-dir", str(repo.prod), "--env", "prod", "--manifest", str(manifest),
            "--question", QUESTION, "--expect-digest", rg.sha256_file(manifest)]
    rc, line, shown = _pty_under_make(args, "y", _environ() | repo.env, tmp_path)  # no nonce
    assert line == approval.REFUSED + "\n" and "no approval run (GENIERAILS_APPROVAL_RUN)" in shown, shown
    assert "[y/N]" not in shown
    # MAKELEVEL=1 typed by hand, not under a make: no make recipe asked.
    rc, line, shown = _pty(args, "y", _environ(MAKELEVEL="1") | repo.env | {approval.RUN_ENV: NONCE}, tmp_path)
    assert (rc, line) == (1, approval.REFUSED + "\n") and "didn't run in a make recipe" in shown, shown
    assert "[y/N]" not in shown and not _approvals(repo.prod, "prod")


@pytest.mark.parametrize("extra", [{"CI": "true"}, {"GENIERAILS_ALLOW_CI_APPLY": "1"},
                                   {"GENIERAILS_INTERACTIVE": "0"}, {"MAKELEVEL": "2"}, {"MAKELEVEL": ""}])
def test_on_a_terminal_ci_or_an_opt_out_still_refuses(repo, manifest, tmp_path, extra):
    args = ["approve", "--env-dir", str(repo.prod), "--env", "prod", "--manifest", str(manifest),
            "--question", QUESTION, "--expect-digest", rg.sha256_file(manifest)]
    rc, line, shown = _pty(args, "y", _environ(MAKELEVEL="1") | repo.env | extra, tmp_path)
    assert (rc, line) == (1, approval.REFUSED + "\n"), shown
    assert "[y/N]" not in shown and not _approvals(repo.prod, "prod")


# -- nothing approves without a person at a terminal -------------------------------------

APPROVE_ENV = {"YES": "1", "APPROVE": "1", "APPROVAL": "1", "GENIERAILS_APPROVE": "1", "GENIERAILS_YES": "1",
               "GENIERAILS_INTERACTIVE": "1", "ASSUME_YES": "1", "FORCE": "1", "MAKELEVEL": "1"}


def test_no_flag_or_variable_approves_off_a_terminal(repo, manifest):
    base = ["approve", "--env-dir", str(repo.prod), "--env", "prod", "--manifest", str(manifest),
            "--question", QUESTION, "--expect-digest", rg.sha256_file(manifest)]
    for flag in ("--yes", "-y", "--force", "--assume-yes", "--approve", "--non-interactive", "--answer=y"):
        rc, out, err = _cli(*base, flag, env=_environ() | repo.env | APPROVE_ENV)
        assert rc == 2 and out == "" and "unrecognized arguments" in err, flag
    piped = subprocess.run([sys.executable, str(SCRIPT), *base], input="y\nyes\n", text=True, capture_output=True,
                           env=_environ() | repo.env | APPROVE_ENV)
    assert (piped.returncode, piped.stdout) == (1, approval.REFUSED + "\n")
    assert not _approvals(repo.prod, "prod")


def test_the_cli_has_no_command_that_records_without_asking():
    parser = approval._parser()
    sub = next(a for a in parser._actions if isinstance(a.choices, dict))
    assert set(sub.choices) == {"interactive", "ask", "show", "approve", "check", "digest-check"}
    options = {name: sorted(o for a in cmd._actions for o in a.option_strings if o not in ("-h", "--help"))
               for name, cmd in sub.choices.items()}
    assert options == {
        "interactive": ["--answer-file"], "ask": ["--answer-file", "--question"],
        "show": ["--answer-file", "--env", "--env-dir", "--manifest"],
        "approve": ["--answer-file", "--env", "--env-dir", "--expect-digest", "--manifest", "--question", "--round"],
        "digest-check": ["--answer-file", "--approval", "--expect-digest", "--manifest"],
        "check": ["--answer-file", "--approval", "--env", "--env-dir", "--manifest"]}


def test_approval_reads_no_variable_that_could_switch_it_on():
    text = SCRIPT.read_text()
    read = set(re.findall(r"environ\.get\(\s*\"?([A-Z_]+)\"?", text)) | set(re.findall(r"environ\[\"([A-Z_]+)\"\]", text))
    # RUN_ENV only binds a record to the run that asked; it never makes anything interactive.
    assert read <= {"MAKELEVEL", "INTERACTIVE_ENV", "RUN_ENV"}, read
