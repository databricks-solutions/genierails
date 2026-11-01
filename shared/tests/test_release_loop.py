"""make release ENV=<deterministic env> at a terminal: the round-by-round loop (scripts/release_loop.py).

Real make runs the real release, release-plan, _release-plan-roots and
_release-manifest recipes on a pseudo-terminal (tests/pty_harness.py), with
the test as the person answering each round's y/N. The multi-round cases use
test_first_release_rounds' fake Unity Catalog ("the world"); the rest use
test_release_gate_make's stubs. Before round 1, an account plan with changes
gets rehearse's account step (the account manifest, y/N, apply-account's path);
elsewhere the account plan is stubbed empty. Off a terminal, in CI, with
MANIFEST= or GENIERAILS_INTERACTIVE=0, release runs _release-manifest once, as
before.
"""

from __future__ import annotations

import json
import os
import re
import signal
import stat
import subprocess
from pathlib import Path
from unittest import mock

import pytest

from scripts import approval
from scripts import environment_lock
from scripts import release_gate as rg
from scripts import release_loop
from tests import script_stubs
from tests.pty_harness import CTRL_C, CTRL_D, pty_run
from tests.test_approval import QUESTION, RUN, THIS_RUN, Tty
from tests.test_first_release_rounds import build_world
from tests.test_release_gate_make import LEGACY_ENV, ROOT, Make, _planned, make, repo  # noqa: F401 (fixtures)

PROMPT = "[y/N] "
COMPLETE = "=== Release complete (prod) after {n} round(s), each approved at this terminal ==="
WORLD_ARGS = ("IMPORT_EXISTING_SCRIPT=true",)
MAKE_FAILED = 2  # make's exit code when the release recipe (the loop) fails


def _on_a_pty(make: Make, answers, *extra, environ_extra=None, stubs=None, timeout=900, account_changes=False,
              root_runner=None):
    if root_runner is None and not account_changes:  # the account step's probe finds no changes
        root_runner = make.empty_account_runner()
    args, environ = make.command("release", "prod", *extra, stubs=stubs, environ_extra=environ_extra,
                                 root_runner=root_runner)
    environ = {k: v for k, v in environ.items() if k != approval.RUN_ENV}
    return pty_run(args, answers, environ, cwd=ROOT / make.cloud, timeout=timeout)


def _unlocked(*env_dirs: Path) -> bool:
    return not any(environment_lock.lock_path(d).exists() for d in env_dirs)


def _approvals(repo) -> list[Path]:
    directory = approval.approvals_dir(repo.prod, "prod")
    return sorted(directory.iterdir()) if directory.is_dir() else []


def _phase_records(repo) -> list[dict]:
    directory = rg.env_state(repo.cloud, "prod") / "receipts" / "phases"
    return [json.loads(p.read_text()) for p in sorted(directory.glob("*.json"))] if directory.is_dir() else []


@pytest.fixture
def world(tmp_path, monkeypatch):
    make, repo, state, stubs = build_world(tmp_path, monkeypatch, passthrough=("release-plan",))
    assert make("rehearse", "dev").returncode == 0
    make.reset()
    return make, repo, state, stubs


@pytest.fixture
def loop_make(tmp_path, repo):
    """The plain stubs, with release-plan and _release-manifest run for real below the loop."""
    return Make(tmp_path, repo, passthrough=("release-plan", "_release-manifest"))


# -- the whole release, one y per round ----------------------------------------------

def test_a_first_release_at_a_terminal_runs_every_round_to_complete(world):
    make, repo, state, stubs = world
    rc, shown = _on_a_pty(make, ["y"] * release_loop.MAX_ROUNDS, *WORLD_ARGS, stubs=stubs)
    assert rc == 0, shown
    # The same four rounds as test_a_first_release_opens_can_run_only_after_masks_and_select_and_never_ends_withheld.
    assert COMPLETE.format(n=4) in shown and shown.count(PROMPT) == 4
    assert "=== Release complete (prod) ===" in shown and "access was withheld" not in shown
    world_state = json.loads(state.read_text())
    assert world_state["violations"] == [] and world_state["events"] == ["masks", "masks", "select", "can_run"]
    names = make.names()
    assert names.count("release-plan") == 4 and names.count("_release-manifest") == 4
    assert not {"rehearse", "apply", "_rehearse-steps", "release"} & set(names)
    phases = _phase_records(repo)
    assert len(phases) == 3
    for phase in phases:
        assert phase["approval"]["via"] == "terminal" and phase["approval"]["manifest_sha256"] == phase["manifest_sha256"]
    receipt = rg.last_release(repo.cloud, "prod")
    assert receipt["approval"]["via"] == "terminal"
    assert receipt["approval"]["manifest_sha256"] == receipt["manifest"]["sha256"]
    assert set(receipt["phase_approvals"]) == set(receipt["phase_manifests"]) == {p["id"] for p in phases}
    assert sorted(a["round"] for a in [*receipt["phase_approvals"].values(), receipt["approval"]]) == [1, 2, 3, 4]
    records = _approvals(repo)
    assert len(records) == 4
    for record in records:
        info = record.lstat()
        assert stat.S_IMODE(info.st_mode) == 0o600 and info.st_uid == os.getuid()
    assert len({json.loads(r.read_text())["run_sha256"] for r in records}) == 1  # one run, its own prompts
    assert _unlocked(repo.prod, repo.dev)


@pytest.mark.parametrize("answer", ["n", "", CTRL_D, "yes please"])
def test_anything_but_y_stops_and_the_same_manifest_is_offered_first_next_time(world, answer):
    make, repo, state, stubs = world
    rc, shown = _on_a_pty(make, ["y", answer], *WORLD_ARGS, stubs=stubs)
    assert rc == MAKE_FAILED, shown
    assert "Not released. Manifest" in shown and "stays current: re-run make release ENV=prod" in shown
    assert len(_phase_records(repo)) == 1  # round 1 stays applied
    second = rg.current_manifest(repo.prod, "prod")  # round 2's manifest: still current, unreleased
    assert len(_approvals(repo)) == 1
    assert make.names().count("_release-manifest") == 1
    make.reset()
    rc, shown = _on_a_pty(make, ["y"] * release_loop.MAX_ROUNDS, *WORLD_ARGS, stubs=stubs)
    assert rc == 0, shown
    assert "the current manifest, planned earlier, can still be released" in shown
    assert shown.index(f"Manifest: {second}") < shown.index(PROMPT)
    calls = make.calls()
    first_release = next(i for i, c in enumerate(calls) if "_release-manifest" in c)
    assert f"MANIFEST={second}" in calls[first_release]
    assert not [c for c in calls[:first_release] if "release-plan" in c]  # no new release-plan before it
    assert COMPLETE.format(n=3) in shown
    assert json.loads(state.read_text())["violations"] == []


def test_ctrl_c_at_a_prompt_exits_130_with_no_lock_and_no_record_for_that_round(world):
    make, repo, state, stubs = world
    rc, shown = _on_a_pty(make, ["y", CTRL_C], *WORLD_ARGS, stubs=stubs)
    # The loop exits 130; make, which got the same SIGINT from the terminal, then dies of it (a shell shows 130).
    assert rc in (130, -signal.SIGINT) and "Error 130" in shown and "Interrupted: nothing was approved." in shown
    assert len(_approvals(repo)) == 1 and len(_phase_records(repo)) == 1
    assert make.names().count("_release-manifest") == 1
    assert _unlocked(repo.prod, repo.dev)


def test_a_world_that_never_completes_stops_after_the_round_cap(loop_make, repo):
    assert loop_make("rehearse", "dev").returncode == 0
    loop_make.reset()
    loop_make.exit_code("staged", 10)  # every manifest is one more staged phase
    rc, shown = _on_a_pty(loop_make, ["y"] * (release_loop.MAX_ROUNDS + 2))
    assert rc == MAKE_FAILED, shown
    assert release_loop.MAX_ROUNDS == 8
    assert f"stopped after 8 rounds without the release completing; run make status ENV=prod" in shown
    assert shown.count(PROMPT) == 8 and loop_make.names().count("_release-manifest") == 8
    assert len(_phase_records(repo)) == 8 and not rg.release_receipts(repo.cloud, "prod")


# -- the first round reuses a manifest only while release can still apply it -------------

def test_a_releasable_current_manifest_is_offered_without_planning(loop_make, repo):
    manifest = _planned(loop_make)
    rc, shown = _on_a_pty(loop_make, ["y"])
    assert rc == 0, shown
    assert loop_make.names()[:1] == ["_release-manifest"] and "release-plan" not in loop_make.names()
    assert f"MANIFEST={manifest}" in loop_make.calls()[0]
    assert COMPLETE.format(n=1) in shown
    assert rg.last_release(repo.cloud, "prod")["manifest"]["id"] == json.loads(manifest.read_text())["id"]


def test_a_stale_current_manifest_is_planned_again(loop_make, repo):
    stale = _planned(loop_make)
    repo.commit({"shared/docs/notes.md": "newer docs\n"})  # HEAD moves; the inputs don't
    rc, shown = _on_a_pty(loop_make, ["y"])
    assert rc == 0, shown
    assert "The current manifest can't be released any more (HEAD is" in shown
    names = loop_make.names()
    assert names.index("release-plan") < names.index("_release-manifest")
    receipt = rg.last_release(repo.cloud, "prod")
    assert receipt["manifest"]["id"] != json.loads(stale.read_text())["id"] and receipt["commit"] == repo.head()


# -- refusals stay in the primitives and stop the loop -----------------------------------

def test_no_rehearsal_stops_and_says_so_without_rehearsing_or_asking(loop_make, repo):
    rc, shown = _on_a_pty(loop_make, ["y"])
    assert rc != 0, shown
    assert "no passing rehearsal of" in shown
    assert "make release never rehearses or applies dev: run make rehearse ENV=dev on this commit" in shown
    assert PROMPT not in shown and not _approvals(repo)
    names = loop_make.names()
    assert names == ["release-plan"], names  # never rehearse, a dev apply or any rehearse step
    assert not list(rg.manifests_dir(repo.cloud, "prod").glob("*/manifest.json"))


def test_a_reader_refusal_stops_with_the_ack_hint_and_writes_nothing(loop_make, repo):
    ack = repo.prod / "ack.txt"
    before = ack.read_bytes() if ack.exists() else None
    assert loop_make("rehearse", "dev").returncode == 0
    loop_make.reset()
    loop_make.exit_code("reports", 1)
    rc, shown = _on_a_pty(loop_make, ["y"])
    assert rc != 0, shown
    assert "refused by the reader or weakening check" in shown
    assert "Acknowledgements are a reviewed change, never a prompt: add the keys you accept to envs/prod/ack.txt" \
        in shown
    assert PROMPT not in shown and not _approvals(repo)
    assert (ack.read_bytes() if ack.exists() else None) == before
    assert not list(rg.manifests_dir(repo.cloud, "prod").glob("*/manifest.json"))
    assert "_release-manifest" not in loop_make.names()


def test_a_refused_apply_stops_the_loop_without_planning_again(loop_make, repo):
    manifest = _planned(loop_make)
    loop_make.live.write_text("live-2\n")  # live UC changed since release-plan: _release-manifest refuses
    rc, shown = _on_a_pty(loop_make, ["y", "y"])
    assert rc != 0, shown
    assert "live Unity Catalog changed since release-plan" in shown
    assert "round 1 stopped (above); nothing after it was planned or applied" in shown
    assert "release-plan" not in loop_make.names()
    assert shown.count(PROMPT) == 1 and not rg.release_receipts(repo.cloud, "prod")
    assert rg.current_manifest(repo.prod, "prod") == manifest and _unlocked(repo.prod)


def test_manifest_changed_after_display_is_not_approved_or_applied(loop_make, repo):
    manifest = _planned(loop_make)
    shown_digest = rg.sha256_file(manifest)

    def mutate_then_approve():
        changed = json.loads(manifest.read_text())
        changed["host"] = "changed-after-display"
        manifest.chmod(0o600)
        manifest.write_text(json.dumps(changed))
        return "y"

    rc, shown = _on_a_pty(loop_make, [mutate_then_approve])
    assert rc == MAKE_FAILED and f"SHA256:   {shown_digest}" in shown, shown
    assert "changed after it was shown for approval" in shown
    assert "_release-manifest" not in loop_make.names()
    assert not _approvals(repo) and not _phase_records(repo) and not rg.release_receipts(repo.cloud, "prod")
    assert _unlocked(repo.prod)


# -- the account step before round 1: rehearse's, reused ------------------------------------

ACCOUNT_QUESTION = "Approve and apply this account manifest? [y/N] "
ACCOUNT_APPLIED = "=== Account layer applied; continuing make release ENV=prod ==="
PROBE = ["runner", "account", "account", "plan", "-input=false", "-detailed-exitcode"]
# release_gate.py account-manifest-check: its first call (apply-account's, after the y's record, before the
# lock) mutates the manifest, then the real command runs.
MUTATE_AFTER_APPROVAL = """import os, subprocess, sys
from pathlib import Path
args, marker = sys.argv[1:], Path({marker!r})
if "--field" not in args and not marker.exists():
    marker.touch()
    subprocess.run([sys.executable, {mutate!r}, args[args.index("--manifest") + 1], "after-approval"], check=True)
os.execv(sys.executable, [sys.executable, {gate!r}, "account-manifest-check", *args])
"""


@pytest.fixture
def account_loop(tmp_path, repo):
    """loop_make, with apply-account's _apply-account-locked run for real too; prod has a releasable manifest."""
    make = Make(tmp_path, repo, passthrough=("release-plan", "_release-manifest", "_apply-account-locked"))
    manifest = _planned(make)
    return make, manifest


def _account_dir(repo) -> Path:
    return repo.cloud / "envs" / "account"


def _account_approvals(repo) -> list[Path]:
    directory = approval.approvals_dir(_account_dir(repo), "account")
    return sorted(directory.iterdir()) if directory.is_dir() else []


def _account_manifest(shown: str) -> Path:
    return Path(shown.split("Manifest (immutable):", 1)[1].split()[0])


def _nothing_released(make: Make, repo) -> None:
    names = make.names()
    assert "_apply-account-locked" not in names and "sync-tags" not in names, names
    assert "release-plan" not in names and "_release-manifest" not in names, names
    assert not rg.account_receipts(repo.cloud, "account") and not _account_approvals(repo)
    assert not _approvals(repo) and not _phase_records(repo) and not rg.release_receipts(repo.cloud, "prod")
    assert _unlocked(repo.prod, _account_dir(repo))


def test_account_changes_are_approved_and_applied_through_apply_account_then_the_rounds_run(account_loop, repo):
    make, prod_manifest = account_loop
    rc, shown = _on_a_pty(make, ["y", "y"], account_changes=True)
    assert rc == 0, shown
    assert shown.count(PROMPT) == 2 and shown.count(ACCOUNT_QUESTION) == 1
    manifest = _account_manifest(shown)
    digest = rg.sha256_file(manifest)
    # Shown with its digest, asked, applied under the account lock, then round 1.
    assert shown.index("=== make release: the account plan has changes; they need an approved account manifest "
                       "first ===") < shown.index("=== Account plan ===") \
        < shown.index(f"  Manifest sha256: {digest} (approving approves exactly these bytes)") \
        < shown.index(ACCOUNT_QUESTION) < shown.index("environment lock acquired (account, apply-account)") \
        < shown.index(ACCOUNT_APPLIED) < shown.index("=== Release round 1 (prod)") \
        < shown.index("Approve and release manifest")
    calls, names = make.calls(), make.names()
    assert calls[0] == PROBE
    assert names.index("_account-plan-locked") < names.index("_apply-account-locked") < names.index("sync-tags") \
        < names.index("_release-manifest")
    assert f"MANIFEST={manifest}" in calls[names.index("_apply-account-locked")]
    assert f"MANIFEST={prod_manifest}" in calls[names.index("_release-manifest")]
    [account_receipt] = rg.account_receipts(repo.cloud, "account")
    account_approval = account_receipt["approval"]
    assert account_approval["via"] == "terminal" and account_approval["question"] == ACCOUNT_QUESTION
    assert account_approval["manifest_sha256"] == digest == account_receipt["manifest"]["sha256"]
    [account_record] = _account_approvals(repo)
    assert account_approval["record"] == str(account_record)
    release_approval = rg.last_release(repo.cloud, "prod")["approval"]
    assert release_approval["via"] == "terminal"
    # One run: the account record and the round's record carry this make release's one nonce.
    [release_record] = _approvals(repo)
    assert json.loads(account_record.read_text())["run_sha256"] == json.loads(release_record.read_text())["run_sha256"]
    assert COMPLETE.format(n=1) in shown
    assert _unlocked(repo.prod, _account_dir(repo))


def test_account_step_accepts_setup_genierails_remote_as_committed_local_main(account_loop, repo):
    make, _ = account_loop
    repo.git("remote", "rename", "origin", "genierails")
    repo.git("remote", "set-url", "genierails", rg.UPSTREAM_URL)
    rc, shown = _on_a_pty(make, ["y", "y"], account_changes=True)
    assert rc == 0, shown
    manifest = json.loads(_account_manifest(shown).read_text())
    assert manifest["provenance"]["local_only"] is True
    assert manifest["provenance"]["remote"] == "local main"
    [account_receipt] = rg.account_receipts(repo.cloud, "account")
    assert account_receipt["commit"] == repo.head()
    assert COMPLETE.format(n=1) in shown


@pytest.mark.parametrize("answer", ["n", "", CTRL_D], ids=["n", "blank", "eof"])
def test_anything_but_y_for_the_account_manifest_applies_nothing_and_stops_before_round_1(account_loop, repo,
                                                                                         answer):
    make, _ = account_loop
    rc, shown = _on_a_pty(make, [answer, "y"], account_changes=True)
    assert rc == MAKE_FAILED, shown
    manifest = _account_manifest(shown)
    assert (f"release: not applied. The account manifest {manifest} stays; apply it later with make "
            "apply-account, or re-run make release ENV=prod to be asked again. Nothing was locked or applied in "
            "prod.") in shown
    assert shown.count(PROMPT) == 1 and "Release round 1" not in shown
    assert (manifest.parent / "account.tfplan").exists()  # still there for make apply-account
    _nothing_released(make, repo)


def test_ctrl_c_at_the_account_prompt_approves_and_releases_nothing(account_loop, repo):
    make, _ = account_loop
    rc, shown = _on_a_pty(make, [CTRL_C], account_changes=True)
    assert rc != 0, shown
    assert ACCOUNT_QUESTION in shown and "Interrupted: nothing was approved." in shown
    assert "Release round 1" not in shown
    _nothing_released(make, repo)


def _mutate(manifest: Path, how: str) -> None:
    manifest.chmod(0o600)
    manifest.write_text(json.dumps(json.loads(manifest.read_text()) | {"host": f"mutated-{how}"}, indent=2))
    manifest.chmod(0o400)


def test_an_account_manifest_changed_between_display_and_y_is_never_approved_or_applied(account_loop, repo):
    make, _ = account_loop

    def mutate_then_approve():  # the one account manifest this run planned and showed
        [path] = rg.account_manifest_paths(repo.cloud, "account")
        _mutate(path, "at-the-prompt")
        return "y"

    rc, shown = _on_a_pty(make, [mutate_then_approve, "y"], account_changes=True)
    assert rc == MAKE_FAILED, shown
    assert shown.index(ACCOUNT_QUESTION) < shown.index("changed after it was shown")
    assert "release: the approval prompt refused (above); nothing was approved or applied." in shown
    assert json.loads(_account_manifest(shown).read_text())["host"] == "mutated-at-the-prompt"
    _nothing_released(make, repo)


@pytest.mark.parametrize("when", ["before the lock", "under the lock"])
def test_an_account_manifest_changed_between_approval_and_apply_is_refused_and_nothing_is_released(
        account_loop, repo, when):
    from tests.test_approval_make import CHECK_THEN_MUTATE, _mutator

    make, _ = account_loop
    stub = make.tmp / "account-check-stub.py"
    if when == "before the lock":
        stub.write_text(MUTATE_AFTER_APPROVAL.format(marker=str(make.tmp / "mutated"), mutate=str(_mutator(make)),
                                                     gate=rg.__file__))
    else:
        stub.write_text(CHECK_THEN_MUTATE.format(mutate=str(_mutator(make)), gate=rg.__file__))
    rc, shown = _on_a_pty(make, ["y", "y"], account_changes=True,
                          stubs={"release_gate.py account-manifest-check": script_stubs.python_file(stub)})
    assert rc == MAKE_FAILED, shown
    manifest = _account_manifest(shown)
    assert shown.count(PROMPT) == 1
    lock = shown.index("environment lock acquired (account, apply-account)")
    if when == "before the lock":
        assert json.loads(manifest.read_text())["host"] == "mutated-after-approval"
        assert shown.index("manifest_sha256 differ") > lock
        assert "apply-account: the approval record doesn't approve this manifest (above); nothing was applied." \
            in shown
    else:
        assert json.loads(manifest.read_text())["host"] == "mutated-under-the-lock"
        assert shown.index("changed after it was shown") > lock
        assert "apply-account: the manifest is not the one approved (above); nothing more was applied." in shown
    assert ("release: the approved account manifest was not applied (above); nothing was locked or applied in "
            "prod.") in shown
    assert not [c for c in make.calls() if c[:1] == ["runner"] and "apply-plan" in c]
    assert "sync-tags" not in make.names() and "release-plan" not in make.names()
    assert "_release-manifest" not in make.names()
    assert not rg.account_receipts(repo.cloud, "account") and not rg.release_receipts(repo.cloud, "prod")
    assert not _approvals(repo) and _unlocked(repo.prod, _account_dir(repo))
    assert (manifest.parent / "account.tfplan").exists()


def test_an_empty_account_plan_asks_nothing_about_the_account_and_releases(account_loop, repo, tmp_path):
    from tests.test_rehearse_account_approval import _runner

    make, prod_manifest = account_loop
    rc, shown = _on_a_pty(make, ["y"], root_runner=_runner(tmp_path, make, 0))
    assert rc == 0, shown
    assert shown.count(PROMPT) == 1 and ACCOUNT_QUESTION not in shown and "account manifest" not in shown
    assert make.calls()[0] == ["runner", "account", "account", "plan"]  # the probe ran
    assert "_account-plan-locked" not in make.names() and "_apply-account-locked" not in make.names()
    assert not rg.account_receipts(repo.cloud, "account") and not _account_approvals(repo)
    assert f"MANIFEST={prod_manifest}" in make.calls()[make.names().index("_release-manifest")]
    assert COMPLETE.format(n=1) in shown


def test_a_failing_account_plan_stops_before_round_1(account_loop, repo, tmp_path):
    from tests.test_rehearse_account_approval import _runner

    make, _ = account_loop
    rc, shown = _on_a_pty(make, ["y"], root_runner=_runner(tmp_path, make, 1))
    assert rc == MAKE_FAILED, shown
    assert "release: the account plan failed (above); nothing was locked, approved or applied." in shown
    assert make.calls() == [["runner", "account", "account", "plan"]]
    _nothing_released(make, repo)


@pytest.mark.parametrize("environ_extra", [{"GENIERAILS_INTERACTIVE": "0"}, {"GENIERAILS_ALLOW_CI_APPLY": "1"},
                                          {"MAKELEVEL": "1"}])
def test_off_the_interactive_path_a_non_empty_account_plan_is_never_probed_or_asked(make, repo, environ_extra):
    rc, shown = _on_a_pty(make, ["y"], account_changes=True, environ_extra=environ_extra)
    assert PROMPT not in shown and "account manifest" not in shown, shown
    assert PROBE not in make.calls() and _primitive_only(make, repo), make.calls()
    assert not rg.account_receipts(repo.cloud, "account") and not _account_approvals(repo)


def test_on_a_terminal_manifest_never_runs_the_account_step(make, repo):
    rc, shown = _on_a_pty(make, ["y"], "MANIFEST=/m/manifest.json", account_changes=True)
    assert rc == 0 and PROMPT not in shown, shown
    assert make.calls() == [PRIMITIVE + [f"ENV_DIR={repo.prod}", "MANIFEST=/m/manifest.json", "APPROVAL="]]


def test_release_runs_rehearses_account_step_once_on_the_interactive_path_only():
    """The same macro as rehearse (no second copy), with the loop's nonce, right before release_loop.py."""
    text = (Path(release_loop.__file__).parents[1] / "Makefile.shared").read_text()
    recipe = text.split("\nrelease: ", 1)[1].split("\n\n", 1)[0]
    interactive = recipe.split('"0:genierails-approval:v1:interactive" ]; then', 1)[1].split("\t\t\tfi; ", 1)[0]
    order = ['GENIERAILS_APPROVAL_RUN="$$(od -An -N16 -tx1 /dev/urandom', "$(call _ACCOUNT_APPROVE_APPLY,release,run)",
             "release_loop.py", "--run-from-env"]
    assert [interactive.index(step) for step in order] == sorted(interactive.index(step) for step in order)
    assert recipe.count("_ACCOUNT_APPROVE_APPLY") == 1 and text.count("override _ACCOUNT_APPROVE_APPLY") == 1
    assert text.count("$(call _ACCOUNT_APPROVE_APPLY,") == 2  # rehearse and release


# -- everything else releases once, exactly as before ------------------------------------

PRIMITIVE = ["--no-print-directory", "_release-manifest", "ENV=prod"]


def _primitive_only(make: Make, repo, manifest: str = "") -> bool:
    return make.calls() == [PRIMITIVE + [f"ENV_DIR={repo.prod}", f"MANIFEST={manifest}", "APPROVAL="]]


@pytest.mark.parametrize("environ_extra", [{}, {"YES": "1"}, {"APPROVE": "1"}, {"GENIERAILS_INTERACTIVE": "1"},
                                           {"GENIERAILS_APPROVE": "1"}])
def test_off_a_terminal_plain_release_is_the_primitive(make, repo, environ_extra):
    result = make("release", "prod", environ_extra=environ_extra)  # stdout captured, stdin not a terminal
    assert result.returncode == 0, result.stderr
    assert _primitive_only(make, repo) and PROMPT not in result.stdout


def test_piped_answers_or_captured_output_are_the_primitive(make, repo):
    args, environ = make.command("release", "prod")
    piped = subprocess.run(args, input="y\ny\n", text=True, capture_output=True, env=environ, cwd=ROOT / "aws")
    assert piped.returncode == 0 and _primitive_only(make, repo) and PROMPT not in piped.stdout
    make.reset()
    rc, shown = pty_run(["/bin/sh", "-c", 'exec "$@" | cat', "sh", *args], "y", environ, cwd=ROOT / "aws")
    assert rc == 0 and _primitive_only(make, repo) and PROMPT not in shown


@pytest.mark.parametrize("environ_extra", [{"GENIERAILS_INTERACTIVE": "0"},
                                          {"CI": "true", "GENIERAILS_ALLOW_CI_APPLY": "1"},
                                          {"GENIERAILS_ALLOW_CI_APPLY": "1"},
                                          {"MAKELEVEL": "1"}])  # a nested make release (run_integration_tests)
def test_on_a_terminal_an_opt_out_ci_or_a_nested_make_is_the_primitive(make, repo, environ_extra):
    rc, shown = _on_a_pty(make, ["y"], environ_extra=environ_extra)
    assert PROMPT not in shown, shown
    assert _primitive_only(make, repo), make.calls()


def test_on_a_terminal_manifest_names_one_release(make, repo):
    rc, shown = _on_a_pty(make, ["y"], "MANIFEST=/m/manifest.json")
    assert rc == 0 and PROMPT not in shown, shown
    assert make.calls() == [PRIMITIVE + [f"ENV_DIR={repo.prod}", "MANIFEST=/m/manifest.json", "APPROVAL="]]


def test_on_a_terminal_empty_command_line_manifest_names_one_release(make, repo):
    rc, shown = _on_a_pty(make, ["y"], "MANIFEST=")
    assert rc == 0 and PROMPT not in shown, shown
    assert _primitive_only(make, repo)


def test_on_a_terminal_exported_manifest_names_one_release(make, repo):
    rc, shown = _on_a_pty(make, ["y"], environ_extra={"MANIFEST": ""})
    assert rc == 0 and PROMPT not in shown, shown
    assert _primitive_only(make, repo)


def test_on_a_terminal_ci_still_refuses(make, repo):
    rc, shown = _on_a_pty(make, ["y"], environ_extra={"CI": "true"})
    assert rc != 0 and "applying targets cannot run in CI" in shown and PROMPT not in shown
    assert make.calls() == []


def test_on_a_terminal_a_legacy_env_keeps_its_release(make, repo):
    (repo.prod / "env.auto.tfvars").write_text(LEGACY_ENV)
    rc, shown = _on_a_pty(make, ["y"])
    assert rc == 0 and PROMPT not in shown, shown
    assert "_release-manifest" not in make.names() and "release-plan" not in make.names()


def test_no_setting_changes_the_round_cap_or_answers_for_you():
    text = Path(release_loop.__file__).read_text()
    assert "MAX_ROUNDS = 8" in text and "environ.get" not in text and "getenv" not in text
    parser_options = {"--env", "--env-dir", "--make", "--verify-key-column", "--run-from-env"}
    assert set(re.findall(r'add_argument\("(--[a-z-]+)"', text)) == parser_options


# -- approval records count only from this run's own prompt -------------------------------

NESTED = {"MAKELEVEL": "1"}


def _record_of_run(repo, manifest: Path, run: str = RUN) -> Path:
    # Written in-process, as if by an asking make recipe (its asker: THIS_RUN); a make run here
    # isn't under that recipe, so the record never counts there (#288's asker binding).
    with mock.patch.object(approval, "_chain", lambda pid: list(THIS_RUN)):
        return approval.record(repo.prod, "prod", manifest, QUESTION, run=run, environ=NESTED, stdin=Tty(),
                               stdout=Tty())


@pytest.mark.parametrize("how", ["forged MAKELEVEL", "MAKEFLAGS"])
def test_a_forged_nesting_without_the_runs_nonce_is_ignored(tmp_path, repo, monkeypatch, how):
    monkeypatch.delenv(approval.RUN_ENV, raising=False)
    make = Make(tmp_path, repo)
    manifest = _planned(make)
    record = _record_of_run(repo, manifest)  # a real record, but of an earlier run
    if how == "forged MAKELEVEL":
        result = make("_release-manifest", "prod", f"MANIFEST={manifest}", f"APPROVAL={record}", environ_extra=NESTED)
    else:
        result = make("_release-manifest", "prod", f"MANIFEST={manifest}",
                      environ_extra=NESTED | {"MAKEFLAGS": f"APPROVAL={record}"})
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Approval record" not in result.stdout
    assert rg.last_release(repo.cloud, "prod")["approval"] == {"via": "external"}


@pytest.mark.parametrize("how", ["another run", "mode 644", "symlink", "another owner"])
def test_a_record_not_written_by_this_runs_prompt_refuses_before_applying(tmp_path, repo, monkeypatch, how):
    make = Make(tmp_path, repo)
    manifest = _planned(make)
    run = approval.new_run()
    monkeypatch.setenv(approval.RUN_ENV, run)
    record = _record_of_run(repo, manifest, run)
    if how == "another run":
        record = _record_of_run(repo, manifest, approval.new_run())
    elif how == "mode 644":
        record.chmod(0o644)
    elif how == "symlink":
        link = record.with_name("link.json")
        link.symlink_to(record)
        record = link
    else:
        monkeypatch.setattr(os, "getuid", lambda: 12345)
        with pytest.raises(approval.Refused, match="owned by uid|not owned by this user"):
            approval.check(repo.prod, "prod", manifest, record)
        return
    make.reset()
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", f"APPROVAL={record}", environ_extra=NESTED)
    assert result.returncode != 0
    assert "release: the approval record doesn't approve this manifest (above); nothing was applied." in result.stderr
    assert not [c for c in make.calls() if c[0] in ("staged", "runner")]
    assert not rg.release_receipts(repo.cloud, "prod") and _unlocked(repo.prod)


def test_the_approve_command_refuses_without_a_run_before_asking(repo, tmp_path, monkeypatch):
    make = Make(tmp_path, repo)
    manifest = _planned(make)
    answer = tmp_path / "answer"
    answer.touch()
    environ = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", approval.RUN_ENV)}
    rc, shown = pty_run(["python3", approval.__file__, "approve", "--env-dir", str(repo.prod), "--env", "prod",
                         "--manifest", str(manifest), "--question", QUESTION, "--expect-digest",
                         rg.sha256_file(manifest), "--answer-file", str(answer)], "y",
                        environ | repo.env | NESTED, timeout=120)
    assert (rc, answer.read_text()) == (1, approval.REFUSED + "\n"), shown
    assert PROMPT not in shown and "no approval run" in shown and not _approvals(repo)
