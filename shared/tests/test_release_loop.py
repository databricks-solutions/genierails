"""make release ENV=<deterministic env> at a terminal: the round-by-round loop (scripts/release_loop.py).

Real make runs the real release, release-plan, _release-plan-roots and
_release-manifest recipes on a pseudo-terminal (tests/pty_harness.py), with
the test as the person answering each round's y/N. The multi-round cases use
test_first_release_rounds' fake Unity Catalog ("the world"); the rest use
test_release_gate_make's stubs. Off a terminal, in CI, with MANIFEST= or
GENIERAILS_INTERACTIVE=0, release runs _release-manifest once, as before.
"""

from __future__ import annotations

import json
import os
import re
import signal
import stat
import subprocess
from pathlib import Path

import pytest

from scripts import approval
from scripts import environment_lock
from scripts import release_gate as rg
from scripts import release_loop
from tests.pty_harness import CTRL_C, CTRL_D, pty_run
from tests.test_approval import QUESTION, RUN, Tty
from tests.test_first_release_rounds import build_world
from tests.test_release_gate_make import LEGACY_ENV, ROOT, Make, _planned, make, repo  # noqa: F401 (fixtures)

PROMPT = "[y/N] "
COMPLETE = "=== Release complete (prod) after {n} round(s), each approved at this terminal ==="
WORLD_ARGS = ("IMPORT_EXISTING_SCRIPT=true",)
MAKE_FAILED = 2  # make's exit code when the release recipe (the loop) fails


def _on_a_pty(make: Make, answers, *extra, environ_extra=None, stubs=None, timeout=900):
    args, environ = make.command("release", "prod", *extra, stubs=stubs, environ_extra=environ_extra)
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


# -- everything else releases once, exactly as before ------------------------------------

PRIMITIVE = ["--no-print-directory", "_release-manifest", "ENV=prod"]


def _primitive_only(make: Make, repo) -> bool:
    return make.calls() == [PRIMITIVE + [f"ENV_DIR={repo.prod}", "MANIFEST=", "APPROVAL="]]


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
    parser_options = {"--env", "--env-dir", "--make", "--verify-key-column"}
    assert set(re.findall(r'add_argument\("(--[a-z-]+)"', text)) == parser_options


# -- approval records count only from this run's own prompt -------------------------------

NESTED = {"MAKELEVEL": "1"}


def _record_of_run(repo, manifest: Path, run: str = RUN) -> Path:
    return approval.record(repo.prod, "prod", manifest, QUESTION, run=run, environ=NESTED, stdin=Tty(), stdout=Tty())


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
        with pytest.raises(approval.Refused, match="owned by uid"):
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
                         "--manifest", str(manifest), "--question", QUESTION, "--answer-file", str(answer)], "y",
                        environ | repo.env | NESTED, timeout=120)
    assert (rc, answer.read_text()) == (1, approval.REFUSED + "\n"), shown
    assert PROMPT not in shown and "no approval run" in shown and not _approvals(repo)
