"""Simplification step 6b: on a terminal, a deterministic make rehearse asks before applying a non-empty account plan.

On a terminal (scripts/approval.py interactive), when the account plan has
changes, rehearse shows the account manifest, asks 'Approve and apply this
account manifest? [y/N]' and on y applies exactly that manifest through
_apply-account-locked with the y's record, before it locks dev. Every
account-plan and apply-account refusal stays in those recipes. CI, no
terminal and GENIERAILS_INTERACTIVE=0 keep today's behaviour: no probe, no
prompt, the same calls (apply's _guard-account-plan-empty refuses a non-empty
account plan). Real make runs the real recipes against the
test_release_gate_make harness on a pseudo-terminal (tests/pty_harness.py).
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from unittest import mock

import pytest

from scripts import approval
from scripts import environment_lock
from scripts import release_gate as rg
from tests.pty_harness import CTRL_C, CTRL_D, pty_run
from tests.test_release_gate_make import Make, _account_planned, repo  # noqa: F401 (fixtures)

QUESTION = "Approve and apply this account manifest? [y/N] "
PROBE = ["runner", "account", "account", "plan", "-input=false", "-detailed-exitcode"]
NOT_APPLIED = "rehearse: not applied. The account manifest "
PLAN_RUNNER = """#!/bin/sh
printf '%s\\n' "[\\"runner\\", \\"$1\\", \\"$2\\", \\"$3\\"]" >> {log}
exit {rc}
"""


def pty_make(make: Make, target: str, env: str, *extra: str, answer, environ_extra=None, before_answer=None,
             **kwargs):
    """The harness's make command (same arguments, stubs and environment) on a pty instead of pipes."""
    seen = {}

    def run(args, **options):
        seen.update(args=args, env=options["env"], cwd=options["cwd"])
        return subprocess.CompletedProcess(args, 0, "", "")

    with mock.patch.object(subprocess, "run", run):
        make(target, env, *extra, **kwargs)
    environ = {k: v for k, v in seen["env"].items() if k not in (approval.INTERACTIVE_ENV, approval.RUN_ENV)}
    return pty_run(seen["args"], answer, environ | (environ_extra or {}), cwd=seen["cwd"],
                   before_answer=before_answer)


@pytest.fixture
def amake(tmp_path, repo):
    return Make(tmp_path, repo, passthrough=("_apply-account-locked",))


def _account_dir(repo) -> Path:
    return repo.cloud / "envs" / "account"


def _unlocked(env_dir: Path) -> bool:
    return not environment_lock.lock_path(env_dir).exists()


def _manifest(shown: str) -> Path:
    return Path(shown.split("Manifest (immutable):", 1)[1].split()[0])


def _approvals(repo) -> list[Path]:
    directory = approval.approvals_dir(_account_dir(repo), "account")
    return sorted(directory.iterdir()) if directory.is_dir() else []


def _nothing_in_dev(make: Make, repo) -> None:
    names = make.names()
    assert "_derive-before-exposure" not in names and "apply" not in names, names
    assert not rg.rehearse_receipts(repo.cloud, "dev")
    assert not rg.env_state(repo.cloud, "dev").joinpath("rehearse.pending.json").exists()
    assert _unlocked(repo.dev) and _unlocked(_account_dir(repo))


# -- y ---------------------------------------------------------------------------------

def test_y_applies_exactly_the_manifest_shown_then_rehearses_dev(amake, repo):
    rc, shown = pty_make(amake, "rehearse", "dev", answer="y")
    assert rc == 0, shown
    assert shown.count("[y/N]") == 1 and QUESTION in shown
    calls, names = amake.calls(), amake.names()
    assert calls[0] == PROBE  # after make check, before any lock
    manifest = _manifest(shown)
    nested = calls[names.index("_apply-account-locked")]
    assert f"MANIFEST={manifest}" in nested
    [record] = [Path(a.split("=", 1)[1]) for a in nested if a.startswith("APPROVAL=")]
    assert names.index("_account-plan-locked") < names.index("_apply-account-locked") < names.index("sync-tags") \
        < names.index("_derive-before-exposure")
    [receipt] = rg.account_receipts(repo.cloud, "account")
    approved = receipt["approval"]
    assert approved["via"] == "terminal" and approved["record"] == str(record) == str(_approvals(repo)[0])
    assert approved["manifest_sha256"] == rg.sha256_file(manifest) == receipt["manifest"]["sha256"]
    assert approved["question"] == QUESTION and approved["tty"].startswith("/dev/")
    # The account lock is taken and released before rehearse locks dev.
    assert shown.index("environment lock acquired (account, apply-account)") \
        < shown.index("=== Account layer applied; continuing make rehearse ENV=dev ===") \
        < shown.index("environment lock acquired (dev, rehearse)")
    [rehearsal] = rg.rehearse_receipts(repo.cloud, "dev")
    assert rehearsal["result"] == "passed"
    assert _unlocked(repo.dev) and _unlocked(_account_dir(repo))


def test_an_unapplied_manifest_already_planned_for_head_is_offered_without_planning_again(amake, repo):
    manifest = _account_planned(amake)
    rc, shown = pty_make(amake, "rehearse", "dev", answer="y")
    assert rc == 0, shown
    assert "=== Account plan (already planned for HEAD, not applied yet) ===" in shown
    assert f"Manifest: {manifest}" in shown.split(QUESTION)[0]
    assert "_account-plan-locked" not in amake.names()
    nested = amake.calls()[amake.names().index("_apply-account-locked")]
    assert f"MANIFEST={manifest}" in nested
    [receipt] = rg.account_receipts(repo.cloud, "account")
    assert receipt["manifest"]["path"] == str(manifest) and receipt["approval"]["via"] == "terminal"


def test_a_manifest_of_an_older_commit_is_not_offered(amake, repo):
    old = _account_planned(amake)
    repo.commit({"shared/docs/notes.md": "later\n"})
    rc, shown = pty_make(amake, "rehearse", "dev", answer="y")
    assert rc == 0, shown
    assert "_account-plan-locked" in amake.names() and _manifest(shown) != old
    [receipt] = rg.account_receipts(repo.cloud, "account")
    assert receipt["commit"] == repo.head()


# -- no ----------------------------------------------------------------------------------

@pytest.mark.parametrize("answer", ["n", "", CTRL_D, "Y es", "no"], ids=["n", "blank", "eof", "y-es", "no"])
def test_anything_but_y_applies_nothing_and_never_starts_the_dev_rehearsal(amake, repo, answer):
    rc, shown = pty_make(amake, "rehearse", "dev", answer=answer)
    assert rc != 0, shown
    manifest = _manifest(shown)
    assert (f"{NOT_APPLIED}{manifest} stays; apply it later with make apply-account, or re-run make rehearse "
            "ENV=dev to be asked again. Nothing was locked or applied in dev.") in shown
    assert "_apply-account-locked" not in amake.names() and "sync-tags" not in amake.names()
    assert not rg.account_receipts(repo.cloud, "account") and not _approvals(repo)
    assert (manifest.parent / "account.tfplan").exists()  # still there for make apply-account
    _nothing_in_dev(amake, repo)


def test_ctrl_c_at_the_prompt_approves_and_locks_nothing(amake, repo):
    rc, shown = pty_make(amake, "rehearse", "dev", answer=CTRL_C)
    assert rc != 0, shown
    assert not _approvals(repo) and not rg.account_receipts(repo.cloud, "account")
    _nothing_in_dev(amake, repo)


# -- refusals stay in the primitives ----------------------------------------------------

def test_head_not_exactly_origin_main_stops_at_account_plans_provenance_before_any_prompt(amake, repo):
    repo.commit({"shared/docs/notes.md": "local only\n"}, push=False)
    rc, shown = pty_make(amake, "rehearse", "dev", answer="y")
    assert rc != 0, shown
    assert "[y/N]" not in shown
    assert "origin/main" in shown
    assert ("rehearse: the account layer needs an approved account manifest, but make account-plan stopped "
            "(above). Nothing was locked, approved or applied in dev.") in shown
    assert not list(rg.manifests_dir(repo.cloud, "account").glob("*/manifest.json"))
    _nothing_in_dev(amake, repo)


def _mutate(manifest: Path, field: str = "host") -> None:
    manifest.chmod(0o600)
    manifest.write_text(json.dumps(json.loads(manifest.read_text()) | {field: "mutated-at-the-prompt"}, indent=2))
    manifest.chmod(0o400)


def test_a_manifest_changed_after_it_was_shown_is_never_approved_or_applied(amake, repo):
    """Codex's repro on #288 @f43e909: mutate the manifest after display, before y; it was applied."""
    def mutate():  # the one manifest this run planned and showed
        [path] = rg.account_manifest_paths(repo.cloud, "account")
        _mutate(path)

    rc, shown = pty_make(amake, "rehearse", "dev", answer="y", before_answer=mutate)
    assert rc != 0, shown
    manifest = _manifest(shown)
    digest_line = next(line for line in shown.splitlines() if line.startswith("  Manifest sha256: "))
    assert shown.index(digest_line) < shown.index(QUESTION) < shown.index("changed after it was shown")
    assert "rehearse: the approval prompt refused (above); nothing was approved or applied." in shown
    assert json.loads(manifest.read_text())["host"] == "mutated-at-the-prompt"
    assert "_apply-account-locked" not in amake.names() and "sync-tags" not in amake.names()
    assert not rg.account_receipts(repo.cloud, "account") and not _approvals(repo)
    _nothing_in_dev(amake, repo)


def test_the_digest_shown_is_the_one_approved_and_applied(amake, repo):
    rc, shown = pty_make(amake, "rehearse", "dev", answer="y")
    assert rc == 0, shown
    manifest = _manifest(shown)
    digest = rg.sha256_file(manifest)
    assert (f"  Manifest sha256: {digest} (approving approves exactly these bytes)"
            in shown.split(QUESTION)[0])
    [receipt] = rg.account_receipts(repo.cloud, "account")
    assert receipt["approval"]["manifest_sha256"] == digest == receipt["manifest"]["sha256"]


def test_a_manifest_superseded_while_the_prompt_is_up_is_refused_by_apply_account(amake, repo, tmp_path):
    (tmp_path / "other").mkdir()
    superseding = Make(tmp_path / "other", repo)

    def plan_again():
        time.sleep(1.1)  # manifest ids are per second
        result = superseding("account-plan", "account")
        assert result.returncode == 0, result.stdout + result.stderr

    rc, shown = pty_make(amake, "rehearse", "dev", answer="y", before_answer=plan_again)
    assert rc != 0, shown
    assert "superseded by a newer account-plan" in shown
    assert "rehearse: the approved account manifest was not applied (above)" in shown
    assert "sync-tags" not in amake.names()
    assert not rg.account_receipts(repo.cloud, "account")
    _nothing_in_dev(amake, repo)


def _runner(tmp_path: Path, make: Make, rc: int) -> Path:
    runner = tmp_path / f"plan-runner-{rc}"
    runner.write_text(PLAN_RUNNER.format(log=make.log, rc=rc))
    runner.chmod(0o755)
    return runner


def test_an_empty_account_plan_asks_nothing_and_rehearses(amake, repo, tmp_path):
    rc, shown = pty_make(amake, "rehearse", "dev", answer="y", root_runner=_runner(tmp_path, amake, 0))
    assert rc == 0, shown
    assert "[y/N]" not in shown and "account manifest" not in shown
    assert amake.calls()[0] == ["runner", "account", "account", "plan"]  # the probe ran
    assert "_account-plan-locked" not in amake.names() and "_apply-account-locked" not in amake.names()
    assert [r["result"] for r in rg.rehearse_receipts(repo.cloud, "dev")] == ["passed"]


def test_a_failing_account_plan_stops_before_any_lock(amake, repo, tmp_path):
    rc, shown = pty_make(amake, "rehearse", "dev", answer="y", root_runner=_runner(tmp_path, amake, 1))
    assert rc != 0, shown
    assert "rehearse: the account plan failed (above); nothing was locked, approved or applied." in shown
    assert amake.calls() == [["runner", "account", "account", "plan"]]
    _nothing_in_dev(amake, repo)


# -- off a terminal: exactly today's behaviour -------------------------------------------

# make rehearse ENV=dev's calls below make check in this harness, as on main.
TODAYS_REHEARSE = ["_derive-before-exposure", "validate-generated", "coverage-gate", "apply", "_verify-access-gate"]


@pytest.mark.parametrize("how", ["captured", "no-tty-interactive-1", "pty-opt-out", "pty-allow-ci-apply"])
def test_off_a_terminal_rehearse_never_probes_or_asks(amake, repo, how):
    """A non-terminal, GENIERAILS_INTERACTIVE=0 and the CI opt-out: no probe, no prompt, today's calls (regression)."""
    if how == "captured":
        result = amake("rehearse", "dev")
        rc, shown = result.returncode, result.stdout + result.stderr
    elif how == "no-tty-interactive-1":  # no value switches prompting on
        result = amake("rehearse", "dev", environ_extra={"GENIERAILS_INTERACTIVE": "1"})
        rc, shown = result.returncode, result.stdout + result.stderr
    else:
        extra = {"pty-opt-out": {"GENIERAILS_INTERACTIVE": "0"},
                 "pty-allow-ci-apply": {"GENIERAILS_ALLOW_CI_APPLY": "1"}}[how]
        rc, shown = pty_make(amake, "rehearse", "dev", answer="y", environ_extra=extra)
    assert rc == 0, shown
    assert "[y/N]" not in shown and "account manifest" not in shown
    assert amake.names() == TODAYS_REHEARSE
    assert not rg.account_receipts(repo.cloud, "account") and not _approvals(repo)


def test_ci_on_a_terminal_refuses_rehearse_as_before(amake, repo):
    rc, shown = pty_make(amake, "rehearse", "dev", answer="y", environ_extra={"CI": "true"})
    assert rc != 0 and "[y/N]" not in shown
    assert amake.calls() == [] and not _approvals(repo)


def test_ci_on_a_terminal_still_refuses_a_non_empty_account_plan_with_todays_message(amake, repo):
    """The CI dev pipeline's account step (apply's _guard-account-plan-empty): the same refusal, no prompt."""
    (_account_dir(repo) / "remote_state.hcl").write_text('backend = "s3"\n')
    rc, shown = pty_make(amake, "_guard-account-plan-empty", "dev", answer="y", environ_extra={"CI": "true"})
    assert rc != 0 and "[y/N]" not in shown
    assert "ERROR: account plan is non-empty; rehearse/release may only add allowed treatment-tag values." in shown
    assert "Run 'make account-plan', get its manifest approved, then 'make apply-account'" in shown
    assert not _approvals(repo)


def test_a_legacy_env_never_probes_or_asks(amake, repo):
    (repo.dev / "env.auto.tfvars").write_text('genie_space_id = "x"\n')
    rc, shown = pty_make(amake, "rehearse", "dev", answer="y")
    assert rc == 0, shown
    assert "[y/N]" not in shown
    assert PROBE not in amake.calls() and "_account-plan-locked" not in amake.names()


# -- where it runs, and what it accepts ------------------------------------------------------

MAKEFILE = Path(approval.__file__).parents[1] / "Makefile.shared"


def _block(text: str, start: str) -> str:
    return text.split(start, 1)[1].split("\n\n", 1)[0]


def test_rehearse_runs_it_after_make_check_and_before_the_custom_mask_cases_and_the_dev_lock():
    recipe = _block(MAKEFILE.read_text(), "\nrehearse: ")
    order = ["clean-check", "$(call _RUN_CHECK,rehearse)", "$(call _ACCOUNT_APPROVE_APPLY,rehearse)",
             "$(_CUSTOM_MASK_CASES)", "$(call _ENV_LOCK,rehearse)", "rehearse-start"]
    assert [recipe.index(step) for step in order] == sorted(recipe.index(step) for step in order)
    assert recipe.count("_ACCOUNT_APPROVE_APPLY") == 1  # the deterministic branch only


def test_it_is_a_macro_in_the_top_level_recipe_not_a_target():
    """approval.py asks only at MAKELEVEL 1; and no new target means nothing new for the -i/-k or compute guards."""
    text = MAKEFILE.read_text()
    assert not [line for line in text.splitlines() if line.startswith(("_account-approve", "_account-plan-probe"))]
    assert "_ACCOUNT_APPROVE_APPLY" not in _block(text, "override _MAKE_IGNORE_ERRORS_ALLOWED_GOALS := ")


def test_it_accepts_only_the_exact_answers_and_fails_closed_on_anything_else():
    import re

    block = _block(MAKEFILE.read_text(), "override _ACCOUNT_APPROVE_APPLY = ")
    pairs = {(int(rc), line) for rc, line in re.findall(r'"(\d+):(genierails-approval:v\d+:[a-z-]+)', block)}
    answers = {(0, approval.INTERACTIVE), (0, approval.NOT_INTERACTIVE), (0, approval.DIGEST), (0, approval.APPROVED),
               (1, approval.NO), (1, approval.REFUSED)}
    assert pairs == answers and answers <= (approval.ANSWERS["interactive"] | approval.ANSWERS["show"]
                                            | approval.ANSWERS["approve"])
    assert block.count('\t\t*) echo "ERROR: ') == 3 and block.count("exit 1 ;;") >= 4  # any other answer
    # The record path comes only from approve's own answer; the nonce only from /dev/urandom, never a variable.
    assert 'APPROVAL="$$record"' in block and '--expect-digest "$$digest"' in block and "GENIERAILS_APPROVAL_RUN=\"$$(od -An -N16 -tx1 /dev/urandom" in block
    assert not re.search(r"\$\((APPROVE|YES|ASSUME_YES|AUTO_APPROVE|GENIERAILS_APPROVE)\)", block)
