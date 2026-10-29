"""make setup / generate / promote-to's commit offer (scripts/commit_envs.py), on real git repos.

Each test builds a GenieRails-shaped checkout (shared/, aws/ with the customer
.gitignore block) in a temp dir and drives the offer through a pseudo-terminal
for the y/N answer. Git runs with GIT_CONFIG_GLOBAL=/dev/null here only, so the
temp repos' commits skip the developer's global hooks; the real commits the
offer makes elsewhere keep them.
"""

import os
import pty
import re
import select
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

from scripts import commit_envs, coverage_fix, customer_gitignore
from tests.test_deterministic_check_promote import (  # noqa: F401 (repo is a fixture)
    DEPLOYER, LEGACY_ACCOUNT, _auth, _envs, _prod_env, repo,
)

ROOT = Path(__file__).resolve().parents[2]
SHARED = ROOT / "shared"
SCRIPT = SHARED / "scripts" / "commit_envs.py"

if shutil.which("git") is None:
    pytest.skip("git is required", allow_module_level=True)

GIT_IDENTITY = {"GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1", "GIT_AUTHOR_NAME": "t",
                "GIT_AUTHOR_EMAIL": "t@example.com", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com"}
DROP = ("MAKEFLAGS", "MAKELEVEL", "MFLAGS", "COMMIT", "GENIERAILS_COMMIT", "CI", "TF_BUILD", "JENKINS_URL",
        "GITLAB_CI", "BUILDKITE", "CIRCLECI", "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE")
PROMPT = "[y/N] "


@pytest.fixture(autouse=True)
def _isolated_git(monkeypatch):
    for name in DROP:
        monkeypatch.delenv(name, raising=False)
    for name, value in GIT_IDENTITY.items():
        monkeypatch.setenv(name, value)


def _git(root, *args, check=True):
    return subprocess.run(["git", "-C", str(root), *args], text=True, capture_output=True, check=check).stdout


@pytest.fixture
def checkout(tmp_path):
    """A customer checkout on main: committed dev env, then a generate's uncommitted output."""
    root = tmp_path / "checkout"
    cloud = root / "aws"
    dev = cloud / "envs" / "dev"
    (dev / "generated").mkdir(parents=True)
    (root / "shared").symlink_to(SHARED)
    shutil.copy2(ROOT / ".gitignore", root / ".gitignore")  # GenieRails' own: ignores envs/
    (dev / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    (dev / "auth.auto.tfvars").write_text('databricks_client_secret = "dose-real-secret"\n')
    (cloud / ".gitignore").write_text(customer_gitignore.block(cloud))
    _git(root, "init", "-q", "-b", "main")
    _git(root, "add", ".gitignore", "aws")
    _git(root, "commit", "-q", "-m", "initial")
    # What make generate leaves: new generated files, a changed env file, and runtime files.
    (dev / "generated" / "abac.auto.tfvars").write_text("groups = {}\n")
    (dev / "generated" / "masking_functions.sql").write_text("-- masks\n")
    (dev / "env.auto.tfvars").write_text('governance_mode = "deterministic"\ntable_readers = {}\n')
    (dev / ".genie_space_id_Payments").write_text("01ef\n")
    (dev / "terraform.tfstate").write_text("{}\n")
    return root


def _cloud(root):
    return root / "aws"


def _args(root, env="dev", step="generate"):
    cloud = _cloud(root)
    return [sys.executable, str(SCRIPT), "offer", "--cloud-dir", str(cloud), "--env", env,
            "--env-dir", str(cloud / "envs" / env), "--step", step]


def _environ(**extra):
    env = {k: v for k, v in os.environ.items() if k not in DROP}
    env.update(GIT_IDENTITY, **extra)
    return env


def _pty_run(cmd, answer, env, cwd=None, timeout=300):
    """Run cmd on a pseudo-terminal; type ``answer`` at the y/N prompt (None: never answer)."""
    master, slave = pty.openpty()
    proc = subprocess.Popen(cmd, stdin=slave, stdout=slave, stderr=slave, env=env, cwd=cwd, close_fds=True)
    os.close(slave)
    out, answered, deadline = b"", False, time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            ready, _, _ = select.select([master], [], [], 0.2)
            if ready:
                try:
                    chunk = os.read(master, 65536)
                except OSError:
                    break
                if not chunk:
                    break
                out += chunk
            if answer is not None and not answered and PROMPT.encode() in out:
                os.write(master, answer.encode() + b"\n")
                answered = True
            if proc.poll() is not None and not ready:
                break
        else:
            proc.kill()
            raise AssertionError(f"timed out:\n{out.decode(errors='replace')}")
    finally:
        os.close(master)
    proc.wait(timeout=30)
    return proc.returncode, out.decode(errors="replace").replace("\r\n", "\n")


def _git_state(root):
    """HEAD, every ref, the index bytes and the full status: what a declined offer must leave alone."""
    index = root / ".git" / "index"
    return (_git(root, "rev-parse", "HEAD"), _git(root, "for-each-ref"),
            index.read_bytes() if index.exists() else None,
            _git(root, "status", "--porcelain", "--ignored", "--untracked-files=all"))


def _committed(root):
    return sorted(_git(root, "show", "--name-only", "--format=", "HEAD").split())


EXPECTED = ["aws/envs/dev/env.auto.tfvars", "aws/envs/dev/generated/abac.auto.tfvars",
            "aws/envs/dev/generated/masking_functions.sql"]


# -- y / n on a terminal ---------------------------------------------------------

def test_yes_commits_exactly_gitignore_and_the_env_locally_without_a_remote(checkout):
    (_cloud(checkout) / "envs" / "account").mkdir()
    (_cloud(checkout) / "envs" / "account" / "env.auto.tfvars").write_text("x = 1\n")  # not this env
    (checkout / "README.md").write_text("mine\n")  # outside the cloud folder
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 0, out
    assert "=== Commit dev's changes (.gitignore and envs/dev/) ===" in out
    assert "aws/envs/dev/generated/abac.auto.tfvars" in out  # the --stat
    assert "Commit these changes and keep them local (no origin remote)? [y/N]" in out
    assert "No origin remote (a local-only repo): committed locally only; nothing was pushed." in out
    assert _committed(checkout) == EXPECTED  # never auth, state or .genie_* files
    assert _git(checkout, "log", "-1", "--format=%s").strip() == (
        "GenieRails: draft the dev rules (make generate ENV=dev)")
    status = _git(checkout, "status", "--porcelain")
    assert "README.md" in status and "aws/envs/account/" in status  # left as they were
    assert "Not included (left as they are):" in out
    assert not _git(checkout, "diff", "--cached", "--name-only")


@pytest.mark.parametrize("answer", ["n", "", "no"])
def test_any_answer_but_yes_leaves_git_unchanged(checkout, answer):
    before = _git_state(checkout)
    code, out = _pty_run(_args(checkout), answer, _environ(GENIERAILS_COMMIT="ask"))
    assert code == 0, out
    assert "Not committed; git is unchanged. To commit yourself: git add .gitignore envs/dev/ && git commit" in out
    assert _git_state(checkout) == before


def test_yes_pushes_to_origin_main_of_a_bare_remote(checkout, tmp_path):
    bare = tmp_path / "origin.git"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(bare))
    _git(checkout, "remote", "add", "origin", str(bare))
    _git(checkout, "push", "-q", "origin", "main")
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 0, out
    assert "Commit these changes and push them to origin/main? [y/N]" in out
    assert "Pushed to origin/main." in out
    head = _git(checkout, "rev-parse", "HEAD").strip()
    assert _git(bare, "rev-parse", "main").strip() == head
    assert _committed(checkout) == EXPECTED


def test_a_non_fast_forward_push_is_refused_and_the_commit_stays_local(checkout, tmp_path):
    bare = tmp_path / "origin.git"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(bare))
    _git(checkout, "remote", "add", "origin", str(bare))
    _git(checkout, "push", "-q", "origin", "main")
    other = tmp_path / "other"
    _git(tmp_path, "clone", "-q", str(bare), str(other))
    (other / "x.txt").write_text("teammate\n")
    _git(other, "add", "x.txt")
    _git(other, "commit", "-q", "-m", "teammate")
    _git(other, "push", "-q", "origin", "main")
    remote_head = _git(bare, "rev-parse", "main")
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 1, out
    assert "locally, but did not push" in out and "not a fast-forward" in out
    assert "never force-pushes" in out and "git pull --rebase origin main" in out
    assert _git(bare, "rev-parse", "main") == remote_head  # nothing forced
    assert _committed(checkout) == EXPECTED


# -- refusals: nothing staged, nothing committed --------------------------------------

def _assert_refused(checkout, before, code, out, *needles):
    assert code == 1, out
    assert "ERROR: not committing:" in out and "Nothing was staged or committed." in out
    for needle in needles:
        assert needle in out, out
    assert PROMPT not in out
    assert _git_state(checkout) == before


def test_a_force_staged_secret_file_is_refused(checkout):
    _git(checkout, "add", "-f", "aws/envs/dev/auth.auto.tfvars")
    before = _git_state(checkout)
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    _assert_refused(checkout, before, code, out, "aws/envs/dev/auth.auto.tfvars: credentials")


def test_a_tracked_state_file_is_refused(checkout):
    _git(checkout, "add", "-f", "aws/envs/dev/terraform.tfstate")
    _git(checkout, "commit", "-q", "-m", "oops")
    (_cloud(checkout) / "envs" / "dev" / "terraform.tfstate").write_text('{"serial": 2}\n')
    before = _git_state(checkout)
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    _assert_refused(checkout, before, code, out, "terraform.tfstate: Terraform state or a plan", "git rm --cached")


def test_a_secret_value_in_a_committed_file_is_refused(checkout):
    env_file = _cloud(checkout) / "envs" / "dev" / "env.auto.tfvars"
    env_file.write_text(env_file.read_text() + 'azure_client_secret = "abc123"\n')
    before = _git_state(checkout)
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    _assert_refused(checkout, before, code, out, "env.auto.tfvars: it holds a client secret or token value")


def test_an_unrelated_staged_file_is_refused(checkout):
    (checkout / "notes.txt").write_text("unrelated\n")
    _git(checkout, "add", "notes.txt")
    before = _git_state(checkout)
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    _assert_refused(checkout, before, code, out, "other changes are already staged (notes.txt)")


def test_a_failed_commit_hook_restores_the_index(checkout):
    hook = checkout / ".git" / "hooks" / "pre-commit"
    hook.write_text("#!/bin/sh\necho 'hook says no' >&2\nexit 1\n")
    hook.chmod(0o755)
    before = _git_state(checkout)
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 1, out
    assert "hook says no" in out and "git commit failed" in out  # the repo's hooks run
    assert _git_state(checkout) == before


def _bare_origin(checkout, tmp_path):
    bare = tmp_path / "origin.git"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(bare))
    _git(checkout, "remote", "add", "origin", str(bare))
    _git(checkout, "push", "-q", "origin", "main")
    return bare


def _hook(checkout, body, name="pre-commit"):
    hook = checkout / ".git" / "hooks" / name
    hook.write_text("#!/bin/sh\n" + body)
    hook.chmod(0o755)


def test_a_hook_that_stages_a_secret_file_is_undone_and_never_pushed(checkout, tmp_path):
    """Codex r1: a pre-commit hook ran after validation and staged auth.auto.tfvars into the commit."""
    bare = _bare_origin(checkout, tmp_path)
    _hook(checkout, "git add -f aws/envs/dev/auth.auto.tfvars\n")
    before, remote_head = _git_state(checkout), _git(bare, "rev-parse", "main")
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 1, out
    assert "a commit hook changed what was committed, so the commit was undone and nothing was pushed" in out
    assert "aws/envs/dev/auth.auto.tfvars: credentials" in out
    assert "Pushed" not in out and "Committed " not in out
    assert _git_state(checkout) == before  # no new commit, index as before the offer
    assert _git(bare, "rev-parse", "main") == remote_head


def test_a_hook_that_stages_a_file_outside_the_env_is_undone_and_never_pushed(checkout, tmp_path):
    bare = _bare_origin(checkout, tmp_path)
    _hook(checkout, "echo hook > notes.txt\ngit add notes.txt\n")
    head, remote_head = _git(checkout, "rev-parse", "HEAD"), _git(bare, "rev-parse", "main")
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 1, out
    assert "notes.txt: outside .gitignore and envs/dev/" in out
    assert "Pushed" not in out
    assert _git(checkout, "rev-parse", "HEAD") == head
    assert not _git(checkout, "diff", "--cached", "--name-only")  # notes.txt unstaged again
    assert _git(bare, "rev-parse", "main") == remote_head


@pytest.mark.parametrize("name", ["prepare-commit-msg", "commit-msg", "post-commit"])
def test_a_late_hook_that_leaves_a_secret_staged_is_undone_and_never_pushed(checkout, tmp_path, name):
    """Codex r2: these hooks run after the commit's tree is built, so what they stage isn't in HEAD
    (which passed its check) but stays staged for the next git commit: exit 0, pushed, secret staged."""
    bare = _bare_origin(checkout, tmp_path)
    _hook(checkout, "git add -f aws/envs/dev/auth.auto.tfvars\n", name)
    before, remote_head = _git_state(checkout), _git(bare, "rev-parse", "main")
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 1, out
    assert "a commit hook staged more after the commit was built, so the commit was undone" in out
    assert "\n    aws/envs/dev/auth.auto.tfvars" in out
    assert "Pushed" not in out and "Committed " not in out
    assert _git_state(checkout) == before  # no new commit, nothing extra staged, index as before
    assert _git(bare, "rev-parse", "main") == remote_head


def test_a_hook_that_switches_branches_is_never_pushed_or_switched_back(checkout, tmp_path):
    """Codex r2: a post-commit hook switched to another branch, and HEAD:main still went to origin/main."""
    bare = _bare_origin(checkout, tmp_path)
    _hook(checkout, "git switch -q -c side\n", "post-commit")
    remote_head = _git(bare, "rev-parse", "main")
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 1, out
    assert "a commit hook moved HEAD: HEAD is on side, not main. Nothing was pushed and nothing was undone" in out
    assert "git switch main, then push yourself: git push origin main" in out
    assert "Pushed" not in out
    assert _git(bare, "rev-parse", "main") == remote_head
    assert _git(checkout, "symbolic-ref", "--short", "HEAD").strip() == "side"  # not switched back
    assert _committed(checkout) == EXPECTED  # the commit stays, on main and side


def test_the_push_sends_the_checked_commit_to_the_branch(checkout, tmp_path, monkeypatch):
    """Even if HEAD moves between the check and the push, the push names the checked commit."""
    bare = _bare_origin(checkout, tmp_path)
    calls = []
    real = commit_envs._git

    def spy(cwd, *args, **kw):
        if args[:1] == ("push",):
            calls.append(args)
        return real(cwd, *args, **kw)
    monkeypatch.setattr(commit_envs, "_git", spy)
    monkeypatch.setenv("GENIERAILS_COMMIT", "1")
    cloud = _cloud(checkout)
    assert commit_envs.offer(cloud, "dev", cloud / "envs" / "dev", "generate", tty=False) == 0
    head = _git(checkout, "rev-parse", "HEAD").strip()
    assert calls == [("push", "origin", f"{head}:refs/heads/main")]
    assert _git(bare, "rev-parse", "main").strip() == head


def test_non_utf8_content_is_scanned_without_crashing(checkout):
    generated = _cloud(checkout) / "envs" / "dev" / "generated"
    (generated / "logo.bin").write_bytes(b"\x89PNG\xff\xfe\x00\x01not text")
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 0 and "Traceback" not in out, out
    assert "aws/envs/dev/generated/logo.bin" in _committed(checkout)


def test_a_secret_in_non_utf8_content_is_refused_cleanly(checkout):
    generated = _cloud(checkout) / "envs" / "dev" / "generated"
    (generated / "dump.bin").write_bytes(b"\xff\xfe\x00\ndatabricks_client_secret = \"dose123\"\n\xc3\x28")
    before = _git_state(checkout)
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert "Traceback" not in out
    _assert_refused(checkout, before, code, out, "dump.bin: it holds a client secret or token value")


def _with_account_change(checkout):
    account = _cloud(checkout) / "envs" / "account"
    account.mkdir()
    (account / "env.auto.tfvars").write_text("x = 1\n")
    _git(checkout, "add", "aws/envs/account")
    _git(checkout, "commit", "-q", "-m", "account")
    (account / "env.auto.tfvars").write_text("x = 2\n")


def test_promote_to_commits_the_account_layer_too(checkout):
    _with_account_change(checkout)
    code, out = _pty_run(_args(checkout, step="promote-to"), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 0, out
    assert "=== Commit dev's changes (.gitignore, envs/dev/ and envs/account/) ===" in out
    assert "aws/envs/account/env.auto.tfvars" in out  # the --stat
    assert _committed(checkout) == sorted(EXPECTED + ["aws/envs/account/env.auto.tfvars"])
    assert "aws/envs/account" not in _git(checkout, "status", "--porcelain")


def test_promote_to_refuses_an_account_layer_that_was_already_dirty(checkout):
    """Codex r2: promote-to swept edits that were in envs/account/ before it ran into its commit."""
    _with_account_change(checkout)
    (_cloud(checkout) / "envs" / "account" / "notes.md").write_text("mine\n")
    dirty = commit_envs.dirty(_cloud(checkout), "dev", "promote-to")
    assert dirty == ["aws/envs/account/env.auto.tfvars", "aws/envs/account/notes.md"]
    before = _git_state(checkout)
    code, out = _pty_run(_args(checkout, step="promote-to") + ["--account-dirty", "\n".join(dirty)], "y",
                         _environ(GENIERAILS_COMMIT="ask"))
    _assert_refused(checkout, before, code, out,
                    "envs/account/ already had uncommitted changes before this promote-to "
                    "(aws/envs/account/env.auto.tfvars, aws/envs/account/notes.md)",
                    "commit or revert them first, then re-run make promote-to ENV=dev")


def test_dirty_is_empty_for_a_clean_account_layer_and_for_generate(checkout):
    _with_account_change(checkout)
    assert commit_envs.dirty(_cloud(checkout), "dev", "generate") == []
    assert commit_envs.dirty(_cloud(checkout), "account", "promote-to") == []
    _git(checkout, "checkout", "--", "aws/envs/account")
    assert commit_envs.dirty(_cloud(checkout), "dev", "promote-to") == []


def test_generate_leaves_the_account_layer_out(checkout):
    _with_account_change(checkout)
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 0, out
    assert _committed(checkout) == EXPECTED
    assert "Not included (left as they are): aws/envs/account/env.auto.tfvars" in out


def test_a_secret_in_the_account_layer_is_refused_for_promote_to(checkout):
    _with_account_change(checkout)
    (_cloud(checkout) / "envs" / "account" / "env.auto.tfvars").write_text('client_secret = "abc"\n')
    before = _git_state(checkout)
    code, out = _pty_run(_args(checkout, step="promote-to"), "y", _environ(GENIERAILS_COMMIT="ask"))
    _assert_refused(checkout, before, code, out, "aws/envs/account/env.auto.tfvars: it holds a client secret")


@pytest.mark.parametrize("path, why", [
    ("envs/dev/auth.auto.tfvars", "credentials"),
    ("envs/dev/data_access/terraform.tfstate.1700000000.backup", "Terraform state or a plan"),
    ("envs/prod/plan.tfplan", "Terraform state or a plan"),
    ("envs/dev/.genierails-state/r.json", "the receipt store"),
    ("envs/prod/generated/evidence/evidence.json", "an evidence record"),
    ("envs/dev/.policy_generations/1.json", "a runtime file (envs/**/.*)"),
    ("envs/dev/ddl/_fetched.sql", "a runtime file"),
    ("envs/dev/data_access/env.auto.tfvars", "a runtime file"),
    ("envs/dev/classification.auto.tfvars", "a runtime file"),
])
def test_denied_paths(path, why):
    assert commit_envs.denied(path) == why


@pytest.mark.parametrize("path", ["envs/dev/env.auto.tfvars", "envs/dev/generated/abac.auto.tfvars",
                                  "envs/dev/agents/Payments.auto.tfvars", "envs/prod/ack.txt", ".gitignore"])
def test_allowed_paths(path):
    assert commit_envs.denied(path) == ""


# -- no prompt: exactly today's behaviour ----------------------------------------------

@pytest.mark.parametrize("extra", [{"GENIERAILS_COMMIT": "ask"}, {"GENIERAILS_COMMIT": "0"}, {},
                                   {"GENIERAILS_COMMIT": "1", "CI": "true"},
                                   {"GENIERAILS_COMMIT": "ask", "TF_BUILD": "True"}])
def test_no_terminal_ci_or_commit_0_writes_and_prints_nothing(checkout, extra):
    before = _git_state(checkout)
    result = subprocess.run(_args(checkout), text=True, capture_output=True, stdin=subprocess.DEVNULL,
                            env=_environ(**extra))
    assert (result.returncode, result.stdout, result.stderr) == (0, "", "")
    assert _git_state(checkout) == before


@pytest.mark.parametrize("extra", [{"GENIERAILS_COMMIT": "0"}, {"GENIERAILS_COMMIT": "ask", "CI": "1"}])
def test_commit_0_and_ci_never_ask_on_a_terminal(checkout, extra):
    before = _git_state(checkout)
    code, out = _pty_run(_args(checkout), None, _environ(**extra))
    assert (code, out) == (0, "")
    assert _git_state(checkout) == before


def test_commit_1_commits_without_asking(checkout):
    result = subprocess.run(_args(checkout), text=True, capture_output=True, stdin=subprocess.DEVNULL,
                            env=_environ(GENIERAILS_COMMIT="1"))
    assert result.returncode == 0, result.stdout + result.stderr
    assert PROMPT not in result.stdout and "committed locally only" in result.stdout
    assert _committed(checkout) == EXPECTED


def test_genierails_own_repo_is_never_offered(checkout):
    (_cloud(checkout) / ".gitignore").write_text("")
    _git(checkout, "commit", "-q", "-am", "no block")
    before = _git_state(checkout)
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 0 and PROMPT not in out
    assert "Not offering to commit: aws/.gitignore has no GenieRails customer block" in out
    assert _git_state(checkout) == before
    result = subprocess.run(_args(checkout), text=True, capture_output=True, stdin=subprocess.DEVNULL,
                            env=_environ(GENIERAILS_COMMIT="1"))
    assert result.returncode == 1 and "COMMIT=1: not committing" in result.stderr
    assert _git_state(checkout) == before


def test_another_branch_is_not_offered(checkout):
    _git(checkout, "checkout", "-q", "-b", "feature")
    before = _git_state(checkout)
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 0 and "Not offering to commit: HEAD is on feature, not main" in out
    assert _git_state(checkout) == before


def test_nothing_to_commit(checkout):
    _git(checkout, "add", "aws")
    _git(checkout, "commit", "-q", "-m", "done")
    code, out = _pty_run(_args(checkout), "y", _environ(GENIERAILS_COMMIT="ask"))
    assert code == 0 and "Nothing to commit: .gitignore and envs/dev/ match HEAD." in out


# -- banners --------------------------------------------------------------------------

def test_generate_banner_names_the_offer_instead_of_git(tmp_path):
    from generate_abac import generate_next_steps
    plain = "\n".join(generate_next_steps(tmp_path, "full", "dev", has_sql=True, champion_flow=True))
    offered = "\n".join(generate_next_steps(tmp_path, "full", "dev", has_sql=True, champion_flow=True,
                                            commit_offer=True))
    assert "2. git add envs/ && git commit, then: make check ENV=dev" in plain
    assert "git add" not in offered
    assert "2. answer y when make generate offers to commit envs/ below, then: make check ENV=dev" in offered


def test_promote_release_steps_name_the_offer_instead_of_git(tmp_path):
    prod = tmp_path / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text('governance_mode = "deterministic"\npromote_from = "dev"\n')
    assert coverage_fix.release_steps("prod", prod).startswith("commit and push envs/prod/ to origin/main")
    offered = coverage_fix.release_steps("prod", prod, commit_offer=True)
    assert offered.startswith("answer y when promote-to offers to commit envs/prod/ below")
    assert "make release-plan ENV=prod" in offered


def test_will_offer_follows_the_same_rules(checkout):
    cloud = _cloud(checkout)
    args = (cloud, "dev", cloud / "envs" / "dev", "generate")
    assert commit_envs.will_offer(*args, environ={"GENIERAILS_COMMIT": "ask"}, tty=True)
    assert not commit_envs.will_offer(*args, environ={"GENIERAILS_COMMIT": "ask"}, tty=False)
    assert not commit_envs.will_offer(*args, environ={"GENIERAILS_COMMIT": "0"}, tty=True)
    assert not commit_envs.will_offer(*args, environ={"GENIERAILS_COMMIT": "ask", "CI": "true"}, tty=True)
    assert commit_envs.will_offer(*args, environ={"GENIERAILS_COMMIT": "1"}, tty=False)
    assert not commit_envs.will_offer(*args, environ={}, tty=True)  # outside make: never


# -- through make: make promote-to ENV=prod --------------------------------------------

def _customer_promote_repo(repo):
    """The deterministic fixture as a customer repo: the .gitignore block committed, prod set up."""
    cloud = repo / "aws"
    prod = _envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text(_prod_env())
    (prod / "auth.auto.tfvars").write_text(_auth(DEPLOYER, host=""))
    (cloud / ".gitignore").write_text(customer_gitignore.block(cloud))
    _git(repo, "branch", "-M", "main")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "customer repo, prod set up")
    return repo


def _make_env(tmp_path, **extra):
    env = _environ(PATH=f"{tmp_path / 'bin'}:{os.environ['PATH']}", FAKE_TF_LOG=str(tmp_path / "tf.log"),
                   HOME=str(tmp_path), PYTHONDONTWRITEBYTECODE="1", **extra)
    for name in ("ENV", "FROM", "CATALOG_MAP", "CHECK", "GENIERAILS_ALLOW_CI_APPLY"):
        env.pop(name, None)
    return {k: v for k, v in env.items() if not k.startswith("DATABRICKS_")}


def _make_cmd(repo, *args):
    return ["make", "--no-print-directory", "-C", str(repo / "aws"), "promote-to", "ENV=prod", *args]


def _fresh_setup_clone(tmp_path):
    """A fresh /tmp clone with this working tree's setup implementation overlaid."""
    checkout = tmp_path / "setup-clone"
    subprocess.run(["git", "clone", "-q", "--no-hardlinks", str(ROOT), str(checkout)], check=True, env=_environ())
    _git(checkout, "remote", "remove", "origin")
    for rel in ("shared/Makefile.shared", "shared/scripts/commit_envs.py"):
        shutil.copy2(ROOT / rel, checkout / rel)
    _git(checkout, "checkout", "-q", "-B", "main")
    _git(checkout, "add", "shared/Makefile.shared", "shared/scripts/commit_envs.py")
    if _git(checkout, "status", "--porcelain"):
        _git(checkout, "commit", "-q", "-m", "test setup implementation")
    return checkout


def _setup_cmd(repo, *args):
    return ["make", "--no-print-directory", "-C", str(repo / "aws"), "setup", "ENV=dev", *args]


def test_make_setup_y_commits_only_its_safe_paths_and_leaves_rehearse_clean(repo, tmp_path):
    checkout = _fresh_setup_clone(tmp_path)
    head = _git(checkout, "rev-parse", "HEAD")
    code, out = _pty_run(_setup_cmd(checkout), "y", _make_env(tmp_path))
    assert code == 0, out
    assert "setup offers to commit and push the .gitignore and env files it created; answer y" in out
    assert "Commit these changes and keep them local (no origin remote)? [y/N]" in out
    assert "Not included (left as they are): aws/envs/" not in out
    assert _git(checkout, "rev-parse", "HEAD~1") == head
    committed = _committed(checkout)
    assert committed
    assert all(path == "aws/.gitignore" or path.startswith(("aws/envs/dev/", "aws/envs/account/"))
               for path in committed), committed
    assert not any(path.endswith("auth.auto.tfvars") for path in committed)
    assert not _git(checkout, "diff", "--cached", "--name-only")
    assert not _git(checkout, "status", "--porcelain")  # rehearse's clean-check sees a clean checkout


def test_make_setup_n_leaves_head_and_index_untouched(tmp_path):
    checkout = _fresh_setup_clone(tmp_path)
    head = _git(checkout, "rev-parse", "HEAD")
    code, out = _pty_run(_setup_cmd(checkout), "n", _make_env(tmp_path))
    assert code == 0, out
    assert "Not committed; git is unchanged." in out
    assert _git(checkout, "rev-parse", "HEAD") == head
    assert not _git(checkout, "diff", "--cached", "--name-only")
    assert (checkout / "aws/envs/dev/auth.auto.tfvars").is_file()


def test_make_setup_twice_second_run_is_a_no_op(tmp_path):
    checkout = _fresh_setup_clone(tmp_path)
    first, first_out = _pty_run(_setup_cmd(checkout), "y", _make_env(tmp_path))
    assert first == 0, first_out
    head = _git(checkout, "rev-parse", "HEAD")

    second, second_out = _pty_run(_setup_cmd(checkout), None, _make_env(tmp_path))

    assert second == 0, second_out
    assert "Nothing to commit: .gitignore, envs/dev/ and envs/account/ match HEAD." in second_out
    assert "already had uncommitted changes" not in second_out
    assert PROMPT not in second_out
    assert _git(checkout, "rev-parse", "HEAD") == head

    # Command-line COMMIT=1 must preserve the same no-op behavior; ignored
    # runtime files from the first setup are not candidates for this commit.
    forced, forced_out = _pty_run(_setup_cmd(checkout, "COMMIT=1"), None, _make_env(tmp_path))
    assert forced == 0, forced_out
    assert "already had uncommitted changes" not in forced_out and PROMPT not in forced_out
    assert _git(checkout, "rev-parse", "HEAD") == head


def test_make_setup_prod_after_committed_dev_ignores_existing_runtime_files(tmp_path):
    checkout = _fresh_setup_clone(tmp_path)
    first, first_out = _pty_run(_setup_cmd(checkout), "y", _make_env(tmp_path))
    assert first == 0, first_out
    head = _git(checkout, "rev-parse", "HEAD")

    code, out = _pty_run(["make", "--no-print-directory", "-C", str(checkout / "aws"),
                          "setup", "ENV=prod"], "y", _make_env(tmp_path))

    assert code == 0, out
    assert "Commit these changes and keep them local (no origin remote)? [y/N]" in out
    assert "already had uncommitted changes" not in out
    assert "auth.auto.tfvars" not in out[out.index("=== Commit prod's changes"):]
    assert _git(checkout, "rev-parse", "HEAD~1") == head


def test_make_setup_repo_rerun_after_local_commit_then_manual_push(tmp_path):
    checkout = _fresh_setup_clone(tmp_path)
    first, first_out = _pty_run(_setup_cmd(checkout), "y", _make_env(tmp_path))
    assert first == 0, first_out
    head = _git(checkout, "rev-parse", "HEAD").strip()
    bare = tmp_path / "customer.git"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(bare))

    code, out = _pty_run(_setup_cmd(checkout, f"REPO={bare}"), None, _make_env(tmp_path))

    assert code == 0, out
    assert f"Git: added remote origin = {bare}" in out
    assert "Nothing to commit: .gitignore, envs/dev/ and envs/account/ match HEAD." in out
    assert "already had uncommitted changes" not in out
    _git(checkout, "push", "-q", "-u", "origin", "main")
    assert _git(bare, "rev-parse", "main").strip() == head


def test_make_setup_refuses_preexisting_scoped_edits(tmp_path):
    checkout = _fresh_setup_clone(tmp_path)
    env = checkout / "aws/envs/dev"
    env.mkdir(parents=True)
    (env / "notes.md").write_text("mine before setup\n")
    head = _git(checkout, "rev-parse", "HEAD")
    code, out = _pty_run(_setup_cmd(checkout), "y", _make_env(tmp_path))
    assert code != 0, out
    assert "already had uncommitted changes before setup (aws/envs/dev/notes.md)" in out
    assert PROMPT not in out
    assert _git(checkout, "rev-parse", "HEAD") == head
    assert not _git(checkout, "diff", "--cached", "--name-only")


def test_make_setup_hook_staging_an_extra_file_is_rolled_back(tmp_path):
    checkout = _fresh_setup_clone(tmp_path)
    _hook(checkout, "echo hook > notes.txt\ngit add notes.txt\n")
    head = _git(checkout, "rev-parse", "HEAD")
    code, out = _pty_run(_setup_cmd(checkout), "y", _make_env(tmp_path))
    assert code != 0, out
    assert "a commit hook changed what was committed, so the commit was undone" in out
    assert "notes.txt: outside .gitignore, envs/dev/ and envs/account/" in out
    assert _git(checkout, "rev-parse", "HEAD") == head
    assert not _git(checkout, "diff", "--cached", "--name-only")


def test_setup_dirty_snapshot_failure_is_fail_closed(checkout, monkeypatch, capsys):
    real = commit_envs._git

    def fail_status(cwd, *args, **kwargs):
        if args[:1] == ("status",):
            return subprocess.CompletedProcess(args, 1, "", "cannot read index")
        return real(cwd, *args, **kwargs)

    monkeypatch.setattr(commit_envs, "_git", fail_status)
    cloud = _cloud(checkout)
    rc = commit_envs.main(["dirty", "--cloud-dir", str(cloud), "--env", "dev",
                           "--env-dir", str(cloud / "envs/dev"), "--step", "setup"])
    assert rc == 1
    assert "cannot snapshot pre-existing setup changes: cannot read index" in capsys.readouterr().err


@pytest.mark.parametrize("mode", ["no-tty", "ci", "commit-0", "exported-commit"])
def test_make_setup_ci_no_tty_and_opt_out_keep_git_unchanged(tmp_path, mode):
    checkout = _fresh_setup_clone(tmp_path)
    head = _git(checkout, "rev-parse", "HEAD")
    extra = {"ci": {"CI": "true"}, "exported-commit": {"COMMIT": "1"}}.get(mode, {})
    args = ("COMMIT=0",) if mode == "commit-0" else ()
    result = subprocess.run(_setup_cmd(checkout, *args), text=True, capture_output=True,
                            stdin=subprocess.DEVNULL, env=_make_env(tmp_path, **extra), timeout=300)
    assert result.returncode == 0, result.stdout + result.stderr
    assert PROMPT not in result.stdout and "=== Commit" not in result.stdout
    assert _git(checkout, "rev-parse", "HEAD") == head
    assert not _git(checkout, "diff", "--cached", "--name-only")


def test_direct_internal_setup_target_never_enables_the_commit_offer(tmp_path):
    checkout = _fresh_setup_clone(tmp_path)
    head = _git(checkout, "rev-parse", "HEAD")
    code, out = _pty_run(["make", "--no-print-directory", "-C", str(checkout / "aws"),
                          "_setup", "ENV=dev"], None, _make_env(tmp_path))
    assert code == 0, out
    assert PROMPT not in out and "=== Commit" not in out and "setup offers to commit and push" not in out
    assert _git(checkout, "rev-parse", "HEAD") == head


def test_make_promote_to_on_a_terminal_commits_prod_after_y(repo, tmp_path):
    repo = _customer_promote_repo(repo)
    head = _git(repo, "rev-parse", "HEAD")
    code, out = _pty_run(_make_cmd(repo), "y", _make_env(tmp_path))
    assert code == 0, out
    assert "answer y when promote-to offers to commit envs/prod/ below" in out
    assert "Commit these changes and keep them local (no origin remote)? [y/N]" in out
    assert _git(repo, "rev-parse", "HEAD~1") == head
    committed = _committed(repo)
    assert committed and all(p.startswith("aws/envs/prod/") or p == "aws/.gitignore" for p in committed), committed
    assert "aws/envs/prod/generated/abac.auto.tfvars" in committed
    assert not any(p.endswith("auth.auto.tfvars") for p in committed)
    assert _git(repo, "log", "-1", "--format=%s").strip() == (
        "GenieRails: promote the rules to prod (make promote-to ENV=prod)")


def test_make_promote_to_answer_n_leaves_git_unchanged(repo, tmp_path):
    repo = _customer_promote_repo(repo)
    head = _git(repo, "rev-parse", "HEAD")
    code, out = _pty_run(_make_cmd(repo), "n", _make_env(tmp_path))
    assert code == 0, out
    assert "Not committed; git is unchanged." in out
    assert _git(repo, "rev-parse", "HEAD") == head
    assert not _git(repo, "diff", "--cached", "--name-only")


@pytest.mark.parametrize("how", ["no-tty", "shell-commit-1", "shell-genierails-commit-1", "commit-0-tty"])
def test_make_promote_to_without_a_prompt_is_unchanged(repo, tmp_path, how):
    repo = _customer_promote_repo(repo)
    head = _git(repo, "rev-parse", "HEAD")
    if how == "commit-0-tty":
        code, out = _pty_run(_make_cmd(repo, "COMMIT=0"), None, _make_env(tmp_path))
    else:
        extra = {"shell-commit-1": {"COMMIT": "1"}, "shell-genierails-commit-1": {"GENIERAILS_COMMIT": "1"}}.get(how, {})
        result = subprocess.run(_make_cmd(repo), text=True, capture_output=True, stdin=subprocess.DEVNULL,
                                env=_make_env(tmp_path, **extra), timeout=600)
        code, out = result.returncode, result.stdout + result.stderr
    assert code == 0, out
    assert PROMPT not in out and "Commit these changes" not in out and "Not offering" not in out
    assert "commit and push envs/prod/ to origin/main (or merge its PR)" in out  # today's banner
    assert _git(repo, "rev-parse", "HEAD") == head  # the shell environment never turns it on
    assert not _git(repo, "diff", "--cached", "--name-only")


def test_make_promote_to_commit_1_on_the_command_line_commits_without_a_terminal(repo, tmp_path):
    repo = _customer_promote_repo(repo)
    head = _git(repo, "rev-parse", "HEAD")
    result = subprocess.run(_make_cmd(repo, "COMMIT=1"), text=True, capture_output=True,
                            stdin=subprocess.DEVNULL, env=_make_env(tmp_path), timeout=600)
    assert result.returncode == 0, result.stdout + result.stderr
    assert PROMPT not in result.stdout and "committed locally only" in result.stdout
    assert _git(repo, "rev-parse", "HEAD~1") == head


def test_make_refuses_an_unknown_commit_value_before_running(repo, tmp_path):
    repo = _customer_promote_repo(repo)
    before = _git(repo, "status", "--porcelain", "--ignored")
    result = subprocess.run(_make_cmd(repo, "COMMIT=yes"), text=True, capture_output=True,
                            stdin=subprocess.DEVNULL, env=_make_env(tmp_path), timeout=600)
    assert result.returncode != 0
    assert "COMMIT=yes: use COMMIT=0 (never commit) or COMMIT=1 (commit without asking)" in result.stderr
    assert _git(repo, "status", "--porcelain", "--ignored") == before


def test_make_check_1_promote_to_never_offers(repo, tmp_path):
    repo = _customer_promote_repo(repo)
    code, out = _pty_run(_make_cmd(repo, "CHECK=1"), "y", _make_env(tmp_path))
    assert PROMPT not in out and "Not offering" not in out, out


def test_a_failed_make_generate_commits_nothing(checkout, tmp_path):
    shutil.copy2(ROOT / "aws" / "Makefile", _cloud(checkout) / "Makefile")
    (_cloud(checkout) / "envs" / "account").mkdir()
    _git(checkout, "add", "aws/Makefile")
    _git(checkout, "commit", "-q", "-m", "Makefile")
    head = _git(checkout, "rev-parse", "HEAD")
    # The auth file has no host or client ID, so generate stops before drafting anything.
    code, out = _pty_run(["make", "--no-print-directory", "-C", str(_cloud(checkout)), "generate", "ENV=dev"],
                         "y", _make_env(tmp_path))
    assert code != 0, out
    assert "before running make generate" in out
    assert PROMPT not in out and "Commit these changes" not in out
    assert _git(checkout, "rev-parse", "HEAD") == head
    assert not _git(checkout, "diff", "--cached", "--name-only")


def _recipe(target):
    text = (SHARED / "Makefile.shared").read_text()
    body = text[re.search(rf"^{re.escape(target)}: .*##", text, re.M).start():]
    lines = body.splitlines()[1:]
    return lines[:next(i for i, line in enumerate(lines) if not line.startswith("\t"))]


@pytest.mark.parametrize("target, step", [("generate", "generate"), ("promote-to", "promote-to")])
def test_the_offer_is_the_last_step_of_the_recipe(target, step):
    """make stops at the first failing recipe line, so the offer runs only after the target succeeded."""
    last = _recipe(target)[-1]
    assert "$(_COMMIT_ENVS) offer" in last and f"--step {step}" in last, last


def test_make_promote_to_commits_the_account_layer_it_changed_and_leaves_a_clean_checkout(repo, tmp_path):
    # A legacy env keeps envs/account/abac.auto.tfvars committed (customer_gitignore.block), and the
    # committed copy predates dev's merged values, so the promotion rewrites it.
    (_envs(repo) / "legacy").mkdir()
    (_envs(repo) / "legacy" / "env.auto.tfvars").write_text('governance_mode = "legacy"\n')
    (_envs(repo) / "account" / "abac.auto.tfvars").write_text(LEGACY_ACCOUNT)
    repo = _customer_promote_repo(repo)
    code, out = _pty_run(_make_cmd(repo), "y", _make_env(tmp_path))
    assert code == 0, out
    assert "(.gitignore, envs/prod/ and envs/account/)" in out
    committed = _committed(repo)
    assert "aws/envs/account/abac.auto.tfvars" in committed, committed
    assert "aws/envs/prod/generated/abac.auto.tfvars" in committed
    assert _git(repo, "status", "--porcelain") == ""  # rehearse and release-plan need this


def test_make_promote_to_refuses_account_edits_made_before_it_ran(repo, tmp_path):
    """Codex r2: an edit already in envs/account/ was committed (and pushed) with the promotion."""
    (_envs(repo) / "account" / "notes.md").write_text("team notes\n")
    repo = _customer_promote_repo(repo)
    (_envs(repo) / "account" / "notes.md").write_text("team notes\nmy unreviewed edit\n")
    head = _git(repo, "rev-parse", "HEAD")
    code, out = _pty_run(_make_cmd(repo), "y", _make_env(tmp_path))
    assert code != 0, out  # make reports the refusal as 2
    assert "envs/account/ already had uncommitted changes before this promote-to (aws/envs/account/notes.md)" in out
    assert PROMPT not in out
    assert _git(repo, "rev-parse", "HEAD") == head
    assert not _git(repo, "diff", "--cached", "--name-only")
    assert "my unreviewed edit" in (_envs(repo) / "account" / "notes.md").read_text()  # left as it was
