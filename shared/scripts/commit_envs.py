#!/usr/bin/env python3
"""make setup, generate and promote-to offer to commit what they wrote (option A).

After a successful run, ``offer`` shows ``git diff --stat`` for exactly
``.gitignore`` and ``envs/<env>/`` (the cloud folder's; for make promote-to also
``envs/account/``, the shared account layer it writes, so rehearse and
release-plan get a clean checkout) and asks y/N. On y it
stages only those paths (never ``git add -A``), commits them with a fixed
message (the repo's commit hooks run) and, when the ``origin`` remote
(RELEASE_REMOTE) exists, pushes HEAD to its main (RELEASE_BRANCH): never
forced, so a non-fast-forward push is refused and the commit stays local. With
no remote (a local-only repo) it commits locally only and says so.

When it asks (``will_offer``):
  - GENIERAILS_COMMIT is ``ask`` or ``1``: the top-level make sets it for make
    generate and make promote-to only, from a command-line COMMIT (unset: ask,
    COMMIT=1: 1, COMMIT=0: 0), and to 0 for every other goal, CHECK=1 and an
    inherited 0; nested makes inherit it. Outside make it is unset: never;
  - not CI (scripts/ci_guard.py): CI never commits, COMMIT=1 included;
  - stdin and stdout are a terminal, or COMMIT=1 (commit without asking);
  - the cloud folder is the top of its own git checkout (setup_repo.checkout),
    on RELEASE_BRANCH, and its .gitignore has the GenieRails customer block,
    which keeps credentials and runtime files out of envs/ (GenieRails' own repo
    ignores envs/ and is never offered);
  - make promote-to: the destination is deterministic.
Otherwise it writes nothing to git and prints nothing (non-TTY, CI and COMMIT=0
behave exactly as before), except a one-line reason on a terminal.

make can't tell a COMMIT typed on the command line from one inherited through
an exported MAKEFLAGS, so both count; a COMMIT in the shell environment never
does. Like setup's REPO=, this is an accident guard, not a security boundary.

Refused, with nothing committed (exit 1):
  - something outside .gitignore and envs/<env>/ is already staged, or a merge,
    rebase, cherry-pick or revert is in progress;
  - make promote-to: envs/account/ already had uncommitted changes when it
    started (``dirty`` snapshots them first, passed as --account-dirty), so they
    aren't swept into its commit; account changes the promotion itself made are
    committed;
  - a path it would commit is credentials, Terraform state or plans, a runtime
    or evidence file (DENIED, or git-ignored by the .gitignore rules even though
    tracked), holds a client secret or token value (SECRET_RE, matched on the raw
    bytes, binary files included), or can't be read to scan. The scan matches
    ASCII/UTF-8 bytes only: a UTF-16 file's secret isn't seen (the repo's own
    secret scan is the backstop); it is an accident guard, not a security boundary.
The repo's commit hooks run (they are its secret scan) and may stage more or
switch branches, so after the commit:
  - HEAD must still be on RELEASE_BRANCH; if a hook moved it, nothing is pushed
    and nothing is undone (it never switches back), and it says where HEAD is;
  - what HEAD holds is checked again, the same way, and the real index must
    equal HEAD's tree (a late hook can stage more after the tree was built):
    anything outside those paths, denied or left staged undoes that commit (git
    reset --soft to the old HEAD, the index restored to what it was before the
    offer) and nothing is pushed;
  - the push sends that checked commit (never whatever HEAD is by then).
The preview stages into a temporary copy of the index, so answering n (or a
refusal) leaves the index, HEAD and the work tree exactly as they were; a
failed commit restores the index.

Exit codes: 0 committed, declined, nothing to commit or not offered; 1 refused
or the push failed (the commit then stays local); 2 usage error.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path, PurePosixPath
from typing import Mapping

SHARED = Path(__file__).resolve().parents[1]
for _path in (SHARED, SHARED / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import ci_guard  # noqa: E402
import customer_gitignore  # noqa: E402
import setup_repo  # noqa: E402

STEPS = {
    "setup": "GenieRails: set up {env} (make setup ENV={env})",
    "generate": "GenieRails: draft the {env} rules (make generate ENV={env})",
    "promote-to": "GenieRails: promote the rules to {env} (make promote-to ENV={env})",
}
# What the customer .gitignore block keeps out (customer_gitignore.COMMON), checked
# again on every path about to be committed, whatever .gitignore says now.
DENIED_NAMES = {"auth.auto.tfvars": "credentials", "classification.auto.tfvars": "a runtime file",
                "discovered_uc_tables.auto.tfvars": "a runtime file"}
DENIED_DIRS = {".terraform": "Terraform's working directory", ".genierails-state": "the receipt store",
               "__pycache__": "Python bytecode", "evidence": "an evidence record"}
# Matched on the raw bytes, so a binary or non-UTF-8 file is scanned too.
SECRET_RE = re.compile(
    rb"""(?mi)^[ \t]*"?(?:databricks_client_secret|azure_client_secret|client_secret|databricks_token)"?"""
    rb"""[ \t]*[=:][ \t]*"[^"\s]+"|\bdapi[0-9a-f]{32}\b""")


def _git_env(**extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in setup_repo._GIT_LOCATION_VARS}
    env.update(extra)
    return env


def _git(cwd: Path, *args: str, index: str = "", stdin: str | None = None) -> subprocess.CompletedProcess:
    env = _git_env(GIT_INDEX_FILE=index) if index else _git_env()
    return subprocess.run(["git", "-C", str(cwd), *args], capture_output=True, env=env, input=stdin,
                          encoding="utf-8", errors="surrogateescape")


def _blob(root: Path, spec: str, index: str = "") -> bytes | None:
    """The raw bytes of ``spec`` (``:path`` in the index, ``<rev>:path``), or None if git can't read it."""
    env = _git_env(GIT_INDEX_FILE=index) if index else _git_env()
    proc = subprocess.run(["git", "-C", str(root), "cat-file", "blob", spec], capture_output=True, env=env)
    return proc.stdout if proc.returncode == 0 else None


def scope(cloud_rel: str, env: str, step: str, account_env: str = "account",
          account_exists: bool = True) -> tuple[list[str], str]:
    """(repo-relative paths offer commits, how they read). promote-to also writes the shared
    account layer (envs/<account>/), so its offer covers it too: rehearse and release-plan need a
    clean checkout. generate writes only envs/<env>/."""
    paths = [f"{cloud_rel}/.gitignore", f"{cloud_rel}/envs/{env}"]
    if step in ("setup", "promote-to") and account_exists and account_env != env:
        paths.append(f"{cloud_rel}/envs/{account_env}")
        return paths, f".gitignore, envs/{env}/ and envs/{account_env}/"
    return paths, f".gitignore and envs/{env}/"


def _inside(path: str, paths: list[str]) -> bool:
    return any(path == p or path.startswith(p + "/") for p in paths)


def mode(environ: Mapping[str, str], interactive: bool) -> str:
    """"ask", "auto" (COMMIT=1) or "" (no git writes, no output)."""
    commit = environ.get("GENIERAILS_COMMIT", "0")
    if commit not in ("ask", "1") or ci_guard.in_ci(environ):
        return ""
    if commit == "1":
        return "auto"
    return "ask" if interactive else ""


def interactive() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def _deterministic(env_dir: Path) -> bool:
    try:
        import hcl2
        return hcl2.loads((env_dir / "env.auto.tfvars").read_text()).get("governance_mode") == "deterministic"
    except Exception:
        return False


def unusable(cloud_dir: Path, env: str, env_dir: Path, step: str, branch: str) -> str:
    """Why this checkout can't take the commit ("" when it can)."""
    if Path(env_dir).resolve() != (cloud_dir / "envs" / env).resolve():
        return f"ENV_DIR is not {cloud_dir.name}/envs/{env}"
    root, remotes, why = setup_repo.checkout(cloud_dir)
    if why:
        return why
    if remotes is None:
        return f"{root} is not a git checkout"
    try:
        has_block = customer_gitignore._split((cloud_dir / ".gitignore").read_text())[1] is not None
    except (OSError, ValueError):
        has_block = False
    if not has_block:
        return (f"{cloud_dir.name}/.gitignore has no GenieRails customer block, so envs/ isn't committed here "
                f"(make setup ENV={env} writes it in your own repo)")
    if step == "promote-to" and not _deterministic(env_dir):
        return f"{env} is not deterministic"
    head = _git(root, "symbolic-ref", "--short", "-q", "HEAD").stdout.strip()
    if head != branch:
        return f"HEAD is {'detached' if not head else 'on ' + head}, not {branch}"
    return ""


def will_offer(cloud_dir: Path, env: str, env_dir: Path, step: str, *, environ: Mapping[str, str] | None = None,
               tty: bool | None = None, branch: str = "main") -> bool:
    """Whether offer would ask (or, with COMMIT=1, commit): what the banners print."""
    environ = os.environ if environ is None else environ
    return bool(mode(environ, interactive() if tty is None else tty)) and not unusable(
        Path(cloud_dir), env, Path(env_dir), step, branch)


def denied(rel: str) -> str:
    """Why a cloud-relative path must never be committed ("" when it may)."""
    parts = PurePosixPath(rel).parts
    name = parts[-1]
    if name in DENIED_NAMES:
        return DENIED_NAMES[name]
    if name.endswith((".tfstate", ".tfstate.backup", ".tfplan")) or ".tfstate." in name:
        return "Terraform state or a plan"
    for part in parts[:-1]:
        if part in DENIED_DIRS:
            return DENIED_DIRS[part]
    if parts[0] == "envs" and any(part.startswith(".") for part in parts[1:]):
        return "a runtime file (envs/**/.*)"
    if rel.endswith("/ddl/_fetched.sql") or (len(parts) == 4 and parts[2] == "data_access"
                                              and name == "env.auto.tfvars"):
        return "a runtime file"
    return ""


def _staged(root: Path, index: str = "") -> list[tuple[str, str]]:
    """(status letter, repo-relative path) of every staged change."""
    out = _git(root, "diff", "--cached", "--name-status", "--no-renames", "-z", index=index)
    if out.returncode:
        raise RuntimeError((out.stderr or out.stdout).strip())
    fields = out.stdout.split("\0")
    return [(fields[i], fields[i + 1]) for i in range(0, len(fields) - 1, 2)]


def _problems(root: Path, cloud_rel: str, staged: list[tuple[str, str]], index: str = "",
              rev: str = "") -> list[str]:
    """Why each staged (or, with ``rev``, committed) path must not be committed."""
    problems = []
    for status, path in staged:
        rel = path[len(cloud_rel) + 1:] if cloud_rel else path
        why = denied(rel)
        if not why and not _git(root, "check-ignore", "-q", "--no-index", "--", path).returncode:
            why = "git-ignored by .gitignore (a runtime file), though tracked"
        if not why and status != "D":
            blob = _blob(root, f"{rev}:{path}", index)
            if blob is None:
                why = "its content can't be read to scan it for secrets"
            elif SECRET_RE.search(blob):
                why = "it holds a client secret or token value"
        if why:
            problems.append(f"{path}: {why}")
    return problems


def _committed(root: Path) -> list[tuple[str, str]]:
    """(status letter, repo-relative path) of every change HEAD made to its parent."""
    out = _git(root, "diff-tree", "--no-commit-id", "--name-status", "--no-renames", "-r", "-z", "--root", "HEAD")
    if out.returncode:
        raise RuntimeError((out.stderr or out.stdout).strip())
    fields = out.stdout.split("\0")
    return [(fields[i], fields[i + 1]) for i in range(0, len(fields) - 1, 2)]


def _left_staged(root: Path) -> list[str]:
    """Paths whose staged content differs from HEAD's tree (nothing after a clean commit)."""
    out = _git(root, "diff-index", "--cached", "--name-only", "--no-renames", "-z", "HEAD")
    if out.returncode:
        raise RuntimeError((out.stderr or out.stdout).strip())
    return sorted(p for p in out.stdout.split("\0") if p)


def _off_branch(root: Path, branch: str, sha: str = "") -> str:
    """Where HEAD went if it isn't ``branch`` (at ``sha``, when given); "" when it is."""
    head = _git(root, "symbolic-ref", "--short", "-q", "HEAD").stdout.strip()
    if head != branch:
        return f"HEAD is {'detached' if not head else 'on ' + head}, not {branch}"
    tip = _git(root, "rev-parse", "-q", "--verify", f"refs/heads/{branch}").stdout.strip()
    if sha and tip != sha:
        return f"{branch} is at {tip[:12] or 'nothing'}, not the checked commit {sha[:12]}"
    return ""


def dirty(cloud_dir: Path, env: str, step: str, account_env: str = "account") -> list[str]:
    """Scoped paths that already differ from HEAD before a command starts.

    promote-to protects envs/<account>/; setup protects every path it may offer,
    so neither command can sweep a user's earlier edit into its own commit.
    """
    cloud_dir = Path(cloud_dir).resolve()
    if step not in ("setup", "promote-to"):
        return []
    probe = _git(cloud_dir.parent, "rev-parse", "--show-toplevel")
    if probe.returncode and "not a git repository" in probe.stderr.lower():
        # CHECK=1 runs setup/promote-to in a deliberately unversioned scratch
        # copy. There is no pre-existing Git state to protect there.
        return []
    if probe.returncode:
        raise RuntimeError((probe.stderr or probe.stdout).strip() or "git rev-parse failed")
    if step == "promote-to":
        if account_env == env or not (cloud_dir / "envs" / account_env).is_dir():
            return []
        paths = [f"{cloud_dir.name}/envs/{account_env}"]
    else:
        paths, _label = scope(cloud_dir.name, env, step, account_env,
                              (cloud_dir / "envs" / account_env).is_dir())
    out = _git(cloud_dir.parent, "status", "--porcelain", "-z", "--no-renames", "--untracked-files=all",
               "--", *paths)
    if out.returncode:
        raise RuntimeError((out.stderr or out.stdout).strip() or "git status failed")
    found = {entry[3:] for entry in out.stdout.split("\0") if len(entry) > 3}
    # Before a first setup writes the customer .gitignore block, the root
    # GenieRails rules ignore envs/. Include ignored files so an existing edit
    # cannot become visible later and ride setup's commit.
    if step == "setup":
        ignored = _git(cloud_dir.parent, "ls-files", "--others", "--ignored", "--exclude-standard", "-z",
                       "--", *paths)
        if ignored.returncode:
            raise RuntimeError((ignored.stderr or ignored.stdout).strip() or "git ls-files failed")
        found.update(path for path in ignored.stdout.split("\0") if path)
    return sorted(found)


def _in_progress(root: Path) -> str:
    for name, what in (("MERGE_HEAD", "a merge"), ("rebase-merge", "a rebase"), ("rebase-apply", "a rebase"),
                       ("CHERRY_PICK_HEAD", "a cherry-pick"), ("REVERT_HEAD", "a revert")):
        path = _git(root, "rev-parse", "--git-path", name).stdout.strip()
        if path and (root / path).exists():
            return what
    return ""


def offer(cloud_dir: Path, env: str, env_dir: Path, step: str, *, remote: str = "origin",
          branch: str = "main", environ: Mapping[str, str] | None = None, tty: bool | None = None,
          account_env: str = "account", account_dirty: list[str] | None = None,
          preexisting_dirty: list[str] | None = None) -> int:
    environ = os.environ if environ is None else environ
    how = mode(environ, interactive() if tty is None else tty)
    if not how:
        return 0
    cloud_dir = Path(cloud_dir).resolve()
    why = unusable(cloud_dir, env, Path(env_dir), step, branch).rstrip(".")
    if why:
        if how == "auto":
            print(f"ERROR: COMMIT=1: not committing: {why}.", file=sys.stderr)
            return 1
        print(f"  (Not offering to commit: {why}.)")
        return 0
    root = cloud_dir.parent
    cloud_rel = cloud_dir.name
    paths, label = scope(cloud_rel, env, step, account_env, (cloud_dir / "envs" / account_env).is_dir())
    manual = f"git add {' '.join(p[len(cloud_rel) + 1:] + ('' if p.endswith('.gitignore') else '/') for p in paths)}"
    manual += " && git commit"
    has_remote = _git(root, "remote", "get-url", remote).returncode == 0
    if has_remote:
        manual += f" && git push {remote} HEAD:{branch}"

    def refuse(message: str) -> int:
        print(f"ERROR: not committing: {message}\n  Nothing was staged or committed. Fix it, then: {manual}",
              file=sys.stderr)
        return 1

    busy = _in_progress(root)
    if busy:
        return refuse(f"{busy} is in progress.")
    before = sorted(p for p in account_dirty or [] if _inside(p, paths))
    if before:
        return refuse(f"envs/{account_env}/ already had uncommitted changes before this promote-to ("
                      + ", ".join(before[:5]) + (", ..." if len(before) > 5 else "")
                      + "), so they'd be swept into its commit; commit or revert them first, then re-run "
                      f"make promote-to ENV={env}.")
    other = sorted(p for _s, p in _staged(root) if not _inside(p, paths))
    if other:
        return refuse("other changes are already staged (" + ", ".join(other[:5])
                      + (", ..." if len(other) > 5 else "") + "); commit or unstage them first (git restore --staged).")
    real_index = root / _git(root, "rev-parse", "--git-path", "index").stdout.strip()
    with tempfile.TemporaryDirectory(prefix="genierails-commit-") as tmp:
        preview = str(Path(tmp) / "index")
        if real_index.exists():
            shutil.copy2(real_index, preview)
        added = _git(root, "add", "--", *paths, index=preview)
        if added.returncode:
            return refuse(f"git add failed: {(added.stderr or added.stdout).strip()}")
        staged = _staged(root, preview)
        if not staged:
            if how == "ask":
                print(f"  Nothing to commit: {label} match HEAD.")
            return 0
        staged_paths = {path for _status, path in staged}
        before = sorted(p for p in preexisting_dirty or [] if p in staged_paths)
        if before:
            return refuse("these paths already had uncommitted changes before setup ("
                          + ", ".join(before[:5]) + (", ..." if len(before) > 5 else "")
                          + "), so they'd be swept into its commit; commit or revert them first, then re-run "
                          f"make setup ENV={env}.")
        problems = _problems(root, cloud_rel, staged, preview)
        if problems:
            return refuse("these would be committed:\n    " + "\n    ".join(problems)
                          + "\n  The GenieRails .gitignore block keeps them out; a tracked one needs "
                          "git rm --cached <path>.")
        stat = _git(root, "diff", "--cached", "--stat", "--no-renames", "--", *paths, index=preview).stdout
    print(f"\n=== Commit {env}'s changes ({label}) ===")
    print(stat.rstrip("\n"))
    rest = sorted({line[3:] for line in _git(root, "status", "--porcelain").stdout.splitlines()
                   if not _inside(line[3:].strip('"').rstrip("/"), paths)
                   and not (line[3:].endswith("/")
                            and any(path.startswith(line[3:]) for _status, path in staged))})
    if rest:
        print("  Not included (left as they are): " + ", ".join(rest[:5]) + (", ..." if len(rest) > 5 else ""))
    target = f"push them to {remote}/{branch}" if has_remote else f"keep them local (no {remote} remote)"
    if how == "ask":
        try:
            answer = input(f"Commit these changes and {target}? [y/N] ")
        except EOFError:
            answer = ""
        if answer.strip().lower() not in ("y", "yes"):
            print(f"  Not committed; git is unchanged. To commit yourself: {manual}")
            return 0
    saved = Path(tempfile.mkdtemp(prefix="genierails-index-"))
    try:
        if real_index.exists():
            shutil.copy2(real_index, saved / "index")

        def restore(message: str) -> int:
            if (saved / "index").exists():
                shutil.copy2(saved / "index", real_index)
            else:
                real_index.unlink(missing_ok=True)
            return refuse(message)

        added = _git(root, "add", "--", *paths)
        if added.returncode:
            return restore(f"git add failed: {(added.stderr or added.stdout).strip()}")
        staged_now = _staged(root)
        if staged_now != staged:
            return restore("the files changed while you answered; re-run the make command.")
        problems = _problems(root, cloud_rel, staged_now, "")
        if problems:
            return restore("these would be committed:\n    " + "\n    ".join(problems))
        old_head = _git(root, "rev-parse", "-q", "--verify", "HEAD").stdout.strip()
        commit = subprocess.run(["git", "-C", str(root), "commit", "-q", "-m", STEPS[step].format(env=env)],
                                env=_git_env())
        if commit.returncode:
            return restore("git commit failed (see above).")
        # A hook that switched branches moved HEAD: resetting or restoring now would hit the
        # other branch, so leave everything where it is and push nothing.
        moved = _off_branch(root, branch)
        if moved:
            print(f"ERROR: a commit hook moved HEAD: {moved}. Nothing was pushed and nothing was undone "
                  f"(GenieRails doesn't switch branches back). Check git log {branch} and git status, "
                  f"git switch {branch}, then push yourself: git push {remote} {branch}", file=sys.stderr)
            return 1

        def undo(message: str) -> int:
            if old_head:
                _git(root, "reset", "-q", "--soft", old_head)
            else:
                _git(root, "update-ref", "-d", "HEAD")
            return restore(message)

        # The repo's hooks ran between the checks above and the commit, and a hook can
        # stage more: check what HEAD actually holds before anything is pushed.
        parent = _git(root, "rev-parse", "-q", "--verify", "HEAD^").stdout.strip()
        committed = _committed(root) if parent == old_head else []
        extra = sorted(p for _s, p in committed if not _inside(p, paths))
        bad = [] if parent != old_head else (
            [f"{p}: outside {label}" for p in extra]
            + _problems(root, cloud_rel, [c for c in committed if c[1] not in extra], rev="HEAD"))
        if parent != old_head or bad:
            detail = ("\n    " + "\n    ".join(bad)) if bad else (
                f" HEAD's parent is {parent[:12] or 'none'}, not {old_head[:12] or 'an unborn branch'}")
            return undo("a commit hook changed what was committed, so the commit was undone and "
                        f"nothing was pushed:{detail}")
        # prepare-commit-msg, commit-msg and post-commit run after the tree is built: what they
        # stage isn't in HEAD, but a later git commit would take it.
        late = _left_staged(root)
        if late:
            return undo("a commit hook staged more after the commit was built, so the commit was undone, "
                        "the index restored and nothing was pushed:\n    " + "\n    ".join(late))
    finally:
        shutil.rmtree(saved, ignore_errors=True)
    full = _git(root, "rev-parse", "HEAD").stdout.strip()
    sha = full[:7]
    print(f"  Committed {sha} on {branch}: {label} ({len(staged)} file(s)).")
    if not has_remote:
        print(f"  No {remote} remote (a local-only repo): committed locally only; nothing was pushed.")
        return 0
    moved = _off_branch(root, branch, full)
    if moved:
        print(f"ERROR: committed {sha}, but did not push: {moved}. Check it, then: git push {remote} {sha}:{branch}",
              file=sys.stderr)
        return 1
    push = _git(root, "push", remote, f"{full}:refs/heads/{branch}")
    if push.returncode == 0:
        print(f"  Pushed to {remote}/{branch}.")
        return 0
    detail = (push.stderr or push.stdout).strip()
    if "non-fast-forward" in detail or "fetch first" in detail or "[rejected]" in detail:
        reason = (f"{remote}/{branch} has commits this checkout doesn't (not a fast-forward), and GenieRails "
                  "never force-pushes. Bring them in with git pull --rebase " f"{remote} {branch}, then git push "
                  f"{remote} HEAD:{branch}; if {branch} is protected, push a branch and merge its PR instead")
    else:
        reason = f"git push failed: {detail}"
    print(f"ERROR: committed {sha} locally, but did not push: {reason}.", file=sys.stderr)
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("offer", "will-offer", "dirty"))
    parser.add_argument("--cloud-dir", required=True, type=Path)
    parser.add_argument("--env", required=True)
    parser.add_argument("--env-dir", required=True, type=Path)
    parser.add_argument("--step", required=True, choices=sorted(STEPS))
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--account-env", default="account")
    parser.add_argument("--account-dirty", default="",
                        help="dirty's output (one path per line), taken when make promote-to started")
    parser.add_argument("--preexisting-dirty", default="",
                        help="dirty's output (one path per line), taken when make setup started")
    args = parser.parse_args(argv)
    if args.command == "dirty":
        try:
            paths = dirty(args.cloud_dir, args.env, args.step, args.account_env)
        except RuntimeError as exc:
            print(f"ERROR: cannot snapshot pre-existing setup changes: {exc}", file=sys.stderr)
            return 1
        for path in paths:
            print(path)
        return 0
    if args.command == "will-offer":
        return 0 if will_offer(args.cloud_dir, args.env, args.env_dir, args.step, branch=args.branch) else 1
    return offer(args.cloud_dir, args.env, args.env_dir, args.step, remote=args.remote, branch=args.branch,
                 account_env=args.account_env, account_dirty=args.account_dirty.splitlines(),
                 preexisting_dirty=args.preexisting_dirty.splitlines())


if __name__ == "__main__":
    raise SystemExit(main())
