#!/usr/bin/env python3
"""make setup's git remotes: REPO=<url> makes the checkout the customer's repo.

GenieRails manages git for the champion (owner decision "option A"): only the
git binary is needed, no GitHub account and no git knowledge. make setup runs
this twice: ``--check`` first, right after the scheduled-governance guard and
before anything is written (a refusal leaves the checkout and its remotes
exactly as they were), then again after the env files exist, to change the
remotes. It never fetches and never pushes.

The URL comes from the GENIERAILS_SETUP_REPO environment variable, which make
exports from a command-line REPO (``$(value REPO)``), so no shell ever parses
it. make itself still expands any $(...) in a command-line value (such as
$(shell ...)) while building the recipe environment: GNU make semantics, run
by whoever typed the command, before this script or any recipe runs. make can't tell a REPO typed on the command line from
one inherited through MAKEFLAGS, so both count; a plain exported REPO (such
as bootstrap-pipelines' <owner>/<name>) doesn't. Like REUSE_SOURCE_AUTH, this
is an accident guard, not a security boundary.

Only https://, ssh://, file://, scp-like ``[user@]host:path`` and absolute
local paths are accepted: never a ``<transport>::`` helper (ext::, fd::, ...),
control characters, spaces or shell metacharacters.

git runs at the GenieRails checkout root, the cloud folder's parent, which must
hold shared/ and be the top of its own repo (GIT_DIR, GIT_WORK_TREE and the
like are scrubbed). A cloud folder inside some other repo (GenieRails vendored
into an outer project) is refused, never touching the outer repo's remotes.
Nor does setup touch a linked worktree (git worktree add, or a copy of one
with its .git file): its remotes live in the common git dir every worktree of
that repo shares, so REPO= is refused there and without REPO nothing is renamed.

With a REPO:
  - origin is GenieRails (github.com/databricks-solutions/genierails) and no
    ``genierails`` remote exists: rename origin to ``genierails`` (kept for
    upgrades), then add origin = <url>;
  - no origin (a local-only repo): add origin = <url>;
  - origin is already <url> (same_repo: the same form, host, exact path,
    user and port, with or without .git or a trailing /; on github.com,
    gitlab.com and bitbucket.org the https, ssh:// and git@ forms of one repo
    match too): nothing to do;
  - not a git checkout (a downloaded archive): git init the checkout root on
    branch main, then add origin = <url>;
  - anything else (origin elsewhere, or GenieRails with a genierails remote
    already there) is refused: setup never repoints a remote.
Without it, a fresh clone (origin at GenieRails is its only remote, not a
linked worktree) becomes a local-only repo: origin is renamed to
``genierails`` and no origin is added, so make setup writes the .gitignore
block and envs/ is committed locally. That
rename is skipped with GITIGNORE=off (contributors working on GenieRails
itself), in CI, and inside another repo; a contributor clone (a fork or
upstream remote next to origin) or a linked worktree only gets a note. Every
other layout is left alone.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple
from urllib.parse import urlsplit

SHARED = Path(__file__).resolve().parents[1]
for _path in (SHARED, SHARED / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from scripts.ci_guard import in_ci  # noqa: E402
from scripts.customer_gitignore import _is_upstream  # noqa: E402

REPO_ENV = "GENIERAILS_SETUP_REPO"
UPSTREAM_REMOTE = "genierails"
BRANCH = "main"
MANUAL = ("Prefer to manage remotes yourself? Leave REPO= off and use git remote "
          "(without REPO, setup only renames an origin that still points at GenieRails).")
# Variables that would point git at another repo or work tree than the checkout root.
_GIT_LOCATION_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY",
                      "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_NAMESPACE", "GIT_CEILING_DIRECTORIES",
                      "GIT_DISCOVERY_ACROSS_FILESYSTEM", "GIT_PREFIX")
_SAFE_CHARS = re.compile(r"[A-Za-z0-9._~:/@%+=,\[\]-]+")
class Refused(Exception):
    pass


_SCP = re.compile(r"(?:(?P<user>[^@/:]+)@)?(?P<host>[^@/:]+):(?P<path>[^:].*)")
_HOST = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?")
_USER = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._-]*")
DEFAULT_PORT = {"https": 443, "ssh": 22}


# Hosted providers whose paths are always repo-relative (owner/repo, groups/sub/repo),
# so git@host:p, ssh://git@host/p and https://host/p name one repo. Azure DevOps
# maps v3/ (ssh) and _git/ (https) paths differently, so it never cross-matches.
HOSTED = frozenset({"github.com", "gitlab.com", "bitbucket.org"})


class Url(NamedTuple):
    form: str  # https, ssh (ssh://), scp (user@host:path) or local (file:// and absolute paths)
    user: str | None
    host: str  # lower case; "" for local
    port: int | None  # the scheme's default when not given; None for local
    path: str  # exactly as given (a leading / kept), less a trailing / and .git


def parse(url: str) -> Url:
    """The parts of an accepted URL; raises Refused for anything else (see validate)."""
    if "://" in url:
        scheme, rest = url.split("://", 1)
        scheme = scheme.lower()
        if scheme not in ("https", "ssh", "file"):
            raise Refused(f"REPO={url!r}: {scheme}:// isn't accepted; use https://, ssh:// or file://.")
        if scheme == "file":
            if not rest.startswith("/"):
                raise Refused(f"REPO={url!r}: use file:///absolute/path (no host).")
            form, user, host, port, path = "local", None, "", None, rest
        else:
            parts = urlsplit(url)
            if parts.password is not None:
                raise Refused("REPO= must not carry a password; use an SSH key or a credential helper.")
            netloc = parts.netloc.rpartition("@")[2]
            host_text, _, port_text = netloc.partition(":")
            if port_text and not (port_text.isdigit() and 0 < int(port_text) < 65536):
                raise Refused(f"REPO={url!r}: the port must be a number from 1 to 65535.")
            form, user, host, path = scheme, parts.username, host_text, parts.path
            port = int(port_text) if port_text else DEFAULT_PORT[scheme]
            if "@" in parts.netloc and not user:
                raise Refused(f"REPO={url!r}: empty user before @.")
    elif url.startswith("/"):
        form, user, host, port, path = "local", None, "", None, url
    else:
        match = _SCP.fullmatch(url)
        if not match:
            raise Refused(f"REPO={url!r} is not a git URL: use https://host/owner/repo, ssh://..., "
                          "git@host:owner/repo or an absolute path.")
        form, user, host, port, path = "scp", match["user"], match["host"], DEFAULT_PORT["ssh"], match["path"]
    if form != "local":
        if not _HOST.fullmatch(host or ""):
            raise Refused(f"REPO={url!r}: the host must be a plain host name (letters, digits, . and -, "
                          "not starting with -).")
        if user is not None and not _USER.fullmatch(user):
            raise Refused(f"REPO={url!r}: the user must be a plain name (letters, digits, . _ -, "
                          "not starting with -).")
    if not path.strip("/"):
        raise Refused(f"REPO={url!r} has no repository path.")
    if any(part.startswith("-") for part in path.split("/")):
        raise Refused(f"REPO={url!r}: no path component may start with -.")
    path = path.rstrip("/").removesuffix(".git").rstrip("/")
    return Url(form, user, host.lower(), port, path)


def validate(repo: str) -> None:
    """Refuse anything but https://, ssh://, file://, scp-like and absolute local paths.

    Hosts, users and path components never start with - (an ssh option such as
    -oProxyCommand), and only conservative host and user characters pass.
    file:// and absolute paths aren't checked to be repos: setup never
    contacts the remote.
    """
    if any(ord(c) < 32 or ord(c) == 127 for c in repo):
        raise Refused("REPO= contains a control character (a newline?); pass one plain git URL.")
    if not _SAFE_CHARS.fullmatch(repo):
        raise Refused(f"REPO={repo!r} has characters a git URL here never needs (spaces, quotes, $, `, ;, |, &, "
                      "...); pass one plain https://, ssh:// or git@host:owner/repo URL.")
    if re.match(r"[^/]*::", repo):
        raise Refused(f"REPO={repo!r} names a git remote helper (<transport>::...); "
                      "only https://, ssh://, file://, git@host:path and absolute paths are accepted.")
    parse(repo)


def same_repo(a: str, b: str) -> bool:
    """True only when a and b surely name one repo; any doubt is False (setup then refuses).

    Same form (scp, ssh://, https://, local): host, the exact path (a leading /
    matters: git@host:/abs and git@host:abs differ), user (both missing, or
    equal) and port (a missing port is the scheme's default: 22 for ssh and
    scp-like, 443 for https) must all match; file:///p and /p are one local
    form. Different forms match only on a HOSTED provider, for the same
    repo-relative path: https with no user on 443, ssh:// and scp as git@ on 22
    (scp without a leading /). Every other host keeps its forms apart.
    """
    try:
        x, y = parse(a.strip()), parse(b.strip())
    except Refused:
        return False
    if x.form == y.form:
        return (x.host, x.path, x.user, x.port) == (y.host, y.path, y.user, y.port)
    if x.host != y.host or x.host not in HOSTED or "local" in (x.form, y.form):
        return False

    def repo_path(u: Url) -> str | None:
        if u.form == "https":
            return u.path.lstrip("/") if u.user is None and u.port == 443 and u.path.startswith("/") else None
        if u.user != "git" or u.port != 22:
            return None
        if u.form == "ssh":
            return u.path[1:] if u.path.startswith("/") else None
        return None if u.path.startswith("/") else u.path

    px, py = repo_path(x), repo_path(y)
    return px is not None and px == py


def plan(remotes: dict[str, str] | None, repo: str, *, auto: bool = True) -> list[tuple[str, ...]]:
    """The git steps setup takes for ``remotes`` (None outside git); raises Refused, changing nothing."""
    if repo:
        validate(repo)
        if _is_upstream(repo):
            raise Refused("REPO= is GenieRails itself; pass the URL of your own (empty) repo. "
                          "GenieRails stays reachable as the genierails remote.")
        if remotes is None:
            return [("init",), ("add", "origin", repo)]
        origin = remotes.get("origin")
        if origin is None:
            return [("add", "origin", repo)]
        if same_repo(origin, repo):
            return []
        if _is_upstream(origin) and UPSTREAM_REMOTE not in remotes:
            return [("rename", "origin", UPSTREAM_REMOTE), ("add", "origin", repo)]
        if _is_upstream(origin):
            raise Refused(f"origin is GenieRails and a {UPSTREAM_REMOTE} remote already exists "
                          f"({remotes[UPSTREAM_REMOTE]}); setup won't guess which to keep. Either:\n"
                          f"         git remote remove origin   (keeps {UPSTREAM_REMOTE}), then re-run with REPO=, or\n"
                          f"         git remote set-url origin {repo}")
        raise Refused(f"origin already points at {origin}, not {repo}; make setup never repoints a remote. "
                      f"Re-run with that REPO (or none), or, to move this checkout to {repo} on purpose:\n"
                      f"         git remote set-url origin {repo}")
    if auto and remotes is not None and set(remotes) == {"origin"} and _is_upstream(remotes["origin"]):
        return [("rename", "origin", UPSTREAM_REMOTE)]
    return []


def _git_env() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if k not in _GIT_LOCATION_VARS}


def _run(cwd: Path, *args: str) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(["git", "-C", str(cwd), *args], text=True, capture_output=True, env=_git_env())
    except OSError as exc:
        raise Refused(f"git is not installed ({exc}); option A needs only the git binary.") from exc


def _git(cwd: Path, *args: str) -> None:
    proc = _run(cwd, *args)
    if proc.returncode:
        raise Refused(f"git {' '.join(args)} failed: {(proc.stderr or proc.stdout).strip()}")


def shared_git_dir(root: Path) -> Path | None:
    """The common git dir when root is a linked worktree (or a copy of its .git file), else None.

    Both dirs are resolved against root: git before 2.31 (no --path-format)
    may print them relative to it.
    """
    proc = _run(root, "rev-parse", "--git-dir", "--git-common-dir")
    dirs = proc.stdout.splitlines()
    if proc.returncode or len(dirs) != 2:
        raise Refused(f"git can't tell whether {root} is a linked worktree: {(proc.stderr or proc.stdout).strip()}")
    git_dir, common = ((root / d).resolve() for d in dirs)
    return None if git_dir == common else common


def checkout(cloud_dir: Path) -> tuple[Path, dict[str, str] | None, str]:
    """(checkout root, its remotes or None outside git, why it's unusable or "")."""
    root = cloud_dir.resolve().parent
    proc = _run(cloud_dir, "rev-parse", "--show-toplevel")
    if proc.returncode:
        top = None
    else:
        top = Path(proc.stdout.strip()).resolve()
    if top is not None and top != root:
        return root, None, (f"{cloud_dir.resolve()} is inside the git repo at {top}, not a GenieRails checkout of "
                            f"its own (expected the repo root at {root}); setup won't change that repo's remotes. "
                            "Clone GenieRails on its own, or manage remotes yourself.")
    if not (root / "shared").is_dir():
        return root, None, (f"{root} doesn't hold GenieRails' shared/ next to {cloud_dir.name}/; "
                            "setup manages git only at a GenieRails checkout root.")
    if top is None:
        return root, None, ""
    remotes: dict[str, str] = {}
    for parts in (line.split() for line in _run(root, "remote", "-v").stdout.splitlines()):
        if len(parts) >= 2:
            remotes.setdefault(parts[0], parts[1])
    return root, remotes, ""


def apply(root: Path, steps: list[tuple[str, ...]]) -> list[str]:
    done = []
    for step in steps:
        if step[0] == "init":
            _git(root, "init", "-q")
            _git(root, "symbolic-ref", "HEAD", f"refs/heads/{BRANCH}")
            done.append(f"initialised a git repo in {root} (branch {BRANCH})")
        elif step[0] == "rename":
            _git(root, "remote", "rename", step[1], step[2])
            done.append(f"renamed remote {step[1]} -> {step[2]} (GenieRails, kept for upgrades)")
        else:
            _git(root, "remote", "add", step[1], step[2])
            done.append(f"added remote {step[1]} = {step[2]}")
    return done


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cloud-dir", required=True, type=Path)
    parser.add_argument("--gitignore", default="", help="make setup's GITIGNORE; off skips the no-REPO rename")
    parser.add_argument("--env", default="dev")
    parser.add_argument("--check", action="store_true", help="refuse or pass; change nothing")
    args = parser.parse_args(argv)
    repo = os.environ.get(REPO_ENV, "")
    try:
        root, remotes, unusable = checkout(args.cloud_dir)
        if unusable:
            if repo:
                raise Refused(unusable)
            return 0  # no REPO: nothing to do outside a GenieRails checkout root
        common = shared_git_dir(root) if remotes is not None else None
        if common is not None and repo:
            raise Refused(f"{root} is a linked git worktree: its remotes live in {common}, shared by every "
                          "worktree of that repo, so setup won't change them. Run make setup REPO= in a clone "
                          "of its own, or manage remotes yourself.")
        auto = args.gitignore != "off" and not in_ci()
        steps = plan(remotes, repo, auto=auto and common is None)
        if args.check:
            return 0
        done = apply(root, steps)
    except Refused as exc:
        print(f"ERROR: make setup REPO=: {exc}" if repo else f"ERROR: make setup: {exc}", file=sys.stderr)
        if repo:
            print(f"       {MANUAL}", file=sys.stderr)
        return 1
    for line in done:
        print(f"Git: {line}")
    if (auto and not repo and not steps and remotes and _is_upstream(remotes.get("origin"))
            and UPSTREAM_REMOTE not in remotes):
        why = (f"a linked worktree, its remotes shared through {common}" if common is not None else
               f"not a fresh clone: origin isn't its only remote ({', '.join(sorted(remotes))})")
        print(f"Git: left origin (GenieRails) as it is: {why}.")
    if repo and not steps:
        print(f"Git: origin is already {remotes.get('origin')}; nothing to change.")
    elif done and not repo:
        print(f"Git: this is now a local-only repo (no origin): commit envs/ here; nothing is pushed. "
              f"Add your own repo later with make setup ENV={args.env} REPO=<url>. "
              f"Undo: git remote rename {UPSTREAM_REMOTE} origin")
    elif done:
        print("Git: origin is ready; setup may offer to commit and push its safe files at the end.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
