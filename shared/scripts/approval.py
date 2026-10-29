#!/usr/bin/env python3
"""y/N approvals typed at a terminal, and the write-once records that say who typed them.

A ``y`` typed by a person at a terminal is the approval of one account or
release manifest (owner decision, 2026-10-26). Owner and reader acks are
never a prompt: they stay a committed envs/<env>/ack.txt change.

What a record is and isn't. It is **audit**, not a gate: the primitives
(``make apply-account``, ``make release MANIFEST=``) still apply with no
record, because approval can happen outside GenieRails (a pipeline's approval
step, a reviewer). Their receipts then say ``{"via": "external"}``. A forged
record forges only that audit trail; it never bypasses a check, since every
check stays in the primitives. A record is honest about being self-reported:
the OS user name and the git identity of the person at the terminal.

Nothing approves without a person at a terminal: there is no flag, variable or
answer file that says yes. ``interactive`` is true only when all of these hold:

- the caller is the top-level make's recipe (``MAKELEVEL`` is ``1``);
- it isn't CI (scripts/ci_guard.py; ``GENIERAILS_ALLOW_CI_APPLY=1`` counts as
  CI here: an integration workflow never prompts; a broken ci_guard counts as CI);
- stdin **and** stdout are terminals (so ``printf 'y\\n' |``, ``| tee`` and
  captured output never prompt);
- ``GENIERAILS_INTERACTIVE`` isn't ``0``. It can only turn prompting off: no
  value turns it on.

Only ``y`` or ``yes`` (any case, surrounding blanks ignored) is yes. EOF, an
empty line or anything else is no. Ctrl-C at the prompt exits 130 and writes
nothing.

Records live at ``<state dir>/<env>/approvals/<manifest id>-<stamp>.json``,
written once (O_EXCL, never through a symlink, 0600), one per y: the stamp lets
a manifest offered again after an aborted apply get a second record.
``check`` ties a record to the manifest as it is now (id, sha256, commit, env,
host, approved after the manifest was written); a receipt embeds what it
returns.

Only this run's own prompt counts. The flow that asks (the consumer, e.g.
scripts/release_loop.py) creates a per-run nonce (new_run) and exports it as
GENIERAILS_APPROVAL_RUN to the nested makes it runs; a record holds the
nonce's sha256, and check counts a record only if it holds the current run's,
is a regular file (never a symlink), is owned by this user and is mode 0600.
A record left by an earlier run, copied in, or typed as APPROVAL= with a
forged MAKELEVEL/MAKEFLAGS never counts: without the nonce in the environment
the Makefile ignores APPROVAL=, and with another run's nonce check refuses.
The nonce narrows which records count; it can never turn prompting on.

Commands. Each answers with exactly one versioned line (ANSWERS) and the exit
code that goes with it; callers accept only an exact (exit code, line) pair, as
for scripts/ci_guard.py. The line goes to stdout, or with ``--answer-file F``
to F (an existing empty regular file, e.g. from mktemp), so that a recipe can
read the answer while stdin and stdout stay the terminal. ``approve`` answers
``approved:<record path>``.
  interactive                                           INTERACTIVE or NOT_INTERACTIVE, both exit 0
  ask --question Q                                      YES (0), NO (1) or REFUSED (1: not interactive)
  approve --env-dir E --env N --manifest M --question Q [--round K]
                                                        asks, and on y writes the record of the RUN_ENV run:
                                                        APPROVED:<path> (0), NO (1) or REFUSED (1; no run either)
  check --env-dir E --env N --manifest M --approval A   VALID (0) or INVALID (1, the reason on stderr; a record
                                                        of another run, or with no RUN_ENV set, is invalid)
Ctrl-C at a prompt exits 130 with no answer. An internal error prints no answer and exits 3.
There is deliberately no command that writes a record without asking.
"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import secrets
import socket
import stat
import subprocess
import sys
from pathlib import Path
from typing import IO, Callable, Mapping

SHARED = Path(__file__).resolve().parents[1]
for _path in (SHARED, SHARED / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

KIND = "approval"
VERSION = 1
VIA_TERMINAL = "terminal"
EXTERNAL = {"via": "external"}
MANIFEST_KINDS = ("account-manifest", "release-manifest")
APPROVALS_DIR = "approvals"
INTERACTIVE_ENV = "GENIERAILS_INTERACTIVE"
RUN_ENV = "GENIERAILS_APPROVAL_RUN"  # the consumer's per-run nonce (module docstring)
SUFFIX = "[y/N]"
YES_ANSWERS = ("y", "yes")
ERROR = 3
INTERRUPTED = 130
_PREFIX = "genierails-approval:v1:"
INTERACTIVE = _PREFIX + "interactive"
NOT_INTERACTIVE = _PREFIX + "not-interactive"
YES = _PREFIX + "yes"
NO = _PREFIX + "no"
REFUSED = _PREFIX + "refused"
APPROVED = _PREFIX + "approved"  # approve answers APPROVED + ":" + the record's path
VALID = _PREFIX + "valid"
INVALID = _PREFIX + "invalid"
ANSWERS = {"interactive": {(0, INTERACTIVE), (0, NOT_INTERACTIVE)}, "ask": {(0, YES), (1, NO), (1, REFUSED)},
           "approve": {(0, APPROVED), (1, NO), (1, REFUSED)}, "check": {(0, VALID), (1, INVALID)}}
NOT_A_TERMINAL = (
    "no person at a terminal to approve this (it needs the top-level make, outside CI, with stdin and stdout "
    "a terminal, and GENIERAILS_INTERACTIVE not 0). Nothing was approved.")


class Refused(RuntimeError):
    pass


def _in_ci(environ: Mapping[str, str]) -> bool:
    """CI, or the CI apply opt-out (an integration workflow never prompts); fail closed."""
    try:
        from scripts import ci_guard

        return ci_guard.status_line(environ, allow_opt_out=False) != ci_guard.STATUS_NOT_CI or ci_guard.opted_out(
            environ)
    except Exception:  # a missing or failing CI check counts as CI
        return True


def _isatty(stream) -> bool:
    try:
        return bool(stream is not None and stream.isatty())
    except (AttributeError, ValueError, OSError):
        return False


def interactive(environ: Mapping[str, str] | None = None, stdin: IO | None = None, stdout: IO | None = None) -> bool:
    """Whether a person at a terminal can be asked (module docstring); every condition must hold."""
    environ = os.environ if environ is None else environ
    stdin = sys.stdin if stdin is None else stdin
    stdout = sys.stdout if stdout is None else stdout
    return (environ.get("MAKELEVEL") == "1" and environ.get(INTERACTIVE_ENV) != "0" and not _in_ci(environ)
            and _isatty(stdin) and _isatty(stdout))


def prompt(question: str) -> str:
    """The exact text shown: the question and ``[y/N]``."""
    question = question.strip()
    return f"{question} " if question.endswith(SUFFIX) else f"{question} {SUFFIX} "


def is_yes(answer: str) -> bool:
    return answer.strip().lower() in YES_ANSWERS


def ask(question: str, *, environ: Mapping[str, str] | None = None, stdin: IO | None = None,
        stdout: IO | None = None, read: Callable[[str], str] = input) -> bool:
    """Ask a person at the terminal; True only for y/yes. Refuses when no one can be asked.

    EOF or any other answer is no. Ctrl-C exits 130 (SystemExit) having written nothing.
    """
    if not interactive(environ, stdin, stdout):
        raise Refused(NOT_A_TERMINAL)
    try:
        return is_yes(read(prompt(question)))
    except EOFError:
        print(file=sys.stderr)
        return False
    except KeyboardInterrupt:
        print("\nInterrupted: nothing was approved.", file=sys.stderr)
        raise SystemExit(INTERRUPTED) from None


def new_run() -> str:
    """A fresh per-run nonce; the consumer exports it as RUN_ENV for the makes it runs."""
    return secrets.token_hex(16)


def run_of(environ: Mapping[str, str] | None = None) -> str:
    """The current run's nonce (RUN_ENV); refuses when none, or a malformed one, is set."""
    run = (os.environ if environ is None else environ).get(RUN_ENV, "")
    if len(run) != 32 or any(c not in "0123456789abcdef" for c in run):
        raise Refused(f"no approval run ({RUN_ENV}): only a record written by the current run's own prompt counts")
    return run


def run_sha256(run: str) -> str:
    return hashlib.sha256(run.encode()).hexdigest()


def owned_private_file(path: Path) -> None:
    """Refuse unless ``path`` is a regular file (not a symlink), owned by this user, mode 0600."""
    try:
        info = os.lstat(path)
    except OSError as exc:
        raise Refused(f"approval record {path} can't be read ({exc.strerror})") from exc
    if not stat.S_ISREG(info.st_mode):
        raise Refused(f"approval record {path} is not a regular file (a symlink or something else)")
    if info.st_uid != os.getuid():
        raise Refused(f"approval record {path} is owned by uid {info.st_uid}, not this user ({os.getuid()})")
    if stat.S_IMODE(info.st_mode) != 0o600:
        raise Refused(f"approval record {path} is mode {stat.S_IMODE(info.st_mode):o}, not 600 (as it was written)")


def _load_manifest(env_dir: Path, env: str, manifest_path: Path) -> dict:
    from scripts import release_gate as rg

    data = rg._read_json(Path(manifest_path))
    kind = (data or {}).get("kind")
    if kind == "account-manifest":
        return rg.load_account_manifest(Path(manifest_path), env)
    if kind == "release-manifest":
        return rg.load_manifest(Path(manifest_path), env)
    raise Refused(f"{manifest_path} is not an account or release manifest")


def approvals_dir(env_dir: Path, env: str) -> Path:
    from scripts import release_gate as rg

    return rg.env_state(rg.cloud_dir_of(Path(env_dir)), env) / APPROVALS_DIR


def _private_dir(path: Path) -> Path:
    """The approvals dir: created 0700, never a symlink."""
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        pass
    info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise Refused(f"{path} is not a directory (a symlink or a file); refusing to write an approval record there")
    return path


def _git_user(env_dir: Path) -> dict:
    def value(key: str) -> str:
        try:
            return subprocess.run(["git", "-C", str(env_dir), "config", key], capture_output=True, text=True,
                                  timeout=30).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return ""

    return {"name": value("user.name"), "email": value("user.email")}


def _tty() -> str:
    try:
        return os.ttyname(0)
    except OSError:
        return ""


def _user() -> str:
    try:
        return getpass.getuser()
    except Exception:
        return ""


def record(env_dir: Path, env: str, manifest_path: Path, question: str, *, run: str, round_no: int | None = None,
           environ: Mapping[str, str] | None = None, stdin: IO | None = None, stdout: IO | None = None) -> Path:
    """Write the write-once record of a y just typed at this terminal for ``manifest_path``, in run ``run``.

    Called only after ask() returned True; it refuses, too, when no one is at
    a terminal, so no code path writes one for a non-interactive run.
    """
    from scripts import release_gate as rg

    if not interactive(environ, stdin, stdout):
        raise Refused(NOT_A_TERMINAL)
    run = run_of({RUN_ENV: run})
    env_dir, manifest_path = Path(env_dir), Path(manifest_path).resolve()
    manifest = _load_manifest(env_dir, env, manifest_path)
    approved_at = rg._now()
    payload = {
        "kind": KIND, "version": VERSION, "env": env, "manifest_kind": manifest["kind"],
        "manifest_id": manifest["id"], "manifest_path": str(manifest_path),
        "manifest_sha256": rg.sha256_file(manifest_path), "commit": manifest["commit"],
        "via": VIA_TERMINAL, "user": _user(), "git_user": _git_user(env_dir), "host": socket.gethostname(),
        "tty": _tty(), "approved_at": approved_at, "question": prompt(question), "round": round_no,
        "run_sha256": run_sha256(run),
    }
    directory = _private_dir(approvals_dir(env_dir, env))
    stamp = approved_at.replace(":", "").replace(".", "")
    path = directory / f"{manifest['id']}-{stamp}.json"
    try:
        # O_EXCL never follows or replaces anything at the path, a dangling symlink included.
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    except FileExistsError as exc:
        raise Refused(f"{path} already exists; approval records are never overwritten") from exc
    with os.fdopen(fd, "w") as handle:
        os.fchmod(handle.fileno(), 0o600)
        handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


def check(env_dir: Path, env: str, manifest_path: Path, approval_path: Path, *, run: str | None = None) -> dict:
    """The record at ``approval_path``, as a receipt embeds it, if it approves ``manifest_path`` as it is now.

    Refuses unless it is a regular 0600 file of this user's in this env's
    approvals dir, of this kind and version, via the terminal, written by
    run ``run`` (default: RUN_ENV), for this env, naming this manifest's kind,
    id, sha256 and commit, written on this host, at or after the manifest was
    written.
    """
    from scripts import release_gate as rg

    env_dir, manifest_path, approval_path = Path(env_dir), Path(manifest_path).resolve(), Path(approval_path)
    if not approval_path.is_absolute():
        raise Refused(f"approval record {approval_path} is not an absolute path")
    run = run_of(None if run is None else {RUN_ENV: run})
    directory = approvals_dir(env_dir, env)
    if os.path.realpath(approval_path.parent) != os.path.realpath(directory):
        raise Refused(f"approval record {approval_path} is not in {directory}")
    owned_private_file(approval_path)
    data = rg._read_json(approval_path)
    if data is None or data.get("kind") != KIND or data.get("version") != VERSION:
        raise Refused(f"{approval_path} is not a version {VERSION} approval record")
    manifest = _load_manifest(env_dir, env, manifest_path)
    expected = {"env": env, "via": VIA_TERMINAL, "manifest_kind": manifest["kind"], "manifest_id": manifest["id"],
                "manifest_sha256": rg.sha256_file(manifest_path), "commit": manifest["commit"],
                "host": socket.gethostname()}
    wrong = sorted(key for key, value in expected.items() if data.get(key) != value)
    if wrong:
        raise Refused(f"approval record {approval_path} doesn't approve {manifest_path} as it is now "
                      f"({', '.join(wrong)} differ)")
    if data.get("run_sha256") != run_sha256(run):
        raise Refused(f"approval record {approval_path} was not written by this run's own prompt")
    if not isinstance(data.get("approved_at"), str) or data["approved_at"] < str(manifest.get("created_at") or ""):
        raise Refused(f"approval record {approval_path} predates the manifest {manifest_path}")
    return dict(data) | {"record": str(approval_path), "record_sha256": rg.sha256_file(approval_path)}


def embed(env_dir: Path, env: str, manifest_path: Path, approval_path: Path | None) -> dict:
    """A receipt's ``approval`` field: the checked record (of the RUN_ENV run), or EXTERNAL when none was given."""
    return dict(EXTERNAL) if approval_path is None else check(env_dir, env, manifest_path, approval_path)


# -- CLI -----------------------------------------------------------------------------

def _answer(line: str, answer_file: Path | None) -> None:
    if answer_file is None:
        print(line, flush=True)
        return
    fd = os.open(answer_file, os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "w") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode) or os.fstat(handle.fileno()).st_size:
            raise Refused(f"answer file {answer_file} must be an existing empty regular file")
        handle.write(line + "\n")


def _parser() -> argparse.ArgumentParser:
    # No abbreviations: --answer=y must never be read as an option (--answer-file=y).
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0], allow_abbrev=False)
    sub = parser.add_subparsers(dest="command", required=True)

    def command(name: str) -> argparse.ArgumentParser:
        cmd = sub.add_parser(name, allow_abbrev=False)
        cmd.add_argument("--answer-file", type=Path, default=None)
        return cmd

    command("interactive")
    command("ask").add_argument("--question", required=True)
    for name in ("approve", "check"):
        cmd = command(name)
        cmd.add_argument("--env-dir", type=Path, required=True)
        cmd.add_argument("--env", required=True)
        cmd.add_argument("--manifest", type=Path, required=True)
    approve = sub.choices["approve"]
    approve.add_argument("--question", required=True)
    approve.add_argument("--round", type=int, default=None)
    sub.choices["check"].add_argument("--approval", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return _run(args)
    except SystemExit:
        raise
    except Exception as exc:  # fail closed: an error is never an approval
        print(f"ERROR: scripts/approval.py {args.command} failed ({type(exc).__name__}: {exc}); nothing was "
              "approved.", file=sys.stderr)
        return ERROR


def _run(args) -> int:
    if args.command == "interactive":
        _answer(INTERACTIVE if interactive() else NOT_INTERACTIVE, args.answer_file)
        return 0
    if args.command == "check":
        try:
            check(args.env_dir, args.env, args.manifest, args.approval)
        except Refused as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            _answer(INVALID, args.answer_file)
            return 1
        _answer(VALID, args.answer_file)
        return 0
    if args.command == "approve":
        _load_manifest(args.env_dir, args.env, args.manifest)  # refuse an unreadable manifest before asking
    try:
        if args.command == "approve":
            run_of()  # no run to record it in: refuse before asking
        yes = ask(args.question)
    except Refused as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        _answer(REFUSED, args.answer_file)
        return 1
    if not yes:
        _answer(NO, args.answer_file)
        return 1
    if args.command == "ask":
        _answer(YES, args.answer_file)
        return 0
    path = record(args.env_dir, args.env, args.manifest, args.question, run=os.environ.get(RUN_ENV, ""),
                  round_no=args.round)
    _answer(f"{APPROVED}:{path}", args.answer_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
