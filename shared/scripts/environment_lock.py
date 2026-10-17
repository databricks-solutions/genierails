#!/usr/bin/env python3
"""Exclusive, stale-owner-aware per-environment governance lock.

The lock file (envs/<env>/generated/.governance.lock, or outside envs/ with
GENIERAILS_STATE_DIR or remote state: lock_path; mode 0600) records the
holder's pid and that process's start time, and a random nonce. ``lock``
prints the nonce; make's _ENV_LOCK exports it to every child as
GENIERAILS_LOCK_NONCE_<ENV> (``nonce_var``). ``held_by_ancestor`` (the
side-door gate of scripts/release_gate.py) counts a lock only when the
inherited nonce matches, the recorded pid is an ancestor, and that pid still
has the recorded start time. A leftover, copied or hand-written lock file, or
a pid reused since, is therefore never a pass. This guards against stale
files and accidental misuse, not a deliberate forger on the deployment
machine, who could run Terraform directly anyway.
"""

import argparse
import errno
import hmac
import secrets
import fcntl
import json
import os
import socket
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

LOCK_RELPATH = Path("generated/.governance.lock")

def lock_path(env_dir: Path, state_dir: str | None = None) -> Path:
    """The env's lock file. ``state_dir`` None reads GENIERAILS_STATE_DIR; "" is the layout without it.

    Without a state dir the lock is envs/<env>/generated/.governance.lock, or
    <cloud>/.genierails-state/<env>/locks/ for an env with remote_state.hcl
    (scripts/state_layout.py), which never writes runtime files under envs/.
    """
    if state_dir is None:
        base = os.environ.get("GENIERAILS_STATE_DIR", "").strip()
        if not base and (Path(env_dir) / ".state-moved").exists():
            raise RuntimeError(f"state was moved to GENIERAILS_STATE_DIR ({Path(env_dir) / '.state-moved'}); "
                               "set it before running")
    else:
        base = state_dir
    cloud = Path(env_dir).resolve().parent.parent
    if base:
        # Never create a mistyped or unmounted state dir by taking a lock in it.
        if not (Path(base).expanduser().is_dir() and os.access(Path(base).expanduser(), os.R_OK | os.X_OK)):
            raise RuntimeError(f"GENIERAILS_STATE_DIR={base} does not exist or can't be read; fix the path "
                               "(a new one is created by make migrate-state-dir)")
        return Path(base).expanduser().resolve() / cloud.name / Path(env_dir).name / "locks" / ".governance.lock"
    if (Path(env_dir) / "remote_state.hcl").is_file():
        return cloud / ".genierails-state" / Path(env_dir).name / "locks" / ".governance.lock"
    return Path(env_dir) / LOCK_RELPATH


@contextmanager
def _guard(env_dir: Path, state_dir: str | None = None):
    path = lock_path(env_dir, state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.parent.name == "locks":
        os.chmod(path.parent.parent, 0o700)
    with open(path.with_suffix(".guard"), "a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _read_lock(path: Path):
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if (not isinstance(data, dict) or not isinstance(data.get("pid"), int)
            or isinstance(data.get("pid"), bool) or data["pid"] <= 0
            or not isinstance(data.get("host"), str) or not data["host"]):
        return None
    return data


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _ancestors() -> set[int]:
    import subprocess

    pids, pid = set(), os.getpid()
    while pid > 1 and pid not in pids:
        pids.add(pid)
        out = subprocess.run(["ps", "-o", "ppid=", "-p", str(pid)], text=True, capture_output=True, check=False)
        try:
            pid = int(out.stdout.strip())
        except ValueError:
            break
    return pids


def nonce_var(env_dir: Path) -> str:
    """The variable a lock holder exports its nonce in (the env dir's name, shell-safe)."""
    return "GENIERAILS_LOCK_NONCE_" + Path(env_dir).name.upper().replace("-", "_")


def process_start(pid: int) -> str:
    """The start time ps reports for ``pid`` ("" when there is no such process)."""
    import subprocess

    out = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], text=True, capture_output=True, check=False)
    return " ".join(out.stdout.split()) if out.returncode == 0 else ""


def held_by_ancestor(env_dir: Path) -> str | None:
    """The owner (make target) of the env lock when a process this one runs under took it.

    All of: the nonce this process inherited matches the lock's, the recorded
    pid is an ancestor on this host, and that pid's start time is the one
    recorded when the lock was taken. Anything missing means not held.
    """
    held = _read_lock(lock_path(env_dir))
    inherited = os.environ.get(nonce_var(env_dir), "")
    if not held or not inherited or not isinstance(held.get("nonce"), str) or not isinstance(held.get("pid_start"), str):
        return None
    if not hmac.compare_digest(held["nonce"], inherited):
        return None
    if held["host"] != socket.gethostname() or held["pid"] not in _ancestors():
        return None
    if not held["pid_start"] or process_start(held["pid"]) != held["pid_start"]:
        return None
    return str(held.get("owner") or "?")


def owns_lock(env_dir: Path, pid: int, state_dir: str | None = None) -> bool:
    held = _read_lock(lock_path(env_dir, state_dir))
    return bool(held) and held["pid"] == pid and held["host"] == socket.gethostname()


def acquire_lock(env_dir: Path, pid: int, owner: str):
    ok, message, _nonce = acquire(env_dir, pid, owner)
    return ok, message


def acquire(env_dir: Path, pid: int, owner: str, state_dir: str | None = None):
    """Take the lock for ``pid``: (ok, message, nonce); the holder exports the nonce (nonce_var)."""
    path = lock_path(env_dir, state_dir)
    host = socket.gethostname()
    with _guard(env_dir, state_dir):
        note = ""
        if path.exists() or path.is_symlink():
            held = _read_lock(path)
            if held is None:
                return False, f"cannot determine the owner of {path}; delete it manually if no release/maintain is running", ""
            if held["host"] == host and not _pid_alive(held["pid"]):
                note = f"removed stale lock from dead pid {held['pid']} (make {held.get('owner', '?')})"
                path.unlink()
            else:
                return False, (f"{path} is held by make {held.get('owner', '?')} "
                               f"(pid {held['pid']} on {held['host']}, since {held.get('started_at', '?')}); wait for it to finish"), ""
        nonce = secrets.token_hex(32)
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                return False, f"{path} was taken concurrently; retry", ""
            raise
        os.fchmod(fd, 0o600)  # whatever the umask
        with os.fdopen(fd, "w") as handle:
            json.dump({"pid": pid, "host": host, "owner": owner, "nonce": nonce, "pid_start": process_start(pid),
                       "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        return True, note, nonce


def release_lock(env_dir: Path, pid: int, state_dir: str | None = None) -> None:
    path = lock_path(env_dir, state_dir)
    if not path.parent.is_dir():
        return
    with _guard(env_dir, state_dir):
        if owns_lock(env_dir, pid, state_dir):
            path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("lock", "unlock"))
    parser.add_argument("env_dir", type=Path)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--owner", default="?")
    parser.add_argument("--env", default="")
    args = parser.parse_args()
    if args.command == "unlock":
        release_lock(args.env_dir, args.pid)
        return 0
    ok, message, nonce = acquire(args.env_dir, args.pid, args.owner)
    if not ok:
        print(f"{args.owner}: {args.env or args.env_dir.name} is locked: {message}", file=sys.stderr)
        return 1
    if message:
        print(f"=== Governance lock: {message} ===", file=sys.stderr)
    print(nonce)  # make's _ENV_LOCK exports it to its children (nonce_var)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
