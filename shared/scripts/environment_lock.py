#!/usr/bin/env python3
"""Exclusive, stale-owner-aware per-environment governance lock."""

import argparse
import errno
import fcntl
import json
import os
import socket
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

LOCK_RELPATH = Path("generated/.governance.lock")


@contextmanager
def _guard(env_dir: Path):
    path = env_dir / LOCK_RELPATH
    path.parent.mkdir(parents=True, exist_ok=True)
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


def owns_lock(env_dir: Path, pid: int) -> bool:
    held = _read_lock(env_dir / LOCK_RELPATH)
    return bool(held) and held["pid"] == pid and held["host"] == socket.gethostname()


def acquire_lock(env_dir: Path, pid: int, owner: str):
    path = env_dir / LOCK_RELPATH
    host = socket.gethostname()
    with _guard(env_dir):
        note = ""
        if path.exists() or path.is_symlink():
            held = _read_lock(path)
            if held is None:
                return False, f"cannot determine the owner of {path}; delete it manually if no release/maintain is running"
            if held["host"] == host and not _pid_alive(held["pid"]):
                note = f"removed stale lock from dead pid {held['pid']} (make {held.get('owner', '?')})"
                path.unlink()
            else:
                return False, (f"{path} is held by make {held.get('owner', '?')} "
                               f"(pid {held['pid']} on {held['host']}, since {held.get('started_at', '?')}); wait for it to finish")
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                return False, f"{path} was taken concurrently; retry"
            raise
        with os.fdopen(fd, "w") as handle:
            json.dump({"pid": pid, "host": host, "owner": owner,
                       "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        return True, note


def release_lock(env_dir: Path, pid: int) -> None:
    path = env_dir / LOCK_RELPATH
    if not path.parent.is_dir():
        return
    with _guard(env_dir):
        if owns_lock(env_dir, pid):
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
    ok, message = acquire_lock(args.env_dir, args.pid, args.owner)
    if not ok:
        print(f"{args.owner}: {args.env or args.env_dir.name} is locked: {message}", file=sys.stderr)
        return 1
    if message:
        print(f"=== Governance lock: {message} ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
