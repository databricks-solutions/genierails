#!/usr/bin/env python3
"""Certification receipt and per-environment lock for the exposure gate.

`make certify` / `make maintain` snapshot the enforcement inputs right before
the coverage gate runs, and write envs/<env>/generated/.certified.json from
that snapshot only if the inputs are still identical after the governance
apply. `make release` refuses to open business_access_enabled unless the
receipt still matches the current inputs, before and after its apply. All three
hold an exclusive per-environment lock for their whole pipeline.

Fingerprinted inputs (symlinks resolved; content hashes, not mtimes):
  - the generated config the coverage gate reads (generated/abac.auto.tfvars,
    generated/masking_functions.sql, ddl/_fetched.sql)
  - data_access/discovered_uc_tables.auto.tfvars and the resolved footprint
  - env.auto.tfvars, with business_access_enabled excluded (opening the gate
    must not invalidate the certification it depends on)
  - every repo file under shared/ that can affect the gate or enforcement
    (validation/derivation code, treatment/tag/function registries, country and
    industry overlays, Terraform roots/modules, scripts, Makefile.shared), so a
    dirty working tree cannot ride on a clean HEAD
The git commit is recorded for provenance only.

Commands (all take ENV_DIR):
  lock / unlock     acquire / release the env lock (--pid, --owner)
  clear             delete the receipt and any pending snapshot
  snapshot          record the gate-time input snapshot
  verify-snapshot   fail if inputs changed since the snapshot
  commit            write the receipt from the snapshot if inputs are unchanged
  check             exit 0 if the receipt is current, 1 otherwise
  warn              loud WARNING if the gate is open without a current receipt
  open-gate         persist business_access_enabled = true (atomically)
  release-failed    print exposure rollback guidance
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import hcl2

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from scripts.footprint import resolve_footprint  # noqa: E402

RECEIPT_VERSION = 2
RECEIPT_RELPATH = Path("generated") / ".certified.json"
PENDING_RELPATH = Path("generated") / ".certified.pending.json"
LOCK_RELPATH = Path("generated") / ".governance.lock"
GATE_KEY = "business_access_enabled"
GATE_LINE = re.compile(rf"^\s*{GATE_KEY}\s*=.*$", re.MULTILINE)
ENV_FILE_INPUTS = (
    "generated/abac.auto.tfvars",
    "generated/masking_functions.sql",
    "ddl/_fetched.sql",
    "data_access/discovered_uc_tables.auto.tfvars",
)
# Repo inputs: everything under shared/ except docs, tests, examples and
# non-executable text. Over-inclusion only costs a re-certify.
REPO_INPUT_ROOT = SHARED_ROOT
REPO_EXCLUDED_DIRS = {"tests", "docs", "examples", "__pycache__"}
REPO_EXCLUDED_SUFFIXES = {".md", ".example", ".pyc"}


class ReceiptError(Exception):
    """The inputs cannot be fingerprinted or the receipt cannot be trusted."""


# ── atomic file writes ────────────────────────────────────────────────────────


def atomic_write_text(path: Path, text: str, default_mode: int = 0o644) -> None:
    """Replace path's content atomically, keeping its mode; follows symlinks."""
    target = path.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        mode = target.stat().st_mode & 0o7777
    except FileNotFoundError:
        mode = default_mode
    fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, target)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    dir_fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


# ── fingerprint ───────────────────────────────────────────────────────────────


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _file_digest(path: Path) -> str | None:
    path = path.resolve()
    return _sha256(path.read_bytes()) if path.is_file() else None


def _env_config_digest(env_dir: Path) -> str | None:
    path = (env_dir / "env.auto.tfvars").resolve()
    if not path.is_file():
        return None
    text = path.read_text()
    try:
        config = hcl2.loads(text)
        config.pop(GATE_KEY, None)
        return _sha256(json.dumps(config, sort_keys=True, default=str).encode())
    except Exception:
        return _sha256(GATE_LINE.sub("", text).encode())


def repo_input_files(root: Path | None = None) -> list[Path]:
    root = root or REPO_INPUT_ROOT
    files = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames[:] = sorted(
            d for d in dirnames if d not in REPO_EXCLUDED_DIRS and not d.startswith(".")
        )
        for name in sorted(filenames):
            if name.startswith(".") or Path(name).suffix in REPO_EXCLUDED_SUFFIXES:
                continue
            files.append(Path(dirpath) / name)
    return files


def _git_commit() -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(SHARED_ROOT), "rev-parse", "HEAD"],
            capture_output=True, text=True, check=False,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def compute_components(env_dir: Path) -> dict[str, str | None]:
    components: dict[str, str | None] = {}
    for rel in ENV_FILE_INPUTS:
        components[rel] = _file_digest(env_dir / rel)
    try:
        footprint = resolve_footprint(env_dir)
    except Exception as exc:
        raise ReceiptError(f"cannot resolve footprint: {exc}") from exc
    components["footprint"] = _sha256(json.dumps(footprint).encode())
    components["env.auto.tfvars"] = _env_config_digest(env_dir)
    root = REPO_INPUT_ROOT
    for path in repo_input_files(root):
        components[f"repo:{path.relative_to(root).as_posix()}"] = _file_digest(path)
    return components


def fingerprint(components: dict) -> str:
    return _sha256(json.dumps(components, sort_keys=True).encode())


def _diff(recorded: dict, current: dict) -> list[str]:
    return sorted(k for k in set(recorded) | set(current) if recorded.get(k) != current.get(k))


def _describe(changed: list[str], limit: int = 6) -> str:
    shown = ", ".join(changed[:limit])
    more = f", +{len(changed) - limit} more" if len(changed) > limit else ""
    return f" (changed: {shown}{more})" if changed else ""


# ── receipt / snapshot ────────────────────────────────────────────────────────


def receipt_path(env_dir: Path) -> Path:
    return env_dir / RECEIPT_RELPATH


def pending_path(env_dir: Path) -> Path:
    return env_dir / PENDING_RELPATH


def clear_receipt(env_dir: Path) -> None:
    receipt_path(env_dir).unlink(missing_ok=True)
    pending_path(env_dir).unlink(missing_ok=True)


def write_snapshot(env_dir: Path) -> Path:
    components = compute_components(env_dir)
    snapshot = {
        "version": RECEIPT_VERSION,
        "taken_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "fingerprint": fingerprint(components),
        "components": components,
    }
    path = pending_path(env_dir)
    atomic_write_text(path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    return path


def _load_trusted(path: Path, label: str) -> dict:
    """Load a receipt/snapshot and check it is well-formed and self-consistent."""
    if not path.is_file():
        raise ReceiptError(f"no {label} at {path}")
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise ReceiptError(f"malformed {label} at {path}: {exc}") from exc
    if (
        not isinstance(data, dict)
        or data.get("version") != RECEIPT_VERSION
        or not isinstance(data.get("components"), dict)
        or not isinstance(data.get("fingerprint"), str)
    ):
        raise ReceiptError(f"malformed {label} at {path}")
    if fingerprint(data["components"]) != data["fingerprint"]:
        raise ReceiptError(f"{label} at {path} is not self-consistent (edited or forged)")
    return data


def verify_snapshot(env_dir: Path) -> None:
    snapshot = _load_trusted(pending_path(env_dir), "gate-time snapshot")
    current = compute_components(env_dir)
    if fingerprint(current) != snapshot["fingerprint"]:
        raise ReceiptError(
            "inputs changed after the coverage gate ran"
            + _describe(_diff(snapshot["components"], current))
        )


def commit_receipt(env_dir: Path, by: str) -> Path:
    """Write the receipt from the gate-time snapshot iff inputs are unchanged."""
    try:
        verify_snapshot(env_dir)
        snapshot = _load_trusted(pending_path(env_dir), "gate-time snapshot")
        receipt = {
            "version": RECEIPT_VERSION,
            "env": env_dir.name,
            "gated_at": snapshot["taken_at"],
            "certified_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "certified_by": by,
            "git_commit": _git_commit(),
            "fingerprint": snapshot["fingerprint"],
            "components": snapshot["components"],
        }
        path = receipt_path(env_dir)
        atomic_write_text(path, json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        return path
    finally:
        pending_path(env_dir).unlink(missing_ok=True)


def write_receipt(env_dir: Path, by: str) -> Path:
    """Snapshot and commit in one step (inputs are by definition unchanged)."""
    write_snapshot(env_dir)
    return commit_receipt(env_dir, by)


def check_receipt(env_dir: Path) -> tuple[bool, str]:
    """Return (current, reason)."""
    try:
        receipt = _load_trusted(receipt_path(env_dir), "certification receipt")
        current = compute_components(env_dir)
    except ReceiptError as exc:
        return False, str(exc)
    if fingerprint(current) == receipt["fingerprint"]:
        return True, (
            f"certified {receipt.get('certified_at', '?')} "
            f"by make {receipt.get('certified_by', '?')}"
        )
    return False, "config changed since certification" + _describe(
        _diff(receipt["components"], current)
    )


# ── exposure gate ─────────────────────────────────────────────────────────────


def gate_open(env_dir: Path) -> bool:
    path = (env_dir / "env.auto.tfvars").resolve()
    if not path.is_file():
        return False
    try:
        value = hcl2.loads(path.read_text()).get(GATE_KEY, False)
    except Exception:
        match = GATE_LINE.search(path.read_text())
        value = match.group(0).split("=", 1)[1].strip() if match else False
    return value is True or str(value).strip().strip('"').lower() == "true"


def persist_gate_open(env_dir: Path) -> None:
    path = env_dir / "env.auto.tfvars"
    text = path.read_text() if path.exists() else ""
    line = f"{GATE_KEY} = true"
    if GATE_LINE.search(text):
        text = GATE_LINE.sub(line, text, count=1)
    else:
        text = text + ("" if not text or text.endswith("\n") else "\n") + line + "\n"
    atomic_write_text(path, text)


# ── per-environment lock ──────────────────────────────────────────────────────


@contextmanager
def _guard(env_dir: Path):
    """Short-lived flock serialising lock acquire/takeover/release."""
    path = env_dir / LOCK_RELPATH
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path.with_suffix(".guard"), "a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _read_lock(path: Path) -> dict:
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def acquire_lock(env_dir: Path, pid: int, owner: str) -> tuple[bool, str]:
    path = env_dir / LOCK_RELPATH
    host = socket.gethostname()
    with _guard(env_dir):
        note = ""
        if path.exists():
            held = _read_lock(path)
            held_pid = held.get("pid")
            same_host = held.get("host") == host
            if held and same_host and isinstance(held_pid, int) and not _pid_alive(held_pid):
                note = f"removed stale lock from dead pid {held_pid} (make {held.get('owner', '?')})"
                path.unlink()
            elif not held:
                note = f"removed unreadable lock file {path}"
                path.unlink()
            else:
                return False, (
                    f"{path} is held by make {held.get('owner', '?')} "
                    f"(pid {held_pid} on {held.get('host', '?')}, since "
                    f"{held.get('started_at', '?')}); wait for it to finish. If that "
                    f"process is gone (e.g. another host), delete {path} and retry"
                )
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                return False, f"{path} was taken concurrently; retry"
            raise
        with os.fdopen(fd, "w") as handle:
            json.dump({
                "pid": pid, "host": host, "owner": owner,
                "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }, handle)
            handle.flush()
            os.fsync(handle.fileno())
        return True, note


def release_lock(env_dir: Path, pid: int) -> None:
    path = env_dir / LOCK_RELPATH
    with _guard(env_dir):
        held = _read_lock(path)
        if held.get("pid") == pid and held.get("host") == socket.gethostname():
            pending_path(env_dir).unlink(missing_ok=True)
            path.unlink(missing_ok=True)


# ── CLI ───────────────────────────────────────────────────────────────────────


def _release_failed_hint(env_dir: Path, env: str, reason: str) -> str:
    env_file = env_dir / "env.auto.tfvars"
    if gate_open(env_dir):
        close = f"set {GATE_KEY} = false in {env_file}, then run: make apply ENV={env}"
    else:
        close = f"{env_file} still has {GATE_KEY} = false, so run: make apply ENV={env}"
    return (
        f"release: {reason}.\n"
        f"  Remote business access (table SELECT / Genie CAN_RUN) may have been PARTIALLY OPENED\n"
        f"  for {env} with the exposure gate forced on. To close it, {close}\n"
        f"  Then fix the cause, re-run make certify ENV={env}, and make release ENV={env}."
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=(
        "lock", "unlock", "clear", "snapshot", "verify-snapshot", "commit",
        "write", "check", "warn", "open-gate", "release-failed",
    ))
    parser.add_argument("env_dir", type=Path)
    parser.add_argument("--env", default="", help="Env name used in messages")
    parser.add_argument("--by", default="certify", help="Target recorded in the receipt")
    parser.add_argument("--owner", default="", help="Target holding the lock")
    parser.add_argument("--pid", type=int, default=0, help="Lock-owning shell pid")
    parser.add_argument("--reason", default="the release apply failed")
    parser.add_argument("--prefix", default="release", help="Message prefix for check")
    args = parser.parse_args(argv)
    env_dir = args.env_dir
    env = args.env or env_dir.name
    pid = args.pid or os.getppid()

    try:
        if args.command == "lock":
            ok, message = acquire_lock(env_dir, pid, args.owner or "?")
            if not ok:
                print(f"{args.owner or 'make'}: {env} is locked: {message}", file=sys.stderr)
                return 1
            if message:
                print(f"=== Governance lock: {message} ===")
        elif args.command == "unlock":
            release_lock(env_dir, pid)
        elif args.command == "clear":
            clear_receipt(env_dir)
        elif args.command == "snapshot":
            write_snapshot(env_dir)
            print("=== Gate-time input snapshot recorded ===")
        elif args.command == "verify-snapshot":
            verify_snapshot(env_dir)
        elif args.command == "commit":
            path = commit_receipt(env_dir, args.by)
            print(f"=== Certification receipt written: {path} ===")
        elif args.command == "write":
            path = write_receipt(env_dir, args.by)
            print(f"=== Certification receipt written: {path} ===")
        elif args.command == "check":
            current, reason = check_receipt(env_dir)
            if not current:
                print(f"{args.prefix}: {reason}; re-run make certify ENV={env}", file=sys.stderr)
                return 1
            print(f"=== Certification receipt is current ({reason}) ===")
        elif args.command == "warn":
            if gate_open(env_dir):
                current, reason = check_receipt(env_dir)
                if not current:
                    bar = "!" * 72
                    print(
                        f"\n{bar}\n"
                        f"WARNING: {GATE_KEY} = true in {env_dir / 'env.auto.tfvars'}, "
                        f"but there is no current certification:\n"
                        f"  {reason}.\n"
                        f"  Business exposure is OPEN without a current certification.\n"
                        f"  Run make certify ENV={env}, then make release ENV={env}.\n"
                        f"{bar}\n",
                        file=sys.stderr,
                    )
        elif args.command == "open-gate":
            persist_gate_open(env_dir)
            print(f"=== Persisted {GATE_KEY} = true in {env_dir / 'env.auto.tfvars'} ===")
        else:
            print(_release_failed_hint(env_dir, env, args.reason), file=sys.stderr)
    except ReceiptError as exc:
        if args.command in ("snapshot", "verify-snapshot", "commit", "write"):
            message = f"{args.by}: {exc}; no receipt written — re-run make {args.by} ENV={env}"
        else:
            message = f"{args.prefix}: {exc}"
        print(message, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
