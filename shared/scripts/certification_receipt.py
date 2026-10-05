#!/usr/bin/env python3
"""Certification receipt and per-environment lock for the exposure gate.

`make certify` / `make maintain` snapshot the enforcement inputs right before
the coverage gate runs, and write envs/<env>/generated/.certified.json from
that snapshot only if the inputs are still identical after the governance
apply. `make release` refuses to open business_access_enabled unless the
receipt still matches the current inputs, before its apply and again (bound to
the gate write) after it. All three hold an exclusive per-environment lock for
their whole pipeline.

Fingerprinted inputs (symlinks resolved; content hashes, not mtimes):
  - env-local inputs: every file under envs/<env>/ and envs/account/ that the
    gate, promote, Terraform layers or audits read -- see env_input_files()
  - the resolved footprint (scripts/footprint.py)
  - env.auto.tfvars is hashed with business_access_enabled excluded (opening the
    gate must not invalidate the certification it depends on)
  - every repo file under shared/ that can affect the gate or enforcement
    (validation/derivation code, treatment/tag/function registries, country and
    industry overlays, Terraform roots/modules, scripts, Makefile.shared), so a
    dirty working tree cannot ride on a clean HEAD
The git commit is recorded for provenance only.

These checks detect accidental drift and races between cooperating runs. They
are not a defence against someone with write access to envs/<env>/ who edits
and reverts files while Terraform reads them, or who hand-forges a receipt.

Commands (all take ENV_DIR):
  lock / unlock     acquire / release the env lock (--pid, --owner)
  clear             delete the receipt and any pending snapshot
  snapshot          record the gate-time input snapshot
  verify-snapshot   fail if inputs changed since the snapshot
  commit            write the receipt from the snapshot if inputs are unchanged
  check             exit 0 if the receipt is current, 1 otherwise
  warn              loud WARNING if the gate is open without a current receipt
  open-gate         verify the receipt, persist business_access_enabled = true
                    atomically, then re-verify (requires owning the env lock)
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

RECEIPT_VERSION = 3
RECEIPT_RELPATH = Path("generated") / ".certified.json"
PENDING_RELPATH = Path("generated") / ".certified.pending.json"
LOCK_RELPATH = Path("generated") / ".governance.lock"
GATE_KEY = "business_access_enabled"
GATE_LINE = re.compile(rf"^\s*{GATE_KEY}\s*=.*$", re.MULTILINE)
CERTIFYING_TARGETS = ("certify", "maintain")

# Env-local inputs. A glob rather than a per-target list so a new input file
# (another *.auto.tfvars, a space config) is covered without code changes:
#   - Terraform and the layer runner read every *.tfvars / *.tfvars.json / *.tf;
#     scripts read *.yaml / *.yml; the gate reads generated/*.sql + ddl/*.sql.
#   - Excluded: dotfiles/dot-dirs (.terraform/, .governance.lock, the receipt,
#     .<layer>.apply.sha), terraform.tfstate*, *.lock.hcl, logs -- volatile or
#     self-referential -- and DERIVED_ENV_FILES below.
ENV_INPUT_SUFFIXES = (".tfvars", ".tfvars.json", ".tf", ".yaml", ".yml")
ENV_SQL_DIRS = ("generated", "ddl")
# Files that promote / _prepare-classification regenerate before EVERY apply
# (including release's) from inputs that are hashed above. Hashing them would
# make every certify fail its own post-apply check, and hand edits are
# overwritten before Terraform reads them.
DERIVED_ENV_FILES = {
    "abac.auto.tfvars",
    "data_access/abac.auto.tfvars",
    "data_access/masking_functions.sql",
    "data_access/classification.auto.tfvars",
    "generated/genie_space_derived_acl_groups.auto.tfvars",
}
DERIVED_ACCOUNT_FILES = {"abac.auto.tfvars"}
# Set from --account-dir (the Makefile's ACCOUNT_ENV_DIR); default envs/account.
ACCOUNT_DIR: Path | None = None

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


def _env_config_digest(path: Path) -> str | None:
    path = path.resolve()
    if not path.is_file():
        return None
    text = path.read_text()
    try:
        config = hcl2.loads(text)
        config.pop(GATE_KEY, None)
        return _sha256(json.dumps(config, sort_keys=True, default=str).encode())
    except Exception:
        return _sha256(GATE_LINE.sub("", text).encode())


def _walk_files(root: Path, excluded_dirs: set[str] = frozenset()) -> list[tuple[str, Path]]:
    """(posix relpath, path) for files under root; follows dir symlinks cycle-safely."""
    files = []
    seen: set[str] = set()
    for dirpath, dirnames, filenames in os.walk(root, followlinks=True):
        real = os.path.realpath(dirpath)
        if real in seen:
            dirnames[:] = []
            continue
        seen.add(real)
        dirnames[:] = sorted(
            d for d in dirnames
            if d not in excluded_dirs and not d.startswith(".")
            and os.path.realpath(os.path.join(dirpath, d)) not in seen
        )
        for name in sorted(filenames):
            if name.startswith("."):
                continue
            path = Path(dirpath) / name
            files.append((path.relative_to(root).as_posix(), path))
    return files


def repo_input_files(root: Path | None = None) -> list[Path]:
    root = root or REPO_INPUT_ROOT
    return [
        path for _rel, path in _walk_files(root, REPO_EXCLUDED_DIRS)
        if path.suffix not in REPO_EXCLUDED_SUFFIXES
    ]


def _is_env_input(rel: str, derived: set[str]) -> bool:
    name = rel.rsplit("/", 1)[-1]
    if rel in derived or name.startswith("terraform.tfstate") or name.endswith(".lock.hcl"):
        return False
    if name.endswith(ENV_INPUT_SUFFIXES):
        return True
    return name.endswith(".sql") and rel.split("/", 1)[0] in ENV_SQL_DIRS


def env_input_files(env_dir: Path, account_dir: Path | None = None) -> dict[str, Path]:
    """Env-local enforcement inputs keyed by 'env:<rel>' / 'account:<rel>'."""
    account_dir = account_dir or ACCOUNT_DIR or env_dir.parent / "account"
    inputs: dict[str, Path] = {}
    for prefix, root, derived in (
        ("env", env_dir, DERIVED_ENV_FILES),
        ("account", account_dir, DERIVED_ACCOUNT_FILES),
    ):
        if root.is_dir():
            for rel, path in _walk_files(root):
                if _is_env_input(rel, derived):
                    inputs[f"{prefix}:{rel}"] = path
    return inputs


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
    env_config = (env_dir / "env.auto.tfvars").resolve()
    for key, path in env_input_files(env_dir).items():
        # Any path resolving to the env config (incl. the data_access symlink)
        # is hashed without the exposure gate.
        if path.resolve() == env_config:
            components[key] = _env_config_digest(path)
        else:
            components[key] = _file_digest(path)
    try:
        footprint = resolve_footprint(env_dir)
    except Exception as exc:
        raise ReceiptError(f"cannot resolve footprint: {exc}") from exc
    components["footprint"] = _sha256(json.dumps(footprint).encode())
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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def clear_receipt(env_dir: Path) -> None:
    receipt_path(env_dir).unlink(missing_ok=True)
    pending_path(env_dir).unlink(missing_ok=True)


def write_snapshot(env_dir: Path) -> Path:
    components = compute_components(env_dir)
    snapshot = {
        "version": RECEIPT_VERSION,
        "taken_at": _now(),
        "fingerprint": fingerprint(components),
        "components": components,
    }
    path = pending_path(env_dir)
    atomic_write_text(path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    return path


def _valid_timestamp(value) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def _load_trusted(path: Path, label: str, env: str | None = None) -> dict:
    """Load a receipt/snapshot and check it is well-formed and self-consistent.

    For receipts (env given), also validate metadata against the target env.
    """
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
    if env is not None:
        if data.get("env") != env:
            raise ReceiptError(
                f"{label} at {path} is for env {data.get('env')!r}, not {env!r}"
            )
        if (
            not _valid_timestamp(data.get("gated_at"))
            or not _valid_timestamp(data.get("certified_at"))
            or data.get("certified_by") not in CERTIFYING_TARGETS
        ):
            raise ReceiptError(f"malformed {label} at {path}: bad metadata")
    elif not _valid_timestamp(data.get("taken_at")):
        raise ReceiptError(f"malformed {label} at {path}: bad metadata")
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


def commit_receipt(env_dir: Path, by: str, env: str | None = None) -> Path:
    """Write the receipt from the gate-time snapshot iff inputs are unchanged."""
    try:
        verify_snapshot(env_dir)
        snapshot = _load_trusted(pending_path(env_dir), "gate-time snapshot")
        receipt = {
            "version": RECEIPT_VERSION,
            "env": env or env_dir.name,
            "gated_at": snapshot["taken_at"],
            "certified_at": _now(),
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


def write_receipt(env_dir: Path, by: str, env: str | None = None) -> Path:
    """Snapshot and commit in one step (inputs are by definition unchanged)."""
    write_snapshot(env_dir)
    return commit_receipt(env_dir, by, env)


def check_receipt(env_dir: Path, env: str | None = None) -> tuple[bool, str]:
    """Return (current, reason)."""
    try:
        receipt = _load_trusted(
            receipt_path(env_dir), "certification receipt", env or env_dir.name
        )
        current = compute_components(env_dir)
    except ReceiptError as exc:
        return False, str(exc)
    if fingerprint(current) == receipt["fingerprint"]:
        return True, (
            f"certified {receipt['certified_at']} by make {receipt['certified_by']}"
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


def release_open_gate(env_dir: Path, pid: int, env: str | None = None) -> None:
    """Verify-then-write the gate under the env lock, then re-verify."""
    if not owns_lock(env_dir, pid):
        raise ReceiptError(f"this release does not hold {env_dir / LOCK_RELPATH}")
    current, reason = check_receipt(env_dir, env)
    if not current:
        raise ReceiptError(f"{reason}; the gate was NOT persisted")
    persist_gate_open(env_dir)
    current, reason = check_receipt(env_dir, env)
    if not current or not gate_open(env_dir):
        raise ReceiptError(
            f"{reason if not current else 'gate write did not take effect'} "
            f"after persisting {GATE_KEY} = true"
        )


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


def _read_lock(path: Path) -> dict | None:
    """Return the lock's owner record, or None if malformed/owner-indeterminate."""
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("pid"), int)
        or isinstance(data.get("pid"), bool)
        or data["pid"] <= 0
        or not isinstance(data.get("host"), str)
        or not data["host"]
    ):
        return None
    return data


def owns_lock(env_dir: Path, pid: int) -> bool:
    held = _read_lock(env_dir / LOCK_RELPATH)
    return bool(held) and held["pid"] == pid and held["host"] == socket.gethostname()


def acquire_lock(env_dir: Path, pid: int, owner: str) -> tuple[bool, str]:
    path = env_dir / LOCK_RELPATH
    host = socket.gethostname()
    with _guard(env_dir):
        note = ""
        if path.exists() or path.is_symlink():
            held = _read_lock(path)
            if held is None:
                return False, (
                    f"cannot determine the owner of {path} (malformed or unreadable). "
                    f"If no certify/maintain/release is running for this env, "
                    f"delete {path} manually and retry"
                )
            if held["host"] == host and not _pid_alive(held["pid"]):
                note = f"removed stale lock from dead pid {held['pid']} (make {held.get('owner', '?')})"
                path.unlink()
            else:
                return False, (
                    f"{path} is held by make {held.get('owner', '?')} "
                    f"(pid {held['pid']} on {held['host']}, since "
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
            json.dump({"pid": pid, "host": host, "owner": owner, "started_at": _now()}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        return True, note


def release_lock(env_dir: Path, pid: int) -> None:
    """Remove the lock only if this pid owns it; never creates directories."""
    path = env_dir / LOCK_RELPATH
    if not path.parent.is_dir():
        return
    with _guard(env_dir):
        if owns_lock(env_dir, pid):
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
    global ACCOUNT_DIR
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=(
        "lock", "unlock", "clear", "snapshot", "verify-snapshot", "commit",
        "write", "check", "warn", "open-gate", "release-failed",
    ))
    parser.add_argument("env_dir", type=Path)
    parser.add_argument("--env", default="", help="Env name used in messages and the receipt")
    parser.add_argument("--account-dir", type=Path, default=None,
                        help="Shared account layer dir (default: <env_dir>/../account)")
    parser.add_argument("--by", default="certify", help="Target recorded in the receipt")
    parser.add_argument("--owner", default="", help="Target holding the lock")
    parser.add_argument("--pid", type=int, default=0, help="Lock-owning shell pid")
    parser.add_argument("--reason", default="the release apply failed")
    parser.add_argument("--prefix", default="release", help="Message prefix for check")
    args = parser.parse_args(argv)
    env_dir = args.env_dir
    env = args.env or env_dir.name
    pid = args.pid or os.getppid()
    ACCOUNT_DIR = args.account_dir

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
            path = commit_receipt(env_dir, args.by, env)
            print(f"=== Certification receipt written: {path} ===")
        elif args.command == "write":
            path = write_receipt(env_dir, args.by, env)
            print(f"=== Certification receipt written: {path} ===")
        elif args.command == "check":
            current, reason = check_receipt(env_dir, env)
            if not current:
                print(f"{args.prefix}: {reason}; re-run make certify ENV={env}", file=sys.stderr)
                return 1
            print(f"=== Certification receipt is current ({reason}) ===")
        elif args.command == "warn":
            if gate_open(env_dir):
                current, reason = check_receipt(env_dir, env)
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
            release_open_gate(env_dir, pid, env)
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
