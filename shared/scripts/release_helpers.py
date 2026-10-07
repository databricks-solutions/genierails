#!/usr/bin/env python3
"""Small atomic helpers for the unified production release."""

import argparse
import os
import re
import sys
import tempfile
from pathlib import Path

try:
    import hcl2
except ImportError:  # pragma: no cover - production dependencies include python-hcl2
    hcl2 = None

try:
    from environment_lock import LOCK_RELPATH, owns_lock
except ImportError:  # imported as scripts.release_helpers in tests
    from scripts.environment_lock import LOCK_RELPATH, owns_lock

GATE_KEY = "business_access_enabled"
GATE_LINE = re.compile(rf"^\s*{GATE_KEY}\s*=.*$", re.MULTILINE)


def atomic_write(path: Path, text: str) -> None:
    target = path.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    mode = (target.stat().st_mode & 0o7777) if target.exists() else 0o644
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


def gate_open(env_dir: Path) -> bool:
    path = env_dir / "env.auto.tfvars"
    if not path.exists():
        return False
    text = path.read_text()
    if hcl2 is not None:
        try:
            return hcl2.loads(text).get(GATE_KEY, False) is True
        except Exception:
            return False
    match = GATE_LINE.search(text)
    return bool(match and match.group(0).split("=", 1)[1].split("#", 1)[0].strip().strip('"').lower() == "true")


def open_gate(env_dir: Path, pid: int) -> None:
    if not owns_lock(env_dir, pid):
        raise RuntimeError(f"release does not own {env_dir / LOCK_RELPATH}")
    path = env_dir / "env.auto.tfvars"
    text = path.read_text() if path.exists() else ""
    line = f"{GATE_KEY} = true"
    text = GATE_LINE.sub(line, text, count=1) if GATE_LINE.search(text) else text + ("" if not text or text.endswith("\n") else "\n") + line + "\n"
    atomic_write(path, text)
    if not owns_lock(env_dir, pid):
        raise RuntimeError(f"release lost ownership of {env_dir / LOCK_RELPATH}")
    if not gate_open(env_dir):
        raise RuntimeError(f"{GATE_KEY} = true did not persist in {path}")


def clear_old_receipts(env_dir: Path) -> None:
    for name in (".certified.json", ".certified.pending.json"):
        (env_dir / "generated" / name).unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("open-gate", "clear-old-receipts", "failed"))
    parser.add_argument("env_dir", type=Path)
    parser.add_argument("--env", default="prod")
    parser.add_argument("--reason", default="the release failed")
    parser.add_argument("--pid", type=int, default=0)
    args = parser.parse_args()
    if args.command == "open-gate":
        open_gate(args.env_dir, args.pid or os.getppid())
        return 0
    if args.command == "clear-old-receipts":
        clear_old_receipts(args.env_dir)
        return 0
    env_file = args.env_dir / "env.auto.tfvars"
    close = (f"set {GATE_KEY} = false in {env_file}, then run: make apply ENV={args.env}"
             if gate_open(args.env_dir) else
             f"{env_file} still has {GATE_KEY} = false, so run: make apply ENV={args.env}")
    print(f"release: {args.reason}.\n  Remote business access (table SELECT / Genie CAN_RUN) may have been PARTIALLY OPENED\n"
          f"  for {args.env} with the exposure gate forced on. To close it, {close}\n"
          f"  Then fix the cause and re-run make release ENV={args.env}.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
