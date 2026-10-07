#!/usr/bin/env python3
"""Small atomic helpers for the unified production release."""

import argparse
import os
import re
import sys
import tempfile
from pathlib import Path

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
    match = GATE_LINE.search(path.read_text())
    return bool(match and match.group(0).split("=", 1)[1].strip().strip('"').lower() == "true")


def open_gate(env_dir: Path) -> None:
    path = env_dir / "env.auto.tfvars"
    text = path.read_text() if path.exists() else ""
    line = f"{GATE_KEY} = true"
    text = GATE_LINE.sub(line, text, count=1) if GATE_LINE.search(text) else text + ("" if not text or text.endswith("\n") else "\n") + line + "\n"
    atomic_write(path, text)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("open-gate", "failed"))
    parser.add_argument("env_dir", type=Path)
    parser.add_argument("--env", default="prod")
    parser.add_argument("--reason", default="the release failed")
    args = parser.parse_args()
    if args.command == "open-gate":
        open_gate(args.env_dir)
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
