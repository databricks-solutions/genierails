#!/usr/bin/env python3
"""Certification receipt for the exposure gate.

`make certify` (and `make maintain`) write envs/<env>/generated/.certified.json
after the coverage gate and governance apply succeed. `make release` refuses to
open business_access_enabled unless the receipt's fingerprint still matches the
inputs that determine enforcement and exposure:

  - the generated config the coverage gate reads (generated/abac.auto.tfvars,
    generated/masking_functions.sql, ddl/_fetched.sql)
  - the resolved table footprint (scripts/footprint.py)
  - env.auto.tfvars, with business_access_enabled excluded (opening the gate
    must not invalidate the certification it depends on)
  - the git commit of the checkout, when available

Commands:
  write   ENV_DIR  record a fresh receipt
  clear   ENV_DIR  delete any receipt
  check   ENV_DIR  exit 0 if current, 1 if missing or stale (prints why)
  warn    ENV_DIR  print a loud WARNING if the gate is open without a current
                   receipt; always exits 0
  open-gate ENV_DIR  persist business_access_enabled = true in env.auto.tfvars
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import hcl2

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from scripts.footprint import resolve_footprint  # noqa: E402

RECEIPT_RELPATH = Path("generated") / ".certified.json"
GATE_KEY = "business_access_enabled"
GATE_LINE = re.compile(rf"^\s*{GATE_KEY}\s*=.*$", re.MULTILINE)
FILE_INPUTS = (
    "generated/abac.auto.tfvars",
    "generated/masking_functions.sql",
    "ddl/_fetched.sql",
)


def receipt_path(env_dir: Path) -> Path:
    return env_dir / RECEIPT_RELPATH


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _env_config_digest(env_dir: Path) -> str | None:
    path = env_dir / "env.auto.tfvars"
    if not path.is_file():
        return None
    text = path.read_text()
    try:
        config = hcl2.loads(text)
        config.pop(GATE_KEY, None)
        return _sha256(json.dumps(config, sort_keys=True, default=str).encode())
    except Exception:
        return _sha256(GATE_LINE.sub("", text).encode())


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
    for rel in FILE_INPUTS:
        path = env_dir / rel
        components[rel] = _sha256(path.read_bytes()) if path.is_file() else None
    components["footprint"] = _sha256(
        json.dumps(resolve_footprint(env_dir)).encode()
    )
    components["env.auto.tfvars"] = _env_config_digest(env_dir)
    components["git_commit"] = _git_commit()
    return components


def fingerprint(components: dict[str, str | None]) -> str:
    return _sha256(json.dumps(components, sort_keys=True).encode())


def write_receipt(env_dir: Path, by: str) -> Path:
    components = compute_components(env_dir)
    receipt = {
        "version": 1,
        "env": env_dir.name,
        "certified_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "certified_by": by,
        "git_commit": components["git_commit"],
        "fingerprint": fingerprint(components),
        "components": components,
    }
    path = receipt_path(env_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return path


def clear_receipt(env_dir: Path) -> None:
    receipt_path(env_dir).unlink(missing_ok=True)


def check_receipt(env_dir: Path) -> tuple[bool, str]:
    """Return (current, reason)."""
    path = receipt_path(env_dir)
    if not path.is_file():
        return False, f"no certification receipt at {path}"
    try:
        receipt = json.loads(path.read_text())
    except (OSError, ValueError):
        return False, f"unreadable certification receipt at {path}"
    components = compute_components(env_dir)
    if receipt.get("fingerprint") == fingerprint(components):
        return True, f"certified {receipt.get('certified_at', '?')} by make {receipt.get('certified_by', '?')}"
    recorded = receipt.get("components") or {}
    changed = sorted(k for k in components if recorded.get(k) != components[k])
    return False, "config changed since certification" + (
        f" (changed: {', '.join(changed)})" if changed else ""
    )


def gate_open(env_dir: Path) -> bool:
    path = env_dir / "env.auto.tfvars"
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
    text = path.read_text() if path.is_file() else ""
    line = f"{GATE_KEY} = true"
    if GATE_LINE.search(text):
        text = GATE_LINE.sub(line, text, count=1)
    else:
        text = text + ("" if not text or text.endswith("\n") else "\n") + line + "\n"
    path.write_text(text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("write", "clear", "check", "warn", "open-gate"))
    parser.add_argument("env_dir", type=Path)
    parser.add_argument("--env", default="", help="Env name used in messages")
    parser.add_argument("--by", default="certify", help="Target recorded in the receipt")
    args = parser.parse_args(argv)
    env_dir = args.env_dir
    env = args.env or env_dir.name

    if args.command == "write":
        path = write_receipt(env_dir, args.by)
        print(f"=== Certification receipt written: {path} ===")
    elif args.command == "clear":
        clear_receipt(env_dir)
    elif args.command == "check":
        current, reason = check_receipt(env_dir)
        if not current:
            print(f"release: {reason}; re-run make certify ENV={env}", file=sys.stderr)
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
    else:
        persist_gate_open(env_dir)
        print(f"=== Persisted {GATE_KEY} = true in {env_dir / 'env.auto.tfvars'} ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
