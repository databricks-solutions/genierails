#!/usr/bin/env python3
"""Small helpers for the unified production release."""

import argparse
import sys
from pathlib import Path


def clear_old_receipts(env_dir: Path) -> None:
    for name in (".certified.json", ".certified.pending.json"):
        (env_dir / "generated" / name).unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("clear-old-receipts", "failed"))
    parser.add_argument("env_dir", type=Path)
    parser.add_argument("--env", default="prod")
    parser.add_argument("--reason", default="the release failed")
    args = parser.parse_args()
    if args.command == "clear-old-receipts":
        clear_old_receipts(args.env_dir)
        return 0
    env_file = args.env_dir / "env.auto.tfvars"
    print(f"release: {args.reason}.\n  Business access (table SELECT / Genie CAN_RUN) for {args.env} may be PARTLY APPLIED;\n"
          f"  Terraform granted only what passed the coverage check. To withdraw access, remove the groups\n"
          f"  (or set acl_groups = []) in {env_file}, then run: make apply ENV={args.env}\n"
          f"  Then fix the cause and re-run make release ENV={args.env}.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
