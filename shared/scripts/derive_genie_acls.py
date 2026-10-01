#!/usr/bin/env python3
"""Re-derive the environment-local Genie ACL sidecar."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from generate_abac import autofix_acl_groups  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("abac")
    parser.add_argument("env")
    parser.add_argument("--canonical-workspace")
    parser.add_argument("--ignore-explicit", action="store_true")
    args = parser.parse_args()
    abac_path = Path(args.abac)
    env_path = Path(args.env)
    if not abac_path.exists() or not env_path.exists():
        print("ERROR: generated ABAC and environment tfvars must both exist")
        return 1
    try:
        count = autofix_acl_groups(
            abac_path,
            env_path,
            canonical_workspace_path=(
                Path(args.canonical_workspace) if args.canonical_workspace else None
            ),
            ignore_explicit=args.ignore_explicit,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 1
    print(f"Derived ACL sidecar for {count} Genie agent(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
