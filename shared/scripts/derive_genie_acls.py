#!/usr/bin/env python3
"""Re-derive the environment-local Genie ACL sidecar."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from generate_abac import autofix_acl_groups  # noqa: E402


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: derive_genie_acls.py <generated-abac.tfvars> <env.tfvars>")
        return 2
    abac_path = Path(sys.argv[1])
    env_path = Path(sys.argv[2])
    if not abac_path.exists() or not env_path.exists():
        print("ERROR: generated ABAC and environment tfvars must both exist")
        return 1
    count = autofix_acl_groups(abac_path, env_path)
    print(f"Derived ACL sidecar for {count} Genie agent(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
