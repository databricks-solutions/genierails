#!/usr/bin/env python3
"""Fail fast when the classification-only bootstrap has no usable footprint."""

import sys
from pathlib import Path

import hcl2


def main() -> int:
    path = Path(sys.argv[1])
    with path.open() as handle:
        config = hcl2.load(handle)

    if config.get("enable_classification") is not True:
        print(f"ERROR: set enable_classification = true in {path}", file=sys.stderr)
        return 1

    tables = list(config.get("uc_tables") or [])
    for space in config.get("genie_spaces") or []:
        tables.extend(space.get("uc_tables") or [])
    discovered_path = path.parent / "data_access" / "discovered_uc_tables.auto.tfvars"
    if discovered_path.exists():
        with discovered_path.open() as handle:
            discovered = hcl2.load(handle)
        tables.extend(discovered.get("discovered_uc_tables") or [])
    if not tables:
        print(
            "ERROR: define uc_tables (top-level or in genie_spaces), or import "
            f"a Genie agent to populate discovered_uc_tables in {path.parent}",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
