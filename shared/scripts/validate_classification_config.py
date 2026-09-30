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
    if not tables:
        print(
            f"ERROR: define uc_tables (top-level or in genie_spaces) in {path}",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
