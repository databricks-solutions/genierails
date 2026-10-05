#!/usr/bin/env python3
"""Fail fast when the classification-only bootstrap has no usable footprint."""

import sys
from pathlib import Path

import hcl2

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from genie_space_placeholder import placeholder_error  # noqa: E402
from scripts.footprint import FootprintError, resolve_footprint


def main() -> int:
    path = Path(sys.argv[1])
    with path.open() as handle:
        config = hcl2.load(handle)

    placeholder = placeholder_error(config, path)
    if placeholder:
        print(f"ERROR: {placeholder}", file=sys.stderr)
        return 1

    if config.get("enable_classification") is not True:
        print(f"ERROR: set enable_classification = true in {path}", file=sys.stderr)
        return 1

    try:
        tables = resolve_footprint(path.parent)
    except FootprintError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
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
