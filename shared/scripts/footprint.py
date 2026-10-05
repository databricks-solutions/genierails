#!/usr/bin/env python3
"""Resolve the effective UC table footprint for an environment."""

from __future__ import annotations

import argparse
from pathlib import Path

import hcl2


def load_hcl(path: Path) -> dict:
    if not path.is_file():
        return {}
    with path.open() as handle:
        return hcl2.load(handle)


def resolve_footprint(env_dir: str | Path) -> list[str]:
    """Return top-level, per-space, and discovered tables in stable order."""
    env_dir = Path(env_dir)
    config = load_hcl(env_dir / "env.auto.tfvars")
    tables = list(config.get("uc_tables") or [])
    for space in config.get("genie_spaces") or []:
        tables.extend(space.get("uc_tables") or [])
    discovered = load_hcl(
        env_dir / "data_access" / "discovered_uc_tables.auto.tfvars"
    )
    tables.extend(discovered.get("discovered_uc_tables") or [])
    return list(dict.fromkeys(table for table in tables if table))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("env_dir")
    parser.add_argument("--catalogs", action="store_true")
    args = parser.parse_args(argv)
    values = resolve_footprint(args.env_dir)
    if args.catalogs:
        values = sorted({table.split(".")[0] for table in values if "." in table})
    print(", ".join(values) if values else "(none detected)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
