#!/usr/bin/env python3
"""Persist deterministic table governance and implement explicit ungovern."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
import hcl2

MANIFEST = Path("generated/governed_tables.json")
TREATMENT_KEY = "gr_treatment"

def _load_hcl(path: Path) -> dict:
    if not path.is_file():
        return {}
    with path.open() as handle:
        return hcl2.load(handle)


def deterministic(env_dir: Path) -> bool:
    return _load_hcl(env_dir / "env.auto.tfvars").get("governance_mode", "legacy") == "deterministic"


def load_manifest(env_dir: Path) -> dict[str, dict[str, str]]:
    path = env_dir / MANIFEST
    if not path.is_file():
        return {}
    raw = json.loads(path.read_text())
    if raw.get("version") != 1 or not isinstance(raw.get("tables"), dict):
        raise RuntimeError(f"invalid governed-table manifest: {path}")
    return {str(t): {str(k): str(v) for k, v in cs.items()} for t, cs in raw["tables"].items()}


def save_manifest(env_dir: Path, tables: dict[str, dict[str, str]]) -> None:
    path = env_dir / MANIFEST
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(
        {"version": 1, "tables": dict(sorted(tables.items()))}, indent=2, sort_keys=True,
    ) + "\n")


def merge_assignments(env_dir: Path, assignments: list[dict]) -> list[dict]:
    if not deterministic(env_dir):
        return assignments
    tables, other = load_manifest(env_dir), []
    for item in assignments:
        if item.get("entity_type") == "columns" and item.get("tag_key") == TREATMENT_KEY:
            column = str(item.get("entity_name", ""))
            table = column.rsplit(".", 1)[0]
            tables.setdefault(table, {})[column] = str(item.get("tag_value", ""))
        else:
            other.append(item)
    save_manifest(env_dir, tables)
    return other + [{"entity_type": "columns", "entity_name": c, "tag_key": TREATMENT_KEY, "tag_value": v}
                    for t in sorted(tables) for c, v in sorted(tables[t].items())]


def configured_tables(env_dir: Path) -> set[str]:
    cfg = _load_hcl(env_dir / "env.auto.tfvars")
    result = set(map(str, cfg.get("uc_tables", []) or []))
    for space in cfg.get("genie_spaces", []) or []:
        result.update(map(str, space.get("uc_tables", []) or []))
    data = _load_hcl(env_dir / "data_access/discovered_uc_tables.auto.tfvars")
    result.update(map(str, data.get("discovered_uc_tables", []) or []))
    catalog = str(cfg.get("uc_catalog", ""))
    return {t if len(t.split(".")) >= 3 or not catalog else f"{catalog}.{t}" for t in result}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("record", "check-ungovern", "commit-ungovern"))
    parser.add_argument("--env-dir", type=Path, required=True)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--table")
    args = parser.parse_args(argv)
    if not deterministic(args.env_dir):
        print('ERROR: stable governance requires governance_mode = "deterministic"', file=sys.stderr)
        return 1
    if args.command == "record":
        config = args.config or args.env_dir / "generated/abac.auto.tfvars"
        merge_assignments(args.env_dir, list(_load_hcl(config).get("tag_assignments", []) or []))
        return 0
    table = str(args.table or "")
    if table in configured_tables(args.env_dir):
        print(f"ERROR: refusing to ungovern {table}: a configured agent still uses it", file=sys.stderr)
        return 1
    tables = load_manifest(args.env_dir)
    if table not in tables:
        print(f"ERROR: {table} is not in the persisted governed-table set", file=sys.stderr)
        return 1
    print(f"Ungovern {table} will remove:")
    for column, treatment in sorted(tables[table].items()):
        print(f"  treatment tag {column}: {TREATMENT_KEY}={treatment}")
    print("  table governance (masks cease to match after these tags are removed)")
    if args.command == "commit-ungovern":
        del tables[table]
        save_manifest(args.env_dir, tables)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
