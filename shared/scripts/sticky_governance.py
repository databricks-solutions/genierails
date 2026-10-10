#!/usr/bin/env python3
"""Keep deterministic tables governed after every agent drops them.

The governed set is the table names in generated/governed_tables.json plus
every table whose treatment tags are already deployed (data_access state).
Only names are stored: treatments are always re-derived from current tags, so
a stale or hand-edited record can neither keep an old tag nor weaken a mask,
and a deleted record is rebuilt from what is deployed. Only ``make ungovern``
takes a table out of the set.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import shlex
import sys
from pathlib import Path
from typing import Iterable

import hcl2

SHARED = Path(__file__).resolve().parents[1]
if str(SHARED) not in sys.path:
    sys.path.insert(0, str(SHARED))

from scripts.footprint import FootprintError, resolve_footprint  # noqa: E402

MANIFEST = Path("generated/governed_tables.json")
STATE = Path("data_access/terraform.tfstate")
UNGOVERN_ENV = "GENIERAILS_UNGOVERN_TABLE"
TAG_RESOURCE = "module.data_access.databricks_entity_tag_assignment"


class GovernedTablesError(RuntimeError):
    """The governed-table set cannot be trusted or changed safely."""


def _load_hcl(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        with path.open() as handle:
            value = hcl2.load(handle)
    except Exception as exc:
        raise GovernedTablesError(f"cannot parse {path}: {exc}") from exc
    return value if isinstance(value, dict) else {}


def deterministic(env_dir: Path) -> bool:
    return _load_hcl(Path(env_dir) / "env.auto.tfvars").get("governance_mode", "legacy") == "deterministic"


def _tag_key() -> str:
    from treatment_derivation import load_treatment_config
    return load_treatment_config().tag_key


def _is_table(name: str) -> bool:
    parts = name.split(".")
    return len(parts) == 3 and all(parts) and not any(c in name for c in "*?[]")


def _table_of(column: str) -> str | None:
    parts = str(column).split(".")
    return ".".join(parts[:3]) if len(parts) == 4 and all(parts) else None


def _state_tags(env_dir: Path, tag_key: str) -> list[tuple[str, object, str]]:
    """(resource name, index key, column) of every deployed treatment tag."""
    path = Path(env_dir) / STATE
    if not path.is_file():
        return []
    try:
        state = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise GovernedTablesError(f"cannot read Terraform state {path}: {exc}") from exc
    tags = []
    for resource in state.get("resources") or []:
        if resource.get("type") != "databricks_entity_tag_assignment" or resource.get("mode") == "data":
            continue
        for instance in resource.get("instances") or []:
            attrs = instance.get("attributes") or {}
            if attrs.get("entity_type") == "columns" and attrs.get("tag_key") == tag_key:
                tags.append((resource.get("name"), instance.get("index_key"), str(attrs.get("entity_name", ""))))
    return tags


def _read_manifest(env_dir: Path) -> list[str]:
    path = Path(env_dir) / MANIFEST
    if not path.is_file():
        return []
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise GovernedTablesError(f"cannot read {path}: {exc}") from exc
    tables = raw.get("tables") if isinstance(raw, dict) else None
    if not isinstance(tables, list) or any(not isinstance(t, str) or not _is_table(t) for t in tables):
        raise GovernedTablesError(
            f'invalid {path}: expected {{"tables": ["catalog.schema.table", ...]}}; '
            "delete it to rebuild it from what is deployed"
        )
    return tables


def _unique(tables: Iterable[str], drop: str = "") -> list[str]:
    seen: dict[str, str] = {}
    for table in tables:
        if table.lower() != drop.lower():
            seen.setdefault(table.lower(), table)
    return sorted(seen.values(), key=str.lower)


def load_governed_tables(env_dir: Path, tag_key: str | None = None) -> list[str]:
    """Recorded names plus deployed tables, minus a table being ungoverned."""
    if not deterministic(env_dir):
        return []
    tag_key = tag_key or _tag_key()
    deployed = [t for _name, _key, column in _state_tags(env_dir, tag_key) if (t := _table_of(column))]
    return _unique(_read_manifest(env_dir) + deployed, os.environ.get(UNGOVERN_ENV, ""))


def save_governed_tables(env_dir: Path, tables: Iterable[str]) -> None:
    path = Path(env_dir) / MANIFEST
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"tables": _unique(tables)}, indent=2) + "\n")


def record(env_dir: Path, assignments: Iterable[dict], tag_key: str | None = None) -> list[str]:
    """Add every table that now has a treatment tag to the governed set."""
    if not deterministic(env_dir):
        return []
    tag_key = tag_key or _tag_key()
    tagged = [
        t for item in assignments
        if item.get("entity_type") == "columns" and item.get("tag_key") == tag_key
        and (t := _table_of(item.get("entity_name", "")))
    ]
    tables = _unique(load_governed_tables(env_dir, tag_key) + tagged, os.environ.get(UNGOVERN_ENV, ""))
    save_governed_tables(env_dir, tables)
    return tables


def check_layout(env_dir: Path, env_name: str, tag_key: str | None = None) -> None:
    """Refuse a deterministic env whose treatment tags still sit at the old address."""
    if not deterministic(env_dir):
        return
    old = [(key, column) for name, key, column in _state_tags(env_dir, tag_key or _tag_key())
           if name == "assignments"]
    if not old:
        return
    runner = SHARED / "scripts" / "terraform_layer.sh"
    envs_dir = Path(env_dir).resolve().parent
    commands = [
        f"ENVS_DIR={shlex.quote(str(envs_dir))} {shlex.quote(str(runner))} data_access {shlex.quote(env_name)} "
        f"state-mv {shlex.quote(f'{TAG_RESOURCE}.assignments[{json.dumps(key)}]')} "
        f"{shlex.quote(f'{TAG_RESOURCE}.treatment[{json.dumps(column)}]')}"
        for key, column in sorted(old, key=lambda item: item[1].lower())
    ]
    raise GovernedTablesError(
        f"{len(old)} treatment tag(s) in {Path(env_dir) / STATE} are at the old Terraform address; "
        "applying would destroy and recreate them, so nothing was planned or applied. "
        "Move them, then re-run:\n  " + "\n  ".join(commands)
    )


def _agent_tables(env_dir: Path) -> list[str]:
    """Every table (or pattern) a configured agent still reads, fully qualified."""
    cfg = _load_hcl(env_dir / "env.auto.tfvars")
    try:
        entries = list(resolve_footprint(env_dir))
    except FootprintError as exc:
        raise GovernedTablesError(str(exc)) from exc
    entries += list(cfg.get("declared_footprint") or [])
    for space in cfg.get("genie_spaces") or []:
        entries += list(space.get("declared_footprint") or [])
    catalog = str(cfg.get("uc_catalog") or "").strip()
    tables = []
    for entry in entries:
        if isinstance(entry, dict):
            entry = entry.get("table") or entry.get("identifier") or entry.get("name") or ""
        parts = str(entry).strip().split(".")
        if len(parts) == 2 and catalog:
            parts = [catalog] + parts
        if len(parts) >= 3:
            tables.append(".".join(parts[:3]))
    return tables


def ungovern(env_dir: Path, table: str, *, commit: bool = False) -> None:
    """Check removing TABLE and print what goes; ``commit`` updates the local files."""
    env_dir = Path(env_dir)
    if not _is_table(table):
        raise GovernedTablesError(f"TABLE must be one catalog.schema.table (no wildcards), got {table!r}")
    if not deterministic(env_dir):
        raise GovernedTablesError(f'{env_dir}: ungovern requires governance_mode = "deterministic"')
    users = sorted({p for p in _agent_tables(env_dir) if fnmatch.fnmatchcase(table.lower(), p.lower())})
    if users:
        raise GovernedTablesError(
            f"refusing to ungovern {table}: a configured agent still uses it ({', '.join(users)}); "
            "remove it from every agent first"
        )
    tag_key = _tag_key()
    governed = load_governed_tables(env_dir, tag_key)
    match = next((t for t in governed if t.lower() == table.lower()), None)
    if match is None:
        raise GovernedTablesError(f"{table} is not governed in {env_dir}")

    def in_table(column) -> bool:
        return str(_table_of(column)).lower() == match.lower()

    config_path = env_dir / "generated/abac.auto.tfvars"
    assignments = list(_load_hcl(config_path).get("tag_assignments") or [])
    ours = [
        item for item in assignments
        if item.get("entity_type") == "columns" and item.get("tag_key") == tag_key
        and in_table(item.get("entity_name", ""))
    ]
    columns = {str(item.get("entity_name")) for item in ours}
    columns |= {column for _name, _key, column in _state_tags(env_dir, tag_key) if in_table(column)}
    print(f"Ungovern {match} removes these treatment tags, so its column masks stop applying:")
    for column in sorted(columns, key=str.lower) or ["(none deployed)"]:
        print(f"  {column}")
    if not commit:
        return
    if ours:
        from generate_abac import _render_tag_assignment_block, _replace_bracket_section
        kept = [item for item in assignments if item not in ours]
        config_path.write_text(_replace_bracket_section(
            config_path.read_text(), "tag_assignments", [_render_tag_assignment_block(i) for i in kept],
        ))
    save_governed_tables(env_dir, [t for t in governed if t.lower() != match.lower()])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    rec = sub.add_parser("record", help="add tables tagged in --config to the governed set")
    rec.add_argument("--env-dir", type=Path, required=True)
    rec.add_argument("--config", type=Path, required=True)
    layout = sub.add_parser("check-layout", help="refuse treatment tags still at the old state address")
    layout.add_argument("--env-dir", type=Path, required=True)
    layout.add_argument("--env-name", required=True)
    ung = sub.add_parser("ungovern", help="check removing --table; --commit updates the local files")
    ung.add_argument("--env-dir", type=Path, required=True)
    ung.add_argument("--table", required=True)
    ung.add_argument("--commit", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "record":
            record(args.env_dir, _load_hcl(args.config).get("tag_assignments") or [])
        elif args.command == "check-layout":
            check_layout(args.env_dir, args.env_name)
        else:
            ungovern(args.env_dir, args.table, commit=args.commit)
    except GovernedTablesError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
