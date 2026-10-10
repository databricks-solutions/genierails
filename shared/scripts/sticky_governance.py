#!/usr/bin/env python3
"""Keep deterministic tables governed after every agent drops them.

The governed set is the table names in generated/governed_tables.json plus
every table whose treatment tags are already deployed (data_access state).
Only names are stored: treatments are always re-derived from current tags, so
a stale or hand-edited record can neither keep an old tag nor weaken a mask,
and a missing record is refused (make rebuild-governed-tables) unless the env
is fresh. Only ``make ungovern`` takes a table out of the set.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import shlex
import subprocess
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


def _rebuild_hint(env_dir: Path) -> str:
    return f"rebuild it with: make rebuild-governed-tables ENV={Path(env_dir).resolve().name}"


def _parse_manifest(text: str, path: Path) -> list[str]:
    try:
        raw = json.loads(text)
    except ValueError as exc:
        raise GovernedTablesError(f"cannot read {path}: {exc}; {_rebuild_hint(path.parent.parent)}") from exc
    tables = raw.get("tables") if isinstance(raw, dict) else None
    if not isinstance(tables, list) or any(not isinstance(t, str) or not _is_table(t) for t in tables):
        raise GovernedTablesError(
            f'invalid {path}: expected {{"tables": ["catalog.schema.table", ...]}}; '
            + _rebuild_hint(path.parent.parent)
        )
    return tables


def _committed_manifest(env_dir: Path) -> list[str] | None:
    """The newest version of the record in git history, if it was ever committed."""
    def git(*args):
        return subprocess.run(["git", "-C", str(env_dir), *args], capture_output=True, text=True)
    log = git("log", "--format=%H", "--", str(MANIFEST))
    for commit in log.stdout.split() if log.returncode == 0 else []:
        shown = git("show", f"{commit}:./{MANIFEST}")
        if shown.returncode == 0:
            return _parse_manifest(shown.stdout, Path(env_dir) / MANIFEST)
    return None


def _config_tables(env_dir: Path, tag_key: str) -> list[str]:
    """Tables with treatment tags in the generated or promoted config."""
    tables = []
    for path in (MANIFEST.parent / "abac.auto.tfvars", STATE.parent / "abac.auto.tfvars"):
        for item in _load_hcl(Path(env_dir) / path).get("tag_assignments") or []:
            if item.get("entity_type") == "columns" and item.get("tag_key") == tag_key:
                if table := _table_of(item.get("entity_name", "")):
                    tables.append(table)
    return tables


def _recorded_elsewhere(env_dir: Path, tag_key: str, check_config: bool) -> list[str]:
    """Why this env has governance even though its record is missing."""
    reasons = []
    if _state_tags(env_dir, tag_key):
        reasons.append(f"treatment tags are deployed ({STATE})")
    if check_config and _config_tables(env_dir, tag_key):
        reasons.append("its generated or promoted config has treatment tags")
    if _committed_manifest(env_dir) is not None:
        reasons.append(f"{MANIFEST} is in git history")
    return reasons


def _read_manifest(env_dir: Path, tag_key: str, check_config: bool = True) -> list[str]:
    path = Path(env_dir) / MANIFEST
    if path.is_file():
        try:
            text = path.read_text()
        except OSError as exc:
            raise GovernedTablesError(f"cannot read {path}: {exc}") from exc
        return _parse_manifest(text, path)
    reasons = _recorded_elsewhere(env_dir, tag_key, check_config)
    if reasons:
        # Starting empty would silently drop governance not yet applied.
        raise GovernedTablesError(
            f"{path} is missing but this env has recorded governance ({'; '.join(reasons)}); "
            + _rebuild_hint(env_dir)
        )
    return []  # A fresh env: nothing generated, nothing deployed.


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
    return _unique(_read_manifest(env_dir, tag_key) + _deployed_tables(env_dir, tag_key),
                   os.environ.get(UNGOVERN_ENV, ""))


def _deployed_tables(env_dir: Path, tag_key: str) -> list[str]:
    return [t for _name, _key, column in _state_tags(env_dir, tag_key) if (t := _table_of(column))]


def rebuild(env_dir: Path) -> list[str]:
    """Rewrite the record from git history, deployed tags and generated/promoted config."""
    if not deterministic(env_dir):
        raise GovernedTablesError(f'{env_dir}: requires governance_mode = "deterministic"')
    tag_key = _tag_key()
    tables = _unique((_committed_manifest(env_dir) or []) + _deployed_tables(env_dir, tag_key)
                     + _config_tables(env_dir, tag_key))
    save_governed_tables(env_dir, tables)
    print(f"Rebuilt {Path(env_dir) / MANIFEST}: {', '.join(tables) or '(no governed tables)'}")
    return tables


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
    # make generate records right after writing its config, so that config is
    # not evidence of a lost record here (generate's own load checked it first).
    recorded = _read_manifest(env_dir, tag_key, check_config=False)
    tables = _unique(recorded + _deployed_tables(env_dir, tag_key) + tagged, os.environ.get(UNGOVERN_ENV, ""))
    save_governed_tables(env_dir, tables)
    return tables


def check_layout(env_dir: Path, env_name: str, tag_key: str | None = None) -> None:
    """Refuse a deterministic env whose treatment tags still sit at the old address."""
    if not deterministic(env_dir):
        return
    tag_key = tag_key or _tag_key()
    old = [(key, column) for name, key, column in _state_tags(env_dir, tag_key) if name == "assignments"]
    if not old:
        return
    prefix = shlex.join([
        f"ENVS_DIR={Path(env_dir).resolve().parent}", str(SHARED / "scripts" / "terraform_layer.sh"),
        "data_access", env_name, "state-mv",
    ])
    commands = []
    for key, column in sorted(old, key=lambda item: item[1].lower()):
        new_key = f"columns|{column}|{tag_key}"
        commands.append(f"{prefix} {shlex.quote(f'{TAG_RESOURCE}.assignments[{json.dumps(key)}]')} "
                        f"{shlex.quote(f'{TAG_RESOURCE}.treatment[{json.dumps(new_key)}]')}")
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
    rebuilt = sub.add_parser("rebuild", help="rewrite the record from history, state and config")
    rebuilt.add_argument("--env-dir", type=Path, required=True)
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
        elif args.command == "rebuild":
            rebuild(args.env_dir)
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
