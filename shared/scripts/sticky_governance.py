#!/usr/bin/env python3
"""Keep deterministic tables governed after every agent drops them.

The governed set is the table names in generated/governed_tables.json plus
every table whose treatment tags are already deployed (data_access state).
Only names are stored: treatments are always re-derived from current tags, so
a stale or hand-edited record can neither keep an old tag nor weaken a mask,
and a missing record is refused (make rebuild-governed-tables) unless the env
is fresh. Only ``make ungovern`` takes a table out of the set.

The record also lists, under ``agent_tables``, the tables of every agent kept
in an agents/ file (design 18.1). The data citizen owns an agent's table list;
this governance-owned record is rewritten from it on every generate, capture
and promote-to, so any agent change to its tables, even one with no classified
column, is a diff in a governance-owned file, and make check fails while it is
stale. Nothing reads it to grant access: SELECT comes only from table_readers.

An inline agent configured only by genie_space_id is listed under that ID.
Its tables come from the discovery make generate writes to the git-ignored
data_access/discovered_uc_tables.auto.tfvars; this record is the committed
copy promote-to reads, so a clean clone or CI promotes the same tables. With
no discovery present, the committed entry is kept as is.
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

SHARED = Path(__file__).resolve().parents[1]
if str(SHARED) not in sys.path:
    sys.path.insert(0, str(SHARED))

from scripts.hcl2_guard import import_hcl2  # noqa: E402
from scripts.footprint import (FootprintError, load_discovered_footprint, load_env_config,  # noqa: E402
                               resolve_footprint)
from scripts.state_layout import state_json  # noqa: E402
from governed_tables_record import (MANIFEST, committed_agent_tables,  # noqa: E402
                                    is_table as _is_table)

STATE = Path("data_access/terraform.tfstate")
UNGOVERN_ENV = "GENIERAILS_UNGOVERN_TABLE"
TAG_RESOURCE = "module.data_access.databricks_entity_tag_assignment"


class GovernedTablesError(RuntimeError):
    """The governed-table set cannot be trusted or changed safely."""


def _load_hcl(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        hcl2 = import_hcl2()
        with path.open() as handle:
            value = hcl2.load(handle)
    except Exception as exc:
        raise GovernedTablesError(f"cannot parse {path}: {exc}") from exc
    return value if isinstance(value, dict) else {}


def deterministic(env_dir: Path, *, require_env: bool = False) -> bool:
    """Whether the env opted in. ``require_env`` fails closed: the env file must
    be a readable, parseable regular file (not missing, a directory or a symlink)."""
    path = Path(env_dir) / "env.auto.tfvars"
    if require_env and (path.is_symlink() or not path.is_file()):
        raise GovernedTablesError(f"{path} is missing or not a regular file; cannot tell legacy from deterministic")
    return _load_hcl(path).get("governance_mode", "legacy") == "deterministic"


def _tag_key() -> str:
    from treatment_derivation import load_treatment_config
    return load_treatment_config().tag_key


def _table_of(column: str) -> str | None:
    parts = str(column).split(".")
    return ".".join(parts[:3]) if len(parts) == 4 and all(parts) else None


def _state_tags(env_dir: Path, tag_key: str) -> list[tuple[str, object, str]]:
    """(resource name, index key, column) of every deployed treatment tag."""
    try:
        state = state_json(env_dir, Path(env_dir).name, "data_access")
    except RuntimeError as exc:
        raise GovernedTablesError(str(exc)) from exc
    if state is None:
        return []
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


def _parse_record(text: str, path: Path) -> tuple[list[str], object]:
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
    return tables, raw.get("agent_tables") if isinstance(raw, dict) else None


def _parse_manifest(text: str, path: Path) -> list[str]:
    return _parse_record(text, path)[0]


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
    """Recorded footprint plus deployed tables, minus a table being ungoverned.

    In a deterministic env, ``agent_tables`` is the committed source of truth
    for an agent whose ID and table list intentionally do not survive
    promotion.  Validate it against the env's agents before admitting it to
    every downstream footprint consumer.
    """
    if not deterministic(env_dir):
        return []
    tag_key = tag_key or _tag_key()
    tables = _read_manifest(env_dir, tag_key)
    path = Path(env_dir) / MANIFEST
    if path.is_file():
        try:
            _recorded, recorded_agents = _parse_record(path.read_text(), path)
        except OSError as exc:
            raise GovernedTablesError(f"cannot read {path}: {exc}") from exc
        from agent_files import AgentFileError, load_all, promoted, settings_path
        try:
            file_agents = load_all(Path(env_dir))
        except AgentFileError as exc:
            raise GovernedTablesError(str(exc)) from exc
        configured = {agent["name"]: _unique(agent["uc_tables"]) for agent in file_agents}
        # Only a promoted file agent with neither a workspace ID nor an
        # explicit table list takes its footprint from the committed record.
        # Dev is config-authoritative: generate must be able to rename/remove
        # agents or change their tables before record rewrites this file.
        authoritative = {
            agent["name"]: configured[agent["name"]]
            for agent in file_agents
            if promoted(Path(env_dir)) and not agent["genie_space_id"]
            and not (_load_hcl(settings_path(env_dir, agent["name"])).get("uc_tables") or [])
        }
        cfg = _load_hcl(Path(env_dir) / "env.auto.tfvars")
        for space in cfg.get("genie_spaces") or []:
            if not isinstance(space, dict) or space.get("uc_tables"):
                continue
            space_id = str(space.get("genie_space_id") or "").strip()
            if space_id:
                configured.setdefault(space_id, [])
        if authoritative and not isinstance(recorded_agents, dict):
            raise GovernedTablesError(
                f"invalid {path}: agent_tables is missing for configured agents; {_rebuild_hint(env_dir)}"
            )
        normalized_agents = {}
        if isinstance(recorded_agents, dict):
            for name, agent_table_list in recorded_agents.items():
                if not isinstance(name, str) or not name or not isinstance(agent_table_list, list):
                    if authoritative:
                        raise GovernedTablesError(
                            f"invalid {path}: agent_tables must map agent names or IDs to table lists; "
                            + _rebuild_hint(env_dir)
                        )
                    continue
                if any(not isinstance(table, str) for table in agent_table_list):
                    if authoritative:
                        raise GovernedTablesError(
                            f"invalid {path}: agent_tables[{name!r}] must contain table names; "
                            + _rebuild_hint(env_dir)
                        )
                    continue
                invalid = [
                    table for table in agent_table_list
                    if not _is_table(table) and not any(char in table for char in "*?[]")
                ]
                if invalid and authoritative:
                    raise GovernedTablesError(
                        f"invalid {path}: agent_tables[{name!r}] contains invalid table names; "
                        + _rebuild_hint(env_dir)
                    )
                # Old records may contain wildcard patterns. They are not a
                # concrete governed footprint; ignore them so the next record
                # can rewrite the entry with resolved tables (or an empty list).
                normalized_agents[name] = _unique(table for table in agent_table_list if _is_table(table))
        if authoritative:
            missing = sorted(set(authoritative) - set(normalized_agents))
            extra = sorted(set(normalized_agents) - set(configured))
            mismatch = sorted(
                name for name, agent_tables in authoritative.items()
                if agent_tables and normalized_agents.get(name, []) != agent_tables
            )
            if missing or extra or mismatch:
                details = []
                if missing:
                    details.append("missing " + ", ".join(missing))
                if extra:
                    details.append("unknown " + ", ".join(extra))
                if mismatch:
                    details.append("tables differ for " + ", ".join(mismatch))
                raise GovernedTablesError(
                    f"invalid {path}: agent_tables does not match the configured agents "
                    f"({'; '.join(details)}); {_rebuild_hint(env_dir)}"
                )
        tables += [table for agent in normalized_agents.values() for table in agent]
    return _unique(tables + _deployed_tables(env_dir, tag_key),
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


def _id_only_tables(env_dir: Path, catalog: str) -> dict[str, list[str]]:
    """genie_space_id -> tables of each inline agent configured without uc_tables."""
    cfg = _load_hcl(Path(env_dir) / "env.auto.tfvars")
    spaces = [s for s in cfg.get("genie_spaces") or [] if isinstance(s, dict)]
    id_only = [s for s in spaces if str(s.get("genie_space_id") or "").strip() and not s.get("uc_tables")]
    if not id_only:
        return {}
    id_to_name = _load_hcl(Path(env_dir) / "generated/abac.auto.tfvars").get("genie_space_id_to_name") or {}
    try:
        discovered, owners = load_discovered_footprint(env_dir)
    except FootprintError as exc:
        raise GovernedTablesError(str(exc)) from exc
    committed = committed_agent_tables(env_dir)
    found = {}
    for space in id_only:
        space_id = str(space["genie_space_id"]).strip()
        names = {space_id, str(space.get("name") or id_to_name.get(space_id) or "").strip()} - {""}
        tables = [table for table, agents in owners.items() if names & set(agents or [])]
        if not tables and not owners and len(spaces) == 1:
            tables = discovered  # single-agent discovery records no attribution
        qualified = []
        for table in tables:
            parts = table.strip().split(".")
            qualified.append(".".join([catalog] + parts if len(parts) == 2 and catalog else parts))
        if qualified:
            found[space_id] = _unique(qualified)
        elif committed.get(space_id):
            found[space_id] = committed[space_id]
    return found


def agent_tables(env_dir: Path) -> dict[str, list[str]]:
    """Each file-defined agent's tables (agents/<agent>.*) by name, and each
    ID-only inline agent's by genie_space_id, fully qualified."""
    from agent_files import AgentFileError, load_all
    try:
        agents = load_all(Path(env_dir))
    except AgentFileError as exc:
        raise GovernedTablesError(str(exc)) from exc
    catalog = str(_load_hcl(Path(env_dir) / "env.auto.tfvars").get("uc_catalog") or "").strip()
    found = {}
    for agent in agents:
        tables = []
        for table in agent["uc_tables"]:
            parts = table.strip().split(".")
            tables.append(".".join([catalog] + parts if len(parts) == 2 and catalog else parts))
        found[agent["name"]] = _unique(table for table in tables if _is_table(table))
    found.update(_id_only_tables(env_dir, catalog))
    return found


def _records_agents(env_dir: Path, agents: dict) -> bool:
    """Whether the record lists agent tables: the env has agent files, or its record already does."""
    if agents:
        return True
    try:
        return "agent_tables" in json.loads((Path(env_dir) / MANIFEST).read_text())
    except (OSError, ValueError, TypeError):
        return False


def save_governed_tables(env_dir: Path, tables: Iterable[str]) -> None:
    path = Path(env_dir) / MANIFEST
    record = {"tables": _unique(tables)}
    # An env whose agents are all inline with listed uc_tables keeps the record
    # it had: their tables are already in the governance-owned env.auto.tfvars.
    agents = agent_tables(env_dir)
    if _records_agents(env_dir, agents):
        record["agent_tables"] = dict(sorted(agents.items()))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, indent=2) + "\n")


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
    # The staged controller deliberately adopts an untouched legacy deployment
    # from this old address.  This guard runs before that controller (and before
    # its read-only description in `make plan`), so exempt only that exact case.
    from governance_policies import parse_treatment_value
    from scripts import migration

    record = migration.load_record(env_dir)
    # Each old-address instance's own value (as migration.deployment reads it),
    # not a per-column merge that a stable-key instance could overwrite.
    attrs = dict(migration._instances(migration._read_state(env_dir, "data_access"),
                                      "databricks_entity_tag_assignment", "assignments"))
    legacy_values = [str((attrs.get(key) or {}).get("tag_value") or str(key).rsplit("|", 1)[-1])
                     for key, _column in old]
    if (migration.pending(env_dir) and not migration.adopted(record)
            and all(not parse_treatment_value(value) for value in legacy_values)):
        print(
            f"NOTE: {len(old)} legacy treatment tag(s) are pending migration; "
            "the staged controller adopts them under the environment lock."
        )
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
    try:
        cfg = load_env_config(env_dir)
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
    rec.add_argument("--if-agent-files", action="store_true",
                     help="only in an env whose agents are in agents/ files or that already has a record "
                          "(promote-to's target)")
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
            # An inline-agent promotion writes no record, but keeps a committed one current.
            if (not args.if_agent_files or (args.env_dir / MANIFEST).is_file()
                    or _records_agents(args.env_dir, agent_tables(args.env_dir))):
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
    except Exception as exc:
        # Migration record/journal readers use their own Refused classes. Map
        # those expected data errors to the command's normal clean error path,
        # while leaving programming errors visible.
        from scripts import migration, staged_policy_apply
        if not isinstance(exc, (migration.Refused, staged_policy_apply.Refused)):
            raise
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
