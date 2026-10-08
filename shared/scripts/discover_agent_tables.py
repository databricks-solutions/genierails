#!/usr/bin/env python3
"""Discover the tables of Genie agents configured only by ID (read-only, no model).

make enable-classification runs this first, so the scan covers an imported
agent's tables without a prior make generate. It uses generate's read-only
Genie API path and writes data_access/discovered_uc_tables.auto.tfvars the same
way. The file attributes tables to agent names, not IDs, so every ID-only agent
is re-read on each run and its entries are replaced: a changed genie_space_id
can never keep the old agent's tables. Entries of other configured agents are
kept. Envs that list uc_tables are left alone. If any agent can't be read,
nothing is written and the existing discovery is kept.

Usage:
    python discover_agent_tables.py <env_dir>
"""

from __future__ import annotations

import sys
from pathlib import Path

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from scripts.footprint import FootprintError, load_discovered_footprint, load_hcl  # noqa: E402

DISCOVERED = Path("data_access") / "discovered_uc_tables.auto.tfvars"


def _str(value) -> str:
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value or "").strip()


def id_only_agents(config: dict) -> list[dict]:
    """Agents named only by ID whose tables come from discovery."""
    if config.get("uc_tables"):
        return []  # tables listed explicitly: the footprint is already known
    return [
        space for space in config.get("genie_spaces") or []
        if isinstance(space, dict)
        and _str(space.get("genie_space_id")) and not (space.get("uc_tables") or [])
    ]


def other_agent_names(env_dir: Path, config: dict) -> set[str]:
    """Names under which generate records the tables of the other configured agents."""
    try:
        id_to_name = load_hcl(env_dir / "generated" / "abac.auto.tfvars").get("genie_space_id_to_name") or {}
    except FootprintError:
        id_to_name = {}
    names = set()
    for space in config.get("genie_spaces") or []:
        if not isinstance(space, dict) or space in id_only_agents(config):
            continue
        space_id = _str(space.get("genie_space_id"))
        names |= {_str(space.get("name")), space_id, _str(id_to_name.get(space_id))}
    return names - {""}


def refreshed_footprint(
    existing: dict[str, list[str]], fresh: dict[str, list[str]], keep: set[str]
) -> dict[str, list[str]]:
    """Fresh ID-only discovery plus existing entries owned by other agents."""
    result: dict[str, list[str]] = {}
    for table, owners in existing.items():
        kept = [owner for owner in owners if owner in keep]
        if kept:
            result[table] = kept
    for table, owners in fresh.items():
        merged = result.setdefault(table, [])
        merged.extend(owner for owner in owners if owner not in merged)
    return result


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        print("usage: discover_agent_tables.py <env_dir>", file=sys.stderr)
        return 2
    env_dir = Path(argv[0]).resolve()
    try:
        config = load_hcl(env_dir / "env.auto.tfvars")
        if not config.get("enable_classification", False):
            return 0
        agents = id_only_agents(config)
        if not agents:
            explicit = list(config.get("uc_tables") or [])
            explicit.extend(
                table
                for space in config.get("genie_spaces") or []
                if isinstance(space, dict)
                for table in (space.get("uc_tables") or [])
            )
            if explicit:
                print("Using listed uc_tables instead of discovering tables from genie_space_id.")
            return 0
        _tables, existing = load_discovered_footprint(env_dir)
        keep = other_agent_names(env_dir, config)
    except FootprintError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    import generate_abac as ga

    print(f"=== Discover Genie agent tables ({env_dir.name}) ===")
    auth_cfg = {**load_hcl(env_dir / "auth.auto.tfvars"), **config}
    fresh: dict[str, list[str]] = {}
    for space in agents:
        space_id = _str(space.get("genie_space_id"))
        found, _config, title, complete = ga.fetch_tables_from_genie_space(space_id, auth_cfg)
        if not complete or not found:
            print(
                f"ERROR: could not read the tables of Genie agent {space_id}; nothing was "
                f"written (existing {DISCOVERED} kept).\n"
                f"  Check envs/{env_dir.name}/auth.auto.tfvars and the agent ID, then re-run "
                f"make enable-classification ENV={env_dir.name}.",
                file=sys.stderr,
            )
            return 1
        name = _str(space.get("name")) or title or space_id
        for table in found:
            owners = fresh.setdefault(table, [])
            if name not in owners:
                owners.append(name)
    table_agents = refreshed_footprint(existing, fresh, keep)
    try:
        ga.persist_discovered_uc_tables(
            env_dir / DISCOVERED, list(table_agents), table_agents=table_agents
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print("")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
