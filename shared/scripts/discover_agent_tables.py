#!/usr/bin/env python3
"""Discover the tables of Genie agents configured only by ID (read-only, no model).

make enable-classification runs this first, so the scan covers an imported
agent's tables without a prior make generate. It uses generate's read-only
Genie API path and writes data_access/discovered_uc_tables.auto.tfvars the same
way, merged with what is already there. Envs that list uc_tables, and agents
whose tables are already discovered, are left alone. If any agent can't be read, nothing is
written and the existing discovery is kept.

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


def pending_agents(env_dir: Path) -> list[dict]:
    """ID-only agents whose tables are not yet in the discovered footprint."""
    config = load_hcl(env_dir / "env.auto.tfvars")
    if config.get("uc_tables"):
        return []  # tables listed explicitly: the footprint is already known
    spaces = [space for space in config.get("genie_spaces") or [] if isinstance(space, dict)]
    id_only = [
        space for space in spaces
        if _str(space.get("genie_space_id")) and not (space.get("uc_tables") or [])
    ]
    if not id_only:
        return []
    tables, agents = load_discovered_footprint(env_dir)
    if tables and not agents:
        return []  # legacy discovery without attribution: leave it as is
    owners = {owner for names in agents.values() for owner in names}
    try:
        id_to_name = load_hcl(env_dir / "generated" / "abac.auto.tfvars").get("genie_space_id_to_name") or {}
    except FootprintError:
        id_to_name = {}
    claimed: set[str] = set()
    unknown = []
    pending = []
    for space in spaces:
        name = _str(space.get("name")) or _str(id_to_name.get(_str(space.get("genie_space_id"))))
        if name:
            claimed.add(name)
        if not any(space is agent for agent in id_only):
            continue
        if not name:
            unknown.append(space)
        elif name not in owners:
            pending.append(space)
    # An agent named only by ID is recorded under its API title, which is
    # unknown until the API is read. Count titles no other agent claims.
    if len(owners - claimed) < len(unknown):
        pending.extend(unknown)
    return pending


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
        pending = pending_agents(env_dir)
    except FootprintError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    if not pending:
        return 0

    import generate_abac as ga

    print(f"=== Discover Genie agent tables ({env_dir.name}) ===")
    auth_cfg = {**load_hcl(env_dir / "auth.auto.tfvars"), **config}
    tables: list[str] = []
    table_agents: dict[str, list[str]] = {}
    for space in pending:
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
            tables.append(table)
            owners = table_agents.setdefault(table, [])
            if name not in owners:
                owners.append(name)
    try:
        ga.persist_discovered_uc_tables(
            env_dir / DISCOVERED, tables, table_agents=table_agents, merge_existing=True
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print("")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
