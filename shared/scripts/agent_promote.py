#!/usr/bin/env python3
"""promote-to's agent step: carry captured agents from the source env to the target.

It reads committed source and target files and never live Databricks. Before
seeding a missing target agent file it also reads the target's detached agent
files and the detached-agent lines in its ack.txt, so a removed detached agent
cannot be silently recreated.

- agents/<agent>.space.json is promote-to output: rewritten every time from
  the source capture through remap_payload (JSON-aware: catalog names and the
  warehouse ID are replaced on identifier boundaries inside string values,
  never in keys or prose; every item ID is kept).
- agents/<agent>.auto.tfvars is env-owned: the first promotion seeds it
  (the remapped display name, the target's own key, acl_groups from the
  source through a deterministic target's group_map, genie_space_id empty
  because the target's ID lives in its state and release receipt); later
  promotions keep it and print how its acl_groups differ from the source's
  (also through group_map).
- A source agent is the target agent with the same file name, else the one
  with its remapped title, else the one with that title's Terraform key; so a
  target split on its own is never seeded a second agent.

    agent_promote.py --source-dir envs/dev --dest-dir envs/prod --catalog-map dev_cat=prod_cat
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

SHARED = Path(__file__).resolve().parents[1]
if str(SHARED) not in sys.path:
    sys.path.insert(0, str(SHARED))

import agent_files  # noqa: E402
from group_map import map_groups as _map_groups, mapping as _group_mapping  # noqa: E402
from scripts.ack_file import AckFileError, parse_file  # noqa: E402
from agent_files import AgentFileError  # noqa: E402
from scripts.agent_ownership import render  # noqa: E402
from scripts.saved_settings import catalog_map_dict  # noqa: E402

# A catalog is an identifier when it starts a dotted name: not preceded by an
# identifier character, a dot (then it is a schema or table name) or a
# backtick-quoted run, and followed by a dot (`cat`.x or cat.x).
_IDENT = r"A-Za-z0-9_"


def _catalog_pattern(catalog: str) -> re.Pattern:
    name = re.escape(catalog)
    return re.compile(rf"(?<![{_IDENT}.`$])(?:`{name}`|{name})(?=\.)")


def _warehouse_pattern(warehouse: str) -> re.Pattern:
    return re.compile(rf"(?<![{_IDENT}-]){re.escape(warehouse)}(?![{_IDENT}-])")


def remap_string(value: str, catalogs: dict[str, str], warehouses: dict[str, str]) -> str:
    if value in catalogs:
        return catalogs[value]
    if value in warehouses:
        return warehouses[value]
    for source in sorted(catalogs, key=len, reverse=True):
        destination = catalogs[source]
        value = _catalog_pattern(source).sub(
            lambda m: f"`{destination}`" if m.group(0).startswith("`") else destination, value)
    for source, destination in warehouses.items():
        value = _warehouse_pattern(source).sub(destination, value)
    return value


def remap_payload(value: Any, catalogs: dict[str, str], warehouses: dict[str, str] | None = None) -> Any:
    """Remap string values only; keys, numbers, booleans and prose stay as they are."""
    warehouses = warehouses or {}
    if isinstance(value, dict):
        return {key: remap_payload(item, catalogs, warehouses) for key, item in value.items()}
    if isinstance(value, list):
        return [remap_payload(item, catalogs, warehouses) for item in value]
    if isinstance(value, str):
        return remap_string(value, catalogs, warehouses)
    return value


def _hcl_list(values: list[str]) -> str:
    return "[" + ", ".join(json.dumps(v) for v in values) + "]"


def seed_settings(agent: dict, catalogs: dict[str, str], title: str, key: str, map_groups=None) -> str:
    acl_groups = agent["acl_groups"] or []
    if map_groups is not None:
        acl_groups = map_groups(acl_groups)
    lines = [
        f"# {agent['name']} in this env. Env-owned: promote-to wrote it once and never overwrites it.",
        "# The agent's ID lives in this env's Terraform state and release receipt, not here.",
        "# key is this env's Terraform key for the agent; do not change it.",
        f"key            = {json.dumps(key)}",
        f"name           = {json.dumps(title)}",
        'genie_space_id = ""',
        f"acl_groups     = {_hcl_list(acl_groups)}",
    ]
    explicit = agent_files._load_hcl(agent_files.settings_path(agent["_dir"], agent["name"])).get("uc_tables")
    if explicit:
        lines.append(f"uc_tables      = {_hcl_list([remap_string(t, catalogs, {}) for t in explicit])}")
    return "\n".join(lines) + "\n"


def group_mapper(source_dir: Path, dest_dir: Path):
    """Map group names the way remap_env_config.py does (decision 8): a
    deterministic target's group_map renames them; the source's
    raw_exempt_principals are never mapped; a legacy target maps nothing."""
    dest = agent_files._load_hcl(dest_dir / "env.auto.tfvars")
    if dest.get("governance_mode") != "deterministic":
        return lambda groups: list(groups or [])
    group_map = _group_mapping(dest)
    exempt = [str(p).strip() for p in agent_files._load_hcl(source_dir / "env.auto.tfvars").get(
        "raw_exempt_principals") or []]
    return lambda groups: _map_groups(groups or [], group_map, exempt)


def warehouse_map(source_dir: Path, dest_dir: Path) -> dict[str, str]:
    source = str(agent_files._load_hcl(source_dir / "env.auto.tfvars").get("sql_warehouse_id") or "").strip()
    dest = str(agent_files._load_hcl(dest_dir / "env.auto.tfvars").get("sql_warehouse_id") or "").strip()
    return {source: dest} if source and dest and source != dest else {}


def detached_dest_keys(dest_dir: Path) -> set[str]:
    agents = agent_files.load_all(dest_dir)
    configured = {agent["key"]: agent for agent in agents}
    path = dest_dir / "ack.txt"
    acked: set[str] = set()
    if path.is_file():
        try:
            acked = {entry.object_name for entry in parse_file(path, dest_dir.name)
                     if entry.kind == "detached-agent"}
        except AckFileError as exc:
            raise AgentFileError(f"{path}: {exc}") from exc
    invalid = sorted(key for key in acked if key not in configured or not configured[key]["detached"])
    if invalid:
        raise AgentFileError(
            f"{', '.join(invalid)} is retired in {dest_dir.name}; remove its detached-agent line from "
            f"envs/{dest_dir.name}/ack.txt in a governance-reviewed PR to revive it; nothing was written"
        )
    return acked | {key for key, agent in configured.items() if agent["detached"]}


def _id_to_name(env_dir: Path) -> dict:
    for path in (env_dir / "generated" / "abac.auto.tfvars", env_dir / "abac.auto.tfvars"):
        found = agent_files._load_hcl(path).get("genie_space_id_to_name")
        if isinstance(found, dict) and found:
            return found
    return {}


def target_title(agent: dict, catalogs: dict[str, str], id_to_name: dict) -> str:
    """The source agent's title as the target shows it: its display name (else its ID's title), remapped."""
    title = agent["display_name"] or str(id_to_name.get(agent["genie_space_id"]) or "")
    if not title:
        raise AgentFileError(
            f"{agent['_dir'].name}/agents/{agent['name']}{agent_files.SETTINGS_SUFFIX} has no name and its ID "
            f"{agent['genie_space_id'] or '(none)'} has no title in genie_space_id_to_name, so promote-to can't "
            "title it in the target; add name = \"<title>\" there (its key keeps the address); nothing was written")
    return remap_string(title, catalogs, {})


def match_targets(source_agents: list[dict], dest_dir: Path, catalogs: dict[str, str]) -> dict[str, dict]:
    """source agent name -> its target agent: {"name", "key", "title", "new"} (refuses ambiguity)."""
    existing = agent_files.load_all(dest_dir)
    id_to_name = _id_to_name(source_agents[0]["_dir"]) if source_agents else {}
    taken = {agent["key"] for agent in existing}
    claimed: dict[str, str] = {}
    result = {}
    for agent in source_agents:
        if not agent_files.capture_path(agent["_dir"], agent["name"]).is_file():
            continue  # not promoted (promote reports it)
        title = target_title(agent, catalogs, id_to_name)
        by_name = [t for t in existing if t["name"] == agent["name"]]
        by_title = [t for t in existing if t["display_name"] == title]
        by_key = [t for t in existing if t["key"] == agent_files.space_key(title)]
        candidates = by_name or by_title or by_key
        others = [t["name"] for t in by_title if by_name and t["name"] != by_name[0]["name"]]
        if len(candidates) > 1 or others:
            names = sorted({t["name"] for t in candidates} | set(others))
            raise AgentFileError(
                f"{agent['name']!r} ({title!r} in {dest_dir.name}) matches more than one {dest_dir.name} agent "
                f"({', '.join(names)}); rename one in a PR so each agent has one counterpart; nothing was written")
        if candidates:
            target = candidates[0]
            match = {"name": target["name"], "key": target["key"], "title": title, "new": False}
        else:
            if agent["name"] in agent_files.names(dest_dir):
                raise AgentFileError(f"{dest_dir.name}/agents/{agent['name']}.* is another agent; nothing was written")
            key = agent_files.space_key(title)
            if not agent_files.KEY.fullmatch(key):
                key = agent_files.space_key(agent["name"])
            base, n = key, 2
            while key in taken:
                key, n = f"{base}--{n}", n + 1
            match = {"name": agent["name"], "key": key, "title": title, "new": True}
        if match["name"] in claimed:
            raise AgentFileError(f"{claimed[match['name']]!r} and {agent['name']!r} both map to {dest_dir.name}'s "
                                 f"agent {match['name']!r}; nothing was written")
        claimed[match["name"]] = agent["name"]
        taken.add(match["key"])
        result[agent["name"]] = match
    return result


def refuse_detached_recreation(source_agents: list[dict], dest_dir: Path, targets: dict[str, dict]) -> set[str]:
    """Refuse before writing when promotion would resurrect a detached target agent.

    A detached target settings file is the committed tombstone left by the
    two-step detach flow. Identity is the target agent's Terraform key (its
    match from match_targets), not its display name, so spelling/case
    variants cannot adopt it again.
    """
    tombstones = {
        agent["key"]: agent["name"]
        for agent in agent_files.load_all(dest_dir)
        if agent["detached"]
    }
    keys = detached_dest_keys(dest_dir)
    blocked = sorted(
        (agent["name"], tombstones.get(targets[agent["name"]]["key"], targets[agent["name"]]["name"]))
        for agent in source_agents
        if not agent["detached"] and not agent["delete"]
        and agent_files.capture_path(agent["_dir"], agent["name"]).is_file()
        and targets[agent["name"]]["key"] in keys
    )
    if blocked:
        source_env = source_agents[0]["_dir"].name
        names = ", ".join(
            repr(source) if source == target else f"{source!r} (detached as {target!r})"
            for source, target in blocked
        )
        raise AgentFileError(
            f"{names} is committed detached in {dest_dir.name}; refusing to promote the active source over its "
            f"tombstone. Detach it in {source_env} too, or remove the detached-agent line from "
            f"envs/{dest_dir.name}/ack.txt in a governance-reviewed PR to revive it; nothing was written"
        )
    return keys


def promote(source_dir: Path, dest_dir: Path, catalogs: dict[str, str]) -> list[str]:
    """Write the target's agent files; returns the agents carried."""
    # Validate governance-owned tombstones even when the source no longer has
    # any agent files. Otherwise the early return would let an agent-only prod
    # edit revive a retired identity without governance review.
    detached_keys = detached_dest_keys(dest_dir)
    source_agents = agent_files.load_all(source_dir)
    if not source_agents:
        return []
    source_agents = [agent | {"_dir": source_dir} for agent in source_agents]
    # A target still in the legacy layout keeps these agents inline, with its
    # own (env-owned) acl_groups; carrying them as files would replace those
    # silently. Moving them is the explicit, reviewed make split-agent-files.
    # An inline agent is matched by its key and title, as remapped for the target.
    spaces = [space for space in agent_files._load_hcl(dest_dir / "env.auto.tfvars").get("genie_spaces") or []
              if isinstance(space, dict)]
    inline = set(agent_files.terraform_keys(spaces)) | {str(space.get("name") or "") for space in spaces} - {""}
    id_to_name = _id_to_name(source_dir)
    clash = sorted(agent["name"] for agent in source_agents
                   if agent_files.space_key(agent["name"]) in inline
                   or (agent_files.capture_path(source_dir, agent["name"]).is_file()
                       and ({target_title(agent, catalogs, id_to_name),
                             agent_files.space_key(target_title(agent, catalogs, id_to_name))} & inline)))
    if clash:
        raise AgentFileError(
            f"{dest_dir.name} still has {', '.join(repr(n) for n in clash)} inline in env.auto.tfvars "
            f"genie_spaces, while {source_dir.name} keeps them in agents/ files. Run make split-agent-files "
            f"ENV={dest_dir.name}, review and commit it, then promote again; nothing was changed")
    targets = match_targets(source_agents, dest_dir, catalogs)
    detached_keys = refuse_detached_recreation(source_agents, dest_dir, targets)
    warehouses = warehouse_map(source_dir, dest_dir)
    map_groups = group_mapper(source_dir, dest_dir)
    folder = agent_files.agents_dir(dest_dir)
    folder.mkdir(parents=True, exist_ok=True)
    carried = []
    for agent in source_agents:
        name = agent["name"]
        capture = agent_files.capture_path(source_dir, name)
        if not capture.is_file():
            print(f"  {name}: not captured in {source_dir.name} yet (make capture ENV={source_dir.name} "
                  f'SPACE="{name}"); not promoted')
            continue
        target = targets[name]
        if target["key"] in detached_keys and not agent_files.capture_path(dest_dir, target["name"]).is_file():
            print(f"  {name}: detached in {dest_dir.name}; not promoted (its tombstone is kept)")
            continue
        payload = json.loads(capture.read_text())
        agent_files.capture_path(dest_dir, target["name"]).write_text(
            render(remap_payload(payload, catalogs, warehouses)))
        settings = agent_files.settings_path(dest_dir, target["name"])
        label = name if target["name"] == name else f"{name} -> {target['name']}"
        if target["new"]:
            settings.write_text(seed_settings(agent, catalogs, target["title"], target["key"], map_groups))
            print(f"  {label}: promoted; seeded agents/{settings.name} (acl_groups from {source_dir.name}, "
                  "through group_map)")
        else:
            kept = agent_files.load(dest_dir, target["name"])["acl_groups"]
            source_acl = map_groups(agent["acl_groups"])
            note = "" if kept == source_acl else (
                f"; {dest_dir.name} keeps acl_groups {kept} ({source_dir.name} has {source_acl}, "
                "through group_map)")
            print(f"  {label}: promoted{note}")
        carried.append(name)
    gone = sorted(set(agent_files.names(dest_dir)) - {t["name"] for t in targets.values()}
                  - {a["name"] for a in source_agents})
    for name in gone:
        print(f"  {name}: only in {dest_dir.name}; kept (remove it with the two-step detach, or delete = true)")
    return carried


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--dest-dir", type=Path, required=True)
    parser.add_argument("--catalog-map", default="")
    args = parser.parse_args(argv)
    try:
        catalogs = catalog_map_dict(args.catalog_map) if args.catalog_map.strip() else {}
        if agent_files.names(args.source_dir):
            print(f"=== Promote agents ({args.source_dir.name} -> {args.dest_dir.name}) ===")
        promote(args.source_dir, args.dest_dir, catalogs)
    except (AgentFileError, ValueError, OSError) as exc:
        print(f"ERROR: promote-to agents: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
