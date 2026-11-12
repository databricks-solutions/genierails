"""Who can read each governed table: the env-owned ``table_readers`` setting.

In a deterministic env GenieRails grants ``SELECT`` only from::

    table_readers = {
      "cat.sch.customers" = ["payments_ops", "viewers"]
      "cat.sch.*"         = ["payments_ops"]          # same key syntax as uc_tables
    }

A table's readers are the union of every entry that matches it (the exact
table and its ``catalog.schema.*``). Every reader must be a tier group or in
``raw_exempt_principals``; ``account users`` is never one. Agents never grant
``SELECT``: an agent's ``acl_groups`` get ``CAN_RUN`` only, and every one of
them must be able to read each of the agent's tables through this setting.

Terraform resolves the same rule (modules/data_access); this module is the
authoring and checking side: ``show`` prints resolved readers, ``propose``
writes entries for an agent's uncovered tables the first time ``make
generate`` imports it (never editing an entry), and ``check`` refuses an agent
whose ``acl_groups`` can't read one of its tables.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Mapping

from governed_tables_record import committed_agent_tables

SETTING = "table_readers"
# Built-in principals that would reach everyone. Never a reader, never raw-exempt.
EVERYONE = frozenset({"account users", "users"})


def is_everyone(name: object) -> bool:
    """Whether ``name`` is a built-in everyone-principal (any case or spacing)."""
    return isinstance(name, str) and " ".join(name.split()).lower() in EVERYONE
# Agents whose first-import proposal was already written (committed record).
PROPOSED_RECORD = Path("generated") / "table_readers_proposed.json"


class TableReadersError(ValueError):
    """table_readers or an agent's access can't be resolved safely."""


def _load_hcl(path: Path) -> dict:
    if not path.is_file():
        return {}
    from scripts.hcl2_guard import import_hcl2

    hcl2 = import_hcl2()
    try:
        value = hcl2.loads(path.read_text())
    except Exception as exc:
        raise TableReadersError(f"cannot parse {path}: {exc}") from exc
    return value if isinstance(value, dict) else {}


def env_config(env_dir: Path, cfg: Mapping | None = None) -> dict:
    """env.auto.tfvars (or cfg) with the per-agent files merged into genie_spaces."""
    from agent_files import AgentFileError, merged_config

    try:
        return merged_config(Path(env_dir), dict(cfg) if cfg is not None else None)
    except AgentFileError as exc:
        raise TableReadersError(str(exc)) from exc


def deterministic(cfg: Mapping) -> bool:
    return cfg.get("governance_mode", "legacy") == "deterministic"


def full_name(name: str, uc_catalog: str = "") -> str:
    """``schema.table`` relative to uc_catalog becomes ``catalog.schema.table``."""
    parts = str(name).strip().split(".")
    if len(parts) == 2 and uc_catalog:
        parts = [uc_catalog] + parts
    return ".".join(parts)


def key_error(key: object) -> str | None:
    """Why a (fully qualified) table_readers key is invalid, or None."""
    if not isinstance(key, str):
        return f"{SETTING} keys must be strings, got {key!r}"
    parts = key.split(".")
    if len(parts) != 3 or not all(part.strip() == part and part for part in parts):
        return (f'{SETTING} key "{key}" must be catalog.schema.table or catalog.schema.* '
                "(or schema.table / schema.* with uc_catalog set)")
    if any(c in key for c in "?[]") or "*" in parts[0] or "*" in parts[1] or ("*" in parts[2] and parts[2] != "*"):
        return f'{SETTING} key "{key}" may use * only as the whole table name (catalog.schema.*)'
    return None


def normalize(table_readers: Mapping | None, uc_catalog: str = "") -> dict[str, list[str]]:
    """Fully qualified keys, readers deduplicated in order."""
    result: dict[str, list[str]] = {}
    for key, readers in (table_readers or {}).items():
        name = full_name(key, uc_catalog)
        merged = result.setdefault(name, [])
        merged.extend(r for r in readers or [] if r not in merged)
    return result


def matches(key: str, table: str) -> bool:
    """Whether a table_readers key applies to ``table`` (a table or a ``cat.sch.*`` pattern)."""
    key, table = key.lower(), table.lower()
    if key == table:
        return True
    return key.endswith(".*") and not table.endswith(".*") and table.rsplit(".", 1)[0] == key[:-2]


def readers_for(table: str, table_readers: Mapping[str, Iterable[str]]) -> list[str]:
    """The union of every entry matching ``table`` (sorted)."""
    found: set[str] = set()
    for key, readers in table_readers.items():
        if matches(key, table):
            found.update(readers)
    return sorted(found)


def covered(table: str, table_readers: Mapping[str, Iterable[str]]) -> bool:
    return any(matches(key, table) for key in table_readers)


def validation_errors(cfg: Mapping) -> list[str]:
    """Schema errors for ``table_readers`` (validate_config adds them)."""
    errors: list[str] = []
    mode = cfg.get("governance_mode", "legacy")
    readers = cfg.get(SETTING)
    if readers is None:
        return errors
    if mode != "deterministic":
        errors.append(f'{SETTING} applies only with governance_mode = "deterministic"; '
                      "legacy envs keep their own grants")
        return errors
    if not isinstance(readers, dict):
        return errors + [f"{SETTING} must be a map of table to reader groups"]
    tiers = [t for t in cfg.get("access_tier_groups") or [] if isinstance(t, str)]
    exempt = [p for p in cfg.get("raw_exempt_principals") or [] if isinstance(p, str)]
    allowed = set(tiers) | set(exempt)
    uc_catalog = str(cfg.get("uc_catalog") or "")
    seen: dict[str, str] = {}
    for key, groups in readers.items():
        name = full_name(key, uc_catalog) if isinstance(key, str) else key
        problem = key_error(name)
        if problem:
            errors.append(problem)
            continue
        if name.lower() in seen:
            errors.append(f'{SETTING} lists "{name}" twice ("{seen[name.lower()]}" and "{key}")')
        seen.setdefault(name.lower(), key)
        label = f'{SETTING}["{key}"]'
        if not isinstance(groups, list) or not all(isinstance(g, str) and g.strip() for g in groups):
            errors.append(f"{label} must be a list of non-empty group names")
            continue
        if len(groups) != len(set(groups)):
            errors.append(f"{label} must not list a group twice")
        for group in groups:
            if group.strip().lower() in EVERYONE:
                errors.append(f"{label} names {group!r}: GenieRails never grants SELECT to everyone")
            elif group not in allowed:
                errors.append(
                    f"{label} names {group!r}, which is neither in access_tier_groups nor in "
                    "raw_exempt_principals; a reader outside the tiers would get an unplanned mask")
    return errors


# ── Agents and their tables ─────────────────────────────────────────────────

def _discovered_attribution(env_dir: Path) -> dict[str, list[str]]:
    cfg = _load_hcl(env_dir / "data_access" / "discovered_uc_tables.auto.tfvars")
    agents = cfg.get("discovered_table_agents") or {}
    by_agent: dict[str, list[str]] = {}
    for table, owners in agents.items() if isinstance(agents, dict) else ():
        for owner in owners or []:
            by_agent.setdefault(str(owner), []).append(str(table))
    return by_agent


def _agent_name(space: Mapping, id_to_name: Mapping) -> str:
    space_id = str(space.get("genie_space_id") or "")
    return str(space.get("name") or "") or str(id_to_name.get(space_id, "")) or space_id


def attributed_tables(env_dir: Path, cfg: Mapping | None = None) -> tuple[dict[str, list[str]], bool]:
    """(tables, by_id): the tables make generate attributed to agents known only by ID.

    A deterministic env reads only the committed generated/governed_tables.json
    agent_tables, keyed by genie_space_id (by_id is true), the same record
    promote-to reads, so a clean clone and a laptop agree; the git-ignored
    discovery file is never read. Names are only labels: two agents may share
    one. A legacy env takes data_access/discovered_uc_tables.auto.tfvars, keyed
    by agent name.
    """
    env_dir = Path(env_dir)
    cfg = env_config(env_dir, cfg)
    if not deterministic(cfg):
        return _discovered_attribution(env_dir), False
    return committed_agent_tables(env_dir), True


def _agents(env_dir: Path, cfg: Mapping) -> list[dict]:
    """agents(), plus each agent's genie_space_id and whether it is an ID-only
    deterministic agent the committed record has no tables for."""
    generated = _load_hcl(env_dir / "generated" / "abac.auto.tfvars")
    id_to_name = generated.get("genie_space_id_to_name") or {}
    attributed, by_id = attributed_tables(env_dir, cfg)
    uc_catalog = str(cfg.get("uc_catalog") or "")
    result = []
    for space in cfg.get("genie_spaces") or []:
        if not isinstance(space, dict):
            continue
        space_id = str(space.get("genie_space_id") or "")
        name = _agent_name(space, id_to_name)
        listed = list(space.get("uc_tables") or [])
        tables = listed or (attributed.get(space_id, []) if by_id else attributed.get(name, []))
        result.append({
            "name": name,
            "acl_groups": space.get("acl_groups"),
            "tables": sorted({full_name(t, uc_catalog) for t in tables if t}),
            "genie_space_id": space_id,
            "uncommitted": by_id and bool(space_id) and not listed and not tables,
        })
    return result


def agents(env_dir: Path, cfg: Mapping | None = None) -> list[dict]:
    """Configured agents: name, acl_groups (None when omitted) and fully qualified tables.

    A space's own uc_tables win; a space known only by ID takes the tables
    make generate attributed to it (attributed_tables).
    """
    env_dir = Path(env_dir)
    return [{key: agent[key] for key in ("name", "acl_groups", "tables")}
            for agent in _agents(env_dir, env_config(env_dir, cfg))]


def access_problems(env_dir: Path, cfg: Mapping | None = None) -> list[str]:
    """Every acl_groups group that can't read one of its agent's tables through table_readers.

    Decision 4: a deterministic agent without acl_groups stops the run
    ([] means nobody).
    """
    env_dir = Path(env_dir)
    cfg = env_config(env_dir, cfg)
    if not deterministic(cfg):
        return []
    problems: list[str] = []
    readers = normalize(cfg.get(SETTING), str(cfg.get("uc_catalog") or ""))
    for agent in _agents(env_dir, cfg):
        groups = agent["acl_groups"]
        if groups is None:
            problems.append(f"agent {agent['name']} has no acl_groups: list the groups that may run it "
                            "([] means nobody)")
            continue
        if not groups:
            continue
        if agent["uncommitted"]:
            problems.append(
                f"agent {agent['name']}: generated/governed_tables.json has no agent_tables entry for "
                f"genie_space_id {agent['genie_space_id']}; run make generate ENV={env_dir.name} "
                "and commit generated/")
            continue
        if not agent["tables"]:
            problems.append(f"agent {agent['name']}: no tables are known for it; run make generate first")
            continue
        for table in agent["tables"]:
            can_read = set(readers_for(table, readers))
            for group in groups:
                if group not in can_read:
                    problems.append(
                        f"agent {agent['name']}: acl_groups group {group!r} cannot read {table} "
                        f"(add it to table_readers[\"{table}\"] or a matching catalog.schema.* entry)")
    return problems


# ── Proposals written by make generate ─────────────────────────────────────

def _hcl_string(value: str) -> str:
    return json.dumps(value).replace("${", "$${").replace("%{", "%%{")


def _proposed_agents(env_dir: Path) -> list[str]:
    path = Path(env_dir) / PROPOSED_RECORD
    if not path.is_file():
        return []
    try:
        names = json.loads(path.read_text()).get("agents")
    except (OSError, ValueError, AttributeError) as exc:
        raise TableReadersError(f"cannot read {path}: {exc}") from exc
    if not isinstance(names, list) or not all(isinstance(n, str) for n in names):
        raise TableReadersError(f"{path}: agents must be a list of names")
    return names


def proposals(env_dir: Path, cfg: Mapping | None = None) -> tuple[dict[str, list[str]], list[str]]:
    """(new entries, newly imported agents): uncovered tables of first-imported agents.

    Only tables no entry (exact or wildcard) covers get one, filled with the
    agent's acl_groups; groups that can't be readers (outside the tiers and
    raw_exempt_principals) are left out, so the access check names them.
    """
    env_dir = Path(env_dir)
    cfg = env_config(env_dir, cfg)
    if not deterministic(cfg):
        return {}, []
    existing = normalize(cfg.get(SETTING), str(cfg.get("uc_catalog") or ""))
    allowed = set(cfg.get("access_tier_groups") or []) | set(cfg.get("raw_exempt_principals") or [])
    done = set(_proposed_agents(env_dir))
    new: dict[str, list[str]] = {}
    imported: list[str] = []
    for agent in agents(env_dir, cfg):
        if agent["name"] in done or not agent["tables"]:
            continue
        imported.append(agent["name"])
        groups = [g for g in agent["acl_groups"] or [] if g in allowed and g.lower() not in EVERYONE]
        if not groups:
            continue
        for table in agent["tables"]:
            if covered(table, existing):
                continue
            entry = new.setdefault(table, [])
            entry.extend(g for g in groups if g not in entry)
    return new, imported


def write_proposals(env_file: Path, new: Mapping[str, list[str]]) -> None:
    """Add ``new`` keys to table_readers in env.auto.tfvars; never edits an entry."""
    from access_tier_groups import _assignment_spans

    text = env_file.read_text()
    lines = [f"  {_hcl_string(table)} = [{', '.join(_hcl_string(g) for g in groups)}]"
             for table, groups in sorted(new.items())]
    comment = "  # Proposed by make generate for newly imported agents: review before committing."
    spans = _assignment_spans(text, SETTING)
    if len(spans) > 1:
        raise TableReadersError(f"{SETTING} is assigned more than once in {env_file}")
    if spans:
        start, end = spans[0]
        current = text[start:end]
        if not current.rstrip().endswith("}"):
            raise TableReadersError(f"{SETTING} in {env_file} is not a map; set it to a map first")
        close = current.rstrip().rfind("}")
        body = current[:close].rstrip()
        updated = text[:start] + body + "\n" + comment + "\n" + "\n".join(lines) + "\n}" + text[start + close + 1:]
    else:
        block = ("\n# Who can read each governed table (SELECT). Env-owned: prod keeps its own.\n"
                 f"{SETTING} = {{\n{comment}\n" + "\n".join(lines) + "\n}\n")
        updated = text.rstrip("\n") + "\n" + block
    env_file.write_text(updated)


def record_proposed(env_dir: Path, names: Iterable[str]) -> None:
    path = Path(env_dir) / PROPOSED_RECORD
    merged = sorted(set(_proposed_agents(env_dir)) | set(names))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"agents": merged}, indent=2) + "\n")


# ── Display ──────────────────────────────────────────────────────────────────

def governed_tables(env_dir: Path, cfg: Mapping) -> list[str]:
    """Tables to print readers for: table_readers keys, uc_tables and agent tables."""
    uc_catalog = str(cfg.get("uc_catalog") or "")
    tables = set(normalize(cfg.get(SETTING), uc_catalog))
    tables.update(full_name(t, uc_catalog) for t in cfg.get("uc_tables") or [])
    for agent in agents(env_dir, cfg):
        tables.update(agent["tables"])
    return sorted(tables, key=str.lower)


def describe(env_dir: Path, cfg: Mapping | None = None) -> list[str]:
    env_dir = Path(env_dir)
    cfg = env_config(env_dir, cfg)
    if not deterministic(cfg):
        return []
    if cfg.get(SETTING) is None:
        return ["Table readers: table_readers is not set, so GenieRails grants no SELECT "
                "(masks apply to whoever already has access)."]
    readers = normalize(cfg.get(SETTING), str(cfg.get("uc_catalog") or ""))
    lines = ["Table readers (SELECT; union of matching table_readers entries):"]
    for table in governed_tables(env_dir, cfg):
        names = readers_for(table, readers)
        lines.append(f"  {table}: {', '.join(names) if names else '(none)'}")
    revoked = [key for key in applied_grants(env_dir)
               if key.split("|", 1)[1] not in readers_for(key.split("|", 1)[0], readers)]
    if revoked:
        lines.append("Revokes SELECT (applied, but no table_readers entry grants it any more):")
        lines.extend(f"  {key}" for key in revoked)
    return lines


def applied_grants(env_dir: Path) -> list[str]:
    """``table|principal`` SELECT grants the data_access state holds (current instances)."""
    try:
        from scripts.state_layout import state_json
        state = state_json(Path(env_dir), Path(env_dir).name, "data_access") or {}
    except RuntimeError as exc:
        raise TableReadersError(str(exc)) from exc
    keys = []
    for resource in state.get("resources") or []:
        if (resource.get("module"), resource.get("type"), resource.get("name")) != (
                "module.data_access", "databricks_grant", "table_access"):
            continue
        for instance in resource.get("instances") or []:
            if instance.get("status") != "tainted" and not instance.get("deposed") and "|" in str(instance.get("index_key", "")):
                keys.append(str(instance["index_key"]))
    return sorted(keys)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("show", "propose", "check"):
        command = sub.add_parser(name)
        command.add_argument("--env-dir", required=True, type=Path)
        command.add_argument("--command-name", default="this command")
    args = parser.parse_args(argv)
    env_dir = args.env_dir
    try:
        cfg = env_config(env_dir)
        if args.command == "show":
            for line in describe(env_dir, cfg):
                print(line)
            return 0
        if args.command == "propose":
            new, imported = proposals(env_dir, cfg)
            if new:
                write_proposals(env_dir / "env.auto.tfvars", new)
                print(f"Proposed table_readers for newly imported agent(s) {', '.join(imported)} "
                      f"in {env_dir / 'env.auto.tfvars'} (review before committing):")
                for table, groups in sorted(new.items()):
                    print(f"  {table} = {', '.join(groups)}")
            if imported:
                record_proposed(env_dir, imported)
            for line in describe(env_dir, env_config(env_dir)):
                print(line)
            return 0
        problems = access_problems(env_dir, cfg)
        if problems:
            print(f"ERROR: {args.command_name} refused: CAN_RUN would be granted to groups that cannot "
                  "read the agent's tables (GenieRails grants SELECT only from table_readers):",
                  file=sys.stderr)
            for problem in problems:
                print(f"  - {problem}", file=sys.stderr)
            print("  Fix table_readers or acl_groups in env.auto.tfvars (make generate proposes entries "
                  "for a newly imported agent), then re-run.", file=sys.stderr)
            return 1
        return 0
    except TableReadersError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
