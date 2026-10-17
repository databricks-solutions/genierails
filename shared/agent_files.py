#!/usr/bin/env python3
"""Per-agent files: envs/<env>/agents/<agent>.auto.tfvars (+ <agent>.space.json).

One loader for every reader. Terraform gets the merge as a var-file that
terraform_layer.sh writes before each run; validate, table_readers and the
promotion remap call merged_config() directly, so all of them see the same
genie_spaces list.

An agent file holds:

    genie_space_id   = ""        # dev: the agent's ID. prod (any promoted
                                 # deterministic env): must stay "" -- kept in
                                 # state and the release receipt, never in git
    acl_groups       = ["analysts"]
    detached         = false     # true (with acl_groups = []) revokes CAN_RUN;
                                 # then make detach-agent forgets it
    delete           = false     # true deletes the agent (and its conversations)
                                 # on the next release; needs the committed ack
                                 # <env>:delete-agent:<key> in envs/<env>/ack.txt
    uc_tables        = []        # optional; else the captured payload's tables
    sql_warehouse_id = ""        # optional per-agent warehouse
    name             = ""        # optional display title (default: the file name)
    key              = ""        # optional immutable Terraform key (make
                                 # split-agent-files writes it to keep the
                                 # pre-split address); never change it

The file name is the agent's name. Its captured config, written by make
capture, is <agent>.space.json beside it. The agent's Terraform key -- its
resource addresses, .genie_space_id_<key>, .genie_adopted_<key> and its ack
lines -- is its explicit key, else the file name sanitized (disambiguated as
roots/workspace does for two that sanitize alike); never its display name, so
retitling an agent never moves it. load_all resolves it once for every reader.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import hcl2

AGENTS_DIR = "agents"
SETTINGS_SUFFIX = ".auto.tfvars"
CAPTURE_SUFFIX = ".space.json"
KEYS = {"key", "name", "genie_space_id", "acl_groups", "detached", "delete", "uc_tables", "sql_warehouse_id"}
# A name becomes a file name and a Terraform key: no paths, no quotes.
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9 _.-]*")
# An explicit key names state addresses and .genie_space_id_<key>: the shapes
# roots/workspace produces (sanitized name, name--<id|index>, an agent ID).
KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")
# Workspace-root resources keyed by the agent's key (modules/workspace).
STATE_RESOURCES = (
    "genie_space", "genie_space_acls", "genie_space_acls_removal_only",
    "genie_space_config_existing", "genie_space_config", "genie_space_acls_created",
    "genie_space_acls_created_removal_only",
)


class AgentFileError(ValueError):
    pass


def promoted(env_dir: Path) -> bool:
    """A deterministic promote-to target (promote_from set, or prod), as release_gate.env_role says."""
    cfg = _load_hcl(Path(env_dir) / "env.auto.tfvars")
    return cfg.get("governance_mode", "legacy") == "deterministic" and bool(
        cfg.get("promote_from") or Path(env_dir).name == "prod")


def _load_hcl(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        with path.open() as handle:
            return hcl2.load(handle)
    except Exception as exc:  # hcl2 raises lark errors
        raise AgentFileError(f"{path}: not valid HCL ({exc})") from exc


def check_key(key: str) -> str:
    if not KEY.fullmatch(key or ""):
        raise AgentFileError(
            f"Terraform key {key!r} is not allowed: use letters, digits, '_' or '-', starting with a letter or digit")
    return key


def check_name(name: str) -> str:
    if not NAME.fullmatch(name or "") or ".." in name:
        raise AgentFileError(
            f"agent name {name!r} is not allowed: use letters, digits, spaces, '_', '-' or '.', "
            "starting with a letter or digit")
    return name


def agents_dir(env_dir: Path) -> Path:
    return Path(env_dir) / AGENTS_DIR


def settings_path(env_dir: Path, name: str) -> Path:
    return agents_dir(env_dir) / f"{check_name(name)}{SETTINGS_SUFFIX}"


def capture_path(env_dir: Path, name: str) -> Path:
    return agents_dir(env_dir) / f"{check_name(name)}{CAPTURE_SUFFIX}"


def space_key(name: str) -> str:
    """The workspace root's key for a named space (merged_spaces in roots/workspace)."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def names(env_dir: Path) -> list[str]:
    folder = agents_dir(env_dir)
    if not folder.is_dir():
        return []
    return sorted(path.name[: -len(SETTINGS_SUFFIX)] for path in folder.glob(f"*{SETTINGS_SUFFIX}"))


def load(env_dir: Path, name: str) -> dict[str, Any]:
    """One agent's settings, validated, with defaults filled in and its Terraform key resolved."""
    settings_path(env_dir, name)
    for agent in load_all(env_dir):
        if agent["name"] == name:
            return agent
    raise AgentFileError(f"{settings_path(env_dir, name)} not found")


def _load_file(env_dir: Path, name: str) -> dict[str, Any]:
    path = settings_path(env_dir, name)
    raw = _load_hcl(path)
    unknown = sorted(set(raw) - KEYS)
    if unknown:
        raise AgentFileError(f"{path}: unknown setting(s) {', '.join(unknown)} (allowed: {', '.join(sorted(KEYS))})")
    acl = raw.get("acl_groups")
    if acl is not None and (not isinstance(acl, list) or not all(isinstance(g, str) and g for g in acl)):
        raise AgentFileError(f"{path}: acl_groups must be a list of group names")
    for flag in ("detached", "delete"):
        if not isinstance(raw.get(flag, False), bool):
            raise AgentFileError(f"{path}: {flag} must be true or false")
    tables = raw.get("uc_tables") or []
    if not isinstance(tables, list) or not all(isinstance(t, str) and t for t in tables):
        raise AgentFileError(f"{path}: uc_tables must be a list of table names")
    for text in ("key", "name"):
        if text in raw and not isinstance(raw[text], str):
            raise AgentFileError(f"{path}: {text} must be a string")
    if "key" in raw:
        try:
            check_key(raw["key"])
        except AgentFileError as exc:
            raise AgentFileError(f"{path}: {exc}") from exc
    agent = {
        "name": name,
        "display_name": raw.get("name", name),
        "key": raw.get("key", ""),  # resolved by load_all
        "explicit_key": "key" in raw,
        "genie_space_id": str(raw.get("genie_space_id") or "").strip(),
        "acl_groups": list(acl) if acl is not None else None,
        "detached": bool(raw.get("detached", False)),
        "delete": bool(raw.get("delete", False)),
        "uc_tables": list(tables),
        "sql_warehouse_id": str(raw.get("sql_warehouse_id") or "").strip(),
    }
    if agent["detached"] and agent["delete"]:
        raise AgentFileError(f"{path}: set detached or delete, not both")
    if agent["detached"] and agent["acl_groups"] != []:
        raise AgentFileError(f"{path}: a detached agent must have acl_groups = [] (detaching revokes CAN_RUN first)")
    if not agent["uc_tables"]:
        agent["uc_tables"] = captured_tables(env_dir, name)
    return agent


def captured_tables(env_dir: Path, name: str) -> list[str]:
    """The tables in an agent's captured payload (data_sources.tables[].identifier)."""
    path = capture_path(env_dir, name)
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise AgentFileError(f"{path}: not valid JSON ({exc})") from exc
    tables = ((payload.get("data_sources") or {}).get("tables") or []) if isinstance(payload, dict) else []
    return sorted({str(t.get("identifier")) for t in tables if isinstance(t, dict) and t.get("identifier")})


def terraform_keys(spaces: list) -> list[str]:
    """roots/workspace merged_spaces' for_each key for each genie_spaces entry, in order.

    An explicit key wins; otherwise the sanitized name (else the agent ID),
    suffixed --<agent ID, else list index> when an earlier entry has the same one.
    """
    keys, bases = [], []
    for index, space in enumerate(spaces):
        space = space if isinstance(space, dict) else {}
        base = space_key(str(space.get("name") or "")) or str(space.get("genie_space_id") or "")
        if space.get("key"):
            keys.append(str(space["key"]))
        else:
            keys.append(base if base not in bases else f"{base}--{space.get('genie_space_id') or index}")
        bases.append(base)
    return keys


def _resolve_keys(inline: list, agents: list[dict]) -> None:
    """Give every agent file its Terraform key, as the workspace root had it before keys were explicit.

    The root then saw the inline entries followed by the (not deleted) agent
    files, each named by its file name; a deleted agent kept the key it had
    while it was still listed. A file without key that sanitizes like an
    earlier entry or any explicit key gets the root's --<id|index> suffix.
    """
    explicit = {agent["key"] for agent in agents if agent["explicit_key"]}
    for include_deleted in (False, True):
        bases = [space_key(str(s.get("name") or "")) or str(s.get("genie_space_id") or "")
                 for s in inline if isinstance(s, dict)]
        index = len(inline)
        for agent in agents:
            if agent["delete"] and not include_deleted:
                continue
            base = space_key(agent["name"])
            if (agent["delete"] == include_deleted) and not agent["explicit_key"]:
                taken = base in bases or base in explicit
                agent["key"] = f"{base}--{agent['genie_space_id'] or index}" if taken else base
            bases.append(agent["key"].split("--", 1)[0] if agent["explicit_key"] else base)
            index += 1


def load_all(env_dir: Path) -> list[dict[str, Any]]:
    env_dir = Path(env_dir)
    result = [_load_file(env_dir, name) for name in names(env_dir)]
    folder = agents_dir(env_dir)
    if folder.is_dir():
        orphans = sorted(p.name[: -len(CAPTURE_SUFFIX)] for p in folder.glob(f"*{CAPTURE_SUFFIX}")
                         if not (folder / (p.name[: -len(CAPTURE_SUFFIX)] + SETTINGS_SUFFIX)).is_file())
        if orphans:
            raise AgentFileError(
                f"{folder}: captured config without settings for {', '.join(orphans)}; "
                f"add agents/<agent>{SETTINGS_SUFFIX} (genie_space_id, acl_groups)")
    if not result:
        return result
    inline = [s for s in (_load_hcl(env_dir / "env.auto.tfvars").get("genie_spaces") or []) if isinstance(s, dict)]
    _resolve_keys(inline, result)
    by_key: dict[str, list[str]] = {}
    for key, space in zip(terraform_keys(inline), inline):
        by_key.setdefault(key, []).append(f"env.auto.tfvars genie_spaces {space.get('name') or key!r}")
    for agent in result:
        by_key.setdefault(agent["key"], []).append(f"{AGENTS_DIR}/{agent['name']}{SETTINGS_SUFFIX}")
    duplicates = {key: owners for key, owners in by_key.items() if len(owners) > 1}
    if duplicates:
        key, owners = sorted(duplicates.items())[0]
        raise AgentFileError(f"{env_dir}: {' and '.join(owners)} share Terraform key {key!r}; "
                             "give one a distinct key")
    for agent in result:
        if agent["genie_space_id"] and promoted(env_dir):
            raise AgentFileError(
                f"{settings_path(env_dir, agent['name'])}: genie_space_id must be empty in {env_dir.name}: a "
                "promoted env's agent IDs live in its Terraform state and release receipt, never in git (to "
                f"adopt an existing agent, write its ID into its ID file .genie_space_id_{agent['key']}, in "
                f"{env_dir.name}/ or, with GENIERAILS_STATE_DIR set, the env's records/ there; "
                "release-plan then shows it)")
    return result


def key_of(env_dir: Path, name: str) -> str:
    """The Terraform key of agent ``name``: its agent file's, else its inline entry's (by name)."""
    env_dir = Path(env_dir)
    if name in names(env_dir):
        return load(env_dir, name)["key"]
    inline = [s for s in (_load_hcl(env_dir / "env.auto.tfvars").get("genie_spaces") or []) if isinstance(s, dict)]
    matches = [key for key, s in zip(terraform_keys(inline), inline) if str(s.get("name") or "") == name]
    return matches[0] if len(matches) == 1 else space_key(name)


def merged_config(env_dir: Path, cfg: dict | None = None) -> dict:
    """env.auto.tfvars with every agent file merged into genie_spaces.

    Agents marked delete = true are left out (the next apply removes them).
    A name defined both in env.auto.tfvars and in an agent file is refused.
    """
    env_dir = Path(env_dir)
    cfg = dict(cfg if cfg is not None else _load_hcl(env_dir / "env.auto.tfvars"))
    spaces = [dict(s) for s in (cfg.get("genie_spaces") or []) if isinstance(s, dict)]
    inline = {str(s.get("name") or "") for s in spaces} - {""}
    for agent in load_all(env_dir):
        entry = _space_entry(agent) | {"detached": agent["detached"]}
        if agent["name"] in inline or agent["display_name"] in inline:
            if entry in spaces:
                continue  # cfg is already a merge
            raise AgentFileError(
                f"agent {agent['name']!r} is defined in env.auto.tfvars genie_spaces and in "
                f"{AGENTS_DIR}/{agent['name']}{SETTINGS_SUFFIX}; keep only the agent file")
        if agent["delete"]:
            continue
        spaces.append(entry)
    if spaces or "genie_spaces" in cfg or names(env_dir):
        cfg["genie_spaces"] = spaces
    return cfg


def with_agents(env_file: Path, cfg: dict) -> dict:
    """``cfg`` (loaded from ``env_file``) with the env's agent files merged in.

    Only an env's own env.auto.tfvars gets the merge; any other file, or an
    env without agent files, is returned unchanged.
    """
    env_file = Path(env_file)
    if env_file.name != "env.auto.tfvars" or not names(env_file.parent):
        return cfg
    return merged_config(env_file.parent, cfg)


def captured_names(env_dir: Path) -> set[str]:
    """Agents whose config is in git (agents/<agent>.space.json): the capture, not the live agent, is their source."""
    return {name for name in names(env_dir) if capture_path(env_dir, name).is_file()}


def _space_entry(agent: dict) -> dict:
    entry = {
        "name": agent["display_name"],
        "genie_space_id": agent["genie_space_id"],
        "sql_warehouse_id": agent["sql_warehouse_id"],
        "uc_tables": agent["uc_tables"],
    }
    if agent["acl_groups"] is not None:
        entry["acl_groups"] = agent["acl_groups"]
    # Always explicit, so the root keys the agent exactly as every script does.
    entry["key"] = agent["key"]
    return entry


def state_keys(env_dir: Path) -> set[str]:
    """Agent keys the env's workspace state holds a module.workspace Genie resource for.

    Read through scripts/state_layout.py (in the checkout, GENIERAILS_STATE_DIR
    or remote state); a state that should exist but can't be read refuses.
    """
    from scripts.state_layout import state_json

    env_dir = Path(env_dir)
    try:
        state = state_json(env_dir, env_dir.name, "workspace") or {}
    except RuntimeError as exc:
        raise AgentFileError(str(exc)) from exc
    resources = state.get("resources") or []
    keys = set()
    for resource in resources:
        if resource.get("module") != "module.workspace" or resource.get("name") not in STATE_RESOURCES:
            continue
        for instance in resource.get("instances") or []:
            key = instance.get("index_key")
            if isinstance(key, str):
                keys.add(key)
    return keys


def agent_keys(agent: dict) -> set[str]:
    """Keys the workspace root may give this agent: its resolved key, else (an inline entry) plain or by ID."""
    if agent.get("key"):
        return {agent["key"]}
    base = space_key(agent.get("name", "")) or agent["genie_space_id"]
    keys = {base}
    if agent.get("genie_space_id"):
        keys.add(f"{base}--{agent['genie_space_id']}")
    return keys


def terraform_spaces(env_dir: Path, layer: str) -> list[dict] | None:
    """genie_spaces for one Terraform layer, or None when there are no agent files.

    A detached agent stays in the workspace config only while the workspace
    state still holds it (so a release can revoke its CAN_RUN); once
    make detach-agent has forgotten it, it is left out, so Terraform neither
    recreates nor destroys it. The data_access layer keeps it: its tables stay
    governed.
    """
    env_dir = Path(env_dir)
    if not names(env_dir):
        return None
    cfg = merged_config(env_dir)
    in_state = state_keys(env_dir) if layer == "workspace" else set()
    spaces = []
    file_by_key = {agent["key"]: agent["name"] for agent in load_all(env_dir)}
    for space in cfg.get("genie_spaces") or []:
        detached = space.pop("detached", False)
        if detached and layer == "workspace" and not (agent_keys(space) & in_state):
            continue
        if layer == "workspace" and space.get("key") in file_by_key:
            space["captured"] = capture_path(env_dir, file_by_key[space["key"]]).is_file()
        spaces.append(space)
    return spaces


def write_tfvars(env_dir: Path, layer: str, out: Path) -> bool:
    spaces = terraform_spaces(env_dir, layer)
    if spaces is None:
        return False
    out.write_text(json.dumps({"genie_spaces": spaces}, indent=2, sort_keys=True) + "\n")
    return True


# ── make split-agent-files: move inline genie_spaces into agent files ───────
#
# The explicit, reviewed migration from the legacy layout (every agent inline
# in env.auto.tfvars) to the persona layout (design 18.1: agent settings only
# in agents/ files, so an agent-only PR never touches a governance file).
# Nothing else moves this; an env that never runs it keeps working as it is.

SPLIT_KEYS = {"name", "genie_space_id", "acl_groups", "uc_tables", "sql_warehouse_id"}
SPLIT_COMMENT = "# Agents: one file each in agents/<agent>.auto.tfvars (moved there by make split-agent-files)."


def _top_level_list(text: str, key: str) -> tuple[int, int] | None:
    """(start, end) of a top-level ``key = [ ... ]`` assignment, string-aware, or None."""
    match = re.search(rf"^{re.escape(key)}\s*=\s*\[", text, re.MULTILINE)
    if not match:
        return None
    depth, quoted, escaped = 0, False, False
    for index in range(match.end() - 1, len(text)):
        char = text[index]
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
            continue
        if char == '"':
            quoted = True
        elif char in "[{":
            depth += 1
        elif char in "]}":
            depth -= 1
            if depth == 0:
                end = index + 1
                return match.start(), end + (1 if text[end:end + 1] == "\n" else 0)
    return None


def _render_agent(space: dict, key: str) -> str:
    title = str(space.get("name") or "")
    lines = [f"# {title or space.get('genie_space_id')}: moved from env.auto.tfvars genie_spaces by make split-agent-files.",
             "# key preserves the pre-split Terraform resource addresses; do not change it.",
             f"key            = {json.dumps(key)}",
             f"name           = {json.dumps(title)}",
             f"genie_space_id = {json.dumps(str(space.get('genie_space_id') or ''))}"]
    if space.get("acl_groups") is not None:
        lines.append("acl_groups     = [" + ", ".join(json.dumps(g) for g in space["acl_groups"]) + "]")
    if space.get("uc_tables"):
        lines.append("uc_tables      = [" + ", ".join(json.dumps(t) for t in space["uc_tables"]) + "]")
    if str(space.get("sql_warehouse_id") or ""):
        lines.append(f"sql_warehouse_id = {json.dumps(str(space['sql_warehouse_id']))}")
    return "\n".join(lines) + "\n"


def _as_terraform(space: dict) -> dict:
    """A genie_spaces entry as Terraform types it (optional attributes defaulted)."""
    return {
        "name": str(space.get("name") or ""),
        "genie_space_id": str(space.get("genie_space_id") or ""),
        "sql_warehouse_id": str(space.get("sql_warehouse_id") or ""),
        "uc_tables": list(space.get("uc_tables") or []),
        "acl_groups": space.get("acl_groups"),
        "delete": bool(space.get("delete", False)),
    }


def _keyed(spaces: list) -> dict[str, dict]:
    """genie_spaces entries by their workspace-root key, as Terraform types them."""
    return dict(zip(terraform_keys(spaces), map(_as_terraform, spaces)))


def _parse_file_names(value: str) -> dict[str, str]:
    """AGENT_FILE_NAMES syntax: comma-separated ``existing-terraform-key=agent-file-name`` pairs."""
    result = {}
    for item in filter(None, (part.strip() for part in value.split(","))):
        old, sep, filename = item.partition("=")
        if not sep or not old or not filename:
            raise AgentFileError("AGENT_FILE_NAMES must be existing-terraform-key=agent-file-name "
                                 "(comma-separate mappings)")
        if old in result:
            raise AgentFileError(f"AGENT_FILE_NAMES maps {old!r} more than once")
        result[old] = check_name(filename)
    return result


def split_problems(env_dir: Path, spaces: list, filenames: list[str], inline_keys: list[str]) -> list[str]:
    problems = []
    keys: dict[str, list[str]] = {}
    for agent in load_all(env_dir):
        keys.setdefault(agent["key"], []).append(agent["name"])
    for index, space in enumerate(spaces):
        if not isinstance(space, dict):
            problems.append(f"genie_spaces[{index}] is not an object")
            continue
        name = str(space.get("name") or "")
        label = f"genie_spaces[{index}] ({name or space.get('genie_space_id') or 'no name'})"
        if not inline_keys[index]:
            problems.append(f"{label} has neither a name nor genie_space_id, so it has no stable Terraform key")
            continue
        if not KEY.fullmatch(inline_keys[index]):
            problems.append(f"{label}: its Terraform key {inline_keys[index]!r} can't name an agent file's "
                            "key (letters, digits, '_' and '-' only); keep it inline")
            continue
        extra = sorted(set(space) - SPLIT_KEYS - {"delete"})
        if extra:
            problems.append(f"{label} sets {', '.join(extra)}, which agent files don't hold; keep it inline")
        if space.get("delete"):
            problems.append(f"{label} has delete = true; release the delete (or undo it) before moving the agent")
        acl = space.get("acl_groups")
        if acl is not None and (not isinstance(acl, list) or not all(isinstance(g, str) and g for g in acl)):
            problems.append(f"{label}: acl_groups must be a list of group names")
        tables = space.get("uc_tables") or []
        if not isinstance(tables, list) or not all(isinstance(t, str) and t for t in tables):
            problems.append(f"{label}: uc_tables must be a list of table names")
        if str(space.get("genie_space_id") or "").strip() and promoted(env_dir):
            problems.append(
                f"{label} has a genie_space_id, which a promoted env keeps in its Terraform state, "
                f"never in git; move it to .genie_space_id_{inline_keys[index]} first")
        filename = filenames[index]
        if settings_path(env_dir, filename).exists() or capture_path(env_dir, filename).exists():
            problems.append(f"{label}: {AGENTS_DIR}/{filename}.* already exists")
        keys.setdefault(inline_keys[index], []).append(filename)
    for key, owners in sorted(keys.items()):
        if len(owners) > 1:
            problems.append(f"agents {', '.join(repr(o) for o in owners)} share the Terraform key {key!r}; "
                            "give them distinct names first")
    return problems


def split(env_dir: Path, file_names: str = "") -> list[str]:
    """Move every inline genie_spaces entry into agents/<name>.auto.tfvars.

    Refuses, writing nothing, unless every entry can move with exactly the
    same Terraform input (same key, ID, tables, warehouse and acl_groups);
    after writing it re-reads both layouts and puts everything back if the
    merged agents differ. Returns the agents moved.
    """
    env_dir = Path(env_dir)
    env_file = env_dir / "env.auto.tfvars"
    text = env_file.read_text()
    before = _load_hcl(env_file)
    spaces = before.get("genie_spaces") or []
    if not spaces:
        return []
    mapping = _parse_file_names(file_names)
    inline_keys = terraform_keys(spaces)
    unknown = sorted(set(mapping) - set(inline_keys))
    if unknown:
        raise AgentFileError(f"{env_file}: nothing was moved: AGENT_FILE_NAMES refers to unknown Terraform "
                             f"key(s): {', '.join(unknown)}")
    filenames = []
    for space, key in zip(spaces, inline_keys):
        title = str(space.get("name") or "") if isinstance(space, dict) else ""
        default = title if NAME.fullmatch(title) and ".." not in title else key
        filenames.append(mapping.get(key, default))
    if len({f.lower() for f in filenames}) != len(filenames):
        raise AgentFileError(f"{env_file}: nothing was moved: two agents map to the same agent-file name "
                             "(file names differing only in case collide on macOS); name them with "
                             "AGENT_FILE_NAMES='existing-key=file-name,...'")
    problems = split_problems(env_dir, spaces, filenames, inline_keys)
    span = _top_level_list(text, "genie_spaces")
    if span is None and not problems:
        problems.append("genie_spaces is not a top-level list in env.auto.tfvars; move the agents by hand")
    if problems:
        raise AgentFileError(f"{env_file}: nothing was moved:\n  - " + "\n  - ".join(problems))
    expected = _keyed(merged_config(env_dir, before).get("genie_spaces") or [])
    folder = agents_dir(env_dir)
    created_dir = not folder.exists()
    written = []
    try:
        folder.mkdir(exist_ok=True)
        for space, filename, key in zip(spaces, filenames, inline_keys):
            path = settings_path(env_dir, filename)
            path.write_text(_render_agent(space, key))
            written.append(path)
        env_file.write_text(text[:span[0]] + SPLIT_COMMENT + "\ngenie_spaces = []\n" + text[span[1]:])
        after = _load_hcl(env_file)
        same_settings = ({k: v for k, v in after.items() if k != "genie_spaces"}
                         == {k: v for k, v in before.items() if k != "genie_spaces"})
        moved = _keyed(merged_config(env_dir).get("genie_spaces") or [])
        if not same_settings or moved != expected:
            raise AgentFileError(f"{env_file}: the moved agents would not match the inline ones; nothing was moved")
    except BaseException:
        env_file.write_text(text)
        for path in written:
            path.unlink(missing_ok=True)
        if created_dir and folder.is_dir() and not any(folder.iterdir()):
            folder.rmdir()
        raise
    return filenames


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("tfvars", "validate", "split"))
    parser.add_argument("--env-dir", type=Path, required=True,
                        help="envs/<env> (the workspace env dir, not data_access/)")
    parser.add_argument("--layer", choices=("workspace", "data_access"), default="workspace")
    parser.add_argument("--var-file", dest="var_file", type=Path)
    parser.add_argument("--file-names", default="",
                        help="split: existing-terraform-key=agent-file-name pairs (AGENT_FILE_NAMES)")
    args = parser.parse_args(argv)
    try:
        if args.command == "validate":
            merged_config(args.env_dir)
            return 0
        if args.command == "split":
            moved = split(args.env_dir, args.file_names)
            env = args.env_dir.name
            if not moved:
                print(f"envs/{env}/env.auto.tfvars has no inline genie_spaces; nothing to move.")
                return 0
            for name in moved:
                print(f"  moved {name!r} -> envs/{env}/{AGENTS_DIR}/{name}{SETTINGS_SUFFIX}")
            print(f"  envs/{env}/env.auto.tfvars: genie_spaces = [] (every other setting unchanged)")
            print("Terraform gets the same agents (same keys, IDs, tables, warehouses and acl_groups), so "
                  f"make plan ENV={env} shows no change. Review the diff, commit envs/{env}/env.auto.tfvars "
                  f"and envs/{env}/{AGENTS_DIR}/ in one PR.")
            return 0
        if args.var_file is None:
            parser.error("tfvars needs --var-file")
        # Exit 3: no agent files, nothing written.
        return 0 if write_tfvars(args.env_dir, args.layer, args.var_file) else 3
    except AgentFileError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
