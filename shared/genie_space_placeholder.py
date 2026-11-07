#!/usr/bin/env python3
"""Fail fast when env.auto.tfvars or an agent file still holds an active ``<...>`` placeholder.

The dev-to-prod agent template (agents/<agent>.auto.tfvars) ships
``genie_space_id = "<your-genie-space-id>"`` and a placeholder acl_groups entry.
Real Genie agent IDs and group names never contain angle brackets, so any
``<...>`` value is an unfilled placeholder that would otherwise surface later as
a confusing API error. Each env file's agents/*.auto.tfvars are checked with it.

It also refuses a catalog_map that maps a catalog to itself unless the env sets
allow_identity_catalog_map = true (scripts/saved_settings.py, finding S2), so
every command behind this guard refuses a hand-edited identity map.
--promotion-destination skips that check: promote-to and promote validate the
map they are about to write (CATALOG_MAP= may replace the saved one).

Usage:
    python genie_space_placeholder.py [--advice] [--promotion-destination] <path/to/env.auto.tfvars> [<more env files>...]
"""

import os
import re
import sys
from pathlib import Path

PLACEHOLDER = "<your-genie-space-id>"


def _display_path(path: Path) -> str:
    resolved = Path(path).resolve()
    if resolved.parent.parent.name == "envs":
        return f"envs/{resolved.parent.name}/{resolved.name}"
    if resolved.parent.name == "agents" and resolved.parent.parent.parent.name == "envs":
        return f"envs/{resolved.parent.parent.name}/agents/{resolved.name}"
    return str(path)


def _link_display(path: Path) -> str:
    """_display_path for a symlink itself: only its parent is resolved, never the link."""
    parent = _display_path(Path(path).parent / "x")
    return f"{parent[:-1]}{Path(path).name}"


def _is_placeholder(value) -> bool:
    return isinstance(value, str) and ("<" in value or ">" in value)


def _acl_placeholder(acl_groups) -> str | None:
    return next((g for g in acl_groups if _is_placeholder(g)), None) if isinstance(acl_groups, list) else None


def placeholder_genie_space_ids(config: dict) -> list[str]:
    """Return every genie_spaces[].genie_space_id that is still a placeholder."""
    found = []
    for space in config.get("genie_spaces") or []:
        if not isinstance(space, dict):
            continue
        space_id = str(space.get("genie_space_id") or "")
        if "<" in space_id or ">" in space_id:
            found.append(space_id)
    return found


def _id_error(placeholder: str, path: Path) -> str:
    return (
        f"replace placeholder {placeholder} in {_display_path(path)} with your Genie agent ID "
        "(Genie UI > your agent > Configure > About this agent > Agent ID, "
        "or the <id> in its /genie/rooms/<id> URL)"
    )


def _acl_error(placeholder: str, path: Path) -> str:
    return (f"replace placeholder {placeholder} in {_display_path(path)} with the groups that may run the "
            "agent (acl_groups: who gets CAN_RUN; [] means nobody)")


def _parse_error(path: Path) -> str:
    # Never the parser's message: it can quote the file.
    return f"fix {_display_path(path)}: it is not valid HCL"


def agent_file_placeholder_errors(settings: dict, path: Path) -> list[str]:
    """Every unfilled placeholder in one agents/<agent>.auto.tfvars."""
    errors = []
    if _is_placeholder(settings.get("genie_space_id")):
        errors.append(_id_error(settings["genie_space_id"], path))
    placeholder = _acl_placeholder(settings.get("acl_groups"))
    if placeholder:
        errors.append(_acl_error(placeholder, path))
    return errors


def agent_file_placeholder_error(settings: dict, path: Path) -> str | None:
    """The first unfilled placeholder in one agents/<agent>.auto.tfvars, else None."""
    return next(iter(agent_file_placeholder_errors(settings, path)), None)


def agent_files_placeholder_errors(env_dir: Path) -> list[str]:
    """Every unfilled placeholder in the env's agents/*.auto.tfvars."""
    from agent_files import broken_links, broken_links_error

    broken = broken_links(Path(env_dir))
    if broken:  # never read through: a loop or dangling link would crash, or read as no agent
        return [broken_links_error([_link_display(path) for path in broken])]
    errors = []
    for path in sorted((Path(env_dir) / "agents").glob("*.auto.tfvars")):
        text = path.read_text()
        if "<" not in text and ">" not in text:
            continue
        import hcl2

        try:
            settings = hcl2.loads(text)
        except Exception:
            errors.append(_parse_error(path))
            continue
        errors.extend(agent_file_placeholder_errors(settings, path))
    return errors


def agent_files_placeholder_error(env_dir: Path) -> str | None:
    """The first unfilled placeholder in the env's agents/*.auto.tfvars, else None."""
    return next(iter(agent_files_placeholder_errors(env_dir)), None)


def placeholder_errors(config: dict, path: Path, *, allow_access_tier_placeholders: bool = False) -> list[str]:
    """Every actionable placeholder error in one env.auto.tfvars, in the order they are fixed."""
    spaces = [space for space in config.get("genie_spaces") or [] if isinstance(space, dict)]
    # Every agent ID first, then every acl_groups placeholder.
    errors = [_id_error(found, path) for found in placeholder_genie_space_ids(config)]
    errors += [_acl_error(placeholder, path) for placeholder in
               (_acl_placeholder(space.get("acl_groups")) for space in spaces) if placeholder]
    deployer = config.get("deployer_principal_id")
    if isinstance(deployer, str) and deployer.startswith("<") and deployer.endswith(">"):
        errors.append(f"replace placeholder {deployer} in {_display_path(path)} with the application ID of the "
                      "service principal that applies this env")
    groups = config.get("access_tier_groups")
    if isinstance(groups, list) and not allow_access_tier_placeholders:
        placeholder = next((group for group in groups if isinstance(group, str)
                            and group.startswith("<") and group.endswith(">")), None)
        if placeholder:
            errors.append(f"replace placeholder {placeholder} in {_display_path(path)} with an IdP-synced group "
                          "name; replace every access_tier_groups placeholder, most to least privileged")
    return errors


def placeholder_error(config: dict, path: Path, *, allow_access_tier_placeholders: bool = False) -> str | None:
    """Return the first actionable placeholder error, else None."""
    return next(iter(placeholder_errors(
        config, path, allow_access_tier_placeholders=allow_access_tier_placeholders)), None)


def main() -> int:
    advice = "--advice" in sys.argv[1:]
    destination = "--promotion-destination" in sys.argv[1:]
    paths = [Path(arg) for arg in sys.argv[1:] if arg not in ("--advice", "--promotion-destination")]
    for path in paths:
        if path.is_file() and len(re.findall(r"(?m)^\s*catalog_map\s*=", path.read_text())) > 1:
            print(f"ERROR: {_display_path(path)}: catalog_map is set twice; keep one", file=sys.stderr)
            return 1
    errors = []
    for path in paths:
        # An agent's ID is reported first, as when it lived in env.auto.tfvars.
        errors.extend(agent_files_placeholder_errors(path.parent))
        if not path.is_file():
            continue  # a missing file passes (ENV=account, a destination not created yet)
        # Every supplied env file is parsed once, so a malformed one is always
        # reported, --promotion-destination or not.
        import hcl2

        text = path.read_text()
        try:
            config = hcl2.loads(text)
        except Exception:
            errors.append(_parse_error(path))
            continue
        if "<" in text or ">" in text:
            errors.extend(placeholder_errors(
                config, path,
                allow_access_tier_placeholders=os.environ.get("GENIERAILS_ALLOW_ACCESS_TIER_PLACEHOLDERS") == "1",
            ))
        # --promotion-destination only drops the identity check: promote-to and
        # promote validate the map they are about to write.
        if not destination and re.search(r"(?m)^\s*catalog_map\s*=", text):
            # Reported with the placeholder problems, after them (S2).
            sys.path.insert(0, str(Path(__file__).resolve().parent))
            from scripts.saved_settings import config_identity_catalog_map_error

            error = config_identity_catalog_map_error(config, path)
            if error:
                errors.append(error)
    # Every problem at once (each once, though a guard may pass one file twice).
    for error in dict.fromkeys(errors):
        if advice:
            print(f"  Setup required: {error}")
        else:
            print(f"ERROR: {error}", file=sys.stderr)
    return 1 if errors and not advice else 0


if __name__ == "__main__":
    raise SystemExit(main())
