#!/usr/bin/env python3
"""Fail fast when env.auto.tfvars or an agent file still holds an active ``<...>`` placeholder.

The dev-to-prod agent template (agents/<agent>.auto.tfvars) ships
``genie_space_id = "<your-genie-space-id>"`` and a placeholder acl_groups entry.
Real Genie agent IDs and group names never contain angle brackets, so any
``<...>`` value is an unfilled placeholder that would otherwise surface later as
a confusing API error. Each env file's agents/*.auto.tfvars are checked with it.

Usage:
    python genie_space_placeholder.py <path/to/env.auto.tfvars> [<more env files>...]
"""

import os
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


def agent_file_placeholder_error(settings: dict, path: Path) -> str | None:
    """The first unfilled placeholder in one agents/<agent>.auto.tfvars, else None."""
    if _is_placeholder(settings.get("genie_space_id")):
        return _id_error(settings["genie_space_id"], path)
    placeholder = _acl_placeholder(settings.get("acl_groups"))
    return _acl_error(placeholder, path) if placeholder else None


def agent_files_placeholder_error(env_dir: Path) -> str | None:
    """The first unfilled placeholder in the env's agents/*.auto.tfvars, else None."""
    for path in sorted((Path(env_dir) / "agents").glob("*.auto.tfvars")):
        text = path.read_text()
        if "<" not in text and ">" not in text:
            continue
        import hcl2

        error = agent_file_placeholder_error(hcl2.loads(text), path)
        if error:
            return error
    return None


def placeholder_error(config: dict, path: Path, *, allow_access_tier_placeholders: bool = False) -> str | None:
    """Return the first actionable placeholder error, else None."""
    found = placeholder_genie_space_ids(config)
    if found:
        return _id_error(found[0], path)
    for space in config.get("genie_spaces") or []:
        placeholder = _acl_placeholder(space.get("acl_groups")) if isinstance(space, dict) else None
        if placeholder:
            return _acl_error(placeholder, path)
    deployer = config.get("deployer_principal_id")
    if isinstance(deployer, str) and deployer.startswith("<") and deployer.endswith(">"):
        return (f"replace placeholder {deployer} in {_display_path(path)} with the application ID of the "
                "service principal that applies this env")
    groups = config.get("access_tier_groups")
    if isinstance(groups, list) and not allow_access_tier_placeholders:
        placeholder = next((group for group in groups if isinstance(group, str)
                            and group.startswith("<") and group.endswith(">")), None)
        if placeholder:
            return (f"replace placeholder {placeholder} in {_display_path(path)} with an IdP-synced group "
                    "name; replace every access_tier_groups placeholder, most to least privileged")
    return None


def main() -> int:
    advice = "--advice" in sys.argv[1:]
    paths = [Path(arg) for arg in sys.argv[1:] if arg != "--advice"]
    for path in paths:
        # An agent's ID is reported first, as when it lived in env.auto.tfvars.
        error = agent_files_placeholder_error(path.parent)
        # Legacy configs have no placeholders. Avoid importing the HCL parser in
        # bootstrap-only environments unless a placeholder can actually exist.
        if not error and path.is_file() and ("<" in path.read_text() or ">" in path.read_text()):
            import hcl2

            with path.open() as handle:
                config = hcl2.load(handle)
            error = placeholder_error(
                config, path,
                allow_access_tier_placeholders=os.environ.get("GENIERAILS_ALLOW_ACCESS_TIER_PLACEHOLDERS") == "1",
            )
        if error:
            if advice:
                print(f"  Setup required: {error}")
            else:
                print(f"ERROR: {error}", file=sys.stderr)
                return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
