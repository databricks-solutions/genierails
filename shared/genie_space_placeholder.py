#!/usr/bin/env python3
"""Fail fast when env.auto.tfvars still holds a template genie_space_id placeholder.

The dev-to-prod template ships ``genie_space_id = "<your-genie-space-id>"``.
Real Genie agent IDs never contain angle brackets, so any ``<...>`` value is an
unfilled placeholder that would otherwise surface later as a confusing API error.

Usage:
    python genie_space_placeholder.py <path/to/env.auto.tfvars>
"""

import sys
from pathlib import Path

PLACEHOLDER = "<your-genie-space-id>"


def _display_path(path: Path) -> str:
    resolved = Path(path).resolve()
    if resolved.parent.parent.name == "envs":
        return f"envs/{resolved.parent.name}/{resolved.name}"
    return str(path)


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


def placeholder_error(config: dict, path: Path) -> str | None:
    """Return a user-facing error when a placeholder ID is present, else None."""
    found = placeholder_genie_space_ids(config)
    if not found:
        return None
    return (
        f"replace {found[0]} in {_display_path(path)} with your Genie agent ID "
        "(Genie UI > your agent > Configure > About this agent > Agent ID, "
        "or the <id> in its /genie/rooms/<id> URL)"
    )


def main() -> int:
    import hcl2

    path = Path(sys.argv[1])
    if not path.exists():
        return 0
    with path.open() as handle:
        config = hcl2.load(handle)
    error = placeholder_error(config, path)
    if error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
