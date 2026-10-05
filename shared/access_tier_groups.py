"""User-owned ``access_tier_groups`` setting in a workspace env.auto.tfvars.

The setting is the consume-mode group->tier mapping: existing IdP-synced group
names, ordered most- to least-privileged. ``make generate`` reads it so the
mapping is typed once instead of repeated as ``GENERATE_ARGS='--groups ...'``,
and ``make promote`` carries it to the destination env so prod uses the same
tiers. Terraform declares it (default ``[]``) only so the shared file loads
without an undeclared-variable warning; no resource reads it.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path

from genie_space_placeholder import _display_path as display_path

SETTING = "access_tier_groups"

# An active (uncommented) top-level assignment, single- or multi-line.
_ACTIVE_ASSIGNMENT = re.compile(
    rf"^{SETTING}[ \t]*=[ \t]*\[[^\]]*\]", re.MULTILINE
)


def parse_groups_arg(value: str) -> list[str]:
    """Split a ``--groups 'a,b,c'`` value into ordered, non-empty names."""
    return [group.strip() for group in value.split(",") if group.strip()]


def persisted_access_tier_groups(config: dict, path: Path) -> list[str]:
    """Return the persisted ordered groups ([] when unset), or raise ValueError."""
    value = config.get(SETTING)
    if value is None:
        return []
    if not isinstance(value, list) or not all(
        isinstance(group, str) and group.strip() for group in value
    ):
        raise ValueError(
            f"{SETTING} in {display_path(path)} must be a list of group names, "
            'most to least privileged (e.g. ["payments_ops", "viewers"])'
        )
    groups = [group.strip() for group in value]
    duplicates = sorted({group for group in groups if groups.count(group) > 1})
    if duplicates:
        raise ValueError(
            f"{SETTING} in {display_path(path)} lists {', '.join(duplicates)} "
            "more than once; each access tier needs its own group"
        )
    return groups


def render(groups: list[str]) -> str:
    return f"{SETTING} = [{', '.join(json.dumps(group) for group in groups)}]"


def persist_access_tier_groups(path: Path, groups: list[str]) -> None:
    """Write ``groups`` into an env.auto.tfvars whose setting is unset or [].

    Replaces an existing active assignment in place, otherwise appends one.
    Comments and every other setting are left untouched.
    """
    target = Path(path).resolve()
    text = target.read_text()
    line = render(groups)
    if _ACTIVE_ASSIGNMENT.search(text):
        updated = _ACTIVE_ASSIGNMENT.sub(lambda _match: line, text, count=1)
    else:
        updated = text + ("" if text.endswith("\n") or not text else "\n")
        updated += (
            "\n# Access-tier groups, most to least privileged (saved by make generate).\n"
            f"{line}\n"
        )
    with tempfile.NamedTemporaryFile(
        mode="w", dir=target.parent, prefix=f".{target.name}.", delete=False
    ) as tmp:
        tmp.write(updated)
        tmp_path = Path(tmp.name)
    os.chmod(tmp_path, target.stat().st_mode & 0o777)
    os.replace(tmp_path, target)


def promoted_lines(config: dict, source_path: Path) -> list[str]:
    """Destination env.auto.tfvars lines carrying the source's tiers verbatim.

    Groups are account-level names, so no catalog remap applies. Returns []
    when the source has no persisted setting.
    """
    groups = persisted_access_tier_groups(config, source_path)
    if not groups:
        return []
    return [
        "",
        "# Same access tiers as the promoted rules (most to least privileged).",
        render(groups),
    ]
