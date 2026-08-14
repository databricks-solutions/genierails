#!/usr/bin/env python3
"""Compare a local Genie space definition against the one live in prod.

`bundle plan` reports that a space will be updated, but not what differs inside
it. Since a deploy overwrites prod's serialized_space wholesale, the customer
needs to see what they are about to lose -- especially instructions or example
queries someone added in the prod UI.

Exit codes:
  0  no differences
  3  differences found (distinct from 1 so callers can tell drift from failure)
  1  could not compare
"""

import argparse
import json
import subprocess
import sys

DIFFERENT = 3
ERROR = 1

# Keys worth calling out individually. Anything else falls back to a generic
# "changed" line, so an unrecognised schema still reports something useful.
TABLE_KEYS = ("data_sources", "tables")
TEXT_KEYS = ("instructions", "curated_questions", "example_queries", "comments")


def fetch_remote(space_id, profile):
    """Return prod's serialized_space as a parsed object, or None."""
    cmd = [
        "databricks", "api", "get",
        f"/api/2.0/genie/spaces/{space_id}?include_serialized_space=true",
    ]
    if profile:
        cmd += ["-p", profile]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None

    serialized = payload.get("serialized_space")
    if isinstance(serialized, str):
        try:
            return json.loads(serialized)
        except json.JSONDecodeError:
            return None
    return serialized


TABLE_NAME_KEYS = ("identifier", "table_name", "full_name", "table", "name")


def _table_names(node):
    """Collect table names from a data-source node of unknown exact shape.

    Real payloads nest these as data_sources.tables[].identifier, but the shape
    has changed across serialized_space versions, so this accepts a list, a dict
    wrapping a list, or plain strings rather than assuming one layout.
    """
    names = set()

    if isinstance(node, str):
        names.add(node)
    elif isinstance(node, list):
        for item in node:
            names |= _table_names(item)
    elif isinstance(node, dict):
        for key in TABLE_NAME_KEYS:
            value = node.get(key)
            if isinstance(value, str):
                names.add(value)
                break
        else:
            # No name on this level, so recurse into whatever it wraps.
            for value in node.values():
                if isinstance(value, (list, dict)):
                    names |= _table_names(value)

    return names


def _normalise_text_items(node):
    """Flatten an instruction/example list into comparable strings."""
    items = []
    if isinstance(node, list):
        for item in node:
            if isinstance(item, str):
                items.append(item.strip())
            elif isinstance(item, dict):
                parts = [
                    str(value).strip()
                    for key, value in sorted(item.items())
                    if isinstance(value, (str, int, float)) and key != "id"
                ]
                if parts:
                    items.append(" | ".join(parts))
    elif isinstance(node, str):
        items.append(node.strip())
    return items


def diff(local, remote):
    """Return a list of human-readable difference lines.

    Compares the two payloads key by key: table sets by membership, text-like
    lists by added/removed entries, everything else by equality.
    """
    if local == remote:
        return []

    lines = []
    local = local if isinstance(local, dict) else {}
    remote = remote if isinstance(remote, dict) else {}

    for key in sorted(set(local) | set(remote)):
        local_value = local.get(key)
        remote_value = remote.get(key)
        if local_value == remote_value:
            continue

        if key in TABLE_KEYS:
            local_tables = _table_names(local_value)
            remote_tables = _table_names(remote_value)
            for table in sorted(local_tables - remote_tables):
                lines.append(f"  + table {table}")
            for table in sorted(remote_tables - local_tables):
                lines.append(f"  - table {table} (present in prod, not in yours)")
            if local_tables == remote_tables:
                lines.append(f"  ~ {key} details changed (same table names)")
            continue

        if key in TEXT_KEYS:
            local_items = _normalise_text_items(local_value)
            remote_items = _normalise_text_items(remote_value)
            for item in local_items:
                if item not in remote_items:
                    lines.append(f"  + {key}: {_clip(item)}")
            for item in remote_items:
                if item not in local_items:
                    lines.append(
                        f"  - {key}: {_clip(item)}  (in prod only, will be lost)"
                    )
            continue

        if isinstance(local_value, (str, int, float, bool)) or local_value is None:
            lines.append(f"  ~ {key}: {_clip(remote_value)} -> {_clip(local_value)}")
        else:
            lines.append(f"  ~ {key} changed")

    return lines


def _clip(value, limit=90):
    text = "(absent)" if value is None else str(value)
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def prod_only_changes(lines):
    """Lines representing content that exists only in prod.

    These are the ones that mean a deploy destroys someone's work, so callers
    gate on them rather than on any difference at all.
    """
    return [line for line in lines if line.lstrip().startswith("-")]


def main():
    parser = argparse.ArgumentParser(
        description="Diff a local .geniespace.json against the live prod space."
    )
    parser.add_argument("local_file")
    parser.add_argument("space_id")
    parser.add_argument("--profile", default=None)
    args = parser.parse_args()

    try:
        with open(args.local_file) as handle:
            local = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"error: cannot read {args.local_file}: {exc}", file=sys.stderr)
        return ERROR

    remote = fetch_remote(args.space_id, args.profile)
    if remote is None:
        print(f"error: cannot read prod space {args.space_id}", file=sys.stderr)
        return ERROR

    lines = diff(local, remote)
    if not lines:
        print("  no differences; prod already matches your definition")
        return 0

    for line in lines:
        print(line)

    lost = prod_only_changes(lines)
    if lost:
        print(f"\n  {len(lost)} item(s) exist only in prod and will be "
              f"overwritten by this deploy.")

    return DIFFERENT


if __name__ == "__main__":
    sys.exit(main())
