#!/usr/bin/env python3
"""Determine whether a Genie space is already tracked (bound) by the bundle.

A customer may have promoted spaces months ago, possibly from a different
machine. Title matching cannot answer "is this already managed by the bundle?" --
only the bundle's deployment state can, and `bundle summary --force-pull` reads
that state back from the workspace rather than from local files.

Binding state per resource key is one of:
  bound   - the bundle tracks it and knows its prod space ID
  unbound - the bundle config declares it but no prod ID is recorded
  absent  - the bundle config has no such resource key
"""

import argparse
import json
import subprocess
import sys

BOUND = "bound"
UNBOUND = "unbound"
ABSENT = "absent"


def _run_summary(target, profile, force_pull=True):
    """Return parsed `bundle summary -o json`, or None if it cannot be read.

    A bundle that has never been deployed to this target has no state, which the
    CLI reports as an error. That is an expected condition, not a failure.
    """
    cmd = ["databricks", "bundle", "summary", "-o", "json", "-t", target]
    if profile:
        cmd += ["-p", profile]
    if force_pull:
        cmd.append("--force-pull")

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout.strip():
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def genie_space_ids(summary):
    """Map resource key -> prod space ID for every Genie space in the summary.

    A key present with an empty ID is declared but not yet deployed.
    """
    if not summary:
        return {}

    spaces = (summary.get("resources") or {}).get("genie_spaces") or {}
    out = {}
    for key, value in spaces.items():
        if isinstance(value, dict):
            out[key] = str(value.get("id") or "")
        else:
            out[key] = ""
    return out


def classify(key, id_map):
    if key not in id_map:
        return ABSENT
    return BOUND if id_map[key] else UNBOUND


def get_state(target="prod", profile=None):
    """Return {resource_key: (status, prod_space_id)} for the target."""
    id_map = genie_space_ids(_run_summary(target, profile))
    return {key: (classify(key, id_map), value) for key, value in id_map.items()}


def main():
    parser = argparse.ArgumentParser(
        description="Report Genie space binding state for a bundle target."
    )
    parser.add_argument("--target", default="prod")
    parser.add_argument("--profile", default=None)
    parser.add_argument(
        "--key", action="append", default=[],
        help="resource key to report on; repeatable. Omit for all known keys.",
    )
    args = parser.parse_args()

    state = get_state(args.target, args.profile)

    keys = args.key or sorted(state)
    for key in keys:
        status, space_id = state.get(key, (ABSENT, ""))
        print(f"{key}\t{status}\t{space_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
