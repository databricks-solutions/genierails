#!/usr/bin/env python3
"""Write the bundle files for a space using PROD's current definition.

Used when promoting a subset. databricks.yml includes resources/*.yml, so a space
the bundle manages but this run excludes must still have a local definition or
the plan schedules a delete and trashes a live prod space.

That placeholder has to come from prod, not dev: re-exporting from dev would push
dev's content (including unmapped dev table names) into a space the run was meant
to leave alone. Taking prod's own definition makes it plan as unchanged.
"""

import argparse
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

sys.path.insert(0, HERE)
import binding_state  # noqa: E402


def prod_space_id(key, profile):
    state = binding_state.get_state("prod", profile)
    status, space_id = state.get(key, ("absent", ""))
    return space_id if status == "bound" else ""


def fetch(space_id, profile):
    cmd = ["databricks", "api", "get",
           f"/api/2.0/genie/spaces/{space_id}?include_serialized_space=true"]
    if profile:
        cmd += ["-p", profile]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    if result.returncode != 0:
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("key")
    parser.add_argument("--profile", default=None)
    args = parser.parse_args()

    space_id = prod_space_id(args.key, args.profile)
    if not space_id:
        print(f"error: {args.key} is not tracked in prod", file=sys.stderr)
        return 1

    payload = fetch(space_id, args.profile)
    if not payload:
        print(f"error: cannot read prod space {space_id}", file=sys.stderr)
        return 1

    serialized = payload.get("serialized_space")
    if isinstance(serialized, str):
        try:
            serialized = json.loads(serialized)
        except json.JSONDecodeError:
            print("error: prod serialized_space is not valid JSON", file=sys.stderr)
            return 1
    if serialized is None:
        print("error: prod space has no serialized_space", file=sys.stderr)
        return 1

    os.makedirs(os.path.join(ROOT, "src"), exist_ok=True)
    os.makedirs(os.path.join(ROOT, "resources"), exist_ok=True)

    space_path = os.path.join(ROOT, "src", f"{args.key}.geniespace.json")
    with open(space_path, "w") as handle:
        json.dump(serialized, handle, indent=2)
        handle.write("\n")

    title = (payload.get("title") or args.key).replace('"', '\\"')
    description = payload.get("description") or ""

    # Mirror prod's literal values rather than pointing at target variables. A
    # space this run did not select must plan as unchanged, and ${var.parent_path}
    # can resolve to a different string than prod stores (prod returns
    # "/Shared/genie" where the variable is "/Workspace/Shared/genie"), which
    # would show up as an update on a space nobody asked to touch.
    warehouse_id = payload.get("warehouse_id") or ""
    parent_path = payload.get("parent_path") or ""

    lines = [
        f"# Generated from PROD space {space_id} so a subset promotion neither",
        "# deletes nor modifies it. Values are prod's own, so this plans as",
        "# unchanged. Regenerated on every run; do not hand-edit.",
        "resources:",
        "  genie_spaces:",
        f"    {args.key}:",
        f'      title: "{title}"',
    ]
    if description:
        lines.append(f'      description: "{description}"')
    if warehouse_id:
        lines.append(f'      warehouse_id: "{warehouse_id}"')
    if parent_path:
        lines.append(f'      parent_path: "{parent_path}"')
    lines += [
        f"      file_path: ../src/{args.key}.geniespace.json",
        "",
    ]

    resource_path = os.path.join(ROOT, "resources", f"{args.key}.genie_space.yml")
    with open(resource_path, "w") as handle:
        handle.write("\n".join(lines))

    print(f"wrote {args.key} from prod space {space_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
