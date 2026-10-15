#!/usr/bin/env python3
"""Replace literal warehouse_id / parent_path in a generated Genie space YAML
with bundle variable references, so each target supplies its own value.

`bundle generate` writes the dev workspace's warehouse ID and folder path as
literals. Deploying that to prod would point the space at a dev warehouse that
does not exist there.
"""

import re
import sys

FIELDS = ("warehouse_id", "parent_path")


def parameterize(text):
    """Return (new_text, changed_field_names)."""
    changed = []
    for field in FIELDS:
        placeholder = "${var.%s}" % field
        pattern = re.compile(
            r"^(\s*)" + field + r":\s*(.+?)\s*$", re.MULTILINE
        )

        def replace(match):
            current = match.group(2).strip().strip("\"'")
            if current == placeholder:
                return match.group(0)  # already parameterized
            changed.append(field)
            return f'{match.group(1)}{field}: "{placeholder}"'

        text = pattern.sub(replace, text)
    return text, changed


def main():
    if len(sys.argv) != 2:
        print("usage: parameterize.py <resource.yml>", file=sys.stderr)
        return 2

    path = sys.argv[1]
    try:
        with open(path) as handle:
            original = handle.read()
    except FileNotFoundError:
        print(f"error: {path} not found", file=sys.stderr)
        return 1

    updated, changed = parameterize(original)

    if changed:
        with open(path, "w") as handle:
            handle.write(updated)
        print(f"  {path}: parameterized {', '.join(sorted(set(changed)))}")
    else:
        print(f"  {path}: already parameterized")

    return 0


if __name__ == "__main__":
    sys.exit(main())
