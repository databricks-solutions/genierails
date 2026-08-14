#!/usr/bin/env python3
"""Read spaces.yml / databricks.yml without requiring PyYAML.

The customer's laptop may not have PyYAML and we do not want to make them set up
a virtualenv just to promote a few Genie spaces, so this parses the narrow subset
of YAML these two files use. Falls back to PyYAML when it is available.
"""

import argparse
import re
import sys

# Field separator for --spaces output. A tab cannot be used: bash's IFS treats
# tab as whitespace and collapses consecutive ones, which drops empty fields.
SEP = "\x1f"


def _load_with_pyyaml(path):
    try:
        import yaml
    except ImportError:
        return None
    with open(path) as handle:
        return yaml.safe_load(handle)


def _strip_comment(line):
    """Remove a trailing comment, respecting quoted strings."""
    out = []
    quote = None
    for char in line:
        if quote:
            out.append(char)
            if char == quote:
                quote = None
        elif char in "\"'":
            quote = char
            out.append(char)
        elif char == "#":
            break
        else:
            out.append(char)
    return "".join(out).rstrip()


def _unquote(value):
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def parse_spaces(path):
    """Return a list of {key, dev_id, prod_id, catalog_map} from spaces.yml.

    A space may carry its own catalog_map, which takes precedence over the
    top-level one: different spaces often read different catalogs, so a single
    global mapping cannot express a real setup.
    """
    data = _load_with_pyyaml(path)
    if data is not None:
        spaces = []
        for item in data.get("spaces") or []:
            spaces.append(
                {
                    "key": str(item.get("key", "")).strip(),
                    "dev_id": str(item.get("dev_id", "")).strip(),
                    "prod_id": str(item.get("prod_id") or "").strip(),
                    "catalog_map": {
                        str(k): str(v)
                        for k, v in (item.get("catalog_map") or {}).items()
                    },
                }
            )
        return spaces

    spaces = []
    current = None
    in_spaces = False
    in_space_map = False
    map_indent = 0

    with open(path) as handle:
        for raw in handle:
            line = _strip_comment(raw)
            if not line.strip():
                continue

            if re.match(r"^spaces\s*:", line):
                in_spaces = True
                continue
            if re.match(r"^[A-Za-z_]", line):
                # A new top-level key ends the spaces block.
                in_spaces = False
                continue
            if not in_spaces:
                continue

            item = re.match(r"^\s*-\s*(.*)$", line)
            if item:
                if current:
                    spaces.append(current)
                current = {"key": "", "dev_id": "", "prod_id": "",
                           "catalog_map": {}}
                in_space_map = False
                line = item.group(1)
                if not line.strip():
                    continue

            if current is None:
                continue

            # A nested catalog_map under this space; its entries are indented
            # deeper than the field itself.
            if re.match(r"^\s+catalog_map\s*:\s*$", line):
                in_space_map = True
                map_indent = len(line) - len(line.lstrip())
                continue

            indent = len(line) - len(line.lstrip())
            if in_space_map:
                if indent > map_indent:
                    entry = re.match(r"^\s+([A-Za-z0-9_.]+)\s*:\s*(.+)$", line)
                    if entry:
                        current["catalog_map"][entry.group(1)] = _unquote(
                            entry.group(2)
                        )
                    continue
                in_space_map = False

            field = re.match(r"^\s*([A-Za-z_]+)\s*:\s*(.*)$", line)
            if field:
                name, value = field.group(1), _unquote(field.group(2))
                if name in ("key", "dev_id", "prod_id"):
                    current[name] = value

    if current:
        spaces.append(current)

    return [s for s in spaces if s["key"]]


def parse_catalog_map(path):
    """Return a list of "dev=prod" strings from the TOP-LEVEL catalog_map block.

    Per-space maps are returned by parse_spaces instead; this is the fallback
    applied to spaces that do not define their own.
    """
    data = _load_with_pyyaml(path)
    if data is not None:
        return [f"{k}={v}" for k, v in (data.get("catalog_map") or {}).items()]

    pairs = []
    in_map = False
    with open(path) as handle:
        for raw in handle:
            line = _strip_comment(raw)
            if not line.strip():
                continue
            # Only the unindented catalog_map is the global one; an indented one
            # belongs to a space.
            if re.match(r"^catalog_map\s*:", line):
                in_map = True
                continue
            if re.match(r"^[A-Za-z_]", line):
                in_map = False
                continue
            if not in_map:
                continue
            # Keys may be dotted (catalog, catalog.schema, or
            # catalog.schema.table), so dots must be allowed here. Splitting on
            # the last colon would break on quoted values, so match explicitly.
            field = re.match(r"^\s+([A-Za-z0-9_.]+)\s*:\s*(.+)$", line)
            if field:
                pairs.append(f"{field.group(1)}={_unquote(field.group(2))}")
    return pairs


def parse_warehouse(path, target):
    """Return the warehouse_id for a given target in databricks.yml."""
    data = _load_with_pyyaml(path)
    if data is not None:
        targets = data.get("targets") or {}
        entry = targets.get(target) or {}
        variables = entry.get("variables") or {}
        return str(variables.get("warehouse_id", "") or "")

    # Text scan: find the target block, then its warehouse_id.
    in_target = False
    target_indent = None
    with open(path) as handle:
        for raw in handle:
            line = _strip_comment(raw)
            if not line.strip():
                continue
            match = re.match(r"^(\s*)([A-Za-z0-9_-]+)\s*:", line)
            if not match:
                continue
            indent, name = len(match.group(1)), match.group(2)
            if name == target and indent == 2:
                in_target = True
                target_indent = indent
                continue
            if in_target and indent <= target_indent and name != target:
                break
            if in_target and name == "warehouse_id":
                value = _unquote(line.split(":", 1)[1])
                return value
    return ""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--spaces", action="store_true")
    parser.add_argument("--catalog-map", action="store_true")
    parser.add_argument("--warehouse", metavar="TARGET")
    args = parser.parse_args()

    if args.spaces:
        # Fields are separated by US (0x1f), not tab: bash treats tab as IFS
        # whitespace and collapses runs of it, so an empty prod_id in the middle
        # of the line would vanish and shift every later field left.
        #
        # Layout: key US dev_id US prod_id US a=b;c=d
        global_map = parse_catalog_map(args.path)
        for space in parse_spaces(args.path):
            if not space["dev_id"]:
                print(
                    f"error: space '{space['key']}' has no dev_id", file=sys.stderr
                )
                return 1
            own = [f"{k}={v}" for k, v in (space.get("catalog_map") or {}).items()]
            effective = own or global_map
            print(SEP.join([space["key"], space["dev_id"], space["prod_id"],
                            ";".join(effective)]))
        return 0

    if args.catalog_map:
        for pair in parse_catalog_map(args.path):
            print(pair)
        return 0

    if args.warehouse:
        print(parse_warehouse(args.path, args.warehouse))
        return 0

    parser.error("choose one of --spaces, --catalog-map, --warehouse")


if __name__ == "__main__":
    sys.exit(main())
