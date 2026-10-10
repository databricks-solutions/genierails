#!/usr/bin/env python3
"""Rewrite dev catalog references inside a Genie space's serialized_space payload.

Table fully-qualified names appear in several places inside serialized_space:
structured data-source entries, example SQL, and free-text curated instructions.
The internal schema of that payload is not a public contract and Databricks
changes it between versions, so this walks every string in the JSON rather than
targeting known keys. Missing a catalog reference buried in example SQL is how a
promoted space ends up querying dev data from prod.
"""

import argparse
import json
import re
import sys


def _boundary_pattern(dev_name):
    """Match a dotted identifier prefix only when it stands on its own.

    The leading guard stops `dev_analytics` from matching inside
    `dev_analytics_staging`.

    What may follow depends on how specific the mapping is:

    - A one- or two-part name (`cat`, `cat.schema`) is a *prefix* of a table
      reference, so a dot must follow it. Requiring that dot also means a bare
      mention of a catalog in prose is left alone.
    - A three-part name (`cat.schema.table`) is a whole table reference, so it
      must be followed by anything that cannot continue an identifier -- end of
      string, whitespace, a comma, a closing paren. Without this, renaming a
      table outright silently did nothing and the space deployed pointing at a
      table that does not exist.
    """
    parts = dev_name.count(".") + 1
    trailer = r"(?=\.)" if parts < 3 else r"(?![A-Za-z0-9_.])"
    return re.compile(r"(?<![A-Za-z0-9_])" + re.escape(dev_name) + trailer)


def _by_specificity(catalog_map):
    """Most specific mapping first: table, then schema, then catalog.

    Order is load-bearing. With both `dev_cat: prod_cat` and
    `dev_cat.sales.orders: prod_cat.retail.txns`, applying the catalog rule
    first rewrites the prefix and the table rule can then never match.
    """
    return sorted(
        catalog_map.items(),
        key=lambda item: (item[0].count("."), len(item[0])),
        reverse=True,
    )


def rewrite_string(text, catalog_map):
    """Replace every dev table/schema/catalog reference in one string.

    Returns (new_text, count).
    """
    total = 0
    for dev_name, prod_name in _by_specificity(catalog_map):
        text, n = _boundary_pattern(dev_name).subn(prod_name, text)
        total += n
    return text, total


def rewrite(node, catalog_map, path="$", replacements=None):
    """Recursively rewrite catalog references in a parsed JSON structure.

    Returns (new_node, replacements) where replacements is a list of
    (json_path, before, after) for every string that changed.
    """
    if replacements is None:
        replacements = []

    if isinstance(node, str):
        new_text, count = rewrite_string(node, catalog_map)
        if count:
            replacements.append((path, node, new_text))
        return new_text, replacements

    if isinstance(node, dict):
        out = {}
        for key, value in node.items():
            out[key], _ = rewrite(value, catalog_map, f"{path}.{key}", replacements)
        return out, replacements

    if isinstance(node, list):
        out = []
        for index, value in enumerate(node):
            new_value, _ = rewrite(value, catalog_map, f"{path}[{index}]", replacements)
            out.append(new_value)
        return out, replacements

    return node, replacements


def find_unmapped(node, dev_catalogs, path="$", found=None):
    """Report any surviving reference to a dev catalog after rewriting.

    A partial catalog_map would otherwise reach prod and silently query dev.
    """
    if found is None:
        found = []

    if isinstance(node, str):
        for dev_catalog in dev_catalogs:
            if _boundary_pattern(dev_catalog).search(node):
                found.append((path, dev_catalog))
    elif isinstance(node, dict):
        for key, value in node.items():
            find_unmapped(value, dev_catalogs, f"{path}.{key}", found)
    elif isinstance(node, list):
        for index, value in enumerate(node):
            find_unmapped(value, dev_catalogs, f"{path}[{index}]", found)

    return found


def extract_tables(node, found=None):
    """Best-effort collection of table FQNs for the preflight readability check.

    Matches three-part dotted identifiers. Deliberately loose: an extra
    candidate costs one harmless SELECT, a missed one costs a broken space.
    """
    if found is None:
        found = set()

    fqn = re.compile(r"\b([A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+)\b")

    if isinstance(node, str):
        found.update(fqn.findall(node))
    elif isinstance(node, dict):
        for value in node.values():
            extract_tables(value, found)
    elif isinstance(node, list):
        for value in node:
            extract_tables(value, found)

    return found


def main():
    parser = argparse.ArgumentParser(
        description="Rewrite dev catalog references in a .geniespace.json file."
    )
    parser.add_argument("path", help="path to the .geniespace.json file")
    parser.add_argument(
        "--map",
        action="append",
        default=[],
        metavar="DEV=PROD",
        help="catalog mapping, repeatable",
    )
    parser.add_argument(
        "--in-place", action="store_true", help="write changes back to the file"
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="print table FQNs found after rewriting, one per line, and exit",
    )
    args = parser.parse_args()

    catalog_map = {}
    for entry in args.map:
        if "=" not in entry:
            print(f"error: --map expects DEV=PROD, got {entry!r}", file=sys.stderr)
            return 2
        dev_catalog, prod_catalog = entry.split("=", 1)
        catalog_map[dev_catalog.strip()] = prod_catalog.strip()

    with open(args.path) as handle:
        original = json.load(handle)

    rewritten, replacements = rewrite(original, catalog_map)

    if args.list_tables:
        for table in sorted(extract_tables(rewritten)):
            print(table)
        return 0

    for json_path, before, after in replacements:
        print(f"  {json_path}")
        print(f"    - {before[:160]}")
        print(f"    + {after[:160]}")

    unmapped = find_unmapped(rewritten, catalog_map.keys())
    if unmapped:
        print(
            f"error: {len(unmapped)} dev catalog reference(s) survived the rewrite:",
            file=sys.stderr,
        )
        for json_path, dev_catalog in unmapped:
            print(f"  {json_path}: {dev_catalog}", file=sys.stderr)
        return 1

    if args.in_place:
        with open(args.path, "w") as handle:
            json.dump(rewritten, handle, indent=2)
            handle.write("\n")

    print(f"  {len(replacements)} replacement(s) in {args.path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
