#!/usr/bin/env python3
"""Remap a generated draft from one env's catalog namespace to another.

Supports multiple catalog mappings for multi-catalog Genie Spaces.
Mappings are sorted by source name length (longest first) to prevent
a shorter catalog name from being substituted inside a longer one.

Usage:
  python scripts/remap_generated_config.py \\
    <source_abac> <source_sql> <out_abac> <out_sql> \\
    --map src_catalog=dest_catalog \\
    [--map src_catalog2=dest_catalog2 ...]

  # Single-pair shorthand (positional, backward-compatible):
  python scripts/remap_generated_config.py \\
    <source_abac> <source_sql> <src_catalog> <dest_catalog> <out_abac> <out_sql>
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments, supporting both new flag-based and legacy positional styles."""
    # Detect legacy positional invocation: 6 positional args with no --map flags.
    # Legacy: source_abac source_sql src_catalog dest_catalog out_abac out_sql
    if len(sys.argv) == 7 and "--map" not in sys.argv:
        ns = argparse.Namespace()
        ns.source_abac = Path(sys.argv[1])
        ns.source_sql = Path(sys.argv[2])
        ns.out_abac = Path(sys.argv[5])
        ns.out_sql = Path(sys.argv[6])
        ns.map = [f"{sys.argv[3]}={sys.argv[4]}"]
        return ns

    parser = argparse.ArgumentParser(
        description="Remap catalog names in a generated ABAC draft.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("source_abac", type=Path, help="Source generated abac.auto.tfvars")
    parser.add_argument("source_sql", type=Path, help="Source masking_functions.sql")
    parser.add_argument("out_abac", type=Path, help="Output abac.auto.tfvars")
    parser.add_argument("out_sql", type=Path, help="Output masking_functions.sql")
    parser.add_argument(
        "--map",
        metavar="SRC=DEST",
        action="append",
        required=True,
        help="Catalog name mapping (repeatable). E.g. --map dev_cat=prod_cat",
    )
    return parser.parse_args()


def parse_catalog_pairs(raw_pairs: list[str]) -> list[tuple[str, str]]:
    """Parse 'src=dest' strings into (src, dest) tuples, sorted longest-src-first.

    Sorting by descending source length prevents a short name (e.g. 'cat') from
    being substituted inside a longer name (e.g. 'cat_v2') before it gets its
    own mapping applied.
    """
    pairs: list[tuple[str, str]] = []
    for entry in raw_pairs:
        # Support comma-separated pairs within a single --map value for shell convenience.
        for item in entry.split(","):
            item = item.strip()
            if not item:
                continue
            if "=" not in item:
                print(f"ERROR: Invalid --map value '{item}'. Expected format: src_catalog=dest_catalog")
                sys.exit(1)
            src, dest = item.split("=", 1)
            src, dest = src.strip(), dest.strip()
            if not src:
                print(f"ERROR: Empty source catalog in mapping '{item}'.")
                sys.exit(1)
            if not dest:
                print(f"ERROR: Empty destination catalog in mapping '{item}'.")
                sys.exit(1)
            pairs.append((src, dest))

    # Longest source name first to avoid prefix collisions.
    pairs.sort(key=lambda p: len(p[0]), reverse=True)
    return pairs


def remap_hcl(text: str, pairs: list[tuple[str, str]]) -> str:
    """Substitute all catalog references in HCL text for every mapping pair.

    Handles:
    - Catalog-prefixed table refs:  "src.schema.table"  -> "dest.schema.table"
    - Standalone catalog fields:    catalog = "src"     -> catalog = "dest"
    - function_catalog fields:      function_catalog = "src" -> function_catalog = "dest"
    - Bare quoted catalog names:    "src"               -> "dest"
      (catches references in genie_space_configs where the LLM may use the
      catalog name without a trailing dot, e.g. in comments or descriptions)
    """
    result = text
    for src, dest in pairs:
        # Replace catalog-prefixed table refs (e.g. in entity_name, inline strings).
        result = result.replace(f"{src}.", f"{dest}.")

        # Replace standalone catalog field assignments not already caught above
        # (e.g. catalog = "src_catalog" without a trailing dot).
        for field in ("catalog", "function_catalog"):
            result = re.sub(
                rf'(^\s*{re.escape(field)}\s*=\s*"){re.escape(src)}(")',
                rf'\g<1>{dest}\g<2>',
                result,
                flags=re.MULTILINE,
            )

        # Replace bare quoted catalog name anywhere (e.g. "dev_fin" → "prod_fin").
        # Uses word boundaries to avoid partial matches inside longer names.
        result = re.sub(
            rf'"{re.escape(src)}"',
            f'"{dest}"',
            result,
        )
    return result


def remap_sql(text: str, pairs: list[tuple[str, str]]) -> str:
    """Substitute catalog references in masking SQL for every mapping pair.

    Handles USE CATALOG statements and catalog-prefixed identifiers.
    """
    for src, dest in pairs:
        lines: list[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            match = re.match(
                r"^(USE\s+CATALOG\s+)([^;\s]+)(;?)$",
                stripped,
                re.IGNORECASE,
            )
            if match:
                prefix, catalog, suffix = match.groups()
                catalog = catalog.rstrip(";")
                new_catalog = dest if catalog == src else catalog
                lines.append(f"{prefix}{new_catalog}{suffix}")
            else:
                lines.append(line.replace(f"{src}.", f"{dest}."))
        text = "\n".join(lines) + "\n"
    return text


def main() -> None:
    args = parse_args()

    if not args.source_abac.exists():
        print(f"ERROR: Required file not found: {args.source_abac}")
        sys.exit(1)

    # masking_functions.sql is optional — genie-mode envs don't generate it.
    has_sql = args.source_sql.exists()

    pairs = parse_catalog_pairs(args.map)
    if not pairs:
        print("ERROR: No catalog mappings provided. Use --map src=dest.")
        sys.exit(1)

    args.out_abac.parent.mkdir(parents=True, exist_ok=True)

    remapped_hcl = remap_hcl(args.source_abac.read_text(), pairs)
    remapped_sql = remap_sql(args.source_sql.read_text(), pairs) if has_sql else None

    # Warn if any source catalog name was not found in either output file.
    source_abac_text = args.source_abac.read_text()
    source_sql_text = args.source_sql.read_text() if has_sql else ""
    for src, dest in pairs:
        if src == dest:
            print(f"  Catalog unchanged: {src} (same-catalog remap is a no-op)")
            continue
        found_in_hcl = f"{src}." in source_abac_text or f'"{src}"' in source_abac_text
        found_in_sql = f"{src}." in source_sql_text or f"CATALOG {src}" in source_sql_text.upper()
        if not found_in_hcl and not found_in_sql:
            print(
                f"  WARNING: Source catalog '{src}' was not found in the generated files "
                f"— the mapping '{src}={dest}' had no effect. Check for typos."
            )
        else:
            print(f"  Catalog remap: {src} -> {dest}")

    args.out_abac.write_text(remapped_hcl)
    print(f"  Wrote remapped generated config: {args.out_abac}")

    if remapped_sql is not None:
        args.out_sql.parent.mkdir(parents=True, exist_ok=True)
        args.out_sql.write_text(remapped_sql)
        print(f"  Wrote remapped masking SQL:      {args.out_sql}")
    else:
        print(f"  Skipped masking SQL (not present in source — genie mode)")


if __name__ == "__main__":
    main()
