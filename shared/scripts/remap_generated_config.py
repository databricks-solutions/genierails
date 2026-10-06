#!/usr/bin/env python3
"""Remap a generated draft from one env's catalog namespace to another.

Supports multiple catalog mappings for multi-catalog Genie agents.
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


def _top_level_list_span(text: str, key: str) -> tuple[int, int, int] | None:
    """Return (key_start, open_bracket, end_after_close) for a top-level list."""
    match = re.search(rf"(?m)^{re.escape(key)}\s*=\s*\[", text)
    if not match:
        return None

    opening_bracket = text.find("[", match.start(), match.end())
    depth = 0
    in_string = False
    escaped = False
    index = opening_bracket

    while index < len(text):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return match.start(), opening_bracket, index + 1
        index += 1

    raise ValueError(f"Unterminated top-level {key} list")


def remove_tag_assignments(text: str) -> str:
    """Empty the generated top-level tag_assignments list.

    Assignments are environment-specific classification facts. Cross-environment
    promotion carries governance rules, while the destination classifier derives
    its own facts from the destination catalog.
    """
    span = _top_level_list_span(text, "tag_assignments")
    if span is None:
        return text
    start, _bracket, end = span
    while end < len(text) and text[end] in " \t":
        end += 1
    if end < len(text) and text[end] == "\n":
        end += 1
    # Keep an explicit empty section. The destination-side
    # derive-assignments command atomically replaces this section.
    return text[:start] + "tag_assignments = []\n" + text[end:]


def remap_policy_name(name: str, src: str, dest: str) -> str:
    """Replace the policy's own catalog where it is a whole ``_``-delimited token.

    Derived masks are named ``gr_mask_<catalog>_<treatment>``; the catalog keeps
    the name unique across an env's catalogs (it is the Terraform for_each key).
    """
    if not src or src == dest:
        return name
    return re.sub(rf"(?<![A-Za-z0-9]){re.escape(src)}(?![A-Za-z0-9])", dest, name)


def load_deployed_policy_names(paths: list[Path]) -> set[tuple[str, str]]:
    """Return (catalog, name) of policies the destination env may already have.

    Reads the destination's current abac.auto.tfvars files (data_access layer
    and generated/) and its local Terraform state when present. Missing files
    are skipped; a file that exists but cannot be read fails closed, because
    guessing "not deployed" would rename a live policy.
    """
    import json

    deployed: set[tuple[str, str]] = set()
    for path in paths:
        if not path.is_file():
            continue
        if path.name.endswith(".tfstate"):
            try:
                state = json.loads(path.read_text() or "{}")
            except ValueError as exc:
                raise RuntimeError(f"Cannot read Terraform state {path}: {exc}") from exc
            for resource in state.get("resources") or []:
                if resource.get("type") != "databricks_policy_info" or resource.get("mode") != "managed":
                    continue
                for instance in resource.get("instances") or []:
                    key = instance.get("index_key")
                    catalog = (instance.get("attributes") or {}).get("on_securable_fullname")
                    if isinstance(key, str) and catalog:
                        deployed.add((catalog, key))
            continue
        import hcl2

        try:
            cfg = hcl2.loads(path.read_text())
        except Exception as exc:
            raise RuntimeError(f"Cannot parse {path}: {exc}") from exc
        for policy in cfg.get("fgac_policies") or []:
            if policy.get("name") and policy.get("catalog"):
                deployed.add((policy["catalog"], policy["name"]))
    return deployed


def remap_policy_names(
    source_text: str,
    remapped_text: str,
    pairs: list[tuple[str, str]],
    deployed: set[tuple[str, str]] | None = None,
) -> tuple[str, list[str]]:
    """Carry the destination catalog into fgac policy names.

    The data_access module names the remote policy ``<catalog>_<name>`` and keys
    it by ``name``. Renaming an applied policy is not safe: provider 1.111's
    update sends the new name as the request path and omits ``name`` from the
    update mask, and a new for_each key is a delete plus a create. So a policy the
    destination already has under its source name keeps that name.
    """
    import hcl2

    try:
        policies = hcl2.loads(source_text).get("fgac_policies") or []
    except Exception as exc:
        raise ValueError(f"Cannot parse the source fgac_policies: {exc}") from exc
    mapping = dict(pairs)
    deployed = deployed or set()
    renames: dict[str, str] = {}
    notes: list[str] = []
    final_names: list[str] = []
    for policy in policies:
        name = policy.get("name") or ""
        src = policy.get("catalog") or ""
        dest = mapping.get(src, src)
        new_name = remap_policy_name(name, src, dest)
        if new_name != name and (dest, name) in deployed:
            notes.append(
                f"  Keeping deployed policy name {dest}_{name} "
                "(renaming a live ABAC policy is not safe)"
            )
            new_name = name
        if new_name != name:
            renames[name] = new_name
        final_names.append(new_name)

    duplicates = sorted({n for n in final_names if final_names.count(n) > 1})
    if duplicates:
        raise ValueError(
            "Promoted fgac_policies would share a name: " + ", ".join(duplicates)
        )
    if not renames:
        return remapped_text, notes

    span = _top_level_list_span(remapped_text, "fgac_policies")
    if span is None:
        return remapped_text, notes
    _start, bracket, end = span
    section = remapped_text[bracket:end]
    pattern = re.compile(r'(?m)^(\s*name\s*=\s*")([^"]*)(")')
    section = pattern.sub(
        lambda m: m.group(1) + renames.get(m.group(2), m.group(2)) + m.group(3),
        section,
    )
    return remapped_text[:bracket] + section + remapped_text[end:], notes


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
        ns.quiet_remaps = False
        ns.deployed = []
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
    parser.add_argument("--quiet-remaps", action="store_true",
                        help="Do not repeat successful catalog remap lines.")
    parser.add_argument(
        "--deployed",
        metavar="PATH",
        type=Path,
        action="append",
        default=[],
        help="Destination abac.auto.tfvars or terraform.tfstate (repeatable). "
             "Policies already there keep their name.",
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
    result = remove_tag_assignments(text)
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

    # Read the destination before out_abac overwrites it.
    try:
        deployed = load_deployed_policy_names(args.deployed)
        remapped_hcl, name_notes = remap_policy_names(
            args.source_abac.read_text(),
            remap_hcl(args.source_abac.read_text(), pairs),
            pairs,
            deployed,
        )
    except (RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)
    for note in name_notes:
        print(note)
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
        elif not args.quiet_remaps:
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
