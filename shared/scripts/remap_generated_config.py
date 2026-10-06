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
import threading
from pathlib import Path
from typing import Callable


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


def read_state_policy_keys(path: Path | None) -> set[tuple[str, str]] | None:
    """Return (catalog, key) of the data_access policies in a Terraform state.

    Only ``module.data_access.databricks_policy_info.policies`` counts. The
    result can only add reasons to keep a name: a state without a key doesn't
    prove the remote policy is absent. None when the state is missing or
    unreadable.
    """
    import json

    if path is None or not path.is_file():
        return None
    try:
        state = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(state, dict) or "resources" not in state:
        return None
    keys: set[tuple[str, str]] = set()
    for resource in state.get("resources") or []:
        if (
            resource.get("module") != "module.data_access"
            or resource.get("mode") != "managed"
            or resource.get("type") != "databricks_policy_info"
            or resource.get("name") != "policies"
        ):
            continue
        for instance in resource.get("instances") or []:
            catalog = (instance.get("attributes") or {}).get("on_securable_fullname")
            if isinstance(instance.get("index_key"), str) and catalog:
                keys.add((catalog, instance["index_key"]))
    return keys


def live_policy_lister(
    auth_path: Path | None, timeout: float = 90.0
) -> Callable[[str], set[str] | None]:
    """Return catalog -> remote policy names on it, read-only via the SDK.

    The lister returns None when the listing can't be trusted: no or placeholder
    credentials, SDK missing, any API/auth error, or no answer within timeout.
    """
    client = None
    unavailable = auth_path is None or not auth_path.is_file()
    if not unavailable:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from auth_configured import auth_configured

        unavailable = not auth_configured(auth_path)

    def fetch(catalog: str) -> set[str]:
        nonlocal client
        if client is None:
            import hcl2
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.core import Config

            auth = hcl2.loads(auth_path.read_text())
            client = WorkspaceClient(config=Config(
                host=auth["databricks_workspace_host"],
                client_id=auth["databricks_client_id"],
                client_secret=auth.get("databricks_client_secret"),
                http_timeout_seconds=30,
                retry_timeout_seconds=60,
                product="genierails",
                product_version="0.1.0",
            ))
        return {
            policy.name
            for policy in client.policies.list_policies(
                on_securable_type="CATALOG", on_securable_fullname=catalog
            )
            if policy.name
        }

    def list_names(catalog: str) -> set[str] | None:
        nonlocal unavailable
        if unavailable:
            return None
        # The SDK's own timeouts don't bound auth discovery against an
        # unreachable host, so cap the whole call; promote must not hang.
        result: dict[str, object] = {}

        def run() -> None:
            try:
                result["names"] = fetch(catalog)
            except Exception as exc:
                result["error"] = exc

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        worker.join(timeout)
        if "names" not in result:
            unavailable = True
            return None
        return result["names"]  # type: ignore[return-value]

    return list_names


def remap_policy_names(
    source_text: str,
    remapped_text: str,
    pairs: list[tuple[str, str]],
    state_keys: set[tuple[str, str]] | None = None,
    live_names: Callable[[str], set[str] | None] | None = None,
) -> tuple[str, list[str]]:
    """Carry the destination catalog into fgac policy names.

    The data_access module names the remote policy ``<catalog>_<name>`` and keys
    it by ``name``. Renaming an applied policy is not safe: provider 1.111's
    update sends the new name as the request path and omits ``name`` from the
    update mask, and a new for_each key is a delete plus a create. So a policy
    is renamed only when a successful live listing of its destination catalog
    shows neither the old nor the new remote name, and the destination state
    doesn't hold the old key. A new remote name that already exists is fine only
    when the state holds it under the new key (the same managed policy). Drafts
    and config files are never evidence.
    """
    import hcl2

    try:
        policies = hcl2.loads(source_text).get("fgac_policies") or []
    except Exception as exc:
        raise ValueError(f"Cannot parse the source fgac_policies: {exc}") from exc
    mapping = dict(pairs)
    live_cache: dict[str, set[str] | None] = {}
    renames: dict[str, str] = {}
    notes: list[str] = []
    final_names: list[str] = []
    for policy in policies:
        name = policy.get("name") or ""
        src = policy.get("catalog") or ""
        dest = mapping.get(src, src)
        new_name = remap_policy_name(name, src, dest)
        if new_name != name:
            if dest not in live_cache:
                live_cache[dest] = live_names(dest) if live_names else None
            live = live_cache[dest]
            managed = state_keys or set()
            old_remote = f"{dest}_{name}"
            new_remote = f"{dest}_{new_name}"
            if (dest, name) in managed or (live is not None and old_remote in live):
                notes.append(f"  kept policy name {old_remote} (deployed in {dest})")
                new_name = name
            elif live is None:
                notes.append(
                    f"  kept policy name {old_remote} "
                    f"(couldn't confirm it isn't deployed in {dest})"
                )
                new_name = name
            elif new_remote in live and (dest, new_name) not in managed:
                notes.append(
                    f"  kept policy name {old_remote} ({new_remote} already exists in {dest})"
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
        ns.state = None
        ns.auth = None
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
        "--state",
        metavar="PATH",
        type=Path,
        help="Destination data_access terraform.tfstate.",
    )
    parser.add_argument(
        "--auth",
        metavar="PATH",
        type=Path,
        help="Destination auth.auto.tfvars, for a read-only listing of live policies.",
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

    try:
        remapped_hcl, name_notes = remap_policy_names(
            args.source_abac.read_text(),
            remap_hcl(args.source_abac.read_text(), pairs),
            pairs,
            read_state_policy_keys(args.state),
            live_policy_lister(args.auth),
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
