#!/usr/bin/env python3
"""Remap env.auto.tfvars for cross-env promotion.

Reads source env.auto.tfvars, remaps catalog names, and writes dest env.auto.tfvars.
When genie_space_id is set but uc_tables/name are missing, queries the Genie API
to discover them (using include_serialized_space=true).

Usage:
    python remap_env_config.py <source_env_dir> <dest_env_dir> <catalog_map>

    catalog_map: comma-separated "src=dest" pairs, e.g. "dev_bank=prod_bank"
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from scripts.footprint import load_hcl, resolve_footprint

try:
    import hcl2
except ImportError:
    print("ERROR: python-hcl2 required")
    sys.exit(2)


def _str(v) -> str:
    return (v[0] if isinstance(v, list) else v or "").strip()


def _discover_from_genie_api(space_id: str, auth_cfg: dict) -> tuple[str, list[str]]:
    """Query Genie agent API to get name and tables.

    Returns (space_title, table_identifiers).
    """
    host = _str(auth_cfg.get("databricks_workspace_host", ""))
    client_id = _str(auth_cfg.get("databricks_client_id", ""))
    client_secret = _str(auth_cfg.get("databricks_client_secret", ""))

    if not host or not client_id:
        return "", []

    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(
            host=host, client_id=client_id, client_secret=client_secret,
            product="genierails", product_version="0.1.0",
        )
        resp = w.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}",
            query={"include_serialized_space": "true"},
        )
    except Exception as e:
        print(f"  WARNING: Could not query Genie agent {space_id}: {e}")
        return "", []

    title = resp.get("title", "")
    serialized = resp.get("serialized_space", "")
    tables = []
    if serialized:
        try:
            space_data = json.loads(serialized) if isinstance(serialized, str) else serialized
            for t in space_data.get("data_sources", {}).get("tables", []):
                ident = t.get("identifier", "")
                if ident:
                    tables.append(ident)
        except (json.JSONDecodeError, TypeError):
            pass

    return title, tables


def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <source_env_dir> <dest_env_dir> <catalog_map>")
        sys.exit(2)

    source_env_dir = sys.argv[1]
    dest_env_dir = sys.argv[2]
    catalog_map_str = sys.argv[3]

    # Parse catalog map
    pairs = {}
    for pair in catalog_map_str.split(","):
        if "=" in pair:
            k, v = pair.split("=", 1)
            pairs[k.strip()] = v.strip()

    def remap_table(table: str) -> str:
        parts = table.split(".", 1)
        if len(parts) == 2 and parts[0] in pairs:
            return pairs[parts[0]] + "." + parts[1]
        return table

    # Load source config
    cfg = load_hcl(Path(source_env_dir) / "env.auto.tfvars")
    spaces = cfg.get("genie_spaces", [])
    effective_tables = resolve_footprint(source_env_dir)
    discovered_cfg = load_hcl(
        Path(source_env_dir) / "data_access" / "discovered_uc_tables.auto.tfvars"
    )
    discovered_agents = discovered_cfg.get("discovered_table_agents") or {}
    generated_path = os.path.join(source_env_dir, "generated", "abac.auto.tfvars")
    id_to_name = {}
    if os.path.exists(generated_path):
        generated_cfg = hcl2.load(open(generated_path))
        id_to_name = generated_cfg.get("genie_space_id_to_name") or {}

    # Load source auth for API queries
    auth_cfg = {}
    for auth_path in [
        os.path.join(source_env_dir, "auth.auto.tfvars"),
        os.path.join(source_env_dir, "..", "auth.auto.tfvars"),
    ]:
        if os.path.exists(auth_path):
            auth_cfg = hcl2.load(open(auth_path))
            break

    # Resolve canonical names before matching discovered table attribution.
    for space in spaces:
        space_id = _str(space.get("genie_space_id", ""))
        if not _str(space.get("name", "")) and space_id:
            canonical_name = _str(id_to_name.get(space_id, ""))
            if canonical_name:
                space["name"] = canonical_name

    # Enrich spaces from persisted discovery first; query the API only as a
    # last resort when a configured space still has no attributable tables.
    for space in spaces:
        space_id = _str(space.get("genie_space_id", ""))
        name = _str(space.get("name", ""))
        uc_tables = space.get("uc_tables") or []

        if not uc_tables and name:
            uc_tables = [
                table for table, agents in discovered_agents.items()
                if name in (agents or [])
            ]
            if uc_tables:
                space["uc_tables"] = uc_tables
                print(f"  Resolved {len(uc_tables)} persisted table(s) for {name}")

        # Legacy single-agent discovery did not record attribution.
        if not uc_tables and len(spaces) == 1 and effective_tables:
            uc_tables = list(effective_tables)
            space["uc_tables"] = uc_tables

        if space_id and (not name or not uc_tables):
            print(f"  Querying Genie agent {space_id} for name/tables...")
            api_title, api_tables = _discover_from_genie_api(space_id, auth_cfg)
            if not name and api_title:
                space["name"] = api_title
                print(f"  Discovered name: {api_title}")
            if not uc_tables and api_tables:
                space["uc_tables"] = api_tables
                effective_tables.extend(
                    table for table in api_tables if table not in effective_tables
                )
                print(f"  Discovered {len(api_tables)} table(s)")

        if not (space.get("uc_tables") or []):
            agent = space_id or name or "<unknown>"
            print(
                f"ERROR: no tables found for agent {agent}; run "
                "`make generate ENV=dev MODE=genie ...` first"
            )
            sys.exit(1)

    if not effective_tables:
        print(
            "ERROR: no tables found for source environment; run "
            "`make generate ENV=dev MODE=genie ...` first"
        )
        sys.exit(1)

    canonical_names = [_str(space.get("name", "")) for space in spaces]
    missing = [i for i, name in enumerate(canonical_names) if not name]
    if missing:
        print(
            "ERROR: Cannot promote Genie space(s) without a canonical name. "
            "Run `make generate` in the source environment first."
        )
        sys.exit(1)
    duplicates = sorted({name for name in canonical_names if canonical_names.count(name) > 1})
    if duplicates:
        print(
            "ERROR: Multiple Genie spaces resolve to the same canonical name: "
            + ", ".join(repr(name) for name in duplicates)
            + ". Set distinct names before promoting."
        )
        sys.exit(1)

    # Preserve destination-owned settings across remediation re-promotions.
    dest_path = os.path.join(dest_env_dir, "env.auto.tfvars")
    dest_cfg = load_hcl(Path(dest_path))
    preserved_warehouse = _str(dest_cfg.get("sql_warehouse_id", ""))
    preserved_auto_tagging = dest_cfg.get("enable_auto_tagging", False)
    if dest_cfg:
        print(
            "  Preserved destination sql_warehouse_id and enable_auto_tagging "
            f"({preserved_auto_tagging})"
        )

    # Build dest env.auto.tfvars. The complete promoted union is top-level so
    # Terraform, classification, derive-assignments, and certify share it.
    lines = ["genie_spaces = ["]
    for space in spaces:
        name = _str(space.get("name", ""))
        uc_tables = space.get("uc_tables") or []
        remapped_tables = [remap_table(t) for t in uc_tables]

        lines.append("  {")
        lines.append(f"    name             = {json.dumps(name)}")
        lines.append(f'    genie_space_id   = ""')
        lines.append(f'    uc_tables = [')
        for t in remapped_tables:
            lines.append(f'      "{t}",')
        lines.append(f'    ]')
        if "acl_groups" in space:
            acl_groups = space["acl_groups"]
            if acl_groups is not None and (
                not isinstance(acl_groups, list) or not all(
                    isinstance(group, str) for group in acl_groups
                )
            ):
                print(
                    f"ERROR: acl_groups for Genie space {name!r} must be a list "
                    "of group names, null, or omitted ([] means nobody)."
                )
                sys.exit(1)
            if acl_groups is not None:
                rendered_acl = ", ".join(json.dumps(group) for group in acl_groups)
                lines.append(f"    acl_groups       = [{rendered_acl}]")
        lines.append("  },")
    lines.append("]")
    lines.append("")
    lines.append("uc_tables = [")
    for table in effective_tables:
        lines.append(f'  "{remap_table(table)}",')
    lines.append("]")
    lines.append("")
    lines.append(f"sql_warehouse_id = {json.dumps(preserved_warehouse)}")
    lines.append("")
    lines.append("# Safe production defaults; use the UI workflow before opening access.")
    lines.append("enable_classification = true")
    lines.append(f"enable_auto_tagging = {str(bool(preserved_auto_tagging)).lower()}")
    lines.append("business_access_enabled = false")

    # Write
    os.makedirs(dest_env_dir, exist_ok=True)
    with open(dest_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"  Wrote {dest_path}")
    for src, dest in pairs.items():
        print(f"  Catalog remap: {src} -> {dest}")


if __name__ == "__main__":
    main()
