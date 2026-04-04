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

try:
    import hcl2
except ImportError:
    print("ERROR: python-hcl2 required")
    sys.exit(2)


def _str(v) -> str:
    return (v[0] if isinstance(v, list) else v or "").strip()


def _discover_from_genie_api(space_id: str, auth_cfg: dict) -> tuple[str, list[str]]:
    """Query Genie Space API to get name and tables.

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
        print(f"  WARNING: Could not query Genie Space {space_id}: {e}")
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
    cfg = hcl2.load(open(os.path.join(source_env_dir, "env.auto.tfvars")))
    spaces = cfg.get("genie_spaces", [])

    # Load source auth for API queries
    auth_cfg = {}
    for auth_path in [
        os.path.join(source_env_dir, "auth.auto.tfvars"),
        os.path.join(source_env_dir, "..", "auth.auto.tfvars"),
    ]:
        if os.path.exists(auth_path):
            auth_cfg = hcl2.load(open(auth_path))
            break

    # Enrich spaces: discover name/tables from Genie API if missing
    for space in spaces:
        space_id = _str(space.get("genie_space_id", ""))
        name = _str(space.get("name", ""))
        uc_tables = space.get("uc_tables") or []

        if space_id and (not name or not uc_tables):
            print(f"  Querying Genie Space {space_id} for name/tables...")
            api_title, api_tables = _discover_from_genie_api(space_id, auth_cfg)
            if not name and api_title:
                space["name"] = api_title
                print(f"  Discovered name: {api_title}")
            if not uc_tables and api_tables:
                space["uc_tables"] = api_tables
                print(f"  Discovered {len(api_tables)} table(s)")

    # Build dest env.auto.tfvars
    lines = ["genie_spaces = ["]
    for space in spaces:
        name = _str(space.get("name", ""))
        uc_tables = space.get("uc_tables") or []
        remapped_tables = [remap_table(t) for t in uc_tables]

        lines.append("  {")
        lines.append(f'    name             = "{name}"')
        lines.append(f'    genie_space_id   = ""')
        lines.append(f'    uc_tables = [')
        for t in remapped_tables:
            lines.append(f'      "{t}",')
        lines.append(f'    ]')
        lines.append("  },")
    lines.append("]")
    lines.append("")
    lines.append('sql_warehouse_id = ""  # auto-create in dest workspace')

    # Write
    os.makedirs(dest_env_dir, exist_ok=True)
    dest_path = os.path.join(dest_env_dir, "env.auto.tfvars")
    with open(dest_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"  Wrote {dest_path}")
    for src, dest in pairs.items():
        print(f"  Catalog remap: {src} -> {dest}")


if __name__ == "__main__":
    main()
