#!/usr/bin/env python3
"""Validate DEST_CATALOG_MAP against source catalogs.

Extracts source catalogs from env.auto.tfvars (uc_tables) or falls back
to generated/data_access abac.auto.tfvars (tag_assignments).

Usage:
    python validate_catalog_map.py <source_env_dir> <dest_catalog_map>
"""
import sys
import os

try:
    import hcl2
except ImportError:
    print("ERROR: python-hcl2 required")
    sys.exit(2)


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <source_env_dir> <dest_catalog_map>")
        sys.exit(2)

    source_env_dir = sys.argv[1]
    dest_catalog_map = sys.argv[2]

    # Extract catalogs from env.auto.tfvars uc_tables
    env_path = os.path.join(source_env_dir, "env.auto.tfvars")
    cfg = hcl2.load(open(env_path))
    spaces = cfg.get("genie_spaces", [])
    tables = [t for s in spaces for t in (s.get("uc_tables") or [])]
    src_cats = sorted(set(
        t.split(".")[0] for t in tables if t.count(".") >= 2
    ))

    # Fallback: extract catalogs from generated or data_access abac.auto.tfvars
    if not src_cats:
        for subdir in ["generated", "data_access"]:
            abac_path = os.path.join(source_env_dir, subdir, "abac.auto.tfvars")
            if not os.path.exists(abac_path):
                continue
            try:
                gcfg = hcl2.load(open(abac_path))
                src_cats = sorted(set(
                    ta.get("entity_name", "").split(".")[0]
                    for ta in gcfg.get("tag_assignments", [])
                    if ta.get("entity_name", "").count(".") >= 2
                ) - {""})
            except Exception:
                continue
            if src_cats:
                break

    # Parse the catalog map
    pairs = {}
    for pair in dest_catalog_map.split(","):
        if "=" in pair:
            k, v = pair.split("=", 1)
            pairs[k.strip()] = v.strip()

    # Validate
    missing = [c for c in src_cats if c not in pairs]
    extra = [k for k in pairs if k not in src_cats]

    if missing:
        print(f"ERROR: DEST_CATALOG_MAP is missing mappings for: {', '.join(missing)}")
        print(f"       Detected source catalogs: {', '.join(src_cats)}")
        sys.exit(1)

    if extra:
        print(f"ERROR: DEST_CATALOG_MAP references unknown source catalogs: {', '.join(extra)}")
        print(f"       Check for typos. Detected source catalogs: {', '.join(src_cats)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
