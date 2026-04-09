#!/usr/bin/env python3
"""Sync tag policy values from abac.auto.tfvars to Databricks via REST API.

The Databricks Terraform provider has a bug where it reorders tag policy
values after apply, causing "Provider produced inconsistent result" errors.
This script bypasses Terraform by updating tag policy values directly via
the Databricks REST API, so Terraform can use ignore_changes = [values] safely.

Usage:
    python3 scripts/sync_tag_policies.py [path/to/abac.auto.tfvars]
"""
import os
import sys
from pathlib import Path

WORK_DIR = Path.cwd()


def _load_auth():
    """Read auth.auto.tfvars and return config dict + set SDK env vars."""
    auth_path = WORK_DIR / "auth.auto.tfvars"
    if not auth_path.exists():
        return {}
    try:
        import hcl2
    except ImportError:
        import subprocess
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", "python-hcl2"],
        )
        import hcl2

    with open(auth_path) as f:
        cfg = hcl2.load(f)

    mapping = {
        "databricks_workspace_host": "DATABRICKS_HOST",
        "databricks_client_id": "DATABRICKS_CLIENT_ID",
        "databricks_client_secret": "DATABRICKS_CLIENT_SECRET",
    }
    for tfvar_key, env_key in mapping.items():
        val = cfg.get(tfvar_key, "")
        if isinstance(val, list):
            val = val[0] if val else ""
        val = (val or "").strip()
        if val:
            os.environ[env_key] = val

    return cfg


def _str(v):
    return (v[0] if isinstance(v, list) else v or "").strip()


def main():
    tfvars_path = (
        Path(sys.argv[1]) if len(sys.argv) > 1 else WORK_DIR / "abac.auto.tfvars"
    )
    if not tfvars_path.exists():
        print(f"  [SKIP] {tfvars_path} not found")
        return

    import hcl2

    with open(tfvars_path) as f:
        config = hcl2.load(f)

    desired_policies = config.get("tag_policies", [])
    if not desired_policies:
        print("  [SKIP] No tag_policies found in config")
        return

    auth_cfg = _load_auth()

    # Get auth token via SDK's config.authenticate()
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.tags import TagPolicy, Value
    host = _str(auth_cfg.get("databricks_workspace_host", ""))
    client_id = _str(auth_cfg.get("databricks_client_id", ""))
    client_secret = _str(auth_cfg.get("databricks_client_secret", ""))

    w = WorkspaceClient(
        host=host or None,
        client_id=client_id or None,
        client_secret=client_secret or None,
        product="genierails",
        product_version="0.1.0",
    )
    token = w.config.authenticate()  # {'Authorization': 'Bearer ...'}
    base = (host or os.environ.get("DATABRICKS_HOST", "")).rstrip("/")
    if not base:
        print("  [WARN] No workspace host found; skipping tag policy sync")
        return

    # List existing tag policies via SDK. The REST list path has returned 404
    # in some workspaces even though the tag policy service is available.
    existing = {}
    try:
        for tp in w.tag_policies.list_tag_policies():
            tag_key = getattr(tp, "tag_key", "") or ""
            values = {
                getattr(v, "name", "") or ""
                for v in (getattr(tp, "values", None) or [])
                if getattr(v, "name", "") or ""
            }
            existing[tag_key] = values
    except Exception as list_err:
        print(f"  [ERROR] Could not list tag policies ({list_err})")
        raise SystemExit(1)

    updated = 0
    for tp in desired_policies:
        key = tp["key"]
        desired_values = set(tp["values"])
        # Exact match first, then suffix match (Terraform appends _<hex> suffix
        # to tag keys for account-level isolation in parallel test environments).
        current_values = existing.get(key)
        actual_key = key
        if current_values is None:
            for db_key, db_vals in existing.items():
                if db_key == key or db_key.startswith(key + "_"):
                    current_values = db_vals
                    actual_key = db_key
                    break

        if current_values is None:
            continue

        missing = desired_values - current_values
        removed = current_values - desired_values
        desired_list = sorted(desired_values)

        try:
            w.tag_policies.update_tag_policy(
                tag_key=actual_key,
                tag_policy=TagPolicy(
                    tag_key=actual_key,
                    values=[Value(name=v) for v in desired_list],
                ),
                update_mask="values",
            )
            changes = []
            if missing:
                changes.append(f"added {sorted(missing)}")
            if removed:
                changes.append(f"removed {sorted(removed)}")
            if not changes:
                changes.append("reasserted desired values")
            print(f"  [SYNC] {actual_key}: {', '.join(changes)}")
            updated += 1
        except Exception as e:
            print(f"  [ERROR] {key}: {e}")
            raise SystemExit(1)

    if updated:
        print(f"  Synced {updated} tag policy/ies")
    else:
        print("  Tag policies already in sync")


if __name__ == "__main__":
    main()
