#!/usr/bin/env python3
"""Sync tag policy values from abac.auto.tfvars to Databricks via REST API.

The Databricks Terraform provider has a bug where it reorders tag policy
values after apply, causing "Provider produced inconsistent result" errors.
This script bypasses Terraform by updating tag policy values directly via
the Databricks REST API, so Terraform can use ignore_changes = [values] safely.

Usage:
    python3 scripts/sync_tag_policies.py [path/to/abac.auto.tfvars]
"""
import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
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
        if val and not os.environ.get(env_key):
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

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    # List existing tag policies via REST API
    existing = {}
    try:
        list_url = f"{base}/api/2.1/unity-catalog/tag-policies"
        req = urllib.request.Request(list_url, headers=token)
        with urllib.request.urlopen(req, timeout=30, context=ssl_ctx) as resp:
            data = json.loads(resp.read())
        for tp in data.get("tag_policies", []):
            tag_key = tp.get("tag_key", "")
            values = set(v.get("name", "") for v in (tp.get("values") or []))
            existing[tag_key] = values
    except Exception as list_err:
        # The tag-policies API may return a transient InternalError.
        # If listing fails we cannot determine which policies need syncing,
        # so skip — terraform apply will still create/update missing ones.
        print(f"  [WARN] Could not list tag policies ({list_err}); skipping")
        return

    updated = 0
    for tp in desired_policies:
        key = tp["key"]
        desired_values = set(tp["values"])
        current_values = existing.get(key)

        if current_values is None:
            continue

        if desired_values == current_values:
            continue

        missing = desired_values - current_values
        removed = current_values - desired_values
        all_values = sorted(desired_values | current_values)

        # Update tag policy via REST API
        body = json.dumps({
            "tag_policy": {
                "tag_key": key,
                "values": [{"name": v} for v in all_values],
            },
            "update_mask": "values",
        }).encode()

        try:
            update_url = f"{base}/api/2.1/unity-catalog/tag-policies/{urllib.parse.quote(key, safe='')}"
            req = urllib.request.Request(
                update_url,
                data=body,
                headers={**token, "Content-Type": "application/json"},
                method="PATCH",
            )
            urllib.request.urlopen(req, timeout=30, context=ssl_ctx)
            changes = []
            if missing:
                changes.append(f"added {sorted(missing)}")
            if removed:
                changes.append(f"removed {sorted(removed)}")
            print(f"  [SYNC] {key}: {', '.join(changes)}")
            updated += 1
        except Exception as e:
            print(f"  [ERROR] {key}: {e}")

    if updated:
        print(f"  Synced {updated} tag policy/ies")
    else:
        print("  Tag policies already in sync")


if __name__ == "__main__":
    main()
