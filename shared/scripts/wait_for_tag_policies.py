#!/usr/bin/env python3
"""Wait until governed tag policies are visible with the expected values.

This is used between the account and data_access Terraform applies. Databricks
can acknowledge tag policy creation before the FGAC compiler can resolve the
same keys/values, so a fixed sleep is not reliable enough.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

WORK_DIR = Path.cwd()


def _load_hcl(path: Path) -> dict:
    try:
        import hcl2
    except ImportError:
        import subprocess

        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", "python-hcl2"],
        )
        import hcl2

    with open(path) as f:
        return hcl2.load(f)


def _str(value) -> str:
    return (value[0] if isinstance(value, list) else value or "").strip()


def _load_auth() -> dict:
    auth_path = WORK_DIR / "auth.auto.tfvars"
    if not auth_path.exists():
        return {}
    cfg = _load_hcl(auth_path)
    mapping = {
        "databricks_workspace_host": "DATABRICKS_HOST",
        "databricks_client_id": "DATABRICKS_CLIENT_ID",
        "databricks_client_secret": "DATABRICKS_CLIENT_SECRET",
    }
    for tfvar_key, env_key in mapping.items():
        val = _str(cfg.get(tfvar_key, ""))
        if val:
            os.environ[env_key] = val
    return cfg


def _list_tag_policies(workspace_client) -> dict[str, set[str]]:
    visible: dict[str, set[str]] = {}
    for tp in workspace_client.tag_policies.list_tag_policies():
        key = getattr(tp, "tag_key", "") or ""
        if not key:
            continue
        values = {
            getattr(v, "name", "") or ""
            for v in (getattr(tp, "values", None) or [])
            if getattr(v, "name", "") or ""
        }
        visible[key] = values
    return visible


def _extract_required_refs(data_access_cfg: dict | None) -> tuple[set[str], dict[str, set[str]]]:
    required_keys: set[str] = set()
    required_values: dict[str, set[str]] = {}
    if not data_access_cfg:
        return required_keys, required_values

    for assignment in data_access_cfg.get("tag_assignments", []):
        key = assignment.get("tag_key", "")
        value = assignment.get("tag_value", "")
        if not key:
            continue
        required_keys.add(key)
        if value:
            required_values.setdefault(key, set()).add(value)

    policies = data_access_cfg.get("fgac_policies", []) or []
    for policy in policies:
        condition = " ".join(
            str(policy.get(field, "") or "")
            for field in ("match_condition", "when_condition")
        )
        for key in re.findall(r"hasTag\(\s*'([^']+)'\s*\)", condition):
            required_keys.add(key)
        for key, value in re.findall(
            r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)",
            condition,
        ):
            required_keys.add(key)
            required_values.setdefault(key, set()).add(value)

    return required_keys, required_values


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Wait until all desired tag policies are visible.",
    )
    parser.add_argument(
        "tfvars_path",
        nargs="?",
        default=str(WORK_DIR / "abac.auto.tfvars"),
        help="Path to abac.auto.tfvars (default: ./abac.auto.tfvars)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=120,
        help="Max time to wait per cycle before recreate attempt (default: 120)",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=5,
        help="Polling interval in seconds (default: 5)",
    )
    parser.add_argument(
        "--required-values-from",
        default="",
        help=(
            "Optional data_access abac.auto.tfvars path. When provided, the wait"
            " only requires keys/values referenced by tag_assignments and FGAC"
            " conditions, not every historical preserved tag-policy value."
        ),
    )
    args = parser.parse_args()

    tfvars_path = Path(args.tfvars_path)
    if not tfvars_path.exists():
        print(f"  [SKIP] {tfvars_path} not found")
        return 0

    cfg = _load_hcl(tfvars_path)
    desired_policies = cfg.get("tag_policies", [])
    if not desired_policies:
        print("  [SKIP] No tag_policies found in config")
        return 0

    auth_cfg = _load_auth()
    host = _str(auth_cfg.get("databricks_workspace_host", "")) or os.environ.get(
        "DATABRICKS_HOST", ""
    )
    client_id = _str(auth_cfg.get("databricks_client_id", "")) or os.environ.get(
        "DATABRICKS_CLIENT_ID", ""
    )
    client_secret = _str(
        auth_cfg.get("databricks_client_secret", "")
    ) or os.environ.get("DATABRICKS_CLIENT_SECRET", "")
    if not host:
        print(
            "  [ERROR] No workspace host found; cannot verify tag policy visibility"
        )
        return 1

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(
        host=host,
        client_id=client_id or None,
        client_secret=client_secret or None,
        product="genierails",
        product_version="0.1.0",
    )

    desired = {
        tp["key"]: set(tp.get("values", []))
        for tp in desired_policies
        if tp.get("key")
    }
    required_keys = set(desired)
    required_values = {key: set(values) for key, values in desired.items()}

    if args.required_values_from:
        refs_path = Path(args.required_values_from)
        if refs_path.exists():
            refs_cfg = _load_hcl(refs_path)
            ref_keys, ref_values = _extract_required_refs(refs_cfg)
            if ref_keys:
                required_keys = ref_keys
                required_values = {key: ref_values.get(key, set()) for key in ref_keys}
                print(
                    "  Waiting on tag refs used by data_access config:"
                    f" {len(required_keys)} key(s)"
                )
        else:
            print(f"  [WARN] required-values file not found: {refs_path}")

    max_recreate_attempts = 2

    for recreate_cycle in range(1 + max_recreate_attempts):
        deadline = time.time() + max(args.timeout_seconds, 1)
        attempt = 0
        last_missing: list[str] = []
        while time.time() <= deadline:
            attempt += 1
            try:
                visible = _list_tag_policies(w)
            except Exception as exc:
                print(
                    f"  [WAIT] Attempt {attempt}: could not list tag policies yet"
                    f" ({exc})"
                )
                time.sleep(max(args.poll_seconds, 1))
                continue

            missing: list[str] = []
            missing_keys: list[str] = []
            for key in sorted(required_keys):
                current_values = visible.get(key)
                if current_values is None:
                    missing.append(f"{key}: key not visible")
                    missing_keys.append(key)
                    continue
                desired_values = required_values.get(key, set())
                absent_values = sorted(desired_values - current_values)
                if absent_values:
                    missing.append(f"{key}: missing values {absent_values}")

            if not missing:
                print(
                    "  Tag policies visible to FGAC compiler:"
                    f" {len(required_keys)} key(s) confirmed after {attempt} poll(s)"
                    + (f" (recreate cycle {recreate_cycle})" if recreate_cycle > 0 else "")
                )
                return 0

            if missing != last_missing:
                print(f"  [WAIT] Tag policies not fully visible yet ({attempt}):")
                for item in missing[:12]:
                    print(f"    - {item}")
                if len(missing) > 12:
                    print(f"    - ... and {len(missing) - 12} more")
                last_missing = missing

            time.sleep(max(args.poll_seconds, 1))

        # Timed out — if we have recreate attempts left, delete + recreate invisible keys
        if recreate_cycle < max_recreate_attempts and missing_keys:
            print(
                f"  [RECREATE] {len(missing_keys)} tag policy key(s) not visible after"
                f" {args.timeout_seconds}s — deleting and recreating (attempt"
                f" {recreate_cycle + 1}/{max_recreate_attempts})"
            )
            for key in missing_keys:
                policy_cfg = desired.get(key)
                if not policy_cfg:
                    continue
                # Delete if it exists (may be invisible to list but present in backend)
                try:
                    w.tag_policies.delete_tag_policy(tag_key=key)
                    print(f"    Deleted: {key}")
                except Exception:
                    pass  # may not exist
                # Recreate with desired values
                try:
                    from databricks.sdk.service.tags import TagPolicy, Value
                    w.tag_policies.create_tag_policy(
                        TagPolicy(
                            tag_key=key,
                            values=[Value(name=v) for v in policy_cfg],
                        )
                    )
                    print(f"    Recreated: {key} with {len(policy_cfg)} value(s)")
                except Exception as exc:
                    print(f"    [WARN] Could not recreate {key}: {exc}")
            print(f"  Waiting for recreated policies to become visible...")
            time.sleep(10)  # brief settle before re-polling
            continue

    print("  [ERROR] Timed out waiting for tag policies to become visible")
    for item in last_missing[:20]:
        print(f"    - {item}")
    if len(last_missing) > 20:
        print(f"    - ... and {len(last_missing) - 20} more")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
