#!/usr/bin/env python3
"""Detect schema drift: new columns missing governed classification tags,
and stale tag_assignments referencing columns that no longer exist.

Designed to run from an env directory (e.g. envs/dev/) where env.auto.tfvars,
auth.auto.tfvars, and data_access/abac.auto.tfvars are accessible via relative paths.

Exit codes:
  0 — no drift detected
  1 — drift detected (forward, reverse, or both)

Known limitations:
  - Overwrite-style rewrites (overwriteSchema=true on direct Delta paths) may
    require REPAIR TABLE ... SYNC METADATA before the drift query sees the
    latest schema.  Standard ALTER TABLE ADD/DROP/RENAME COLUMN DDL reflects
    immediately in system.information_schema.
  - PII name-pattern heuristics have false positives (e.g. patient_count) and
    false negatives (e.g. home_addr).  The regex is a starting filter.
"""
from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path

PII_COLUMN_PATTERN = re.compile(
    r"(?i)(ssn|social_sec|passport|dob|birth_?date|email|phone|"
    r"address|credit_?card|cvv|account_?num|diagnosis|medication|"
    r"patient|mrn|npi|insurance)"
)

DEFAULT_GOVERNED_KEYS = ["pii_level", "phi_level", "pci_level", "financial_sensitivity"]


def _str(v):
    return (v[0] if isinstance(v, list) else v or "").strip()


def _load_hcl(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        import hcl2
        with open(path) as f:
            return hcl2.load(f)
    except Exception:
        return {}


def extract_managed_tables(env_dir: Path) -> list[str]:
    """Read managed table FQNs from env.auto.tfvars (both shapes)."""
    cfg = _load_hcl(env_dir / "env.auto.tfvars")
    tables: list[str] = []
    for t in cfg.get("uc_tables", []):
        if t and t not in tables:
            tables.append(t)
    for space in cfg.get("genie_spaces", []):
        for t in space.get("uc_tables", []):
            if t and t not in tables:
                tables.append(t)
    return tables


def resolve_governed_keys(env_dir: Path) -> list[str]:
    """Resolve governed classification tag keys from config with 4-level fallback."""
    # 1. envs/account/abac.auto.tfvars → tag_policies[*].key
    account_abac = env_dir.parent / "account" / "abac.auto.tfvars"
    cfg = _load_hcl(account_abac)
    keys = [tp.get("key", "") for tp in cfg.get("tag_policies", []) if tp.get("key")]
    if keys:
        return keys

    # 2. data_access/abac.auto.tfvars → unique tag_assignments[*].tag_key
    da_abac = env_dir / "data_access" / "abac.auto.tfvars"
    cfg = _load_hcl(da_abac)
    keys = sorted({ta.get("tag_key", "") for ta in cfg.get("tag_assignments", []) if ta.get("tag_key")})
    if keys:
        return keys

    # 3. generated/abac.auto.tfvars → tag_policies[*].key
    gen_abac = env_dir / "generated" / "abac.auto.tfvars"
    cfg = _load_hcl(gen_abac)
    keys = [tp.get("key", "") for tp in cfg.get("tag_policies", []) if tp.get("key")]
    if keys:
        return keys

    # 4. Hardcoded fallback
    print("  WARNING: Could not find governed tag keys in any config file. Using defaults.")
    return list(DEFAULT_GOVERNED_KEYS)


def extract_config_tag_assignments(env_dir: Path) -> list[dict]:
    """Load tag_assignments from the most authoritative config file.

    Prefers generated/abac.auto.tfvars (pre-split source of truth) over
    data_access/abac.auto.tfvars, since generate-delta writes to generated/
    and the split hasn't run yet until the next make apply.
    """
    for path in [
        env_dir / "generated" / "abac.auto.tfvars",
        env_dir / "data_access" / "abac.auto.tfvars",
    ]:
        cfg = _load_hcl(path)
        assignments = cfg.get("tag_assignments", [])
        if assignments:
            return assignments
    return []


def _get_sdk_client(env_dir: Path):
    """Build a WorkspaceClient from auth.auto.tfvars."""
    cfg = _load_hcl(env_dir / "auth.auto.tfvars")
    host = _str(cfg.get("databricks_workspace_host", ""))
    client_id = _str(cfg.get("databricks_client_id", ""))
    client_secret = _str(cfg.get("databricks_client_secret", ""))
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(
        host=host or None,
        client_id=client_id or None,
        client_secret=client_secret or None,
        product="genierails",
        product_version="0.1.0",
    )


def _get_warehouse_id(env_dir: Path, w) -> str:
    cfg = _load_hcl(env_dir / "env.auto.tfvars")
    wh = _str(cfg.get("sql_warehouse_id", ""))
    if wh:
        return wh
    for warehouse in w.warehouses.list():
        if warehouse.id:
            return warehouse.id
    return ""


def _run_sql(w, warehouse_id: str, sql: str) -> list[list[str]]:
    from databricks.sdk.service.sql import StatementState
    r = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=warehouse_id, wait_timeout="50s",
    )
    while r.status and r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status and r.status.state == StatementState.FAILED:
        err = getattr(r.status, "error", None)
        msg = getattr(err, "message", str(err)) if err else "unknown"
        raise RuntimeError(f"SQL failed: {msg}")
    if r.result and r.result.data_array:
        return r.result.data_array
    return []


def detect_forward_drift(
    w, warehouse_id: str, managed_tables: list[str], governed_keys: list[str],
) -> list[tuple[str, str, str, str, str]]:
    """Find columns in managed tables that match PII patterns but lack a governed tag."""
    if not managed_tables or not governed_keys:
        return []

    table_list = ", ".join(f"'{t}'" for t in managed_tables)
    key_list = ", ".join(f"'{k}'" for k in governed_keys)

    sql = f"""\
WITH classification_tags AS (
  SELECT catalog_name AS ct_catalog, schema_name AS ct_schema,
         table_name AS ct_table, column_name AS ct_column
  FROM system.information_schema.column_tags
  WHERE tag_name IN ({key_list})
)
SELECT c.table_catalog, c.table_schema, c.table_name, c.column_name,
       COALESCE(c.comment, '') AS col_comment
FROM system.information_schema.columns c
LEFT ANTI JOIN classification_tags t
  ON  c.table_catalog = t.ct_catalog
  AND c.table_schema  = t.ct_schema
  AND c.table_name    = t.ct_table
  AND c.column_name   = t.ct_column
WHERE concat(c.table_catalog, '.', c.table_schema, '.', c.table_name) IN ({table_list})
ORDER BY c.table_catalog, c.table_schema, c.table_name, c.column_name"""

    rows = _run_sql(w, warehouse_id, sql)
    results = []
    for row in rows:
        catalog, schema, table, column = row[0], row[1], row[2], row[3]
        comment = row[4] if len(row) > 4 else ""
        if PII_COLUMN_PATTERN.search(column):
            results.append((catalog, schema, table, column, comment))
    return results


def detect_reverse_drift(
    w, warehouse_id: str, managed_tables: list[str], config_assignments: list[dict],
) -> list[str]:
    """Find tag_assignments in config whose entity_name references a non-existent column."""
    if not managed_tables or not config_assignments:
        return []

    table_list = ", ".join(f"'{t}'" for t in managed_tables)
    sql = f"""\
SELECT concat(table_catalog, '.', table_schema, '.', table_name, '.', column_name) AS fqn
FROM system.information_schema.columns
WHERE concat(table_catalog, '.', table_schema, '.', table_name) IN ({table_list})"""

    rows = _run_sql(w, warehouse_id, sql)
    live_columns = {row[0] for row in rows}

    stale = []
    for ta in config_assignments:
        if ta.get("entity_type") != "columns":
            continue
        entity = ta.get("entity_name", "")
        if not entity:
            continue
        table_fqn = ".".join(entity.split(".")[:3])
        if table_fqn not in managed_tables:
            continue
        if entity not in live_columns:
            stale.append(entity)
    return stale


def main() -> int:
    env_dir = Path.cwd()
    print("=" * 60)
    print("  Schema Drift Audit")
    print("=" * 60)
    print(f"  Env dir: {env_dir}")

    managed_tables = extract_managed_tables(env_dir)
    if not managed_tables:
        print("  No managed tables found in env.auto.tfvars — nothing to audit.")
        return 0
    print(f"  Managed tables: {len(managed_tables)}")

    governed_keys = resolve_governed_keys(env_dir)
    print(f"  Governed keys: {governed_keys}")

    config_assignments = extract_config_tag_assignments(env_dir)

    w = _get_sdk_client(env_dir)
    warehouse_id = _get_warehouse_id(env_dir, w)
    if not warehouse_id:
        print("  ERROR: No SQL warehouse available.")
        return 1

    # Forward drift
    print("\n  Checking forward drift (untagged sensitive columns)...")
    forward = detect_forward_drift(w, warehouse_id, managed_tables, governed_keys)

    # Reverse drift
    print("  Checking reverse drift (stale tag assignments)...")
    reverse = detect_reverse_drift(w, warehouse_id, managed_tables, config_assignments)

    # Report
    drift_found = False
    if forward:
        drift_found = True
        print(f"\n  FORWARD DRIFT: {len(forward)} untagged sensitive column(s):")
        for cat, sch, tbl, col, comment in forward:
            fqn = f"{cat}.{sch}.{tbl}.{col}"
            suffix = f"  -- {comment}" if comment else ""
            print(f"    {fqn}{suffix}")

    if reverse:
        drift_found = True
        print(f"\n  REVERSE DRIFT: {len(reverse)} stale tag assignment(s) (column no longer exists):")
        for entity in reverse:
            print(f"    {entity}")

    if not drift_found:
        print("\n  No schema drift detected.")

    print()
    return 1 if drift_found else 0


if __name__ == "__main__":
    sys.exit(main())
