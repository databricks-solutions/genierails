#!/usr/bin/env python3
"""Detect schema drift and rulebook drift for governed Genie tables.

Three independent checks, selected with --mode:

  forward drift (name-regex) — columns whose names match PII patterns but carry
    no governed classification tag.  A "column that should be tagged but isn't".

  reverse drift — tag_assignments in config whose entity_name references a
    column that no longer exists in the workspace.  A "stale rule".

  rulebook drift — tags ACTUALLY applied in the workspace (EVERY column tag on
    the in-scope tables, from system.information_schema.column_tags — including
    the `class.*` classification namespace and any governance key, without
    restricting to keys still present in the config) that NO policy or mask
    covers.  The "new prod tag with no rule" case AND the "rule dropped in
    promotion, orphaned tag left behind" case: the RULEBOOK is derived from the
    PROMOTED config only (account + data_access tag_policies + column-mask
    fgac_policies; the local generated/ draft is excluded), so a tag whose key
    was dropped from promotion has no coverage and is flagged.

--mode drift    (default) runs forward + reverse — the original behaviour.
--mode rulebook runs only the rulebook check.
--mode all      runs all three.

Designed to run from an env directory (e.g. envs/dev/) where env.auto.tfvars,
auth.auto.tfvars, and data_access/abac.auto.tfvars are accessible via relative paths.

Exit codes:
  0 — no drift detected
  1 — drift detected (forward, reverse, rulebook, or any combination)

Known limitations:
  - Overwrite-style rewrites (overwriteSchema=true on direct Delta paths) may
    require REPAIR TABLE ... SYNC METADATA before the drift query sees the
    latest schema.  Standard ALTER TABLE ADD/DROP/RENAME COLUMN DDL reflects
    immediately in system.information_schema.
  - PII name-pattern heuristics have false positives (e.g. patient_count) and
    false negatives (e.g. home_addr).  The regex is a starting filter.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

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


def extract_tag_policies(env_dir: Path) -> list[dict]:
    """tag_policies from the PROMOTED config only (the coverage source of truth).

    The rulebook must reflect what is actually promoted — account/abac.auto.tfvars
    (shared groups + tag_policies) and <env>/data_access/abac.auto.tfvars.  The
    local generated/ draft is DELIBERATELY excluded: a rule dropped during
    `make promote` but still lingering in the pre-split draft must NOT be counted
    as coverage, or a dropped rule's orphaned prod tag would be masked.
    Duplicate keys across the two promoted layers are harmless — build_rulebook()
    merges their allowed values.
    """
    policies: list[dict] = []
    for path in [
        env_dir.parent / "account" / "abac.auto.tfvars",
        env_dir / "data_access" / "abac.auto.tfvars",
    ]:
        cfg = _load_hcl(path)
        policies.extend(cfg.get("tag_policies", []) or [])
    return policies


def extract_fgac_policies(env_dir: Path) -> list[dict]:
    """fgac_policies (column masks + row filters) from the PROMOTED config only.

    Reads <env>/data_access/abac.auto.tfvars.  The local generated/ draft is
    DELIBERATELY excluded (see extract_tag_policies): coverage is derived strictly
    from what was promoted, so a rule dropped in promotion cannot be masked by a
    stale draft entry.
    """
    cfg = _load_hcl(env_dir / "data_access" / "abac.auto.tfvars")
    return list(cfg.get("fgac_policies", []) or [])


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


# ---------------------------------------------------------------------------
# Rulebook drift: prod-applied tags with no covering policy or mask
# ---------------------------------------------------------------------------

def _parse_condition_tag_refs(condition: str) -> tuple[set[tuple[str, str]], set[str]]:
    """Extract tag references from an fgac_policy match/when condition.

    Returns (value_refs, key_refs):
      - value_refs: {(key, value)} from hasTagValue('key', 'value')
      - key_refs:   {key}          from hasTag('key')   — covers any value of key
    """
    value_refs = set(re.findall(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", condition or ""))
    key_refs = set(re.findall(r"hasTag\(\s*'([^']+)'\s*\)", condition or ""))
    return value_refs, key_refs


COLUMN_MASK_POLICY_TYPE = "POLICY_TYPE_COLUMN_MASK"


def _is_column_mask(policy: dict) -> bool:
    """True only for an explicit column-mask policy.

    Allowlist, not denylist: a row filter, an unknown type, or a missing
    policy_type all return False.  Only a column mask matches COLUMN tags, so
    only a column mask may contribute column-tag coverage.
    """
    return (policy.get("policy_type") or "").strip().upper() == COLUMN_MASK_POLICY_TYPE


def build_rulebook(tag_policies: list[dict], fgac_policies: list[dict]) -> dict:
    """Compile the RULEBOOK for COLUMN-tag coverage.

    This audit reads `system.information_schema.column_tags`, so a COLUMN tag is
    covered only by things that actually act on column tags:

      - tag_policies declare the governance vocabulary: key -> {allowed values}.
        These are metastore-level, so their coverage is catalog-independent.
      - COLUMN MASK fgac_policies (policy_type == POLICY_TYPE_COLUMN_MASK, and
        ONLY those — allowlist) reference tags in their `match_condition`
        (hasTagValue()/hasTag()).  A mask only enforces within its own `catalog`,
        so this coverage is scoped PER CATALOG — a mask in catalog A does not
        cover a tag applied in catalog B.
      - Everything else — ROW FILTER (when_condition on TABLE tags), unknown
        policy types, and a missing policy_type — is EXCLUDED and never covers a
        column tag.

    Returns a dict:
      policy_vocab:    {key: {allowed value, ...}}                  (global)
      mask_value_refs: {catalog: {(key, value), ...}}   from column-mask match_condition
      mask_key_refs:   {catalog: {key, ...}}            from column-mask hasTag() (any value)
    """
    policy_vocab: dict[str, set[str]] = {}
    for tp in tag_policies or []:
        key = tp.get("key")
        if not key:
            continue
        policy_vocab.setdefault(key, set()).update(v for v in (tp.get("values") or []) if v)

    mask_value_refs: dict[str, set[tuple[str, str]]] = {}
    mask_key_refs: dict[str, set[str]] = {}
    for p in fgac_policies or []:
        # Only an explicit column mask matches COLUMN tags (via match_condition).
        # Row filters (when_condition on TABLE tags), unknown types, and a missing
        # policy_type are all excluded — coverage requires POLICY_TYPE_COLUMN_MASK.
        if not _is_column_mask(p):
            continue
        catalog = p.get("catalog", "") or ""
        value_refs, key_refs = _parse_condition_tag_refs(p.get("match_condition") or "")
        if value_refs:
            mask_value_refs.setdefault(catalog, set()).update(value_refs)
        if key_refs:
            mask_key_refs.setdefault(catalog, set()).update(key_refs)

    # Native classifier tags are facts, not a second enforcement vocabulary.
    # Resolve each mapped class.* semantic through the same governed-source ->
    # gr_treatment derivation used by generation. The class fact is covered
    # when its derived treatment is present in the promoted rulebook.
    from sensitivity_source import _CLASS_TO_GOVERNED
    from treatment_derivation import load_treatment_config

    treatment_cfg = load_treatment_config()
    treatment_by_source = {
        source: treatment.value
        for treatment in treatment_cfg.treatments
        for source in treatment.sources
    }
    class_treatments = {
        semantic: treatment_by_source[source]
        for semantic, source in _CLASS_TO_GOVERNED.items()
        if source in treatment_by_source
    }

    return {
        "policy_vocab": policy_vocab,
        "mask_value_refs": mask_value_refs,
        "mask_key_refs": mask_key_refs,
        "class_treatments": class_treatments,
        "treatment_tag_key": treatment_cfg.tag_key,
    }


def is_tag_covered(catalog: str, tag_key: str, tag_value: str, rulebook: dict) -> bool:
    """True if a tag_policy declares this key/value, or a COLUMN MASK in the SAME
    catalog references it."""
    if tag_key.lower().startswith("class."):
        semantic = tag_key.split(".", 1)[1].strip().lower().replace("-", "_").replace(" ", "_")
        treatment = rulebook.get("class_treatments", {}).get(semantic)
        if treatment:
            treatment_key = rulebook.get("treatment_tag_key", "gr_treatment")
            return is_tag_covered(catalog, treatment_key, treatment, rulebook)

    allowed = rulebook["policy_vocab"].get(tag_key)
    if allowed is not None and tag_value in allowed:
        return True
    if (tag_key, tag_value) in rulebook["mask_value_refs"].get(catalog, set()):
        return True
    if tag_key in rulebook["mask_key_refs"].get(catalog, set()):
        return True
    return False


def _coverage_gap(catalog: str, tag_key: str, tag_value: str, rulebook: dict) -> str:
    """Human-readable reason a tag is uncovered (assumes is_tag_covered is False)."""
    cat_value_refs = rulebook["mask_value_refs"].get(catalog, set())
    cat_key_refs = rulebook["mask_key_refs"].get(catalog, set())
    known_key = (
        tag_key in rulebook["policy_vocab"]
        or tag_key in cat_key_refs
        or any(k == tag_key for k, _ in cat_value_refs)
    )
    if not known_key:
        return "unknown tag key — no tag_policy declares it and no column mask in this catalog references it"
    return "value not covered — key is governed but no policy value or in-catalog column mask covers this value"


def find_uncovered_tags(applied_tags: list[dict], rulebook: dict) -> list[dict]:
    """Filter applied tags down to those the rulebook does not cover.

    applied_tags: dicts with keys catalog, schema, table, column, tag_key, tag_value.
    Returns the uncovered subset, each annotated with a ``reason``.
    """
    uncovered: list[dict] = []
    for t in applied_tags:
        catalog = t["catalog"]
        if is_tag_covered(catalog, t["tag_key"], t["tag_value"], rulebook):
            continue
        uncovered.append({**t, "reason": _coverage_gap(catalog, t["tag_key"], t["tag_value"], rulebook)})
    return uncovered


def _applied_tags_sql(managed_tables: list[str]) -> str:
    """SQL that reads EVERY column tag applied to the managed tables.

    Deliberately NOT restricted to `class.*` or to currently-known governed
    keys: a governance/enforcement tag whose key was COMPLETELY dropped during
    promotion still lingers in prod under a key no longer discoverable from the
    config.  Restricting the query to known keys would exclude that orphaned tag
    and it could never be flagged.  We fetch every applied tag and let the
    rulebook comparison decide coverage, so a dropped rule's orphaned tag surfaces
    as uncovered.
    """
    table_list = ", ".join(f"'{t}'" for t in managed_tables)
    return f"""\
SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value
FROM system.information_schema.column_tags
WHERE concat(catalog_name, '.', schema_name, '.', table_name) IN ({table_list})
ORDER BY catalog_name, schema_name, table_name, column_name, tag_name"""


def _query_applied_tags(w, warehouse_id: str, managed_tables: list[str]) -> list[dict]:
    """Read every tag applied to columns of the managed tables (unrestricted)."""
    if not managed_tables:
        return []

    rows = _run_sql(w, warehouse_id, _applied_tags_sql(managed_tables))
    applied = []
    for row in rows:
        applied.append({
            "catalog": row[0],
            "schema": row[1],
            "table": row[2],
            "column": row[3],
            "tag_key": row[4],
            "tag_value": row[5] if len(row) > 5 else "",
        })
    return applied


def detect_rulebook_drift(
    w, warehouse_id: str, managed_tables: list[str], rulebook: dict,
) -> list[dict]:
    """Find prod-applied tags that no policy or mask in the rulebook covers."""
    applied = _query_applied_tags(w, warehouse_id, managed_tables)
    return find_uncovered_tags(applied, rulebook)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit governed Genie tables for schema drift and rulebook drift.",
    )
    parser.add_argument(
        "--mode",
        choices=["drift", "rulebook", "all"],
        default="drift",
        help=(
            "drift (default): forward + reverse schema drift; "
            "rulebook: prod-applied tags with no covering policy/mask; "
            "all: run every check."
        ),
    )
    args = parser.parse_args(argv)

    run_drift = args.mode in ("drift", "all")
    run_rulebook = args.mode in ("rulebook", "all")

    env_dir = Path.cwd()
    print("=" * 60)
    print("  Schema Drift Audit")
    print("=" * 60)
    print(f"  Env dir: {env_dir}")
    print(f"  Mode: {args.mode}")

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

    drift_found = False

    if run_drift:
        # Forward drift
        print("\n  Checking forward drift (untagged sensitive columns)...")
        forward = detect_forward_drift(w, warehouse_id, managed_tables, governed_keys)

        # Reverse drift
        print("  Checking reverse drift (stale tag assignments)...")
        reverse = detect_reverse_drift(w, warehouse_id, managed_tables, config_assignments)

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

    if run_rulebook:
        print("\n  Checking rulebook drift (prod tags with no covering policy/mask)...")
        rulebook = build_rulebook(
            extract_tag_policies(env_dir), extract_fgac_policies(env_dir),
        )
        uncovered = detect_rulebook_drift(w, warehouse_id, managed_tables, rulebook)
        if uncovered:
            drift_found = True
            print(f"\n  RULEBOOK DRIFT: {len(uncovered)} applied tag(s) with no covering policy/mask:")
            for t in uncovered:
                fqn = f"{t['catalog']}.{t['schema']}.{t['table']}.{t['column']}"
                print(f"    {fqn}  [{t['tag_key']}={t['tag_value']}]  -- {t['reason']}")

    if not drift_found:
        print("\n  No drift detected.")

    print()
    return 1 if drift_found else 0


if __name__ == "__main__":
    sys.exit(main())
