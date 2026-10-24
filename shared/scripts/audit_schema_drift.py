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
    was dropped from promotion has no coverage and is flagged.  In a
    deterministic env a class.* tag is covered through the env's generation:
    its column's versioned gr_treatment and that value's column masks
    (deterministic_class_coverage).

--mode drift    (default) runs forward + reverse — the original behaviour.
--mode rulebook runs only the rulebook check.
--mode all      runs all three.

Designed to run from an env directory (e.g. envs/dev/) where env.auto.tfvars,
auth.auto.tfvars, and data_access/abac.auto.tfvars are accessible via relative paths.

Exit codes:
  0 — no drift detected
  1 — drift detected (forward, reverse, rulebook, or any combination)
  2 — audit error (invalid config, authentication/query failure, or no warehouse)

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
    if not path.is_file():
        return {}
    try:
        import hcl2
    except ImportError as exc:
        raise RuntimeError(
            f"cannot parse {path}: python-hcl2 is not installed"
        ) from exc
    try:
        with path.open() as f:
            value = hcl2.load(f)
    except Exception as exc:
        if path.name != "auth.auto.tfvars":
            raise RuntimeError(f"cannot parse {path}: {exc}") from exc
        value = None
    if value is None:
        # Raised outside the handler with no parser text, which can echo a
        # malformed secret (also via __context__).
        raise RuntimeError(f"cannot parse {path}: not valid HCL") from None
    if not isinstance(value, dict):
        raise RuntimeError(f"cannot parse {path}: expected top-level object")
    return value


def extract_managed_tables(env_dir: Path) -> list[str]:
    """Read the environment's effective managed table footprint."""
    from scripts.footprint import resolve_footprint
    return resolve_footprint(env_dir)


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
    from scripts.runtime_overlay import effective

    for path in [
        effective(env_dir, "abac.auto.tfvars"),
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


def extract_promoted_tag_assignments(env_dir: Path) -> list[dict]:
    """tag_assignments from the PROMOTED config only (see extract_fgac_policies)."""
    cfg = _load_hcl(env_dir / "data_access" / "abac.auto.tfvars")
    return list(cfg.get("tag_assignments", []) or [])


def _get_sdk_client(env_dir: Path):
    """Build a WorkspaceClient from auth.auto.tfvars."""
    from scripts.auth_configured import workspace_client
    return workspace_client(
        env_dir / "auth.auto.tfvars",
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


def _run_sql(w, warehouse_id: str, sql: str, *, complete: bool = False) -> list[list[str]]:
    """Every result row. With ``complete``, a truncated result raises instead of returning a prefix."""
    from databricks.sdk.service.sql import StatementState
    r = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=warehouse_id, wait_timeout="50s",
    )
    while r.status and r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        r = w.statement_execution.get_statement(r.statement_id)
    state = getattr(r.status, "state", None) if r.status else None
    if state != StatementState.SUCCEEDED:
        err = getattr(r.status, "error", None)
        msg = getattr(err, "message", str(err)) if err else "unknown"
        code = getattr(err, "error_code", None) if err else None
        code_suffix = f" [{code}]" if code else ""
        raise RuntimeError(f"SQL failed ({state or 'UNKNOWN'}){code_suffix}: {msg}")
    if complete and getattr(getattr(r, "manifest", None), "truncated", False):
        raise RuntimeError("SQL result was truncated; not every row was returned")
    rows = list(getattr(r.result, "data_array", None) or [])
    next_chunk = getattr(r.result, "next_chunk_index", None) if r.result else None
    if next_chunk is not None:
        seen = set()
        while next_chunk is not None:
            if next_chunk in seen:
                raise RuntimeError(f"SQL result paging repeated chunk {next_chunk}")
            seen.add(next_chunk)
            chunk = w.statement_execution.get_statement_result_chunk_n(r.statement_id, next_chunk)
            rows.extend(getattr(chunk, "data_array", None) or [])
            next_chunk = getattr(chunk, "next_chunk_index", None)
    else:
        chunk_count = int(getattr(getattr(r, "manifest", None), "total_chunk_count", 1) or 1)
        for chunk_index in range(1, chunk_count):
            chunk = w.statement_execution.get_statement_result_chunk_n(r.statement_id, chunk_index)
            rows.extend(getattr(chunk, "data_array", None) or [])
    return rows


def detect_forward_drift(
    w, warehouse_id: str, managed_tables: list[str], governed_keys: list[str],
    *, deterministic: bool = False,
) -> list[tuple[str, str, str, str, str]]:
    """Find columns in managed tables that match PII patterns but lack a governed tag."""
    if not managed_tables or not governed_keys:
        return []

    table_list = ", ".join(f"'{t}'" for t in managed_tables)
    key_list = ", ".join(f"'{k}'" for k in governed_keys)

    native_classification = " OR tag_name LIKE 'class.%'" if deterministic else ""
    sql = f"""\
WITH classification_tags AS (
  SELECT catalog_name AS ct_catalog, schema_name AS ct_schema,
         table_name AS ct_table, column_name AS ct_column
  FROM system.information_schema.column_tags
  WHERE tag_name IN ({key_list}){native_classification}
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
    # when its derived treatment is present in the promoted rulebook. This is
    # the legacy vocabulary; a deterministic env resolves its class.* tags per
    # column instead (deterministic_class_coverage, find_uncovered_tags).
    from sensitivity_source import CLASS_NAMESPACE, _CLASS_TO_GOVERNED, _normalize_semantic
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
    # Match ClassificationSource's config-driven class-label extension. Native
    # class.* tags remain read-only; this only resolves them to the promoted
    # GenieRails-owned treatment for coverage auditing.
    for treatment in treatment_cfg.treatments:
        for label in treatment.class_labels:
            semantic = _normalize_semantic(
                label[len(CLASS_NAMESPACE):]
                if label.lower().startswith(CLASS_NAMESPACE) else label
            )
            class_treatments[semantic] = treatment.value

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


CLASS_GAP_UNMAPPED = "unmapped"
CLASS_GAP_GENERATION = "generation"


def deterministic_class_coverage(
    env_dir: Path,
    classes_by_column: dict[str, list[str]],
    fgac_policies: list[dict],
    tag_assignments: list[dict],
) -> dict:
    """Coverage of a deterministic env's class.* tags, through its generation.

    A deterministic env doesn't map a class through treatment_config.json (the
    legacy vocabulary): generation resolves each column's classes to a
    versioned gr_treatment value (governance_policies.resolve_column) masked by
    the policies build_policies names for it. A class.* tag is covered only
    when all of these hold, else it is drift (fail closed):

      - the env's library (mask library, adopted custom masks, tag_treatments)
        maps the class;
      - the promoted config assigns the column a gr_treatment, and it is the
        value resolve_column gives the column's live classes now;
      - every column mask build_policies names for that value (the full one,
        and the partial one when a middle tier exists) is promoted in the
        column's catalog.

    ``classes_by_column``: live class semantics per column, as generation reads
    them (ClassificationSource.classes_by_column). Returns ``unmapped``
    {(column, semantic)} and ``columns`` {column: gap reason, "" if covered},
    keyed by lowercase column.
    """
    from governance_policies import (
        TAG_KEY, build_policies, normalize_column_overrides, resolve_column,
        value_sql_type, with_tag_treatments,
    )
    from scripts.custom_masks import effective_library

    settings = _load_hcl(env_dir / "env.auto.tfvars")
    library = with_tag_treatments(effective_library(env_dir), settings.get("tag_treatments"))
    overrides = normalize_column_overrides(settings.get("column_overrides"))
    known = {name for item in library["treatments"].values() for name in item["classes"]}
    promoted = {
        str(a.get("entity_name", "")).lower(): str(a.get("tag_value", ""))
        for a in tag_assignments or []
        if a.get("entity_type") == "columns" and a.get("tag_key") == TAG_KEY
    }
    masks: set[tuple[str, str, str]] = set()
    for p in fgac_policies or []:
        if not _is_column_mask(p):
            continue
        value_refs, _key_refs = _parse_condition_tag_refs(p.get("match_condition") or "")
        for key, value in value_refs:
            if key == TAG_KEY:
                masks.add((str(p.get("catalog", "")).lower(), value, str(p.get("function_name", ""))))

    unmapped: set[tuple[str, str]] = set()
    columns: dict[str, str] = {}
    for column in sorted(classes_by_column):
        key = column.lower()
        names = sorted({c.removeprefix("class.") for c in classes_by_column[column] if c})
        unmapped.update((key, name) for name in names if name not in known)
        value = promoted.get(key, "")
        sql_type = value_sql_type(value)
        if not value:
            columns[key] = f"no {TAG_KEY} for this column in the promoted config"
            continue
        if not sql_type:
            columns[key] = f"promoted {TAG_KEY}={value} is not a deterministic treatment value"
            continue
        derived = resolve_column(
            column, names, sql_type, library=library,
            treatment_versions=settings.get("treatment_versions") or {},
            column_overrides=overrides,
        ).value
        if derived != value:
            columns[key] = (f"the column's class.* tags derive {TAG_KEY}={derived}, "
                            f"but the promoted config assigns {value}")
            continue
        catalog = key.split(".", 1)[0]
        expected = build_policies(
            values_by_catalog={catalog: [value]},
            access_tier_groups=settings.get("access_tier_groups") or [],
        )
        missing = [p["function_name"] for p in expected
                   if (catalog, value, p["function_name"]) not in masks]
        columns[key] = (f"no promoted column mask in catalog {catalog} applies "
                        f"{', '.join(missing)} to {TAG_KEY}={value}") if missing else ""
    return {"unmapped": unmapped, "columns": columns}


def _deterministic_class_gap(tag: dict, coverage: dict) -> tuple[str, str] | None:
    """(kind, reason) when a class.* tag is uncovered in a deterministic env, else None."""
    from sensitivity_source import _semantic_from_tag

    column = f"{tag['catalog']}.{tag['schema']}.{tag['table']}.{tag['column']}".lower()
    semantic = _semantic_from_tag(tag["tag_key"], tag["tag_value"])
    if (column, semantic) in coverage["unmapped"]:
        return CLASS_GAP_UNMAPPED, (
            f"class not mapped — no mask-library treatment or tag_treatments entry maps "
            f"class.{semantic} (the column is fail-safe redacted)")
    reason = coverage["columns"].get(column)
    if reason is None:
        return CLASS_GAP_GENERATION, "no live class.* reading for this column"
    return (CLASS_GAP_GENERATION, f"treatment not covered — {reason}") if reason else None


def find_uncovered_tags(applied_tags: list[dict], rulebook: dict) -> list[dict]:
    """Filter applied tags down to those the rulebook does not cover.

    applied_tags: dicts with keys catalog, schema, table, column, tag_key, tag_value.
    Returns the uncovered subset, each annotated with a ``reason``. In a
    deterministic env (``rulebook["deterministic_classes"]``, from
    deterministic_class_coverage) a class.* tag is also annotated with its
    ``class_gap`` kind, which picks the fix to print.
    """
    deterministic = rulebook.get("deterministic_classes")
    uncovered: list[dict] = []
    for t in applied_tags:
        catalog = t["catalog"]
        if deterministic is not None and t["tag_key"].lower().startswith("class."):
            gap = _deterministic_class_gap(t, deterministic)
            if gap:
                uncovered.append({**t, "reason": gap[1], "class_gap": gap[0]})
            continue
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


def _query_live_classes(w, warehouse_id: str, managed_tables: list[str]) -> dict[str, list[str]]:
    """Live class semantics per column, read as deterministic generation reads them."""
    from sensitivity_source import ClassificationSource

    source = ClassificationSource.from_run_sql(
        lambda sql: _run_sql(w, warehouse_id, sql), managed_tables, require_native=True,
    )
    return source.classes_by_column()


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
    parser.add_argument(
        "--env-name", default="",
        help="env name, so a class.* coverage gap prints its fix commands",
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

    try:
        from scripts.footprint import FootprintError
        managed_tables = extract_managed_tables(env_dir)
    except ImportError as exc:
        print(
            f"  ERROR: audit could not complete: python-hcl2 is not installed ({exc})",
            file=sys.stderr,
        )
        return 2
    except FootprintError as exc:
        print(f"  ERROR: {exc}")
        return 2
    if not managed_tables:
        print("  No managed tables found in the environment footprint — nothing to audit.")
        return 0
    print(f"  Managed tables: {len(managed_tables)}")

    governed_keys = resolve_governed_keys(env_dir)
    print(f"  Governed keys: {governed_keys}")
    deterministic = _load_hcl(env_dir / "env.auto.tfvars").get(
        "governance_mode", "legacy"
    ) == "deterministic"

    config_assignments = extract_config_tag_assignments(env_dir)

    w = _get_sdk_client(env_dir)
    warehouse_id = _get_warehouse_id(env_dir, w)
    if not warehouse_id:
        print("  ERROR: No SQL warehouse available.")
        return 2

    drift_found = False

    if run_drift:
        # Forward drift
        print("\n  Checking forward drift (untagged sensitive columns)...")
        forward = detect_forward_drift(
            w, warehouse_id, managed_tables, governed_keys,
            deterministic=deterministic,
        )

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
            print("    Re-run derive-assignments to remove assignments for dropped columns.")

    if run_rulebook:
        print("\n  Checking rulebook drift (prod tags with no covering policy/mask)...")
        fgac_policies = extract_fgac_policies(env_dir)
        rulebook = build_rulebook(extract_tag_policies(env_dir), fgac_policies)
        if deterministic:
            rulebook["deterministic_classes"] = deterministic_class_coverage(
                env_dir, _query_live_classes(w, warehouse_id, managed_tables),
                fgac_policies, extract_promoted_tag_assignments(env_dir),
            )
        uncovered = detect_rulebook_drift(w, warehouse_id, managed_tables, rulebook)
        if uncovered:
            drift_found = True
            print(f"\n  RULEBOOK DRIFT: {len(uncovered)} applied tag(s) with no covering policy/mask:")
            for t in uncovered:
                fqn = f"{t['catalog']}.{t['schema']}.{t['table']}.{t['column']}"
                print(f"    {fqn}  [{t['tag_key']}={t['tag_value']}]  -- {t['reason']}")
            gaps = [
                (f"{t['catalog']}.{t['schema']}.{t['table']}.{t['column']}", t["tag_key"].lower())
                for t in uncovered if t["tag_key"].lower().startswith("class.")
                and t.get("class_gap") != CLASS_GAP_GENERATION
            ]
            if gaps and args.env_name:
                from scripts.coverage_fix import fix_lines

                print("\n".join(fix_lines(args.env_name, env_dir, gaps)))
            # A mapped class whose generated treatment or masks are stale or
            # missing: scaffold-treatments would change nothing; regenerate.
            if args.env_name and any(t.get("class_gap") == CLASS_GAP_GENERATION for t in uncovered):
                from scripts.coverage_fix import regenerate_lines

                print("\n".join(regenerate_lines(args.env_name, env_dir)))

    if not drift_found:
        print("\n  No drift detected.")

    print()
    return 1 if drift_found else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"  ERROR: audit could not complete: {exc}", file=sys.stderr)
        sys.exit(2)
