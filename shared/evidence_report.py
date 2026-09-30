#!/usr/bin/env python3
"""Build versioned JSON and Markdown compliance evidence for a GenieRails footprint.

Offline mode (the default) uses local configuration only and never imports the
Databricks SDK. Set GENIERAILS_EVIDENCE_INTEGRATION=1 to collect live Unity
Catalog state through a Databricks SQL warehouse.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

SCHEMA_VERSION = "1.0"
INTEGRATION_ENV = "GENIERAILS_EVIDENCE_INTEGRATION"


def _utc(value: datetime | None = None) -> str:
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _fqn(row: dict[str, Any]) -> str:
    return ".".join(str(row[k]) for k in ("catalog", "schema", "table", "column"))


def assemble_report(
    columns: Iterable[dict[str, Any]],
    *,
    classifications: Iterable[dict[str, Any]] = (),
    tags: Iterable[dict[str, Any]] = (),
    masks: Iterable[dict[str, Any]] = (),
    policies: Iterable[dict[str, Any]] = (),
    grants: Iterable[dict[str, Any]] = (),
    generated_at: datetime | None = None,
    approved_by: str = "unapproved",
    approved_at: str | None = None,
    source: str = "offline-config",
) -> dict[str, Any]:
    """Join normalized state rows into a stable, auditable report."""
    tag_map: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in tags:
        tag_map[_fqn(row)].append({"name": str(row["name"]), "value": str(row.get("value", ""))})
    mask_map = {_fqn(row): str(row["name"]) for row in masks}
    policy_map: dict[str, list[str]] = defaultdict(list)
    for row in policies:
        policy_map[_fqn(row)].append(str(row["name"]))
    class_map = {_fqn(row): row for row in classifications}
    grant_map: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in grants:
        grant_map[_fqn(row)].append({
            "principal": str(row["principal"]),
            "privilege": str(row["privilege"]),
            "scope": str(row.get("scope", "COLUMN")),
        })

    evidence = []
    for column in sorted(columns, key=_fqn):
        key = _fqn(column)
        scan = class_map.get(key, {})
        evidence.append({
            "catalog": str(column["catalog"]),
            "schema": str(column["schema"]),
            "table": str(column["table"]),
            "column": str(column["column"]),
            "classification": {
                "status": str(scan.get("status", "not_run")),
                "scanned_at": scan.get("scanned_at"),
            },
            "detected_tags": sorted(tag_map.get(key, []), key=lambda item: (item["name"], item["value"])),
            "applied_mask": mask_map.get(key),
            "applied_policies": sorted(set(policy_map.get(key, []))),
            "grants": sorted(grant_map.get(key, []), key=lambda item: (item["principal"], item["privilege"], item["scope"])),
        })

    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "genierails.compliance_evidence",
        "header": {
            "generated_at": _utc(generated_at),
            "approved_by": approved_by,
            "approved_at": approved_at,
            "source": source,
        },
        "evidence": evidence,
    }


def render_markdown(report: dict[str, Any]) -> str:
    """Render a compact human-readable view of the canonical JSON artifact."""
    header = report["header"]
    lines = [
        "# GenieRails Compliance Evidence",
        "",
        f"- Schema version: `{report['schema_version']}`",
        f"- Generated at: `{header['generated_at']}`",
        f"- Source: `{header['source']}`",
        f"- Approved by: `{header['approved_by']}`",
        f"- Approved at: `{header['approved_at'] or 'pending'}`",
    ]
    if "column_privileges_availability" in report:
        availability = report["column_privileges_availability"]
        detail = ", ".join(f"{catalog}={status}" for catalog, status in sorted(availability.items())) or "not queried"
        lines.append(f"- Column privileges: `{detail}`")
    lines.extend([
        "", "| Table | Column | Scan status | Scanned at | Tags | Applied mask / policies | Grants |",
        "|---|---|---|---|---|---|---|",
    ])
    for item in report["evidence"]:
        table = ".".join((item["catalog"], item["schema"], item["table"]))
        tags = ", ".join(f"{tag['name']}={tag['value']}" for tag in item["detected_tags"]) or "—"
        grants = ", ".join(
            f"{grant['principal']}:{grant['privilege']} ({grant['scope']})" for grant in item["grants"]
        ) or "—"
        protections = ", ".join(([item["applied_mask"]] if item["applied_mask"] else []) + item["applied_policies"]) or "—"
        values = [table, item["column"], item["classification"]["status"],
                  item["classification"]["scanned_at"] or "—", tags,
                  protections, grants]
        lines.append("| " + " | ".join(str(value).replace("|", "\\|") for value in values) + " |")
    return "\n".join(lines) + "\n"


def _load_hcl(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        import hcl2
    except ImportError as exc:
        raise RuntimeError("python-hcl2 is required; install it with: pip install python-hcl2") from exc
    with path.open(encoding="utf-8") as handle:
        return hcl2.load(handle)


def configured_columns(env_dir: Path) -> list[dict[str, str]]:
    """Return column footprint declared by environment tag assignments."""
    configs = [env_dir / "data_access" / "abac.auto.tfvars", env_dir / "abac.auto.tfvars"]
    found: dict[str, dict[str, str]] = {}
    for config in configs:
        for assignment in _load_hcl(config).get("tag_assignments", []):
            if assignment.get("entity_type") != "columns":
                continue
            parts = str(assignment.get("entity_name", "")).split(".")
            if len(parts) == 4:
                row = dict(zip(("catalog", "schema", "table", "column"), parts))
                found[_fqn(row)] = row
    return list(found.values())


def configured_tags(env_dir: Path) -> list[dict[str, str]]:
    rows = []
    for config in (env_dir / "data_access" / "abac.auto.tfvars", env_dir / "abac.auto.tfvars"):
        for item in _load_hcl(config).get("tag_assignments", []):
            parts = str(item.get("entity_name", "")).split(".")
            if item.get("entity_type") == "columns" and len(parts) == 4:
                row = dict(zip(("catalog", "schema", "table", "column"), parts))
                row.update(name=str(item.get("tag_key", "")), value=str(item.get("tag_value", "")))
                rows.append(row)
    return rows


def configured_tables(env_dir: Path) -> list[str]:
    """Return the tables managed by configured Genie agents."""
    config = _load_hcl(env_dir / "env.auto.tfvars")
    tables = set(str(table) for table in config.get("uc_tables", []))
    for space in config.get("genie_spaces", []):
        tables.update(str(table) for table in space.get("uc_tables", []))
    # Tag assignments remain a useful fallback for governance-only environments.
    tables.update(".".join(_fqn(row).split(".")[:3]) for row in configured_columns(env_dir))
    return sorted(table for table in tables if len(table.split(".")) == 3)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def collect_live_state(columns: list[dict[str, str]], tables: list[str], warehouse_id: str) -> dict[str, Any]:
    """Read live state through the SDK; called only in explicitly enabled integration mode."""
    if not tables:
        return {name: [] for name in ("columns", "classifications", "tags", "masks", "policies", "grants")}
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(product="genierails-evidence", product_version=SCHEMA_VERSION)
    def query(sql: str) -> list[list[Any]]:
        response = client.statement_execution.execute_statement(
            statement=sql, warehouse_id=warehouse_id, wait_timeout="50s"
        )
        state = str(getattr(getattr(response, "status", None), "state", ""))
        while state.endswith(("PENDING", "RUNNING")):
            time.sleep(1)
            response = client.statement_execution.get_statement(response.statement_id)
            state = str(getattr(getattr(response, "status", None), "state", ""))
        if state.endswith("FAILED"):
            error = getattr(getattr(response, "status", None), "error", None)
            raise RuntimeError(getattr(error, "message", "Databricks SQL statement failed"))
        return list(getattr(getattr(response, "result", None), "data_array", None) or [])

    table_key_sql = ", ".join(_sql_literal(key) for key in tables)
    live_columns = query("SELECT table_catalog, table_schema, table_name, column_name FROM system.information_schema.columns WHERE concat(table_catalog, '.', table_schema, '.', table_name) IN (" + table_key_sql + ")")
    live_column_dicts = [dict(zip(("catalog", "schema", "table", "column"), row[:4])) for row in live_columns]
    keys = ", ".join(_sql_literal(_fqn(row)) for row in live_column_dicts)
    if not keys:
        return {name: [] for name in ("columns", "classifications", "tags", "masks", "policies", "grants")}
    predicate = "concat(table_catalog, '.', table_schema, '.', table_name, '.', column_name) IN (" + keys + ")"
    tag_rows = query("SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value FROM system.information_schema.column_tags WHERE concat(catalog_name, '.', schema_name, '.', table_name, '.', column_name) IN (" + keys + ")")
    # Azure's live view has no mask catalog/schema columns, so mask_name must
    # remain bare; table_catalog/table_schema identify the protected table.
    mask_rows = query("SELECT table_catalog, table_schema, table_name, column_name, mask_name FROM system.information_schema.column_masks WHERE concat(table_catalog, '.', table_schema, '.', table_name, '.', column_name) IN (" + keys + ")")
    # COLUMN_PRIVILEGES is optional in Databricks information_schema. Discover it
    # per catalog rather than assuming the system-wide relation exists (Azure
    # workspaces commonly expose no column-level privilege relation at all).
    grant_rows = []
    column_privileges_availability = {}
    for catalog in sorted({key.split(".")[0] for key in tables}):
        information_schema = _sql_identifier(catalog) + ".information_schema"
        relations = query("SHOW TABLES IN " + information_schema)
        available = any(len(row) > 1 and str(row[1]).lower() == "column_privileges" for row in relations)
        column_privileges_availability[catalog] = "available" if available else "not_available"
        if available:
            grant_rows.extend(query("SELECT table_catalog, table_schema, table_name, column_name, grantee, privilege_type FROM " + information_schema + ".column_privileges WHERE " + predicate))
    table_grant_rows = query("SELECT table_catalog, table_schema, table_name, grantee, privilege_type FROM system.information_schema.table_privileges WHERE concat(table_catalog, '.', table_schema, '.', table_name) IN (" + table_key_sql + ")")
    schema_keys = sorted({".".join(key.split(".")[:2]) for key in tables})
    catalog_keys = sorted({key.split(".")[0] for key in tables})
    schema_grant_rows = query("SELECT catalog_name, schema_name, grantee, privilege_type FROM system.information_schema.schema_privileges WHERE concat(catalog_name, '.', schema_name) IN (" + ", ".join(_sql_literal(key) for key in schema_keys) + ")")
    catalog_grant_rows = query("SELECT catalog_name, grantee, privilege_type FROM system.information_schema.catalog_privileges WHERE catalog_name IN (" + ", ".join(_sql_literal(key) for key in catalog_keys) + ")")
    # Azure's live view likewise has no filter catalog/schema columns.
    policy_rows = query("SELECT table_catalog, table_schema, table_name, filter_name FROM system.information_schema.row_filters WHERE concat(table_catalog, '.', table_schema, '.', table_name) IN (" + table_key_sql + ")")

    base = lambda row: dict(zip(("catalog", "schema", "table", "column"), row[:4]))
    result: dict[str, Any] = {
        "columns": live_column_dicts, "classifications": [],
        "tags": [], "masks": [], "policies": [], "grants": [],
    }
    result["column_privileges_availability"] = column_privileges_availability
    for row in tag_rows:
        result["tags"].append({**base(row), "name": row[4], "value": row[5]})
    for row in mask_rows:
        result["masks"].append({**base(row), "name": row[4]})
    for row in grant_rows:
        result["grants"].append({**base(row), "principal": row[4], "privilege": row[5], "scope": "COLUMN"})
    by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for column in result["columns"]:
        by_table[".".join(_fqn(column).split(".")[:3])].append(column)
    for row in table_grant_rows:
        for column in by_table[".".join(str(value) for value in row[:3])]:
            result["grants"].append({**column, "principal": row[3], "privilege": row[4], "scope": "TABLE"})
    for row in schema_grant_rows:
        prefix = ".".join(str(value) for value in row[:2]) + "."
        for table, table_columns in by_table.items():
            if table.startswith(prefix):
                for column in table_columns:
                    result["grants"].append({**column, "principal": row[2], "privilege": row[3], "scope": "SCHEMA"})
    for row in catalog_grant_rows:
        prefix = str(row[0]) + "."
        for table, table_columns in by_table.items():
            if table.startswith(prefix):
                for column in table_columns:
                    result["grants"].append({**column, "principal": row[1], "privilege": row[2], "scope": "CATALOG"})
    for row in policy_rows:
        for column in by_table[".".join(str(value) for value in row[:3])]:
            result["policies"].append({**column, "name": row[3]})
    try:
        class_rows = query("SELECT catalog_name, schema_name, table_name, column_name, class_tag, latest_detected_time FROM system.data_classification.results WHERE concat(catalog_name, '.', schema_name, '.', table_name, '.', column_name) IN (" + keys + ")")
        classification_available = True
    except Exception:
        class_rows = []
        classification_available = False
    detected = set()
    for row in class_rows:
        column = base(row)
        detected.add(_fqn(column))
        result["classifications"].append({**column, "status": "detected", "scanned_at": row[5]})
        result["tags"].append({**column, "name": "data_classification", "value": row[4]})
    for column in result["columns"]:
        if _fqn(column) not in detected:
            status = "no_detection_record" if classification_available else "unavailable"
            result["classifications"].append({**column, "status": status, "scanned_at": None})
    return result


def write_report(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = "compliance-evidence-v" + report["schema_version"]
    json_path, md_path = output_dir / f"{stem}.json", output_dir / f"{stem}.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, md_path


def main(argv: list[str] | None = None, *, collector: Callable[..., dict[str, Any]] = collect_live_state) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-dir", type=Path, default=Path.cwd())
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--warehouse-id", default=os.getenv("DATABRICKS_WAREHOUSE_ID"))
    parser.add_argument("--approved-by", default=os.getenv("GENIERAILS_EVIDENCE_APPROVED_BY", "unapproved"))
    parser.add_argument("--approved-at", default=os.getenv("GENIERAILS_EVIDENCE_APPROVED_AT"))
    args = parser.parse_args(argv)
    columns, tags = configured_columns(args.env_dir), configured_tags(args.env_dir)
    tables = configured_tables(args.env_dir)
    live = os.getenv(INTEGRATION_ENV, "").lower() in {"1", "true", "yes"}
    if live:
        if not args.warehouse_id:
            parser.error(f"--warehouse-id or DATABRICKS_WAREHOUSE_ID is required when {INTEGRATION_ENV}=1")
        state = collector(columns, tables, args.warehouse_id)
        report = assemble_report(state["columns"], classifications=state["classifications"], tags=state["tags"], masks=state["masks"], policies=state["policies"], grants=state["grants"], approved_by=args.approved_by, approved_at=args.approved_at, source="databricks-live")
        report["column_privileges_availability"] = state.get("column_privileges_availability", {})
    else:
        report = assemble_report(columns, tags=tags, approved_by=args.approved_by, approved_at=args.approved_at)
    paths = write_report(report, args.output_dir or args.env_dir / "generated" / "evidence")
    print("Wrote " + " and ".join(str(path) for path in paths))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
