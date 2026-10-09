#!/usr/bin/env python3
"""Promote and verify the classified-column completeness manifest."""
from __future__ import annotations
import argparse, json, os, sys, time
from pathlib import Path
from typing import Any, Iterable, Mapping
import hcl2

def _load_hcl(path: Path) -> dict[str, Any]:
    with path.open() as handle: return hcl2.load(handle)

def classified_columns(config: Mapping[str, Any]) -> dict[str, list[str]]:
    found: dict[str, set[str]] = {}
    for item in config.get("tag_assignments", []) or []:
        if str(item.get("entity_type", "columns")) != "columns": continue
        entity = str(item.get("entity_name", "")).strip()
        tag = str(item.get("tag_key") or item.get("tag_name") or "").strip()
        if len(entity.split(".")) == 4 and tag.startswith("class."):
            found.setdefault(entity, set()).add(tag)
    return {column: sorted(tags) for column, tags in sorted(found.items())}

def parse_catalog_map(value: str) -> dict[str, str]:
    out = {}
    for item in filter(None, (part.strip() for part in value.split(","))):
        source, sep, dest = item.partition("=")
        if not sep or not source or not dest: raise ValueError(f"catalog mapping {item!r} is not <source>=<destination>")
        out[source] = dest
    return out

def remap_manifest(manifest: Mapping[str, Iterable[str]], catalog_map: Mapping[str, str]) -> dict[str, list[str]]:
    remapped = {}
    for column, tags in manifest.items():
        parts = column.split(".")
        if len(parts) != 4: raise ValueError(f"classified column {column!r} is not catalog.schema.table.column")
        parts[0] = catalog_map.get(parts[0], parts[0])
        remapped[".".join(parts)] = sorted(set(map(str, tags)))
    return dict(sorted(remapped.items()))

def parse_ack(value: str) -> set[str]: return {item.strip() for item in value.split(",") if item.strip()}

def missing_classification(expected: Mapping[str, Any], live_columns: Iterable[str], ack: Iterable[str] = ()) -> list[str]:
    live, acknowledged = ({str(x).lower() for x in live_columns}, {str(x).lower() for x in ack})
    return sorted(column for column in expected if column.lower() not in live and column.lower() not in acknowledged)

def write_manifest(source: Path, destination: Path, catalog_map: str) -> None:
    payload = remap_manifest(classified_columns(_load_hcl(source) if source.is_file() else {}), parse_catalog_map(catalog_map))
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_name(f".{destination.name}.tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n"); os.replace(temp, destination)

def _value(config: Mapping[str, Any], key: str) -> str:
    value = config.get(key, ""); value = value[0] if isinstance(value, list) and value else value
    return str(value).strip()

def live_classified_columns(env_dir: Path) -> set[str]:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
    auth, env = _load_hcl(env_dir / "auth.auto.tfvars"), _load_hcl(env_dir / "env.auto.tfvars")
    client = WorkspaceClient(host=_value(auth, "databricks_workspace_host"), client_id=_value(auth, "databricks_client_id"), client_secret=_value(auth, "databricks_client_secret"))
    warehouse = _value(env, "sql_warehouse_id")
    if not warehouse: raise RuntimeError("sql_warehouse_id is required to check production classification")
    stmt = client.statement_execution.execute_statement(warehouse_id=warehouse, statement="SELECT DISTINCT concat(catalog_name, '.', schema_name, '.', table_name, '.', column_name) FROM system.information_schema.column_tags WHERE lower(tag_name) LIKE 'class.%'", wait_timeout="50s")
    while stmt.status.state not in (StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
        time.sleep(2); stmt = client.statement_execution.get_statement(stmt.statement_id)
    if stmt.status.state != StatementState.SUCCEEDED: raise RuntimeError(f"classification query failed ({stmt.status.state}): {stmt.status.error}")
    return {str(row[0]) for row in (stmt.result.data_array or []) if row}

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(); sub = parser.add_subparsers(dest="command", required=True)
    write = sub.add_parser("write"); write.add_argument("--source", type=Path, required=True); write.add_argument("--destination", type=Path, required=True); write.add_argument("--catalog-map", default="")
    check = sub.add_parser("check"); check.add_argument("--env-dir", type=Path, required=True); check.add_argument("--mode", choices=("deterministic", "legacy"), default="legacy"); check.add_argument("--ack", default="")
    args = parser.parse_args(argv)
    if args.command == "write": write_manifest(args.source, args.destination, args.catalog_map); return 0
    manifest = args.env_dir / "generated/expected_classification.json"
    if not manifest.is_file(): return 0
    missing = missing_classification(json.loads(manifest.read_text()), live_classified_columns(args.env_dir), parse_ack(args.ack))
    if not missing: return 0
    label = "ERROR" if args.mode == "deterministic" else "WARNING"
    print(f"{label}: columns classified in the promoted source but untagged in production:", file=sys.stderr)
    for column in missing: print(f"  - {column}", file=sys.stderr)
    print('Acknowledge reviewed exceptions with ACK_UNCLASSIFIED="cat.sch.table.column,...".', file=sys.stderr)
    return 1 if args.mode == "deterministic" else 0

if __name__ == "__main__": raise SystemExit(main())
