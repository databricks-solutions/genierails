#!/usr/bin/env python3
"""Promote and verify the classified-column completeness manifest."""
from __future__ import annotations

import argparse
import json
import os
import queue
import sys
import threading
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

import hcl2


def _load_hcl(path: Path) -> dict[str, Any]:
    with path.open() as handle:
        return hcl2.load(handle)


def classified_columns(config: Mapping[str, Any]) -> dict[str, list[str]]:
    """Columns classified in dev, using native tags or derived treatment proxy."""
    found: dict[str, set[str]] = {}
    for item in config.get("tag_assignments", []) or []:
        if str(item.get("entity_type", "columns")) != "columns":
            continue
        entity = str(item.get("entity_name", "")).strip()
        tag = str(item.get("tag_key") or item.get("tag_name") or "").strip()
        value = str(item.get("tag_value") or "").strip()
        if len(entity.split(".")) != 4:
            continue
        if tag.startswith("class."):
            found.setdefault(entity, set()).add(tag)
        elif tag == "gr_treatment":
            # Generation intentionally strips native source assignments. A
            # gr_treatment assignment exists only for a classified column and
            # is therefore the durable promotion-side completeness proxy.
            found.setdefault(entity, set()).add(f"gr_treatment:{value}")
    return {column: sorted(tags) for column, tags in sorted(found.items())}


def parse_catalog_map(value: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in filter(None, (part.strip() for part in value.split(","))):
        source, separator, destination = item.partition("=")
        if not separator or not source or not destination:
            raise ValueError(f"catalog mapping {item!r} is not <source>=<destination>")
        out[source] = destination
    return out


def remap_manifest(
    manifest: Mapping[str, Iterable[str]], catalog_map: Mapping[str, str],
) -> dict[str, list[str]]:
    remapped: dict[str, list[str]] = {}
    for column, tags in manifest.items():
        parts = column.split(".")
        if len(parts) != 4:
            raise ValueError(f"classified column {column!r} is not catalog.schema.table.column")
        parts[0] = catalog_map.get(parts[0], parts[0])
        remapped[".".join(parts)] = sorted(set(map(str, tags)))
    return dict(sorted(remapped.items()))


def parse_ack(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def missing_classification(
    expected: Mapping[str, Any], live_columns: Iterable[str], ack: Iterable[str] = (),
) -> list[str]:
    live = {str(column).lower() for column in live_columns}
    acknowledged = {str(column).lower() for column in ack}
    return sorted(
        column for column in expected
        if column.lower() not in live and column.lower() not in acknowledged
    )


def write_manifest(source: Path, destination: Path, catalog_map: str) -> None:
    config = _load_hcl(source) if source.is_file() else {}
    payload = remap_manifest(classified_columns(config), parse_catalog_map(catalog_map))
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, destination)


def _value(config: Mapping[str, Any], key: str) -> str:
    value = config.get(key, "")
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value).strip()


def _live_classified_columns_query(env_dir: Path, deadline: float) -> set[str]:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    auth = _load_hcl(env_dir / "auth.auto.tfvars")
    env = _load_hcl(env_dir / "env.auto.tfvars")
    client = WorkspaceClient(
        host=_value(auth, "databricks_workspace_host"),
        client_id=_value(auth, "databricks_client_id"),
        client_secret=_value(auth, "databricks_client_secret"),
    )
    warehouse = _value(env, "sql_warehouse_id")
    if not warehouse:
        raise RuntimeError("sql_warehouse_id is required to check production classification")
    statement = client.statement_execution.execute_statement(
        warehouse_id=warehouse,
        statement=(
            "SELECT DISTINCT concat(catalog_name, '.', schema_name, '.', table_name, '.', column_name) "
            "FROM system.information_schema.column_tags WHERE lower(tag_name) LIKE 'class.%'"
        ),
        wait_timeout="50s",
    )
    terminal = {
        StatementState.SUCCEEDED, StatementState.FAILED,
        StatementState.CANCELED, StatementState.CLOSED,
    }
    while statement.status.state not in terminal:
        if time.monotonic() >= deadline:
            try:
                client.statement_execution.cancel_execution(statement.statement_id)
            except Exception:
                pass
            raise TimeoutError(
                "production classification query exceeded its deadline")
        time.sleep(min(2, max(0, deadline - time.monotonic())))
        statement = client.statement_execution.get_statement(statement.statement_id)
    if statement.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(
            f"classification query failed ({statement.status.state}): {statement.status.error}")
    rows = list(statement.result.data_array or []) if statement.result else []
    chunk_count = int(getattr(statement.manifest, "total_chunk_count", 1) or 1)
    for chunk_index in range(1, chunk_count):
        if time.monotonic() >= deadline:
            raise TimeoutError(
                "production classification result paging exceeded its deadline")
        chunk = client.statement_execution.get_statement_result_chunk_n(
            statement.statement_id, chunk_index)
        rows.extend((chunk.data_array or []) if chunk else [])
    return {str(row[0]) for row in rows if row}


def live_classified_columns(env_dir: Path, timeout_seconds: int = 120) -> set[str]:
    """Read every live class-tagged column, paging with a hard wall deadline."""
    outcome: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)
    deadline = time.monotonic() + timeout_seconds

    def run() -> None:
        try:
            outcome.put((True, _live_classified_columns_query(env_dir, deadline)))
        except BaseException as exc:
            outcome.put((False, exc))

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    worker.join(max(0, deadline - time.monotonic()))
    if worker.is_alive():
        raise TimeoutError(
            f"production classification check exceeded hard timeout of {timeout_seconds} seconds")
    succeeded, value = outcome.get_nowait()
    if not succeeded:
        raise value
    return value


def _check(env_dir: Path, mode: str, ack: str, timeout_seconds: int) -> int:
    manifest = env_dir / "generated" / "expected_classification.json"
    if not manifest.is_file():
        if mode == "deterministic":
            print(
                "ERROR: deterministic release requires generated/expected_classification.json; "
                "run make promote-to from the reviewed source environment.",
                file=sys.stderr,
            )
            return 1
        return 0
    try:
        expected = json.loads(manifest.read_text())
    except Exception as exc:
        label = "ERROR" if mode == "deterministic" else "WARNING"
        print(f"{label}: cannot read expected classification manifest: {exc}", file=sys.stderr)
        return 1 if mode == "deterministic" else 0
    if not expected:
        if mode == "deterministic":
            print("NOTE: deterministic classification manifest is empty; no classified columns are expected.")
        return 0
    try:
        live = live_classified_columns(env_dir, timeout_seconds)
    except Exception as exc:
        label = "ERROR" if mode == "deterministic" else "WARNING"
        print(f"{label}: could not check production classification: {exc}", file=sys.stderr)
        return 1 if mode == "deterministic" else 0
    missing = missing_classification(expected, live, parse_ack(ack))
    if not missing:
        return 0
    label = "ERROR" if mode == "deterministic" else "WARNING"
    print(
        f"{label}: columns classified in the promoted source but untagged in production:",
        file=sys.stderr,
    )
    for column in missing:
        print(f"  - {column}", file=sys.stderr)
    print(
        'Acknowledge reviewed exceptions with ACK_UNCLASSIFIED="cat.sch.table.column,...".',
        file=sys.stderr,
    )
    return 1 if mode == "deterministic" else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    write = commands.add_parser("write")
    write.add_argument("--source", type=Path, required=True)
    write.add_argument("--destination", type=Path, required=True)
    write.add_argument("--catalog-map", default="")
    check = commands.add_parser("check")
    check.add_argument("--env-dir", type=Path, required=True)
    check.add_argument("--mode", choices=("deterministic", "legacy"), default="legacy")
    check.add_argument("--ack", default="")
    check.add_argument("--timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    if args.command == "write":
        write_manifest(args.source, args.destination, args.catalog_map)
        return 0
    return _check(args.env_dir, args.mode, args.ack, args.timeout_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
