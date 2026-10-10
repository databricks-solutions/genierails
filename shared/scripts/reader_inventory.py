#!/usr/bin/env python3
"""Inventory effective readers and grant-capable principals for a governed table."""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, Protocol, Sequence


READ_PRIVILEGES = frozenset({"SELECT", "ALL PRIVILEGES"})
GRANT_PRIVILEGES = frozenset({"MANAGE"})


@dataclass(frozen=True, order=True)
class Grant:
    securable_type: str
    securable_name: str
    principal: str
    privilege: str

    def normalized(self) -> "Grant":
        return Grant(
            self.securable_type.upper(), self.securable_name.lower(),
            self.principal, self.privilege.upper(),
        )


class InventorySource(Protocol):
    def grants(self, securable_type: str, securable_name: str) -> Iterable[Grant]: ...
    def owner(self, securable_type: str, securable_name: str) -> str | None: ...
    def dependent_views(self, object_name: str) -> Iterable[str]: ...
    def group_members(self) -> Mapping[str, Iterable[str]]: ...
    def metastore_admins(self) -> Iterable[str]: ...


@dataclass(frozen=True)
class PrincipalEvidence:
    principal: str
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class Inventory:
    table: str
    dependent_views: tuple[str, ...]
    readers: tuple[PrincipalEvidence, ...]
    granters: tuple[PrincipalEvidence, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class FakeSource:
    """Small in-memory source used by unit tests and downstream callers."""
    grant_rows: list[Grant] = field(default_factory=list)
    owners: dict[tuple[str, str], str] = field(default_factory=dict)
    dependencies: dict[str, set[str]] = field(default_factory=dict)
    groups: dict[str, set[str]] = field(default_factory=dict)
    admins: set[str] = field(default_factory=set)

    def grants(self, securable_type: str, securable_name: str) -> Iterable[Grant]:
        wanted = (securable_type.upper(), securable_name.lower())
        return [g for g in self.grant_rows if (g.securable_type.upper(), g.securable_name.lower()) == wanted]

    def owner(self, securable_type: str, securable_name: str) -> str | None:
        return self.owners.get((securable_type.upper(), securable_name.lower()))

    def dependent_views(self, object_name: str) -> Iterable[str]:
        return self.dependencies.get(object_name.lower(), set())

    def group_members(self) -> Mapping[str, Iterable[str]]:
        return self.groups

    def metastore_admins(self) -> Iterable[str]:
        return self.admins


def _parents(table: str) -> tuple[tuple[str, str], ...]:
    parts = table.split(".")
    if len(parts) != 3 or any(not part for part in parts):
        raise ValueError(f"table must be catalog.schema.table, got {table!r}")
    catalog, schema, _ = parts
    return (("CATALOG", catalog), ("SCHEMA", f"{catalog}.{schema}"), ("TABLE", table))


def _expand(principal: str, groups: Mapping[str, Iterable[str]]) -> set[str]:
    """Include a granted principal and every transitively nested member."""
    expanded: set[str] = set()
    visiting: set[str] = set()

    def visit(item: str) -> None:
        if item in expanded:
            return
        expanded.add(item)
        if item in visiting:
            return
        visiting.add(item)
        for member in groups.get(item, ()):  # cycles are tolerated
            visit(str(member))
        visiting.remove(item)

    visit(principal)
    return expanded


def inventory_readers(
    source: InventorySource,
    table: str,
    genierails_grants: Iterable[Grant] = (),
) -> Inventory:
    """Compute readers not granted by GenieRails and report granters separately."""
    scopes = [(kind, name, True) for kind, name in _parents(table.lower())]
    views: set[str] = set()
    queue = [table.lower()]
    while queue:
        base = queue.pop(0)
        for view in source.dependent_views(base):
            normalized = str(view).lower()
            if normalized not in views and normalized != table.lower():
                views.add(normalized)
                queue.append(normalized)
    for view in sorted(views):
        scopes.extend((kind, name, False) for kind, name in _parents(view))

    managed = {grant.normalized() for grant in genierails_grants}
    groups = source.group_members()
    reader_reasons: dict[str, set[str]] = {}
    granter_reasons: dict[str, set[str]] = {}

    def add(target: dict[str, set[str]], principal: str, reason: str) -> None:
        for effective in _expand(principal, groups):
            target.setdefault(effective, set()).add(reason)

    seen_scopes: set[tuple[str, str]] = set()
    for securable_type, name, belongs_to_base in scopes:
        scope = (securable_type, name.lower())
        if scope in seen_scopes:
            continue
        seen_scopes.add(scope)
        for raw_grant in source.grants(*scope):
            grant = raw_grant.normalized()
            if grant.privilege in READ_PRIVILEGES and grant not in managed:
                add(reader_reasons, grant.principal, f"{grant.privilege} on {scope[0]} {scope[1]}")
            if grant.privilege in GRANT_PRIVILEGES:
                add(granter_reasons, grant.principal, f"MANAGE on {scope[0]} {scope[1]}")

        owner = source.owner(*scope)
        if owner:
            owner_reason = f"owner of {scope[0]} {scope[1]}"
            add(granter_reasons, owner, owner_reason)
            # The governed table and its parents confer read access. View owners
            # are deliberately risk-only: they can grant view access but do not
            # thereby receive SELECT on the governed base table.
            if belongs_to_base:
                add(reader_reasons, owner, owner_reason)

    for admin in source.metastore_admins():
        add(reader_reasons, str(admin), "metastore admin")
        add(granter_reasons, str(admin), "metastore admin")

    def evidence(values: Mapping[str, set[str]]) -> tuple[PrincipalEvidence, ...]:
        return tuple(
            PrincipalEvidence(principal, tuple(sorted(reasons)))
            for principal, reasons in sorted(values.items())
        )

    return Inventory(table.lower(), tuple(sorted(views)), evidence(reader_reasons), evidence(granter_reasons))


class DatabricksSource:
    """Databricks-backed source. SDK imports occur only when instantiated."""

    def __init__(self, warehouse_id: str, client: object | None = None, timeout_seconds: int = 120):
        if client is None:
            from databricks.sdk import WorkspaceClient  # lazy: pure callers need no SDK
            client = WorkspaceClient()
        self.client = client
        self.warehouse_id = warehouse_id
        self.timeout_seconds = timeout_seconds
        self._groups: dict[str, set[str]] | None = None

    @staticmethod
    def _literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _query(self, statement: str) -> list[list[str | None]]:
        execution = self.client.statement_execution
        result = execution.execute_statement(
            warehouse_id=self.warehouse_id, statement=statement, wait_timeout="50s"
        )
        deadline = time.monotonic() + self.timeout_seconds
        while str(getattr(result.status, "state", "")).split(".")[-1] not in {
            "SUCCEEDED", "FAILED", "CANCELED", "CLOSED"
        }:
            if time.monotonic() >= deadline:
                execution.cancel_execution(result.statement_id)
                raise TimeoutError("reader inventory SQL query timed out")
            time.sleep(1)
            result = execution.get_statement(result.statement_id)
        state = str(getattr(result.status, "state", "")).split(".")[-1]
        if state != "SUCCEEDED":
            raise RuntimeError(f"reader inventory SQL failed ({state}): {getattr(result.status, 'error', '')}")
        rows = list(getattr(result.result, "data_array", None) or [])
        chunks = int(getattr(getattr(result, "manifest", None), "total_chunk_count", 1) or 1)
        for index in range(1, chunks):
            chunk = execution.get_statement_result_chunk_n(result.statement_id, index)
            rows.extend(getattr(chunk, "data_array", None) or [])
        return rows

    def grants(self, securable_type: str, securable_name: str) -> Iterable[Grant]:
        catalog, *rest = securable_name.split(".")
        if securable_type == "CATALOG":
            view, predicate = "catalog_privileges", f"catalog_name = {self._literal(catalog)}"
        elif securable_type == "SCHEMA":
            view = "schema_privileges"
            predicate = f"catalog_name = {self._literal(catalog)} AND schema_name = {self._literal(rest[0])}"
        else:
            view = "table_privileges"
            predicate = (
                f"table_catalog = {self._literal(catalog)} AND table_schema = {self._literal(rest[0])} "
                f"AND table_name = {self._literal(rest[1])}"
            )
        rows = self._query(
            f"SELECT grantee, privilege_type FROM system.information_schema.{view} WHERE {predicate}"
        )
        return [Grant(securable_type, securable_name, str(row[0]), str(row[1])) for row in rows]

    def owner(self, securable_type: str, securable_name: str) -> str | None:
        parts = securable_name.split(".")
        if securable_type == "CATALOG":
            query = f"SELECT catalog_owner FROM system.information_schema.catalogs WHERE catalog_name={self._literal(parts[0])}"
        elif securable_type == "SCHEMA":
            query = (
                "SELECT schema_owner FROM system.information_schema.schemata WHERE "
                f"catalog_name={self._literal(parts[0])} AND schema_name={self._literal(parts[1])}"
            )
        else:
            query = (
                "SELECT table_owner FROM system.information_schema.tables WHERE "
                f"table_catalog={self._literal(parts[0])} AND table_schema={self._literal(parts[1])} "
                f"AND table_name={self._literal(parts[2])}"
            )
        rows = self._query(query)
        return str(rows[0][0]) if rows and rows[0][0] else None

    def dependent_views(self, object_name: str) -> Iterable[str]:
        catalog, schema, table = object_name.split(".")
        rows = self._query(
            "SELECT view_catalog, view_schema, view_name FROM system.information_schema.view_table_usage WHERE "
            f"table_catalog={self._literal(catalog)} AND table_schema={self._literal(schema)} "
            f"AND table_name={self._literal(table)}"
        )
        return [".".join(map(str, row[:3])) for row in rows]

    def group_members(self) -> Mapping[str, Iterable[str]]:
        if self._groups is None:
            self._groups = {}
            for group in self.client.groups.list(attributes="id,displayName,members"):
                name = str(group.display_name)
                members = set()
                for member in group.members or []:
                    members.add(str(getattr(member, "display", None) or member.value))
                self._groups[name] = members
        return self._groups

    def metastore_admins(self) -> Iterable[str]:
        # Account admins and workspace admins are Databricks' metastore-admin
        # principals. Expanding their membership makes the risk explicit.
        return ("account admins", "admins")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table", help="catalog.schema.table")
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--genierails-grants", type=Path, help="optional JSON list of Grant objects")
    args = parser.parse_args(argv)
    try:
        managed: Sequence[Grant] = ()
        if args.genierails_grants:
            managed = [Grant(**row) for row in json.loads(args.genierails_grants.read_text())]
        report = inventory_readers(DatabricksSource(args.warehouse_id), args.table, managed)
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        print(f"ERROR: reader inventory failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
