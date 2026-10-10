#!/usr/bin/env python3
"""Refuse deterministic-governance changes that weaken effective protection."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable, Mapping

from ack_file import AckEntry, AckFileError, parse_file


@dataclass(frozen=True)
class Protection:
    level: str
    consequence: str


@dataclass(frozen=True)
class RowFilter:
    values: frozenset[str] | None

    @property
    def consequence(self) -> str:
        return "all rows" if self.values is None else "rows where value is in {" + ", ".join(sorted(self.values)) + "}"


@dataclass
class GovernanceState:
    columns: dict[str, dict[str, Protection]] = field(default_factory=dict)
    raw_exempt_principals: set[str] = field(default_factory=set)
    partial_versions: dict[str, str] = field(default_factory=dict)
    row_filters: dict[str, dict[str, RowFilter]] = field(default_factory=dict)
    table_readers: dict[str, set[str]] = field(default_factory=dict)


@dataclass(frozen=True, order=True)
class Weakening:
    ack_key: str
    category: str
    object_name: str
    principal: str
    before: str
    after: str

    @property
    def consequence(self) -> str:
        return f"{self.before} -> {self.after}"


@dataclass(frozen=True)
class CheckResult:
    all_weakenings: tuple[Weakening, ...]
    acknowledged: tuple[Weakening, ...]
    unacknowledged: tuple[Weakening, ...]
    unmatched_acknowledgements: tuple[AckEntry, ...]

    @property
    def ok(self) -> bool:
        return not self.unacknowledged and not self.unmatched_acknowledgements


def protection_rank(level: str) -> int:
    normalized = level.strip().lower()
    if normalized == "raw":
        return 0
    if normalized == "partial" or normalized.startswith("partial:"):
        return 1
    if normalized in {"redacted", "null"}:
        return 2
    raise ValueError(f"unknown protection level {level!r}; expected raw, partial[:version], redacted, or NULL")


def _table(column: str) -> str:
    parts = column.split(".")
    if len(parts) != 4:
        raise ValueError(f"column must be catalog.schema.table.column, got {column!r}")
    return ".".join(parts[:3])


def _issue(category: str, column: str, principal: str, before: str, after: str) -> Weakening:
    return Weakening(f"weaken:{column}:{principal}", category, column, principal, before, after)


def find_weakenings(before: GovernanceState, after: GovernanceState) -> tuple[Weakening, ...]:
    """Compare exact principal/column consequences and access declarations."""
    found: dict[tuple[str, str, str], Weakening] = {}

    def add(item: Weakening) -> None:
        found[(item.ack_key, item.category, item.consequence)] = item

    for column, prior_by_principal in before.columns.items():
        for principal, prior in prior_by_principal.items():
            current = after.columns.get(column, {}).get(principal)
            if current is not None and protection_rank(current.level) < protection_rank(prior.level):
                add(_issue("protection", column, principal, prior.consequence, current.consequence))

    for principal in after.raw_exempt_principals - before.raw_exempt_principals:
        for column, current_by_principal in after.columns.items():
            current = current_by_principal.get(principal)
            prior = before.columns.get(column, {}).get(principal)
            add(_issue(
                "raw_exempt_principal", column, principal,
                prior.consequence if prior else "not raw-exempt",
                current.consequence if current else "raw through raw_exempt_principals",
            ))

    for column, version in after.partial_versions.items():
        if version.strip().lower() != "raw" or before.partial_versions.get(column, "").strip().lower() == "raw":
            continue
        principals = set(before.columns.get(column, {})) | set(after.columns.get(column, {}))
        for principal in principals:
            previous = before.columns.get(column, {}).get(principal)
            current = after.columns.get(column, {}).get(principal)
            if (previous and previous.level.lower().startswith("partial")) or (
                current and current.level.lower().startswith("partial")
            ):
                add(_issue(
                    "partial_raw", column, principal,
                    previous.consequence if previous else "partial version",
                    "raw through partial='raw'",
                ))

    all_columns = set(before.columns) | set(after.columns)
    for table, prior_filters in before.row_filters.items():
        current_filters = after.row_filters.get(table, {})
        for principal, prior_filter in prior_filters.items():
            current = current_filters.get(principal)
            widened = current is None or current.values is None or (
                prior_filter.values is not None
                and current.values is not None
                and not current.values.issubset(prior_filter.values)
            )
            if not widened:
                continue
            for column in sorted(c for c in all_columns if _table(c) == table):
                add(_issue(
                    "row_filter", column, principal, prior_filter.consequence,
                    current.consequence if current else "all rows (filter removed)",
                ))

    for table, readers in after.table_readers.items():
        for principal in readers - before.table_readers.get(table, set()):
            item = Weakening(
                f"reader:{table}:{principal}", "table_reader", table, principal,
                "no GenieRails SELECT", "GenieRails grants SELECT",
            )
            add(item)
    return tuple(sorted(found.values()))


def check_weakenings(
    before: GovernanceState, after: GovernanceState, acknowledgements: Iterable[AckEntry]
) -> CheckResult:
    found = find_weakenings(before, after)
    entries = tuple(acknowledgements)
    by_key = {entry.key: entry for entry in entries}
    acknowledged = tuple(item for item in found if item.ack_key in by_key)
    unacknowledged = tuple(item for item in found if item.ack_key not in by_key)
    matched_keys = {item.ack_key for item in acknowledged}
    unmatched = tuple(entry for entry in entries if entry.key not in matched_keys)
    return CheckResult(found, acknowledged, unacknowledged, unmatched)


def _state(payload: Mapping[str, object]) -> GovernanceState:
    columns = {
        str(column): {
            str(principal): Protection(str(value["level"]), str(value["consequence"]))
            for principal, value in dict(principals).items()
        }
        for column, principals in dict(payload.get("columns", {})).items()
    }
    filters = {
        str(table): {
            str(principal): RowFilter(None if values is None else frozenset(map(str, values)))
            for principal, values in dict(principals).items()
        }
        for table, principals in dict(payload.get("row_filters", {})).items()
    }
    return GovernanceState(
        columns=columns,
        raw_exempt_principals=set(map(str, payload.get("raw_exempt_principals", []))),
        partial_versions={str(k): str(v) for k, v in dict(payload.get("partial_versions", {})).items()},
        row_filters=filters,
        table_readers={str(k): set(map(str, v)) for k, v in dict(payload.get("table_readers", {})).items()},
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True, help="before-state JSON")
    parser.add_argument("--after", type=Path, required=True, help="after-state JSON")
    parser.add_argument("--ack-file", type=Path, required=True)
    parser.add_argument("--json", action="store_true", help="emit the report as JSON")
    args = parser.parse_args(argv)
    try:
        before = _state(json.loads(args.before.read_text()))
        after = _state(json.loads(args.after.read_text()))
        result = check_weakenings(before, after, parse_file(args.ack_file))
        if args.json:
            print(json.dumps({
                "ok": result.ok,
                "acknowledged": [asdict(item) for item in result.acknowledged],
                "unacknowledged": [asdict(item) for item in result.unacknowledged],
                "unmatched_acknowledgements": [entry.key for entry in result.unmatched_acknowledgements],
            }, indent=2, sort_keys=True))
        else:
            for item in result.acknowledged:
                print(f"ACKNOWLEDGED {item.ack_key} [{item.category}]: {item.consequence}")
            for item in result.unacknowledged:
                print(f"ERROR: unacknowledged {item.ack_key} [{item.category}]: {item.consequence}", file=sys.stderr)
            for entry in result.unmatched_acknowledgements:
                print(f"ERROR: line {entry.line}: acknowledgement matches nothing: {entry.key}", file=sys.stderr)
            if result.ok:
                print("OK: no unacknowledged weakening")
        return 0 if result.ok else 1
    except (AckFileError, OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        print(f"ERROR: weakening check failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
