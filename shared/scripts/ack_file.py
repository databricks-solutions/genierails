#!/usr/bin/env python3
"""Parse and reconcile committed deterministic-governance acknowledgements."""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping


_IDENT = r"[A-Za-z0-9_][A-Za-z0-9_$-]*"
_OBJECT = re.compile(rf"^{_IDENT}(?:\.{_IDENT})+$")
_PRINCIPAL = re.compile(r"^[^\s:#,]+(?: [^\s:#,]+)*$")
_COMMIT = re.compile(r"^[0-9a-fA-F]{7,64}$")
_PARTS = {"weaken": 4, "unclassified": 4, "reader": 3, "revoke": 3}


class AckFileError(ValueError):
    """The acknowledgement file is malformed or cannot be reconciled."""


@dataclass(frozen=True, order=True)
class AckEntry:
    kind: str
    object_name: str
    principal: str | None = None
    line: int = 0

    @property
    def key(self) -> str:
        if self.kind == "rollback":
            return f"rollback:{self.object_name}"
        suffix = f":{self.principal}" if self.principal is not None else ""
        return f"{self.kind}:{self.object_name}{suffix}"


def parse_line(text: str, line_number: int = 1) -> AckEntry | None:
    """Parse one line. Empty lines and full-line comments are ignored."""
    value = text.strip()
    if not value or value.startswith("#"):
        return None
    if "#" in value:
        raise AckFileError(f"line {line_number}: inline comments are not allowed")
    parts = value.split(":")
    kind = parts[0]
    if kind == "rollback":
        if len(parts) != 2 or not _COMMIT.fullmatch(parts[1]):
            raise AckFileError(f"line {line_number}: rollback must be rollback:<7-64 hex commit>")
        return AckEntry(kind, parts[1].lower(), line=line_number)
    if kind not in _PARTS:
        raise AckFileError(f"line {line_number}: unknown acknowledgement kind {kind!r}")
    expected_object_parts = _PARTS[kind]
    expected_fields = 2 if kind == "unclassified" else 3
    if len(parts) != expected_fields:
        shape = f"{kind}:<catalog.schema.table"
        shape += ".column>" if expected_object_parts == 4 else ">:<principal>"
        if kind == "weaken":
            shape += ":<principal>"
        raise AckFileError(f"line {line_number}: expected {shape}")
    object_name = parts[1]
    if len(object_name.split(".")) != expected_object_parts or not _OBJECT.fullmatch(object_name):
        raise AckFileError(
            f"line {line_number}: {kind} object must have {expected_object_parts} valid identifier parts"
        )
    principal = None if kind == "unclassified" else parts[2]
    if principal is not None and not _PRINCIPAL.fullmatch(principal):
        raise AckFileError(f"line {line_number}: invalid principal {principal!r}")
    return AckEntry(kind, object_name, principal, line_number)


def parse_text(text: str) -> tuple[AckEntry, ...]:
    entries: list[AckEntry] = []
    seen: dict[str, int] = {}
    for number, line in enumerate(text.splitlines(), 1):
        entry = parse_line(line, number)
        if entry is None:
            continue
        if entry.key in seen:
            raise AckFileError(
                f"line {number}: duplicate acknowledgement {entry.key!r} "
                f"(first declared on line {seen[entry.key]})"
            )
        seen[entry.key] = number
        entries.append(entry)
    return tuple(entries)


def parse_file(path: Path | str) -> tuple[AckEntry, ...]:
    ack_path = Path(path)
    try:
        return parse_text(ack_path.read_text())
    except OSError as exc:
        raise AckFileError(f"cannot read acknowledgement file {ack_path}: {exc}") from exc


def reconcile(
    entries: Iterable[AckEntry], consequences: Mapping[str, str]
) -> tuple[dict[str, str], tuple[AckEntry, ...]]:
    """Return acknowledged consequences and acknowledgements matching nothing."""
    consequence_keys = set(consequences)
    matched = {entry.key: consequences[entry.key] for entry in entries if entry.key in consequence_keys}
    unmatched = tuple(entry for entry in entries if entry.key not in consequence_keys)
    return matched, unmatched


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ack_file", type=Path)
    parser.add_argument(
        "--consequences", type=Path,
        help="JSON object mapping exact acknowledgement keys to before/after consequences",
    )
    args = parser.parse_args(argv)
    try:
        entries = parse_file(args.ack_file)
        if args.consequences is None:
            for entry in entries:
                print(entry.key)
            print(f"OK: {len(entries)} acknowledgement(s) are syntactically valid")
            return 0
        payload = json.loads(args.consequences.read_text())
        if not isinstance(payload, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in payload.items()
        ):
            raise AckFileError("consequences must be a JSON object of string keys and values")
        matched, unmatched = reconcile(entries, payload)
        for key, consequence in matched.items():
            print(f"ACKNOWLEDGED {key}: {consequence}")
        if unmatched:
            for entry in unmatched:
                print(f"ERROR: line {entry.line}: acknowledgement matches nothing: {entry.key}", file=sys.stderr)
            return 1
        print(f"OK: all {len(entries)} acknowledgement(s) match")
        return 0
    except (AckFileError, OSError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
