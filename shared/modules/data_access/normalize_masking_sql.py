#!/usr/bin/env python3
"""Return a fail-closed, stable hash of the complete masking SQL file."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

SHARED_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SHARED_ROOT))

from sql_tokenizer import sql_tokens  # noqa: E402


def _statements(tokens: list[str]) -> list[list[str]]:
    statements: list[list[str]] = []
    current: list[str] = []
    for token in tokens:
        current.append(token)
        if token == ";":
            statements.append(current)
            current = []
    if current:
        statements.append(current)
    return statements


def _function_name(statement: list[str]) -> str | None:
    """Return a CREATE FUNCTION sort key, or None for every other statement."""
    try:
        create = statement.index("create")
    except ValueError:
        return None
    cursor = create + 1
    if statement[cursor:cursor + 2] == ["or", "replace"]:
        cursor += 2
    if statement[cursor:cursor + 1] != ["function"]:
        return None
    cursor += 1
    name: list[str] = []
    while cursor < len(statement) and statement[cursor] != "(":
        name.append(statement[cursor])
        cursor += 1
    return "".join(name) if name and cursor < len(statement) else None


def normalized_tokens(sql_text: str) -> list[list[str]]:
    """Normalize only consecutive function order; retain every statement/token."""
    statements = _statements(sql_tokens(sql_text))
    normalized: list[list[str]] = []
    run: list[tuple[str, list[str]]] = []

    def flush() -> None:
        normalized.extend(statement for _name, statement in sorted(run, key=lambda item: item[0]))
        run.clear()

    for statement in statements:
        name = _function_name(statement)
        if name is None:
            flush()
            normalized.append(statement)
        else:
            run.append((name, statement))
    flush()
    return normalized


def normalized_definitions(sql_text: str) -> str:
    # JSON provides an unambiguous token boundary encoding. SqlTokenizeError is
    # intentionally uncaught: Terraform then fails closed instead of accepting
    # an unsafe "unchanged" hash.
    return json.dumps(normalized_tokens(sql_text), ensure_ascii=False, separators=(",", ":"))


def main() -> int:
    query = json.load(sys.stdin)
    normalized = normalized_definitions(Path(query["sql_file"]).read_text())
    print(json.dumps({"hash": hashlib.sha256(normalized.encode()).hexdigest()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
