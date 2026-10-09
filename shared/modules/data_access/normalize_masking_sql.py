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


def _identifier_parts(tokens: list[str]) -> list[str] | None:
    """Return normalized dotted identifier parts, or None if ambiguous."""
    if not tokens or len(tokens) % 2 == 0:
        return None
    parts: list[str] = []
    for index, token in enumerate(tokens):
        if index % 2:
            if token != ".":
                return None
        elif token.startswith("`") and token.endswith("`"):
            parts.append(token[1:-1].replace("``", "`").lower())
        elif token.replace("_", "a").isalnum():
            parts.append(token.lower())
        else:
            return None
    return parts


def _function_name(statement: list[str]) -> list[str] | None:
    """Return the CREATE FUNCTION name parts, or None for another statement."""
    if statement[:1] != ["create"]:
        return None
    cursor = 1
    if statement[cursor:cursor + 2] == ["or", "replace"]:
        cursor += 2
    if statement[cursor:cursor + 1] != ["function"]:
        return None
    cursor += 1
    name: list[str] = []
    while cursor < len(statement) and statement[cursor] != "(":
        name.append(statement[cursor])
        cursor += 1
    if not name or cursor >= len(statement):
        return None
    return _identifier_parts(name)


def _use_context(statement: list[str]) -> tuple[str, list[str]] | None:
    """Return a USE CATALOG/SCHEMA context update when fully classified."""
    if len(statement) < 4 or statement[0] != "use" or statement[-1] != ";":
        return None
    kind = statement[1]
    if kind not in ("catalog", "schema"):
        return None
    parts = _identifier_parts(statement[2:-1])
    if parts is None or (kind == "catalog" and len(parts) != 1) or (kind == "schema" and len(parts) not in (1, 2)):
        return None
    return kind, parts


def normalized_tokens(sql_text: str) -> list[list[object]]:
    """Resolve and sort functions while retaining every unclassified token."""
    statements = _statements(sql_tokens(sql_text))
    catalog: str | None = None
    schema: str | None = None
    functions: list[tuple[tuple[str, str, str], list[str]]] = []
    normalized: list[list[object]] = [["format", 2]]

    def flush_functions() -> None:
        # Python's sort is stable: definitions with the same resolved target
        # keep file order, matching SQL's later-definition-wins behavior.
        functions.sort(key=lambda item: item[0])
        normalized.extend(["function", *target, statement] for target, statement in functions)
        functions.clear()

    for statement in statements:
        context = _use_context(statement)
        if context is not None:
            if context[0] == "catalog":
                catalog = context[1][0]
                schema = None
            elif len(context[1]) == 2:
                catalog, schema = context[1]
            else:
                schema = context[1][0]
            continue

        parts = _function_name(statement)
        if parts is None or len(parts) > 3:
            # Unknown executable SQL remains byte-significant (after comment
            # and whitespace tokenization), so classification failures always
            # move the hash instead of being silently ignored.
            # It is also a sort barrier: moving a function across executable
            # SQL we do not understand must move the hash.
            flush_functions()
            normalized.append(["unclassified", statement])
            continue

        if len(parts) == 3:
            target = tuple(parts)
        elif len(parts) == 2:
            target = (catalog or "", parts[0], parts[1])
        else:
            target = (catalog or "", schema or "", parts[0])
        functions.append((target, statement))

    flush_functions()
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
