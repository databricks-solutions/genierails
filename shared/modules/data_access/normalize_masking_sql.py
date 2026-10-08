#!/usr/bin/env python3
"""Return a stable hash of executable masking-function definitions."""
from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path


def _tokens(sql: str) -> list[str]:
    """Tokenize SQL while discarding comments and insignificant whitespace."""
    token_re = re.compile(
        r"--[^\n]*|/\*.*?\*/|"
        r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|`(?:``|[^`])*`|"
        r"[A-Za-z_][A-Za-z0-9_$]*|\d+(?:\.\d+)?|<>|!=|<=|>=|=>|[-+*/%=<>()\[\]{},.;:]",
        re.S,
    )
    return [token for token in token_re.findall(sql) if not token.startswith(("--", "/*"))]


def _without_comments(sql: str) -> str:
    pattern = re.compile(
        r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|`(?:``|[^`])*`|--[^\n]*|/\*.*?\*/",
        re.S,
    )

    def replace(match: re.Match) -> str:
        token = match.group(0)
        if token.startswith(("--", "/*")):
            return "".join("\n" if char == "\n" else " " for char in token)
        return token

    return pattern.sub(replace, sql)


def _definitions(sql_text: str) -> list[tuple[str, str]]:
    """Extract CREATE FUNCTION statements with their effective USE namespace."""
    clean = _without_comments(sql_text)
    starts = list(re.finditer(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\b", clean, re.I))
    definitions = []
    for index, start in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(clean)
        statement = clean[start.start():end].strip().rstrip(";").strip()
        name_match = re.search(r"\bFUNCTION\s+([^\s(]+)\s*\(", statement, re.I)
        if not name_match:
            continue
        raw_name = name_match.group(1).strip("`")
        prefix = clean[:start.start()]
        catalogs = re.findall(r"\bUSE\s+CATALOG\s+([^\s;]+)", prefix, re.I)
        schemas = re.findall(r"\bUSE\s+SCHEMA\s+([^\s;]+)", prefix, re.I)
        parts = [part.strip("`") for part in raw_name.split(".")]
        if len(parts) >= 3:
            identity = ".".join(parts[-3:]).lower()
        else:
            identity = ".".join(
                part for part in (
                    catalogs[-1].strip("`") if catalogs else "",
                    schemas[-1].strip("`") if schemas else "",
                    parts[-1],
                ) if part
            ).lower()
        definitions.append((identity, statement))
    return definitions


def normalized_definitions(sql_text: str) -> str:
    definitions = [(name, " ".join(_tokens(statement))) for name, statement in _definitions(sql_text)]
    definitions.sort(key=lambda item: item[0])
    return "\n".join(f"{name}\0{body}" for name, body in definitions)


def main() -> int:
    query = json.load(sys.stdin)
    normalized = normalized_definitions(Path(query["sql_file"]).read_text())
    print(json.dumps({"hash": hashlib.sha256(normalized.encode()).hexdigest()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
