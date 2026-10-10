#!/usr/bin/env python3
"""Return a fail-closed, stable hash of the complete masking SQL file."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

SHARED_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SHARED_ROOT))

from masking_sql_blocks import analyze_sql_blocks, extract_function_name  # noqa: E402
from sql_tokenizer import sql_tokens  # noqa: E402


def normalized_tokens(sql_text: str) -> list[list[object]]:
    """Hash the exact blocks and execution contexts used by deployment."""
    # Tokenize the full file first so every lexer fail-closed rule still runs.
    sql_tokens(sql_text)
    parsed = analyze_sql_blocks(sql_text)

    records: list[list[object]] = []
    final_names: list[str] = []
    for catalog, schema, statement in parsed.blocks:
        name = extract_function_name(statement).split(".")[-1].strip("`").lower()
        final_names.append(name)
        records.append([
            "function",
            catalog.lower() if catalog else "",
            schema.lower() if schema else "",
            sql_tokens(statement),
        ])

    # Match #94's conservative collision rule: if any final function name is
    # shared, preserve the entire deployment order because qualification and
    # the execution context can make either definition the last winner.
    if "<unknown>" not in final_names and len(set(final_names)) == len(final_names):
        records = [record for _, record in sorted(zip(final_names, records), key=lambda item: item[0])]
    # Ambiguous input is still represented by position-sensitive ambiguity
    # records. Distinct function targets can be sorted independently because
    # their deployment order cannot affect which definition wins.
    ambiguity_records = [["fail_closed", sql_tokens(text)] for text in parsed.ambiguities]
    return [["format", 3], *ambiguity_records, *records]


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
