"""The single source of truth for masking SQL deployment blocks."""
from __future__ import annotations

import re
from collections import Counter
from typing import NamedTuple

from sql_tokenizer import sql_tokens


class ParsedSqlBlocks(NamedTuple):
    blocks: list[tuple[str | None, str | None, str]]
    unambiguous: bool
    ambiguities: list[str]


def extract_function_name(stmt: str) -> str:
    """Extract a function name exactly as the deployer has always done."""
    match = re.search(r"FUNCTION\s+(\S+)\s*\(", stmt, re.IGNORECASE)
    return match.group(1) if match else "<unknown>"


def _normalized_identifier(token: str) -> str:
    return token.strip("`").lower()


def _function_definitions(
    sql_text: str,
    catalog: str | None = None,
    schema: str | None = None,
) -> list[tuple[str | None, str | None, str]]:
    """Find CREATE FUNCTION targets without matching comments or literals."""
    tokens = sql_tokens(sql_text)
    definitions: list[tuple[str | None, str | None, str]] = []
    i = 0
    while i < len(tokens):
        if tokens[i:i + 2] == ["use", "catalog"] and i + 2 < len(tokens):
            catalog = _normalized_identifier(tokens[i + 2]).rstrip(";")
        elif tokens[i:i + 2] == ["use", "schema"] and i + 2 < len(tokens):
            schema = _normalized_identifier(tokens[i + 2]).rstrip(";")
        elif tokens[i] == "create":
            j = i + 1
            if tokens[j:j + 2] == ["or", "replace"]:
                j += 2
            if j < len(tokens) and tokens[j] == "function":
                j += 1
                name_parts: list[str] = []
                while j < len(tokens) and tokens[j] != "(":
                    if tokens[j] != ".":
                        name_parts.append(tokens[j])
                    j += 1
                if j < len(tokens) and name_parts:
                    definitions.append((catalog, schema, _normalized_identifier(name_parts[-1])))
        i += 1
    return definitions


def analyze_sql_blocks(sql_text: str) -> ParsedSqlBlocks:
    """Return the deployer's exact blocks plus whether they map unambiguously.

    Deployment intentionally retains its established splitter and context
    behavior.  The extra flag lets hashing fail closed whenever that behavior
    skipped or only heuristically recovered part of the input.
    """
    catalog, schema = None, None
    blocks: list[tuple[str | None, str | None, str]] = []
    unambiguous = True
    ambiguities: list[str] = []

    for raw_stmt in re.split(r";\s*(?:--[^\n]*)?\n", sql_text):
        lines = [
            line for line in raw_stmt.split("\n")
            if line.strip() and not line.strip().startswith("--")
        ]
        stmt = "\n".join(lines).strip()
        if not stmt:
            continue

        match = re.match(r"USE\s+CATALOG\s+(\S+)", stmt, re.IGNORECASE)
        if match:
            catalog = match.group(1).rstrip(";")
            if stmt[match.end():].strip():
                unambiguous = False
                ambiguities.append(stmt)
            continue

        match = re.match(r"USE\s+SCHEMA\s+(\S+)", stmt, re.IGNORECASE)
        if match:
            schema = match.group(1).rstrip(";")
            if "." in schema or stmt[match.end():].strip():
                unambiguous = False
                ambiguities.append(stmt)
            continue

        if stmt.upper().startswith("CREATE"):
            blocks.append((catalog, schema, stmt))
            if extract_function_name(stmt) == "<unknown>":
                unambiguous = False
                ambiguities.append(stmt)
        else:
            # This includes bare/unrecognized USE and block-comment-prefixed
            # USE. The deployer skips it, so normalization must retain order.
            # A comment-prefixed CREATE FUNCTION can still be recovered below;
            # defer that decision until the fallback is checked.
            create_match = re.search(
                r"\bCREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\b", stmt, re.IGNORECASE
            )
            comment_prefixed_create = stmt.startswith("/*") and create_match is not None
            if not comment_prefixed_create:
                unambiguous = False
                ambiguities.append(stmt[:create_match.start()] if create_match else stmt)

    primary_names = {
        extract_function_name(stmt).split(".")[-1].lower()
        for _, _, stmt in blocks
    }
    definitions = _function_definitions(sql_text)
    all_fn_names: set[str] = set()
    missing: list[str] = []
    for _, _, function_name in definitions:
        if function_name not in all_fn_names:
            all_fn_names.add(function_name)
            if function_name not in primary_names:
                missing.append(function_name)
    if missing:
        # Preserve the deployer's established fallback exactly. It is only
        # unambiguous when the whole file has one possible execution context;
        # this retains #94's block-comment/formatting contract without guessing
        # across multiple USE regions.
        catalogs = re.findall(r"USE\s+CATALOG\s+(\S+)", sql_text, re.IGNORECASE)
        schemas = re.findall(r"USE\s+SCHEMA\s+(\S+)", sql_text, re.IGNORECASE)
        if len({value.rstrip(";") for value in catalogs}) > 1 or len(
            {value.rstrip(";") for value in schemas}
        ) > 1:
            unambiguous = False
            ambiguities.append(sql_text)
        cat_match = re.search(r"USE\s+CATALOG\s+(\S+)", sql_text, re.IGNORECASE)
        schema_match = re.search(r"USE\s+SCHEMA\s+(\S+)", sql_text, re.IGNORECASE)
        fallback_catalog = cat_match.group(1).rstrip(";") if cat_match else catalog
        fallback_schema = schema_match.group(1).rstrip(";") if schema_match else schema
        recovered: set[str] = set()
        for function_name in missing:
            pattern = re.compile(
                r"(CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:\S+\.)*"
                + re.escape(function_name)
                + r"\s*\(.*?(?:;|\Z))",
                re.IGNORECASE | re.DOTALL,
            )
            match = pattern.search(sql_text)
            if match:
                statement = match.group(1).rstrip(";").strip()
                blocks.append((fallback_catalog, fallback_schema, statement))
                recovered.add(function_name)
        if recovered != set(missing):
            unambiguous = False
            ambiguities.append(sql_text)

    source_counts = Counter(name for _, _, name in definitions)
    parsed_by_context: Counter[tuple[str | None, str | None, str]] = Counter()
    for block_catalog, block_schema, statement in blocks:
        parsed_by_context.update(_function_definitions(statement, block_catalog, block_schema))
    parsed_counts = Counter()
    for (_, _, name), count in parsed_by_context.items():
        parsed_counts[name] += count
    unrepresented = source_counts - parsed_counts
    if unrepresented:
        function_name = next(iter(unrepresented))
        raise ValueError(
            "Ambiguous masking SQL: function "
            f"'{function_name}' has a definition that deployment would not execute; "
            "put each definition on its own semicolon-terminated line or remove the duplicate"
        )

    return ParsedSqlBlocks(blocks, unambiguous, ambiguities)


def parse_sql_blocks(sql_text: str) -> list[tuple[str | None, str | None, str]]:
    """Parse the exact catalog, schema and statements the deployer executes."""
    return analyze_sql_blocks(sql_text).blocks
