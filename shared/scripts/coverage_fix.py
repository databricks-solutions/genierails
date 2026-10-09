#!/usr/bin/env python3
"""Shared pieces of the coverage-gap fix: UDF/column types and the fix message.

A class.* tag no rule maps (a "coverage gap") is fixed one of two ways:

  a) reuse an existing treatment: make scaffold-treatments TREATMENT=<name>
     maps the class to it in shared/treatment_config.json;
  b) a new kind of mask: make scaffold-treatments adds a REVIEW stub, and in a
     promoted env make materialize-treatment carries its mask through dev.

This module finds each treatment's UDF input type and each column's data type
so (a) can refuse an incompatible pairing, and prints both options with exact
commands wherever release, maintain or the coverage check stop on a gap.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

SHARED_ROOT = Path(__file__).resolve().parents[1]
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

import hcl2  # noqa: E402

REGISTRY_PATH = SHARED_ROOT / "function_registry.json"

_CREATE_FUNCTION = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:[`\w]+\.)*`?(\w+)`?\s*\(",
    re.IGNORECASE,
)
_DDL_TABLE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([\w.`]+)\s*\((.*?)\)\s*;",
    re.IGNORECASE | re.DOTALL,
)
_COLUMN_OPTIONS = re.compile(
    r"\s+(?:COMMENT|NOT\s+NULL|DEFAULT|GENERATED|MASK|CONSTRAINT)\b.*$",
    re.IGNORECASE | re.DOTALL,
)
_TYPE_ALIASES = {
    "VARCHAR": "STRING", "CHAR": "STRING", "TEXT": "STRING",
    "INTEGER": "INT", "LONG": "BIGINT", "SHORT": "SMALLINT", "BYTE": "TINYINT",
    "REAL": "FLOAT", "NUMERIC": "DECIMAL", "DEC": "DECIMAL",
}


def _first_parameter_type(params: str) -> str | None:
    """Type of the first ``name TYPE`` parameter in ``params`` (text after '(')."""
    depth, end = 0, len(params)
    for index, char in enumerate(params):
        if char == "(":
            depth += 1
        elif char == ")":
            if depth == 0:
                end = index
                break
            depth -= 1
        elif char == "," and depth == 0:
            end = index
            break
    parts = params[:end].strip().split(None, 1)
    return parts[1].strip().upper() if len(parts) == 2 else None


def signature_input_type(signature: str) -> str | None:
    """``mask_x(input STRING) RETURNS STRING`` -> ``STRING``."""
    _, paren, rest = (signature or "").partition("(")
    return _first_parameter_type(rest) if paren else None


def sql_function_input_types(sql_text: str) -> dict[str, str]:
    """Function name (lower-case, unqualified) -> input type, from masking SQL."""
    types: dict[str, str] = {}
    for match in _CREATE_FUNCTION.finditer(sql_text or ""):
        input_type = _first_parameter_type(sql_text[match.end():])
        if input_type:
            types.setdefault(match.group(1).lower(), input_type)
    return types


def _registry_signatures() -> dict[str, str]:
    try:
        functions = json.loads(REGISTRY_PATH.read_text())["functions"]
    except (OSError, ValueError, KeyError):
        return {}
    signatures: dict[str, str] = {}
    for name, entry in functions.items():
        for alias in [name, *(entry.get("aliases") or [])]:
            signatures.setdefault(alias.lower(), entry.get("signature", ""))
    return signatures


def treatment_input_type(treatment, sql_text: str = "") -> str | None:
    """The UDF input type a treatment's mask takes, or None if unknown.

    The env's masking SQL (what is deployed) wins, then the treatment's own
    udf_signature, then shared/function_registry.json.
    """
    function = treatment.masking_function.lower()
    return (
        sql_function_input_types(sql_text).get(function)
        or signature_input_type(treatment.udf_signature)
        or signature_input_type(_registry_signatures().get(function, ""))
    )


def ddl_column_types(ddl_text: str) -> dict[str, str]:
    """``catalog.schema.table.column`` (lower-case) -> data type, from fetched DDL."""
    types: dict[str, str] = {}
    for match in _DDL_TABLE.finditer(ddl_text or ""):
        table = match.group(1).replace("`", "").lower()
        depth, current, columns = 0, "", []
        for char in match.group(2):
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            if char == "," and depth == 0:
                columns.append(current)
                current = ""
            else:
                current += char
        columns.append(current)
        for column in columns:
            lines = [ln for ln in column.strip().splitlines() if not ln.strip().startswith("--")]
            parts = " ".join(lines).strip().split(None, 1)
            if len(parts) != 2 or parts[0].upper() in {"CONSTRAINT", "PRIMARY", "FOREIGN"}:
                continue
            data_type = _COLUMN_OPTIONS.sub("", parts[1]).strip()
            name = parts[0].strip("`\"").lower()
            if data_type:
                types[f"{table}.{name}"] = data_type.upper()
    return types


def env_column_types(env_dir: Path) -> dict[str, str]:
    """Column types from every ``ddl/*.sql`` in an env (``_fetched.sql`` wins)."""
    ddl_dir = env_dir / "ddl"
    types: dict[str, str] = {}
    fetched = ddl_dir / "_fetched.sql"
    for path in sorted(ddl_dir.glob("*.sql")) + ([fetched] if fetched.is_file() else []):
        try:
            types.update(ddl_column_types(path.read_text()))
        except OSError:
            continue
    return types


def _normalized(data_type: str) -> tuple[str, str]:
    text = re.sub(r"\s+", "", data_type.upper())
    match = re.match(r"([A-Z_]+)(.*)$", text)
    if not match:
        return text, ""
    base = _TYPE_ALIASES.get(match.group(1), match.group(1))
    args = match.group(2)
    if base == "STRING":
        return base, ""
    if base == "DECIMAL":
        numbers = re.findall(r"\d+", args)
        precision = numbers[0] if numbers else "10"
        scale = numbers[1] if len(numbers) > 1 else "0"
        return base, f"({precision},{scale})"
    return base, args


def types_compatible(udf_input_type: str, column_type: str) -> bool:
    """Whether a mask taking ``udf_input_type`` binds to a ``column_type`` column.

    Strict on purpose: same type after aliasing (VARCHAR/CHAR are STRING),
    DECIMAL with the same precision and scale, and complex types verbatim.
    """
    return _normalized(udf_input_type) == _normalized(column_type)


def promote_source(env_dir: Path) -> str:
    """The env this one is promoted from (its promote_from), or ''."""
    path = env_dir / "env.auto.tfvars"
    try:
        value = hcl2.loads(path.read_text()).get("promote_from") if path.is_file() else ""
    except Exception:
        return ""
    if isinstance(value, list):
        value = value[0] if value else ""
    value = str(value or "").strip()
    return value if re.fullmatch(r"[a-z][a-z0-9_-]*", value) else ""


def rerun_command(env_name: str, promoted: bool) -> str:
    """The command that hit the gap: release/maintain export GENIERAILS_RERUN_TARGET."""
    target = os.environ.get("GENIERAILS_RERUN_TARGET", "").strip()
    if not re.fullmatch(r"[a-z][a-z-]*", target):
        target = "release" if promoted else "rehearse"
    return f"make {target} ENV={env_name}"


def _candidate_lines(config, gaps: list[tuple[str, str]], env_dir: Path, sql_text: str) -> list[str]:
    """One line per distinct gap column type, naming the treatments that fit it."""
    column_types = env_column_types(env_dir)
    by_type: dict[str, list[str]] = {}
    for column, _label in gaps:
        by_type.setdefault(column_types.get(column.lower(), ""), []).append(column)
    lines = []
    for column_type, columns in sorted(by_type.items()):
        if column_type:
            names = [
                t.value for t in config.treatments
                if (udf := treatment_input_type(t, sql_text)) and types_compatible(udf, column_type)
            ]
            what = f"{column_type} column(s) {', '.join(sorted(columns))}"
            lines.append(
                f"       Existing treatments for {what}: {', '.join(names) or 'none'}"
            )
        else:
            names = []
            for treatment in config.treatments:
                udf = treatment_input_type(treatment, sql_text)
                names.append(f"{treatment.value} ({udf or 'type unknown'})")
            lines.append(f"       Existing treatments (input type): {', '.join(names)}")
    return lines


def fix_lines(
    env_name: str,
    env_dir: Path,
    gaps: list[tuple[str, str]] | None = None,
    *,
    config=None,
) -> list[str]:
    """Both ways to fix a class.* coverage gap, with exact commands for this env.

    ``gaps`` is (column, class.<label>) pairs; with them the candidate
    treatments whose UDF fits each column's type are listed.
    """
    from treatment_derivation import load_treatment_config

    config = config or load_treatment_config()
    source = promote_source(env_dir)
    rerun = rerun_command(env_name, bool(source))
    sql_path = env_dir / "generated" / "masking_functions.sql"
    sql_text = sql_path.read_text() if sql_path.is_file() else ""
    lines = [
        "  How to fix (a rule change in shared/treatment_config.json, not a prod hand-edit):",
        "    a) Reuse an existing mask (the common case):",
        f"         make scaffold-treatments ENV={env_name} TREATMENT=<name>",
    ]
    if gaps:
        lines.extend(_candidate_lines(config, gaps, env_dir, sql_text))
    lines.append(f"       then commit shared/treatment_config.json and run {rerun}")
    lines.append("    b) A new kind of mask:")
    lines.append(f"         make scaffold-treatments ENV={env_name}   (adds a REVIEW full-redaction stub)")
    if source:
        lines.append(
            f"       review the stub, then: make materialize-treatment ENV={source} TREATMENT=<new>, "
            f"make rehearse ENV={source}, make promote-to ENV={env_name}, {rerun}"
        )
        lines.append(
            f"       commit shared/treatment_config.json, shared/tag_vocabulary_registry.json "
            f"and envs/{source}/generated/"
        )
    else:
        lines.append(f"       review the stub, then run {rerun}")
        lines.append(
            "       commit shared/treatment_config.json and shared/tag_vocabulary_registry.json"
        )
    return lines


def materialize_lines(env_name: str, env_dir: Path, treatments: list[str]) -> list[str]:
    """How to add a mapped treatment's missing mask policy, for this env."""
    source = promote_source(env_dir)
    rerun = rerun_command(env_name, bool(source))
    lines = ["  How to fix (add the mask; no column needs the tag):"]
    for treatment in treatments:
        lines.append(
            f"    make materialize-treatment ENV={source or env_name} TREATMENT={treatment}"
        )
    if source:
        lines.append(
            f"    then make rehearse ENV={source}, make promote-to ENV={env_name}, {rerun}; "
            f"commit envs/{source}/generated/"
        )
    else:
        lines.append(f"    then {rerun}; commit envs/{env_name}/generated/")
    return lines
