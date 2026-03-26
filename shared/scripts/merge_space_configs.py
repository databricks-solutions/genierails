#!/usr/bin/env python3
"""Merge one per-space generated config into the assembled generated/ outputs.

Usage:
  python scripts/merge_space_configs.py <generated_dir> <space_key>

Where:
  <generated_dir>  Path to the env's generated/ directory (e.g. envs/dev/generated)
  <space_key>      Sanitized space key matching the subdirectory name
                   (e.g. "finance_analytics" for generated/spaces/finance_analytics/)

The script patches (not replaces) the assembled outputs:
  - generated/abac.auto.tfvars:
      * genie_space_configs: replaces/adds the entry for <space_key>
      * tag_policies: adds new keys from the per-space config (dedup by key;
        existing keys have their values union-merged so the account layer can
        create any tag_key introduced by the new space)
      * tag_assignments: appends new entries (dedup by entity_name + tag_key)
      * fgac_policies: appends new entries (dedup by policy name)
  - generated/masking_functions.sql:
      * appends new CREATE FUNCTION blocks (dedup by function name)

Groups and group_members are NEVER touched — they are shared governance state
established by full generation.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

try:
    import hcl2
except ImportError:
    print("ERROR: python-hcl2 is required. Install with:")
    print("  pip install python-hcl2")
    sys.exit(2)


# ---------------------------------------------------------------------------
# HCL rendering helpers (mirrors generate_abac.py utilities)
# ---------------------------------------------------------------------------

IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_key(key: str) -> str:
    if IDENT_RE.match(key):
        return key
    return json.dumps(key)


def _render_scalar(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value)


def _render_value(value, indent: int = 0) -> str:
    pad = " " * indent
    if isinstance(value, dict):
        if not value:
            return "{}"
        lines = ["{"]
        for key, item in value.items():
            rendered = _render_value(item, indent + 2)
            lines.append(f"{pad}  {_quote_key(key)} = {rendered}")
        lines.append(f"{pad}}}")
        return "\n".join(lines)
    if isinstance(value, list):
        if not value:
            return "[]"
        if all(not isinstance(item, (dict, list)) for item in value):
            return "[" + ", ".join(_render_scalar(item) for item in value) + "]"
        lines = ["["]
        for item in value:
            rendered = _render_value(item, indent + 2)
            lines.append(f"{pad}  {rendered},")
        lines.append(f"{pad}]")
        return "\n".join(lines)
    return _render_scalar(value)


def _hcl_str(s: str) -> str:
    escaped = s.replace("\\", "\\\\").replace('"', '\\"').replace("${", "$${")
    return f'"{escaped}"'


# ---------------------------------------------------------------------------
# genie_space_configs HCL formatter (mirrors generate_abac.py)
# ---------------------------------------------------------------------------

def format_genie_space_configs_hcl(configs: dict[str, dict]) -> str:
    """Render the full genie_space_configs = { ... } HCL block."""
    lines = ["genie_space_configs = {"]

    for space_name, cfg in configs.items():
        lines.append(f"  {_hcl_str(space_name)} = {{")

        for simple_key in ("title", "description", "instructions"):
            if cfg.get(simple_key):
                lines.append(f"    {simple_key} = {_hcl_str(cfg[simple_key])}")

        if cfg.get("sample_questions"):
            lines.append("    sample_questions = [")
            for q in cfg["sample_questions"]:
                lines.append(f"      {_hcl_str(q)},")
            lines.append("    ]")

        if cfg.get("benchmarks"):
            lines.append("    benchmarks = [")
            for bm in cfg["benchmarks"]:
                lines.append("      {")
                lines.append(f"        question = {_hcl_str(bm['question'])}")
                lines.append(f"        sql      = {_hcl_str(bm['sql'])}")
                lines.append("      },")
            lines.append("    ]")

        if cfg.get("sql_filters"):
            lines.append("    sql_filters = [")
            for f in cfg["sql_filters"]:
                lines.append("      {")
                lines.append(f"        sql          = {_hcl_str(f['sql'])}")
                lines.append(f"        display_name = {_hcl_str(f.get('display_name', ''))}")
                lines.append("      },")
            lines.append("    ]")

        if cfg.get("sql_expressions"):
            lines.append("    sql_expressions = [")
            for e in cfg["sql_expressions"]:
                lines.append("      {")
                lines.append(f"        alias = {_hcl_str(e['alias'])}")
                lines.append(f"        sql   = {_hcl_str(e['sql'])}")
                lines.append("      },")
            lines.append("    ]")

        if cfg.get("sql_measures"):
            lines.append("    sql_measures = [")
            for m in cfg["sql_measures"]:
                lines.append("      {")
                lines.append(f"        alias = {_hcl_str(m['alias'])}")
                lines.append(f"        sql   = {_hcl_str(m['sql'])}")
                lines.append("      },")
            lines.append("    ]")

        if cfg.get("join_specs"):
            lines.append("    join_specs = [")
            for j in cfg["join_specs"]:
                lines.append("      {")
                lines.append(f"        left_table  = {_hcl_str(j['left_table'])}")
                lines.append(f"        right_table = {_hcl_str(j['right_table'])}")
                lines.append(f"        sql         = {_hcl_str(j['sql'])}")
                lines.append("      },")
            lines.append("    ]")

        lines.append("  }")

    lines.append("}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# HCL block removal (mirrors generate_abac.py)
# ---------------------------------------------------------------------------

def remove_hcl_top_level_block(text: str, key: str) -> str:
    """Remove a top-level HCL assignment block 'key = { ... }' from text."""
    pattern = re.compile(rf"^{re.escape(key)}\s*=\s*\{{", re.MULTILINE)
    m = pattern.search(text)
    if not m:
        return text

    depth = 0
    end = m.end() - 1

    for i in range(m.end() - 1, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                end = i
                break

    block_end = end + 1
    if block_end < len(text) and text[block_end] == "\n":
        block_end += 1

    return text[:m.start()] + text[block_end:]


def remove_hcl_top_level_list(text: str, key: str) -> str:
    """Remove a top-level HCL assignment block 'key = [ ... ]' from text."""
    pattern = re.compile(rf"^{re.escape(key)}\s*=\s*\[", re.MULTILINE)
    m = pattern.search(text)
    if not m:
        return text

    depth = 0
    end = m.end() - 1

    for i in range(m.end() - 1, len(text)):
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
            if depth == 0:
                end = i
                break

    block_end = end + 1
    if block_end < len(text) and text[block_end] == "\n":
        block_end += 1

    return text[:m.start()] + text[block_end:]


# ---------------------------------------------------------------------------
# Masking SQL helpers
# ---------------------------------------------------------------------------

_FUNC_NAME_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE\s+)?FUNCTION\s+(?:\w+\.)*(\w+)\s*\(",
    re.IGNORECASE,
)


def extract_function_names(sql_text: str) -> set[str]:
    """Return the set of SQL function names defined in sql_text."""
    return {m.group(1).lower() for m in _FUNC_NAME_RE.finditer(sql_text)}


def split_into_function_blocks(sql_text: str) -> list[str]:
    """Split a SQL file into individual CREATE FUNCTION blocks, each with its
    USE CATALOG / USE SCHEMA context prepended.

    The deploy_masking_functions.py script uses USE CATALOG/SCHEMA directives
    to determine the execution context for each CREATE statement.  Without the
    context header, appended functions would be deployed under the last catalog
    active in the assembled file (usually dev_fin), not their own catalog.
    """
    # Track current catalog/schema context as we scan
    catalog: str = ""
    schema: str = ""
    blocks: list[str] = []

    # Split on CREATE boundaries (positive lookahead keeps the keyword)
    parts = re.split(
        r"(?=CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE\s+)?FUNCTION\b)",
        sql_text,
        flags=re.IGNORECASE,
    )
    for part in parts:
        stripped = part.strip()
        if not stripped:
            continue

        # Update current context from USE directives in this segment.
        # Strip any trailing semicolon so the catalog/schema name is clean.
        for m in re.finditer(r"USE\s+CATALOG\s+(\S+)", stripped, re.IGNORECASE):
            catalog = m.group(1).rstrip(";")
        for m in re.finditer(r"USE\s+SCHEMA\s+(\S+)", stripped, re.IGNORECASE):
            schema = m.group(1).rstrip(";")

        # Only keep segments that contain a CREATE FUNCTION statement
        if not re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE\s+)?FUNCTION\b",
                         stripped, re.IGNORECASE):
            continue

        # Prepend the context so the function is deployed to the right catalog/schema
        ctx_lines: list[str] = []
        if catalog:
            ctx_lines.append(f"USE CATALOG {catalog};")
        if schema:
            ctx_lines.append(f"USE SCHEMA {schema};")
        header = "\n".join(ctx_lines)

        # Strip any trailing USE directives from the function body (they're
        # already captured above and will be prepended as the context header)
        body = re.sub(r"^\s*USE\s+(?:CATALOG|SCHEMA)\s+\S+\s*;\s*", "",
                      stripped, flags=re.IGNORECASE | re.MULTILINE)

        if header:
            blocks.append(f"{header}\n\n{body.strip()}")
        else:
            blocks.append(body.strip())

    return blocks


# ---------------------------------------------------------------------------
# Main merge logic
# ---------------------------------------------------------------------------

def load_hcl_safe(path: Path) -> dict:
    """Load an HCL file, returning empty dict on missing or parse error."""
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            return hcl2.load(f)
    except Exception as e:
        print(f"  WARNING: Could not parse {path}: {e}")
        return {}


def merge_into_assembled(generated_dir: Path, space_key: str) -> None:
    """Patch the assembled generated/abac.auto.tfvars and masking_functions.sql
    with content from generated/spaces/<space_key>/.
    """
    space_dir = generated_dir / "spaces" / space_key
    space_abac = space_dir / "abac.auto.tfvars"
    space_sql = space_dir / "masking_functions.sql"
    assembled_abac = generated_dir / "abac.auto.tfvars"
    assembled_sql = generated_dir / "masking_functions.sql"

    if not space_abac.exists():
        print(f"  ERROR: Per-space config not found: {space_abac}")
        sys.exit(1)

    print(f"\n  Merging generated/spaces/{space_key}/ into generated/...")

    # ── Load per-space content ────────────────────────────────────────────
    space_cfg = load_hcl_safe(space_abac)

    new_genie_cfgs: dict = space_cfg.get("genie_space_configs") or {}
    new_tag_assignments: list = space_cfg.get("tag_assignments") or []
    new_fgac_policies: list = space_cfg.get("fgac_policies") or []
    new_tag_policies: list = space_cfg.get("tag_policies") or []

    # ── Load assembled content ────────────────────────────────────────────
    assembled_cfg = load_hcl_safe(assembled_abac)
    assembled_text = assembled_abac.read_text() if assembled_abac.exists() else ""

    existing_genie_cfgs: dict = assembled_cfg.get("genie_space_configs") or {}
    existing_tag_assignments: list = assembled_cfg.get("tag_assignments") or []
    existing_fgac_policies: list = assembled_cfg.get("fgac_policies") or []
    existing_tag_policies: list = assembled_cfg.get("tag_policies") or []

    # ── Merge tag_policies (dedup by key — add any new keys from per-space) ─
    # Per-space generation may introduce tag_keys not present in the assembled
    # config (e.g. phi_level for a Clinical space).  Those keys must be added
    # to the assembled tag_policies so that validation passes and the account
    # layer creates the corresponding Databricks tag policies.
    existing_tp_keys = {tp.get("key", "") for tp in existing_tag_policies}
    added_tp = 0
    merged_tag_policies = list(existing_tag_policies)
    for tp in new_tag_policies:
        key = tp.get("key", "")
        if key and key not in existing_tp_keys:
            merged_tag_policies.append(tp)
            existing_tp_keys.add(key)
            added_tp += 1
        elif key in existing_tp_keys:
            # Merge values for existing keys (union of values)
            for i, etp in enumerate(merged_tag_policies):
                if etp.get("key") == key:
                    existing_vals = set(etp.get("values") or [])
                    new_vals = set(tp.get("values") or [])
                    combined = sorted(existing_vals | new_vals)
                    if combined != sorted(existing_vals):
                        merged_tag_policies[i] = dict(etp, values=combined)
                        added_tp += 1
                    break
    if added_tp:
        print(f"    tag_policies:     added/updated {added_tp} key(s) from per-space config")

    # ── Merge genie_space_configs ─────────────────────────────────────────
    merged_genie = dict(existing_genie_cfgs)
    for space_name, cfg in new_genie_cfgs.items():
        merged_genie[space_name] = cfg
        print(f"    genie_space_configs: updated entry '{space_name}'")

    # ── Merge tag_assignments (dedup by entity_name + tag_key) ───────────
    existing_ta_keys = {
        (ta.get("entity_name", ""), ta.get("tag_key", ""))
        for ta in existing_tag_assignments
    }
    added_ta = 0
    merged_tag_assignments = list(existing_tag_assignments)
    for ta in new_tag_assignments:
        key = (ta.get("entity_name", ""), ta.get("tag_key", ""))
        if key not in existing_ta_keys:
            merged_tag_assignments.append(ta)
            existing_ta_keys.add(key)
            added_ta += 1
    if added_ta:
        print(f"    tag_assignments: added {added_ta} new entry/entries")

    # ── Merge fgac_policies (dedup by name) ───────────────────────────────
    existing_pol_names = {p.get("name", "") for p in existing_fgac_policies}
    added_pol = 0
    merged_fgac = list(existing_fgac_policies)
    for pol in new_fgac_policies:
        name = pol.get("name", "")
        if name not in existing_pol_names:
            merged_fgac.append(pol)
            existing_pol_names.add(name)
            added_pol += 1
    if added_pol:
        print(f"    fgac_policies:    added {added_pol} new entry/entries")

    # ── Rewrite assembled abac.auto.tfvars ────────────────────────────────
    # Remove the sections we are replacing, then append the new ones.
    updated = assembled_text

    # Replace tag_policies block (if we added/updated any keys)
    if added_tp:
        updated = remove_hcl_top_level_list(updated, "tag_policies")
        if merged_tag_policies:
            tp_hcl = "tag_policies = " + _render_value(merged_tag_policies)
            updated = updated.rstrip() + "\n\n" + tp_hcl + "\n"

    # Replace genie_space_configs block
    updated = remove_hcl_top_level_block(updated, "genie_space_configs")
    if merged_genie:
        updated = updated.rstrip() + "\n\n" + format_genie_space_configs_hcl(merged_genie) + "\n"

    # Replace tag_assignments block
    updated = remove_hcl_top_level_list(updated, "tag_assignments")
    if merged_tag_assignments:
        ta_hcl = "tag_assignments = " + _render_value(merged_tag_assignments)
        updated = updated.rstrip() + "\n\n" + ta_hcl + "\n"

    # Replace fgac_policies block
    updated = remove_hcl_top_level_list(updated, "fgac_policies")
    if merged_fgac:
        fgac_hcl = "fgac_policies = " + _render_value(merged_fgac)
        updated = updated.rstrip() + "\n\n" + fgac_hcl + "\n"

    assembled_abac.write_text(updated)
    print(f"    Written: {assembled_abac}")

    # ── Merge masking_functions.sql (dedup by function name) ─────────────
    if space_sql.exists():
        new_sql = space_sql.read_text()
        new_func_blocks = split_into_function_blocks(new_sql)

        existing_sql = assembled_sql.read_text() if assembled_sql.exists() else ""
        existing_func_names = extract_function_names(existing_sql)

        appended = 0
        for block in new_func_blocks:
            names = extract_function_names(block)
            if names and not names.issubset(existing_func_names):
                existing_sql = existing_sql.rstrip() + "\n\n" + block + "\n"
                existing_func_names |= names
                appended += 1

        if appended:
            assembled_sql.write_text(existing_sql)
            print(f"    masking_functions.sql: appended {appended} new function(s) → {assembled_sql}")
        else:
            print("    masking_functions.sql: no new functions (all already present)")
    else:
        print("    masking_functions.sql: none in space dir (skipping)")

    print(f"\n  Merge complete for space '{space_key}'.")


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: python scripts/merge_space_configs.py "
            "<generated_dir> <space_key>"
        )
        print()
        print("  <generated_dir>  Path to envs/<env>/generated/")
        print("  <space_key>      Sanitized space key (e.g. finance_analytics)")
        sys.exit(1)

    generated_dir = Path(sys.argv[1])
    space_key = sys.argv[2]

    if not generated_dir.is_dir():
        print(f"ERROR: generated_dir does not exist: {generated_dir}")
        sys.exit(1)

    merge_into_assembled(generated_dir, space_key)


if __name__ == "__main__":
    main()
