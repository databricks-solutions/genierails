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
      * treatment_overrides: replaces entries for the merged space's columns,
        preserves other spaces, and resolves conflicts strictest-first
      * fgac_policies: appends new entries (dedup by policy name)
  - generated/masking_functions.sql:
      * appends new CREATE FUNCTION blocks (dedup by function name)

Groups and group_members are NEVER touched — they are shared governance state
established by full generation.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import hcl2
except ImportError:
    print("ERROR: python-hcl2 is required. Install with:")
    print("  pip install python-hcl2")
    sys.exit(2)

from tag_vocabulary import REGISTRY  # noqa: E402
from generate_abac import (  # noqa: E402
    autofix_acl_groups,
    reject_unowned_draft_acls,
    sanitize_space_key,
)
from treatment_derivation import load_treatment_config  # noqa: E402


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


def merge_treatment_overrides(
    existing: list[dict],
    incoming: list[dict],
    merged_space_columns: set[str],
) -> list[dict]:
    """Replace one space's overrides while preserving strictest protection."""
    treatment_config = load_treatment_config()
    treatment_rank = {
        treatment.value: index
        for index, treatment in enumerate(treatment_config.treatments)
    }

    def strictest(items: list[dict]) -> dict[str, dict]:
        by_column: dict[str, dict] = {}
        for item in items:
            column = item.get("entity_name", "")
            treatment = item.get("treatment", "")
            if not column or treatment not in treatment_rank:
                raise ValueError(
                    f"Invalid treatment override {column or '<missing column>'}="
                    f"{treatment!r} in per-space merge"
                )
            current = by_column.get(column)
            if (
                current is None
                or treatment_rank[treatment]
                < treatment_rank[current["treatment"]]
            ):
                by_column[column] = {
                    "entity_name": column,
                    "treatment": treatment,
                }
        return by_column

    retained = [
        item for item in existing
        if item.get("entity_name", "") not in merged_space_columns
    ]
    incoming_columns = {
        item.get("entity_name", "") for item in incoming if item.get("entity_name")
    }
    conflicts = [
        item for item in existing
        if item.get("entity_name", "") in incoming_columns
    ]
    merged = strictest(retained + conflicts + incoming)
    return [merged[column] for column in sorted(merged)]


def merge_into_assembled(generated_dir: Path, space_key: str) -> None:
    """Patch the assembled generated/abac.auto.tfvars and masking_functions.sql
    with content from generated/spaces/<space_key>/.
    """
    space_dir = generated_dir / "spaces" / space_key
    space_abac = space_dir / "abac.auto.tfvars"
    space_sql = space_dir / "masking_functions.sql"
    assembled_abac = generated_dir / "abac.auto.tfvars"
    assembled_sql = generated_dir / "masking_functions.sql"
    env_path = generated_dir.parent / "env.auto.tfvars"

    if not space_abac.exists():
        print(f"  ERROR: Per-space config not found: {space_abac}")
        sys.exit(1)

    print(f"\n  Merging generated/spaces/{space_key}/ into generated/...")

    # Security migration guard: inspect both sources before the formatter drops
    # draft ACL fields. This prevents a sibling's legacy ACL from silently
    # widening to fresh policy derivation during per-space generation.
    reject_unowned_draft_acls(assembled_abac, env_path)
    reject_unowned_draft_acls(space_abac, env_path)

    # ── Load per-space content ────────────────────────────────────────────
    space_cfg = load_hcl_safe(space_abac)

    new_genie_cfgs: dict = space_cfg.get("genie_space_configs") or {}
    new_id_to_name: dict = space_cfg.get("genie_space_id_to_name") or {}
    new_tag_assignments: list = space_cfg.get("tag_assignments") or []
    new_treatment_overrides: list = space_cfg.get("treatment_overrides") or []
    new_fgac_policies: list = space_cfg.get("fgac_policies") or []
    new_tag_policies: list = space_cfg.get("tag_policies") or []

    # ── Load assembled content ────────────────────────────────────────────
    assembled_cfg = load_hcl_safe(assembled_abac)
    assembled_text = assembled_abac.read_text() if assembled_abac.exists() else ""

    existing_genie_cfgs: dict = assembled_cfg.get("genie_space_configs") or {}
    existing_id_to_name: dict = assembled_cfg.get("genie_space_id_to_name") or {}
    existing_tag_assignments: list = assembled_cfg.get("tag_assignments") or []
    existing_treatment_overrides: list = assembled_cfg.get("treatment_overrides") or []
    existing_fgac_policies: list = assembled_cfg.get("fgac_policies") or []
    existing_tag_policies: list = assembled_cfg.get("tag_policies") or []

    # ── Merge tag_policies (dedup by key — add any new keys from per-space) ─
    # Per-space generation may introduce tag_keys not present in the assembled
    # config (e.g. phi_level for a Clinical space).  Those keys must be added
    # to the assembled tag_policies so that validation passes and the account
    # layer creates the corresponding Databricks tag policies.
    def _normalize_policy(policy: dict) -> dict | None:
        key = policy.get("key", "")
        if not key:
            return None
        canonical_key = REGISTRY.canonical_key(key)
        values: list[str] = []
        for raw_value in policy.get("values", []) or []:
            canonical_value = REGISTRY.canonical_value(canonical_key, raw_value)
            if REGISTRY.is_allowed_value(canonical_key, canonical_value) is False:
                raise ValueError(
                    f"Per-space tag_policy '{canonical_key}' contains unknown canonical value "
                    f"'{canonical_value}'"
                )
            if canonical_value not in values:
                values.append(canonical_value)
        return {
            **policy,
            "key": canonical_key,
            "values": values,
        }

    existing_tag_policies = [
        normalized
        for normalized in (_normalize_policy(tp) for tp in existing_tag_policies)
        if normalized
    ]
    new_tag_policies = [
        normalized
        for normalized in (_normalize_policy(tp) for tp in new_tag_policies)
        if normalized
    ]

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
    merged_id_to_name = {**existing_id_to_name, **new_id_to_name}
    new_names = set(new_genie_cfgs)
    for old_name in list(merged_genie):
        if sanitize_space_key(old_name) == space_key and old_name not in new_names:
            del merged_genie[old_name]
            print(f"    genie_space_configs: removed renamed entry '{old_name}'")
    for space_name, cfg in new_genie_cfgs.items():
        merged_genie[space_name] = cfg
        print(f"    genie_space_configs: updated entry '{space_name}'")

    if env_path.exists():
        try:
            with env_path.open() as handle:
                env_cfg = hcl2.load(handle)
        except Exception as exc:
            raise ValueError(
                f"Cannot safely prune orphan Genie configs because {env_path} "
                f"is invalid: {exc}"
            ) from exc
        unknown_id_only = [
            space.get("genie_space_id", "")
            for space in (env_cfg.get("genie_spaces") or [])
            if isinstance(space, dict)
            and not space.get("name")
            and not merged_id_to_name.get(space.get("genie_space_id", ""))
        ]
        active_names = {
            (space.get("name") or merged_id_to_name.get(space.get("genie_space_id", ""), ""))
            for space in (env_cfg.get("genie_spaces") or [])
            if isinstance(space, dict)
        }
        active_names.discard("")
        if unknown_id_only:
            print(
                "  WARNING: Cannot safely identify orphan Genie configs because "
                "id-only space(s) lack canonical-name attribution: "
                + ", ".join(repr(space_id) for space_id in unknown_id_only)
                + ". Keeping all configs."
            )
        else:
            for orphan in sorted(set(merged_genie) - active_names - new_names):
                del merged_genie[orphan]
                print(
                    f"  WARNING: Dropped orphan genie_space_configs entry {orphan!r}; "
                    "it has no matching genie_spaces entry."
                )

    # ── Merge tag_assignments (dedup by entity_name + tag_key) ───────────
    def _normalize_assignment(assignment: dict) -> dict:
        canonical_key = REGISTRY.canonical_key(assignment.get("tag_key", ""))
        canonical_value = REGISTRY.canonical_value(
            canonical_key,
            assignment.get("tag_value", ""),
        )
        return {
            **assignment,
            "tag_key": canonical_key,
            "tag_value": canonical_value,
        }

    existing_tag_assignments = [_normalize_assignment(ta) for ta in existing_tag_assignments]
    new_tag_assignments = [_normalize_assignment(ta) for ta in new_tag_assignments]

    existing_ta_keys = {
        (ta.get("entity_type", ""), ta.get("entity_name", ""), ta.get("tag_key", "")): ta
        for ta in existing_tag_assignments
    }
    added_ta = 0
    merged_tag_assignments = list(existing_tag_assignments)
    for ta in new_tag_assignments:
        key = (ta.get("entity_type", ""), ta.get("entity_name", ""), ta.get("tag_key", ""))
        existing_assignment = existing_ta_keys.get(key)
        if existing_assignment and existing_assignment.get("tag_value") != ta.get("tag_value"):
            # Per-space generate takes precedence — it's a more focused, recent
            # call specifically for the target space's tables.  Update in place.
            print(
                f"    tag_assignments: resolved merge conflict for "
                f"{ta.get('entity_name', '')} / {ta.get('tag_key', '')}: "
                f"'{existing_assignment.get('tag_value')}' → '{ta.get('tag_value')}'"
            )
            existing_assignment["tag_value"] = ta.get("tag_value")
            continue
        if key not in existing_ta_keys:
            merged_tag_assignments.append(ta)
            existing_ta_keys[key] = ta
            added_ta += 1
    if added_ta:
        print(f"    tag_assignments: added {added_ta} new entry/entries")

    # ── Merge reviewed treatment overrides by column ─────────────────────
    merged_space_columns = {
        item.get("entity_name", "")
        for item in new_tag_assignments
        if item.get("entity_type") == "columns" and item.get("entity_name")
    } | {
        item.get("entity_name", "")
        for item in new_treatment_overrides
        if item.get("entity_name")
    }

    merged_treatment_overrides = merge_treatment_overrides(
        existing_treatment_overrides,
        new_treatment_overrides,
        merged_space_columns,
    )

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

    updated = remove_hcl_top_level_block(updated, "genie_space_id_to_name")
    if merged_id_to_name:
        updated = (
            updated.rstrip()
            + "\n\ngenie_space_id_to_name = "
            + _render_value(dict(sorted(merged_id_to_name.items())))
            + "\n"
        )

    # Replace tag_assignments block
    updated = remove_hcl_top_level_list(updated, "tag_assignments")
    if merged_tag_assignments:
        ta_hcl = "tag_assignments = " + _render_value(merged_tag_assignments)
        updated = updated.rstrip() + "\n\n" + ta_hcl + "\n"

    # Replace treatment_overrides even when the merged space removed its last
    # override; omission of an empty block keeps the native-only path unchanged.
    updated = remove_hcl_top_level_list(updated, "treatment_overrides")
    if merged_treatment_overrides:
        overrides_hcl = "treatment_overrides = " + _render_value(
            merged_treatment_overrides
        )
        updated = updated.rstrip() + "\n\n" + overrides_hcl + "\n"

    # Replace fgac_policies block
    updated = remove_hcl_top_level_list(updated, "fgac_policies")
    if merged_fgac:
        fgac_hcl = "fgac_policies = " + _render_value(merged_fgac)
        updated = updated.rstrip() + "\n\n" + fgac_hcl + "\n"

    # Validate ACL derivation against the complete candidate before replacing
    # the assembled file, so a failed rename/catalog change is atomic.
    fd, candidate_name = tempfile.mkstemp(
        prefix=".abac.auto.tfvars.", dir=generated_dir, text=True
    )
    os.close(fd)
    candidate = Path(candidate_name)
    try:
        candidate.write_text(updated)
        autofix_acl_groups(
            candidate,
            env_path if env_path.exists() else None,
            reject_draft_acls=True,
        )
        candidate.replace(assembled_abac)
    except Exception:
        candidate.unlink(missing_ok=True)
        raise
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


# ---------------------------------------------------------------------------
# Sticky reviewed rules (re-running generate over a reviewed draft)
# ---------------------------------------------------------------------------

ALLOW_RULE_CHANGES_FLAG = "--allow-rule-changes"
_RULE_SECTIONS = ("tag_assignments", "treatment_overrides", "fgac_policies")
_ACCEPT_HINT = f"re-run with {ALLOW_RULE_CHANGES_FLAG} to accept"


class SqlTokenizeError(ValueError):
    """SQL that cannot be tokenized unambiguously (e.g. an unterminated literal)."""


def sql_tokens(sql: str) -> list[str]:
    """Tokenize SQL so two function bodies can be compared for meaning.

    String literals ('...', "...", $$...$$) and back-quoted identifiers are
    kept byte-exact; comments outside them are dropped; only unquoted words
    (keywords and identifiers, which are case-insensitive) are lower-cased.
    """
    tokens: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch.isspace():
            i += 1
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            i = n if end < 0 else end + 1
        elif sql.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if sql.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif sql.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            if depth:
                raise SqlTokenizeError("unterminated /* comment")
            i = j
        elif sql.startswith("$$", i):
            end = sql.find("$$", i + 2)
            if end < 0:
                raise SqlTokenizeError("unterminated $$ body")
            tokens.append(sql[i:end + 2])
            i = end + 2
        elif ch in "'\"`":
            # Backslash escapes apply to strings, but not to raw r'...' strings
            # or to back-quoted identifiers; a doubled quote always escapes.
            backslash = ch != "`" and not (i > 0 and sql[i - 1] in "rR" and tokens[-1:] == ["r"])
            j = i + 1
            while True:
                if j >= n:
                    raise SqlTokenizeError(f"unterminated {ch} literal")
                if backslash and sql[j] == "\\":
                    j += 2
                elif sql[j] == ch and sql.startswith(ch * 2, j):
                    j += 2
                elif sql[j] == ch:
                    break
                else:
                    j += 1
            tokens.append(sql[i:j + 1])
            i = j + 1
        elif ch.isalnum() or ch == "_":
            j = i
            while j < n and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            tokens.append(sql[i:j].lower())
            i = j
        else:
            tokens.append(ch)
            i += 1
    return tokens


def _same_sql(a: str, b: str) -> bool:
    """Equal token streams; anything that can't be tokenized counts as different."""
    try:
        return sql_tokens(a) == sql_tokens(b)
    except SqlTokenizeError:
        return False


def _unreadable(path: Path, exc: Exception) -> str:
    return (
        f"cannot read the reviewed rules in {path}: {exc}. Fix the file, or re-run "
        f"with {ALLOW_RULE_CHANGES_FLAG} to replace the reviewed rules with a new draft"
    )


def load_reviewed_rules(generated_dir: Path) -> tuple[dict, str] | None:
    """Return (abac config, masking SQL) of the reviewed draft, or None.

    None means there is no prior draft with rules (first-time generate, or a
    genie-mode import only). A draft that can't be read — including missing
    masking SQL that its policies need — raises ValueError, so a re-run never
    silently treats reviewed rules as absent.
    """
    abac_path = generated_dir / "abac.auto.tfvars"
    if not abac_path.exists():
        return None
    try:
        cfg = hcl2.loads(abac_path.read_text())
    except Exception as exc:
        raise ValueError(_unreadable(abac_path, exc)) from exc
    if not any(cfg.get(section) for section in _RULE_SECTIONS):
        return None
    sql_path = generated_dir / "masking_functions.sql"
    if not sql_path.exists():
        if any(p.get("function_name") for p in cfg.get("fgac_policies") or []):
            raise ValueError(_unreadable(
                sql_path, "missing, but the reviewed policies use masking functions"
            ))
        return cfg, ""
    try:
        sql = sql_path.read_text()
        for block in split_into_function_blocks(sql):
            sql_tokens(block)
    except (OSError, UnicodeDecodeError, SqlTokenizeError) as exc:
        raise ValueError(_unreadable(sql_path, exc)) from exc
    return cfg, sql


def footprint_from_ddl(ddl_text: str) -> dict[str, set[str] | None] | None:
    """Map each fetched table to its columns (lower-case); None if unknown.

    A table whose columns could not be parsed maps to None, so its reviewed
    column rules are never treated as stale on a parse miss.
    """
    from validate_abac import parse_ddl_columns

    tables: dict[str, set[str] | None] = {
        match.group(1).replace("`", "").lower(): None
        for match in re.finditer(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([\w.`]+)", ddl_text or "", re.IGNORECASE
        )
    }
    for column in parse_ddl_columns(ddl_text or ""):
        table, name = column.lower().rsplit(".", 1)
        tables[table] = (tables.get(table) or set()) | {name}
    return tables or None


def _function_key(block: str) -> tuple[str, str, str] | None:
    names = extract_function_names(block)
    if len(names) != 1:
        return None
    catalog = re.search(r"USE\s+CATALOG\s+(\S+?);", block, re.IGNORECASE)
    schema = re.search(r"USE\s+SCHEMA\s+(\S+?);", block, re.IGNORECASE)
    return (
        catalog.group(1).lower() if catalog else "",
        schema.group(1).lower() if schema else "",
        names.pop(),
    )


def _function_blocks_by_key(sql_text: str) -> dict[tuple[str, str, str], str]:
    blocks: dict[tuple[str, str, str], str] = {}
    for block in split_into_function_blocks(sql_text):
        key = _function_key(block)
        if key:
            blocks[key] = block
    return blocks


def _policy_function(policy: dict) -> tuple[str, str, str] | None:
    name = str(policy.get("function_name") or "").lower()
    if not name:
        return None
    return (
        str(policy.get("function_catalog") or "").lower(),
        str(policy.get("function_schema") or "").lower(),
        name,
    )


def _find_function(
    blocks: dict[tuple[str, str, str], str], ref: tuple[str, str, str]
) -> tuple[str, str, str] | None:
    """The block defining ref: exact catalog.schema.name, else by name (as validation does)."""
    if ref in blocks:
        return ref
    return next((key for key in blocks if key[2] == ref[2]), None)


def _fingerprint(value) -> str:
    return json.dumps(value, sort_keys=True)


def _tag_set(items: list[dict]) -> list[tuple[str, str]]:
    return sorted((i.get("tag_key", ""), i.get("tag_value", "")) for i in items)


def _entity(item: dict) -> tuple[str, str]:
    return item.get("entity_type", ""), item.get("entity_name", "")


def _by_entity(assignments: list[dict]) -> dict[tuple[str, str], list[dict]]:
    grouped: dict[tuple[str, str], list[dict]] = {}
    for item in assignments:
        grouped.setdefault(_entity(item), []).append(item)
    return grouped


def _policy_targets(policy: dict, by_entity: dict[tuple[str, str], list[dict]]) -> set[tuple[str, str]]:
    """Entities a policy protects: columns for a mask, tables for a row filter."""
    from validate_abac import _condition_matches_tags

    def tags(entity: tuple[str, str]) -> dict[str, set[str]]:
        result: dict[str, set[str]] = {}
        for item in by_entity.get(entity, []):
            result.setdefault(item.get("tag_key", ""), set()).add(item.get("tag_value", ""))
        return result

    kind = {
        "POLICY_TYPE_COLUMN_MASK": "columns",
        "POLICY_TYPE_ROW_FILTER": "tables",
    }.get(policy.get("policy_type", ""))
    catalog = policy.get("catalog", "") or policy.get("function_catalog", "")
    targets: set[tuple[str, str]] = set()
    for entity_type, name in by_entity:
        if entity_type != kind or (catalog and name.split(".")[0] != catalog):
            continue
        if kind == "columns":
            table = ("tables", name.rsplit(".", 1)[0])
            if (_condition_matches_tags(policy.get("match_condition", ""), tags((entity_type, name)))
                    and _condition_matches_tags(policy.get("when_condition", ""), tags(table))):
                targets.add((entity_type, name))
        elif _condition_matches_tags(policy.get("when_condition", ""), tags((entity_type, name))):
            targets.add((entity_type, name))
    return targets


def _condition_tag_refs(policy: dict) -> set[tuple[str, str | None]]:
    text = f"{policy.get('match_condition', '')} {policy.get('when_condition', '')}"
    refs: set[tuple[str, str | None]] = set(
        re.findall(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", text)
    )
    refs |= {(key, None) for key in re.findall(r"hasTag\(\s*'([^']+)'\s*\)", text)}
    return refs


def keep_reviewed_rules(
    reviewed: tuple[dict, str],
    abac_path: Path,
    sql_path: Path,
    *,
    footprint: dict[str, set[str] | None] | None = None,
    partial_footprint: bool = False,
    allow_changes: bool = False,
) -> list[str]:
    """Merge a new draft additively over the reviewed rules of the prior run.

    The unit is the protected target — a column or table, together with its
    tags, treatment override and the policies that match it. A target the
    reviewed draft protects keeps exactly its reviewed protection: model
    edits are reverted and model additions that would also land on it (a new
    override, a differently named policy) are discarded. Targets the reviewed
    draft did not cover take the model's rules. Reviewed policies (by name)
    and masking functions (by catalog.schema.name, compared by SQL token
    stream) are restored the same way.

    Reviewed rules for objects no longer in ``footprint`` (table -> columns,
    from the fetched DDL) are dropped as stale. With ``partial_footprint``
    (SPACE= / --tables runs) only tables in the footprint are checked.

    allow_changes accepts the new draft as written. Returns one message per
    affected rule; raises ValueError if a kept policy's function is in
    neither the reviewed nor the new SQL.
    """
    prior_cfg, prior_sql = reviewed
    new_cfg = load_hcl_safe(abac_path)
    stale: list[str] = []
    changes: list[tuple[str, str]] = []  # (reviewed rule, what the model proposed)

    def stale_reason(entity_type: str, name: str) -> str | None:
        if footprint is None:
            return None
        parts = name.lower().split(".")
        table = ".".join(parts[:3])
        what = "table" if entity_type == "tables" else "column"
        if table not in footprint:
            return None if partial_footprint else f"{what} no longer exists"
        columns = footprint[table]
        if entity_type == "columns" and len(parts) == 4 and columns is not None and parts[3] not in columns:
            return "column no longer exists"
        return None

    def label(entity: tuple[str, str], items: list[dict]) -> str:
        tags = dict(_tag_set(items))
        if "gr_treatment" in tags:
            return f"{entity[1]} → {tags['gr_treatment']}"
        if tags:
            return f"{entity[1]} → " + ", ".join(f"{k}={v}" for k, v in tags.items())
        return entity[1]

    # ── Tag mappings: one target's full tag set is one rule ──────────────
    prior_by_entity = _by_entity(prior_cfg.get("tag_assignments") or [])
    new_assignments = list(new_cfg.get("tag_assignments") or [])
    new_by_entity = _by_entity(new_assignments)
    targets: dict[tuple[str, str], str] = {}  # reviewed target -> its label
    for entity, items in prior_by_entity.items():
        reason = stale_reason(*entity)
        if reason:
            stale.append(f"{label(entity, items)} ({reason})")
            continue
        targets[entity] = label(entity, items)
        proposed = new_by_entity.get(entity)
        if proposed is None:
            changes.append((targets[entity], "model proposed removing it"))
        elif _tag_set(proposed) != _tag_set(items):
            changes.append((targets[entity], "model proposed changing it"))
    merged_assignments = [
        item for item in new_assignments if _entity(item) not in targets
    ] + [item for entity in targets for item in prior_by_entity[entity]]
    merged_by_entity = _by_entity(merged_assignments)

    # ── Treatment overrides belong to their column's target ──────────────
    prior_overrides = {o.get("entity_name", ""): o for o in prior_cfg.get("treatment_overrides") or []}
    new_overrides = list(new_cfg.get("treatment_overrides") or [])
    new_override_by_col = {o.get("entity_name", ""): o for o in new_overrides}
    kept_overrides: dict[str, dict] = {}
    for column, override in prior_overrides.items():
        rule = f"override {column} → {override.get('treatment', '')}"
        reason = stale_reason("columns", column)
        if reason:
            stale.append(f"{rule} ({reason})")
            continue
        kept_overrides[column] = override
        proposed = new_override_by_col.get(column)
        if proposed is None:
            changes.append((rule, "model proposed removing it"))
        elif proposed.get("treatment") != override.get("treatment"):
            changes.append((rule, "model proposed changing it"))
    for column, override in new_override_by_col.items():
        if column not in kept_overrides and ("columns", column) in targets:
            changes.append((
                targets[("columns", column)],
                f"model proposed a conflicting override → {override.get('treatment', '')}",
            ))
    merged_overrides = [
        o for o in new_overrides
        if o.get("entity_name", "") not in kept_overrides
        and ("columns", o.get("entity_name", "")) not in targets
    ] + list(kept_overrides.values())

    # ── FGAC policies: reviewed by name; model additions only off-target ──
    prior_policies = {p.get("name", ""): p for p in prior_cfg.get("fgac_policies") or []}
    new_policies = list(new_cfg.get("fgac_policies") or [])
    new_policy_by_name = {p.get("name", ""): p for p in new_policies}
    kept_policies: dict[str, dict] = {}
    stale_policy_functions: set[tuple[str, str, str]] = set()
    for name, policy in prior_policies.items():
        before = _policy_targets(policy, prior_by_entity)
        if (before and not _policy_targets(policy, merged_by_entity)
                and all(stale_reason(*entity) for entity in before)):
            what = "columns" if policy.get("policy_type") == "POLICY_TYPE_COLUMN_MASK" else "tables"
            stale.append(f"policy {name} (its {what} no longer exist)")
            if ref := _policy_function(policy):
                stale_policy_functions.add(ref)
            continue
        kept_policies[name] = policy
        proposed = new_policy_by_name.get(name)
        if proposed is None:
            changes.append((f"policy {name}", "model proposed removing it"))
        elif _fingerprint(proposed) != _fingerprint(policy):
            changes.append((f"policy {name}", "model proposed changing it"))
    discarded_policies: set[str] = set()
    for policy in new_policies:
        name = policy.get("name", "")
        if name in prior_policies:
            continue
        overlap = sorted(_policy_targets(policy, merged_by_entity) & set(targets))
        if overlap:
            discarded_policies.add(name)
            more = f" and {len(overlap) - 1} more" if len(overlap) > 1 else ""
            changes.append((
                targets[overlap[0]] + more,
                f"model proposed a conflicting policy {name}",
            ))
    merged_policies = [
        kept_policies.get(p.get("name", ""), p) for p in new_policies
        if p.get("name", "") not in discarded_policies
    ] + [p for name, p in kept_policies.items() if name not in new_policy_by_name]
    abac_changed = bool(changes)

    # ── Masking functions, by catalog.schema.name ────────────────────────
    new_sql = sql_path.read_text() if sql_path.exists() else ""
    prior_fns = _function_blocks_by_key(prior_sql)
    new_fns = _function_blocks_by_key(new_sql)
    used = {ref for p in merged_policies if (ref := _policy_function(p))}
    footprint_catalogs = {table.split(".")[0] for table in footprint or {}}
    kept_fns: set[tuple[str, str, str]] = set()
    for key, block in prior_fns.items():
        proposed = new_fns.get(key)
        if proposed is not None and _same_sql(proposed, block):
            continue
        if not any(_find_function({key: block}, ref) for ref in used):
            # Unused by any remaining policy: stale if only stale policies used
            # it, or if its catalog left the footprint.
            if any(_find_function({key: block}, ref) for ref in stale_policy_functions):
                stale.append(f"function {'.'.join(p for p in key if p)} (only stale policies used it)")
                continue
            if (footprint is not None and not partial_footprint
                    and key[0] and key[0] not in footprint_catalogs):
                stale.append(f"function {'.'.join(key)} (catalog no longer in the footprint)")
                continue
        kept_fns.add(key)
        changes.append((
            f"function {'.'.join(p for p in key if p)}",
            "model proposed removing it" if proposed is None else "model proposed changing it",
        ))

    messages = [f"  dropped stale reviewed rule {rule}" for rule in stale]
    if allow_changes:
        return messages + [
            f"  accepted model change to reviewed rule {rule} ({ALLOW_RULE_CHANGES_FLAG})"
            for rule, _ in changes
        ]

    # Every kept reviewed policy must still resolve to a masking function.
    final_fns = {k: (prior_fns[k] if k in kept_fns else b) for k, b in new_fns.items()}
    final_fns.update({k: prior_fns[k] for k in kept_fns})
    for name, policy in kept_policies.items():
        ref = _policy_function(policy)
        if ref is None or _find_function(final_fns, ref):
            continue
        source = _find_function(prior_fns, ref)
        if source is None:
            raise ValueError(
                f"reviewed policy {name} uses function {'.'.join(p for p in ref if p)}, which "
                "neither the reviewed nor the new masking_functions.sql defines. Add it to "
                f"generated/masking_functions.sql, or re-run with {ALLOW_RULE_CHANGES_FLAG}"
            )
        kept_fns.add(source)
        final_fns[source] = prior_fns[source]
        changes.append((f"function {'.'.join(p for p in source if p)}",
                        f"policy {name} still uses it"))

    if not changes:
        return messages

    if abac_changed:
        # Restored rules must stay valid against the tag vocabulary: carry the
        # reviewed tag_policies keys/values they reference into the new draft.
        needed: set[tuple[str, str | None]] = {
            (item.get("tag_key", ""), item.get("tag_value", ""))
            for entity in targets for item in prior_by_entity[entity]
        }
        for policy in kept_policies.values():
            needed |= _condition_tag_refs(policy)
        merged_tag_policies = [dict(p) for p in new_cfg.get("tag_policies") or []]
        by_key = {p.get("key", ""): p for p in merged_tag_policies}
        prior_tag_policies = {p.get("key", ""): p for p in prior_cfg.get("tag_policies") or []}
        for key, value in sorted(needed, key=lambda ref: (ref[0], ref[1] or "")):
            if key not in by_key and key in prior_tag_policies:
                by_key[key] = dict(prior_tag_policies[key], values=[])
                merged_tag_policies.append(by_key[key])
            if key in by_key and value is not None:
                values = list(by_key[key].get("values") or [])
                if value not in values:
                    by_key[key]["values"] = values + [value]
        text = abac_path.read_text()
        for section, items in (
            ("tag_policies", merged_tag_policies),
            ("tag_assignments", merged_assignments),
            ("treatment_overrides", merged_overrides),
            ("fgac_policies", merged_policies),
        ):
            text = remove_hcl_top_level_list(text, section)
            if items:
                text = text.rstrip() + f"\n\n{section} = " + _render_value(items) + "\n"
        abac_path.write_text(re.sub(r"\n{3,}", "\n\n", text))

    if kept_fns:
        first_create = re.search(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE\s+)?FUNCTION\b", new_sql, re.IGNORECASE
        )
        prefix = new_sql[:first_create.start()] if first_create else new_sql
        blocks = [
            prior_fns[key] if (key := _function_key(b)) in kept_fns else b
            for b in split_into_function_blocks(new_sql)
        ]
        blocks += [prior_fns[k] for k in prior_fns if k in kept_fns and k not in new_fns]
        sql_path.write_text(prefix.rstrip() + "\n\n" + "\n\n".join(blocks) + "\n")

    return messages + [
        f"  kept reviewed rule {rule} ({reason}); {_ACCEPT_HINT}"
        for rule, reason in changes
    ]


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
