#!/usr/bin/env python3
"""Scaffold fail-safe treatments for offline or live unmapped class.* semantics."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import hcl2

SHARED_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED_ROOT))

from generate_abac import (  # noqa: E402
    _fetch_live_classification_source,
    _render_fgac_policy_block,
    _render_tag_assignment_block,
    _render_tag_policy_block,
    _replace_bracket_section,
    discover_agent_footprint,
    footprint_table_refs,
    load_auth_config,
)
from treatment_derivation import derive_treatment_model, load_treatment_config  # noqa: E402
from scripts.footprint import load_discovered_footprint  # noqa: E402

MARKER_RE = re.compile(
    r"^\s*#\s*gr\.classification_unmapped:\s*([^|\n]+)\|(class\.([^\s]+))\s*$",
    re.MULTILINE,
)


def _slug(label: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    if not value:
        raise ValueError(f"Cannot derive a safe name from {label!r}")
    if value[0].isdigit():
        value = "class_" + value
    return value


def _live_unmapped_markers(auth_path: Path, env_path: Path) -> list[tuple[str, str, str]]:
    """Read unmapped class.* semantics from the release-time native source."""
    runtime = load_auth_config(auth_path, env_path)
    declared = list(runtime.get("uc_tables") or [])
    declared.extend(runtime.get("declared_footprint") or [])
    for space in runtime.get("genie_spaces") or []:
        declared.extend(space.get("declared_footprint") or space.get("uc_tables") or [])
    discovered, _agents = load_discovered_footprint(env_path.parent)
    declared.extend(discovered)
    table_refs = footprint_table_refs(discover_agent_footprint(declared_footprint=declared))
    native = _fetch_live_classification_source(table_refs, runtime, require_native=True)
    if native is None or not native.has_native_data():
        return []
    return [
        (column, f"class.{semantic}", semantic)
        for column, semantic in native.unmapped_columns(sorted(native.classified_columns()))
    ]


def scaffold(
    tfvars_path: Path,
    sql_path: Path,
    config_path: Path,
    vocabulary_path: Path | None = None,
    auth_path: Path | None = None,
    env_path: Path | None = None,
) -> list[dict[str, object]]:
    if not tfvars_path.is_file():
        raise FileNotFoundError(f"Generated ABAC config not found: {tfvars_path}")
    if not sql_path.is_file():
        raise FileNotFoundError(f"Masking-functions SQL not found: {sql_path}")

    tfvars_text = tfvars_path.read_text()
    markers = [(m.group(1).strip(), m.group(2).lower(), m.group(3).lower())
               for m in MARKER_RE.finditer(tfvars_text)]
    if not markers and auth_path is not None and env_path is not None:
        markers = _live_unmapped_markers(auth_path, env_path)
    if not markers:
        return []

    vocabulary_path = vocabulary_path or config_path.with_name("tag_vocabulary_registry.json")
    raw = json.loads(config_path.read_text())
    vocabulary = json.loads(vocabulary_path.read_text())
    configured_by_label = {
        label.lower(): treatment
        for treatment in raw["treatments"]
        for label in treatment.get("class_labels", [])
    }
    existing_values = {item["value"] for item in raw["treatments"]}
    existing_functions = {item["masking_function"] for item in raw["treatments"]}
    sql_functions = set(re.findall(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:[\w]+\.)*([\w]+)\s*\(",
        sql_path.read_text(), re.IGNORECASE,
    ))

    additions: list[dict[str, str]] = []
    for label in sorted({label for _, label, _ in markers}):
        if label in configured_by_label:
            continue
        semantic = label.removeprefix("class.")
        slug = _slug(semantic)
        value = f"{slug}_redacted"
        function = f"mask_{slug}_redact"
        if value in existing_values or function in existing_functions or function in sql_functions:
            raise ValueError(
                f"Refusing to overwrite existing treatment/function for {label}: "
                f"{value} / {function}"
            )
        additions.append({"label": label, "semantic": semantic,
                          "value": value, "function": function})
        existing_values.add(value)
        existing_functions.add(function)

    strict_insert_at = next(
        (index + 1 for index, treatment in enumerate(raw["treatments"])
         if treatment["value"] == "redact"),
        0,
    )
    for item in additions:
        treatment = {
            "value": item["value"],
            "masking_function": item["function"],
            "udf_signature": f'{item["function"]}(input STRING) RETURNS STRING',
            "udf_body": "CASE WHEN input IS NULL THEN NULL ELSE '[REDACTED]' END",
            "sources": [],
            "class_labels": [item["label"]],
            "review": (
                f'REVIEW: auto-scaffolded for {item["label"]}; defaults to full '
                "redaction — implement type-appropriate masking or leave as-is"
            ),
        }
        # The config is strictest-first. These stubs have the same effective
        # strength as full redaction, so keep them ahead of every partial mask.
        raw["treatments"].insert(strict_insert_at, treatment)
        strict_insert_at += 1
        configured_by_label[item["label"]] = treatment
        family = vocabulary["families"]["gr_treatment"]
        if item["value"] not in family["canonical_values"]:
            family["canonical_values"].append(item["value"])
        family.setdefault("review_values", {})[item["value"]] = (
            f'REVIEW: auto-scaffolded for {item["label"]}; defaults to full '
            "redaction — implement type-appropriate masking or leave as-is"
        )

    # Validate uniqueness and schema before writing any file.
    staged_config = config_path.with_name(config_path.name + ".scaffold-check")
    try:
        staged_config.write_text(json.dumps(raw, indent=2) + "\n")
        config = load_treatment_config(staged_config)
    finally:
        staged_config.unlink(missing_ok=True)

    value_by_label = {
        label: treatment["value"] for label, treatment in configured_by_label.items()
    }
    cfg = hcl2.loads(tfvars_text)
    assignments = list(cfg.get("tag_assignments") or [])
    seen_assignments = {
        (a.get("entity_name"), a.get("tag_key"), a.get("tag_value")) for a in assignments
    }
    for entity, label, _ in markers:
        value = value_by_label.get(label)
        if value and (entity, config.tag_key, value) not in seen_assignments:
            assignments.append({"entity_type": "columns", "entity_name": entity,
                                "tag_key": config.tag_key, "tag_value": value})
    cfg["tag_assignments"] = assignments
    derived, _ = derive_treatment_model(cfg, config)

    # A marker may be removed only when derivation actually left that column
    # with the configured treatment or something stricter. Abort before any
    # persistent write if this invariant cannot be proved, leaving the marker
    # in place so coverage-gate continues to block.
    treatment_rank = {
        treatment.value: rank for rank, treatment in enumerate(config.treatments)
    }
    derived_by_entity = {
        item.get("entity_name"): item.get("tag_value")
        for item in derived.get("tag_assignments", [])
        if item.get("tag_key") == config.tag_key
    }
    for entity, label, _ in markers:
        expected = value_by_label[label]
        actual = derived_by_entity.get(entity)
        if (
            actual not in treatment_rank
            or treatment_rank[actual] > treatment_rank[expected]
        ):
            raise ValueError(
                f"Refusing to remove marker for {entity}|{label}: derived "
                f"treatment {actual!r} is missing or weaker than {expected!r}"
            )

    updated_tfvars = MARKER_RE.sub(
        lambda match: match.group(0) if match.group(2).lower() not in value_by_label else "",
        tfvars_text,
    )
    updated_tfvars = _replace_bracket_section(
        updated_tfvars, "tag_policies",
        [_render_tag_policy_block(item) for item in derived.get("tag_policies", [])],
    )
    updated_tfvars = _replace_bracket_section(
        updated_tfvars, "tag_assignments",
        [_render_tag_assignment_block(item) for item in derived.get("tag_assignments", [])],
    )
    updated_tfvars = _replace_bracket_section(
        updated_tfvars, "fgac_policies",
        [_render_fgac_policy_block(item) for item in derived.get("fgac_policies", [])],
    )
    hcl2.loads(updated_tfvars)

    sql = sql_path.read_text().rstrip()
    blocks = []
    for item in additions:
        blocks.append(
            f'-- REVIEW: auto-scaffolded for {item["label"]}; defaults to full '
            "redaction — implement type-appropriate masking or leave as-is\n"
            f'CREATE OR REPLACE FUNCTION {item["function"]}(input STRING)\n'
            "RETURNS STRING\n"
            f"COMMENT 'REVIEW: fail-safe full-redaction stub for {item['label']}.'\n"
            "RETURN CASE\n"
            "  WHEN input IS NULL THEN NULL\n"
            "  ELSE '[REDACTED]'\n"
            "END;"
        )

    if additions:
        config_path.write_text(json.dumps(raw, indent=2) + "\n")
        vocabulary_path.write_text(json.dumps(vocabulary, indent=2) + "\n")
        sql_path.write_text(sql + "\n\n" + "\n\n".join(blocks) + "\n")
    tfvars_path.write_text(updated_tfvars)
    actions = []
    added_labels = {item["label"] for item in additions}
    for label in sorted({label for _, label, _ in markers}):
        treatment = configured_by_label[label]
        actions.append({
            "label": label,
            "semantic": label.removeprefix("class."),
            "value": treatment["value"],
            "function": treatment["masking_function"],
            "added": label in added_labels,
        })
    return actions


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tfvars", type=Path, required=True)
    parser.add_argument("--sql", type=Path, required=True)
    parser.add_argument("--treatment-config", type=Path, required=True)
    parser.add_argument("--tag-vocabulary", type=Path)
    parser.add_argument("--auth-file", type=Path)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    try:
        additions = scaffold(
            args.tfvars, args.sql, args.treatment_config, args.tag_vocabulary,
            args.auth_file, args.env_file,
        )
    except (RuntimeError, FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    if not additions:
        print("No unmapped class.* tags found; nothing changed.")
        return 0
    for item in additions:
        verb = "ADDED" if item["added"] else "APPLIED EXISTING"
        suffix = " (full-redaction REVIEW stub)" if item["added"] else ""
        print(
            f'{verb} {item["label"]} -> gr_treatment={item["value"]} '
            f'-> {item["function"]}{suffix}'
        )
    print("Review the stubs, then re-run `make release` (or `make generate`+`make coverage-gate`).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
