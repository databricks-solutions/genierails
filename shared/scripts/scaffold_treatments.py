#!/usr/bin/env python3
"""Scaffold fail-safe treatments for offline or live unmapped class.* semantics.

With --treatment, map each unmapped class.* to that existing treatment instead
(no new treatment, UDF or vocabulary entry), refusing before any write when the
treatment is unknown or its UDF can't take the column's data type.
"""

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
    fetch_tables_from_databricks,
    footprint_table_refs,
    load_auth_config,
)
from treatment_derivation import (  # noqa: E402
    derive_treatment_model,
    effective_treatment,
    load_treatment_config,
)
from scripts.coverage_fix import (  # noqa: E402
    ddl_column_types,
    env_column_types,
    fitting_treatments,
    promote_source,
    rerun_command,
    treatment_input_type,
    types_compatible,
)
from scripts.footprint import load_discovered_footprint  # noqa: E402

MARKER_RE = re.compile(
    r"^\s*#\s*gr\.classification_unmapped:\s*([^|\n]+)\|(class\.([^\s]+))\s*$",
    re.MULTILINE,
)

TAG_VALUE_RE = re.compile(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)")


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


def _unused_treatment_masks(before: dict, derived: dict, config) -> list[dict]:
    """Reviewed treatment masks no column uses yet (make materialize-treatment).

    Derivation rebuilds masks only for treatments its assignments use; keep
    the others exactly as reviewed so a materialized mask is not dropped.
    """
    def key(policy: dict) -> set[tuple[str, str]]:
        return {
            (policy.get("catalog", ""), value)
            for tag_key, value in TAG_VALUE_RE.findall(policy.get("match_condition", "") or "")
            if tag_key == config.tag_key and value in config.values
        }

    masks = [p for p in derived.get("fgac_policies") or []
             if p.get("policy_type") == "POLICY_TYPE_COLUMN_MASK"]
    names = {p.get("name") for p in derived.get("fgac_policies") or []}
    covered = set().union(*(key(p) for p in masks)) if masks else set()
    return [
        policy for policy in before.get("fgac_policies") or []
        if policy.get("policy_type") == "POLICY_TYPE_COLUMN_MASK"
        and policy.get("name") not in names
        and key(policy) and not key(policy) & covered
    ]


def scaffold(
    tfvars_path: Path,
    sql_path: Path,
    config_path: Path,
    vocabulary_path: Path | None = None,
    auth_path: Path | None = None,
    env_path: Path | None = None,
    *,
    reuse_treatment: str | None = None,
    allow_unknown_type: bool = False,
) -> list[dict[str, object]]:
    if not tfvars_path.is_file():
        raise FileNotFoundError(f"Generated ABAC config not found: {tfvars_path}")
    if not sql_path.is_file():
        raise FileNotFoundError(f"Masking-functions SQL not found: {sql_path}")

    tfvars_text = tfvars_path.read_text()
    markers = [(m.group(1).strip(), m.group(2).lower(), m.group(3).lower())
               for m in MARKER_RE.finditer(tfvars_text)]
    live = False
    if not markers and auth_path is not None and env_path is not None:
        markers = _live_unmapped_markers(auth_path, env_path)
        live = True
    if not markers:
        return []
    if reuse_treatment is not None:
        return reuse(
            tfvars_path, sql_path, config_path, reuse_treatment, markers,
            live_auth=(auth_path, env_path) if live else None,
            allow_unknown_type=allow_unknown_type,
        )

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
        [_render_fgac_policy_block(item)
         for item in derived.get("fgac_policies", []) + _unused_treatment_masks(
             hcl2.loads(tfvars_text), derived, config)],
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


def _live_column_types(auth_path: Path, env_path: Path, columns: list[str]) -> dict[str, str]:
    """Data types of ``columns`` read from Unity Catalog (as make generate does)."""
    import contextlib
    import io

    tables = sorted({column.rsplit(".", 1)[0] for column in columns})
    runtime = load_auth_config(auth_path, env_path)
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            ddl_text, _ = fetch_tables_from_databricks(tables, runtime)
    except (Exception, SystemExit) as exc:
        print(f"WARNING: could not read column types from Unity Catalog: {exc}", file=sys.stderr)
        return {}
    return ddl_column_types(ddl_text)


def _shown(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(SHARED_ROOT.parent))
    except ValueError:
        return str(path)


def reuse(
    tfvars_path: Path,
    sql_path: Path,
    config_path: Path,
    treatment_value: str,
    markers: list[tuple[str, str, str]],
    *,
    live_auth: tuple[Path, Path] | None = None,
    allow_unknown_type: bool = False,
) -> list[dict[str, object]]:
    """Map every unmapped class.* in ``markers`` to an existing treatment.

    Only that treatment's class_labels in the shared config change; the env's
    generated files are left alone (the next derivation maps the columns), so
    a promoted env's applied policy names stay exactly as deployed.
    """
    raw = json.loads(config_path.read_text())
    config = load_treatment_config(config_path)
    target = next((t for t in config.treatments if t.value == treatment_value), None)
    if target is None:
        raise ValueError(
            f"TREATMENT={treatment_value} is not a treatment in {_shown(config_path)}. "
            f"Existing treatments: {', '.join(config.values)}. For a new kind of mask, "
            "run make scaffold-treatments without TREATMENT"
        )
    configured = {label.lower(): t.value for t in config.treatments for label in t.class_labels}
    pending = [(entity, label) for entity, label, _ in markers if label not in configured]
    already = sorted({(label, configured[label]) for _, label, _ in markers if label in configured})

    sql_text = sql_path.read_text()
    udf_type = treatment_input_type(target, sql_text)
    env_dir = tfvars_path.parent.parent
    column_types = env_column_types(env_dir)
    unknown = [entity for entity, _ in pending if entity.lower() not in column_types]
    if unknown and live_auth is not None:
        column_types.update(_live_column_types(*live_auth, unknown))

    problems, unknown_types, checked = [], [], []
    for entity, label in pending:
        column_type = column_types.get(entity.lower())
        checked.append((entity, label, column_type))
        if udf_type is None or column_type is None:
            missing = (
                f"what type {target.masking_function} takes" if udf_type is None
                else f"the data type of {entity}"
            )
            unknown_types.append(f"{entity} ({label}): cannot tell {missing}")
        elif not types_compatible(udf_type, column_type):
            fits = fitting_treatments(config, entity, column_type, sql_text)
            problems.append(
                f"{entity} ({label}) is {column_type} but {target.masking_function} "
                f"takes {udf_type}; treatments that fit it: {', '.join(fits) or 'none'}"
            )
        effective = effective_treatment(entity, target, config)
        if effective.value != target.value:
            problems.append(
                f"{entity} ({label}) does not look like a {target.masking_function} column, "
                f"so derivation would mask it with {effective.value} instead of "
                f"{treatment_value} (and audit-rulebook would report drift); "
                f"use TREATMENT={effective.value}"
            )
    if problems:
        raise ValueError(
            f"Refusing to map to TREATMENT={treatment_value}; nothing was changed:\n  - "
            + "\n  - ".join(problems)
        )
    if unknown_types and not allow_unknown_type:
        raise ValueError(
            f"Refusing to map to TREATMENT={treatment_value} without a type check; nothing "
            "was changed:\n  - " + "\n  - ".join(unknown_types)
            + "\n  Refresh the column types (make derive-assignments, or the make release "
            "that stopped, writes ddl/_fetched.sql), or re-run with ALLOW_UNKNOWN_TYPE=1 "
            "after checking the types yourself"
        )

    labels = sorted({label for _, label in pending})
    entry = next(item for item in raw["treatments"] if item["value"] == treatment_value)
    entry["class_labels"] = list(entry.get("class_labels", [])) + labels
    staged = config_path.with_name(config_path.name + ".scaffold-check")
    try:
        staged.write_text(json.dumps(raw, indent=2) + "\n")
        load_treatment_config(staged)
    finally:
        staged.unlink(missing_ok=True)

    cfg = hcl2.loads(tfvars_path.read_text())
    covered = {
        policy.get("catalog")
        for policy in cfg.get("fgac_policies") or []
        if policy.get("policy_type") == "POLICY_TYPE_COLUMN_MASK"
        and (config.tag_key, treatment_value) in TAG_VALUE_RE.findall(
            policy.get("match_condition", "") or "")
        and policy.get("function_name") == target.masking_function
    }
    missing_catalogs = sorted({entity.split(".", 1)[0] for entity, _ in pending} - covered)

    if labels:
        config_path.write_text(json.dumps(raw, indent=2) + "\n")
    return [{
        "reuse": True, "treatment": treatment_value, "function": target.masking_function,
        "udf_type": udf_type, "labels": labels, "already": already, "checked": checked,
        "config": _shown(config_path), "offline": live_auth is None,
        "missing_catalogs": missing_catalogs,
    }]


def _print_reuse(result: dict, env_name: str, env_dir: Path) -> None:
    treatment = result["treatment"]
    for label, value in result["already"]:
        print(f"UNCHANGED {label} is already mapped to gr_treatment={value}")
    if not result["labels"]:
        print("No unmapped class.* tags to map; nothing changed.")
        return
    print(f"REUSED gr_treatment={treatment} -> {result['function']} "
          f"(input {result['udf_type'] or 'type unknown'}) for:")
    for entity, label, column_type in result["checked"]:
        print(f"  {label}  ({entity}: {column_type or 'type unknown'})")
    print("Changed:")
    print(f"  {result['config']}: added {', '.join(result['labels'])} to the class_labels "
          f"of treatment {treatment}")
    source = promote_source(env_dir)
    rerun = rerun_command(env_name, bool(source))
    if result["offline"]:
        # The markers came from this env's generated draft; regenerating maps them.
        rerun = f"make generate ENV={env_name}, then {rerun}"
    if result["missing_catalogs"]:
        catalogs = ", ".join(result["missing_catalogs"])
        print(f"The {treatment} mask is not in this env's rules for catalog(s) {catalogs} yet.")
        if source:
            print(f"Next: commit {result['config']}, then run make materialize-treatment "
                  f"ENV={source} TREATMENT={treatment}, make rehearse ENV={source}, "
                  f"make promote-to ENV={env_name}, {rerun}")
        else:
            print(f"Next: make materialize-treatment ENV={env_name} TREATMENT={treatment}, "
                  f"then {rerun}; commit {result['config']}")
        return
    print(f"Next: commit {result['config']}, then run: {rerun}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tfvars", type=Path, required=True)
    parser.add_argument("--sql", type=Path, required=True)
    parser.add_argument("--treatment-config", type=Path, required=True)
    parser.add_argument("--tag-vocabulary", type=Path)
    parser.add_argument("--auth-file", type=Path)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--env-name", default="",
                        help="env name, for the next-step commands (default: the env dir name)")
    parser.add_argument("--treatment", default="",
                        help="map unmapped class.* tags to this existing treatment instead of "
                             "scaffolding a new one")
    parser.add_argument("--allow-unknown-type", action="store_true",
                        help="with --treatment: map even when a column or UDF type is unknown")
    args = parser.parse_args()
    env_dir = args.tfvars.resolve().parent.parent
    env_name = args.env_name or env_dir.name
    try:
        additions = scaffold(
            args.tfvars, args.sql, args.treatment_config, args.tag_vocabulary,
            args.auth_file, args.env_file,
            reuse_treatment=args.treatment.strip() or None,
            allow_unknown_type=args.allow_unknown_type,
        )
    except (RuntimeError, FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    if not additions:
        print("No unmapped class.* tags found; nothing changed.")
        return 0
    if additions[0].get("reuse"):
        _print_reuse(additions[0], env_name, env_dir)
        return 0
    for item in additions:
        verb = "ADDED" if item["added"] else "APPLIED EXISTING"
        suffix = " (full-redaction REVIEW stub)" if item["added"] else ""
        print(
            f'{verb} {item["label"]} -> gr_treatment={item["value"]} '
            f'-> {item["function"]}{suffix}'
        )
    source = promote_source(env_dir)
    rerun = rerun_command(env_name, bool(source))
    new = sorted({item["value"] for item in additions if item["added"]})
    if new and source:
        print("Review each stub's udf_body in shared/treatment_config.json (keep the "
              "redaction or write a type-appropriate mask), then carry the mask through "
              f"{source}; no {source} column needs the tag:")
        for value in new:
            print(f"  make materialize-treatment ENV={source} TREATMENT={value}")
        print(f"  make rehearse ENV={source}")
        print(f"  make promote-to ENV={env_name}")
        print(f"  {rerun}")
        print("Commit shared/treatment_config.json, shared/tag_vocabulary_registry.json "
              f"and envs/{source}/generated/.")
    else:
        print(f"Review the stubs, then run {rerun}. Commit shared/treatment_config.json"
              + (" and shared/tag_vocabulary_registry.json." if new else "."))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
