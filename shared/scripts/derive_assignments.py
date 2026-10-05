#!/usr/bin/env python3
"""Refresh only gr_treatment facts from live native class.* tags."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import hcl2

SHARED = Path(__file__).resolve().parents[1]
if str(SHARED) not in sys.path:
    sys.path.insert(0, str(SHARED))

from generate_abac import (  # noqa: E402
    NativeClassificationRequiredError,
    _fetch_live_classification_source,
    _find_bracket_section,
    _render_tag_assignment_block,
    _replace_bracket_section,
    discover_agent_footprint,
    footprint_contains_column,
    footprint_table_refs,
    load_auth_config,
)
from treatment_derivation import (  # noqa: E402
    collapse_sensitivity_assignments,
    derive_treatment_model,
    load_treatment_config,
)
from scripts.footprint import resolve_footprint  # noqa: E402


def _retained_promoted_assignments(assignments: list[dict], config) -> list[dict]:
    """Discard all stale column sensitivity facts while preserving other facts."""
    sensitivity_keys = {
        key for treatment in config.treatments for key, _value in treatment.sources
    }
    return [
        dict(item) for item in assignments
        if not (
            item.get("entity_type") == "columns"
            and item.get("tag_key") in sensitivity_keys | {config.tag_key}
        )
    ]


def _assert_promoted_masks_cover(assignments: list[dict], promoted: dict, tag_key: str) -> None:
    """Fail closed unless every derived treatment is covered by a reviewed mask."""
    covered: set[tuple[str, str]] = set()
    pattern = re.compile(
        rf"hasTagValue\(\s*['\"]{re.escape(tag_key)}['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)"
    )
    for policy in promoted.get("fgac_policies") or []:
        if policy.get("policy_type") != "POLICY_TYPE_COLUMN_MASK":
            continue
        catalog = policy.get("catalog", "")
        for value in pattern.findall(policy.get("match_condition", "") or ""):
            covered.add((catalog, value))

    missing = []
    for item in assignments:
        if item.get("entity_type") != "columns" or item.get("tag_key") != tag_key:
            continue
        catalog = item.get("entity_name", "").split(".", 1)[0]
        key = (catalog, item.get("tag_value", ""))
        if key not in covered:
            missing.append(f"{item.get('entity_name')} ({tag_key}={key[1]}, catalog={catalog})")
    if missing:
        raise RuntimeError(
            "Promoted rules have no matching column-mask policy for derived treatment(s): "
            + ", ".join(missing)
        )


def derive_assignments(config_path: Path, auth_path: Path, env_path: Path) -> int:
    """Atomically replace only the promoted config's tag_assignments section."""
    if not config_path.is_file():
        raise RuntimeError(
            f"Promoted config not found: {config_path}. Run `make promote` first."
        )

    original = config_path.read_text()
    try:
        promoted = hcl2.loads(original)
    except Exception as exc:
        raise RuntimeError(f"Cannot parse promoted config {config_path}: {exc}") from exc
    if _find_bracket_section(original, "tag_assignments") is None:
        raise RuntimeError(f"Promoted config {config_path} has no tag_assignments section")

    runtime = load_auth_config(auth_path, env_path)
    declared = resolve_footprint(env_path.parent)
    uc_catalog = str(runtime.get("uc_catalog") or "").strip()
    if uc_catalog:
        declared = [
            table if len(str(table).split(".")) >= 3 else f"{uc_catalog}.{table}"
            for table in declared
        ]
    declared.extend(runtime.get("declared_footprint") or [])
    for space in runtime.get("genie_spaces") or []:
        declared.extend(space.get("declared_footprint") or [])
    footprint = discover_agent_footprint(declared_footprint=declared)
    table_refs = footprint_table_refs(footprint)

    native = _fetch_live_classification_source(table_refs, runtime, require_native=True)
    # require_native guarantees a non-empty source; retain this assertion as a
    # second fail-closed boundary for injected/test implementations.
    if native is None or not native.has_native_data():
        raise NativeClassificationRequiredError(
            "Native classification returned no class.* findings; refusing to replace assignments"
        )

    config = load_treatment_config()
    unmapped = native.unmapped_columns(sorted(native.classified_columns()))
    if unmapped:
        details = ", ".join(f"{column}=class.{semantic}" for column, semantic in unmapped)
        raise NativeClassificationRequiredError(
            f"Native classification contains unmapped class.* findings: {details}"
        )
    native_assignments = [
        finding.as_assignment()
        for finding in native.findings_for(sorted(native.classified_columns()))
    ]
    native_assignments = collapse_sensitivity_assignments(native_assignments, config)
    valid_treatments = set(config.values)
    override_assignments = []
    for override in promoted.get("treatment_overrides") or []:
        column = str(override.get("entity_name") or "")
        treatment = str(override.get("treatment") or "")
        if treatment not in valid_treatments:
            raise RuntimeError(
                f"Promoted treatment override for {column or '<missing column>'} "
                f"uses unknown treatment {treatment!r}"
            )
        if not footprint_contains_column(footprint, column):
            print(
                f"WARNING: Skipping treatment override for {column}: table/column "
                "is no longer in the governed footprint",
                file=sys.stderr,
            )
            continue
        override_assignments.append({
            "entity_type": "columns",
            "entity_name": column,
            "tag_key": config.tag_key,
            "tag_value": treatment,
        })
    retained = _retained_promoted_assignments(
        list(promoted.get("tag_assignments") or []), config
    )
    # Use the exact treatment transform used by generate. Only its assignments
    # are consumed; its rebuilt policy model is intentionally discarded.
    derived, _changes = derive_treatment_model(
        {"tag_assignments": retained + native_assignments + override_assignments}, config
    )
    sensitivity_keys = {
        key for treatment in config.treatments for key, _value in treatment.sources
    }
    # Match generate's native-authoritative finalization: class.* findings are
    # source facts, while only the single gr_treatment assignment is persisted
    # for enforcement. Keeping intermediate pii_level/pci_level assignments
    # makes the promoted rules invalid because those source-family tag policies
    # are intentionally not promoted.
    refreshed = [
        item for item in derived["tag_assignments"]
        if not (
            item.get("entity_type") == "columns"
            and item.get("tag_key") in sensitivity_keys
        )
    ]
    _assert_promoted_masks_cover(refreshed, promoted, config.tag_key)
    updated = _replace_bracket_section(
        original,
        "tag_assignments",
        [_render_tag_assignment_block(item) for item in refreshed],
    )
    if updated == original:
        return 0
    config_path.write_text(updated)
    return sum(1 for item in refreshed if item.get("tag_key") == config.tag_key)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Re-derive only tag_assignments from live native class.* tags (no LLM)."
    )
    parser.add_argument("--config", default="generated/abac.auto.tfvars")
    parser.add_argument("--auth-file", default="auth.auto.tfvars")
    parser.add_argument("--env-file", default="env.auto.tfvars")
    args = parser.parse_args(argv)
    try:
        count = derive_assignments(Path(args.config), Path(args.auth_file), Path(args.env_file))
    except (RuntimeError, NativeClassificationRequiredError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"Derived {count} gr_treatment assignment(s); reviewed rules were not changed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
