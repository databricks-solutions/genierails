#!/usr/bin/env python3
"""Refresh only gr_treatment facts from live native class.* tags."""

from __future__ import annotations

import argparse
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
    footprint_table_refs,
    load_auth_config,
)
from treatment_derivation import (  # noqa: E402
    collapse_sensitivity_assignments,
    derive_treatment_assignments,
    load_treatment_config,
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
    declared = list(runtime.get("uc_tables") or [])
    declared.extend(runtime.get("declared_footprint") or [])
    for space in runtime.get("genie_spaces") or []:
        declared.extend(space.get("declared_footprint") or space.get("uc_tables") or [])
    table_refs = footprint_table_refs(discover_agent_footprint(declared_footprint=declared))

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
    assignments = list(promoted.get("tag_assignments") or []) + native_assignments
    refreshed = derive_treatment_assignments(assignments, config)
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
