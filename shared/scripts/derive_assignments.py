#!/usr/bin/env python3
"""Refresh only gr_treatment facts from live native class.* tags."""

from __future__ import annotations

import argparse
import contextlib
import io
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
    fetch_tables_from_databricks,
    footprint_contains_column,
    footprint_table_refs,
    load_auth_config,
    scope_ddl_to_footprint,
)
from treatment_derivation import (  # noqa: E402
    collapse_sensitivity_assignments,
    derive_treatment_model,
    load_treatment_config,
)
from scripts.coverage_gate import write_refresh_record  # noqa: E402
from scripts.footprint import FootprintError, resolve_footprint  # noqa: E402
from scripts.sticky_governance import load_governed_tables, record  # noqa: E402


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


def refresh_fetched_ddl(table_refs: list[str], runtime: dict, footprint: list[dict], ddl_out: Path) -> None:
    """Write the footprint's live DDL, as make generate does, for the coverage check.

    Promoted envs never ran generate, so this is the only DDL the first-exposure
    check has there. Any read failure raises: the gate must not judge stale DDL.
    """
    captured = io.StringIO()
    try:
        with contextlib.redirect_stdout(captured):
            ddl_text, _catalog_schemas = fetch_tables_from_databricks(table_refs, runtime)
    except SystemExit as exc:
        raise RuntimeError(
            "Could not fetch DDL for the governed footprint: " + captured.getvalue().strip()
        ) from exc
    except Exception as exc:
        raise RuntimeError(f"Could not fetch DDL for the governed footprint: {exc}") from exc
    text = scope_ddl_to_footprint(ddl_text, footprint) + "\n"
    if not ddl_out.is_file() or ddl_out.read_text() != text:
        ddl_out.parent.mkdir(parents=True, exist_ok=True)
        ddl_out.write_text(text)


def _governed_footprint(auth_path: Path, env_path: Path) -> tuple[dict, list[dict], list[str]]:
    runtime = load_auth_config(auth_path, env_path)
    # The live read must target this env's workspace. Without a host the SDK
    # falls back to ambient configuration (environment, ~/.databrickscfg),
    # which may be another workspace or the account console.
    if not str(runtime.get("databricks_workspace_host") or "").strip():
        raise RuntimeError(
            f"{auth_path} does not set databricks_workspace_host; refusing a live read of "
            "Unity Catalog with ambient credentials"
        )
    declared = resolve_footprint(env_path.parent, env_file=env_path)
    uc_catalog = str(runtime.get("uc_catalog") or "").strip()
    if uc_catalog:
        declared = [
            table if len(str(table).split(".")) >= 3 else f"{uc_catalog}.{table}"
            for table in declared
        ]
    declared.extend(runtime.get("declared_footprint") or [])
    # Deterministic envs keep every governed table in scope after its agents
    # drop it, so its treatments still come from current tags (until ungovern).
    declared.extend(load_governed_tables(env_path.parent))
    for space in runtime.get("genie_spaces") or []:
        declared.extend(space.get("declared_footprint") or [])
    footprint = discover_agent_footprint(declared_footprint=declared)
    return runtime, footprint, footprint_table_refs(footprint)


def refresh_ddl_only(auth_path: Path, env_path: Path, ddl_out: Path) -> None:
    """Refresh only the live DDL, for envs whose tags come from make generate."""
    runtime, footprint, table_refs = _governed_footprint(auth_path, env_path)
    refresh_fetched_ddl(table_refs, runtime, footprint, ddl_out)


def derive_assignments(
    config_path: Path, auth_path: Path, env_path: Path, ddl_out: Path | None = None,
) -> int:
    """Atomically replace only the promoted config's tag_assignments section.

    With ``ddl_out``, also refresh the fetched DDL the coverage check reads.
    """
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

    runtime, footprint, table_refs = _governed_footprint(auth_path, env_path)

    native = _fetch_live_classification_source(table_refs, runtime, require_native=True)
    # require_native guarantees a non-empty source; retain this assertion as a
    # second fail-closed boundary for injected/test implementations.
    if native is None or not native.has_native_data():
        raise NativeClassificationRequiredError(
            "Native classification returned no class.* findings; refusing to replace assignments"
        )
    if ddl_out is not None:
        refresh_fetched_ddl(table_refs, runtime, footprint, ddl_out)

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
    record(env_path.parent, refreshed, config.tag_key)
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
    parser.add_argument("--write-ddl", type=Path, metavar="PATH",
                        help="also write the footprint's live DDL here (ddl/_fetched.sql) for the coverage check")
    parser.add_argument("--refresh-record", type=Path, metavar="PATH",
                        help="after a successful live refresh, record it here for the coverage check "
                             "(removed first, so a failed refresh leaves none)")
    parser.add_argument("--ddl-only", action="store_true",
                        help="refresh only the live DDL (envs without native classification); "
                             "tag_assignments are left as make generate wrote them")
    args = parser.parse_args(argv)
    if args.ddl_only and not args.write_ddl:
        parser.error("--ddl-only requires --write-ddl")
    if args.refresh_record:
        args.refresh_record.unlink(missing_ok=True)
    try:
        if args.ddl_only:
            refresh_ddl_only(Path(args.auth_file), Path(args.env_file), args.write_ddl)
            message = f"Refreshed live DDL ({args.write_ddl}); tag_assignments were not changed."
        else:
            count = derive_assignments(
                Path(args.config), Path(args.auth_file), Path(args.env_file), ddl_out=args.write_ddl,
            )
            message = f"Derived {count} gr_treatment assignment(s); reviewed rules were not changed."
        if args.refresh_record:
            write_refresh_record(
                args.refresh_record,
                mode="ddl" if args.ddl_only else "full",
                ddl_path=args.write_ddl,
                config_path=None if args.ddl_only else Path(args.config),
            )
    except (RuntimeError, NativeClassificationRequiredError, FootprintError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
