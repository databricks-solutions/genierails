#!/usr/bin/env python3
"""Split a generated ABAC draft into shared account, env governance, and workspace tfvars."""

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


ACCOUNT_KEYS = (
    "groups",
    "group_members",
    "tag_policies",
)

DATA_ACCESS_KEYS = (
    "groups",
    "tag_assignments",
    "fgac_policies",
)

WORKSPACE_KEYS = (
    "groups",
    # New multi-space format.
    "genie_space_configs",
    # Legacy single-space keys (kept for backward compatibility with old generated configs).
    "genie_space_title",
    "genie_space_description",
    "genie_sample_questions",
    "genie_instructions",
    "genie_benchmarks",
    "genie_sql_filters",
    "genie_sql_expressions",
    "genie_sql_measures",
    "genie_join_specs",
)

IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_hcl(path: Path) -> dict:
    with open(path) as f:
        return hcl2.load(f)


def quote_key(key: str) -> str:
    if IDENT_RE.match(key):
        return key
    return json.dumps(key)


def render_scalar(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value)


def render_value(value, indent: int = 0) -> str:
    pad = " " * indent
    if isinstance(value, dict):
        if not value:
            return "{}"
        lines = ["{"]
        for key, item in value.items():
            rendered = render_value(item, indent + 2)
            lines.append(f"{pad}  {quote_key(key)} = {rendered}")
        lines.append(f"{pad}}}")
        return "\n".join(lines)
    if isinstance(value, list):
        if not value:
            return "[]"
        if all(not isinstance(item, (dict, list)) for item in value):
            return "[" + ", ".join(render_scalar(item) for item in value) + "]"
        lines = ["["]
        for item in value:
            rendered = render_value(item, indent + 2)
            lines.append(f"{pad}  {rendered},")
        lines.append(f"{pad}]")
        return "\n".join(lines)
    return render_scalar(value)


def merge_groups(existing: dict, new: dict) -> dict:
    merged = dict(existing or {})
    for name, cfg in (new or {}).items():
        old_cfg = merged.get(name, {})
        old_desc = (
            old_cfg.get("description", "") if isinstance(old_cfg, dict) else ""
        )
        new_desc = cfg.get("description", "") if isinstance(cfg, dict) else ""
        merged[name] = {"description": old_desc or new_desc}
    return merged


def merge_group_members(existing: dict, new: dict) -> dict:
    merged = {k: list(v) for k, v in (existing or {}).items()}
    for group, members in (new or {}).items():
        current = merged.setdefault(group, [])
        for member in members:
            if member not in current:
                current.append(member)
    return merged


def merge_tag_policies(existing: list, new: list) -> list:
    """Union tag policies by key; new env's definition wins on values.

    Existing keys from other environments are preserved so that promoting
    an older env never silently drops policies added by a newer environment.
    """
    merged = {tp["key"]: tp for tp in (existing or [])}
    for tp in (new or []):
        merged[tp["key"]] = tp
    return list(merged.values())


def build_account_config(full_cfg: dict, existing_cfg: dict | None) -> dict:
    existing_cfg = existing_cfg or {}
    cfg: dict = {}

    groups = merge_groups(
        existing_cfg.get("groups", {}),
        full_cfg.get("groups", {}),
    )
    if groups:
        cfg["groups"] = groups

    group_members = merge_group_members(
        existing_cfg.get("group_members", {}),
        full_cfg.get("group_members", {}),
    )
    if group_members:
        cfg["group_members"] = group_members

    tag_policies = merge_tag_policies(
        existing_cfg.get("tag_policies", []),
        full_cfg.get("tag_policies", []),
    )
    if tag_policies:
        cfg["tag_policies"] = tag_policies

    return cfg


def reconcile_tag_policy_values(account_cfg: dict, data_access_cfg: dict) -> None:
    """Ensure every tag_assignment value appears in the account-layer tag policy.

    The autofix in generate_abac.py runs on the full generated ABAC before
    splitting, but edge cases (per-space assembly, regex mismatches) can leave
    a tag_assignment in the data_access layer whose value is absent from the
    account-layer policy.  Catching this here prevents silent Terraform errors
    at apply time ('Tag value X is not an allowed value for tag policy key Y').
    """
    policies = {tp["key"]: tp for tp in account_cfg.get("tag_policies", [])}
    for ta in data_access_cfg.get("tag_assignments", []):
        key = ta.get("tag_key")
        val = ta.get("tag_value")
        if not key or not val or key not in policies:
            continue
        allowed = policies[key].get("values", [])
        if val not in allowed:
            policies[key]["values"] = allowed + [val]
            print(f"  [SPLIT-REPAIR] Added '{val}' to account tag_policy '{key}'")


def _strip_var_refs(space_cfg: dict) -> dict:
    """Remove ${var.*} string values from a genie_space_configs entry.

    The LLM sometimes hallucinates Terraform variable references for optional
    list fields (benchmarks, sql_filters, etc.).  These are illegal in .tfvars
    files, so strip them.
    """
    return {
        k: v for k, v in space_cfg.items()
        if not (isinstance(v, str) and "${var." in v)
    }


def build_workspace_config(full_cfg: dict) -> dict:
    cfg: dict = {}
    for key in WORKSPACE_KEYS:
        if key not in full_cfg:
            continue
        value = full_cfg[key]
        if value in ("", [], {}):
            continue
        # Sanitize genie_space_configs: strip ${var.*} refs from each space
        if key == "genie_space_configs" and isinstance(value, dict):
            value = {
                name: _strip_var_refs(sc) if isinstance(sc, dict) else sc
                for name, sc in value.items()
            }
        # Also strip top-level legacy keys with ${var.*} values
        if isinstance(value, str) and "${var." in value:
            continue
        cfg[key] = value
    return cfg


def build_data_access_config(full_cfg: dict) -> dict:
    cfg: dict = {}
    for key in DATA_ACCESS_KEYS:
        if key not in full_cfg:
            continue
        value = full_cfg[key]
        if value in ("", [], {}):
            continue
        cfg[key] = value
    return cfg


def write_tfvars(
    path: Path,
    config: dict,
    header: str,
    ordered_keys: tuple[str, ...],
):
    path.parent.mkdir(parents=True, exist_ok=True)
    pieces = [header.strip(), ""]
    handled = set()

    for key in ordered_keys:
        if key not in config:
            continue
        handled.add(key)
        pieces.append(f"{key} = {render_value(config[key])}")
        pieces.append("")

    for key in config:
        if key in handled:
            continue
        pieces.append(f"{key} = {render_value(config[key])}")
        pieces.append("")

    path.write_text("\n".join(pieces).rstrip() + "\n")


def main():
    if len(sys.argv) != 5:
        print(
            "Usage: python scripts/split_abac_config.py "
            "generated/abac.auto.tfvars "
            "envs/account/abac.auto.tfvars "
            "envs/<env>/data_access/abac.auto.tfvars "
            "envs/dev/abac.auto.tfvars"
        )
        sys.exit(1)

    source_path = Path(sys.argv[1])
    account_path = Path(sys.argv[2])
    data_access_path = Path(sys.argv[3])
    workspace_path = Path(sys.argv[4])

    if not source_path.exists():
        print(f"ERROR: source file not found: {source_path}")
        sys.exit(1)

    full_cfg = load_hcl(source_path)
    existing_account_cfg = (
        load_hcl(account_path) if account_path.exists() else None
    )

    account_cfg = build_account_config(full_cfg, existing_account_cfg)
    data_access_cfg = build_data_access_config(full_cfg)
    workspace_cfg = build_workspace_config(full_cfg)

    # Safety net: ensure all tag_assignment values are in the account-layer
    # tag policies before we write.  Catches edge cases missed by generate_abac.py's
    # autofix (e.g. per-space assembly, regex edge cases).
    reconcile_tag_policy_values(account_cfg, data_access_cfg)

    account_header = """
# ============================================================================
# ACCOUNT-OWNED ABAC CONFIG
# ============================================================================
# Generated from the workspace draft and merged into shared account state.
# Owns groups, optional group membership, and tag policy definitions.
# Tag policies are account-scoped and shared across all workspace environments.
# ============================================================================
"""

    data_access_header = """
# ============================================================================
# DATA-ACCESS-OWNED ABAC CONFIG
# ============================================================================
# Generated from the workspace draft for this environment's governance layer.
# Owns group references, tag assignments, and FGAC policies.
# Tag policy definitions live in envs/account — not here.
# ============================================================================
"""

    workspace_header = """
# ============================================================================
# WORKSPACE-OWNED ABAC CONFIG
# ============================================================================
# Generated from the workspace draft for this environment only.
# Owns workspace lookups/ACLs and Genie config only.
# ============================================================================
"""

    write_tfvars(
        account_path,
        account_cfg,
        account_header,
        ACCOUNT_KEYS,
    )
    write_tfvars(
        data_access_path,
        data_access_cfg,
        data_access_header,
        DATA_ACCESS_KEYS,
    )
    write_tfvars(
        workspace_path,
        workspace_cfg,
        workspace_header,
        WORKSPACE_KEYS,
    )

    print(f"  Wrote shared account config: {account_path}")
    print(f"  Wrote env-scoped data-access config: {data_access_path}")
    print(f"  Wrote workspace config:      {workspace_path}")


if __name__ == "__main__":
    main()
