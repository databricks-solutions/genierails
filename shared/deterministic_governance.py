"""Pure schema validation for deterministic-governance environment settings.

Step 1 only validates and resolves configuration.  No value in this module is
consumed by generation or Terraform resources yet.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


ACCESS_LEVELS = {"raw", "partial", "full"}

# Version names are the public schema contract from design section 2.  The
# implementation and typed variants land with rollout step 3.
PARTIAL_VERSIONS: dict[str, frozenset[str]] = {
    # Deterministic-library treatment name used by design section 2.  Its
    # implementation lands in step 3; ssn_last4 remains a known legacy name.
    "ssn": frozenset({"hmac_sha256", "last4"}),
    "redact": frozenset({"redacted"}),
    "compensation_redacted": frozenset({"rounded"}),
    "ssn_last4": frozenset({"hmac_sha256", "last4"}),
    "card_last4": frozenset({"last4"}),
    "account_last4": frozenset({"last4"}),
    "email_partial": frozenset({"partial", "raw"}),
    "phone_partial": frozenset({"last4"}),
    "name_partial": frozenset({"initials"}),
    "date_year": frozenset({"year"}),
    "tfn_partial": frozenset({"hmac_sha256", "last4"}),
    "medicare_partial": frozenset({"hmac_sha256", "last4"}),
    "bsb_partial": frozenset({"last4"}),
    "aadhaar_partial": frozenset({"hmac_sha256", "last4"}),
    "generic_partial": frozenset({"redacted", "prefix_3", "raw"}),
    "round_amount": frozenset({"rounded"}),
}


def _known_treatments(registry_path: Path | None = None) -> set[str]:
    path = registry_path or Path(__file__).with_name("treatment_config.json")
    data = json.loads(path.read_text())
    return {item["value"] for item in data.get("treatments", [])} | set(PARTIAL_VERSIONS)


def resolve_precedence(
    *,
    column: str,
    treatment: str,
    group: str,
    library_default: str,
    column_overrides: Mapping[str, Mapping[str, str]] | None = None,
    treatment_versions: Mapping[str, Mapping[str, str]] | None = None,
    tier_access_overrides: Mapping[str, Mapping[str, str]] | None = None,
) -> str:
    """Return the effective access/version value, highest precedence first."""
    column_rule = (column_overrides or {}).get(column, {})
    if "treatment" in column_rule:
        return column_rule["treatment"]
    if "partial" in column_rule:
        return column_rule["partial"]
    treatment_rule = (treatment_versions or {}).get(treatment, {})
    if "partial" in treatment_rule:
        return treatment_rule["partial"]
    group_rule = (tier_access_overrides or {}).get(treatment, {})
    if group in group_rule:
        return group_rule[group]
    return library_default


def validate_config(cfg: Mapping[str, Any]) -> list[str]:
    """Return all deterministic-governance schema errors in an env config."""
    errors: list[str] = []
    tiers = cfg.get("access_tier_groups", [])
    if not isinstance(tiers, list) or not all(isinstance(x, str) and x for x in tiers):
        errors.append("access_tier_groups must be an ordered list of non-empty group names")
        tiers = []
    elif len(tiers) != len(set(tiers)):
        errors.append("access_tier_groups must not contain duplicate groups")

    known = _known_treatments()
    versions = cfg.get("treatment_versions", {})
    if not isinstance(versions, dict):
        errors.append("treatment_versions must be a map")
        versions = {}
    for treatment, rule in versions.items():
        label = f'treatment_versions["{treatment}"]'
        if treatment not in known:
            errors.append(f"{label} names unknown treatment {treatment!r}")
        if not isinstance(rule, dict) or set(rule) != {"partial"}:
            errors.append(f"{label} must contain only partial")
        elif rule["partial"] not in PARTIAL_VERSIONS.get(treatment, frozenset()):
            errors.append(f"{label}.partial names unknown version {rule['partial']!r}")

    tier_overrides = cfg.get("tier_access_overrides", {})
    if not isinstance(tier_overrides, dict):
        errors.append("tier_access_overrides must be a map")
        tier_overrides = {}
    for treatment, rules in tier_overrides.items():
        label = f'tier_access_overrides["{treatment}"]'
        if treatment not in known:
            errors.append(f"{label} names unknown treatment {treatment!r}")
        if not isinstance(rules, dict):
            errors.append(f"{label} must be a map of group to raw, partial, or full")
            continue
        for group, access in rules.items():
            if group not in tiers:
                errors.append(f"{label} names unknown group {group!r}")
            if access not in ACCESS_LEVELS:
                errors.append(f"{label}[{group!r}] must be raw, partial, or full")
            elif group in tiers:
                index = tiers.index(group)
                if access == "partial" and len(tiers) < 3:
                    errors.append(f"{label}[{group!r}] names partial, but no partial tier exists")
                if access == "full" and len(tiers) < 2:
                    errors.append(f"{label}[{group!r}] names full, but no full tier exists")

    columns = cfg.get("column_overrides", {})
    if not isinstance(columns, dict):
        errors.append("column_overrides must be a map")
        columns = {}
    for column, rule in columns.items():
        label = f'column_overrides["{column}"]'
        if not isinstance(column, str) or len(column.split(".")) != 4 or not all(column.split(".")):
            errors.append(f"{label} key must be cat.sch.tbl.col")
        if not isinstance(rule, dict) or not rule:
            errors.append(f"{label} must contain partial or treatment")
            continue
        if "full" in rule:
            errors.append(f"{label} may not set full; the full version is fixed")
        extra = set(rule) - {"partial", "treatment", "full"}
        if extra or ({"partial", "treatment"} <= set(rule)):
            errors.append(f"{label} must set exactly one of partial or treatment")
        if "treatment" in rule and rule["treatment"] not in known:
            errors.append(f"{label}.treatment names unknown treatment {rule['treatment']!r}")
        if "partial" in rule:
            valid_versions = set().union(*PARTIAL_VERSIONS.values())
            if rule["partial"] not in valid_versions:
                errors.append(f"{label}.partial names unknown version {rule['partial']!r}")

    filters = cfg.get("row_filters", [])
    if not isinstance(filters, list):
        errors.append("row_filters must be a list")
        filters = []
    seen: dict[tuple[str, str], Any] = {}
    for index, rule in enumerate(filters):
        label = f"row_filters[{index}]"
        if not isinstance(rule, dict) or set(rule) != {"table", "column", "values_by_group"}:
            errors.append(f"{label} must contain exactly table, column, and values_by_group")
            continue
        if not isinstance(rule["table"], str) or len(rule["table"].split(".")) != 3:
            errors.append(f"{label}.table must be cat.sch.tbl")
        if not isinstance(rule["column"], str) or not rule["column"]:
            errors.append(f"{label}.column must be a non-empty string")
        values = rule["values_by_group"]
        if not isinstance(values, dict):
            errors.append(f"{label}.values_by_group must be a map")
            continue
        for group, literals in values.items():
            if group not in tiers:
                errors.append(f"{label}.values_by_group names unknown group {group!r}")
            if not isinstance(literals, list) or not all(isinstance(v, str) for v in literals):
                errors.append(f"{label}.values_by_group[{group!r}] must be a list of strings")
        key = (rule["table"], rule["column"])
        if key in seen and seen[key] != values:
            errors.append(f"{label} conflicts with an earlier row-filter rule for {key[0]}.{key[1]}")
        seen.setdefault(key, values)

    require_acls = cfg.get("require_acl_groups", False)
    if not isinstance(require_acls, bool):
        errors.append("require_acl_groups must be a boolean")
    spaces = cfg.get("genie_spaces", [])
    if isinstance(spaces, list):
        for index, space in enumerate(spaces):
            if not isinstance(space, dict):
                continue
            name = space.get("name") or space.get("genie_space_id") or str(index)
            missing = "acl_groups" not in space or space["acl_groups"] is None
            if require_acls is True and missing:
                errors.append(f"agent {name} has no acl_groups — list the groups that may run it")
            if not missing and (not isinstance(space["acl_groups"], list) or not all(isinstance(group, str) for group in space["acl_groups"])):
                errors.append(f"agent {name} acl_groups must be a list of strings")
    return errors
