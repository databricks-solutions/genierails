"""Pure schema validation for deterministic-governance environment settings.

Step 1 only validates and resolves configuration.  No value in this module is
consumed by generation or Terraform resources yet.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence


ACCESS_LEVELS = {"raw", "partial", "full"}
NEVER_RAW_TREATMENTS = frozenset({
    "card_security_code", "card_pin", "card_track_data", "secret",
})

# Public treatment vocabulary. mask_library.json uses these names directly, so
# every library treatment is configurable through treatment_versions.
PARTIAL_VERSIONS: dict[str, frozenset[str]] = {
    "ssn": frozenset({"last4"}),
    "redact": frozenset({"redacted"}),
    "compensation_redacted": frozenset({"rounded"}),
    "ssn_last4": frozenset({"last4"}),
    "card_last4": frozenset({"last4"}),
    "account_last4": frozenset({"last4"}),
    "email_partial": frozenset({"partial"}),
    "phone_partial": frozenset({"last4"}),
    "name_partial": frozenset({"initials"}),
    "date_year": frozenset({"year"}),
    "tfn_partial": frozenset({"last4"}),
    "medicare_partial": frozenset({"last4"}),
    "bsb_partial": frozenset({"last4"}),
    "aadhaar_partial": frozenset({"last4"}),
    "generic_partial": frozenset({"redacted", "prefix_3"}),
    "round_amount": frozenset({"rounded"}),
    # Mask-library treatments with no step-1 name; see mask_library.json.
    "identifier": frozenset(),
    "age": frozenset({"age_band_10"}),
    "credit_score": frozenset({"credit_score_band_50"}),
    "ip_address": frozenset({"ip_network"}),
    "mac_address": frozenset({"mac_vendor"}),
    "url": frozenset({"url_domain"}),
    "location": frozenset({"location_1dp"}),
    **{treatment: frozenset({"redacted"}) for treatment in NEVER_RAW_TREATMENTS},
}
PARTIAL_VERSIONS = {
    treatment: versions | {"redacted"}
    for treatment, versions in PARTIAL_VERSIONS.items()
}
VERSION_NAMES = frozenset().union(*PARTIAL_VERSIONS.values())
# Deferred from v1: identifiers are redacted for tiers 2 and 3.
KEYED_HASH_UNAVAILABLE = "keyed hash is not available in this version; use redacted"


def _known_treatments(registry_path: Path | None = None) -> set[str]:
    path = registry_path or Path(__file__).with_name("treatment_config.json")
    data = json.loads(path.read_text())
    return {item["value"] for item in data.get("treatments", [])} | set(PARTIAL_VERSIONS)


@dataclass(frozen=True)
class AccessResolution:
    """One effective access decision; only partial access carries a version."""

    access: Literal["raw", "partial", "full"]
    version: str | None = None


def resolve_precedence(
    *,
    column: str,
    treatment: str,
    group: str | Sequence[str],
    library_default: str,
    access_tier_groups: Sequence[str],
    column_overrides: Mapping[str, Mapping[str, str]] | None = None,
    treatment_versions: Mapping[str, Mapping[str, str]] | None = None,
    tier_access_overrides: Mapping[str, Mapping[str, str]] | None = None,
    principal: str | None = None,
    deployer_principal: str | None = None,
    raw_exempt_principals: list[str] | None = None,
    never_raw: bool = False,
) -> AccessResolution:
    """Resolve access first, then resolve a version only for partial access.

    A principal in several configured groups receives the most privileged
    effective access. Principals outside every tier fail closed to full.
    ``never_raw`` is set when any class on the column is never raw, even if
    strictest-wins picked an ordinary treatment (see mask_library.resolve_class).
    """
    memberships = [group] if isinstance(group, str) else list(group)
    configured = list(access_tier_groups)
    member_groups = [candidate for candidate in configured if candidate in memberships]
    effective_treatment = (column_overrides or {}).get(column, {}).get("treatment", treatment)

    # Invalid or drifted names can never open access. validate_config reports
    # the schema error; direct resolver callers still fail closed.
    if treatment not in PARTIAL_VERSIONS or effective_treatment not in PARTIAL_VERSIONS:
        return AccessResolution("full")

    # Never-raw beats everything, including the deployer SP: nobody sees raw.
    if never_raw or treatment in NEVER_RAW_TREATMENTS or effective_treatment in NEVER_RAW_TREATMENTS:
        return AccessResolution("full")
    if deployer_principal is not None and principal == deployer_principal:
        return AccessResolution("raw")
    if principal is not None and (
        principal in (raw_exempt_principals or [])
    ):
        return AccessResolution("raw")
    if not member_groups:
        return AccessResolution("full")

    # Tier 1 is always raw and the last tier is always full. Overrides are
    # meaningful only for intermediate (partial) tier groups.
    if member_groups[0] == configured[0]:
        return AccessResolution("raw")
    group_rules = (tier_access_overrides or {}).get(effective_treatment, {})
    access_rank = {"raw": 0, "partial": 1, "full": 2}
    resolved_access = min(
        (
            "full" if member == configured[-1]
            else group_rules.get(member, "partial")
            for member in member_groups
        ),
        key=access_rank.__getitem__,
    )
    if resolved_access != "partial":
        return AccessResolution(resolved_access)

    # The remaining precedence chain selects only the partial version. It can
    # never loosen a raw/full access decision already made above.
    column_rule = (column_overrides or {}).get(column, {})
    if "partial" in column_rule:
        version = column_rule["partial"]
    else:
        treatment_rule = (treatment_versions or {}).get(effective_treatment, {})
        version = treatment_rule.get("partial", library_default)
    if version == "raw":
        return AccessResolution("raw")
    return AccessResolution("partial", version)


_FQN4 = re.compile(r"^[^.\s]+\.[^.\s]+\.[^.\s]+\.[^.\s]+$")


def _validate_ack_list(name: str, value: str | None, *, principal: bool) -> list[str]:
    if value is None or value == "":
        return []
    if not isinstance(value, str):
        expected = "cat.sch.tbl.col:principal,..." if principal else "cat.sch.tbl.col,..."
        return [f"{name} must use {expected}"]
    errors: list[str] = []
    for item in value.split(","):
        parts = item.split(":", 1) if principal else [item]
        valid = bool(_FQN4.fullmatch(parts[0]))
        if principal:
            valid = valid and len(parts) == 2 and bool(parts[1]) and parts[1].strip() == parts[1]
        if not valid:
            expected = "cat.sch.tbl.col:principal,..." if principal else "cat.sch.tbl.col,..."
            errors.append(f"{name} must use {expected}")
            break
    return errors


def validate_config(
    cfg: Mapping[str, Any], *, ack_unclassified: str | None = None,
    ack_weaken: str | None = None,
) -> list[str]:
    """Return all deterministic-governance schema errors in an env config."""
    errors: list[str] = []
    governance_mode = cfg.get("governance_mode", "legacy")
    if not isinstance(governance_mode, str) or governance_mode not in {"legacy", "deterministic"}:
        errors.append("governance_mode must be legacy or deterministic")
    raw_exempt = cfg.get("raw_exempt_principals", [])
    if not isinstance(raw_exempt, list) or not all(
            isinstance(x, str) and x.strip() for x in raw_exempt):
        errors.append("raw_exempt_principals must be a list of non-empty principal names")
    elif any("@" in principal for principal in raw_exempt):
        errors.append(
            "raw_exempt_principals must name account groups; user emails cannot be verified. "
            "UUID/hex-shaped names are allowed because they may be legitimate group display names; "
            "live verification refuses them only when no account group with that exact name exists")
    tiers = cfg.get("access_tier_groups", [])
    if not isinstance(tiers, list) or not all(isinstance(x, str) and x.strip() for x in tiers):
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
        if not isinstance(treatment, str) or treatment not in known:
            errors.append(f"{label} names unknown treatment {treatment!r}")
        if not isinstance(rule, dict) or set(rule) != {"partial"}:
            errors.append(f"{label} must contain only partial")
        elif not isinstance(rule["partial"], str):
            errors.append(f"{label}.partial must be a string")
        elif rule["partial"] == "raw":
            errors.append(f"{label}.partial may not be raw; use a column or tier access override")
        elif rule["partial"] == "hmac_sha256":
            errors.append(f"{label}.partial: {KEYED_HASH_UNAVAILABLE}")
        elif rule["partial"] not in PARTIAL_VERSIONS.get(treatment, frozenset()):
            errors.append(f"{label}.partial names unknown version {rule['partial']!r}")

    tier_overrides = cfg.get("tier_access_overrides", {})
    if not isinstance(tier_overrides, dict):
        errors.append("tier_access_overrides must be a map")
        tier_overrides = {}
    for treatment, rules in tier_overrides.items():
        label = f'tier_access_overrides["{treatment}"]'
        if not isinstance(treatment, str) or treatment not in known:
            errors.append(f"{label} names unknown treatment {treatment!r}")
        if not isinstance(rules, dict):
            errors.append(f"{label} must be a map of group to raw, partial, or full")
            continue
        for group, access in rules.items():
            if group not in tiers:
                errors.append(f"{label} names unknown group {group!r}")
            if not isinstance(access, str) or access not in ACCESS_LEVELS:
                errors.append(f"{label}[{group!r}] must be raw, partial, or full")
            elif group in tiers:
                if access == "partial" and len(tiers) < 3:
                    errors.append(f"{label}[{group!r}] names partial, but no partial tier exists")
                if access == "full" and len(tiers) < 2:
                    errors.append(f"{label}[{group!r}] names full, but no full tier exists")
                if treatment in NEVER_RAW_TREATMENTS and access == "raw":
                    errors.append(f"{label}[{group!r}] may not give raw access to never-raw treatment {treatment!r}")
                if group == tiers[0] and access != "raw":
                    errors.append(f"{label}[{group!r}] may not change tier 1 from raw")
                if group == tiers[-1] and len(tiers) > 1 and access != "full":
                    errors.append(f"{label}[{group!r}] may not change the last tier from full")

    columns = cfg.get("column_overrides", {})
    if not isinstance(columns, dict):
        errors.append("column_overrides must be a map")
        columns = {}
    for column, rule in columns.items():
        label = f'column_overrides["{column}"]'
        if not isinstance(column, str) or len(column.split(".")) != 4 or not all(column.split(".")):
            errors.append(f"{label} key must be cat.sch.tbl.col")
        if not isinstance(rule, dict) or not rule:
            errors.append(f"{label} must contain partial, treatment, or keep_current")
            continue
        if "full" in rule:
            errors.append(f"{label} may not set full; the full version is fixed")
        extra = set(rule) - {"partial", "treatment", "keep_current", "full"}
        choices = set(rule) & {"partial", "treatment", "keep_current"}
        if extra or len(choices) != 1:
            errors.append(f"{label} must set exactly one of partial, treatment, or keep_current")
        if "keep_current" in rule and rule["keep_current"] is not True:
            errors.append(f"{label}.keep_current must be true")
        if "treatment" in rule and (
            not isinstance(rule["treatment"], str) or rule["treatment"] not in known
        ):
            errors.append(f"{label}.treatment names unknown treatment {rule['treatment']!r}")
        # TODO(step 3): compare a treatment override with live class-derived
        # treatment data and refuse it unless the protection order is stricter.
        if "partial" in rule:
            valid_versions = VERSION_NAMES
            if not isinstance(rule["partial"], str):
                errors.append(f"{label}.partial must be a string")
            elif rule["partial"] == "hmac_sha256":
                errors.append(f"{label}.partial: {KEYED_HASH_UNAVAILABLE}")
            elif rule["partial"] not in valid_versions | {"raw"}:
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
        table = rule["table"]
        column_name = rule["column"]
        table_valid = (
            isinstance(table, str)
            and len(table.split(".")) == 3
            and all(table.split("."))
        )
        if not table_valid:
            errors.append(f"{label}.table must be cat.sch.tbl")
        if not isinstance(column_name, str) or not column_name:
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
            if tiers and group == tiers[0]:
                errors.append(f"{label}.values_by_group may not name tier-1 group {group!r}")
        if table_valid and isinstance(column_name, str) and column_name:
            key = (table, column_name)
            if key in seen and seen[key] != values:
                errors.append(f"{label} conflicts with an earlier row-filter rule for {key[0]}.{key[1]}")
            seen.setdefault(key, values)

    require_acls = cfg.get("require_acl_groups", False)
    if not isinstance(require_acls, bool):
        errors.append("require_acl_groups must be a boolean")
    spaces = cfg.get("genie_spaces", [])
    if not isinstance(spaces, list):
        errors.append("genie_spaces must be a list")
    else:
        for index, space in enumerate(spaces):
            if not isinstance(space, dict):
                errors.append(f"genie_spaces[{index}] must be an object")
                continue
            name = space.get("name") or space.get("genie_space_id") or str(index)
            missing = "acl_groups" not in space or space["acl_groups"] is None
            if require_acls is True and missing:
                errors.append(f"agent {name} has no acl_groups — list the groups that may run it")
            if not missing:
                acl_groups = space["acl_groups"]
                if not isinstance(acl_groups, list) or not all(
                    isinstance(group, str) and group for group in acl_groups
                ):
                    errors.append(f"agent {name} acl_groups must be a list of non-empty strings")
                elif len(acl_groups) != len(set(acl_groups)):
                    errors.append(f"agent {name} acl_groups must not contain duplicates")
            if "delete" in space and not isinstance(space["delete"], bool):
                errors.append(f"agent {name} delete must be a boolean")
    errors.extend(_validate_ack_list("ACK_UNCLASSIFIED", ack_unclassified, principal=False))
    errors.extend(_validate_ack_list("ACK_WEAKEN", ack_weaken, principal=True))
    return errors
