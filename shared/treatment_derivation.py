"""Derive one GenieRails-owned enforcement treatment per sensitive column."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path

CONFIG_PATH = Path(__file__).with_name("treatment_config.json")
LOGGER = logging.getLogger(__name__)

# A fallback mask is required when classification finds a sensitive column in a
# catalog for which the model emitted no mask.  It must mask fail-closed without
# making its borrowed principals authoritative for catalog access derivation.
DERIVED_TREATMENT_MASK_COMMENT_PREFIX = "GenieRails treatment"
ACL_NEUTRAL_FALLBACK_COMMENT = (
    f"{DERIVED_TREATMENT_MASK_COMMENT_PREFIX} fallback; principals are masking-only, "
    "not access scope"
)


@dataclass(frozen=True)
class Treatment:
    value: str
    masking_function: str
    sources: frozenset[tuple[str, str]]
    udf_signature: str = ""
    udf_body: str = ""
    class_labels: frozenset[str] = frozenset()


@dataclass(frozen=True)
class TreatmentConfig:
    tag_key: str
    description: str
    treatments: tuple[Treatment, ...]

    @property
    def values(self) -> list[str]:
        return [item.value for item in self.treatments]


def load_treatment_config(path: Path = CONFIG_PATH) -> TreatmentConfig:
    raw = json.loads(path.read_text())
    treatments = tuple(
        Treatment(
            value=item["value"],
            masking_function=item["masking_function"],
            sources=frozenset(tuple(source) for source in item["sources"]),
            udf_signature=item.get("udf_signature", ""),
            udf_body=item.get("udf_body", ""),
            class_labels=frozenset(item.get("class_labels", [])),
        )
        for item in raw["treatments"]
    )
    values = [item.value for item in treatments]
    sources = [source for item in treatments for source in item.sources]
    class_labels = [label for item in treatments for label in item.class_labels]
    if (len(values) != len(set(values)) or len(sources) != len(set(sources))
            or len(class_labels) != len(set(class_labels))):
        raise ValueError("Treatment values, source mappings, and class labels must be unique")
    return TreatmentConfig(raw["tag_key"], raw["description"], treatments)


# Treatments whose masking function operates on STRING values and is shaped for
# one specific identifier format (email/phone/SSN/card/...).  Applied to a
# free-text column these leak any *other* PII embedded in the text, so a
# free-text column holding one of these is escalated to full redaction.
# Numeric/date treatments (round_amount, compensation_redacted, date_year) are
# excluded: mask_redact is STRING-typed and would not bind to DECIMAL/DATE columns.
_FREE_TEXT_ESCALATION_EXCLUDED = frozenset(
    {"redact", "round_amount", "date_year", "compensation_redacted"}
)
_FREE_TEXT_ESCALATION_TARGET = "redact"

# Column-name tokens that mark a narrative / free-text column.
FREE_TEXT_NAME_TOKENS = frozenset({
    "note", "notes", "comment", "comments", "description", "desc", "remark",
    "remarks", "message", "messages", "msg", "narrative", "memo", "text",
    "freetext", "body", "summary", "feedback",
})


def is_free_text_column(entity_name: str, masking_function: str) -> bool:
    """Whether a column must be treated as free text for masking purposes.

    Heuristic (either signal is sufficient):
      1. Name signal: an underscore-delimited token of the column name is in
         ``FREE_TEXT_NAME_TOKENS`` (``free_text``, ``notes``, ``email_body``...).
      2. Category signal: the validator's column-category inference
         (``validate_abac._infer_column_categories``) yields only ``generic`` —
         i.e. the name carries no identifier semantics — while the treatment's
         masking function is format-specific and does not accept ``generic``
         (``validate_abac.FUNCTION_EXPECTED_CATEGORIES``).  This is exactly the
         condition the validator reports as a function/category mismatch.
    """
    column = entity_name.split(".")[-1].lower()
    if set(column.split("_")) & FREE_TEXT_NAME_TOKENS:
        return True
    # Lazy import: validate_abac imports this module at load time.
    from validate_abac import FUNCTION_EXPECTED_CATEGORIES, _infer_column_categories
    expected = FUNCTION_EXPECTED_CATEGORIES.get(masking_function)
    return (
        expected is not None
        and "generic" not in expected
        and _infer_column_categories(entity_name) == {"generic"}
    )


def resolve_treatment(findings: list[tuple[str, str]], config: TreatmentConfig) -> Treatment | None:
    """Return the strictest matching treatment; config order is precedence."""
    observed = set(findings)
    return next((item for item in config.treatments if item.sources & observed), None)


def collapse_sensitivity_assignments(
    assignments: list[dict], config: TreatmentConfig
) -> list[dict]:
    """Collapse same-key native findings with generation's configured precedence."""
    rank = {
        source: index
        for index, treatment in enumerate(config.treatments)
        for source in treatment.sources
    }
    grouped: dict[tuple[str, str], list[dict]] = {}
    for assignment in assignments:
        key = (assignment.get("entity_name", ""), assignment.get("tag_key", ""))
        grouped.setdefault(key, []).append(dict(assignment))

    collapsed: list[dict] = []
    for (_entity, tag_key), candidates in grouped.items():
        current = min(
            candidates,
            key=lambda item: rank.get(
                (item.get("tag_key", ""), item.get("tag_value", "")), len(rank)
            ),
        )
        if tag_key == "pii_level" and len({item.get("tag_value") for item in candidates}) > 1:
            current = dict(current)
            current["tag_value"] = "redacted_mixed"
        collapsed.append(current)
    return collapsed


def derive_treatment_model(
    cfg: dict,
    config: TreatmentConfig,
    *,
    capture_source_less_explicit: bool = False,
) -> tuple[dict, int]:
    """Collapse mapped column findings and rebuild masks on ``gr_treatment``.

    Non-column assignments and unmapped governance tags are preserved. All
    generated column masks are replaced, making the single treatment tag the
    only route by which a mask can match a column.
    """
    assignments = [dict(item) for item in (cfg.get("tag_assignments") or [])]
    mapped_sources = {source for item in config.treatments for source in item.sources}
    by_column: dict[str, list[tuple[str, str]]] = {}
    existing_treatments: dict[str, str] = {}
    retained: list[dict] = []
    for assignment in assignments:
        source = (assignment.get("tag_key", ""), assignment.get("tag_value", ""))
        if assignment.get("entity_type") == "columns" and source in mapped_sources:
            by_column.setdefault(assignment.get("entity_name", ""), []).append(source)
        if (
            assignment.get("entity_type") == "columns"
            and assignment.get("tag_key") == config.tag_key
        ):
            existing_treatments[assignment.get("entity_name", "")] = assignment.get("tag_value", "")
        # Sensitivity tags are owned by their detection sources and must remain
        # in the emitted model. Only this transform's prior column assignment is
        # replaced, so repeated derivation cannot accumulate treatment values.
        if not (
            assignment.get("entity_type") == "columns"
            and assignment.get("tag_key") == config.tag_key
        ):
            retained.append(assignment)

    derived: list[dict] = []
    used: dict[str, Treatment] = {}
    source_tags_by_catalog_treatment: dict[tuple[str, str], list[set[tuple[str, str]]]] = {}
    treatments_by_value = {item.value: item for item in config.treatments}
    treatment_rank = {
        item.value: index for index, item in enumerate(config.treatments)
    }
    # Overrides are reviewed, portable rules rather than environment facts.
    # Preserve already-materialized rules, and add only a genuinely stricter
    # explicit treatment observed alongside a native-derived treatment.  This
    # distinction prevents an ordinary derived gr_treatment from becoming a
    # sticky override on a later, native-only pass.
    overrides_by_column = {
        item.get("entity_name", ""): {
            "entity_name": item.get("entity_name", ""),
            "treatment": item.get("treatment", ""),
        }
        for item in (cfg.get("treatment_overrides") or [])
        if item.get("entity_name") and item.get("treatment") in treatments_by_value
    }
    for column in sorted(set(by_column) | set(existing_treatments)):
        findings = by_column.get(column, [])
        source_treatment = resolve_treatment(findings, config)
        explicit_treatment = treatments_by_value.get(existing_treatments.get(column, ""))
        # An explicit gr_treatment produced from a native class_label is a real
        # finding, not a fallback. Compare it with source-derived findings using
        # the same strictest-first config order so a scaffolded full-redaction
        # treatment can never be silently replaced by a weaker partial mask.
        candidates = {
            item.value: item
            for item in (source_treatment, explicit_treatment)
            if item
        }
        treatment = next(
            (item for item in config.treatments if item.value in candidates),
            None,
        )
        if treatment is None:
            continue
        if explicit_treatment and (
            (
                source_treatment
                and treatment_rank[explicit_treatment.value]
                < treatment_rank[source_treatment.value]
            )
            or (source_treatment is None and capture_source_less_explicit)
        ):
            overrides_by_column[column] = {
                "entity_name": column,
                "treatment": explicit_treatment.value,
            }
        if (
            treatment.value not in _FREE_TEXT_ESCALATION_EXCLUDED
            and is_free_text_column(column, treatment.masking_function)
        ):
            escalated = next(
                (item for item in config.treatments if item.value == _FREE_TEXT_ESCALATION_TARGET),
                None,
            )
            if escalated is not None:
                treatment = escalated
        used[treatment.value] = treatment
        catalog = column.split(".", 1)[0]
        source_tags = set(findings)
        if explicit_treatment:
            source_tags.add((config.tag_key, explicit_treatment.value))
        source_tags_by_catalog_treatment.setdefault(
            (catalog, treatment.value), []
        ).append(source_tags)
        derived.append({
            "entity_type": "columns", "entity_name": column,
            "tag_key": config.tag_key, "tag_value": treatment.value,
        })

    tag_policies = [
        dict(policy) for policy in (cfg.get("tag_policies") or [])
        if policy.get("key") != config.tag_key
    ]
    tag_policies.append({
        "key": config.tag_key,
        "description": config.description,
        "values": config.values,
    })

    existing_policies = [dict(policy) for policy in (cfg.get("fgac_policies") or [])]
    column_masks = [p for p in existing_policies if p.get("policy_type") == "POLICY_TYPE_COLUMN_MASK"]
    other_policies = [p for p in existing_policies if p.get("policy_type") != "POLICY_TYPE_COLUMN_MASK"]
    global_mask_principals = sorted({
        principal
        for policy in column_masks
        for principal in (policy.get("to_principals") or [])
    }) or ["account users"]

    def policy_refs(policy: dict) -> set[tuple[str, str]]:
        return set(re.findall(
            r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)",
            policy.get("match_condition", ""),
        ))

    catalogs_by_treatment: dict[str, set[str]] = {}
    for assignment in derived:
        parts = assignment["entity_name"].split(".")
        if len(parts) >= 4:
            catalogs_by_treatment.setdefault(assignment["tag_value"], set()).add(parts[0])
    for catalog, treatment_value in source_tags_by_catalog_treatment:
        catalogs_by_treatment.setdefault(treatment_value, set()).add(catalog)

    new_masks: list[dict] = []
    for treatment in config.treatments:
        for catalog in sorted(catalogs_by_treatment.get(treatment.value, set())):
            tag_sets = source_tags_by_catalog_treatment[(catalog, treatment.value)]
            replaced_masks = []
            for candidate in column_masks:
                if candidate.get("catalog") != catalog:
                    continue
                if candidate.get("comment") == ACL_NEUTRAL_FALLBACK_COMMENT:
                    continue
                refs = policy_refs(candidate)
                if refs and any(refs <= tags for tags in tag_sets):
                    replaced_masks.append(candidate)

            fallback = not replaced_masks
            principals = sorted({
                principal
                for candidate in replaced_masks
                for principal in (candidate.get("to_principals") or [])
            }) if replaced_masks else global_mask_principals
            exceptions = sorted({
                principal
                for candidate in replaced_masks
                for principal in (candidate.get("except_principals") or [])
            })
            overlap = sorted(set(principals) & set(exceptions))
            if overlap:
                raise ValueError(
                    f"Column masks for catalog {catalog!r} and treatment "
                    f"{treatment.value!r} put principals in both to_principals "
                    f"and except_principals: {overlap!r}"
                )
            if len(replaced_masks) > 1:
                LOGGER.warning(
                    "Collapsing %d model column masks into treatment %s on catalog %s; "
                    "unioning their principals",
                    len(replaced_masks), treatment.value, catalog,
                )
            template = replaced_masks[0] if replaced_masks else (
                next((p for p in column_masks if p.get("catalog") == catalog), None)
                or (column_masks[0] if column_masks else {})
            )
            policy = {
                "name": f"gr_mask_{catalog}_{treatment.value}",
                "policy_type": "POLICY_TYPE_COLUMN_MASK",
                "catalog": catalog,
                "to_principals": principals,
                "comment": ACL_NEUTRAL_FALLBACK_COMMENT if fallback else (
                    f"{DERIVED_TREATMENT_MASK_COMMENT_PREFIX} {treatment.value}; "
                    "strictest-wins derivation"
                ),
                "match_condition": f"hasTagValue('{config.tag_key}', '{treatment.value}')",
                "match_alias": f"gr_treatment_{treatment.value}",
                "function_name": treatment.masking_function,
                "function_catalog": catalog,
                "function_schema": template.get("function_schema") or "default",
            }
            if exceptions:
                policy["except_principals"] = exceptions
            new_masks.append(policy)

    result = dict(cfg)
    result["tag_policies"] = tag_policies
    result["tag_assignments"] = retained + derived
    result["fgac_policies"] = other_policies + new_masks
    if overrides_by_column or "treatment_overrides" in cfg:
        result["treatment_overrides"] = [
            overrides_by_column[column] for column in sorted(overrides_by_column)
        ]
    changes = len(derived) + len(column_masks) + len(new_masks)
    return result, changes


def matching_masks_by_column(cfg: dict) -> dict[str, list[str]]:
    """Return matching column-mask names for each assigned treatment column."""
    import re
    masks = [p for p in (cfg.get("fgac_policies") or []) if p.get("policy_type") == "POLICY_TYPE_COLUMN_MASK"]
    tags_by_column: dict[str, set[tuple[str, str]]] = {}
    for assignment in cfg.get("tag_assignments") or []:
        if assignment.get("entity_type") != "columns":
            continue
        column = assignment.get("entity_name", "")
        tags_by_column.setdefault(column, set()).add(
            (assignment.get("tag_key", ""), assignment.get("tag_value", ""))
        )

    result: dict[str, list[str]] = {}
    for column, tags in tags_by_column.items():
        catalog = column.split(".", 1)[0]
        for policy in masks:
            if policy.get("catalog") != catalog:
                continue
            refs = set(re.findall(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", policy.get("match_condition", "")))
            if refs and refs <= tags:
                result.setdefault(column, []).append(policy.get("name", ""))
        result.setdefault(column, [])
    return result
