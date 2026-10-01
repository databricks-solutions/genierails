"""Option-B single enforcement-tag derivation tests."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from treatment_derivation import (
    derive_treatment_model,
    load_treatment_config,
    matching_masks_by_column,
    resolve_treatment,
)


def _base_config():
    return {
        "tag_policies": [
            {"key": "pii_level", "values": ["masked_email", "masked_ssn"]},
            {"key": "pci_level", "values": ["redacted_cvv"]},
        ],
        "tag_assignments": [
            {"entity_type": "columns", "entity_name": "cat.sch.people.contact", "tag_key": "pii_level", "tag_value": "masked_email"},
            {"entity_type": "columns", "entity_name": "cat.sch.people.contact", "tag_key": "pci_level", "tag_value": "redacted_cvv"},
            {"entity_type": "columns", "entity_name": "cat.sch.people.ssn", "tag_key": "pii_level", "tag_value": "masked_ssn"},
        ],
        "fgac_policies": [
            {"name": "old_email", "policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "cat", "to_principals": ["analysts"], "match_condition": "hasTagValue('pii_level', 'masked_email')", "function_name": "mask_email", "function_schema": "security"},
            {"name": "old_cvv", "policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "cat", "to_principals": ["analysts"], "match_condition": "hasTagValue('pci_level', 'redacted_cvv')", "function_name": "mask_redact", "function_schema": "security"},
        ],
    }


def test_precedence_multi_tag_column_resolves_to_single_strictest_treatment():
    config = load_treatment_config()
    treatment = resolve_treatment(
        [("pii_level", "masked_email"), ("pci_level", "redacted_cvv")], config
    )
    assert treatment is not None
    assert treatment.value == "redact"

    derived, _ = derive_treatment_model(_base_config(), config)
    contact = [
        item for item in derived["tag_assignments"]
        if item["entity_name"] == "cat.sch.people.contact"
    ]
    assert contact[-1:] == [{
        "entity_type": "columns", "entity_name": "cat.sch.people.contact",
        "tag_key": "gr_treatment", "tag_value": "redact",
    }]
    assert {(item["tag_key"], item["tag_value"]) for item in contact[:-1]} == {
        ("pii_level", "masked_email"),
        ("pci_level", "redacted_cvv"),
    }


def test_single_tag_ssn_resolves_to_treatment():
    derived, _ = derive_treatment_model(_base_config(), load_treatment_config())
    treatments = [
        item for item in derived["tag_assignments"]
        if item["entity_name"] == "cat.sch.people.ssn"
        and item["tag_key"] == "gr_treatment"
    ]
    assert [item["tag_value"] for item in treatments] == ["ssn_last4"]


def test_compensation_mask_is_numeric_type_compatible():
    config = load_treatment_config()
    treatment = next(
        item for item in config.treatments
        if item.value == "compensation_redacted"
    )
    assert treatment.udf_signature == (
        "mask_compensation_redact(input DECIMAL(18,2)) RETURNS DECIMAL(18,2)"
    )
    assert treatment.udf_body == "CAST(NULL AS DECIMAL(18,2))"


def test_unknown_tag_resolves_to_no_treatment():
    assert resolve_treatment(
        [("pii_level", "future_unknown_value")], load_treatment_config()
    ) is None


def test_duplicate_class_labels_across_treatments_are_rejected(tmp_path):
    config_path = tmp_path / "treatment_config.json"
    config_path.write_text(json.dumps({
        "tag_key": "gr_treatment",
        "description": "test",
        "treatments": [
            {
                "value": "one", "masking_function": "mask_one",
                "sources": [], "class_labels": ["class.biometric"],
            },
            {
                "value": "two", "masking_function": "mask_two",
                "sources": [], "class_labels": ["class.biometric"],
            },
        ],
    }))

    with pytest.raises(ValueError, match="class labels must be unique"):
        load_treatment_config(config_path)


def test_column_without_sensitivity_tag_gets_no_treatment():
    cfg = _base_config()
    cfg["tag_assignments"].append({
        "entity_type": "columns", "entity_name": "cat.sch.people.public_id",
        "tag_key": "quality", "tag_value": "verified",
    })
    derived, _ = derive_treatment_model(cfg, load_treatment_config())
    assert not any(
        item["entity_name"] == "cat.sch.people.public_id"
        and item["tag_key"] == "gr_treatment"
        for item in derived["tag_assignments"]
    )


def test_derivation_is_idempotent_and_preserves_masks_and_source_tags():
    config = load_treatment_config()
    once, _ = derive_treatment_model(_base_config(), config)
    twice, _ = derive_treatment_model(once, config)
    assert twice == once

    masks = [
        policy for policy in twice["fgac_policies"]
        if policy["policy_type"] == "POLICY_TYPE_COLUMN_MASK"
    ]
    assert masks
    treatment_counts = {}
    for item in twice["tag_assignments"]:
        if item["entity_type"] == "columns" and item["tag_key"] == "gr_treatment":
            treatment_counts[item["entity_name"]] = treatment_counts.get(item["entity_name"], 0) + 1
    assert treatment_counts
    assert set(treatment_counts.values()) == {1}
    assert any(item["tag_key"] == "pii_level" for item in twice["tag_assignments"])
    assert any(item["tag_key"] == "pci_level" for item in twice["tag_assignments"])


def test_rekeyed_masks_match_no_column_more_than_once():
    derived, _ = derive_treatment_model(_base_config(), load_treatment_config())
    matches = matching_masks_by_column(derived)
    assert matches
    assert all(len(policy_names) == 1 for policy_names in matches.values())
    masks = [p for p in derived["fgac_policies"] if p["policy_type"] == "POLICY_TYPE_COLUMN_MASK"]
    assert all("hasTagValue('gr_treatment'," in p["match_condition"] for p in masks)


def test_multi_catalog_masks_are_scoped_to_one_match_per_column():
    cfg = _base_config()
    cfg["tag_assignments"].append({
        "entity_type": "columns", "entity_name": "other.sch.people.ssn",
        "tag_key": "pii_level", "tag_value": "masked_ssn",
    })
    derived, _ = derive_treatment_model(cfg, load_treatment_config())
    matches = matching_masks_by_column(derived)
    assert len(matches["cat.sch.people.ssn"]) == 1
    assert len(matches["other.sch.people.ssn"]) == 1
    assert matches["cat.sch.people.ssn"] != matches["other.sch.people.ssn"]


def _single(entity, key, value):
    return {
        "tag_policies": [],
        "tag_assignments": [{"entity_type": "columns", "entity_name": entity, "tag_key": key, "tag_value": value}],
        "fgac_policies": [],
    }


def _treatment_of(cfg, entity):
    derived, _ = derive_treatment_model(cfg, load_treatment_config())
    return [
        (a["tag_value"]) for a in derived["tag_assignments"]
        if a["entity_name"] == entity and a["tag_key"] == "gr_treatment"
    ]


def test_free_text_column_with_single_class_is_fully_redacted():
    # notes.free_text natively tagged only class.email_address must not get the
    # email-shaped mask: mask_email leaks embedded phone numbers from the text.
    col = "cat.sch.notes.free_text"
    assert _treatment_of(_single(col, "pii_level", "masked_email"), col) == ["redact"]
    derived, _ = derive_treatment_model(_single(col, "pii_level", "masked_email"), load_treatment_config())
    masks = [p["function_name"] for p in derived["fgac_policies"]]
    assert masks == ["mask_redact"]


def test_generic_category_column_with_format_mask_is_redacted():
    # No identifier semantics in the name (category == generic) → redact.
    col = "cat.sch.tickets.details"
    assert _treatment_of(_single(col, "pii_level", "masked_phone"), col) == ["redact"]


def test_identifier_columns_keep_partial_treatment():
    assert _treatment_of(_single("cat.sch.c.email", "pii_level", "masked_email"), "cat.sch.c.email") == ["email_partial"]
    assert _treatment_of(_single("cat.sch.c.phone", "pii_level", "masked_phone"), "cat.sch.c.phone") == ["phone_partial"]
    assert _treatment_of(
        _single("cat.sch.p.credit_card_number", "pci_level", "masked_card_last4"),
        "cat.sch.p.credit_card_number",
    ) == ["card_last4"]


def test_numeric_and_date_treatments_are_not_escalated():
    # mask_redact is STRING-typed; escalating a DECIMAL/DATE column would break binding.
    assert _treatment_of(
        _single("cat.sch.p.notes_amount", "financial_sensitivity", "rounded_amounts"),
        "cat.sch.p.notes_amount",
    ) == ["round_amount"]


@pytest.mark.parametrize("column_name", ["compensation_summary", "salary_notes"])
def test_free_text_named_compensation_stays_numeric_treatment(column_name):
    column = f"cat.sch.p.{column_name}"
    assert _treatment_of(
        # Generation maps class.compensation to this treatment before derivation.
        _single(column, "gr_treatment", "compensation_redacted"), column
    ) == ["compensation_redacted"]


def test_rederive_treatment_only_config_is_idempotent():
    cfg = {
        "tag_policies": [],
        "tag_assignments": [{
            "entity_type": "columns", "entity_name": "cat.sch.tbl.email",
            "tag_key": "gr_treatment", "tag_value": "email_partial",
        }],
        "fgac_policies": [{
            "name": "existing", "policy_type": "POLICY_TYPE_COLUMN_MASK",
            "catalog": "cat", "to_principals": ["account users"],
            "match_condition": "hasTagValue('gr_treatment', 'email_partial')",
            "function_name": "mask_email", "function_schema": "security",
        }],
    }
    first, _ = derive_treatment_model(cfg, load_treatment_config())
    second, _ = derive_treatment_model(first, load_treatment_config())
    assert second["tag_assignments"] == first["tag_assignments"]
    assert second["fgac_policies"] == first["fgac_policies"]
    assert second["tag_assignments"][0]["tag_value"] == "email_partial"
