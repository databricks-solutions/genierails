"""Option-B single enforcement-tag derivation tests."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from treatment_derivation import (
    ACL_NEUTRAL_FALLBACK_COMMENT,
    derive_treatment_model,
    load_treatment_config,
    matching_masks_by_column,
    resolve_treatment,
)
from generate_abac import (
    autofix_acl_groups,
    derive_enforcement_treatments,
    strip_native_source_assignments,
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
    assert "treatment_overrides" not in derived
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


def test_generic_redaction_mask_has_deterministic_string_udf():
    config = load_treatment_config()
    treatment = next(item for item in config.treatments if item.value == "redact")
    assert treatment.udf_signature == "mask_redact(input STRING) RETURNS STRING"
    assert treatment.udf_body == (
        "CASE WHEN input IS NULL THEN NULL ELSE '[REDACTED]' END"
    )


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


def test_catalog_treatment_uses_only_its_model_principals():
    cfg = _base_config()
    cfg["groups"] = {"payments": {}, "hr": {}}
    for policy in cfg["fgac_policies"]:
        policy["to_principals"] = ["hr"]

    derived, _ = derive_treatment_model(cfg, load_treatment_config())

    masks = [
        policy for policy in derived["fgac_policies"]
        if policy["policy_type"] == "POLICY_TYPE_COLUMN_MASK"
    ]
    assert masks
    assert all(policy["to_principals"] == ["hr"] for policy in masks)


def test_b1_real_acl_autofix_keeps_catalog_groups_isolated(tmp_path):
    abac = tmp_path / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    abac.write_text('''
groups = { gA = {}, gB = {} }
tag_policies = [
  { key = "pay_level", values = ["secret"] },
  { key = "hr_level", values = ["secret"] },
]
tag_assignments = [
  { entity_type = "columns", entity_name = "pay.s.t.card", tag_key = "pay_level", tag_value = "secret" },
  { entity_type = "columns", entity_name = "hr.s.t.ssn", tag_key = "hr_level", tag_value = "secret" },
]
fgac_policies = [
  { name = "pay", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "pay", to_principals = ["gA"], match_condition = "hasTagValue('pay_level', 'secret')", function_name = "mask_redact", function_schema = "security" },
  { name = "hr", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "hr", to_principals = ["gB"], match_condition = "hasTagValue('hr_level', 'secret')", function_name = "mask_redact", function_schema = "security" },
]
genie_space_configs = { payments = {}, hr = {} }
''')
    env.write_text('''genie_spaces = [
  { name = "payments", uc_tables = ["pay.s.t"] },
  { name = "hr", uc_tables = ["hr.s.t"] },
]''')

    # Extend the real treatment mapping only for this focused reproduction.
    config = load_treatment_config()
    mapped = list(config.treatments)
    redact = next(item for item in mapped if item.value == "redact")
    from treatment_derivation import Treatment, TreatmentConfig
    custom = TreatmentConfig(config.tag_key, config.description, tuple([
        Treatment(
            value=redact.value, masking_function=redact.masking_function,
            sources=redact.sources | {("pay_level", "secret"), ("hr_level", "secret")},
            udf_signature=redact.udf_signature, udf_body=redact.udf_body,
            class_labels=redact.class_labels,
        ),
        *[item for item in mapped if item.value != "redact"],
    ]))
    import generate_abac
    original_loader = generate_abac.load_treatment_config
    generate_abac.load_treatment_config = lambda: custom
    try:
        derive_enforcement_treatments(abac)
    finally:
        generate_abac.load_treatment_config = original_loader
    assert autofix_acl_groups(abac, env) == 2

    import hcl2
    with (tmp_path / "genie_space_derived_acl_groups.auto.tfvars").open() as handle:
        assert hcl2.load(handle)["genie_space_derived_acl_groups"] == {
            "payments": ["gA"], "hr": ["gB"],
        }


def test_b2_exceptions_stay_catalog_local_and_never_overlap():
    cfg = _base_config()
    cfg["tag_assignments"].extend([{
        "entity_type": "columns", "entity_name": "cat.sch.people.email",
        "tag_key": "pii_level", "tag_value": "masked_email",
    }, {
        "entity_type": "columns", "entity_name": "hr.sch.people.email",
        "tag_key": "pii_level", "tag_value": "masked_email",
    }])
    cfg["fgac_policies"] = [
        {**cfg["fgac_policies"][0], "catalog": "cat", "to_principals": ["pay"], "except_principals": ["gA"]},
        {**cfg["fgac_policies"][0], "name": "hr_email", "catalog": "hr", "to_principals": ["gA"]},
    ]
    derived, _ = derive_treatment_model(cfg, load_treatment_config())
    masks = {
        p["catalog"]: p for p in derived["fgac_policies"]
        if "email_partial" in p["match_condition"]
    }
    assert masks["cat"]["except_principals"] == ["gA"]
    assert "except_principals" not in masks["hr"]
    assert masks["hr"]["to_principals"] == ["gA"]
    assert all(
        not (set(p["to_principals"]) & set(p.get("except_principals", [])))
        for p in masks.values()
    )


def test_catalog_without_model_mask_falls_back_fail_closed_but_acl_neutral():
    cfg = _base_config()
    cfg["fgac_policies"][1]["to_principals"] = ["pci_team"]
    cfg["tag_assignments"].append({
        "entity_type": "columns", "entity_name": "other.sch.people.email",
        "tag_key": "pii_level", "tag_value": "masked_email",
    })
    derived, _ = derive_treatment_model(cfg, load_treatment_config())
    other = next(p for p in derived["fgac_policies"] if p["catalog"] == "other")
    assert other["to_principals"] == ["analysts", "pci_team"]
    assert other["comment"] == ACL_NEUTRAL_FALLBACK_COMMENT


def test_pattern_a_privileged_tier_is_raw_only_where_model_omits_it():
    cfg = _base_config()
    cfg["tag_assignments"].extend([{
        "entity_type": "columns", "entity_name": "cat.sch.people.email",
        "tag_key": "pii_level", "tag_value": "masked_email",
    }, {
        "entity_type": "columns", "entity_name": "hr.sch.people.email",
        "tag_key": "pii_level", "tag_value": "masked_email",
    }])
    cfg["fgac_policies"] = [
        {**cfg["fgac_policies"][0], "to_principals": ["standard"]},
        {**cfg["fgac_policies"][0], "name": "hr_email", "catalog": "hr", "to_principals": ["standard", "privileged"]},
    ]
    derived, _ = derive_treatment_model(cfg, load_treatment_config())
    masks = {
        p["catalog"]: p for p in derived["fgac_policies"]
        if "email_partial" in p["match_condition"]
    }
    assert masks["cat"]["to_principals"] == ["standard"]
    assert masks["hr"]["to_principals"] == ["privileged", "standard"]


def test_multiple_masks_collapsed_for_one_catalog_treatment_warn_and_union(caplog):
    cfg = _base_config()
    cfg["fgac_policies"][0]["to_principals"] = ["email_team"]
    cfg["fgac_policies"][1]["to_principals"] = ["pci_team"]
    derived, _ = derive_treatment_model(cfg, load_treatment_config())
    redact = next(
        p for p in derived["fgac_policies"]
        if p["catalog"] == "cat" and "'redact'" in p["match_condition"]
    )
    assert redact["to_principals"] == ["email_team", "pci_team"]
    assert "Collapsing 2 model column masks" in caplog.text


def test_model_mask_principal_overlap_is_rejected_fail_closed():
    cfg = _base_config()
    cfg["fgac_policies"][0]["except_principals"] = ["analysts"]
    with pytest.raises(ValueError, match="both to_principals and except_principals"):
        derive_treatment_model(cfg, load_treatment_config())


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


def test_stricter_explicit_treatment_becomes_reviewed_override():
    column = "cat.sch.p.card_number"
    cfg = {
        "tag_policies": [],
        "tag_assignments": [
            _single(column, "pci_level", "masked_card_last4")["tag_assignments"][0],
            _single(column, "gr_treatment", "redact")["tag_assignments"][0],
        ],
        "fgac_policies": [{
            "name": "model_redact",
            "policy_type": "POLICY_TYPE_COLUMN_MASK",
            "catalog": "cat",
            "to_principals": ["payments"],
            "match_condition": "hasTagValue('gr_treatment', 'redact')",
            "function_name": "mask_redact",
            "function_schema": "security",
        }],
    }

    derived, _ = derive_treatment_model(cfg, load_treatment_config())
    assert _treatment_of(cfg, column) == ["redact"]
    masks = {p["match_alias"]: p for p in derived["fgac_policies"]}
    assert set(masks) == {"gr_treatment_redact"}
    assert derived["treatment_overrides"] == [{
        "entity_name": column,
        "treatment": "redact",
    }]


def test_weaker_explicit_treatment_is_not_recorded_as_override():
    column = "cat.sch.p.card_number"
    cfg = _single(column, "pci_level", "redacted_card_full")
    cfg["tag_assignments"].append(
        _single(column, "gr_treatment", "card_last4")["tag_assignments"][0]
    )
    derived, _ = derive_treatment_model(cfg, load_treatment_config())
    assert _treatment_of(derived, column) == ["redact"]
    assert "treatment_overrides" not in derived


def test_file_derivation_persists_reviewed_override_as_rule(tmp_path):
    path = tmp_path / "abac.auto.tfvars"
    path.write_text('''
tag_policies = []
tag_assignments = [
  { entity_type = "columns", entity_name = "dev.sales.cards.card_number", tag_key = "pci_level", tag_value = "masked_card_last4" },
  { entity_type = "columns", entity_name = "dev.sales.cards.card_number", tag_key = "gr_treatment", tag_value = "redact" },
]
fgac_policies = [
  { name = "redact", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "dev", to_principals = ["payments"], match_condition = "hasTagValue('gr_treatment', 'redact')", function_name = "mask_redact", function_schema = "security" },
]
''')
    derive_enforcement_treatments(path)

    import hcl2
    parsed = hcl2.loads(path.read_text())
    assert parsed["treatment_overrides"] == [{
        "entity_name": "dev.sales.cards.card_number",
        "treatment": "redact",
    }]


def test_source_less_explicit_draft_treatment_becomes_override(tmp_path):
    path = tmp_path / "abac.auto.tfvars"
    path.write_text('''
tag_policies = []
tag_assignments = [
  { entity_type = "columns", entity_name = "dev.sales.payments.amount", tag_key = "gr_treatment", tag_value = "round_amount" },
]
fgac_policies = [
  { name = "round", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "dev", to_principals = ["payments"], match_condition = "hasTagValue('gr_treatment', 'round_amount')", function_name = "mask_amount_rounded", function_schema = "security" },
]
''')
    derive_enforcement_treatments(path)
    import hcl2
    assert hcl2.loads(path.read_text())["treatment_overrides"] == [{
        "entity_name": "dev.sales.payments.amount",
        "treatment": "round_amount",
    }]


def test_source_less_override_fallback_mask_is_acl_neutral():
    column = "dev.sales.payments.amount"
    cfg = {
        "tag_policies": [],
        "tag_assignments": [{
            "entity_type": "columns", "entity_name": column,
            "tag_key": "gr_treatment", "tag_value": "round_amount",
        }],
        "fgac_policies": [],
    }
    derived, _ = derive_treatment_model(
        cfg, load_treatment_config(), capture_source_less_explicit=True
    )
    mask = derived["fgac_policies"][0]
    assert mask["comment"] == ACL_NEUTRAL_FALLBACK_COMMENT
    assert mask["to_principals"] == ["account users"]
    assert mask["function_schema"] == "sales"
    assert derived["treatment_overrides"] == [{
        "entity_name": column, "treatment": "round_amount",
    }]


def test_native_derived_treatment_never_becomes_sticky_override(tmp_path):
    path = tmp_path / "abac.auto.tfvars"
    path.write_text('''
tag_policies = []
tag_assignments = [
  { entity_type = "columns", entity_name = "dev.sales.payments.amount", tag_key = "financial_sensitivity", tag_value = "rounded_amounts" },
]
fgac_policies = [
  { name = "round", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "dev", to_principals = ["payments"], match_condition = "hasTagValue('financial_sensitivity', 'rounded_amounts')", function_name = "mask_amount_rounded", function_schema = "security" },
]
''')
    derive_enforcement_treatments(path)
    strip_native_source_assignments(path)
    derive_enforcement_treatments(path)

    import hcl2
    parsed = hcl2.loads(path.read_text())
    assert "treatment_overrides" not in parsed
    assert [
        item["tag_value"] for item in parsed["tag_assignments"]
        if item["tag_key"] == "gr_treatment"
    ] == ["round_amount"]


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
