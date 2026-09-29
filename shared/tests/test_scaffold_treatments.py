import copy
import json
import sys
from pathlib import Path

import hcl2

import validate_abac
from tag_vocabulary import TagVocabularyRegistry
from treatment_derivation import load_treatment_config

SCRIPTS = Path(__file__).parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
from scaffold_treatments import scaffold  # noqa: E402


def _fixture(tmp_path, marker="class.biometric"):
    config = tmp_path / "treatment_config.json"
    config.write_text(json.dumps({
        "tag_key": "gr_treatment",
        "description": "test treatments",
        "treatments": [
            {
                "value": "redact", "masking_function": "mask_redact",
                "sources": [["pii_level", "redacted"]],
            },
            {
                "value": "email_partial", "masking_function": "mask_email",
                "sources": [["pii_level", "masked_email"]],
                "class_labels": ["class.email"],
            },
        ],
    }, indent=2))
    vocabulary = tmp_path / "tag_vocabulary_registry.json"
    vocabulary.write_text(json.dumps({"families": {"gr_treatment": {
        "canonical_key": "gr_treatment", "description": "test",
        "canonical_values": ["redact", "email_partial"],
        "key_aliases": [], "value_aliases": {},
    }}}))
    tfvars = tmp_path / "abac.auto.tfvars"
    tfvars.write_text(f'''tag_policies = [
  {{ key = "pii_level", description = "PII", values = ["masked_email"] }}
]
tag_assignments = [
  {{ entity_type = "columns", entity_name = "cat.sch.people.email", tag_key = "pii_level", tag_value = "masked_email" }}
# gr.classification_unmapped: cat.sch.people.biometric|{marker}
]
fgac_policies = [
  {{ name = "email", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "cat", to_principals = ["account users"], match_condition = "hasTagValue('gr_treatment', 'email_partial')", match_alias = "email_partial", function_name = "mask_email", function_catalog = "cat", function_schema = "security" }}
]
''')
    sql = tmp_path / "masking_functions.sql"
    sql.write_text('''CREATE OR REPLACE FUNCTION mask_redact(input STRING)
RETURNS STRING RETURN CASE WHEN input IS NULL THEN NULL ELSE '[REDACTED]' END;
CREATE OR REPLACE FUNCTION mask_email(input STRING) RETURNS STRING RETURN input;
''')
    return tfvars, sql, config, vocabulary


def test_scaffold_adds_reviewed_redact_stub_treatment_mapping_and_coverage(tmp_path, monkeypatch):
    tfvars, sql, config_path, vocabulary = _fixture(tmp_path)
    before = json.loads(config_path.read_text())["treatments"]

    added = scaffold(tfvars, sql, config_path, vocabulary)

    assert added == [{
        "label": "class.biometric", "semantic": "biometric",
        "value": "biometric_redacted", "function": "mask_biometric_redact",
    }]
    raw = json.loads(config_path.read_text())
    assert raw["treatments"][:2] == before
    treatment = raw["treatments"][-1]
    assert treatment["class_labels"] == ["class.biometric"]
    assert treatment["value"] == "biometric_redacted"
    assert treatment["masking_function"] == "mask_biometric_redact"
    assert treatment["udf_body"] == "CASE WHEN input IS NULL THEN NULL ELSE '[REDACTED]' END"
    assert "REVIEW" in treatment["review"]
    family = json.loads(vocabulary.read_text())["families"]["gr_treatment"]
    assert "biometric_redacted" in family["canonical_values"]
    assert "REVIEW" in family["review_values"]["biometric_redacted"]
    sql_text = sql.read_text()
    assert "-- REVIEW: auto-scaffolded for class.biometric" in sql_text
    assert "CREATE OR REPLACE FUNCTION mask_biometric_redact(input STRING)" in sql_text
    assert "ELSE '[REDACTED]'" in sql_text

    tfvars_text = tfvars.read_text()
    assert "classification_unmapped" not in tfvars_text
    cfg = hcl2.loads(tfvars_text)
    assert any(
        item.get("entity_name") == "cat.sch.people.biometric"
        and item.get("tag_key") == "gr_treatment"
        and item.get("tag_value") == "biometric_redacted"
        for item in cfg["tag_assignments"]
    )
    monkeypatch.setattr(
        validate_abac, "load_treatment_config", lambda: load_treatment_config(config_path)
    )
    monkeypatch.setattr(
        validate_abac, "REGISTRY", TagVocabularyRegistry.load_default()
    )
    result = validate_abac.ValidationResult()
    validate_abac.validate_coverage_gate(
        cfg, {"mask_email", "mask_biometric_redact"}, tfvars_text, result,
    )
    assert result.passed, result.errors
    assert not any("detected tags with no mapping/rule" in e for e in result.errors)


def test_scaffold_is_idempotent(tmp_path):
    tfvars, sql, config, vocabulary = _fixture(tmp_path)
    scaffold(tfvars, sql, config, vocabulary)
    snapshot = (tfvars.read_text(), sql.read_text(), config.read_text(), vocabulary.read_text())

    assert scaffold(tfvars, sql, config, vocabulary) == []
    assert (tfvars.read_text(), sql.read_text(), config.read_text(), vocabulary.read_text()) == snapshot
    assert sql.read_text().count("CREATE OR REPLACE FUNCTION mask_biometric_redact") == 1


def test_scaffold_does_not_touch_already_mapped_label(tmp_path):
    tfvars, sql, config, vocabulary = _fixture(tmp_path, marker="class.email")
    snapshot = (tfvars.read_text(), sql.read_text(), config.read_text(), vocabulary.read_text())
    original_treatments = copy.deepcopy(json.loads(config.read_text())["treatments"])

    assert scaffold(tfvars, sql, config, vocabulary) == []
    assert (tfvars.read_text(), sql.read_text(), config.read_text(), vocabulary.read_text()) == snapshot
    assert json.loads(config.read_text())["treatments"] == original_treatments
