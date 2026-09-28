"""Assignment-only native-classification refresh regression tests."""

import importlib.util
from pathlib import Path

import hcl2
import pytest

import generate_abac
from generate_abac import NativeClassificationRequiredError, _find_bracket_section
from sensitivity_source import ClassificationSource


SCRIPT = Path(__file__).parents[1] / "scripts/derive_assignments.py"
MAKEFILE = Path(__file__).parents[1] / "Makefile.shared"
SPEC = importlib.util.spec_from_file_location("derive_assignments_script", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


PROMOTED = '''# reviewed champion rules
groups = [{ display_name = "reviewed_group", roles = ["analyst"] }]
tag_policies = [{ key = "gr_treatment", description = "reviewed", values = ["redact", "email_partial", "ssn_last4"] }]
tag_assignments = [
  { entity_type = "tables", entity_name = "prod.sales.customers", tag_key = "row_scope", tag_value = "anz" },
  { entity_type = "columns", entity_name = "prod.sales.customers.old", tag_key = "gr_treatment", tag_value = "email_partial" },
]
fgac_policies = [
  { name = "reviewed_mask", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "prod", to_principals = ["reviewed_group"], match_condition = "hasTagValue('gr_treatment', 'redact')", function_name = "mask_redact", function_schema = "security" },
  { name = "reviewed_row_filter", policy_type = "POLICY_TYPE_ROW_FILTER", catalog = "prod", to_principals = ["reviewed_group"], match_condition = "hasTagValue('row_scope', 'anz')", function_name = "filter_anz", function_schema = "security" },
]
'''


def _files(tmp_path):
    generated = tmp_path / "generated"
    generated.mkdir()
    config = generated / "abac.auto.tfvars"
    config.write_text(PROMOTED)
    auth = tmp_path / "auth.auto.tfvars"
    auth.write_text('databricks_workspace_host = "https://unused.invalid"\n')
    env = tmp_path / "env.auto.tfvars"
    env.write_text('uc_catalog = "prod"\nuc_tables = ["sales.customers"]\n')
    return config, auth, env


def _outside_assignments(text):
    start, end = _find_bracket_section(text, "tag_assignments")
    return text[:start], text[end:]


def test_refresh_changes_only_assignments_and_derives_one_treatment_per_column(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    native = ClassificationSource(tag_rows=[
        ("prod", "sales", "customers", "email", "class.email_address", ""),
        ("prod", "sales", "customers", "ssn", "class.us_ssn", ""),
        ("prod", "sales", "customers", "free_text", "class.email_address", ""),
        ("prod", "sales", "customers", "free_text", "class.phone_number", ""),
    ])
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", lambda *a, **k: native)
    monkeypatch.setattr(
        generate_abac,
        "call_with_retries",
        lambda *_a, **_k: pytest.fail("assignment refresh must never call a model"),
    )
    before = config.read_text()

    assert MODULE.derive_assignments(config, auth, env) == 3

    after = config.read_text()
    assert _outside_assignments(after) == _outside_assignments(before)
    parsed = hcl2.loads(after)
    assignments = parsed["tag_assignments"]
    assert {tuple(sorted(item.items())) for item in assignments} == {
        tuple(sorted({"entity_type": "tables", "entity_name": "prod.sales.customers", "tag_key": "row_scope", "tag_value": "anz"}.items())),
        tuple(sorted({"entity_type": "columns", "entity_name": "prod.sales.customers.email", "tag_key": "gr_treatment", "tag_value": "email_partial"}.items())),
        tuple(sorted({"entity_type": "columns", "entity_name": "prod.sales.customers.ssn", "tag_key": "gr_treatment", "tag_value": "ssn_last4"}.items())),
        tuple(sorted({"entity_type": "columns", "entity_name": "prod.sales.customers.free_text", "tag_key": "gr_treatment", "tag_value": "redact"}.items())),
    }
    counts = {}
    for item in assignments:
        if item["tag_key"] == "gr_treatment":
            counts[item["entity_name"]] = counts.get(item["entity_name"], 0) + 1
    assert set(counts.values()) == {1}


@pytest.mark.parametrize("failure", [
    NativeClassificationRequiredError("unreadable"),
    ClassificationSource(),
])
def test_refresh_fails_closed_without_native_findings(tmp_path, monkeypatch, failure):
    config, auth, env = _files(tmp_path)
    before = config.read_bytes()

    def fetch(*_args, **_kwargs):
        if isinstance(failure, Exception):
            raise failure
        return failure

    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", fetch)
    with pytest.raises(NativeClassificationRequiredError):
        MODULE.derive_assignments(config, auth, env)
    assert config.read_bytes() == before


def test_missing_promoted_config_says_run_promote_first(tmp_path):
    with pytest.raises(RuntimeError, match="Run `make promote` first"):
        MODULE.derive_assignments(
            tmp_path / "generated/abac.auto.tfvars",
            tmp_path / "auth.auto.tfvars",
            tmp_path / "env.auto.tfvars",
        )


def test_command_has_no_model_call_surface():
    source = SCRIPT.read_text()
    assert "call_model" not in source
    assert "build_prompt" not in source
    assert "provider" not in source


def test_make_target_exposes_assignment_only_command():
    source = MAKEFILE.read_text()
    body = source[source.index("derive-assignments:") : source.index("\naudit-schema:")]
    assert "scripts/derive_assignments.py" in body
    assert "generated/abac.auto.tfvars" in body
    assert "generate_abac.py" not in body
