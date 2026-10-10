"""Assignment-only native-classification refresh regression tests."""

import hashlib
import importlib.util
import json
import re
from pathlib import Path

import hcl2
import pytest

import generate_abac
from generate_abac import (
    NativeClassificationRequiredError,
    _find_bracket_section,
    derive_enforcement_treatments,
)
from sensitivity_source import ClassificationSource
from scripts.remap_generated_config import remap_hcl
from scripts.merge_space_configs import merge_into_assembled
from scripts.coverage_gate import tag_assignments_digest


SCRIPT = Path(__file__).parents[1] / "scripts/derive_assignments.py"
MAKEFILE = Path(__file__).parents[1] / "Makefile.shared"
SPEC = importlib.util.spec_from_file_location("derive_assignments_script", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


PROMOTED = '''# reviewed dev-to-prod rules
groups = [{ display_name = "reviewed_group", roles = ["analyst"] }]
tag_policies = [{ key = "gr_treatment", description = "reviewed", values = ["redact", "email_partial", "ssn_last4"] }]
tag_assignments = [
  { entity_type = "tables", entity_name = "prod.sales.customers", tag_key = "row_scope", tag_value = "anz" },
  { entity_type = "columns", entity_name = "prod.sales.customers.old", tag_key = "gr_treatment", tag_value = "email_partial" },
  { entity_type = "columns", entity_name = "prod.sales.customers.no_longer_sensitive", tag_key = "pii_level", tag_value = "masked_email" },
  { entity_type = "columns", entity_name = "prod.sales.customers.no_longer_sensitive", tag_key = "gr_treatment", tag_value = "email_partial" },
  { entity_type = "columns", entity_name = "prod.sales.customers.email", tag_key = "pii_level", tag_value = "masked_ssn" },
  { entity_type = "columns", entity_name = "prod.sales.customers.email", tag_key = "gr_treatment", tag_value = "ssn_last4" },
]
fgac_policies = [
  { name = "reviewed_mask", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "prod", to_principals = ["reviewed_group"], match_condition = "hasTagValue('gr_treatment', 'redact')", function_name = "mask_redact", function_schema = "security" },
  { name = "reviewed_email", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "prod", to_principals = ["reviewed_group"], match_condition = "hasTagValue('gr_treatment', 'email_partial')", function_name = "mask_email", function_schema = "security" },
  { name = "reviewed_ssn", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "prod", to_principals = ["reviewed_group"], match_condition = "hasTagValue('gr_treatment', 'ssn_last4')", function_name = "mask_ssn", function_schema = "security" },
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
    _forbid_model_calls(monkeypatch)
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
    assert not any("no_longer_sensitive" in item["entity_name"] for item in assignments)
    counts = {}
    for item in assignments:
        if item["tag_key"] == "gr_treatment":
            counts[item["entity_name"]] = counts.get(item["entity_name"], 0) + 1
    assert set(counts.values()) == {1}
    classified_columns = {"prod.sales.customers.email", "prod.sales.customers.ssn",
                          "prod.sales.customers.free_text"}
    assert set(counts) <= classified_columns


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


def test_refresh_fails_closed_on_unmapped_native_class(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    before = config.read_bytes()
    native = ClassificationSource(tag_rows=[
        ("prod", "sales", "customers", "secret", "class.future_secret", ""),
    ])
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", lambda *a, **k: native)

    with pytest.raises(NativeClassificationRequiredError, match=r"unmapped class\.\* findings"):
        MODULE.derive_assignments(config, auth, env)
    assert config.read_bytes() == before


def test_refresh_fails_closed_when_promoted_mask_does_not_cover_treatment(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    original = config.read_text()
    config.write_text(original.replace(
        "hasTagValue('gr_treatment', 'ssn_last4')",
        "hasTagValue('gr_treatment', 'email_partial')",
    ))
    before = config.read_bytes()
    native = ClassificationSource(tag_rows=[
        ("prod", "sales", "customers", "ssn", "class.us_ssn", ""),
    ])
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", lambda *a, **k: native)

    with pytest.raises(RuntimeError, match="no matching column-mask policy"):
        MODULE.derive_assignments(config, auth, env)
    assert config.read_bytes() == before


def _add_override(config, column, treatment):
    text = config.read_text()
    config.write_text(
        f'treatment_overrides = [{{ entity_name = "{column}", treatment = "{treatment}" }}]\n'
        + text
    )


def test_stricter_card_override_survives_certification_and_is_idempotent(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    column = "prod.sales.customers.card_number"
    _add_override(config, column, "redact")
    native = ClassificationSource(tag_rows=[
        ("prod", "sales", "customers", "card_number", "class.credit_card_number", ""),
    ])
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", lambda *a, **k: native)

    assert MODULE.derive_assignments(config, auth, env) == 1
    assignments = hcl2.loads(config.read_text())["tag_assignments"]
    assert next(a for a in assignments if a.get("entity_name") == column)["tag_value"] == "redact"
    once = config.read_bytes()
    assert MODULE.derive_assignments(config, auth, env) == 0
    assert config.read_bytes() == once


def test_source_less_dev_draft_override_survives_promote_and_prod_certify(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    dev_draft = tmp_path / "dev.auto.tfvars"
    dev_draft.write_text('''
tag_policies = []
tag_assignments = [
  { entity_type = "columns", entity_name = "dev.sales.customers.amount", tag_key = "gr_treatment", tag_value = "round_amount" },
  { entity_type = "columns", entity_name = "dev.sales.customers.email", tag_key = "pii_level", tag_value = "masked_email" },
]
fgac_policies = [
  { name = "round", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "dev", to_principals = ["reviewed_group"], match_condition = "hasTagValue('gr_treatment', 'round_amount')", function_name = "mask_amount_rounded", function_schema = "security" },
  { name = "email", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "dev", to_principals = ["reviewed_group"], match_condition = "hasTagValue('pii_level', 'masked_email')", function_name = "mask_email", function_schema = "security" },
]
''')
    derive_enforcement_treatments(dev_draft)
    promoted = remap_hcl(dev_draft.read_text(), [("dev", "prod")])
    assert hcl2.loads(promoted)["treatment_overrides"] == [{
        "entity_name": "prod.sales.customers.amount",
        "treatment": "round_amount",
    }]
    config.write_text(promoted)
    native = ClassificationSource(tag_rows=[
        # Native mode remains available, but amount itself has no native finding.
        ("prod", "sales", "customers", "email", "class.email_address", ""),
    ])
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", lambda *a, **k: native)

    assert MODULE.derive_assignments(config, auth, env) == 2
    assignments = hcl2.loads(config.read_text())["tag_assignments"]
    assert next(
        item for item in assignments
        if item.get("entity_name") == "prod.sales.customers.amount"
    )["tag_value"] == "round_amount"
    once = config.read_bytes()
    assert MODULE.derive_assignments(config, auth, env) == 0
    assert config.read_bytes() == once


def test_per_space_override_merge_survives_promote_and_prod_certify(tmp_path, monkeypatch):
    generated = tmp_path / "dev" / "generated"
    second = generated / "spaces" / "second"
    second.mkdir(parents=True)
    (tmp_path / "dev" / "env.auto.tfvars").write_text('''
genie_spaces = [
  { name = "First", uc_tables = ["dev.sales.customers"], acl_groups = ["g"] },
  { name = "Second", uc_tables = ["dev.sales.customers"], acl_groups = ["g"] },
]
''')
    common_policies = '''
  { name = "redact", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "dev", to_principals = ["g"], match_condition = "hasTagValue('gr_treatment', 'redact')", function_name = "mask_redact", function_schema = "security" },
  { name = "round", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "dev", to_principals = ["g"], match_condition = "hasTagValue('gr_treatment', 'round_amount')", function_name = "mask_amount_rounded", function_schema = "security" },
  { name = "email", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "dev", to_principals = ["g"], match_condition = "hasTagValue('gr_treatment', 'email_partial')", function_name = "mask_email", function_schema = "security" },
'''
    (generated / "abac.auto.tfvars").write_text(f'''
groups = {{ g = {{}} }}
tag_policies = [{{ key = "gr_treatment", values = ["redact", "round_amount", "email_partial"] }}]
tag_assignments = []
treatment_overrides = [
  {{ entity_name = "dev.sales.customers.first_secret", treatment = "redact" }},
  {{ entity_name = "dev.sales.customers.shared", treatment = "redact" }},
]
fgac_policies = [{common_policies}]
genie_space_configs = {{ First = {{ title = "First" }} }}
''')
    (second / "abac.auto.tfvars").write_text(f'''
tag_policies = [{{ key = "gr_treatment", values = ["redact", "round_amount", "email_partial"] }}]
tag_assignments = [
  {{ entity_type = "columns", entity_name = "dev.sales.customers.amount", tag_key = "gr_treatment", tag_value = "round_amount" }},
  {{ entity_type = "columns", entity_name = "dev.sales.customers.shared", tag_key = "gr_treatment", tag_value = "round_amount" }},
  {{ entity_type = "columns", entity_name = "dev.sales.customers.email", tag_key = "gr_treatment", tag_value = "email_partial" }},
]
treatment_overrides = [
  {{ entity_name = "dev.sales.customers.amount", treatment = "round_amount" }},
  {{ entity_name = "dev.sales.customers.shared", treatment = "round_amount" }},
]
fgac_policies = [{common_policies}]
genie_space_configs = {{ Second = {{ title = "Second" }} }}
''')
    (generated / "masking_functions.sql").write_text("")
    (second / "masking_functions.sql").write_text("")

    merge_into_assembled(generated, "second")
    assembled = hcl2.loads((generated / "abac.auto.tfvars").read_text())
    assert assembled["treatment_overrides"] == [
        {"entity_name": "dev.sales.customers.amount", "treatment": "round_amount"},
        {"entity_name": "dev.sales.customers.first_secret", "treatment": "redact"},
        {"entity_name": "dev.sales.customers.shared", "treatment": "redact"},
    ]

    prod = tmp_path / "prod"
    prod.mkdir()
    config, auth, env = _files(prod)
    config.write_text(remap_hcl((generated / "abac.auto.tfvars").read_text(), [("dev", "prod")]))
    native = ClassificationSource(tag_rows=[
        ("prod", "sales", "customers", "email", "class.email_address", ""),
    ])
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", lambda *a, **k: native)
    MODULE.derive_assignments(config, auth, env)
    assignments = {
        item["entity_name"]: item["tag_value"]
        for item in hcl2.loads(config.read_text())["tag_assignments"]
        if item.get("tag_key") == "gr_treatment"
    }
    assert assignments["prod.sales.customers.amount"] == "round_amount"
    assert assignments["prod.sales.customers.first_secret"] == "redact"
    assert assignments["prod.sales.customers.shared"] == "redact"


def test_weaker_override_cannot_downgrade_native(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    column = "prod.sales.customers.card_number"
    _add_override(config, column, "card_last4")
    native = ClassificationSource(tag_rows=[
        ("prod", "sales", "customers", "card_number", "class.card_security_code", ""),
    ])
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", lambda *a, **k: native)

    MODULE.derive_assignments(config, auth, env)
    assignments = hcl2.loads(config.read_text())["tag_assignments"]
    assert next(a for a in assignments if a.get("entity_name") == column)["tag_value"] == "redact"


def test_override_applies_without_native_tag_when_column_is_in_footprint(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    column = "prod.sales.customers.card_number"
    _add_override(config, column, "redact")
    native = ClassificationSource(tag_rows=[
        ("prod", "sales", "customers", "email", "class.email_address", ""),
    ])
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", lambda *a, **k: native)

    MODULE.derive_assignments(config, auth, env)
    assignments = hcl2.loads(config.read_text())["tag_assignments"]
    assert next(a for a in assignments if a.get("entity_name") == column)["tag_value"] == "redact"


def test_override_for_removed_table_warns_and_skips(tmp_path, monkeypatch, capsys):
    config, auth, env = _files(tmp_path)
    column = "prod.sales.removed.card_number"
    _add_override(config, column, "redact")
    native = ClassificationSource(tag_rows=[
        ("prod", "sales", "customers", "email", "class.email_address", ""),
    ])
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", lambda *a, **k: native)

    MODULE.derive_assignments(config, auth, env)
    assert "no longer in the governed footprint" in capsys.readouterr().err
    assert not any(
        a.get("entity_name") == column
        for a in hcl2.loads(config.read_text())["tag_assignments"]
    )


def test_override_still_requires_promoted_mask_coverage(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    column = "prod.sales.customers.card_number"
    _add_override(config, column, "redact")
    config.write_text(config.read_text().replace(
        "hasTagValue('gr_treatment', 'redact')",
        "hasTagValue('gr_treatment', 'email_partial')",
    ))
    before = config.read_bytes()
    native = ClassificationSource(tag_rows=[
        ("prod", "sales", "customers", "card_number", "class.credit_card_number", ""),
    ])
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", lambda *a, **k: native)

    with pytest.raises(RuntimeError, match="no matching column-mask policy"):
        MODULE.derive_assignments(config, auth, env)
    assert config.read_bytes() == before


def test_missing_promoted_config_says_run_promote_first(tmp_path):
    with pytest.raises(RuntimeError, match="Run `make promote` first"):
        MODULE.derive_assignments(
            tmp_path / "generated/abac.auto.tfvars",
            tmp_path / "auth.auto.tfvars",
            tmp_path / "env.auto.tfvars",
        )


def test_nondefault_env_filename_supplies_footprint(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    custom_env = tmp_path / "prod.custom.tfvars"
    env.rename(custom_env)
    native = ClassificationSource(tag_rows=[
        ("prod", "sales", "customers", "email", "class.email_address", ""),
    ])
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", lambda *a, **k: native)
    assert MODULE.derive_assignments(config, auth, custom_env) == 1


def test_main_reports_malformed_discovery_without_traceback(tmp_path, capsys):
    config, auth, env = _files(tmp_path)
    data_access = tmp_path / "data_access"
    data_access.mkdir()
    (data_access / "discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = "prod.sales.customers"\n'
    )
    result = MODULE.main([
        "--config", str(config), "--auth-file", str(auth), "--env-file", str(env)
    ])
    assert result == 1
    assert "ERROR: invalid discovered footprint" in capsys.readouterr().err


MODEL_SURFACES = (
    "call_with_retries", "call_databricks", "call_openai", "call_anthropic",
    "build_prompt", "serving_endpoints",
)


def _forbid_model_calls(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("assignment refresh must never call a model")

    for namespace in (generate_abac, MODULE):
        for name in MODEL_SURFACES:
            monkeypatch.setattr(namespace, name, forbidden, raising=False)


def test_command_has_no_model_call_surface():
    source = SCRIPT.read_text()
    assert all(name not in source for name in MODEL_SURFACES)


def test_make_target_exposes_assignment_only_command():
    source = MAKEFILE.read_text()
    body = source[source.index("derive-assignments:") : source.index("\naudit-schema:")]
    assert '"$(DERIVE_ASSIGNMENTS_SCRIPT)"' in body
    assert "DERIVE_ASSIGNMENTS_SCRIPT ?= $(SHARED_ROOT)/scripts/derive_assignments.py" in source
    assert "generated/abac.auto.tfvars" in body
    assert "generate_abac.py" not in body


def _native_email(monkeypatch):
    native = ClassificationSource(tag_rows=[
        ("prod", "sales", "customers", "email", "class.email_address", ""),
    ])
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", lambda *a, **k: native)
    _forbid_model_calls(monkeypatch)


def test_write_ddl_refreshes_the_footprint_ddl_the_coverage_gate_reads(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    _native_email(monkeypatch)
    fetched = []

    def fake_fetch(table_refs, runtime):
        fetched.append(list(table_refs))
        print("  Fetching: prod.sales.customers...")  # must not leak into derive's output
        return "CREATE TABLE prod.sales.customers (\n  email STRING,\n  ssn STRING\n);", [("prod", "sales")]

    monkeypatch.setattr(MODULE, "fetch_tables_from_databricks", fake_fetch)
    ddl = tmp_path / "ddl" / "_fetched.sql"
    MODULE.derive_assignments(config, auth, env, ddl_out=ddl)
    assert fetched == [["prod.sales.customers"]]
    assert ddl.read_text() == "CREATE TABLE prod.sales.customers (\n  email STRING,\n  ssn STRING\n);\n"
    # Unchanged DDL is not rewritten.
    mtime = ddl.stat().st_mtime_ns
    MODULE.derive_assignments(config, auth, env, ddl_out=ddl)
    assert ddl.stat().st_mtime_ns == mtime


def test_write_ddl_failure_fails_derive(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    _native_email(monkeypatch)

    def no_tables(table_refs, runtime):
        print("ERROR: No tables found for the given references.")
        raise SystemExit(1)

    monkeypatch.setattr(MODULE, "fetch_tables_from_databricks", no_tables)
    before = config.read_text()
    with pytest.raises(RuntimeError, match="No tables found"):
        MODULE.derive_assignments(config, auth, env, ddl_out=tmp_path / "ddl" / "_fetched.sql")
    assert config.read_text() == before
    assert not (tmp_path / "ddl" / "_fetched.sql").exists()


def test_derive_target_refreshes_fetched_ddl():
    source = MAKEFILE.read_text()
    body = source[source.index("derive-assignments:"):source.index("\naudit-schema:")]
    assert "--write-ddl ddl/_fetched.sql" in body


def _fake_ddl(monkeypatch, text="CREATE TABLE prod.sales.customers (\n  email STRING\n);"):
    monkeypatch.setattr(MODULE, "fetch_tables_from_databricks", lambda refs, runtime: (text, [("prod", "sales")]))


def test_successful_refresh_records_what_it_read(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    _native_email(monkeypatch)
    _fake_ddl(monkeypatch)
    record = tmp_path / "generated" / ".live_refresh.json"
    ddl = tmp_path / "ddl" / "_fetched.sql"
    assert MODULE.main([
        "--config", str(config), "--auth-file", str(auth), "--env-file", str(env),
        "--write-ddl", str(ddl), "--refresh-record", str(record),
    ]) == 0
    written = json.loads(record.read_text())
    assert written["mode"] == "full"
    assert written["ddl_sha256"] == hashlib.sha256(ddl.read_bytes()).hexdigest()
    assert written["tag_assignments_sha256"] == tag_assignments_digest(
        hcl2.loads(config.read_text())["tag_assignments"]
    )
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", written["refreshed_at"])


def test_failed_refresh_removes_the_previous_record(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    record = tmp_path / "generated" / ".live_refresh.json"
    record.write_text('{"mode": "full", "refreshed_at": "2026-01-01T00:00:00Z"}')

    def unreadable(*args, **kwargs):
        raise generate_abac.NativeClassificationRequiredError("Could not read required native classification")

    monkeypatch.setattr(MODULE, "_fetch_live_classification_source", unreadable)
    assert MODULE.main([
        "--config", str(config), "--auth-file", str(auth), "--env-file", str(env),
        "--write-ddl", str(tmp_path / "ddl" / "_fetched.sql"), "--refresh-record", str(record),
    ]) == 1
    assert not record.exists()


def test_ddl_only_refresh_leaves_tags_alone(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    _forbid_model_calls(monkeypatch)
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source",
                        lambda *a, **k: pytest.fail("ddl-only must not read tags"))
    _fake_ddl(monkeypatch)
    before = config.read_text()
    record = tmp_path / "generated" / ".live_refresh.json"
    assert MODULE.main([
        "--ddl-only", "--auth-file", str(auth), "--env-file", str(env),
        "--write-ddl", str(tmp_path / "ddl" / "_fetched.sql"), "--refresh-record", str(record),
    ]) == 0
    assert config.read_text() == before
    written = json.loads(record.read_text())
    assert written["mode"] == "ddl" and "tag_assignments_sha256" not in written


def test_live_refresh_refuses_ambient_credentials(tmp_path, monkeypatch):
    config, auth, env = _files(tmp_path)
    auth.write_text('databricks_workspace_host = ""\n')
    monkeypatch.setattr(MODULE, "fetch_tables_from_databricks",
                        lambda *a, **k: pytest.fail("must not reach Databricks"))
    monkeypatch.setattr(MODULE, "_fetch_live_classification_source",
                        lambda *a, **k: pytest.fail("must not reach Databricks"))
    for extra in ([], ["--ddl-only"]):
        assert MODULE.main([*extra, "--config", str(config), "--auth-file", str(auth),
                            "--env-file", str(env), "--write-ddl", str(tmp_path / "ddl.sql")]) == 1
