import importlib.util
import re
import subprocess
from pathlib import Path
from types import SimpleNamespace


SHARED = Path(__file__).parents[1]
MODULE_MAIN = SHARED / "modules/data_access/main.tf"
MODULE_VARIABLES = SHARED / "modules/data_access/variables.tf"
ROOT_MAIN = SHARED / "roots/data_access/main.tf"
MAKEFILE = SHARED / "Makefile.shared"
VALIDATOR = SHARED / "scripts/validate_classification_config.py"
ROOT = SHARED / "roots/data_access"


def test_classification_is_opt_in_and_forwarded_by_the_root():
    variables = MODULE_VARIABLES.read_text()
    start = variables.index('variable "enable_classification"')
    body = variables[start : variables.index("}\n", start) + 2]

    assert "default     = false" in body
    assert "enable_classification" in ROOT_MAIN.read_text()
    assert "= var.enable_classification" in ROOT_MAIN.read_text()


def test_auto_tagging_is_default_off_and_forwarded_by_the_root():
    variables = MODULE_VARIABLES.read_text()
    start = variables.index('variable "enable_auto_tagging"')
    body = variables[start : variables.index("}\n", start) + 2]

    assert "default     = false" in body
    root = ROOT_MAIN.read_text()
    assert 'variable "enable_auto_tagging"' in root
    assert "enable_auto_tagging             = var.enable_auto_tagging" in root


def test_classification_is_scoped_to_governed_uc_schemas():
    source = MODULE_MAIN.read_text()

    assert "var.enable_classification ? local.classification_catalog_schemas : {}" in source
    assert 'parent   = "catalogs/${each.key}"' in source
    assert "names = each.value" in source
    assert "distinct(local._classification_catalogs)" in source
    assert "classification_existing_schemas" in source
    assert "prevent_destroy = true" in source


def test_auto_tagging_false_emits_no_configs_while_classification_stays_enabled():
    source = MODULE_MAIN.read_text()

    assert "var.enable_classification ? local.classification_catalog_schemas : {}" in source
    match = re.search(
        r"(?ms)^  auto_tag_configs = (var\.enable_auto_tagging \? \[.*?^  \] : \[\])$",
        source,
    )
    assert match, "auto_tag_configs must render an empty list when auto-tagging is false"
    expression = match.group(1)
    assert expression.endswith("] : []")
    assert expression.count('auto_tagging_mode  = "AUTO_TAGGING_ENABLED"') == 1


def test_auto_tagging_true_emits_configs_for_dev_to_prod_types():
    source = MODULE_MAIN.read_text()

    expected = {
        "class.card_security_code",
        "class.credit_card",
        "class.date_of_birth",
        "class.email_address",
        "class.name",
        "class.phone_number",
        "class.us_ssn",
    }
    assert all(f'"{tag}"' in source for tag in expected)
    assert "auto_tag_configs = var.enable_auto_tagging ? [" in source
    assert 'auto_tagging_mode  = "AUTO_TAGGING_ENABLED"' in source


def test_auto_tagging_opt_in_plan_covers_default_off_and_enabled_configs():
    init = subprocess.run(
        ["terraform", "init", "-backend=false", "-input=false"],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert init.returncode == 0, init.stdout + init.stderr

    plan_test = subprocess.run(
        ["terraform", "test", "-no-color", "-filter=tests/auto_tagging_opt_in.tftest.hcl"],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert plan_test.returncode == 0, plan_test.stdout + plan_test.stderr
    assert 'run "classification_scans_without_auto_tagging"... pass' in plan_test.stdout
    assert 'run "auto_tagging_emits_all_dev_to_prod_classifier_types"... pass' in plan_test.stdout


def test_enable_classification_target_is_a_classification_only_apply():
    source = MAKEFILE.read_text()
    start = source.index("enable-classification:")
    body = source[start : source.index("\ngenerate:", start)]

    assert "_bootstrap _guard-workspace-target" in body
    assert "validate_classification_config.py" in body
    assert "-target=module.data_access.databricks_data_classification_catalog_config.classification" in body
    assert "abac.auto.tfvars" not in body
    assert "masking_functions.sql" not in body
    assert "_apply-layer" not in body


def test_data_access_root_unions_all_effective_tables_for_grants_and_classification():
    source = ROOT_MAIN.read_text()

    assert 'variable "genie_spaces"' in source
    assert "flatten([for space in var.genie_spaces : space.uc_tables])" in source
    assert "var.discovered_uc_tables" in source
    assert "full_uc_tables = [for t in local.configured_uc_tables" in source
    assert "full_discovered_uc_tables = [for t in var.discovered_uc_tables" in source
    assert "full_effective_uc_tables = distinct(concat" in source
    assert "classification_uc_tables        = local.full_effective_uc_tables" in source


def test_classification_and_grant_footprints_are_independent():
    source = MODULE_MAIN.read_text()

    assert "for t in var.classification_uc_tables" in source
    assert "for catalog in distinct(local._classification_catalogs)" in source
    assert "for schema in local.classification_uc_schemas" in source
    assert "for t in local.effective_uc_tables" in source

    grant_section = source[source.index('resource "databricks_grant"') :]
    assert "classification_uc_tables" not in grant_section


def test_full_apply_plan_unions_space_and_discovered_tables_into_grants():
    init = subprocess.run(
        ["terraform", "init", "-backend=false", "-input=false"],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert init.returncode == 0, init.stdout + init.stderr

    plan_test = subprocess.run(
        ["terraform", "test", "-filter=tests/grant_isolation.tftest.hcl"],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert plan_test.returncode == 0, plan_test.stdout + plan_test.stderr


def test_classification_plan_unions_live_scope_and_second_env_cannot_shrink_it():
    init = subprocess.run(
        ["terraform", "init", "-backend=false", "-input=false"],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert init.returncode == 0, init.stdout + init.stderr

    plan_test = subprocess.run(
        ["terraform", "test", "-filter=tests/classification_scope_union.tftest.hcl"],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    assert plan_test.returncode == 0, plan_test.stdout + plan_test.stderr


def test_prepare_resolves_two_part_tables_and_preserves_all_schema_scope(tmp_path, monkeypatch, capsys):
    env_dir = tmp_path / "envs" / "dev"
    (env_dir / "data_access").mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text(
        'enable_classification = true\nuc_catalog = "real_catalog"\n'
        'uc_tables = ["schema_a.table_a"]\n'
    )
    (env_dir / "auth.auto.tfvars").write_text(
        'databricks_workspace_host = "https://example.invalid"\n'
        'databricks_client_id = "client"\ndatabricks_client_secret = "secret"\n'
    )

    spec = importlib.util.spec_from_file_location(
        "prepare_classification_config",
        SHARED / "scripts/prepare_classification_config.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    requested = []

    class FakeClassification:
        def get_catalog_config(self, name):
            requested.append(name)
            return SimpleNamespace(included_schemas=None)

    client_kwargs = {}

    def fake_workspace_client(**kwargs):
        client_kwargs.update(kwargs)
        return SimpleNamespace(data_classification=FakeClassification())

    monkeypatch.setattr(
        module,
        "WorkspaceClient",
        fake_workspace_client,
    )
    monkeypatch.setattr(module.sys, "argv", ["prepare", str(env_dir)])

    assert module.main() == 0
    assert client_kwargs["custom_headers"] is None
    assert requested == ["catalogs/real_catalog/config"]
    generated = (env_dir / "data_access/classification.auto.tfvars").read_text()
    assert 'classification_all_schemas = ["real_catalog"]' in generated
    assert "WARNING: real_catalog classification includes ALL schemas" in capsys.readouterr().err


def test_prepare_includes_imported_genie_discovery_in_classification_scope(
    tmp_path, monkeypatch
):
    env_dir = tmp_path / "envs" / "dev"
    (env_dir / "data_access").mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text(
        'enable_classification = true\nuc_tables = []\n'
        'genie_spaces = [{ genie_space_id = "space-123" }]\n'
    )
    (env_dir / "data_access/discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["imported_catalog.agent.orders"]\n'
    )
    (env_dir / "auth.auto.tfvars").write_text(
        'databricks_workspace_host = "https://example.invalid"\n'
        'databricks_client_id = "client"\ndatabricks_client_secret = "secret"\n'
    )

    spec = importlib.util.spec_from_file_location(
        "prepare_classification_config_discovered",
        SHARED / "scripts/prepare_classification_config.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    requested = []

    class FakeClassification:
        def get_catalog_config(self, name):
            requested.append(name)
            raise module.NotFound("missing")

    monkeypatch.setattr(
        module,
        "WorkspaceClient",
        lambda **_: SimpleNamespace(data_classification=FakeClassification()),
    )
    monkeypatch.setattr(module.sys, "argv", ["prepare", str(env_dir)])

    assert module.main() == 0
    assert requested == ["catalogs/imported_catalog/config"]


def test_prepare_seeds_aws_classification_with_serverless_usage_policy(
    tmp_path, monkeypatch
):
    env_dir = tmp_path / "envs" / "dev"
    (env_dir / "data_access").mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text(
        'enable_classification = true\n'
        'uc_tables = ["imported_catalog.agent.orders"]\n'
    )
    (env_dir / "auth.auto.tfvars").write_text(
        'databricks_workspace_host = "https://example.invalid"\n'
        'databricks_workspace_id = "12345"\n'
        'databricks_client_id = "client"\n'
        'databricks_client_secret = "secret"\n'
        'serverless_usage_policy_id = "policy-123"\n'
    )

    spec = importlib.util.spec_from_file_location(
        "prepare_classification_config_aws_policy",
        SHARED / "scripts/prepare_classification_config.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    class FakeClassification:
        def get_catalog_config(self, _name):
            raise module.NotFound("missing")

    api_calls = []
    client_kwargs = {}

    class FakeAPI:
        def do(self, method, path, **kwargs):
            api_calls.append((method, path, kwargs))
            return {}

    def fake_workspace_client(**kwargs):
        client_kwargs.update(kwargs)
        return SimpleNamespace(
            data_classification=FakeClassification(), api_client=FakeAPI()
        )

    monkeypatch.setattr(module, "WorkspaceClient", fake_workspace_client)
    monkeypatch.setattr(module.sys, "argv", ["prepare", str(env_dir)])

    assert module.main() == 0
    assert client_kwargs["custom_headers"] == {
        "X-Databricks-Org-Id": "12345"
    }
    assert api_calls == [
        (
            "POST",
            "/api/data-classification/v1/catalogs/imported_catalog/config",
            {
                "body": {
                    "included_schemas": {"names": ["agent"]},
                    "auto_tag_configs": [],
                    "usage_policy_id": "policy-123",
                },
            },
        )
    ]
    assert (env_dir / "data_access/classification.auto.tfvars").read_text() == (
        'classification_existing_schemas = {\n'
        '  "imported_catalog" = ["agent"]\n'
        '}\nclassification_all_schemas = []\n'
    )


def test_data_access_plan_and_apply_refresh_classification_without_swallowing_errors():
    source = MAKEFILE.read_text()
    prepare = source[source.index("_prepare-classification:") : source.index("_plan-layer:")]
    plan = source[source.index("_plan-layer:") : source.index("plan:", source.index("_plan-layer:"))]
    apply = source[source.index("_apply-layer:") : source.index("apply:", source.index("_apply-layer:"))]

    assert "prepare_classification_config.py" in prepare
    assert '|| exit 1' in prepare
    assert "@set -e" in plan
    assert "_prepare-classification" in plan
    assert "_prepare-classification" in apply
    assert "classification.auto.tfvars" in apply


def test_classification_validator_requires_opt_in_and_accepts_space_footprint(tmp_path):
    import subprocess
    import sys

    config = tmp_path / "env.auto.tfvars"
    config.write_text('genie_spaces = [{ uc_tables = ["cat.schema.table"] }]\n')
    disabled = subprocess.run([sys.executable, VALIDATOR, config], capture_output=True, text=True)
    assert disabled.returncode == 1
    assert "enable_classification = true" in disabled.stderr

    config.write_text(
        'enable_classification = true\n'
        'genie_spaces = [{ uc_tables = ["cat.schema.table"] }]\n'
    )
    enabled = subprocess.run([sys.executable, VALIDATOR, config], capture_output=True, text=True)
    assert enabled.returncode == 0


def test_classification_validator_accepts_imported_genie_discovery(tmp_path):
    import subprocess
    import sys

    config = tmp_path / "env.auto.tfvars"
    config.write_text(
        'enable_classification = true\nuc_tables = []\n'
        'genie_spaces = [{ genie_space_id = "space-123" }]\n'
    )
    data_access = tmp_path / "data_access"
    data_access.mkdir()
    (data_access / "discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["imported_catalog.agent.orders"]\n'
    )

    enabled = subprocess.run(
        [sys.executable, VALIDATOR, config], capture_output=True, text=True
    )
    assert enabled.returncode == 0, enabled.stderr
