"""Champion dev-to-prod flow: the template needs only a Genie agent ID.

Covers the placeholder guard (generate, enable-classification, plan/apply) and
that a template with no uc_tables reaches classification through the
import-discovered tables.
"""

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import hcl2
import pytest

import generate_abac
from genie_space_placeholder import PLACEHOLDER, placeholder_error


SHARED = Path(__file__).parents[1]
TEMPLATE = SHARED / "examples/dev_to_prod/env.auto.tfvars.example"
VALIDATOR = SHARED / "scripts/validate_classification_config.py"
GUARD = SHARED / "genie_space_placeholder.py"
MAKEFILE = SHARED / "Makefile.shared"
EXPECTED_ERROR = (
    f"replace {PLACEHOLDER} in envs/dev/env.auto.tfvars with your Genie agent ID"
)


def _dev_env(tmp_path, genie_space_id=PLACEHOLDER, discovered=None):
    env_dir = tmp_path / "envs" / "dev"
    (env_dir / "data_access").mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text(
        TEMPLATE.read_text().replace(PLACEHOLDER, genie_space_id)
    )
    (env_dir / "auth.auto.tfvars").write_text(
        'databricks_workspace_host = "https://example.invalid"\n'
        'databricks_client_id = "client"\ndatabricks_client_secret = "secret"\n'
    )
    if discovered is not None:
        rendered = ", ".join(f'"{table}"' for table in discovered)
        (env_dir / "data_access/discovered_uc_tables.auto.tfvars").write_text(
            f"discovered_uc_tables = [{rendered}]\n"
        )
    return env_dir


def test_template_genie_space_placeholder_is_the_only_active_footprint():
    cfg = hcl2.loads(TEMPLATE.read_text())

    assert cfg["genie_spaces"] == [{"genie_space_id": PLACEHOLDER}]
    assert "uc_tables" not in cfg
    assert cfg["sql_warehouse_id"] == ""
    assert cfg["enable_classification"] is True
    assert cfg["enable_auto_tagging"] is False
    assert cfg["business_access_enabled"] is False


def test_placeholder_error_names_the_file_and_the_fix(tmp_path):
    env_dir = _dev_env(tmp_path)
    cfg = hcl2.loads((env_dir / "env.auto.tfvars").read_text())

    assert EXPECTED_ERROR in placeholder_error(cfg, env_dir / "env.auto.tfvars")
    assert placeholder_error({"genie_spaces": [{"genie_space_id": "01ef7b3c"}]}, env_dir) is None
    assert placeholder_error({"genie_spaces": [{"genie_space_id": ""}]}, env_dir) is None
    assert placeholder_error({}, env_dir) is None


def test_generate_config_loading_fails_fast_on_placeholder(tmp_path):
    env_dir = _dev_env(tmp_path)

    with pytest.raises(ValueError, match="replace <your-genie-space-id> in envs/dev/env.auto.tfvars"):
        generate_abac.load_auth_config(env_dir / "auth.auto.tfvars", strict_env=True)


def test_generate_config_loading_accepts_agent_id_without_uc_tables(tmp_path):
    env_dir = _dev_env(tmp_path, genie_space_id="01ef7b3c2a4d5e6f")

    cfg = generate_abac.load_auth_config(env_dir / "auth.auto.tfvars", strict_env=True)

    assert cfg["genie_spaces"] == [{"genie_space_id": "01ef7b3c2a4d5e6f"}]
    assert "uc_tables" not in cfg


def test_enable_classification_validator_fails_fast_on_placeholder(tmp_path):
    env_dir = _dev_env(tmp_path, discovered=["dev_finance.sales.orders"])

    result = subprocess.run(
        [sys.executable, VALIDATOR, env_dir / "env.auto.tfvars"],
        capture_output=True, text=True,
    )

    assert result.returncode == 1
    assert EXPECTED_ERROR in result.stderr


def test_plan_apply_guard_fails_fast_on_placeholder(tmp_path):
    env_dir = _dev_env(tmp_path)

    result = subprocess.run(
        [sys.executable, GUARD, env_dir / "env.auto.tfvars"],
        capture_output=True, text=True,
    )
    assert result.returncode == 1
    assert EXPECTED_ERROR in result.stderr

    (env_dir / "env.auto.tfvars").write_text(
        TEMPLATE.read_text().replace(PLACEHOLDER, "01ef7b3c2a4d5e6f")
    )
    result = subprocess.run(
        [sys.executable, GUARD, env_dir / "env.auto.tfvars"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr


def test_plan_and_apply_run_the_placeholder_guard():
    source = MAKEFILE.read_text()
    start = source.index("_guard-workspace-config:")
    body = source[start : source.index("\n\n", start)]
    assert 'genie_space_placeholder.py" "$(ENV_DIR)/env.auto.tfvars"' in body
    for target in ("plan:", "apply:", "apply-governance:", "apply-genie:"):
        target_start = source.index(f"\n{target}") + 1
        target_body = source[target_start : source.index("\n\n", target_start)]
        assert "_guard-workspace-config" in target_body, target


def test_enable_classification_footprint_uses_discovered_tables_without_uc_tables(
    tmp_path, monkeypatch
):
    env_dir = _dev_env(
        tmp_path,
        genie_space_id="01ef7b3c2a4d5e6f",
        discovered=["dev_finance.sales.orders", "dev_ops.support.tickets"],
    )

    validated = subprocess.run(
        [sys.executable, VALIDATOR, env_dir / "env.auto.tfvars"],
        capture_output=True, text=True,
    )
    assert validated.returncode == 0, validated.stderr

    spec = importlib.util.spec_from_file_location(
        "prepare_classification_config_champion",
        SHARED / "scripts/prepare_classification_config.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    requested = []

    class FakeClassification:
        def get_catalog_config(self, name):
            requested.append(name)
            return SimpleNamespace(included_schemas=SimpleNamespace(names=["existing"]))

    monkeypatch.setattr(
        module,
        "WorkspaceClient",
        lambda **_: SimpleNamespace(data_classification=FakeClassification()),
    )
    monkeypatch.setattr(module.sys, "argv", ["prepare", str(env_dir)])

    assert module.main() == 0
    assert requested == ["catalogs/dev_finance/config", "catalogs/dev_ops/config"]


def test_enable_classification_without_import_explains_how_to_get_tables(tmp_path):
    env_dir = _dev_env(tmp_path, genie_space_id="01ef7b3c2a4d5e6f")

    result = subprocess.run(
        [sys.executable, VALIDATOR, env_dir / "env.auto.tfvars"],
        capture_output=True, text=True,
    )

    assert result.returncode == 1
    assert "import a Genie agent to populate discovered_uc_tables" in result.stderr


def test_sample_env_snippet_replaces_template_lines_without_duplicates():
    spec = importlib.util.spec_from_file_location(
        "setup_sample_env_champion", SHARED / "examples/dev_to_prod/setup_sample_env.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    snippet = module._tfvars(
        "01ef7b3c2a4d5e6f", ["dev_finance.genierails_e2e.customers"], "wh-123"
    )

    # Follow SAMPLE_ENV.md: replace the template's genie_spaces + sql_warehouse_id.
    template = TEMPLATE.read_text()
    genie_block = '\ngenie_spaces = [\n  { genie_space_id = "<your-genie-space-id>" },\n]\n'
    warehouse_line = '\nsql_warehouse_id = ""\n'
    assert genie_block in template and warehouse_line in template
    merged = template.replace(genie_block, "\n" + snippet + "\n").replace(warehouse_line, "\n")

    cfg = hcl2.loads(merged)
    assert cfg["uc_tables"] == ["dev_finance.genierails_e2e.customers"]
    assert cfg["genie_spaces"] == [{"genie_space_id": "01ef7b3c2a4d5e6f"}]
    assert cfg["sql_warehouse_id"] == "wh-123"
    assert placeholder_error(cfg, TEMPLATE) is None
    for key in ("genie_spaces", "sql_warehouse_id", "uc_tables"):
        assert sum(
            line.startswith(f"{key} =") for line in merged.splitlines()
        ) == 1, key


@pytest.mark.parametrize("root", ["account", "data_access", "workspace"])
def test_terraform_roots_default_uc_tables_so_it_can_be_omitted(root):
    source = (SHARED / "roots" / root / "main.tf").read_text()
    start = source.index('variable "uc_tables"')
    body = source[start : source.index("}\n", start)]
    assert "default" in body and "[]" in body


@pytest.mark.parametrize("mode_args", [["--mode", "genie"], []], ids=["import", "generate"])
def test_generate_discovers_tables_from_agent_id_only_template(
    mode_args, tmp_path, monkeypatch
):
    env_dir = _dev_env(tmp_path, genie_space_id="01ef7b3c2a4d5e6f")
    api_tables = ["dev_finance.sales.orders", "dev_finance.sales.customers"]
    monkeypatch.setattr(
        generate_abac,
        "fetch_tables_from_genie_space",
        lambda *a, **k: (api_tables, {}, "Finance Agent", True),
    )
    captured = {}

    def stop_after_footprint(table_refs, auth_cfg):
        captured["table_refs"] = list(table_refs)
        captured["uc_tables"] = list(auth_cfg["uc_tables"])
        raise RuntimeError("stop after footprint")

    monkeypatch.setattr(generate_abac, "fetch_tables_from_databricks", stop_after_footprint)
    monkeypatch.setattr(
        sys,
        "argv",
        ["generate_abac.py", "--auth-file", str(env_dir / "auth.auto.tfvars"),
         "--create-groups", *mode_args],
    )
    with pytest.raises(RuntimeError, match="stop after footprint"):
        generate_abac.main()

    discovered = hcl2.loads(
        (env_dir / "data_access/discovered_uc_tables.auto.tfvars").read_text()
    )
    assert discovered["discovered_uc_tables"] == api_tables
    assert set(captured["table_refs"]) == set(api_tables)
    assert set(captured["uc_tables"]) == set(api_tables)
