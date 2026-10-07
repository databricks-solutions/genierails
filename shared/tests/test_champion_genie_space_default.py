"""Champion dev-to-prod flow: the template needs only a Genie agent ID.

Covers the placeholder guard (generate, enable-classification, plan/apply) and
that a template with no uc_tables reaches classification through the
import-discovered tables.
"""

import hashlib
import importlib.util
import os
import shutil
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
    # The retired exposure flag is no longer needed (or set) in the template.
    assert "business_access_enabled" not in cfg


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


REPO = SHARED.parent
GUARDED_TARGETS = [
    ("generate", "ENV=dev", "MODE=genie"),
    ("generate", "ENV=dev"),
    ("enable-classification", "ENV=dev"),
    ("scaffold-treatments", "ENV=dev"),
    ("derive-assignments", "ENV=dev"),
    ("generate-delta", "ENV=dev"),
    ("audit-schema", "ENV=dev"),
    ("audit-rulebook", "ENV=dev"),
    ("evidence", "ENV=dev"),
    ("validate-generated", "ENV=dev"),
    ("coverage-gate", "ENV=dev"),
    ("validate", "ENV=dev"),
    ("rehearse", "ENV=dev"),
    ("certify", "ENV=dev"),
    ("promote", "ENV=dev"),
    ("promote", "SOURCE_ENV=dev", "DEST_ENV=prod", "DEST_CATALOG_MAP=dev_finance=prod_finance"),
    ("plan", "ENV=dev"),
    ("apply", "ENV=dev"),
    ("apply-governance", "ENV=dev"),
    ("apply-genie", "ENV=dev"),
    ("import", "ENV=dev"),
    ("verify-access-spec", "ENV=dev"),
    ("verify-access", "ENV=dev"),
]


def _run_make(cloud_root, *args):
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "MODE", "SOURCE_ENV", "DEST_ENV")
    }
    return subprocess.run(
        ["make", "--no-print-directory", *args,
         f"CLOUD_ROOT={cloud_root}", f"SHARED_ROOT={SHARED}"],
        cwd=REPO / "aws", text=True, capture_output=True, env=env, timeout=120,
    )


@pytest.fixture(scope="module")
def _prepared_cloud(tmp_path_factory):
    """One `make setup` tree per module; each test gets a copy."""
    cloud_root = tmp_path_factory.mktemp("prepared") / "aws"
    cloud_root.mkdir()
    for env_name in ("dev", "prod"):
        assert _run_make(cloud_root, "setup", f"ENV={env_name}").returncode == 0
    (cloud_root / "envs/dev/env.auto.tfvars").write_text(TEMPLATE.read_text())
    return cloud_root


@pytest.fixture
def placeholder_cloud(_prepared_cloud, tmp_path):
    cloud_root = tmp_path / "aws"
    shutil.copytree(_prepared_cloud, cloud_root, symlinks=True)
    return cloud_root


def _snapshot(root):
    """Every path under root with its type, symlink target or content hash, and mtime."""
    entries = {}
    for path in sorted(root.rglob("*")):
        stat = path.lstat()
        if path.is_symlink():
            detail = ("link", os.readlink(path))
        elif path.is_dir():
            detail = ("dir",)
        else:
            detail = ("file", hashlib.sha256(path.read_bytes()).hexdigest())
        entries[str(path.relative_to(root))] = (*detail, stat.st_mtime_ns)
    return entries


def _assert_fails_fast(result, env_name="dev"):
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert (
        f"ERROR: replace {PLACEHOLDER} in envs/{env_name}/env.auto.tfvars "
        "with your Genie agent ID"
    ) in result.stderr, output
    # Nothing ran past the guard: no target banner, Terraform, or Genie API call.
    assert "===" not in result.stdout, output
    assert "Querying Genie agent" not in output
    assert "Terraform" not in output


@pytest.mark.parametrize("target", GUARDED_TARGETS, ids=" ".join)
def test_champion_targets_fail_fast_on_placeholder(placeholder_cloud, target):
    _assert_fails_fast(_run_make(placeholder_cloud, *target))


def test_promote_fails_fast_on_placeholder_in_existing_dest(placeholder_cloud):
    (placeholder_cloud / "envs/dev/env.auto.tfvars").write_text(
        TEMPLATE.read_text().replace(PLACEHOLDER, "01ef7b3c2a4d5e6f")
    )
    (placeholder_cloud / "envs/prod/env.auto.tfvars").write_text(TEMPLATE.read_text())

    result = _run_make(
        placeholder_cloud, "promote", "SOURCE_ENV=dev", "DEST_ENV=prod",
        "DEST_CATALOG_MAP=dev_finance=prod_finance",
    )

    _assert_fails_fast(result, env_name="prod")


@pytest.mark.parametrize("jobs", [[], ["-j4"]], ids=["serial", "j4"])
@pytest.mark.parametrize(
    "target",
    [("generate", "ENV=dev", "MODE=genie"), ("enable-classification", "ENV=dev"),
     ("plan", "ENV=dev"), ("apply", "ENV=dev"),
     ("promote", "SOURCE_ENV=dev", "DEST_ENV=prod", "DEST_CATALOG_MAP=dev_finance=prod_finance")],
    ids=" ".join,
)
def test_placeholder_guard_runs_before_any_bootstrap_side_effect(placeholder_cloud, target, jobs):
    # Seed state that _prepare-env would change: it deletes *.tf/*.py and
    # scripts/ in workspace envs, recreates the data_access symlinks, and
    # recreates envs/account.
    dev = placeholder_cloud / "envs/dev"
    (dev / "legacy.tf").write_text("# stale\n")
    (dev / "helper.py").write_text("# stale\n")
    (dev / "scripts").mkdir()
    (dev / "scripts/old.sh").write_text("# stale\n")
    (dev / "data_access/env.auto.tfvars").unlink()
    shutil.rmtree(placeholder_cloud / "envs/account")
    before = _snapshot(placeholder_cloud)

    _assert_fails_fast(_run_make(placeholder_cloud, *jobs, *target))

    assert _snapshot(placeholder_cloud) == before


def test_account_env_without_env_tfvars_passes_the_guard(tmp_path):
    cloud_root = tmp_path / "aws"
    cloud_root.mkdir()

    guard = _run_make(cloud_root, "_guard-genie-placeholder", "ENV=account")
    assert guard.returncode == 0, guard.stdout + guard.stderr
    assert not (cloud_root / "envs").exists()

    # A guarded target then bootstraps the account env as before.
    result = _run_make(cloud_root, "validate", "ENV=account")
    assert result.returncode == 0, result.stdout + result.stderr
    assert (cloud_root / "envs/account/env.auto.tfvars").exists()


def test_setup_is_not_blocked_by_the_placeholder(placeholder_cloud):
    result = _run_make(placeholder_cloud, "setup", "ENV=dev")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Next steps" in result.stdout
    assert PLACEHOLDER in (placeholder_cloud / "envs/dev/env.auto.tfvars").read_text()


def test_placeholder_guard_passes_once_agent_id_is_set(placeholder_cloud):
    (placeholder_cloud / "envs/dev/env.auto.tfvars").write_text(
        TEMPLATE.read_text().replace(PLACEHOLDER, "01ef7b3c2a4d5e6f")
    )

    result = _run_make(placeholder_cloud, "_guard-genie-placeholder", "ENV=dev")

    assert result.returncode == 0, result.stdout + result.stderr


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
