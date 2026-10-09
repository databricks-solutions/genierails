"""The simplified champion command flow.

enable-classification finds an ID-only agent's tables itself; one plain
`make generate` imports the agent and drafts rules (or stops, before any model
call, until class.* tags exist); `make promote-to` wraps the cross-env promote
and saves FROM / CATALOG_MAP in the destination; a passing rehearse/release
saves an explicit VERIFY_KEY_COLUMN.
"""

import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import hcl2
import pytest

import generate_abac
from scripts import discover_agent_tables, saved_settings
from tests.test_sticky_reviewed_rules import (  # noqa: F401  (env_dir is a fixture)
    E2E_DDL,
    EMAIL,
    LIMIT,
    _model_response,
    env_dir,
)

SHARED = Path(__file__).parents[1]
REPO = SHARED.parent
MAKEFILE = SHARED / "Makefile.shared"
TEMPLATE = SHARED / "examples/dev_to_prod/env.auto.tfvars.example"
TF_CAPTURE = SHARED / "tests/fixtures/terraform_target_apply.txt"
SPACE_ID = "01ef7b3c2a4d5e6f"
TABLES = ["dev_fin.payments.customers", "dev_fin.payments.payments"]
SERIALIZED = json.dumps({
    "data_sources": {"tables": [{"identifier": table} for table in TABLES]},
    "instructions": {"text_instructions": [{"content": ["Amounts are in AUD."]}]},
})


def _clean_env(**extra):
    env = {k: v for k, v in os.environ.items()
           if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "MODE", "FROM", "CATALOG_MAP",
                        "SOURCE_ENV", "DEST_ENV", "DEST_CATALOG_MAP", "VERIFY_KEY_COLUMN", "APPLY_FLAGS")}
    env.update(extra)
    return env


# ── 1. enable-classification discovers an ID-only agent's tables ────────────

FAKE_TERRAFORM = """#!/usr/bin/env bash
case "$1" in
  init|state) exit 0 ;;
  apply) echo apply >> "$FAKE_TF_LOG"; cat "$FAKE_TF_STDOUT"; exit 0 ;;
esac
"""
FAKE_SDK = """import json, os
from .errors import NotFound
class _Classification:
    def get_catalog_config(self, name):
        raise NotFound(name)
class _Api:
    def do(self, method, path, **kwargs):
        open(os.environ["FAKE_GENIE_LOG"], "a").write(f"{method} {path}\\n")
        if os.environ.get("FAKE_GENIE_FAIL"):
            raise RuntimeError("403 Forbidden")
        return {"title": "Payments Agent", "serialized_space": os.environ["FAKE_SERIALIZED"]}
class WorkspaceClient:
    def __init__(self, **kwargs):
        self.data_classification = _Classification()
        self.api_client = _Api()
"""


@pytest.fixture
def classification_cloud(tmp_path):
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    cloud_root = tmp_path / "aws"
    cloud_root.mkdir()
    args = [f"CLOUD_ROOT={cloud_root}", f"SHARED_ROOT={SHARED}"]
    setup = subprocess.run(["make", "--no-print-directory", "setup", "ENV=dev", *args],
                           cwd=REPO / "aws", text=True, capture_output=True, env=_clean_env())
    assert setup.returncode == 0, setup.stdout + setup.stderr
    env_file = cloud_root / "envs/dev/env.auto.tfvars"
    env_file.write_text(TEMPLATE.read_text().replace("<your-genie-space-id>", SPACE_ID))
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "terraform").write_text(FAKE_TERRAFORM)
    (fake_bin / "terraform").chmod(0o755)
    sdk = tmp_path / "sdk/databricks/sdk"
    sdk.mkdir(parents=True)
    (sdk.parent / "__init__.py").write_text("")
    (sdk / "__init__.py").write_text(FAKE_SDK)
    (sdk / "errors.py").write_text("class NotFound(Exception):\n    pass\n")
    (sdk / "useragent.py").write_text("")
    logs = {"genie": tmp_path / "genie.log", "tf": tmp_path / "tf.log"}

    def run(**fake):
        env = _clean_env(**{
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}", "PYTHONPATH": str(tmp_path / "sdk"),
            "FAKE_TF_STDOUT": str(TF_CAPTURE), "FAKE_TF_LOG": str(logs["tf"]),
            "FAKE_GENIE_LOG": str(logs["genie"]), "FAKE_SERIALIZED": SERIALIZED, **fake,
        })
        return subprocess.run(
            ["make", "--no-print-directory", "enable-classification", "ENV=dev", *args],
            cwd=REPO / "aws", text=True, capture_output=True, env=env, timeout=120,
        )

    run.env_dir = cloud_root / "envs/dev"
    run.logs = logs
    return run


def _calls(path):
    return path.read_text().splitlines() if path.exists() else []


def test_enable_classification_discovers_tables_from_the_agent_id(classification_cloud):
    result = classification_cloud()
    out = result.stdout + result.stderr

    assert result.returncode == 0, out
    assert _calls(classification_cloud.logs["genie"]) == [f"GET /api/2.0/genie/spaces/{SPACE_ID}"]
    discovered = hcl2.loads(
        (classification_cloud.env_dir / "data_access/discovered_uc_tables.auto.tfvars").read_text())
    assert discovered["discovered_uc_tables"] == TABLES
    assert discovered["discovered_table_agents"] == {t: ["Payments Agent"] for t in TABLES}
    assert "=== Discover Genie agent tables (dev) ===" in out
    assert _calls(classification_cloud.logs["tf"]) == ["apply"]
    assert out.index("Discover Genie agent tables") < out.index("Enable UC Data Classification")


def test_enable_classification_rereads_an_unchanged_agent_without_rewriting(classification_cloud):
    assert classification_cloud().returncode == 0
    path = classification_cloud.env_dir / "data_access/discovered_uc_tables.auto.tfvars"
    before = path.read_text()

    result = classification_cloud()

    assert result.returncode == 0, result.stdout + result.stderr
    assert len(_calls(classification_cloud.logs["genie"])) == 2
    assert path.read_text() == before


def test_changed_agent_id_with_the_same_name_replaces_the_stale_tables(classification_cloud):
    env_file = classification_cloud.env_dir / "env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace(
        f'{{ genie_space_id = "{SPACE_ID}" }}', f'{{ genie_space_id = "{SPACE_ID}", name = "Pay" }}'))
    assert classification_cloud().returncode == 0
    env_file.write_text(env_file.read_text().replace(SPACE_ID, "01new"))
    new_tables = json.dumps({"data_sources": {"tables": [{"identifier": "dev_fin.cards.cards"}]}})

    result = classification_cloud(FAKE_SERIALIZED=new_tables)

    assert result.returncode == 0, result.stdout + result.stderr
    assert _calls(classification_cloud.logs["genie"])[-1] == "GET /api/2.0/genie/spaces/01new"
    discovered = hcl2.loads(
        (classification_cloud.env_dir / "data_access/discovered_uc_tables.auto.tfvars").read_text())
    assert discovered["discovered_uc_tables"] == ["dev_fin.cards.cards"]
    assert discovered["discovered_table_agents"] == {"dev_fin.cards.cards": ["Pay"]}


def test_enable_classification_api_failure_writes_nothing(classification_cloud):
    # A second, already-discovered agent: its discovery must survive the failure.
    env_file = classification_cloud.env_dir / "env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace(
        f'{{ genie_space_id = "{SPACE_ID}" }},',
        f'{{ genie_space_id = "{SPACE_ID}" }},\n  {{ genie_space_id = "01hr", name = "HR" }},'))
    path = classification_cloud.env_dir / "data_access/discovered_uc_tables.auto.tfvars"
    other = 'discovered_uc_tables = ["dev_hr.people.staff"]\n\ndiscovered_table_agents = {\n  "dev_hr.people.staff" = ["HR"]\n}\n'
    path.write_text(other)

    result = classification_cloud(FAKE_GENIE_FAIL="1")
    out = result.stdout + result.stderr

    assert result.returncode != 0, out
    assert f"could not read the tables of Genie agent {SPACE_ID}; nothing was written" in out
    assert path.read_text() == other
    assert _calls(classification_cloud.logs["tf"]) == []


def test_enable_classification_with_uc_tables_never_calls_the_genie_api(classification_cloud):
    env_file = classification_cloud.env_dir / "env.auto.tfvars"
    env_file.write_text(env_file.read_text() + f'\nuc_tables = ["{TABLES[0]}"]\n')
    env_file.write_text(env_file.read_text().replace(
        f'{{ genie_space_id = "{SPACE_ID}" }}', f'{{ genie_space_id = "{SPACE_ID}", uc_tables = ["{TABLES[0]}"] }}'))

    result = classification_cloud(FAKE_GENIE_FAIL="1")

    assert result.returncode == 0, result.stdout + result.stderr
    assert _calls(classification_cloud.logs["genie"]) == []
    assert not (classification_cloud.env_dir / "data_access/discovered_uc_tables.auto.tfvars").exists()


def _discovery_env(tmp_path, spaces, discovered=None, id_to_name=None):
    (tmp_path / "data_access").mkdir()
    (tmp_path / "generated").mkdir()
    (tmp_path / "env.auto.tfvars").write_text(f"genie_spaces = {spaces}\nenable_classification = true\n")
    if discovered is not None:
        (tmp_path / "data_access/discovered_uc_tables.auto.tfvars").write_text(discovered)
    if id_to_name:
        (tmp_path / "generated/abac.auto.tfvars").write_text(f"genie_space_id_to_name = {id_to_name}\n")
    return tmp_path


@pytest.mark.parametrize(
    ("spaces", "agents"),
    [
        ('[{ genie_space_id = "a" }]', ["a"]),
        ('[{ genie_space_id = "a", name = "Pay" }, { genie_space_id = "b" }]', ["a", "b"]),
        ('[{ genie_space_id = "a", uc_tables = ["c.s.t"] }]', []),
        ('[{ genie_space_id = "" }]', []),
        ('[{ genie_space_id = "a" }]\nuc_tables = ["c.s.t"]', []),
    ],
)
def test_every_id_only_agent_is_refreshed(spaces, agents):
    config = hcl2.loads(f"genie_spaces = {spaces}\n")
    assert [s["genie_space_id"] for s in discover_agent_tables.id_only_agents(config)] == agents


def test_refresh_keeps_only_other_agents_entries():
    existing = {"c.s.old": ["Pay"], "c.s.hr": ["HR"], "c.s.both": ["Pay", "HR"], "c.s.gone": ["Old title"]}
    fresh = {"c.s.new": ["Pay"], "c.s.both": ["Pay"]}

    assert discover_agent_tables.refreshed_footprint(existing, fresh, keep={"HR"}) == {
        "c.s.hr": ["HR"], "c.s.both": ["HR", "Pay"], "c.s.new": ["Pay"],
    }


def test_other_agent_names_come_from_name_id_and_imported_title(tmp_path):
    env_dir = _discovery_env(
        tmp_path, '[{ genie_space_id = "a" }, { name = "HR", genie_space_id = "", uc_tables = ["c.s.t"] }, '
                  '{ genie_space_id = "b", uc_tables = ["c.s.u"] }]',
        id_to_name='{ b = "Cards" }')
    config = hcl2.loads((env_dir / "env.auto.tfvars").read_text())
    assert discover_agent_tables.other_agent_names(env_dir, config) == {"HR", "b", "Cards"}


def test_discovery_is_skipped_when_classification_is_off(tmp_path, monkeypatch):
    env_dir = _discovery_env(tmp_path, '[{ genie_space_id = "a" }]')
    (env_dir / "env.auto.tfvars").write_text('genie_spaces = [{ genie_space_id = "a" }]\n')
    monkeypatch.setattr(generate_abac, "fetch_tables_from_genie_space",
                        lambda *a, **k: pytest.fail("no API call when classification is off"))
    assert discover_agent_tables.main([str(env_dir)]) == 0


# ── 2. One plain `make generate`: import + discover + draft ─────────────────

@pytest.fixture
def agent_env(env_dir, monkeypatch):
    (env_dir / "env.auto.tfvars").write_text(
        f'genie_spaces = [{{ genie_space_id = "{SPACE_ID}" }}]\nenable_classification = true\n')
    calls = []

    def fetch_agent(space_id, cfg, quick_check_only=False):
        calls.append(space_id)
        genie_cfg = generate_abac.parse_genie_config_from_serialized_space(SERIALIZED)
        return list(TABLES), genie_cfg, "Payments Agent", True

    monkeypatch.setattr(generate_abac, "fetch_tables_from_genie_space", fetch_agent)
    monkeypatch.setattr(generate_abac, "fetch_tables_from_databricks",
                        lambda refs, cfg: (E2E_DDL, [("dev_fin", "payments")]))
    monkeypatch.setattr(sys, "argv", [
        "generate_abac.py", "--auth-file", str(env_dir / "auth.auto.tfvars"),
        "--groups", "payments_ops,viewers", "--out-dir", str(env_dir / "generated"),
    ])
    return env_dir, calls


def test_plain_generate_imports_the_agent_and_drafts_rules_in_one_run(agent_env, monkeypatch):
    agent_env, agent_calls = agent_env
    model_calls = []
    monkeypatch.setattr(generate_abac, "call_with_retries",
                        lambda *a, **k: model_calls.append(1) or _model_response({EMAIL: EMAIL, LIMIT: LIMIT}))

    generate_abac.main()

    assert agent_calls == [SPACE_ID]
    assert model_calls == [1]
    discovered = hcl2.loads((agent_env / "data_access/discovered_uc_tables.auto.tfvars").read_text())
    assert discovered["discovered_uc_tables"] == TABLES
    generated = hcl2.loads((agent_env / "generated/abac.auto.tfvars").read_text())
    assert generated["genie_space_configs"]["Payments Agent"]["instructions"] == "Amounts are in AUD."
    assert generated["genie_space_id_to_name"] == {SPACE_ID: "Payments Agent"}
    assert generated["fgac_policies"]
    assert (agent_env / "generated/masking_functions.sql").exists()


def test_plain_generate_without_class_tags_stops_before_the_model(agent_env, monkeypatch, capfd):
    agent_env, _calls = agent_env
    def no_tags(*_a, **_k):
        raise generate_abac.NativeClassificationRequiredError(
            "Native classification read succeeded but returned 0 class.* findings for the declared footprint")

    monkeypatch.setattr(generate_abac, "_fetch_live_classification_source", no_tags)
    monkeypatch.setattr(generate_abac, "call_with_retries",
                        lambda *a, **k: pytest.fail("the model must not be called"))

    with pytest.raises(SystemExit) as exc:
        generate_abac.main()

    out = capfd.readouterr().out
    assert exc.value.code == 1
    assert f"Tables found (2): {', '.join(TABLES)} (saved). No model was called." in out
    env_name = agent_env.name
    assert "Next: in Catalog Explorer, open the catalog > Details tab > Data classification: turn it on" in out
    assert "review the detections and exclude any false positives" in out
    assert "auto-tagging. Wait until the class.* tags appear." in out
    assert f"Prefer a script? make enable-classification ENV={env_name} turns it on" in out
    assert f"Then re-run make generate ENV={env_name}." in out
    # The discovery is persisted for enable-classification and the re-run.
    discovered = hcl2.loads((agent_env / "data_access/discovered_uc_tables.auto.tfvars").read_text())
    assert discovered["discovered_uc_tables"] == TABLES
    assert discovered["discovered_table_agents"] == {t: ["Payments Agent"] for t in TABLES}
    assert not (agent_env / "generated/abac.auto.tfvars").exists()


def test_genie_mode_never_reads_class_tags(agent_env, monkeypatch):
    agent_env, _calls = agent_env
    monkeypatch.setattr(generate_abac, "_fetch_live_classification_source",
                        lambda *a, **k: pytest.fail("MODE=genie drafts no rules"))
    monkeypatch.setattr(generate_abac, "call_with_retries",
                        lambda *a, **k: '```hcl\ngenie_space_configs = {}\n```')
    sys.argv.extend(["--mode", "genie"])

    generate_abac.main()

    generated = hcl2.loads((agent_env / "generated/abac.auto.tfvars").read_text())
    assert "Payments Agent" in generated["genie_space_configs"]
    assert not generated.get("fgac_policies")


# ── 3. make promote-to ───────────────────────────────────────────────────────

DEV_ENV = '''genie_spaces = [
  { genie_space_id = "s1", uc_tables = ["paycat.s.t"] },
]
verify_key_column = "customer_id"
'''
DEV_GENERATED = '''
groups = { pay_group = {} }
fgac_policies = [
  { name = "pay" catalog = "paycat" to_principals = ["pay_group"] },
]
tag_assignments = [
  { entity_type = "columns", entity_name = "paycat.s.t.account_id", tag_key = "gr_treatment", tag_value = "account_last4" },
]
genie_space_configs = {
  Payments = { title = "Payments" }
}
genie_space_id_to_name = { s1 = "Payments" }
'''


def _promote_cloud(root):
    cloud = root / "cloud"
    dev = cloud / "envs/dev"
    (dev / "generated").mkdir(parents=True)
    (cloud / "envs/account").mkdir(parents=True)
    (root / "Makefile").write_text(
        f"SHARED_ROOT := {SHARED}\nCLOUD_ROOT := {cloud}\nCLOUD := aws\n"
        f"include {MAKEFILE}\n")
    (dev / "env.auto.tfvars").write_text(DEV_ENV)
    (dev / "generated/abac.auto.tfvars").write_text(DEV_GENERATED)
    (dev / "generated/masking_functions.sql").write_text("-- none\n")
    bin_dir = root / "bin"
    bin_dir.mkdir()
    (bin_dir / "python3").write_text(
        "#!/bin/sh\ncase \"$1\" in *validate_abac.py) exit 0;; esac\n"
        f"exec {sys.executable} \"$@\"\n")
    (bin_dir / "python3").chmod(0o755)
    return cloud


@pytest.fixture
def promote_cloud(tmp_path):
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    cloud = _promote_cloud(tmp_path)

    def run(*args, **env):
        return subprocess.run(
            ["make", "--no-print-directory", *args], cwd=tmp_path, text=True, capture_output=True,
            env=_clean_env(PATH=f"{tmp_path / 'bin'}:{os.environ['PATH']}", **env), timeout=120)

    run.envs = cloud / "envs"
    return run


def _env_cfg(envs, name):
    return hcl2.loads((envs / name / "env.auto.tfvars").read_text())


def _promotion_settings(envs, text):
    prod = envs / "prod"
    prod.mkdir(exist_ok=True)
    (prod / "env.auto.tfvars").write_text(text)
    return prod


@pytest.mark.parametrize("catalog_map", [
    'catalog_map = { "paycat" = "ppay" }',
    'catalog_map = "paycat=ppay"',
])
def test_promote_to_reads_map_or_legacy_string_from_target_env(promote_cloud, catalog_map):
    _promotion_settings(promote_cloud.envs, f'promote_from = "dev"\n{catalog_map}\n')
    dev_before = (promote_cloud.envs / "dev/env.auto.tfvars").read_text()

    result = promote_cloud("promote-to", "ENV=prod")

    assert result.returncode == 0, result.stdout + result.stderr
    assert _env_cfg(promote_cloud.envs, "prod")["catalog_map"] == {"paycat": "ppay"}
    assert (promote_cloud.envs / "dev/env.auto.tfvars").read_text() == dev_before


def test_promote_to_command_line_overrides_win_over_template_values(promote_cloud):
    _promotion_settings(
        promote_cloud.envs,
        'promote_from = "missing"\ncatalog_map = { "<dev_catalog>" = "<prod_catalog>" }\n',
    )

    result = promote_cloud("promote-to", "ENV=prod", "FROM=dev", "CATALOG_MAP=paycat=ppay")

    assert result.returncode == 0, result.stdout + result.stderr
    config = _env_cfg(promote_cloud.envs, "prod")
    assert (config["promote_from"], config["catalog_map"]) == ("dev", {"paycat": "ppay"})


@pytest.mark.parametrize(("settings", "message"), [
    ('promote_from = "dev"\ncatalog_map = { "paycat" = "<stg_catalog>" }\n', "unfilled <...> placeholder"),
    ('promote_from = "dev"\ncatalog_map = { "paycat" = "<PROD_CATALOG>" }\n', "unfilled <...> placeholder"),
    ('promote_from = "dev"\ncatalog_map = { "paycat" = "<prod-catalog>" }\n', "unfilled <...> placeholder"),
    ('promote_from = "dev"\ncatalog_map = { "<stg_catalog>" = "ppay" }\n', "unfilled <...> placeholder"),
    ('promote_from = "dev"\ncatalog_map = { "paycat" = "prod-catalog" }\n', "not a valid UC identifier"),
    ('promote_from = "dev"\ncatalog_map = { "pay.cat" = "ppay" }\n', "not a valid UC identifier"),
    ('promote_from = "dev"\ncatalog_map = { "other" = "ppay" }\n', "unknown source catalog(s): other"),
    ('promote_from = "dev"\ncatalog_map = { "paycat" = "same", "other" = "same" }\n', "more than one source catalog to: same"),
    ('catalog_map = { "paycat" = "ppay" }\n', "promote_from in envs/prod/env.auto.tfvars is missing"),
])
def test_promote_to_saved_setting_refusals_write_nothing(promote_cloud, settings, message):
    prod = _promotion_settings(promote_cloud.envs, settings)
    before = _snapshot(promote_cloud.envs)

    result = promote_cloud("promote-to", "ENV=prod")

    assert result.returncode != 0
    assert message in result.stdout + result.stderr
    assert _snapshot(promote_cloud.envs) == before
    assert sorted(path.name for path in prod.iterdir()) == ["env.auto.tfvars"]


def test_promote_resolve_falls_back_to_generated_tag_assignments(tmp_path):
    envs = tmp_path / "envs"
    dev = envs / "dev"
    prod = envs / "prod"
    (dev / "generated").mkdir(parents=True)
    prod.mkdir()
    (dev / "env.auto.tfvars").write_text("genie_spaces = []\nuc_tables = []\n")
    (dev / "generated/abac.auto.tfvars").write_text(
        'tag_assignments = [{ entity_name = "paycat.s.t", tag_name = "class.us_ssn", tag_value = "true" }]\n'
    )
    (prod / "env.auto.tfvars").write_text(
        'promote_from = "dev"\ncatalog_map = { "paycat" = "ppay" }\n'
    )

    assert saved_settings.resolve_promote("prod", prod, envs, "", "") == "dev paycat=ppay"


def test_promote_to_first_use_saves_from_and_map_in_the_destination_only(promote_cloud):
    dev_before = (promote_cloud.envs / "dev/env.auto.tfvars").read_text()

    result = promote_cloud("promote-to", "ENV=prod", "FROM=dev", "CATALOG_MAP=paycat=ppay")

    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert "=== Cross-env promote: dev -> prod ===" in out
    assert "Saved FROM=dev CATALOG_MAP=paycat=ppay in envs/prod/env.auto.tfvars; next time just: make promote-to ENV=prod" in out
    prod = _env_cfg(promote_cloud.envs, "prod")
    assert (prod["promote_from"], prod["catalog_map"]) == ("dev", {"paycat": "ppay"})
    assert prod["uc_tables"] == ["ppay.s.t"]
    # Promote already carries the verify key to the destination.
    assert prod["verify_key_column"] == "customer_id"
    assert (promote_cloud.envs / "dev/env.auto.tfvars").read_text() == dev_before


def test_promote_to_reuses_saved_values(promote_cloud):
    assert promote_cloud("promote-to", "ENV=prod", "FROM=dev", "CATALOG_MAP=paycat=ppay").returncode == 0
    before = (promote_cloud.envs / "prod/env.auto.tfvars").read_text()

    result = promote_cloud("promote-to", "ENV=prod")

    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert "Using saved FROM and CATALOG_MAP from envs/prod/env.auto.tfvars: FROM=dev CATALOG_MAP=paycat=ppay" in out
    assert "=== Cross-env promote: dev -> prod ===" in out
    assert (promote_cloud.envs / "prod/env.auto.tfvars").read_text() == before


def test_promote_to_explicit_map_overrides_and_updates_the_saved_one(promote_cloud):
    assert promote_cloud("promote-to", "ENV=prod", "FROM=dev", "CATALOG_MAP=paycat=ppay").returncode == 0

    result = promote_cloud("promote-to", "ENV=prod", "CATALOG_MAP=paycat=ppay2")

    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert "CATALOG_MAP=paycat=ppay2 overrides the saved 'paycat=ppay'" in out
    prod = _env_cfg(promote_cloud.envs, "prod")
    assert (prod["promote_from"], prod["catalog_map"], prod["uc_tables"]) == ("dev", {"paycat": "ppay2"}, ["ppay2.s.t"])


def test_promote_to_invalid_map_fails_validation_and_keeps_the_saved_values(promote_cloud):
    assert promote_cloud("promote-to", "ENV=prod", "FROM=dev", "CATALOG_MAP=paycat=ppay").returncode == 0
    before = (promote_cloud.envs / "prod/env.auto.tfvars").read_text()

    result = promote_cloud("promote-to", "ENV=prod", "CATALOG_MAP=paycat=ppay,othercat=x")

    out = result.stdout + result.stderr
    assert result.returncode != 0
    assert "catalog_map in envs/prod/env.auto.tfvars has unknown source catalog(s): othercat" in out
    assert (promote_cloud.envs / "prod/env.auto.tfvars").read_text() == before


def test_promote_to_chains_dev_to_stg_to_prod(promote_cloud):
    assert promote_cloud("promote-to", "ENV=stg", "FROM=dev", "CATALOG_MAP=paycat=stgpay").returncode == 0
    result = promote_cloud("promote-to", "ENV=prod", "FROM=stg", "CATALOG_MAP=stgpay=ppay")

    assert result.returncode == 0, result.stdout + result.stderr
    stg, prod = _env_cfg(promote_cloud.envs, "stg"), _env_cfg(promote_cloud.envs, "prod")
    assert (stg["promote_from"], stg["catalog_map"]) == ("dev", {"paycat": "stgpay"})
    assert (prod["promote_from"], prod["catalog_map"]) == ("stg", {"stgpay": "ppay"})
    assert prod["uc_tables"] == ["ppay.s.t"]
    assert prod["verify_key_column"] == "customer_id"
    assert "ppay" in (promote_cloud.envs / "prod/generated/abac.auto.tfvars").read_text()


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["ENV=prod"], "promote_from in envs/prod/env.auto.tfvars is missing"),
        (["ENV=prod", "FROM=dev"], "catalog_map in envs/prod/env.auto.tfvars is missing or empty"),
        (["ENV=prod", "FROM=prod", "CATALOG_MAP=paycat=ppay"], "FROM must name another env"),
        (["ENV=prod", "FROM=dve", "CATALOG_MAP=paycat=ppay"], "source env 'dve' not found"),
        (["ENV=prod", "FROM=dev", "CATALOG_MAP=paycat"], "is not <src_catalog>=<dest_catalog>"),
        (["ENV=account", "FROM=dev", "CATALOG_MAP=paycat=ppay"], "This command is workspace-driven"),
        (["ENV=prod", "FROM=account", "CATALOG_MAP=paycat=ppay"], "FROM must be a workspace env"),
        (["ENV=prod", "FROM=../envs/prod", "CATALOG_MAP=paycat=ppay"], "FROM='../envs/prod' is not an env name"),
        (["ENV=prod", "FROM=../envs/dev", "CATALOG_MAP=paycat=ppay"], "FROM='../envs/dev' is not an env name"),
        (["ENV=prod", "FROM=prod/", "CATALOG_MAP=paycat=ppay"], "FROM='prod/' is not an env name"),
        (["ENV=prod", "FROM=dev/", "CATALOG_MAP=paycat=ppay"], "FROM='dev/' is not an env name"),
        (["ENV=prod", "FROM=/tmp/dev", "CATALOG_MAP=paycat=ppay"], "FROM='/tmp/dev' is not an env name"),
        (["ENV=Prod", "FROM=dev", "CATALOG_MAP=paycat=ppay"], "ENV='Prod' is not an env name"),
    ],
)
def test_promote_to_refusals_write_nothing(promote_cloud, args, message):
    result = promote_cloud("promote-to", *args)

    assert result.returncode != 0
    assert message in result.stdout + result.stderr
    assert not (promote_cloud.envs / "prod").exists()
    assert not (promote_cloud.envs / "dve").exists()


def test_promote_to_refuses_a_symlinked_source_env(promote_cloud):
    (promote_cloud.envs / "link").symlink_to(promote_cloud.envs / "dev")

    result = promote_cloud("promote-to", "ENV=prod", "FROM=link", "CATALOG_MAP=paycat=ppay")

    assert result.returncode != 0
    assert "source env 'link' not found (no envs/link/ directory)" in result.stderr
    assert not (promote_cloud.envs / "prod").exists()


def _snapshot(envs):
    return {str(p.relative_to(envs)): (p.is_symlink(), p.read_bytes() if p.is_file() and not p.is_symlink() else b"")
            for p in sorted(envs.rglob("*"))}


def test_promote_to_refuses_an_env_dir_that_is_not_envs_env(promote_cloud):
    stg = promote_cloud.envs / "stg"
    stg.mkdir()
    (stg / "env.auto.tfvars").write_text('promote_from = "dev"\ncatalog_map = "paycat=stgpay"\n')
    before = _snapshot(promote_cloud.envs)

    result = promote_cloud("promote-to", "ENV=prod", f"ENV_DIR={stg}", "FROM=dev", "CATALOG_MAP=paycat=ppay")

    assert result.returncode != 0
    assert f"ENV_DIR={stg} is not envs/prod for ENV=prod; drop ENV_DIR" in result.stderr
    assert "Cross-env promote" not in result.stdout
    assert _snapshot(promote_cloud.envs) == before


def test_promote_to_refuses_a_symlinked_destination(promote_cloud):
    stg = promote_cloud.envs / "stg"
    stg.mkdir()
    (stg / "env.auto.tfvars").write_text('promote_from = "dev"\ncatalog_map = "paycat=stgpay"\n')
    (promote_cloud.envs / "prod").symlink_to(stg)
    before = _snapshot(promote_cloud.envs)

    result = promote_cloud("promote-to", "ENV=prod", "FROM=dev", "CATALOG_MAP=paycat=ppay")

    assert result.returncode != 0
    assert "envs/prod is a symlink; promote-to writes only a real envs/prod directory" in result.stderr
    assert "Cross-env promote" not in result.stdout
    assert _snapshot(promote_cloud.envs) == before


def test_promote_to_refuses_a_destination_under_a_symlinked_parent(promote_cloud, tmp_path):
    # envs/prod/ is a real dir, but ENV_DIR names it through another path.
    alias = tmp_path / "alias"
    alias.symlink_to(promote_cloud.envs)
    before = _snapshot(promote_cloud.envs)

    result = promote_cloud("promote-to", "ENV=prod", f"ENV_DIR={alias / 'prod'}", "FROM=dev",
                           "CATALOG_MAP=paycat=ppay")

    assert result.returncode != 0
    assert "is not envs/prod for ENV=prod" in result.stderr
    assert _snapshot(promote_cloud.envs) == before


def test_promote_to_refuses_a_saved_promote_from_that_is_a_path(promote_cloud):
    prod = promote_cloud.envs / "prod"
    prod.mkdir()
    text = 'promote_from = "../dev"\ncatalog_map = "paycat=ppay"\n'
    (prod / "env.auto.tfvars").write_text(text)

    result = promote_cloud("promote-to", "ENV=prod")

    assert result.returncode != 0
    assert "promote_from in envs/prod/env.auto.tfvars='../dev' is not an env name" in result.stderr
    assert (prod / "env.auto.tfvars").read_text() == text
    assert sorted(p.name for p in prod.iterdir()) == ["env.auto.tfvars"]


def test_promote_to_ignores_from_and_map_leaked_from_the_shell(promote_cloud):
    result = promote_cloud("promote-to", "ENV=prod", FROM="dev", CATALOG_MAP="paycat=ppay")

    assert result.returncode != 0
    assert "promote_from in envs/prod/env.auto.tfvars is missing" in result.stderr
    assert not (promote_cloud.envs / "prod").exists()


def test_promote_to_new_source_needs_its_own_map(promote_cloud):
    assert promote_cloud("promote-to", "ENV=stg", "FROM=dev", "CATALOG_MAP=paycat=stgpay").returncode == 0
    assert promote_cloud("promote-to", "ENV=prod", "FROM=dev", "CATALOG_MAP=paycat=ppay").returncode == 0

    result = promote_cloud("promote-to", "ENV=prod", "FROM=stg")

    assert result.returncode != 0
    assert "FROM=stg differs from the saved promote_from (dev); pass CATALOG_MAP" in result.stderr
    assert _env_cfg(promote_cloud.envs, "prod")["promote_from"] == "dev"


def _tree(envs):
    return {
        str(path.relative_to(envs)): path.read_text()
        for path in sorted(envs.rglob("*"))
        if path.is_file() and not path.is_symlink()
    }


def test_promote_to_is_exactly_the_legacy_cross_env_promote(tmp_path):
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    results = {}
    for name, args in (
        ("legacy", ["promote", "SOURCE_ENV=dev", "DEST_ENV=prod", "DEST_CATALOG_MAP=paycat=ppay"]),
        ("wrapper", ["promote-to", "ENV=prod", "FROM=dev", "CATALOG_MAP=paycat=ppay"]),
    ):
        root = tmp_path / name
        root.mkdir()
        cloud = _promote_cloud(root)
        result = subprocess.run(
            ["make", "--no-print-directory", *args], cwd=root, text=True, capture_output=True,
            env=_clean_env(PATH=f"{root / 'bin'}:{os.environ['PATH']}"), timeout=120)
        assert result.returncode == 0, result.stdout + result.stderr
        tree = {k: v.replace(str(root), "<root>") for k, v in _tree(cloud / "envs").items()}
        results[name] = (tree, result.stdout.replace(str(root), "<root>"))

    legacy_tree, legacy_out = results["legacy"]
    wrapper_tree, wrapper_out = results["wrapper"]
    assert json.loads(wrapper_tree.pop("prod/generated/expected_classification.json")) == {
        "ppay.s.t.account_id": ["gr_treatment:account_last4"]
    }
    wrapper_prod = hcl2.loads(wrapper_tree.pop("prod/env.auto.tfvars"))
    legacy_prod = hcl2.loads(legacy_tree.pop("prod/env.auto.tfvars"))
    assert wrapper_prod.pop("promote_from") == "dev"
    assert wrapper_prod.pop("catalog_map") == {"paycat": "ppay"}
    assert "promote_from" not in legacy_prod
    assert "catalog_map" not in legacy_prod
    assert wrapper_prod == legacy_prod
    assert wrapper_tree == legacy_tree
    assert wrapper_out.startswith(legacy_out)


def test_legacy_promote_keeps_promote_to_saved_values(promote_cloud):
    assert promote_cloud("promote-to", "ENV=prod", "FROM=dev", "CATALOG_MAP=paycat=ppay").returncode == 0

    result = promote_cloud("promote", "SOURCE_ENV=dev", "DEST_ENV=prod", "DEST_CATALOG_MAP=paycat=ppay")

    assert result.returncode == 0, result.stdout + result.stderr
    prod = _env_cfg(promote_cloud.envs, "prod")
    assert (prod["promote_from"], prod["catalog_map"]) == ("dev", {"paycat": "ppay"})


def test_promote_to_calls_promote_with_every_cross_env_variable(tmp_path):
    """promote-to hands promote all of its cross-env inputs explicitly, so
    nothing leaked from the shell (or the outer command line) can redirect it."""
    cloud = _promote_cloud(tmp_path)
    log = tmp_path / "calls"
    stub = tmp_path / "make-stub"
    stub.write_text(f"#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{log}'\n")
    stub.chmod(0o755)
    leaked = _clean_env(PATH=f"{tmp_path / 'bin'}:{os.environ['PATH']}", SOURCE_ENV="x", DEST_ENV="x",
                        DEST_CATALOG_MAP="a=b", SOURCE_ENV_DIR="/x", DEST_ENV_DIR="/x")

    result = subprocess.run(["make", "--no-print-directory", "promote-to", "ENV=prod", "FROM=dev",
                             "CATALOG_MAP=paycat=ppay", f"MAKE={stub}"],
                            cwd=tmp_path, text=True, capture_output=True, env=leaked)

    assert result.returncode == 0, result.stdout + result.stderr
    dev, prod = cloud / "envs/dev", cloud / "envs/prod"
    assert [shlex.split(line) for line in log.read_text().splitlines()] == [[
        "--no-print-directory", "promote", "ENV=dev", f"ENV_DIR={dev}", "SOURCE_ENV=dev",
        f"SOURCE_ENV_DIR={dev}", "DEST_ENV=prod", f"DEST_ENV_DIR={prod}", "DEST_CATALOG_MAP=paycat=ppay",
    ]]


# ── 4. A passing rehearse / release saves an explicit VERIFY_KEY_COLUMN ─────
# Only when verify-access's result file proves a passing mask check paired by it.

MASK_PASS = {"passed": True, "mask_checks_passed": 1, "mask_checks_passed_by_key": {"customer_id": 1},
             "row_filter_checks_passed": 0}
ROW_FILTER_ONLY = {"passed": True, "mask_checks_passed": 0, "mask_checks_passed_by_key": {},
                   "row_filter_checks_passed": 2}


def _stub_make(tmp_path, fail=None, result=None, result_path=None):
    log = tmp_path / "calls"
    stub = tmp_path / "make-stub"
    body = ["#!/bin/sh", f"printf '%s\\n' \"$*\" >> '{log}'"]
    if result is not None:
        body.append(f"""if [ "$1" = "verify-access" ]; then printf '%s' '{json.dumps(result)}' > '{result_path}'; fi""")
    if fail:
        body.append(f'[ "$1" = "{fail}" ] && exit 1')
    stub.write_text("\n".join(body + ["exit 0"]) + "\n")
    stub.chmod(0o755)
    return stub


def _run_target(tmp_path, target, env_name, *extra, fail=None, result=MASK_PASS,
                env_text="enable_classification = true\n"):
    env_dir = tmp_path / env_name
    (env_dir / "generated").mkdir(parents=True, exist_ok=True)
    env_file = env_dir / "env.auto.tfvars"
    if not env_file.exists():
        env_file.write_text(env_text)
    stub = _stub_make(tmp_path, fail, result, env_dir / "generated/.verify_access.json")
    result = subprocess.run(
        ["make", target, f"ENV={env_name}", f"ENV_DIR={env_dir}", f"MAKE={stub}", *extra],
        cwd=REPO / "aws", text=True, capture_output=True, env=_clean_env())
    return result, env_file


@pytest.mark.parametrize(("target", "env_name"), [("rehearse", "dev"), ("release", "prod")])
def test_passing_mask_check_saves_the_explicit_verify_key(tmp_path, target, env_name):
    result, env_file = _run_target(tmp_path, target, env_name, "VERIFY_KEY_COLUMN= customer_id ")

    assert result.returncode == 0, result.stdout + result.stderr
    assert hcl2.loads(env_file.read_text())["verify_key_column"] == "customer_id"
    assert env_file.read_text().startswith("enable_classification = true\n")
    assert f"Saved VERIFY_KEY_COLUMN=customer_id as verify_key_column in {env_file}" in result.stdout


@pytest.mark.parametrize(("target", "env_name"), [("rehearse", "dev"), ("release", "prod")])
@pytest.mark.parametrize(
    "proof",
    [ROW_FILTER_ONLY, None,
     {**MASK_PASS, "mask_checks_passed_by_key": {"account_id": 1}},
     {**MASK_PASS, "passed": False}],
    ids=["row-filter-only", "no-result-file", "other-key", "not-passed"],
)
def test_pass_without_mask_proof_never_saves_the_key(tmp_path, target, env_name, proof):
    result, env_file = _run_target(tmp_path, target, env_name, "VERIFY_KEY_COLUMN=customer_id", result=proof)

    assert result.returncode == 0, result.stdout + result.stderr
    assert env_file.read_text() == "enable_classification = true\n"
    assert "VERIFY_KEY_COLUMN=customer_id not saved: verify-access proved no mask check paired by it" in result.stdout


@pytest.mark.parametrize(("target", "env_name"), [("rehearse", "dev"), ("release", "prod")])
def test_failed_verify_never_saves_the_key(tmp_path, target, env_name):
    result, env_file = _run_target(tmp_path, target, env_name, "VERIFY_KEY_COLUMN=customer_id", fail="verify-access")

    assert result.returncode != 0
    assert env_file.read_text() == "enable_classification = true\n"


def test_run_without_a_key_leaves_the_saved_one(tmp_path):
    result, env_file = _run_target(tmp_path, "rehearse", "dev", env_text='verify_key_column = "account_id"\n')

    assert result.returncode == 0, result.stdout + result.stderr
    assert env_file.read_text() == 'verify_key_column = "account_id"\n'


def test_different_explicit_key_updates_the_saved_one_with_a_note(tmp_path):
    result, env_file = _run_target(tmp_path, "rehearse", "dev", "VERIFY_KEY_COLUMN=customer_id",
                                   env_text='verify_key_column = "account_id"  # pairs rows\nx = 1\n')

    assert result.returncode == 0, result.stdout + result.stderr
    assert env_file.read_text() == 'verify_key_column = "customer_id"  # pairs rows\nx = 1\n'
    assert "changed from 'account_id' to 'customer_id' (the VERIFY_KEY_COLUMN you passed)" in result.stdout


MASKED_DATA_ACCESS = '''fgac_policies = [
  {
    name = "gr_mask_redact"
    policy_type = "POLICY_TYPE_COLUMN_MASK"
    catalog = "cat"
    to_principals = ["analysts"]
    match_condition = "hasTagValue('gr_treatment', 'redact')"
    match_alias = "gr_treatment_redact"
    function_name = "mask_redact"
    function_catalog = "cat"
    function_schema = "sch"
  },
]
'''


@pytest.mark.parametrize("env_text", [
    'verify_key_column = "customer_id"\n',  # promoted from dev (legacy single key)
    "enable_classification = true\n",       # no key: picked per table
])
def test_release_needs_no_key_and_proves_keys_before_applying(tmp_path, env_text):
    env_dir = tmp_path / "prod"
    (env_dir / "data_access").mkdir(parents=True)
    (env_dir / "data_access/abac.auto.tfvars").write_text(MASKED_DATA_ACCESS)

    result, _env_file = _run_target(tmp_path, "release", "prod", env_text=env_text)
    calls = (tmp_path / "calls").read_text().splitlines()

    assert result.returncode == 0, result.stdout + result.stderr
    keys = calls.index("verify-access-keys ENV=prod")
    assert keys < calls.index("apply ENV=prod APPLY_FLAGS= _EXPOSURE_DERIVED=1")
    assert "verify-access ENV=prod VERIFY_REQUIRE_MASKS=1" in calls


def _proof(tmp_path, payload=MASK_PASS):
    path = tmp_path / ".verify_access.json"
    path.write_text(json.dumps(payload))
    return path


def test_save_verify_key_is_idempotent_and_keeps_the_file_mode(tmp_path, capsys):
    env_file = tmp_path / "env.auto.tfvars"
    env_file.write_text('# keep\nverify_key_column = ""\n')
    env_file.chmod(0o640)

    assert saved_settings.save_verify_key(env_file, "customer_id", _proof(tmp_path)) == 0
    assert saved_settings.save_verify_key(env_file, "customer_id", _proof(tmp_path)) == 0

    assert env_file.read_text() == '# keep\nverify_key_column = "customer_id"\n'
    assert env_file.stat().st_mode & 0o777 == 0o640
    assert capsys.readouterr().out.count("Saved VERIFY_KEY_COLUMN") == 1


def test_save_refuses_a_duplicated_setting(tmp_path, capsys):
    env_file = tmp_path / "env.auto.tfvars"
    text = 'verify_key_column = "a"\nverify_key_column = "b"\n'
    env_file.write_text(text)

    assert saved_settings.save_verify_key(env_file, "customer_id", _proof(tmp_path)) == 0

    assert env_file.read_text() == text
    assert "VERIFY_KEY_COLUMN not saved" in capsys.readouterr().out


@pytest.mark.parametrize("bad", ["", "{", '{"passed": true, "mask_checks_passed_by_key": []}'])
def test_unreadable_proof_is_no_proof(tmp_path, bad):
    path = tmp_path / ".verify_access.json"
    path.write_text(bad)
    assert not saved_settings.key_proven(path, "customer_id")
    assert not saved_settings.key_proven(tmp_path / "missing.json", "customer_id")


def _live_main(monkeypatch, tmp_path, spec, statuses):
    import verify_effective_access as vea

    spec_path = tmp_path / "spec.json"
    spec_path.write_text(json.dumps(spec))

    def live(spec_obj, *_a, **_k):
        report = vea.EffectiveAccessReport()
        for check in list(spec_obj.column_masks) + list(spec_obj.row_filters):
            kind = "column-mask" if isinstance(check, vea.ColumnMaskCheck) else "row-filter"
            report.add(vea.CheckResult(kind, check.describe(), statuses[kind], "x"))
        return report

    monkeypatch.setattr(vea, "verify_effective_access_live", live)
    result_file = tmp_path / "result.json"
    rc = vea.main(["--spec", str(spec_path), "--live", "--auth-file", str(tmp_path / "auth"),
                   "--result-file", str(result_file)])
    return rc, json.loads(result_file.read_text())


MASK = {"table": "c.s.t", "column": "email", "key_column": "customer_id",
        "masked_principals": ["viewers"], "unmasked_principals": ["ops"]}
ROW = {"table": "c.s.t", "restricted_principals": ["viewers"], "unrestricted_principals": ["ops"]}


def test_verify_result_file_counts_mask_passes_per_key(monkeypatch, tmp_path, capsys):
    rc, result = _live_main(monkeypatch, tmp_path, {"column_masks": [MASK], "row_filters": [ROW]},
                            {"column-mask": "PASS", "row-filter": "PASS"})
    assert rc == 0
    assert result == {"passed": True, "mask_checks_passed": 1,
                      "masked_tables": ["c.s.t"], "mask_keys_complete": False,
                      "mask_checks_passed_by_key": {"customer_id": 1}, "mask_keys_proven_by_table": {},
                      "row_filter_checks_passed": 1}


def test_verify_result_file_row_filter_only_proves_no_key(monkeypatch, tmp_path, capsys):
    rc, result = _live_main(monkeypatch, tmp_path, {"row_filters": [ROW]},
                            {"column-mask": "PASS", "row-filter": "PASS"})
    assert rc == 0
    assert result["passed"] is True and result["mask_checks_passed_by_key"] == {}


def test_verify_result_file_records_a_failed_mask(monkeypatch, tmp_path, capsys):
    rc, result = _live_main(monkeypatch, tmp_path, {"column_masks": [MASK]},
                            {"column-mask": "FAIL", "row-filter": "PASS"})
    assert rc == 1
    assert result["passed"] is False and result["mask_checks_passed"] == 0


def test_verify_access_target_writes_and_clears_the_result_file():
    makefile = MAKEFILE.read_text()
    body = makefile[makefile.index("\nverify-access:"):]
    body = body[:body.index("\n\n")]
    assert '@rm -f "$(_VERIFY_RESULT)"' in body
    assert '--live --result-file "$(_VERIFY_RESULT)"' in body
    assert '--result-file "$(_VERIFY_RESULT)"' in makefile[makefile.index("_SAVE_VERIFY_KEY ="):]


# ── 5. Make wiring and dry runs ─────────────────────────────────────────────

@pytest.mark.parametrize("cloud", ["aws", "azure"])
@pytest.mark.parametrize(
    "args",
    [
        ["setup", "ENV=dev"],
        ["promote-to", "ENV=prod", "FROM=dev", "CATALOG_MAP=a=b"],
        ["release", "ENV=prod"],
        ["promote", "SOURCE_ENV=dev", "DEST_ENV=prod", "DEST_CATALOG_MAP=a=b"],
    ],
    ids=" ".join,
)
def test_dry_runs_have_no_side_effects(tmp_path, cloud, args):
    cloud_root = tmp_path / cloud
    cloud_root.mkdir()
    result = subprocess.run(
        ["make", "-n", *args, f"CLOUD_ROOT={cloud_root}", f"SHARED_ROOT={SHARED}"],
        cwd=REPO / cloud, text=True, capture_output=True, env=_clean_env(), timeout=120)

    assert result.returncode == 0, result.stdout + result.stderr
    assert not (cloud_root / "envs/prod").exists()
