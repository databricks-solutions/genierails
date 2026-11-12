"""PR #183 follow-ups N1–N3: auth parse secrecy, dangling auth symlinks, DATA_ACCESS_SUBDIR."""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
SCRIPTS = SHARED / "scripts"
for _path in (SHARED, SCRIPTS):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from scripts import release_gate as rg  # noqa: E402

# Marker that must never appear in any parser-facing output.
SECRET_FRAGMENT = "!Sekret$Value"
MALFORMED_AUTH = (
    'databricks_account_id = "123"\n'
    'databricks_workspace_host = "https://workspace.example"\n'
    f"databricks_client_secret = Zq9{SECRET_FRAGMENT}\n"
)


def _write_malformed(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(MALFORMED_AUTH)
    return path


def _assert_no_secret(text: str) -> None:
    assert SECRET_FRAGMENT not in text
    assert "Sekret" not in text


# -- N1: malformed auth never echoes the secret --------------------------------

def test_account_auth_endpoint_hash_refuses_malformed_auth_without_secret(tmp_path, capsys):
    env_dir = tmp_path / "envs" / "account"
    auth = _write_malformed(env_dir / "auth.auto.tfvars")
    with pytest.raises(rg.Refused, match="is not valid HCL") as raised:
        rg.account_input_hashes(env_dir)
    _assert_no_secret(str(raised.value))
    _assert_no_secret(capsys.readouterr().out + capsys.readouterr().err)
    # CLI path uses the same gate (generic Exception handler must not fire).
    rc = rg.main(["account-tag-sync-diff", "--env-dir", str(env_dir), "--env", "account",
                  "--out", str(tmp_path / "out.json")])
    captured = capsys.readouterr()
    assert rc == 1
    assert "is not valid HCL" in captured.err
    _assert_no_secret(captured.out + captured.err)
    assert auth.is_file()


def test_sync_tag_policies_load_auth_refuses_malformed_without_secret(tmp_path):
    _write_malformed(tmp_path / "auth.auto.tfvars")
    (tmp_path / "abac.auto.tfvars").write_text("tag_policies = [{ key = \"k\", values = [\"v\"] }]\n")
    result = subprocess.run(
        [sys.executable, str(SCRIPTS / "sync_tag_policies.py"), "--dry-run", "--json",
         str(tmp_path / "abac.auto.tfvars")],
        cwd=tmp_path, text=True, capture_output=True, check=False,
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "is not valid HCL" in combined
    _assert_no_secret(combined)


def test_wait_for_tag_policies_load_auth_refuses_malformed_without_secret(tmp_path):
    _write_malformed(tmp_path / "auth.auto.tfvars")
    result = subprocess.run(
        [sys.executable, "-c",
         "from pathlib import Path; import sys; "
         f"sys.path.insert(0, {str(SCRIPTS)!r}); "
         "import wait_for_tag_policies as w; "
         f"w.WORK_DIR = Path({str(tmp_path)!r}); w._load_auth()"],
        text=True, capture_output=True, check=False,
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "is not valid HCL" in combined
    _assert_no_secret(combined)


def test_auth_configured_malformed_is_false_without_raising(tmp_path):
    from auth_configured import auth_configured

    auth = _write_malformed(tmp_path / "auth.auto.tfvars")
    assert auth_configured(auth) is False


def test_generate_abac_credentials_parse_omits_secret(tmp_path, capsys):
    import generate_abac

    auth = _write_malformed(tmp_path / "auth.auto.tfvars")
    cfg = generate_abac._load_tfvars(auth, "credentials")
    assert cfg == {}
    captured = capsys.readouterr()
    assert "not valid HCL" in captured.out
    _assert_no_secret(captured.out + captured.err)


def test_verify_load_auth_refuses_malformed_without_secret(tmp_path):
    import verify_effective_access as vea

    auth = _write_malformed(tmp_path / "auth.auto.tfvars")
    with pytest.raises(ValueError, match="is not valid HCL") as raised:
        vea.load_auth(auth)
    _assert_no_secret(str(raised.value))


def test_custom_masks_workspace_client_refuses_malformed_without_secret(tmp_path):
    from scripts import custom_masks as cm

    auth = _write_malformed(tmp_path / "auth.auto.tfvars")
    with pytest.raises(cm.Refused, match="is not valid HCL") as raised:
        cm.workspace_client(auth)
    _assert_no_secret(str(raised.value))


def test_footprint_load_hcl_auth_omits_secret(tmp_path):
    from scripts.footprint import FootprintError, load_hcl

    auth = _write_malformed(tmp_path / "auth.auto.tfvars")
    with pytest.raises(FootprintError, match="not valid HCL") as raised:
        load_hcl(auth)
    _assert_no_secret(str(raised.value))


def test_audit_schema_drift_auth_omits_secret(tmp_path):
    from scripts import audit_schema_drift as asd

    auth = _write_malformed(tmp_path / "auth.auto.tfvars")
    with pytest.raises(RuntimeError, match="not valid HCL") as raised:
        asd._load_hcl(auth)
    _assert_no_secret(str(raised.value))


def test_audit_schema_drift_refuses_python_hcl2_8_without_secret(monkeypatch, tmp_path):
    from scripts import audit_schema_drift as asd
    from scripts import hcl2_guard

    auth = tmp_path / "auth.auto.tfvars"
    auth.write_text('databricks_client_id = "client"\n'
                    'databricks_workspace_host = "https://dbc.example.com"\n'
                    f'databricks_client_secret = "{SECRET_FRAGMENT}"\n')
    monkeypatch.setattr(hcl2_guard.importlib.metadata, "version", lambda _name: "8.1.4")
    with pytest.raises(hcl2_guard.Hcl2VersionError) as raised:
        asd._load_hcl(auth)
    message = str(raised.value)
    assert message == (
        "ERROR: installed python-hcl2 8.1.4 is unsupported; "
        "fix with: pip install 'python-hcl2>=7.3.1,<8'"
    )
    _assert_no_secret(message)


# -- N2: dangling auth symlink is refused, never written through ---------------

@pytest.mark.skipif(__import__("shutil").which("make") is None, reason="make not installed")
def test_prepare_env_refuses_dangling_auth_symlink(tmp_path):
    REPO = SHARED.parent
    CLOUD_DIR_LINE = next(
        line for line in (REPO / "aws/Makefile").read_text().splitlines()
        if line.startswith("override GENIERAILS_CLOUD_DIR"))
    root = tmp_path / "cloud"
    (root / "envs").mkdir(parents=True)
    (root / "Makefile").write_text(
        f"{CLOUD_DIR_LINE}\nSHARED_ROOT := {SHARED}\nCLOUD_ROOT := {root}\nCLOUD := aws\ninclude {SHARED / 'Makefile.shared'}\n")
    env_dir = root / "envs" / "dev"
    env_dir.mkdir(parents=True)
    dangling = env_dir / "auth.auto.tfvars"
    dangling.symlink_to("missing-target.auto.tfvars")
    victim = env_dir / "missing-target.auto.tfvars"
    assert not victim.exists()

    env = {k: v for k, v in os.environ.items()
           if k not in ("MAKEFLAGS", "MAKELEVEL", "MAKEFILES", "GENIERAILS_CLOUD_DIR", "ENV", "ENV_DIR")}
    result = subprocess.run(
        ["make", "--no-print-directory", "setup", "ENV=dev"],
        cwd=root, text=True, capture_output=True, timeout=120, env=env,
    )
    assert result.returncode != 0, result.stdout + result.stderr
    assert "dangling symlink" in (result.stdout + result.stderr)
    assert dangling.is_symlink()
    assert not victim.exists(), "setup must not write through a dangling auth symlink"


# -- N3: DATA_ACCESS_SUBDIR override -------------------------------------------

def test_data_access_subdir_override_used_for_live_policies_path(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_ACCESS_SUBDIR", "gov_layer")
    assert rg.data_access_subdir() == "gov_layer"
    assert rg.runtime_dirs() == {"gov_layer"}
    env_dir = tmp_path / "envs" / "dev"
    auth = env_dir / "gov_layer" / "auth.auto.tfvars"
    auth.parent.mkdir(parents=True)
    auth.write_text('databricks_workspace_host = "https://example.invalid"\n')
    seen: list[Path] = []

    def fake_lister(path: Path):
        seen.append(Path(path))
        return lambda _catalog: set()

    import remap_generated_config
    monkeypatch.setattr(remap_generated_config, "live_policy_lister", fake_lister)
    rg.live_policies(env_dir, ["cat"])
    assert seen == [auth]


def test_data_access_subdir_override_skips_reserved_env_name(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_ACCESS_SUBDIR", "gov_layer")
    account = tmp_path / "envs" / "account"
    account.mkdir(parents=True)
    (tmp_path / "envs" / "gov_layer").mkdir()
    (tmp_path / "envs" / "gov_layer" / "env.auto.tfvars").write_text(
        'governance_mode = "deterministic"\n')
    (tmp_path / "envs" / "dev").mkdir()
    (tmp_path / "envs" / "dev" / "env.auto.tfvars").write_text(
        'governance_mode = "legacy"\n')
    assert rg.any_deterministic_env(account) is False


# -- Every auth parser, in a subprocess ----------------------------------------
#
# Each parser reads a malformed auth.auto.tfvars in a fresh interpreter. The
# secret must not reach stdout, stderr or the traceback, and no parser or
# decoder exception may stay attached to the one raised (__cause__ or
# __context__, even when suppressed): a debugger or logger can still reach it,
# and it holds the secret even when its message doesn't (a lark error's parser
# state, a UnicodeDecodeError's bytes).

MALFORMED_VARIANTS = {
    "unquoted": MALFORMED_AUTH.encode(),
    "unterminated": (
        'databricks_workspace_host = "https://workspace.example"\n'
        f'databricks_client_secret = "Zq9{SECRET_FRAGMENT}\n'
        'databricks_client_id = "abc"\n'
    ).encode(),
    "non_utf8": (
        'databricks_workspace_host = "https://workspace.example"\n'
        f'databricks_client_secret = "Zq9{SECRET_FRAGMENT}'
    ).encode() + b'\xff\xfe"\n',
}

HARNESS = """
import os, sys, traceback
from pathlib import Path
sys.path[:0] = [{shared!r}, {scripts!r}]
AUTH = Path({auth!r})
ENV = AUTH.parent
TMP = Path({tmp!r})


def _attached(exc):
    seen, todo = [], [exc]
    while todo:
        item = todo.pop()
        if item is None or any(item is s for s in seen):
            continue
        seen.append(item)
        todo += [item.__cause__, item.__context__]
    return seen


try:
{body}
except BaseException as exc:
    for item in _attached(exc):
        kind = type(item)
        print("ATTACHED", kind.__module__ + "." + kind.__qualname__, repr(item), repr(item.args), str(item),
              file=sys.stderr)
    traceback.print_exception(exc)
    raise
"""

DETERMINISTIC_ENV = ('governance_mode = "deterministic"\n'
                     'deployer_principal_id = "11111111-2222-3333-4444-555555555555"\n')
LEGACY_DEMOS = ("asean_bank_demo", "aus_bank_demo", "india_bank_demo")


def _demo(name: str) -> str:
    return ("import importlib.util as u\n"
            f"spec = u.spec_from_file_location('demo', {str(SHARED / 'examples/legacy' / name / 'setup_demo.py')!r})\n"
            "demo = u.module_from_spec(spec); spec.loader.exec_module(demo)\n"
            "demo._load_auth(AUTH)")


# (id, env dir name, extra files, body, text the output must contain, or None
# when the parser swallows the failure).
AUTH_PARSERS = [
    ("release_gate.account_input_hashes", "account", {},
     "from scripts import release_gate as rg\nrg.account_input_hashes(ENV)", "is not valid HCL"),
    ("release_gate.main_account_tag_sync_diff", "account", {},
     "from scripts import release_gate as rg\n"
     "sys.exit(rg.main(['account-tag-sync-diff', '--env-dir', str(ENV), '--env', 'account', "
     "'--out', str(TMP / 'out.json')]))", "is not valid HCL"),
    ("sync_tag_policies._load_auth", "dev", {},
     "import sync_tag_policies as s\ns.WORK_DIR = ENV\ns._load_auth()", "is not valid HCL"),
    ("wait_for_tag_policies._load_auth", "dev", {},
     "import wait_for_tag_policies as w\nw.WORK_DIR = ENV\nw._load_auth()", "is not valid HCL"),
    ("auth_configured.auth_configured", "dev", {},
     "from auth_configured import auth_configured\nassert auth_configured(AUTH) is False", None),
    ("generate_abac._load_tfvars_strict", "dev", {},
     "import generate_abac as g\ng._load_tfvars(AUTH, 'credentials', strict=True)", "not valid HCL"),
    ("generate_abac.load_auth_config", "dev", {},
     "import generate_abac as g\ng.load_auth_config(AUTH)", "not valid HCL"),
    ("verify_effective_access.load_auth", "dev", {},
     "import verify_effective_access as v\nv.load_auth(AUTH)", "is not valid HCL"),
    ("custom_masks.workspace_client", "dev", {},
     "from scripts import custom_masks as c\nc.workspace_client(AUTH)", "is not valid HCL"),
    ("footprint.load_hcl", "dev", {},
     "from scripts.footprint import load_hcl\nload_hcl(AUTH)", "not valid HCL"),
    ("audit_schema_drift._get_sdk_client", "dev", {},
     "from scripts import audit_schema_drift as a\na._get_sdk_client(ENV)", "not valid HCL"),
    ("policy_generations._load", "dev", {},
     "from scripts import policy_generations as p\np._load(AUTH)", "is not valid HCL"),
    ("prepare_classification_config._load", "dev", {},
     "from scripts import prepare_classification_config as p\np._load(AUTH)", "is not valid HCL"),
    ("remap_env_config.main", "dev", {"env.auto.tfvars": 'uc_tables = ["cat.sch.t"]\n'},
     "from scripts import remap_env_config as r\n"
     "sys.argv = ['remap_env_config.py', str(ENV), str(TMP / 'dest'), 'cat=prod_cat']\nr.main()",
     "is not valid HCL"),
    ("remap_generated_config.live_policy_lister", "dev", {},
     "import auth_configured\nauth_configured.auth_configured = lambda path: True\n"
     "from remap_generated_config import live_policy_lister\nprint(live_policy_lister(AUTH)('cat'))", None),
    ("run_integration_tests._load_auth_cfg", "dev", {},
     "from scripts import run_integration_tests as r\nr._load_auth_cfg(AUTH)", "is not valid HCL"),
    ("run_integration_tests._load_tfvars", "dev", {},
     "from scripts import run_integration_tests as r\nr._load_tfvars(AUTH)", "is not valid HCL"),
    ("run_integration_tests._resolve_warehouse_id", "dev", {},
     "from scripts import run_integration_tests as r\nprint(r._resolve_warehouse_id(AUTH, ''))", None),
    ("setup_test_data._load_auth", "dev", {},
     "from scripts import setup_test_data as s\ns._load_auth(AUTH)", "is not valid HCL"),
    ("live_mask_library.main", "dev", {"env.auto.tfvars": 'sql_warehouse_id = "w"\n'},
     "from scripts import live_mask_library as l\n"
     "sys.argv = ['live_mask_library.py', '--auth-file', str(AUTH), '--env-file', str(ENV / 'env.auto.tfvars')]\n"
     "sys.exit(l.main())", "is not valid HCL"),
    ("agent_files._load_hcl", "dev", {},
     "import agent_files\nagent_files._load_hcl(AUTH)", "is not valid HCL"),
    ("agent_ownership._api", "dev", {},
     "from scripts import agent_ownership as a\na._api(ENV)", "is not valid HCL"),
    ("classification_completeness.live_classified_columns", "dev", {},
     "from scripts import classification_completeness as c\nc.live_classified_columns(ENV, 30)",
     "is not valid HCL"),
    ("coverage_gate.deployment_binding", "dev", {},
     "from scripts import coverage_gate as c\nc.deployment_binding(ENV)", "is not valid HCL"),
    ("deploy_masking_functions.load_credentials", "dev", {},
     "import deploy_masking_functions as d\nd.load_credentials(str(AUTH), 'https://workspace.example')",
     "is not valid HCL"),
    ("deployer_identity.apply_problems", "dev", {"env.auto.tfvars": DETERMINISTIC_ENV},
     "import deployer_identity as d\nd.apply_problems(ENV)", "is not valid HCL"),
    ("validate_abac.validate_auth", "dev", {},
     "import validate_abac as v\nresult = v.ValidationResult()\n"
     "v.validate_auth({}, result, ENV / 'abac.auto.tfvars')\nprint(result.warnings)", "not valid HCL"),
    ("genie_adopt_preflight.load_auth", "dev", {},
     "from scripts import genie_adopt_preflight as g\ng.load_auth(ENV)", "is not valid HCL"),
    ("run_parallel_tests._cleanup_account_resources", "dev", {},
     "from scripts import run_parallel_tests as r\nr._cleanup_account_resources(str(AUTH), 'sfx', 'aws')", None),
    ("e2e.driver.read_tfvars", "dev", {},
     "from scripts.e2e import driver\ndriver.read_tfvars(AUTH)", None),
    *[(f"legacy_{name}._load_auth", "dev", {}, _demo(name), "is not valid HCL") for name in LEGACY_DEMOS],
]


@pytest.mark.parametrize("variant", sorted(MALFORMED_VARIANTS))
@pytest.mark.parametrize("parser_id, env_name, extra, body, expected",
                         AUTH_PARSERS, ids=[case[0] for case in AUTH_PARSERS])
def test_auth_parser_never_surfaces_secret(tmp_path, parser_id, env_name, extra, body, expected, variant):
    env_dir = tmp_path / "cloud" / "envs" / env_name
    env_dir.mkdir(parents=True)
    auth = env_dir / "auth.auto.tfvars"
    auth.write_bytes(MALFORMED_VARIANTS[variant])
    for name, text in extra.items():
        (env_dir / name).write_text(text)
    script = HARNESS.format(shared=str(SHARED), scripts=str(SCRIPTS), auth=str(auth), tmp=str(tmp_path),
                            body="\n".join("    " + line for line in body.splitlines()))
    env = {k: v for k, v in os.environ.items() if not k.startswith(("DATABRICKS_", "ARM_", "AZURE_"))}
    env |= {"PYTHONUTF8": "1", "PIP_NO_INDEX": "1", "GENIERAILS_STATE_DIR": ""}
    result = subprocess.run([sys.executable, "-c", script], cwd=env_dir, text=True, capture_output=True,
                            env=env, timeout=120, check=False)
    combined = result.stdout + result.stderr
    _assert_no_secret(combined)
    attached = [line.split()[1] for line in result.stderr.splitlines() if line.startswith("ATTACHED ")]
    assert not [kind for kind in attached
                if kind.startswith(("lark.", "hcl2.")) or kind.endswith("UnicodeDecodeError")], combined
    if expected is not None:
        assert expected in combined, combined
    if parser_id == "e2e.driver.read_tfvars" and variant == "non_utf8":
        assert "is not valid UTF-8" in combined, combined


# -- B2: DATA_ACCESS_SUBDIR through the coverage-gate state path ----------------

def _gov_layer_env(tmp_path: Path) -> tuple[Path, Path]:
    from scripts import coverage_gate as cg

    env = tmp_path / "cloud" / "envs" / "prod"
    layer = env / "gov_layer"
    layer.mkdir(parents=True)
    (env / "env.auto.tfvars").write_text('uc_tables = ["cat.sch.customers"]\n')
    (env / "auth.auto.tfvars").write_text('databricks_workspace_host = "https://example.invalid"\n')
    (layer / "auth.auto.tfvars").symlink_to("../auth.auto.tfvars")
    (layer / "env.auto.tfvars").symlink_to("../env.auto.tfvars")
    (layer / cg.GATE_FILENAME).write_text(json.dumps({"grant_keys": ["cat.sch.customers|analysts"]}))
    state_dir = tmp_path / "state"
    state = state_dir / "cloud" / "prod" / "layers" / "data_access" / "terraform.tfstate"
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({
        "version": 4,
        "outputs": {"coverage_gate": {"value": {"deployment_binding": cg.deployment_binding(layer)}}},
        "resources": [{"module": "module.data_access", "mode": "managed", "type": "databricks_grant",
                       "name": "table_access", "instances": [{"index_key": "cat.sch.customers|analysts"}]}],
    }))
    return layer, state_dir


def test_coverage_gate_reads_data_access_state_from_overridden_subdir(tmp_path, monkeypatch):
    from scripts import coverage_gate as cg

    layer, state_dir = _gov_layer_env(tmp_path)
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(state_dir))
    monkeypatch.setenv("DATA_ACCESS_SUBDIR", "gov_layer")
    assert cg.granted_tables(layer) == {"cat.sch.customers"}
    assert cg.withheld_access("data_access", layer) == []

    env = {k: v for k, v in os.environ.items() if not k.startswith(("DATABRICKS_", "ARM_", "AZURE_"))}
    result = subprocess.run(
        [sys.executable, str(SCRIPTS / "coverage_gate.py"), "withheld", "--layer", "data_access",
         "--layer-dir", str(layer), "--env-name", "prod"],
        text=True, capture_output=True, check=False,
        env=env | {"GENIERAILS_STATE_DIR": str(state_dir), "DATA_ACCESS_SUBDIR": "gov_layer"},
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_coverage_gate_reads_in_checkout_state_from_overridden_subdir(tmp_path, monkeypatch):
    import hashlib

    from scripts import coverage_gate as cg

    layer, state_dir = _gov_layer_env(tmp_path)
    # The in-checkout layout keeps the data_access state in the layer's directory.
    (state_dir / "cloud" / "prod" / "layers" / "data_access" / "terraform.tfstate").rename(
        layer / "terraform.tfstate")
    monkeypatch.delenv("GENIERAILS_STATE_DIR", raising=False)
    monkeypatch.setenv("DATA_ACCESS_SUBDIR", "gov_layer")
    assert cg.granted_tables(layer) == {"cat.sch.customers"}
    # The workspace layer's skip key folds in the data_access state it reads.
    no_state = "data_access " + hashlib.sha256(json.dumps([None, None], sort_keys=True).encode()).hexdigest()
    assert cg.skip_key("workspace", layer.parent) not in (no_state, "noskip")
