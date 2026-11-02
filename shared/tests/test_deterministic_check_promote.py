"""make check, make promote and make promote-to on deterministic output (real make).

The live AWS Phase A run found three gaps that only show with real
deterministic output: the deployer SP in every EXCEPT list, gr_treatment values
that aren't in the tag registry, and a prod that already has an agent:

- check validates in a scratch copy without credentials, so it refused the
  deployer SP that plain make validate accepts from auth.auto.tfvars (N2);
- the split refused deterministic tag values, so promote, promote-to and
  check ENV=prod crashed (N3);
- promote-to kept prod's existing agent without acl_groups, which a
  deterministic env refuses (N4).

Terraform is a stub on PATH; nothing reaches Databricks.
"""

import json
import re
import os
import signal
import shutil
import subprocess
import sys
from pathlib import Path

import hcl2
import pytest

import generate_abac
from deterministic_governance import validate_config
from governance_policies import DeterministicGovernanceError, build_model, column_types_from_ddl
from sensitivity_source import ClassificationSource
from scripts import derive_assignments as derive_script
from scripts import remap_env_config
from tests.test_check_target import FAKE_TERRAFORM, GITIGNORE, _env, _git, _snapshot

ROOT = Path(__file__).resolve().parents[2]
SHARED = ROOT / "shared"

if shutil.which("make") is None or shutil.which("git") is None:
    pytest.skip("make and git are required", allow_module_level=True)

# One deployer SP for both envs and the same group names, like the live run
# (the tests at the end give prod its own SP and its own group names).
DEPLOYER = "82bb9d6d-0000-0000-0000-000000000001"
TIERS = ["dev_ops", "dev_analysts"]
SPACE = "Payments"
DEV_SPACE_ID = "01ef00000000000a"
DDL = """CREATE TABLE paycat.s.t (
  id BIGINT,
  card STRING,
  email STRING
);"""
CLASSES = {"paycat.s.t.card": ["class.credit_card"], "paycat.s.t.email": ["class.email_address"]}
DEV_ENV = f'''governance_mode = "deterministic"
deployer_principal_id = "{DEPLOYER}"
access_tier_groups = {json.dumps(TIERS)}
table_readers = {{ "paycat.s.t" = {json.dumps(TIERS)} }}
genie_spaces = [
  {{ name = "{SPACE}", genie_space_id = "{DEV_SPACE_ID}", uc_tables = ["paycat.s.t"], acl_groups = {json.dumps(TIERS)} }},
]
'''
# What the existing (legacy) account layer already holds in a live env.
LEGACY_ACCOUNT = '''tag_policies = [
  { key = "gr_treatment", description = "legacy", values = ["account_last4", "email_partial"] },
]
'''
# Prod opted in by hand; its agent already exists and, from legacy, has no acl_groups.
# _prod_env adds prod's committed deployer ID.
PROD_ENV_BASE = '''governance_mode = "deterministic"
promote_from = "dev"
catalog_map = { "paycat" = "ppay" }
genie_spaces = [
  { name = "Payments", genie_space_id = "", uc_tables = ["ppay.s.t"] },
]
'''




def _prod_env(deployer=DEPLOYER, extra=""):
    line = f'deployer_principal_id = "{deployer}"\n' if deployer is not None else ""
    return PROD_ENV_BASE + line + extra


PROD_ENV = _prod_env()


def _auth(client_id, host="https://unreachable.invalid"):
    # A runtime auth file (gitignored); nothing before the plan step reads it.
    return (f'databricks_account_id = ""\ndatabricks_client_id = "{client_id}"\n'
            'databricks_client_secret = ""\ndatabricks_workspace_id = "1"\n'
            f'databricks_workspace_host = "{host}"\n')


def _make(repo, tmp_path, *args, **env):
    return subprocess.run(["make", "--no-print-directory", "-C", str(repo / "aws"), *args],
                          text=True, capture_output=True, env=_env(tmp_path, **env), timeout=600)


def _envs(repo):
    return repo / "aws" / "envs"


def _commit(repo, message):
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)


def _write_generated(dev):
    """dev's deterministic generate output, built the way generate builds it."""
    settings = hcl2.loads(DEV_ENV)
    model = build_model(CLASSES, column_types_from_ddl(DDL), settings, deployer_principal=DEPLOYER)
    generated = dev / "generated"
    generated.mkdir(parents=True)
    tfvars = generated / "abac.auto.tfvars"
    groups = "\n".join(f'  "{group}" = {{ description = "tier" }}' for group in TIERS)
    tfvars.write_text(f'groups = {{\n{groups}\n}}\n'
                      f'genie_space_configs = {{\n  "{SPACE}" = {{ title = "{SPACE}" }}\n}}\n'
                      f'genie_space_id_to_name = {{ "{DEV_SPACE_ID}" = "{SPACE}" }}\n')
    sql = generated / "masking_functions.sql"
    sql.write_text("USE CATALOG paycat;\nUSE SCHEMA s;\n")
    generate_abac.write_deterministic_model(tfvars, sql, model)
    (dev / "ddl").mkdir(exist_ok=True)
    return model


@pytest.fixture
def repo(tmp_path):
    repo = tmp_path / "repo"
    dev = _envs(repo) / "dev"
    dev.mkdir(parents=True)
    (_envs(repo) / "account").mkdir()
    shutil.copy2(ROOT / "aws" / "Makefile", repo / "aws" / "Makefile")
    (repo / "shared").symlink_to(SHARED)
    (dev / "env.auto.tfvars").write_text(DEV_ENV)
    (dev / "auth.auto.tfvars").write_text(_auth(DEPLOYER))
    (_envs(repo) / "account" / "abac.auto.tfvars").write_text(LEGACY_ACCOUNT)
    model = _write_generated(dev)
    assert any(DEPLOYER in (p.get("except_principals") or []) for p in model.fgac_policies)
    (repo / ".gitignore").write_text(GITIGNORE)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "terraform").write_text(FAKE_TERRAFORM)
    (bin_dir / "terraform").chmod(0o755)
    result = _make(repo, tmp_path, "promote", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    # The rest of what make generate writes (_write_generated stands in for it).
    subprocess.run([sys.executable, str(SHARED / "scripts" / "sticky_governance.py"), "record",
                    "--env-dir", str(dev), "--config", str(dev / "generated" / "abac.auto.tfvars")], check=True)
    _git(repo, "init", "-q")
    _commit(repo, "dev")
    return repo


def _promote_to_prod(repo, tmp_path, env_text=None, deployer=DEPLOYER):
    prod = _envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text(env_text if env_text is not None else _prod_env(deployer))
    # No host: check's plan step (6) reads prod's classification config live
    # (promote-to turns classification on), which fails fast without one.
    (prod / "auth.auto.tfvars").write_text(_auth(deployer, host=""))
    result = _make(repo, tmp_path, "promote-to", "ENV=prod")
    assert result.returncode == 0, result.stdout + result.stderr
    (tmp_path / "promote.log").write_text(result.stdout + result.stderr)
    record = prod / "generated" / "governed_tables.json"
    if (prod / "agents").is_dir():
        # Agent files: promote-to records prod's agent tables, which make check ENV=prod expects.
        assert json.loads(record.read_text())["agent_tables"] == {SPACE: ["ppay.s.t"]}
    else:
        # Inline agents: promote-to writes no governed-tables record (as before agent files),
        # and make check ENV=prod expects none.
        assert not record.exists()
    _commit(repo, "promote")
    return repo


@pytest.fixture
def promoted(repo, tmp_path):
    return _promote_to_prod(repo, tmp_path)


def _state(repo):
    return _snapshot(_envs(repo)), _git(repo, "status", "--porcelain", "--ignored", "--untracked-files=all")


def _set_dev_override(repo, tmp_path, column="paycat.s.notes.free_text", treatment="redact"):
    dev = _envs(repo) / "dev"
    env_file = dev / "env.auto.tfvars"
    env_file.write_text(env_file.read_text() +
                        f'column_overrides = {{ {json.dumps(column)} = {{ treatment = {json.dumps(treatment)} }} }}\n'
                        'treatment_versions = { phone_partial = { partial = "last4" } }\n'
                        'tag_treatments = { "class.loyalty_id" = "redact" }\n')
    settings = hcl2.loads(env_file.read_text())
    classes = CLASSES | {
        "paycat.s.notes.free_text": ["class.phone_number"],
        "paycat.s.notes.callback_number": ["class.phone_number"],
    }
    ddl = DDL + "\nCREATE TABLE paycat.s.notes (free_text STRING, callback_number STRING);\n"
    model = build_model(classes, column_types_from_ddl(ddl), settings,
                        deployer_principal=DEPLOYER)
    generate_abac.write_deterministic_model(
        dev / "generated" / "abac.auto.tfvars",
        dev / "generated" / "masking_functions.sql", model,
    )
    result = _make(repo, tmp_path, "promote", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    _commit(repo, "dev governance override")


def test_promote_and_prod_live_derivation_honor_remapped_column_override(repo, tmp_path):
    """Regression: migrate/release/maintain all use this live derivation path."""
    _set_dev_override(repo, tmp_path)
    _promote_to_prod(repo, tmp_path)

    prod = hcl2.loads((_envs(repo) / "prod" / "env.auto.tfvars").read_text())
    assert prod["column_overrides"] == {"ppay.s.notes.free_text": {"treatment": "redact"}}
    assert prod["treatment_versions"] == {"phone_partial": {"partial": "last4"}}
    assert prod["tag_treatments"] == {"class.loyalty_id": "redact"}
    assert validate_config(prod) == []

    native_prod = {
        "ppay.s.notes.free_text": ["class.phone_number"],
        "ppay.s.notes.callback_number": ["class.phone_number"],
    }
    types = {column: "STRING" for column in native_prod}
    model = build_model(native_prod, types, prod,
                        deployer_principal=DEPLOYER)
    resolved = model.columns["ppay.s.notes.free_text"]
    assert resolved.treatment == "redact"
    assert resolved.partial == "redacted"
    assert resolved.full == "redacted"
    callback = model.columns["ppay.s.notes.callback_number"]
    assert callback.treatment == "phone_partial" and callback.partial == "last4"


def test_promoted_override_is_honored_by_derive_assignments(repo, tmp_path, monkeypatch):
    _set_dev_override(repo, tmp_path)
    _promote_to_prod(repo, tmp_path)
    prod = _envs(repo) / "prod"
    (prod / "auth.auto.tfvars").write_text(_auth(DEPLOYER))
    native = ClassificationSource(tag_rows=[
        ("ppay", "s", "notes", "free_text", "class.phone_number", ""),
        ("ppay", "s", "notes", "callback_number", "class.phone_number", ""),
    ])
    monkeypatch.setattr(derive_script, "_fetch_live_classification_source", lambda *a, **k: native)
    monkeypatch.setattr(derive_script, "fetched_ddl_text", lambda *a, **k:
        "CREATE TABLE ppay.s.notes (free_text STRING, callback_number STRING);\n")

    count = derive_script.derive_assignments(
        prod / "generated" / "abac.auto.tfvars",
        prod / "auth.auto.tfvars", prod / "env.auto.tfvars",
        record_governed=False,
    )

    assert count == 2
    assignments = hcl2.load((prod / "generated" / "abac.auto.tfvars").open())["tag_assignments"]
    values = {item["entity_name"]: item["tag_value"] for item in assignments
              if item.get("tag_key") == "gr_treatment"}
    assert values["ppay.s.notes.free_text"].startswith("redact__string__")
    assert values["ppay.s.notes.callback_number"].startswith("phone_partial__string__")


def test_promote_refuses_column_override_with_unmappable_catalog(repo, tmp_path):
    dev_env = _envs(repo) / "dev" / "env.auto.tfvars"
    dev_env.write_text(dev_env.read_text() +
        'column_overrides = { "other.s.t.card" = { treatment = "redact" } }\n')
    _commit(repo, "unmappable override")
    prod = _envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text(PROD_ENV)
    before = (prod / "env.auto.tfvars").read_bytes()

    result = _make(repo, tmp_path, "promote-to", "ENV=prod")

    assert result.returncode != 0
    assert "column_overrides" in result.stdout + result.stderr
    assert "has no DEST_CATALOG_MAP mapping" in result.stdout + result.stderr
    assert (prod / "env.auto.tfvars").read_bytes() == before


def test_promote_refuses_override_absent_from_generated_classification(repo, tmp_path):
    dev_env = _envs(repo) / "dev" / "env.auto.tfvars"
    dev_env.write_text(dev_env.read_text() +
        'column_overrides = { "paycat.s.t.missing" = { treatment = "redact" } }\n')
    _commit(repo, "stale override")
    prod = _envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text(PROD_ENV)
    before = (prod / "env.auto.tfvars").read_bytes()

    result = _make(repo, tmp_path, "promote-to", "ENV=prod")

    assert result.returncode != 0
    assert "does not name a classified column in generated governance" in result.stdout + result.stderr
    assert (prod / "env.auto.tfvars").read_bytes() == before


def _write_prod(repo, text):
    prod = _envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text(text)
    return prod


def _direct_promote_args():
    return ("promote", "SOURCE_ENV=dev", "DEST_ENV=prod", "DEST_CATALOG_MAP=paycat=ppay")


@pytest.mark.parametrize("jobs", [(), ("-j4",), ("-i",), ("-k",)], ids=["serial", "make-j4", "make-i", "make-k"])
@pytest.mark.parametrize("selected_env", [False, True], ids=["default-env", "env-prod"])
def test_direct_promote_refuses_deterministic_destination_without_writing(repo, tmp_path, jobs, selected_env):
    _write_prod(repo, PROD_ENV)
    before = _snapshot(repo), _state(repo)
    args = [*jobs, *_direct_promote_args()]
    if selected_env:
        args.append("ENV=prod")

    result = _make(repo, tmp_path, *args)

    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    if jobs in (("-i",), ("-k",)):
        # make -i/-k would skip a failing guard, so the parse-time refusal (#280)
        # stops promote before any recipe runs.
        assert "make promote refused: -i/-k are not supported for this target because guards would be skipped" in out
    else:
        assert "make promote SOURCE_ENV=... DEST_ENV=prod is refused for deterministic destinations" in out
        assert "make promote-to ENV=prod" in out
    assert "Cross-env promote" not in out
    assert (_snapshot(repo), _state(repo)) == before


@pytest.mark.parametrize("how", ["env", "makeflags", "cmdline"])
def test_direct_promote_marker_cannot_be_forged_from_user_inputs(repo, tmp_path, how):
    sys.path.insert(0, str(SHARED / "scripts"))
    from promote_token import mint

    _write_prod(repo, PROD_ENV)
    before = _state(repo)
    wrong = tmp_path / "other"
    wrong.mkdir()
    token = mint(wrong)
    args = list(_direct_promote_args())
    env = {}
    if how == "env":
        env["GENIERAILS_PROMOTE_TO_TOKEN"] = str(token)
    elif how == "makeflags":
        env["MAKEFLAGS"] = f"GENIERAILS_PROMOTE_TO_TOKEN={token}"
    else:
        args.append(f"GENIERAILS_PROMOTE_TO_TOKEN={token}")

    result = _make(repo, tmp_path, *args, **env)

    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert "make promote-to ENV=prod" in out
    assert token.exists(), how
    assert _state(repo) == before


UNREADABLE_MODES = {"missing": None, "malformed": "governance_mode = \n", "unknown": 'governance_mode = "future"\n'}
MODE_FIX = 'Set governance_mode = "legacy" or "deterministic" in envs/prod/env.auto.tfvars.'


@pytest.mark.parametrize("contents", UNREADABLE_MODES.values(), ids=UNREADABLE_MODES.keys())
def test_direct_promote_fails_closed_when_destination_mode_is_not_readable(repo, tmp_path, contents):
    prod = _envs(repo) / "prod"
    prod.mkdir()
    if contents is not None:
        (prod / "env.auto.tfvars").write_text(contents)
    before = _state(repo)

    result = _make(repo, tmp_path, *_direct_promote_args())

    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert "refusing make promote" in out
    # promote-to refuses this env too, so the refusal names the fix instead.
    assert MODE_FIX in out
    assert "make promote-to" not in out
    assert "Cross-env promote" not in out
    assert _state(repo) == before


def _refusal_commands(out):
    """Every make command but promote itself named in an ERROR: block (the ERROR line and its indented follow-ups)."""
    commands, in_refusal = [], False
    for line in out.splitlines():
        if line.startswith("ERROR:"):
            in_refusal = True
        elif not line.startswith(" "):
            in_refusal = False
        if in_refusal:
            commands += re.findall(r"make ([a-z][a-z-]*(?: [A-Z_]+=[^\s.]+)*)", line)
    return [c for c in commands if c.split()[0] != "promote"]


@pytest.mark.parametrize("mode", [*UNREADABLE_MODES, "legacy", "deterministic"])
def test_direct_promote_refusal_never_names_a_command_refused_for_the_same_env(repo, tmp_path, mode):
    prod = _envs(repo) / "prod"
    prod.mkdir()
    if mode == "legacy":
        (prod / "env.auto.tfvars").write_text(PROD_ENV.replace('"deterministic"', '"legacy"'))
    elif mode == "deterministic":
        (prod / "env.auto.tfvars").write_text(PROD_ENV)
    elif UNREADABLE_MODES[mode] is not None:
        (prod / "env.auto.tfvars").write_text(UNREADABLE_MODES[mode])
    (prod / "auth.auto.tfvars").write_text(_auth(DEPLOYER, host=""))

    result = _make(repo, tmp_path, *_direct_promote_args())
    out = result.stdout + result.stderr
    commands = _refusal_commands(out)

    if mode == "legacy":
        assert "refusing make promote" not in out and "is refused" not in out, out
    elif mode == "deterministic":
        assert result.returncode != 0, out
        assert commands == ["promote-to ENV=prod"], out
    else:
        assert result.returncode != 0, out
        assert commands == [], out
        assert MODE_FIX in out
        # Why it can't name promote-to: promote-to refuses the same env.
        loop = _make(repo, tmp_path, "promote-to", "ENV=prod")
        assert loop.returncode != 0, loop.stdout + loop.stderr
    for command in commands:
        follow = _make(repo, tmp_path, *command.split())
        assert follow.returncode == 0, (command, follow.stdout + follow.stderr)


def test_direct_promote_to_legacy_destination_keeps_the_existing_path(repo, tmp_path):
    _write_prod(repo, PROD_ENV.replace('"deterministic"', '"legacy"'))

    result = _make(repo, tmp_path, *_direct_promote_args())

    out = result.stdout + result.stderr
    assert "=== Cross-env promote: dev -> prod ===" in out
    assert "make promote SOURCE_ENV=... DEST_ENV=prod is refused" not in out


def _path_without_ps(tmp_path):
    """Mirror PATH directories containing ps, omitting only ps itself."""
    result = []
    for index, raw in enumerate(os.environ["PATH"].split(os.pathsep)):
        source = Path(raw)
        if not (source / "ps").exists():
            result.append(raw)
            continue
        mirror = tmp_path / f"path-{index}"
        mirror.mkdir()
        for item in source.iterdir():
            if item.name != "ps":
                (mirror / item.name).symlink_to(item)
        result.append(str(mirror))
    return os.pathsep.join(result)


def test_promote_to_works_end_to_end_without_ps_on_path(repo, tmp_path):
    prod = _envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text(PROD_ENV)
    (prod / "auth.auto.tfvars").write_text(_auth(DEPLOYER, host=""))

    environ = _env(tmp_path)
    environ["PATH"] = f"{tmp_path / 'bin'}:{_path_without_ps(tmp_path)}"
    result = subprocess.run(
        ["make", "--no-print-directory", "-C", str(repo / "aws"), "promote-to", "ENV=prod"],
        text=True, capture_output=True, env=environ, timeout=600,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "=== Promote complete: dev -> prod ===" in result.stdout


def test_live_derivation_refuses_override_for_column_absent_from_prod():
    settings = hcl2.loads(DEV_ENV +
        'column_overrides = { "paycat.s.t.missing" = { treatment = "redact" } }\n')

    with pytest.raises(DeterministicGovernanceError,
                       match="absent from the governed DDL.*missing"):
        build_model(CLASSES, column_types_from_ddl(DDL), settings,
                    deployer_principal=DEPLOYER)


def test_live_derivation_refuses_override_without_native_class_tag():
    classes = {"ppay.s.notes.other": ["class.phone_number"]}
    types = {"ppay.s.notes.other": "STRING", "ppay.s.notes.free_text": "STRING"}
    settings = {"column_overrides": {
        "ppay.s.notes.free_text": {"treatment": "redact"},
    }}

    with pytest.raises(DeterministicGovernanceError,
                       match=r"no class\.\* tag.*ppay\.s\.notes\.free_text"):
        build_model(classes, types, settings)


def test_live_derivation_normalizes_override_identifier_case():
    column = "ppay.s.notes.free_text"
    model = build_model(
        {column: ["class.phone_number"]}, {column: "STRING"},
        {"column_overrides": {
            "PPAY.s.notes.FREE_TEXT": {"treatment": "redact"},
        }},
    )

    assert model.columns[column].treatment == "redact"
    assert model.columns[column].partial == "redacted"


def test_case_only_duplicate_override_error_names_both_spellings():
    column = "ppay.s.notes.free_text"
    with pytest.raises(DeterministicGovernanceError,
                       match=r"PPAY\.s\.notes\.FREE_TEXT and ppay\.s\.notes\.free_text"):
        build_model(
            {column: ["class.phone_number"]}, {column: "STRING"},
            {"column_overrides": {
                "ppay.s.notes.free_text": {"treatment": "redact"},
                "PPAY.s.notes.FREE_TEXT": {"partial": "last4"},
            }},
        )


def test_promote_case_only_duplicate_override_error_names_both_spellings():
    with pytest.raises(ValueError, match=r"'paycat\.s\.t\.card' and 'PAYCAT\.S\.T\.CARD'"):
        remap_env_config._promoted_governance_settings(
            {"column_overrides": {
                "paycat.s.t.card": {"treatment": "redact"},
                "PAYCAT.S.T.CARD": {"partial": "last4"},
            }},
            {"paycat": "ppay"}, {"paycat.s.t.card"},
        )


def test_promote_refuses_catalog_map_sources_that_collide_after_lowercasing():
    with pytest.raises(ValueError, match=r"catalogs 'paycat' and 'PayCat'.*same catalog"):
        remap_env_config._promoted_governance_settings(
            {"column_overrides": {"paycat.s.t.card": {"treatment": "redact"}}},
            {"paycat": "ppay", "PayCat": "other"}, {"paycat.s.t.card"},
        )


# ── N3: the split takes deterministic tag values next to legacy ones ────────

def test_same_env_promote_merges_deterministic_values_into_the_legacy_account_policy(repo):
    account = hcl2.loads((_envs(repo) / "account" / "abac.auto.tfvars").read_text())
    values = {p["key"]: p["values"] for p in account["tag_policies"]}["gr_treatment"]
    generated = hcl2.loads((_envs(repo) / "dev" / "generated" / "abac.auto.tfvars").read_text())
    deterministic = sorted({a["tag_value"] for a in generated["tag_assignments"]})
    assert len(deterministic) == 2 and all("__" in value for value in deterministic)
    assert values[:2] == ["account_last4", "email_partial"]
    assert sorted(values[2:]) == deterministic


# ── N2: check validates the deployer SP the way plain validate does ─────────

@pytest.mark.parametrize("ci", ["", "true"])
def test_check_accepts_the_deployer_sp_without_credentials_and_writes_nothing(repo, tmp_path, ci):
    assert _make(repo, tmp_path, "validate", "ENV=dev").returncode == 0
    before = _state(repo)

    result = _make(repo, tmp_path, "check", "ENV=dev", **({"CI": ci} if ci else {}))

    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert "not defined in 'groups'" not in out
    assert "OK: generated files (envs/dev): committed files match." in out
    assert "=== make check ENV=dev: passed ===" in out
    assert _state(repo) == before


def test_check_still_refuses_an_unknown_except_principal(repo, tmp_path):
    for rel in ("generated/abac.auto.tfvars", "data_access/abac.auto.tfvars"):
        path = _envs(repo) / "dev" / rel
        path.write_text(path.read_text().replace(DEPLOYER, "stranger-sp"))
    _commit(repo, "unknown principal")

    result = _make(repo, tmp_path, "check", "ENV=dev", CI="true")

    assert result.returncode != 0
    assert "except_principals group 'stranger-sp' not defined in 'groups'" in result.stdout + result.stderr


def test_check_refuses_a_deterministic_env_without_deployer_principal_id(repo, tmp_path):
    env_file = _envs(repo) / "dev" / "env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace(f'deployer_principal_id = "{DEPLOYER}"\n', ""))
    _commit(repo, "no deployer id")

    result = _make(repo, tmp_path, "check", "ENV=dev", CI="true")

    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert "deployer_principal_id is required in deterministic mode" in out, out


@pytest.mark.parametrize("value", ["account users", "deployer-sp", "users"])
def test_env_validation_refuses_a_deployer_that_is_not_an_application_id(value):
    from deterministic_governance import validate_config

    errors = validate_config({"governance_mode": "deterministic", "deployer_principal_id": value})

    assert any("deployer_principal_id must be a service principal application ID" in e for e in errors), errors


def test_check_copy_gets_no_auth_file(tmp_path):
    sys.path.insert(0, str(SHARED / "scripts"))
    from check_env import Scratch

    cloud = tmp_path / "aws"
    for env in ("dev", "prod", "account"):
        (cloud / "envs" / env).mkdir(parents=True)
        (cloud / "envs" / env / "auth.auto.tfvars").write_text(_auth(DEPLOYER).replace(
            'databricks_client_secret = ""', 'databricks_client_secret = "do-not-copy"'))
    shutil.copy2(ROOT / "aws" / "Makefile", cloud / "Makefile")
    # Even an auth file git tracks by mistake stays out of the copy.
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True)
    scratch = Scratch(cloud, cloud / "Makefile", "make")
    try:
        assert not [p for p in scratch.root.rglob("auth.auto.tfvars") if p.exists() and not p.is_symlink()]
        assert not any("do-not-copy" in p.read_text() or DEPLOYER in p.read_text()
                       for p in scratch.root.rglob("*") if p.is_file() and not p.is_symlink())
    finally:
        scratch.close()


# ── N3 + N4: promote-to into a prod that opted in by hand ───────────────────

def test_promote_to_seeds_the_missing_acl_groups(promoted, tmp_path):
    prod = hcl2.loads((_envs(promoted) / "prod" / "env.auto.tfvars").read_text())

    assert prod["governance_mode"] == "deterministic"
    assert prod["genie_spaces"][0]["acl_groups"] == TIERS
    assert prod["access_tier_groups"] == TIERS
    assert f"Seeded destination Genie space 'Payments' acl_groups={TIERS!r}" in (tmp_path / "promote.log").read_text()


def test_promote_to_keeps_acl_groups_prod_already_has(promoted, tmp_path):
    env_file = _envs(promoted) / "prod" / "env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace('acl_groups       = ["dev_ops", "dev_analysts"]',
                                                     'acl_groups       = ["dev_ops"]'))
    _commit(promoted, "prod narrows the agent")

    result = _make(promoted, tmp_path, "promote-to", "ENV=prod")

    assert result.returncode == 0, result.stdout + result.stderr
    prod = hcl2.loads(env_file.read_text())
    assert prod["genie_spaces"][0]["acl_groups"] == ["dev_ops"]
    assert "Preserved destination Genie space 'Payments' acl_groups=['dev_ops']" in result.stdout


def _assert_check_and_promote_check_pass(repo, tmp_path, ci):
    before = _state(repo)
    extra = {"CI": ci} if ci else {}

    check = _make(repo, tmp_path, "check", "ENV=prod", **extra)
    promote_check = _make(repo, tmp_path, "promote-to", "ENV=prod", "CHECK=1", **extra)

    out = check.stdout + check.stderr
    # Steps 1-5 pass offline; the plan (6) needs prod's live classification config.
    assert "OK: promote-to ENV=prod CHECK=1: committed files match." in out, out
    assert "OK: generated files (envs/prod): committed files match." in out, out
    assert "--- check 5/6: mask-library unit tests ---" in out and "--- check 6/6: plan ---" in out, out
    assert "not defined in 'groups'" not in out and "unknown canonical value" not in out
    assert check.returncode == 0 or "=== make check ENV=prod: FAILED (plan) ===" in out, out
    assert promote_check.returncode == 0, promote_check.stdout + promote_check.stderr
    assert "OK: promote-to ENV=prod CHECK=1: committed files match." in promote_check.stdout
    assert _state(repo) == before


@pytest.mark.parametrize("ci", ["", "true"])
def test_check_prod_and_promote_check_pass_on_the_promotion(promoted, tmp_path, ci):
    """An inline-agent promotion commits no governed-tables record, and check expects none."""
    _assert_check_and_promote_check_pass(promoted, tmp_path, ci)
    assert not (_envs(promoted) / "prod" / "generated" / "governed_tables.json").exists()


def _commit_prod_record(repo, record, message):
    (_envs(repo) / "prod" / "generated" / "governed_tables.json").write_text(json.dumps(record, indent=2) + "\n")
    _commit(repo, message)


def _stale_prod_record(repo):
    """A record from when Payments was in an agents/ file; its agents are inline now."""
    _commit_prod_record(repo, {"tables": ["ppay.s.t"], "agent_tables": {"Payments": ["ppay.s.t"]}},
                        "prod's record is stale")


def _tampered_prod_record(repo):
    """A hand edit: the governed set out of order, one table twice."""
    _commit_prod_record(repo, {"tables": ["ppay.s.u", "ppay.s.t", "PPAY.S.T"]}, "prod's record edited by hand")


def _added_prod_record(repo):
    """A record added by hand, not as a regeneration writes it."""
    (_envs(repo) / "prod" / "generated" / "governed_tables.json").write_text('{"tables": ["ppay.s.t"]}')
    _commit(repo, "prod's record added by hand")


STALE_RECORDS = {"added": _added_prod_record, "stale": _stale_prod_record, "tampered": _tampered_prod_record}


@pytest.mark.parametrize("ci", ["", "true"])
@pytest.mark.parametrize("case", sorted(STALE_RECORDS))
def test_check_prod_names_promote_to_when_the_governed_tables_record_is_stale(promoted, tmp_path, case, ci):
    """An inline-agent promotion writes no record, but a committed one is checked, and promote-to fixes it."""
    STALE_RECORDS[case](promoted)
    extra = {"CI": ci} if ci else {}

    check = _make(promoted, tmp_path, "check", "ENV=prod", **extra)

    out = check.stdout + check.stderr
    assert check.returncode != 0, out
    assert "governed_tables.json" in out, out
    assert "Run make promote-to ENV=prod" in out, out
    assert "make generate ENV=prod" not in out, out
    assert "OK: generated files (envs/prod): committed files match." not in out, out

    result = _make(promoted, tmp_path, "promote-to", "ENV=prod")
    assert result.returncode == 0, result.stdout + result.stderr
    _commit(promoted, "re-promote")
    _assert_check_and_promote_check_pass(promoted, tmp_path, ci)


@pytest.mark.parametrize("case", sorted(STALE_RECORDS))
def test_checks_same_env_step_names_promote_to_for_a_promoted_env(promoted, tmp_path, monkeypatch, capsys, case):
    """The same-env regeneration's hint, past promote-to CHECK=1 (which catches the same gap first)."""
    from scripts import check_env

    STALE_RECORDS[case](promoted)
    for key, value in _env(tmp_path).items():
        monkeypatch.setenv(key, value)
    for key in ("CI", "MAKEFLAGS", "MAKELEVEL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(check_env, "_promote_check", lambda args: None)

    code = check_env.main(["check", "--env", "prod", "--cloud-dir", str(promoted / "aws"),
                           "--makefile", str(promoted / "aws" / "Makefile")])

    out = capsys.readouterr()
    text = out.out + out.err
    assert code == 1, text
    assert "governed_tables.json" in text, text
    assert "Run make promote-to ENV=prod on the latest main and commit the result." in text, text
    assert "make generate ENV=prod" not in text, text


def _policies(env_dir):
    return {rel: hcl2.loads((env_dir / rel).read_text())["fgac_policies"]
            for rel in ("generated/abac.auto.tfvars", "data_access/abac.auto.tfvars")}


# ── Each env applies with its own deployer SP ───────────────────────────────

PROD_DEPLOYER = "5a7e0000-0000-0000-0000-00000000beef"


@pytest.mark.parametrize("ci", ["", "true"])
def test_promote_to_swaps_dev_deployer_for_prods_and_check_passes(repo, tmp_path, ci):
    dev_policies = {p["name"]: p.get("except_principals") or []
                    for p in hcl2.loads((_envs(repo) / "dev" / "generated" / "abac.auto.tfvars").read_text())["fgac_policies"]}
    assert any(DEPLOYER in names for names in dev_policies.values())

    _promote_to_prod(repo, tmp_path, deployer=PROD_DEPLOYER)

    for rel, policies in _policies(_envs(repo) / "prod").items():
        assert len(policies) == len(dev_policies), rel
        for policy, dev_except in zip(policies, dev_policies.values()):
            # Only the deployer changes, in place; a list without it gets none.
            expected = [PROD_DEPLOYER if name == DEPLOYER else name for name in dev_except]
            assert (policy.get("except_principals") or []) == expected, (rel, policy["name"])
            assert PROD_DEPLOYER not in (policy.get("to_principals") or []), (rel, policy["name"])
    for path in (_envs(repo) / "prod").rglob("*"):
        if path.is_file() and path.name != "auth.auto.tfvars":
            assert DEPLOYER not in path.read_text(), path
    _assert_check_and_promote_check_pass(repo, tmp_path, ci)


@pytest.mark.parametrize("ci", ["", "true"])
def test_authless_pr_checkout_reproduces_a_distinct_sp_promotion(repo, tmp_path, ci):
    """The PR checkout has no auth.auto.tfvars: check and promote-check use the committed IDs."""
    _promote_to_prod(repo, tmp_path, deployer=PROD_DEPLOYER)
    # Like a fresh clone: every auth file and the layers' links to them are gitignored.
    for path in list(_envs(repo).rglob("auth.auto.tfvars")):
        path.unlink()
    assert "auth.auto.tfvars" not in _git(repo, "ls-files")
    before = _state(repo)
    extra = {"CI": ci} if ci else {}

    check = _make(repo, tmp_path, "check", "ENV=prod", **extra)
    promote_check = _make(repo, tmp_path, "promote-to", "ENV=prod", "CHECK=1", **extra)

    out = check.stdout + check.stderr
    assert "OK: promote-to ENV=prod CHECK=1: committed files match." in out, out
    assert "OK: generated files (envs/prod): committed files match." in out, out
    assert "--- check 6/6: plan ---" in out, out
    assert "not defined in 'groups'" not in out, out
    assert check.returncode == 0 or "=== make check ENV=prod: FAILED (plan) ===" in out, out
    assert promote_check.returncode == 0, promote_check.stdout + promote_check.stderr
    assert "OK: promote-to ENV=prod CHECK=1: committed files match." in promote_check.stdout
    assert _state(repo) == before


def _break_account(repo):
    account = _envs(repo) / "account" / "abac.auto.tfvars"
    account.write_text(account.read_text() + "tag_policies = [\n")


# Every way a promotion refuses: each must leave every file under envs/ as it was.
REFUSALS = {
    "missing-deployer": (dict(deployer=None), None, "deployer_principal_id is required in deterministic mode"),
    "everyone-deployer": (dict(deployer="account users"), None,
                          "deployer_principal_id must be a service principal application ID"),
    "group-collision": (dict(extra='group_map = { dev_ops = "dev_analysts" }\n'), None,
                        "group_map would give groups 'dev_ops', 'dev_analysts' the one name 'dev_analysts'"),
    "many-to-one-group-map": (dict(extra='group_map = { dev_ops = "x", dev_analysts = "x" }\n'), None,
                              "many-to-one mappings"),
    "everyone-exempt": (dict(extra='raw_exempt_principals = ["account users"]\n'), None, "everyone"),
    "split-fails": (dict(), _break_account, "Unexpected"),
}


@pytest.mark.parametrize("case", sorted(REFUSALS))
def test_every_promote_refusal_changes_nothing(repo, tmp_path, case):
    env_kwargs, setup, message = REFUSALS[case]
    prod = _envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text(_prod_env(**env_kwargs))
    (prod / "auth.auto.tfvars").write_text(_auth(PROD_DEPLOYER, host=""))
    if setup:
        setup(repo)
    _commit(repo, f"prod set up ({case})")
    before = _state(repo)

    result = _make(repo, tmp_path, "promote-to", "ENV=prod")

    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert message in out, out
    assert "Refused: nothing was changed" in out, out
    assert _state(repo) == before, out


def test_promotion_restore_puts_back_bytes_links_and_removes_additions(tmp_path):
    sys.path.insert(0, str(SHARED / "scripts"))
    from promotion_txn import restore, snapshot

    env = tmp_path / "envs" / "prod"
    (env / "data_access" / ".terraform").mkdir(parents=True)
    (env / "data_access" / ".terraform" / "provider").write_text("big")
    (env / "env.auto.tfvars").write_text("a = 1\n")
    (env / "data_access" / "auth.auto.tfvars").symlink_to("../auth.auto.tfvars")
    missing = tmp_path / "envs" / "new"
    snapshot([env, missing], tmp_path / "txn")
    before = _snapshot(tmp_path / "envs")

    (env / "env.auto.tfvars").write_text("a = 2\n")
    (env / "data_access" / "auth.auto.tfvars").unlink()
    (env / "data_access" / "auth.auto.tfvars").write_text("not a link")
    (env / "generated").mkdir()
    (env / "generated" / "abac.auto.tfvars").write_text("x")
    (env / "data_access" / ".terraform" / "provider").write_text("big")
    missing.mkdir()
    (missing / "env.auto.tfvars").write_text("y")
    restore(tmp_path / "txn")

    assert _snapshot(tmp_path / "envs") == before


# ── Applies use the SP the committed policies exempt ────────────────────────

@pytest.mark.parametrize("target", ["rehearse ENV=dev", "release-plan ENV=prod", "release ENV=prod MANIFEST=m.json",
                                    "maintain ENV=prod"])
def test_apply_refuses_before_the_lock_when_auth_is_not_the_committed_deployer(repo, tmp_path, target):
    _promote_to_prod(repo, tmp_path, deployer=PROD_DEPLOYER)
    env = target.split("ENV=")[1].split()[0]
    (_envs(repo) / env / "auth.auto.tfvars").write_text(_auth("0badc0de-0000-0000-0000-000000000000", host=""))
    before = _state(repo)

    result = _make(repo, tmp_path, *target.split())

    out = result.stdout + result.stderr
    name = target.split()[0]
    assert result.returncode != 0, out
    assert f"ERROR: make {name} refused before taking the lock:" in out, out
    assert "databricks_client_id is 0badc0de-0000-0000-0000-000000000000" in out, out
    assert not list(_envs(repo).rglob(".governance.lock")), out
    assert _state(repo) == before


# Every Make variable a test harness used to stub a check with, plus the
# derive-skip flag. None may skip the identity gate, from the command line or
# the environment (make -e lets the environment beat ordinary assignments).
FORMER_STUBS = {name: "true" for name in (
    "_DEPLOYER_CHECK", "_ACCESS_CHECK", "_GROUP_CHECK", "_GROUP_MAP_RECORD", "_TABLE_READERS", "_SAVE_VERIFY_KEY",
    "_STAGED_POLICY", "_STAGED_CONFIRM_RUN", "_EXTERNAL_MASK_INVENTORY", "_CLASSIFICATION_CHECK", "_LIVE_SNAPSHOT",
    "_RELEASE_REPORTS", "_RELEASE_GATE", "_COVERAGE_GATE", "_STICKY", "_AGENT_OWNERSHIP", "_AGENT_COVERAGE",
    "_ENV_LOCK_HELPER", "_RELEASE_HELPER", "_REQUIRE_MASK_PROOF", "_LEGACY_GUARD", "_APPLY_GUARD",
    "_PROMOTION_TXN_BEGIN", "_PROMOTION_TXN_COMMIT", "_IS_DETERMINISTIC", "_GOVERNANCE_MODE_OF")} | {
    "_EXPOSURE_DERIVED": "1", "AUDIT_SCHEMA_SCRIPT": "/dev/null", "DERIVE_ASSIGNMENTS_SCRIPT": "/dev/null",
    "GENIE_ADOPT_PREFLIGHT_SCRIPT": "/dev/null"}
ENTRY_POINTS = [("rehearse", "dev", ()), ("release-plan", "prod", ()), ("release", "prod", ("MANIFEST=m.json",)),
                ("maintain", "prod", ()), ("ungovern", "prod", ("TABLE=ppay.s.t", "YES=1")),
                ("detach-agent", "prod", ("SPACE=Payments",))]


@pytest.mark.parametrize("target, env, extra", ENTRY_POINTS, ids=[t for t, _e, _x in ENTRY_POINTS])
def test_no_override_skips_the_deployer_gate_at_any_entry_point(repo, tmp_path, target, env, extra):
    _promote_to_prod(repo, tmp_path, deployer=PROD_DEPLOYER)
    auth = _envs(repo) / env / "auth.auto.tfvars"
    cmdline = [f"{name}={value}" for name, value in FORMER_STUBS.items()]
    for auth_text, how in ((_auth("0badc0de-0000-0000-0000-000000000000", host=""), "cmdline"),
                           (_auth("0badc0de-0000-0000-0000-000000000000", host=""), "environment"),
                           (_auth("0badc0de-0000-0000-0000-000000000000", host=""), "make -e"),
                           (_auth("", host=""), "cmdline")):
        auth.write_text(auth_text)
        before = _state(repo)
        args = [target, f"ENV={env}", *extra]
        if how == "cmdline":
            result = _make(repo, tmp_path, *args, *cmdline)
        else:
            result = _make(repo, tmp_path, *(["-e"] if how == "make -e" else []), *args, **FORMER_STUBS)
        out = result.stdout + result.stderr
        case = f"{target} ENV={env}, overrides via {how}, auth {auth_text.splitlines()[1]}"
        assert result.returncode != 0, case + "\n" + out
        assert f"ERROR: make {target} refused before taking the lock:" in out, case + "\n" + out
        committed = PROD_DEPLOYER if env == "prod" else DEPLOYER
        assert "databricks_client_id is" in out and committed in out, case
        assert not list(_envs(repo).rglob(".governance.lock")), case
        assert _state(repo) == before, case


def test_every_check_guard_and_transaction_helper_is_non_overridable():
    makefile = (SHARED / "Makefile.shared").read_text()
    loose = [line.split("=")[0].strip() for line in makefile.splitlines()
             if re.match(r"^_[A-Z][A-Z0-9_]* *[:?]?= ", line)
             and any(word in line for word in ("python3", "$(MAKE)", "trap", "exit", "$(_"))]
    assert loose == [], f"overridable helpers: {loose}"
    for name in ("AUDIT_SCHEMA_SCRIPT", "DERIVE_ASSIGNMENTS_SCRIPT", "GENIE_ADOPT_PREFLIGHT_SCRIPT"):
        assert re.search(rf"(?m)^override {name} = ", makefile), name
    assert "_EXPOSURE_DERIVED" not in makefile


def test_the_exposure_token_works_once_and_only_for_its_env(tmp_path, monkeypatch):
    sys.path.insert(0, str(SHARED / "scripts"))
    from exposure_token import consume, mint

    env, other = tmp_path / "prod", tmp_path / "dev"
    env.mkdir()
    other.mkdir()
    token = mint(env)
    assert oct(token.stat().st_mode & 0o777) == "0o600"
    assert not consume(other, str(token)) and token.exists()  # another env's token skips nothing
    assert consume(env, str(token)) and not token.exists()
    assert not consume(env, str(token))  # once
    forged = tmp_path / "forged"
    forged.write_text(f"{env.resolve()}\n")
    forged.chmod(0o644)
    assert not consume(env, str(forged))  # not private
    link = tmp_path / "link"
    good = mint(env)
    link.symlink_to(good)
    assert not consume(env, str(link)) and good.exists()  # never through a link
    assert not consume(env, None) and not consume(env, "")
    good.unlink()


def test_the_promote_to_token_works_once_and_only_for_its_env(tmp_path):
    sys.path.insert(0, str(SHARED / "scripts"))
    from promote_token import TOKEN_PREFIX, check, consume, mint

    env, other = tmp_path / "prod", tmp_path / "dev"
    env.mkdir()
    other.mkdir()
    wrong_env = mint(other)
    assert oct(wrong_env.stat().st_mode & 0o777) == "0o600"
    assert not consume(env, str(wrong_env)) and wrong_env.exists()
    wrong_env.unlink()

    non_private = mint(env)
    non_private.chmod(0o644)
    assert not consume(env, str(non_private)) and non_private.exists()
    non_private.unlink()

    link = tmp_path / "link"
    good = mint(env)
    link.symlink_to(good)
    assert not consume(env, str(link)) and good.exists()
    assert check(env, str(good)) and good.exists()
    assert consume(env, str(good)) and not good.exists()
    assert not consume(env, str(good))  # one use only

    exposure_shape = tmp_path / "exposure-token"
    exposure_shape.write_text(f"{env.resolve()}\n")
    exposure_shape.chmod(0o600)
    assert not consume(env, str(exposure_shape)) and exposure_shape.exists()
    assert TOKEN_PREFIX not in exposure_shape.read_text()
    assert not consume(env, None) and not consume(env, "")


def test_promote_to_token_is_removed_on_sigterm(tmp_path):
    env = tmp_path / "prod"
    env.mkdir()
    token_tmp = tmp_path / "tokens"
    token_tmp.mkdir()
    command = [
        sys.executable, str(SHARED / "scripts" / "promote_token.py"),
        "run", "--env-dir", str(env), "--", sys.executable, "-c",
        "import time; time.sleep(60)",
    ]
    process = subprocess.Popen(command, env={**os.environ, "TMPDIR": str(token_tmp)})
    for _ in range(200):
        if list(token_tmp.glob(".genierails-promote-to-*")):
            break
        import time
        time.sleep(0.01)
    assert list(token_tmp.glob(".genierails-promote-to-*")), "token was not minted"

    process.terminate()
    process.wait(timeout=10)

    assert process.returncode != 0
    assert not list(token_tmp.glob(".genierails-promote-to-*"))


def test_promote_to_token_signal_at_mint_cannot_leave_token(tmp_path):
    env = tmp_path / "prod"
    env.mkdir()
    marker = tmp_path / "minted-path"
    program = f'''\
import os, signal, sys
sys.path.insert(0, {str(SHARED / "scripts")!r})
import promote_token
real_mint = promote_token.mint
def signal_during_mint(env_dir):
    os.kill(os.getpid(), signal.SIGINT)
    token = real_mint(env_dir)
    open({str(marker)!r}, "w").write(str(token))
    return token
promote_token.mint = signal_during_mint
promote_token.run(promote_token.Path({str(env)!r}), [sys.executable, "-c", "pass"])
'''

    result = subprocess.run([sys.executable, "-c", program])

    assert result.returncode == 128 + signal.SIGINT
    assert marker.exists()
    assert not Path(marker.read_text()).exists()


def test_promote_to_token_signal_during_final_unlink_cannot_leave_token(tmp_path):
    env = tmp_path / "prod"
    env.mkdir()
    token = tmp_path / "promote-token"
    program = f'''\
import os, signal, sys
from pathlib import Path
sys.path.insert(0, {str(SHARED / "scripts")!r})
import promote_token
class SignallingPath(type(Path())):
    def unlink(self, *args, **kwargs):
        os.kill(os.getpid(), signal.SIGTERM)
        return super().unlink(*args, **kwargs)
def mint(env_dir):
    path = SignallingPath({str(token)!r})
    path.write_text(promote_token.TOKEN_PREFIX + str(env_dir.resolve()) + "\\n")
    path.chmod(0o600)
    return path
promote_token.mint = mint
promote_token.run(Path({str(env)!r}), [sys.executable, "-c", "pass"])
'''

    subprocess.run([sys.executable, "-c", program])

    assert not token.exists()


@pytest.mark.parametrize("first,second", [
    (signal.SIGTERM, signal.SIGINT),
    (signal.SIGINT, signal.SIGTERM),
    (signal.SIGHUP, signal.SIGTERM),
])
def test_promote_to_token_two_signals_leave_no_token_and_keep_first_exit(tmp_path, first, second):
    env = tmp_path / "prod"
    env.mkdir()
    token = tmp_path / "promote-token"
    child = (
        "import os,signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        f"os.kill(os.getppid(), {first}); time.sleep(.05); "
        f"os.kill(os.getppid(), {second}); time.sleep(2)"
    )
    program = f'''\
import sys, time
from pathlib import Path
sys.path.insert(0, {str(SHARED / "scripts")!r})
import promote_token
class SlowPath(type(Path())):
    def unlink(self, *args, **kwargs):
        time.sleep(.2)
        return super().unlink(*args, **kwargs)
def mint(env_dir):
    path = SlowPath({str(token)!r})
    path.write_text(promote_token.TOKEN_PREFIX + str(env_dir.resolve()) + "\\n")
    path.chmod(0o600)
    return path
promote_token.mint = mint
promote_token.run(Path({str(env)!r}), [sys.executable, "-c", {child!r}])
'''

    result = subprocess.run([sys.executable, "-c", program], timeout=10)

    assert result.returncode == 128 + first
    assert not token.exists()


def test_promote_to_token_signal_at_cleanup_entry_leaves_no_token(tmp_path):
    env = tmp_path / "prod"
    env.mkdir()
    token = tmp_path / "promote-token"
    program = f'''\
import os, signal, sys
from pathlib import Path
sys.path.insert(0, {str(SHARED / "scripts")!r})
import promote_token
real_mask = signal.pthread_sigmask
calls = 0
def mask_with_signal(how, mask):
    global calls
    calls += 1
    if calls == 3:
        os.kill(os.getpid(), signal.SIGTERM)
    return real_mask(how, mask)
def mint(env_dir):
    path = Path({str(token)!r})
    path.write_text(promote_token.TOKEN_PREFIX + str(env_dir.resolve()) + "\\n")
    path.chmod(0o600)
    return path
signal.pthread_sigmask = mask_with_signal
promote_token.mint = mint
promote_token.run(Path({str(env)!r}), [sys.executable, "-c", "pass"])
'''

    result = subprocess.run([sys.executable, "-c", program])

    assert result.returncode == -signal.SIGTERM
    assert not token.exists()


def test_apply_check_passes_when_auth_matches_and_ignores_legacy(tmp_path):
    from deployer_identity import apply_problems

    env = tmp_path / "prod"
    env.mkdir()
    (env / "env.auto.tfvars").write_text(_prod_env(PROD_DEPLOYER))
    (env / "auth.auto.tfvars").write_text(_auth(PROD_DEPLOYER.upper()))
    assert apply_problems(env) == []
    (env / "auth.auto.tfvars").unlink()
    assert "databricks_client_id is not set" in apply_problems(env)[0]
    (env / "env.auto.tfvars").write_text('governance_mode = "legacy"\n')
    assert apply_problems(env) == []


def test_promote_to_never_adds_the_deployer_to_a_never_raw_policy(tmp_path):
    sys.path.insert(0, str(SHARED / "scripts"))
    from remap_generated_config import map_principals

    text = ('fgac_policies = [\n  {\n    name = "raw"\n    except_principals = ["t1", "dev-sp"]\n  },\n'
            '  {\n    name = "never"\n    except_principals = []\n    to_principals = ["dev-sp"]\n  },\n]\n')

    out = hcl2.loads(map_principals(text, {}, set(), [], ("dev-sp", "prod-sp")))["fgac_policies"]

    assert out[0]["except_principals"] == ["t1", "prod-sp"]
    assert out[1]["except_principals"] == [] and out[1]["to_principals"] == ["dev-sp"]


# ── group_map renames the groups the policies name ──────────────────────────

PROD_TIERS = ["prod_ops", "dev_analysts"]
PROD_ENV_GROUP_MAP = _prod_env(extra='group_map = { dev_ops = "prod_ops" }\n')


@pytest.mark.parametrize("ci", ["", "true"])
def test_promote_to_maps_groups_with_policies_tiers_and_acls_and_check_passes(repo, tmp_path, ci):
    _promote_to_prod(repo, tmp_path, env_text=PROD_ENV_GROUP_MAP)

    prod = _envs(repo) / "prod"
    env = hcl2.loads((prod / "env.auto.tfvars").read_text())
    assert env["access_tier_groups"] == PROD_TIERS
    assert env["genie_spaces"][0]["acl_groups"] == PROD_TIERS
    assert env["table_readers"] == {"ppay.s.t": PROD_TIERS}
    for rel in ("generated/abac.auto.tfvars", "data_access/abac.auto.tfvars", "abac.auto.tfvars"):
        assert list(hcl2.loads((prod / rel).read_text())["groups"]) == PROD_TIERS, rel
    account = hcl2.loads((_envs(repo) / "account" / "abac.auto.tfvars").read_text())
    assert "prod_ops" in account["groups"]
    for rel, policies in _policies(prod).items():
        named = {n for p in policies for f in ("to_principals", "except_principals") for n in p.get(f) or []}
        assert "prod_ops" in named and named <= set(PROD_TIERS) | {DEPLOYER, "account users"}, rel
    for path in prod.rglob("*"):
        if path.is_file():
            text = path.read_text()
            if path.name == "env.auto.tfvars":
                text = text.replace('"dev_ops" = "prod_ops"', "")
            assert '"dev_ops"' not in text, path
    _assert_check_and_promote_check_pass(repo, tmp_path, ci)


# The champion layout: dev keeps its agent in agents/ files, prod maps every
# dev group. Before the fix promote-to seeded prod's agent file with dev's
# acl_groups and exited 2: group 'dev_ops' not defined in 'groups'.
PROD_GROUPS = ["prod_ops", "prod_analysts"]
PROD_ENV_AGENT_FILES = ('governance_mode = "deterministic"\npromote_from = "dev"\ncatalog_map = { "paycat" = "ppay" }\n'
                        f'deployer_principal_id = "{DEPLOYER}"\n'
                        'group_map = { dev_ops = "prod_ops", dev_analysts = "prod_analysts" }\n')


def _move_dev_agent_to_files(repo, tmp_path):
    from scripts.agent_ownership import write_payload

    dev = _envs(repo) / "dev"
    env = dev / "env.auto.tfvars"
    env.write_text(env.read_text().replace(re.search(r"genie_spaces = \[.*?\n\]\n", DEV_ENV, re.S).group(0), ""))
    (dev / "agents").mkdir()
    (dev / "agents" / f"{SPACE}.auto.tfvars").write_text(
        f'genie_space_id = "{DEV_SPACE_ID}"\nuc_tables = ["paycat.s.t"]\nacl_groups = {json.dumps(TIERS)}\n')
    write_payload({"version": 2, "data_sources": {"tables": [{"identifier": "paycat.s.t"}]}},
                  dev / "agents" / f"{SPACE}.space.json")
    result = _make(repo, tmp_path, "promote", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    _commit(repo, "dev agent in agents/ files")


@pytest.mark.parametrize("ci", ["", "true"])
def test_promote_to_maps_agent_file_acl_groups_through_group_map_and_check_passes(repo, tmp_path, ci):
    _move_dev_agent_to_files(repo, tmp_path)
    _promote_to_prod(repo, tmp_path, env_text=PROD_ENV_AGENT_FILES)

    prod = _envs(repo) / "prod"
    agent = hcl2.loads((prod / "agents" / f"{SPACE}.auto.tfvars").read_text())
    assert agent["acl_groups"] == PROD_GROUPS
    env = hcl2.loads((prod / "env.auto.tfvars").read_text())
    assert env["access_tier_groups"] == PROD_GROUPS
    assert env["table_readers"] == {"ppay.s.t": PROD_GROUPS}
    assert list(hcl2.loads((prod / "generated" / "abac.auto.tfvars").read_text())["groups"]) == PROD_GROUPS
    _assert_check_and_promote_check_pass(repo, tmp_path, ci)


def test_promote_to_refuses_prods_own_tiers_without_group_map_and_restores_prod(repo, tmp_path):
    """README Prod step 1 used to say to replace prod's access_tier_groups placeholders with prod's groups."""
    prod = _envs(repo) / "prod"
    prod.mkdir()
    text = _prod_env(extra=f"access_tier_groups = {json.dumps(PROD_GROUPS)}\n")
    (prod / "env.auto.tfvars").write_text(text)
    (prod / "auth.auto.tfvars").write_text(_auth(DEPLOYER, host=""))
    before = _snapshot(_envs(repo))

    result = _make(repo, tmp_path, "promote-to", "ENV=prod")

    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert "access_tier_groups promoted" not in out
    assert "name prod's own group(s) prod_ops, prod_analysts" in out, out
    assert 'group_map = { "dev_analysts" = "prod_analysts", "dev_ops" = "prod_ops" }' in out, out
    assert _snapshot(_envs(repo)) == before, "a refused promotion restores everything it touched"


@pytest.mark.parametrize("ci", ["", "true"])
def test_promote_to_accepts_prods_tier_placeholders_and_writes_tiers_through_group_map(repo, tmp_path, ci):
    """README Prod step 1: set group_map, leave the setup template's access_tier_groups placeholders."""
    text = PROD_ENV_GROUP_MAP + 'access_tier_groups = ["<most-privileged-group>", "<least-privileged-group>"]\n'
    _promote_to_prod(repo, tmp_path, env_text=text)

    env = hcl2.loads((_envs(repo) / "prod" / "env.auto.tfvars").read_text())
    assert env["access_tier_groups"] == PROD_TIERS
    _assert_check_and_promote_check_pass(repo, tmp_path, ci)


def test_group_keys_rename_bare_and_quoted_keys_and_group_members():
    sys.path.insert(0, str(SHARED / "scripts"))
    from remap_generated_config import map_group_keys

    text = ('groups = {\n  dev_ops = {\n    description = "a = { b }"\n  }\n  "dev x" = { description = "" }\n'
            '  keep = {}\n}\ngroup_members = {\n  dev_ops = ["u1"]\n}\n')

    out = hcl2.loads(map_group_keys(text, {"dev_ops": "prod_ops", "dev x": "prod x"}, set()))

    assert list(out["groups"]) == ["prod_ops", "prod x", "keep"]
    assert out["groups"]["prod_ops"]["description"] == "a = { b }"
    assert out["group_members"] == {"prod_ops": ["u1"]}


def test_check_runs_promote_check_exactly_when_env_role_says_promoted(repo, tmp_path, monkeypatch):
    """check decides promotion by release_gate.env_role, the same rule rehearse and release use."""
    from scripts import check_env
    from scripts.release_gate import PROMOTED

    for key, value in _env(tmp_path).items():
        monkeypatch.setenv(key, value)
    for key in ("CI", "MAKEFLAGS", "MAKELEVEL"):
        monkeypatch.delenv(key, raising=False)
    roles = []
    monkeypatch.setattr(check_env, "env_role", lambda env_dir: roles.append(Path(env_dir).name) or PROMOTED)

    def stop(args):
        raise check_env.CheckFailed("promote-check reached")

    monkeypatch.setattr(check_env, "_promote_check", stop)

    code = check_env.main(["check", "--env", "dev", "--cloud-dir", str(repo / "aws"),
                           "--makefile", str(repo / "aws" / "Makefile")])

    assert roles == ["dev"]
    assert code == 1


def test_promote_to_under_a_held_account_lock_refuses_and_writes_nothing(repo, tmp_path):
    """PR #297 r3: every writer of envs/account/abac.auto.tfvars respects the account lock. promote-to's
    split would add dev's part back to the account file; another run holds the account lock."""
    from scripts import environment_lock

    account = _envs(repo) / "account"
    (account / "abac.auto.tfvars").write_text(LEGACY_ACCOUNT)  # dev's part not merged in yet
    prod = _envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text(PROD_ENV)
    (prod / "auth.auto.tfvars").write_text(_auth(DEPLOYER, host=""))
    holder = subprocess.Popen(["sleep", "300"])
    try:
        ok, message, _ = environment_lock.acquire(account, holder.pid, "apply-account")
        assert ok, message

        def files():
            return {k: v for k, v in _snapshot(_envs(repo)).items() if not k.endswith(".governance.guard")}

        before = files()
        result = _make(repo, tmp_path, "promote-to", "ENV=prod")
        out = result.stdout + result.stderr
        assert result.returncode != 0, out
        assert f"the shared account layer is locked by another run (make apply-account, pid {holder.pid}" in out
        assert "nothing was written" in out
        assert files() == before  # the account file, prod and dev, byte for byte (the lock file too)
        assert environment_lock.owns_lock(account, holder.pid)
    finally:
        holder.kill()
        holder.wait()
