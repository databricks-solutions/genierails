"""Regression tests for `make setup` next steps (dev_to_prod champion flow)."""

import os
import re
import stat
import subprocess
import sys
from pathlib import Path

import hcl2
import pytest


ROOT = Path(__file__).parents[2]
SHARED_ROOT = ROOT / "shared"
CLOUDS = ("aws", "azure")


def _clean_env():
    return {
        key: value
        for key, value in os.environ.items()
        if key not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "ACCOUNT_ADMIN_ENV")
    }


def _make(cloud, tmp_path, *args):
    cloud_root = tmp_path / cloud
    cloud_root.mkdir(exist_ok=True)
    return subprocess.run(
        [
            "make",
            "--no-print-directory",
            *args,
            f"CLOUD_ROOT={cloud_root}",
            f"SHARED_ROOT={SHARED_ROOT}",
        ],
        cwd=ROOT / cloud,
        text=True,
        capture_output=True,
        env=_clean_env(),
    )


def _defined_targets():
    makefile = (SHARED_ROOT / "Makefile.shared").read_text()
    return {
        line.split(":", 1)[0]
        for line in makefile.splitlines()
        if line and not line[0].isspace() and ":" in line and "=" not in line.split(":", 1)[0]
    }


def _mentioned_targets(output):
    # Commands appear as "Run: make <t>", "or: make <t>", or indented on their own line.
    return re.findall(r"(?:^\s+|: )make ([a-z][a-z-]*)", output, flags=re.MULTILINE)


def _mode(path):
    return stat.S_IMODE(path.stat().st_mode)


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_dev_prints_champion_phase_1_steps(cloud, tmp_path):
    result = _make(cloud, tmp_path, "setup", "ENV=dev")
    assert result.returncode == 0, result.stderr
    out = result.stdout

    assert "cp ../shared/examples/dev_to_prod/env.auto.tfvars.example envs/dev/env.auto.tfvars" in out
    assert "envs/dev/auth.auto.tfvars" in out
    assert "manage_groups" not in out
    assert "Edit envs/account/env.auto.tfvars" not in out
    # Each agent gets its own file with its ID and who may run it (decision 4).
    assert (
        "  3. Per agent, create its file (the file name is the agent's name) and set genie_space_id to its\n"
        "     Genie agent ID and acl_groups to the groups that may run it ([] means nobody; required):\n"
        "       mkdir -p envs/dev/agents\n"
        '       cp ../shared/examples/dev_to_prod/agent.auto.tfvars.example "envs/dev/agents/<agent name>.auto.tfvars"\n'
        "     In envs/dev/env.auto.tfvars set deployer_principal_id to the applying SP's client_id, and\n"
        "     access_tier_groups to your IdP groups, most to least privileged (the template starts new champions in deterministic mode)\n"
    ) in out
    # One plain generate imports the agent; no separate MODE=genie step.
    assert "MODE=genie" not in out
    # access_tier_groups makes --groups unnecessary on every champion command.
    assert "GENERATE_ARGS" not in out
    assert "--groups" not in out
    assert "     (No agent yet? See ../shared/examples/dev_to_prod/SAMPLE_ENV.md)\n" in out
    # verify-access picks the key itself; setup never asks for one. A deterministic
    # rehearse only prints the proven keys: committing them is optional.
    assert (
        "     verify-access picks a row-pairing key per masked table and prints the keys it proved;\n"
        "     nothing is saved: committing that line is optional and pins those keys (promote-to copies committed keys).\n"
    ) in out
    assert "the proven keys are saved" not in out
    assert "VERIFY_KEY_COLUMN" not in out and "verify_key_column" not in out
    # The champion flow needs only the agent ID; tables are discovered.
    assert "uc_tables" not in out
    assert "existing agent:" not in out
    assert "pick one" not in out
    # Setup offers its own files; make check, rehearse receipts and release-plan read only committed files.
    assert "setup offers to commit and push" not in out  # captured output is not a terminal
    assert "git add .gitignore envs/" not in out
    assert "  4. In Catalog Explorer, open the catalog > Details tab > Data classification: turn it on.\n" in out
    assert "     When the scan finishes, review the detections and exclude any false positives, then turn on\n" in out
    assert "     auto-tagging. Wait until the class.* tags appear.\n" in out
    assert "     Prefer a script? make enable-classification ENV=dev turns it on (you still review detections in the UI).\n" in out
    assert "  5. Run: make generate ENV=dev   (one run: imports the agent, finds its tables, drafts rules)\n" in out
    assert "     Review envs/dev/generated/, then commit and push envs/ (generate offers it on a terminal: answer y).\n" in out
    # No separate make check step: a deterministic rehearse runs it first.
    assert "Run: make check" not in out
    # A first deterministic rehearse refuses a non-empty account plan (finding #5).
    assert (
        "  6. First deploy only: the account layer (e.g. the new gr_treatment tag policy) needs an approved account "
        "manifest.\n"
        "       On a terminal, the rehearse below shows its plan and asks y/N. Off a terminal (rehearse refuses it), "
        "run:\n"
        "       make account-plan    (plans the shared account layer and writes the manifest to approve; changes nothing live)\n"
        "       make apply-account   (applies exactly that approved manifest)\n"
    ) in out
    assert ("  7. Run: make rehearse ENV=dev   (runs make check ENV=dev first: refuses before any lock or live "
            "call if the committed files fail it)\n") in out
    assert "business_access_enabled" not in out
    assert "     (grants SELECT only from table_readers after its coverage check passes, and Genie CAN_RUN only from acl_groups after the live agent coverage check)\n" in out
    assert re.findall(r"^  (\d+)\. ", out, flags=re.MULTILINE) == [str(n) for n in range(1, 8)]
    assert "shared/examples/dev_to_prod/README.md" in out
    # The champion flow relies on native Data Classification, not the country overlay.
    assert "APJ" not in out
    assert "country" not in out
    # Setup points dev at rehearse (derive, validate and gate first), not plain apply;
    # apply-account is the account layer's approved-manifest apply.
    assert re.findall(r"make apply[\w-]*", out) == ["make apply-account"]
    # Setup names every unfilled placeholder at once, above the steps (finding #21).
    advice = out[:out.index("Next steps")]
    assert re.findall(r"Setup required: replace placeholder (<[^>]+>)", advice) == [
        "<your-deployer-principal-id>", "<most-privileged-group>"], advice
    # The README's Dev order: commit, classify, generate (commit), account layer, rehearse (which checks).
    steps = out[out.index("Next steps"):]
    assert (
        steps.index("envs/dev/auth.auto.tfvars")
        < steps.index("set genie_space_id")
        < steps.index("access_tier_groups")
        < steps.index("SAMPLE_ENV.md")
        < steps.index("enable-classification")
        < steps.rindex("make generate")
        < steps.index("then commit and push envs/")
        < steps.index("make account-plan")
        < steps.index("make apply-account")
        < steps.index("make rehearse")
    )


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_creates_auth_tfvars_private(cloud, tmp_path):
    result = _make(cloud, tmp_path, "setup", "ENV=dev")

    assert result.returncode == 0, result.stderr
    assert _mode(tmp_path / cloud / "envs/dev/auth.auto.tfvars") == 0o600


def test_setup_warns_and_tightens_existing_auth_tfvars(tmp_path):
    auth = tmp_path / "aws/envs/dev/auth.auto.tfvars"
    auth.parent.mkdir(parents=True)
    auth.write_text('databricks_client_secret = "already-here"\n')
    auth.chmod(0o644)

    result = _make("aws", tmp_path, "setup", "ENV=dev")

    assert result.returncode == 0, result.stderr
    assert _mode(auth) == 0o600
    assert "tightening to 0600" in result.stderr


def test_bootstrap_auth_overwrite_only_honored_on_command_line(tmp_path):
    cloud_root = tmp_path / "aws"
    cloud_root.mkdir()
    common = [
        "make", "--no-print-directory", "-n", "bootstrap-sp",
        f"CLOUD_ROOT={cloud_root}", f"SHARED_ROOT={SHARED_ROOT}",
        "ACCOUNT_ID=acct", "WORKSPACE_ID=123", "BOOTSTRAP_AUTH_ENV=dev",
    ]
    env = {**_clean_env(), "BOOTSTRAP_AUTH_OVERWRITE": "1"}

    from_env = subprocess.run(common, cwd=ROOT / "aws", text=True, capture_output=True, env=env)
    from_cli = subprocess.run(
        [*common, "BOOTSTRAP_AUTH_OVERWRITE=1"],
        cwd=ROOT / "aws", text=True, capture_output=True, env=_clean_env(),
    )

    assert "--force-auth-file" not in from_env.stdout
    assert "--force-auth-file" in from_cli.stdout


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_omits_completed_walkthrough_copy_and_renumbers(cloud, tmp_path):
    first = _make(cloud, tmp_path, "setup", "ENV=dev")
    assert first.returncode == 0
    env_file = tmp_path / cloud / "envs/dev/env.auto.tfvars"
    env_file.write_text((SHARED_ROOT / "examples/dev_to_prod/env.auto.tfvars.example").read_text())

    result = _make(cloud, tmp_path, "setup", "ENV=dev")
    assert result.returncode == 0, result.stderr
    assert "cp ../shared/examples/dev_to_prod/env.auto.tfvars.example" not in result.stdout
    assert re.findall(r"^  (\d+)\. ", result.stdout, flags=re.MULTILINE) == [str(n) for n in range(1, 7)]


def test_setup_existing_legacy_env_does_not_request_a_deployer_principal(tmp_path):
    cloud_root = tmp_path / "aws"
    env_dir = cloud_root / "envs" / "dev"
    env_dir.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text('governance_mode = "legacy"\n')

    result = _make("aws", tmp_path, "setup", "ENV=dev")

    assert result.returncode == 0, result.stderr
    assert "deployer_principal_id" not in result.stdout


MALFORMED = 'governance_mode = "deterministic"\ndeployer_principal_id = "<x>"\nnote = "s3cr3t-token-value" {\n'
MODE_SPECIFIC_COMMANDS = ("generate", "rehearse", "release", "release-plan", "promote-to", "apply",
                          "account-plan", "apply-account", "maintain")


@pytest.mark.parametrize("env", ["dev", "prod", "stg"])
def test_setup_with_a_malformed_env_file_warns_and_prints_only_safe_generic_steps(env, tmp_path):
    """Setup's own work is done by then; only the banner can't pick a role, so it never fails on it."""
    env_dir = tmp_path / "aws/envs" / env
    env_dir.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text(MALFORMED)

    result = _make("aws", tmp_path, "setup", f"ENV={env}")

    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert (f"WARNING: could not tell whether envs/{env} is a rehearsal or a promoted env (env-role failed; "
            f"is envs/{env}/env.auto.tfvars valid HCL?), so these steps are generic.\n") in result.stderr
    assert f"  Setup required: fix envs/{env}/env.auto.tfvars: it is not valid HCL\n" in result.stdout
    assert "Traceback" not in out and "UnexpectedToken" not in out and "env-role failed:" not in out
    assert "s3cr3t" not in out
    assert (f"  1. Fix envs/{env}/env.auto.tfvars so it parses, then re-run: make setup ENV={env}   "
            "(prints this env's steps)\n") in result.stdout
    assert f"  2. Then: make check ENV={env}   (read-only check of the committed files)\n" in result.stdout
    # Only commands every env role and mode accepts: nothing a promoted deterministic env refuses.
    assert set(re.findall(r"make ([a-z][a-z-]*)", result.stdout)) <= {"setup", "check"}
    assert not set(re.findall(r"make ([a-z][a-z-]*)", result.stdout)) & set(MODE_SPECIFIC_COMMANDS)
    assert (env_dir / "auth.auto.tfvars").is_file()
    # The step after the banner still runs.
    assert "Version control:" in result.stdout


def _shared_without(tmp_path, *missing):
    """A copy of shared/ (without tests/) missing the given files, for SHARED_ROOT."""
    import shutil

    shared = tmp_path / "shared-copy"
    shutil.copytree(SHARED_ROOT, shared, symlinks=True,
                    ignore=shutil.ignore_patterns("tests", "__pycache__", ".terraform"))
    for name in missing:
        (shared / name).unlink()
    return shared


@pytest.mark.parametrize("env_text", [None, MALFORMED], ids=["no-env-file", "malformed-env-file"])
def test_setup_fails_when_bootstrap_cannot_write_the_auth_file(env_text, tmp_path):
    """Only the banner's role lookup may fall back; a failed bootstrap step fails setup."""
    shared = _shared_without(tmp_path, "auth.auto.tfvars.example")
    cloud_root = tmp_path / "aws"
    env_dir = cloud_root / "envs/dev"
    if env_text is not None:
        env_dir.mkdir(parents=True)
        (env_dir / "env.auto.tfvars").write_text(env_text)

    result = subprocess.run(
        ["make", "--no-print-directory", "setup", "ENV=dev", f"CLOUD_ROOT={cloud_root}", f"SHARED_ROOT={shared}"],
        cwd=ROOT / "aws", text=True, capture_output=True, env=_clean_env())

    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert not (env_dir / "auth.auto.tfvars").exists()
    assert "environment ready" not in out
    assert "Next steps" not in out
    assert "Version control:" not in out
    assert not (cloud_root / ".gitignore").exists()


@pytest.mark.parametrize("env", ["dev", "prod"])
def test_setup_with_every_template_present_still_exits_zero(env, tmp_path):
    result = _make("aws", tmp_path, "setup", f"ENV={env}")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "WARNING" not in result.stderr
    assert (tmp_path / "aws/envs" / env / "auth.auto.tfvars").is_file()
    assert "Workspace environment ready:" in result.stdout and "Next steps" in result.stdout


def test_setup_legacy_dev_banner_commits_and_checks_but_skips_the_account_manifest(tmp_path):
    """A legacy-only checkout applies the account layer itself; its rehearse saves the proven keys."""
    env_dir = tmp_path / "aws/envs/dev"
    env_dir.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text('governance_mode = "legacy"\n')

    result = _make("aws", tmp_path, "setup", "ENV=dev")

    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "setup offers to commit and push" not in out
    assert "Run: make check ENV=dev" in out
    assert "account-plan" not in out and "apply-account" not in out
    assert "     verify-access picks a row-pairing key per masked table; the proven keys are saved and promoted.\n" in out
    assert "prints the keys it proved" not in out
    assert (out.index("make generate ENV=dev")
            < out.index("make check ENV=dev") < out.index("make rehearse ENV=dev"))


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_prod_prints_promote_release_maintain_steps(cloud, tmp_path):
    result = _make(cloud, tmp_path, "setup", "ENV=prod")
    assert result.returncode == 0, result.stderr
    out = result.stdout

    assert "Next steps (prod promotion — the walkthrough's Prod section; finish the source rehearsal first):" in out
    # promote-to refuses a placeholder deployer, so the SP and its ID come first (finding #21).
    order = [
        "  1. Create or confirm the prod deployment SP (none yet? ../shared/docs/prerequisites.md, make bootstrap-sp), then edit\n"
        "     envs/prod/auth.auto.tfvars — its client_id / client_secret + prod workspace host & id\n",
        "     Then commit that SP's client_id as deployer_principal_id in envs/prod/env.auto.tfvars",
        "     promote-to refuses while deployer_principal_id is still a placeholder.\n",
        "  2. In envs/prod/env.auto.tfvars, also set sql_warehouse_id",
        "  3. Replace the seeded promote_from and catalog_map values in envs/prod/env.auto.tfvars",
        'make promote-to ENV=prod\n',
        "     (FROM=/CATALOG_MAP= remain optional command-line overrides.)\n",
        "In Catalog Explorer, open the catalog > Details tab > Data classification: turn it on",
        "Prefer a script? make enable-classification ENV=prod turns it on",
        "  5. Commit and push .gitignore and envs/ to origin/main (promote-to offers it on a terminal: answer y;\n"
        "     or git add .gitignore envs/prod/ envs/account/ && git commit yourself); on that commit run:\n",
        "     make rehearse ENV=dev   (release-plan needs a passing rehearsal of this commit), then:\n",
        "     make release-plan ENV=prod   (runs make check ENV=prod and promote-to CHECK=1, checks the commit "
        "was rehearsed",
        "Get that manifest approved, then: make release ENV=prod\n",
        "make maintain ENV=prod",
    ]
    positions = [out.index(step) for step in order]
    assert positions == sorted(positions)
    # release-plan needs a dev rehearsal of the promotion commit; prod itself never rehearses.
    assert "make rehearse ENV=prod" not in out
    assert "make generate" not in out
    assert "deployer_principal_id" in out
    assert "VERIFY_KEY_COLUMN" not in out
    assert "DEST_CATALOG_MAP" not in out
    assert "     It proves a row-pairing key for every masked table before applying (no key to set).\n" in out
    assert "VERIFY_KEY_COLUMN" not in out and "verify_key_column" not in out
    assert "Set business_access_enabled" not in out
    assert "business_access_enabled" not in out
    assert "     (grants SELECT only from table_readers after its coverage check passes, and Genie CAN_RUN only from acl_groups after the live agent coverage check)\n" in out
    assert "make apply ENV=prod" not in out
    assert "make verify-access ENV=prod" not in out
    assert "make certify" not in out
    assert re.findall(r"^  (\d+)\. ", out, flags=re.MULTILINE) == ["1", "2", "3", "4", "5", "6"]
    assert "shared/examples/dev_to_prod/README.md" in out
    env_text = (tmp_path / cloud / "envs/prod/env.auto.tfvars").read_text()
    env_cfg = hcl2.loads(env_text)
    assert env_cfg["governance_mode"] == "deterministic"
    assert env_cfg["deployer_principal_id"] == "<your-deployer-principal-id>"
    assert env_cfg["access_tier_groups"] == ["<most-privileged-group>", "<least-privileged-group>"]
    assert "enable_auto_tagging" not in hcl2.loads(env_text)
    assert 'promote_from = "dev"' in env_text
    assert 'catalog_map  = { "<dev_catalog>" = "<prod_catalog>" }' in env_text


def test_setup_prod_rehearses_its_promote_from_source_before_release_plan(tmp_path):
    """release-plan needs a same-commit receipt from promote_from's env, not always dev."""
    assert _make("aws", tmp_path, "setup", "ENV=prod").returncode == 0
    env_file = tmp_path / "aws" / "envs" / "prod" / "env.auto.tfvars"
    text = env_file.read_text()
    assert 'promote_from = "dev"' in text
    env_file.write_text(text.replace('promote_from = "dev"', 'promote_from = "stg"'))

    result = _make("aws", tmp_path, "setup", "ENV=prod")

    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "     make rehearse ENV=stg   (release-plan needs a passing rehearsal of this commit), then:\n" in out
    assert "make rehearse ENV=dev" not in out
    assert out.index("make rehearse ENV=stg") < out.index("make release-plan ENV=prod")


def test_setup_prod_banner_uses_legacy_promote_from_mode(tmp_path):
    dev = tmp_path / "aws/envs/dev"
    dev.mkdir(parents=True)
    dev.joinpath("env.auto.tfvars").write_text('governance_mode = "legacy"\n')
    result = _make("aws", tmp_path, "setup", "ENV=prod")
    assert result.returncode == 0, result.stderr
    assert "  5. Run: make release ENV=prod" in result.stdout
    assert "make rehearse ENV=dev" not in result.stdout
    assert "make release-plan ENV=prod" not in result.stdout
    assert 'migrate envs/dev (shared/docs/legacy-migration.md), or set governance_mode = "legacy"' in result.stdout


@pytest.mark.parametrize("env", ["prod", "stg"])
def test_setup_legacy_promoted_env_keeps_the_promotion_banner(env, tmp_path):
    dev = tmp_path / "aws/envs/dev"
    target = tmp_path / f"aws/envs/{env}"
    dev.mkdir(parents=True)
    target.mkdir()
    dev.joinpath("env.auto.tfvars").write_text('governance_mode = "legacy"\n')
    target.joinpath("env.auto.tfvars").write_text(
        'governance_mode = "legacy"\npromote_from = "dev"\n'
        f'catalog_map = {{ "dev_catalog" = "{env}_catalog" }}\n')
    result = _make("aws", tmp_path, "setup", f"ENV={env}")
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert f"Next steps ({env} promotion" in out
    assert f"{env} workspace host & id" in out
    assert f"make promote-to ENV={env}" in out
    assert f"make release ENV={env}" in out
    assert f"make rehearse ENV={env}" not in out
    assert "make release-plan" not in out
    assert "cp ../shared/examples/dev_to_prod/env.auto.tfvars.example" not in out


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_non_default_env_seeds_promotion_settings(cloud, tmp_path):
    result = _make(cloud, tmp_path, "setup", "ENV=stg")
    assert result.returncode == 0, result.stderr
    env_text = (tmp_path / cloud / "envs/stg/env.auto.tfvars").read_text()
    assert 'promote_from = "dev"' in env_text
    assert 'catalog_map  = { "<dev_catalog>" = "<stg_catalog>" }' in env_text


def test_setup_stg_promoted_uses_the_promotion_banner_without_overwrite_or_rehearse(tmp_path):
    result = _make("aws", tmp_path, "setup", "ENV=stg")
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "Next steps (stg promotion" in out
    assert "stg workspace host & id" in out
    assert "prod workspace host & id" not in out
    assert "make promote-to ENV=stg" in out
    assert "make rehearse ENV=dev" in out
    assert "make rehearse ENV=stg" not in out
    assert "cp ../shared/examples/dev_to_prod/env.auto.tfvars.example envs/stg/env.auto.tfvars" not in out


@pytest.mark.parametrize("env", ["prod", "stg"])
def test_setup_promoted_env_says_set_group_map_and_keeps_the_tier_placeholders(env, tmp_path):
    """promote-to writes a promoted env's tiers from its source's through group_map."""
    result = _make("aws", tmp_path, "setup", f"ENV={env}")
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert (f"     Where {env}'s group names differ from dev's, set group_map there (each dev group = its {env} "
            "group): promote-to writes this env's access_tier_groups from dev's through it, so leave those "
            "placeholders.\n") in out, out
    assert "Replace this env's access_tier_groups placeholders" not in out
    assert "access_tier_groups placeholder" not in out  # no "Setup required" advice to replace them
    cfg = hcl2.loads((tmp_path / "aws" / "envs" / env / "env.auto.tfvars").read_text())
    assert cfg["access_tier_groups"] == ["<most-privileged-group>", "<least-privileged-group>"]


def test_setup_stg_without_promote_from_uses_the_rehearsal_banner(tmp_path):
    env = tmp_path / "aws/envs/stg"
    env.mkdir(parents=True)
    env.joinpath("env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    result = _make("aws", tmp_path, "setup", "ENV=stg")
    assert result.returncode == 0, result.stderr
    assert "Next steps (dev rehearsal" in result.stdout
    assert "make rehearse ENV=stg" in result.stdout
    assert "promotion — the walkthrough's Prod section" not in result.stdout


@pytest.mark.parametrize("mode", ["deterministic", "legacy"])
def test_setup_non_dev_rehearsal_banner_says_generate_imports_agents_only_in_dev(mode, tmp_path):
    # make generate imports agents (agent_ownership import-new) only with ENV=dev.
    outs = {}
    for env in ("dev", "stg"):
        env_dir = tmp_path / "aws/envs" / env
        env_dir.mkdir(parents=True)
        # An agent ID, so deterministic isn't governance-only.
        env_dir.joinpath("env.auto.tfvars").write_text(f'governance_mode = "{mode}"\ngenie_space_id = "01ef00000000000a"\n')
        result = _make("aws", tmp_path, "setup", f"ENV={env}")
        assert result.returncode == 0, result.stderr
        outs[env] = result.stdout
    dev, stg = outs["dev"], outs["stg"]
    assert "   (one run: imports the agent, finds its tables, drafts rules)\n" in dev
    assert "imports the agent" not in stg
    assert ("Run: make generate ENV=stg   (one run: finds the agent's tables, drafts rules; "
            "agents are imported only in dev)\n") in stg
    # Same commands in the same order; only the wording differs.
    commands = lambda out: re.findall(r"make [a-z][a-z-]*(?: [A-Z_]+=\S+)*", out)
    assert commands(stg) == [c.replace("ENV=dev", "ENV=stg") for c in commands(dev)]


@pytest.mark.parametrize("target", ["promote-to", "check"])
@pytest.mark.parametrize("ci", [False, True])
def test_duplicate_catalog_map_is_actionable_without_parser_traceback(target, ci, tmp_path):
    assert _make("aws", tmp_path, "setup", "ENV=prod").returncode == 0
    env_file = tmp_path / "aws/envs/prod/env.auto.tfvars"
    env_file.write_text(env_file.read_text() + '\ncatalog_map = { "other" = "prod" }\n')
    args = [target, "ENV=prod"] + (["CI=true"] if ci else [])
    result = _make("aws", tmp_path, *args)
    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "envs/prod/env.auto.tfvars: catalog_map is set twice; keep one" in output
    assert "Traceback" not in output and "VisitError" not in output


def test_duplicate_catalog_map_helpers_return_one(tmp_path):
    envs = tmp_path / "envs"
    dev, prod = envs / "dev", envs / "prod"
    dev.mkdir(parents=True)
    prod.mkdir()
    dev.joinpath("env.auto.tfvars").write_text('uc_tables = ["dev.s.t"]\n')
    duplicate = ('governance_mode = "deterministic"\npromote_from = "dev"\n'
                 'catalog_map = { "dev" = "prod" }\ncatalog_map = { "other" = "prod" }\n')
    prod_file = prod / "env.auto.tfvars"
    prod_file.write_text(duplicate)
    commands = [
        [sys.executable, str(SHARED_ROOT / "genie_space_placeholder.py"), str(prod_file)],
        [sys.executable, str(SHARED_ROOT / "scripts/saved_settings.py"), "promote-resolve",
         "--env", "prod", "--env-dir", str(prod), "--envs-dir", str(envs)],
    ]
    for command in commands:
        result = subprocess.run(command, text=True, capture_output=True, env=_clean_env())
        assert result.returncode == 1
        assert "catalog_map is set twice; keep one" in result.stderr
        assert "Traceback" not in result.stderr and "VisitError" not in result.stderr


@pytest.mark.parametrize("env", ["dev", "prod", "account"])
def test_setup_mentions_only_existing_make_targets(env, tmp_path):
    result = _make("aws", tmp_path, "setup", f"ENV={env}")
    assert result.returncode == 0, result.stderr
    mentioned = _mentioned_targets(result.stdout)
    assert mentioned
    assert set(mentioned) <= _defined_targets()


def test_setup_does_not_create_account_admin_env(tmp_path):
    admin_env = tmp_path / "account-admin.aws.env"
    result = _make("aws", tmp_path, "setup", "ENV=dev", f"ACCOUNT_ADMIN_ENV={admin_env}")
    assert result.returncode == 0, result.stderr
    assert not admin_env.exists()
    assert "account-admin" not in result.stdout
    assert "test-ci" not in result.stdout


def test_test_ci_guard_rejects_missing_custom_account_admin_env(tmp_path):
    admin_env = tmp_path / "missing.env"
    result = _make("aws", tmp_path, "test-ci", f"ACCOUNT_ADMIN_ENV={admin_env}")
    assert result.returncode != 0
    assert f"ACCOUNT_ADMIN_ENV file not found: {admin_env}" in result.stdout
    assert "CI Pipeline" not in result.stdout
    assert not admin_env.exists()


def test_test_ci_guard_creates_default_account_admin_env_and_stops(tmp_path):
    admin_env = tmp_path / "account-admin.aws.env"
    result = _make(
        "aws",
        tmp_path,
        "test-ci",
        f"_DEFAULT_ACCOUNT_ADMIN_ENV={admin_env}",
        f"ACCOUNT_ADMIN_ENV={admin_env}",
    )
    assert result.returncode != 0
    assert f"Created {admin_env}" in result.stdout
    assert "CI Pipeline" not in result.stdout
    assert admin_env.read_text() == (SHARED_ROOT / "scripts" / "account-admin.aws.env.example").read_text()


def test_cross_env_promote_points_to_release_not_apply():
    makefile = (SHARED_ROOT / "Makefile.shared").read_text()
    block = makefile[makefile.index("=== Promote complete:"):]
    block = block[:block.index("trap - EXIT")]

    # release-steps prints release-plan and release after a same-commit source rehearsal, or a legacy release.
    assert '$(_RELEASE_STEPS) --env-dir "$$dest_env_dir" --env "$(DEST_ENV)"' in block
    assert "make apply ENV=$(DEST_ENV)" not in block


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_seeds_the_azure_account_host_in_a_new_auth_file_only(cloud, tmp_path):
    """The roots default databricks_account_host to AWS's, so azure/ setup writes Azure's."""
    existing = tmp_path / cloud / "envs/prod/auth.auto.tfvars"
    existing.parent.mkdir(parents=True)
    existing.write_text('databricks_client_id = "kept"\n')
    existing.chmod(0o600)

    for env in ("dev", "prod"):
        result = _make(cloud, tmp_path, "setup", f"ENV={env}")
        assert result.returncode == 0, result.stdout + result.stderr

    auth = tmp_path / cloud / "envs/dev/auth.auto.tfvars"
    config = hcl2.loads(auth.read_text())
    if cloud == "azure":
        assert config["databricks_account_host"] == "https://accounts.azuredatabricks.net"
    else:
        assert "databricks_account_host" not in config
    assert _mode(auth) == 0o600
    # An auth file that already exists is never rewritten.
    assert existing.read_text() == 'databricks_client_id = "kept"\n'
