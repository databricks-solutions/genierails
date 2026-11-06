"""make check and make promote-to CHECK=1 are read-only (real make, real git).

Each test builds a throwaway repo with the real aws/Makefile and shared/, runs
make the way CI would, and proves envs/ is byte-identical afterwards and git
sees no change. Terraform is a stub on PATH, so the structural plan runs
without credentials.
"""

import base64
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import hcl2
import pytest

from shared.scripts import check_env
from shared.scripts.exposure_token import ENV_VAR as EXPOSURE_TOKEN_ENV, mint as mint_exposure_token

ROOT = Path(__file__).resolve().parents[2]
SHARED = ROOT / "shared"

if shutil.which("make") is None or shutil.which("git") is None:
    pytest.skip("make and git are required", allow_module_level=True)

DEV_ENV = '''governance_mode = "deterministic"
deployer_principal_id = "d0000000-0000-0000-0000-00000000d001"
genie_spaces = [
  { genie_space_id = "s1", uc_tables = ["paycat.s.t"] },
]
'''
DEV_GENERATED = '''groups = { pay_group = {} }
tag_policies = [
  { key = "gr_treatment", description = "t", values = ["account_last4"] },
]
tag_assignments = [
  { entity_type = "columns", entity_name = "paycat.s.t.account_id", tag_key = "gr_treatment", tag_value = "account_last4" },
]
fgac_policies = [
  { name = "pay_mask", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "paycat", to_principals = ["pay_group"], comment = "c", match_condition = "hasTagValue('gr_treatment', 'account_last4')", match_alias = "a", function_name = "mask_account_number", function_catalog = "paycat", function_schema = "s" },
]
genie_space_configs = {
  Payments = { title = "Payments" }
}
genie_space_id_to_name = { s1 = "Payments" }
'''
MASKING_SQL = '''USE CATALOG paycat;
USE SCHEMA s;
CREATE OR REPLACE FUNCTION mask_account_number(v STRING) RETURNS STRING RETURN CONCAT('****', RIGHT(v, 4));
'''
GITIGNORE = ("auth.auto.tfvars\n*.tfstate*\n.terraform/\n.*.json\n!.genie_space_id\nclassification.auto.tfvars\n"
             "discovered_uc_tables.auto.tfvars\n")
# The champion template: the agent is given by ID only, and make generate
# discovers its tables into the git-ignored data_access file.
CHAMPION_ENV = DEV_ENV.replace(', uc_tables = ["paycat.s.t"]', "")
DISCOVERED = ('discovered_uc_tables = ["paycat.s.t"]\n'
              'discovered_table_agents = { "paycat.s.t" = ["Payments"] }\n')


def _b64(value):
    return '"' + base64.b64encode(json.dumps(value).encode()).decode() + '"'


# Answers the coverage check's and CAN_RUN check's terraform console queries
# like an empty state would, and logs every plan (noting the structural-check
# variable terraform_layer.sh adds only for check's one-use token); no provider ever runs.
FAKE_TERRAFORM = f"""#!/bin/sh
case "$1" in
  console)
    case "$PWD" in
      */data_access) echo '{_b64({"fingerprint": "f", "grant_tables": [], "acknowledged_columns": [], "needs_gate": False, "new_grants": []})}' ;;
      *) echo '{_b64({"groups": {}, "blocker": "no state", "widening": {}, "missing": {}, "applied": {}})}' ;;
    esac ;;
  plan)
    expected=""
    structural=""
    for arg in "$@"; do
      [ "$arg" = "-var=expected_pre_release_withholding=true" ] && expected=" expected-withholding"
      [ "$arg" = "-var=structural_check=true" ] && structural=" structural-check"
    done
    if [ "$(basename "$PWD")" = "data_access" ] && [ -z "$structural" ]; then
      echo 'Warning: Check block assertion failed' >&2
      echo 'run make check ENV=dev, which runs the coverage check' >&2
    fi
    echo "plan $(basename "$PWD")$expected$structural" >> "$FAKE_TF_LOG"; echo "FAKE PLAN" ;;
esac
exit 0
"""


def _env(tmp_path, **extra):
    drop = ("MAKEFLAGS", "MAKELEVEL", "MFLAGS", "ENV", "FROM", "CATALOG_MAP", "CHECK", "GENIERAILS_ALLOW_CI_APPLY",
            "CI", "TF_BUILD", "JENKINS_URL", "GITLAB_CI", "BUILDKITE", "CIRCLECI")
    env = {k: v for k, v in os.environ.items() if k not in drop and not k.startswith("DATABRICKS_")}
    env.update(PATH=f"{tmp_path / 'bin'}:{os.environ['PATH']}", FAKE_TF_LOG=str(tmp_path / "tf.log"),
               HOME=str(tmp_path), PYTHONDONTWRITEBYTECODE="1", **extra)
    return env


def _git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), "-c", "user.email=t@example.com", "-c", "user.name=t",
                           *args],
                          text=True, capture_output=True, check=True).stdout


def _make(repo, tmp_path, *args, **env):
    return subprocess.run(["make", "--no-print-directory", "-C", str(repo / "aws"), *args],
                          text=True, capture_output=True, env=_env(tmp_path, **env), timeout=300)


def _snapshot(envs):
    return {str(p.relative_to(envs)): (os.readlink(p) if p.is_symlink() else p.read_bytes() if p.is_file() else None)
            for p in sorted(envs.rglob("*"))}


@pytest.fixture
def repo(tmp_path):
    """A committed dev env: generate output plus the split promote writes."""
    return _build(tmp_path, DEV_ENV)


def _build(tmp_path, dev_env, discovered=None):
    repo = tmp_path / "repo"
    dev = repo / "aws" / "envs" / "dev"
    (dev / "generated").mkdir(parents=True)
    (repo / "aws" / "envs" / "account").mkdir()
    shutil.copy2(ROOT / "aws" / "Makefile", repo / "aws" / "Makefile")
    (repo / "shared").symlink_to(SHARED)
    (dev / "env.auto.tfvars").write_text(dev_env)
    if discovered:
        (dev / "data_access").mkdir()
        (dev / "data_access" / "discovered_uc_tables.auto.tfvars").write_text(discovered)
    (dev / "generated" / "abac.auto.tfvars").write_text(DEV_GENERATED)
    (dev / "generated" / "masking_functions.sql").write_text(MASKING_SQL)
    (repo / ".gitignore").write_text(GITIGNORE)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "terraform").write_text(FAKE_TERRAFORM)
    (bin_dir / "terraform").chmod(0o755)
    assert _make(repo, tmp_path, "promote", "ENV=dev").returncode == 0
    _record(dev)
    _git(repo, "init", "-q")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "dev")
    return repo


def _record(env_dir):
    """What make generate (dev) or a release (prod) leaves in generated/governed_tables.json."""
    subprocess.run([sys.executable, str(SHARED / "scripts" / "sticky_governance.py"), "record",
                    "--env-dir", str(env_dir), "--config", str(env_dir / "generated" / "abac.auto.tfvars")],
                   check=True)


def _commit(repo, message):
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)


@pytest.fixture
def promoted(repo, tmp_path):
    return _promote(repo, tmp_path)


def _promote(repo, tmp_path):
    prod = _envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n'
                                          'deployer_principal_id = "d0000000-0000-0000-0000-00000000d002"\n')
    result = _make(repo, tmp_path, "promote-to", "ENV=prod", "FROM=dev", "CATALOG_MAP=paycat=ppay")
    assert result.returncode == 0, result.stdout + result.stderr
    _commit(repo, "promote")
    return repo


def _envs(repo):
    return repo / "aws" / "envs"


def _state(repo):
    """Every byte and symlink under envs/, plus git's view including ignored files."""
    status = _git(repo, "status", "--porcelain", "--ignored", "--untracked-files=all")
    return _snapshot(_envs(repo)), status


_baseline = _state


def _assert_read_only(repo, before):
    assert _state(repo) == before


@pytest.mark.parametrize("ci", ["", "true"])
def test_check_passes_on_a_clean_generated_tree_and_writes_nothing(repo, tmp_path, ci):
    before = _baseline(repo)

    result = _make(repo, tmp_path, "check", "ENV=dev", **({"CI": ci} if ci else {}))

    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert "OK: generated files (envs/dev): committed files match." in out
    assert "STRUCTURAL plan: envs/dev as committed, against EMPTY state" in out
    assert "--- check 5/6: mask-library unit tests ---" in out and " passed in " in out
    assert "=== make check ENV=dev: passed ===" in out
    assert "run make check ENV=dev, which runs the coverage check" not in out
    assert "applying targets cannot run in CI" not in out
    assert (tmp_path / "tf.log").read_text().split("\n")[:2] == ["plan data_access structural-check",
                                                                  "plan workspace expected-withholding"]
    _assert_read_only(repo, before)
    assert _git(repo, "status", "--porcelain") == ""


@pytest.mark.parametrize("structural_token", ["unset", "missing", "world-readable"])
@pytest.mark.parametrize("ci", ["", "true"])
def test_real_plan_keeps_business_select_withheld_warning(repo, tmp_path, ci, structural_token):
    token = mint_exposure_token(repo / "aws" / "envs" / "dev")
    run_env = {EXPOSURE_TOKEN_ENV: str(token)} | ({"CI": ci} if ci else {})
    if structural_token == "missing":
        run_env["GENIERAILS_STRUCTURAL_CHECK_TOKEN"] = str(tmp_path / "missing-structural-token")
    elif structural_token == "world-readable":
        forged = tmp_path / "forged-structural-token"
        forged.write_text(str((repo / "aws" / "envs" / "dev").resolve()) + "\n")
        forged.chmod(0o644)
        run_env["GENIERAILS_STRUCTURAL_CHECK_TOKEN"] = str(forged)
    result = _make(repo, tmp_path, "plan", "ENV=dev", **run_env)

    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert "run make check ENV=dev, which runs the coverage check" in out
    assert "plan data_access structural-check" not in (tmp_path / "tf.log").read_text()


def test_check_fails_when_a_generated_file_is_hand_edited(repo, tmp_path):
    # Still valid config, so only the regeneration catches it.
    split = _envs(repo) / "dev" / "data_access" / "abac.auto.tfvars"
    text = split.read_text()
    assert 'comment = "c"' in text or '"c"' in text
    split.write_text(text.replace('"c"', '"hand edited"', 1))
    _commit(repo, "hand edit")
    before = _baseline(repo)

    result = _make(repo, tmp_path, "check", "ENV=dev", CI="true")

    assert result.returncode != 0
    assert "envs/dev/data_access/abac.auto.tfvars: differs from the regenerated file" in result.stderr
    assert "Generated files are never hand-edited. Run make generate ENV=dev" in result.stderr
    assert "=== make check ENV=dev: FAILED (generated files (envs/dev)) ===" in result.stderr
    assert not (tmp_path / "tf.log").exists()
    _assert_read_only(repo, before)


def test_check_fails_when_the_governed_table_record_is_not_committed(repo, tmp_path):
    _git(repo, "rm", "-q", "aws/envs/dev/generated/governed_tables.json")
    _commit(repo, "drop record")
    before = _baseline(repo)

    result = _make(repo, tmp_path, "check", "ENV=dev")

    assert result.returncode != 0
    assert "envs/dev/generated/governed_tables.json: missing from the commit" in result.stderr
    _assert_read_only(repo, before)


def test_check_notes_uncommitted_split_files_without_failing(repo, tmp_path):
    _git(repo, "rm", "-q", "aws/envs/dev/abac.auto.tfvars")
    _commit(repo, "split not committed")
    before = _baseline(repo)

    result = _make(repo, tmp_path, "check", "ENV=dev")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "NOTE: envs/dev/abac.auto.tfvars is not committed; the applying commands write it." in result.stdout
    _assert_read_only(repo, before)


@pytest.mark.parametrize("ci", ["", "true"])
def test_check_of_setup_but_not_generated_env_skips_with_next_step(repo, tmp_path, ci):
    dev = _envs(repo) / "dev"
    shutil.rmtree(dev / "generated")
    for path in (dev / "abac.auto.tfvars", dev / "workspace.auto.tfvars"):
        path.unlink(missing_ok=True)
    shutil.rmtree(dev / "data_access", ignore_errors=True)
    _commit(repo, "setup only")
    before = _baseline(repo)

    result = _make(repo, tmp_path, "check", "ENV=dev", **({"CI": ci} if ci else {}))

    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert "NOTE: skipped: envs/dev has not been generated; run make generate ENV=dev." in out
    assert "FAILED (validate)" not in out
    _assert_read_only(repo, before)


@pytest.mark.parametrize("target", ["check", "plan"])
@pytest.mark.parametrize("ci", ["", "true"])
def test_read_only_targets_never_post_classification_config(repo, tmp_path, target, ci):
    dev = _envs(repo) / "dev"
    env_file = dev / "env.auto.tfvars"
    env_file.write_text(env_file.read_text() + '\nenable_classification = true\n')
    (dev / "auth.auto.tfvars").write_text(
        'databricks_workspace_host = "https://example.invalid"\n'
        'databricks_client_id = "client"\n'
        'databricks_client_secret = "secret"\n'
        'serverless_usage_policy_id = "policy-123"\n'
    )
    _commit(repo, "classification configured")

    sdk = tmp_path / "fake-sdk/databricks/sdk"
    sdk.mkdir(parents=True)
    (sdk.parent / "__init__.py").write_text("")
    (sdk / "errors.py").write_text("class NotFound(Exception):\n    pass\n")
    (sdk / "useragent.py").write_text(
        "def with_extra(*args, **kwargs):\n    return None\n"
    )
    (sdk / "config.py").write_text(
        "class Config:\n    def __init__(self, **kwargs):\n        self.__dict__.update(kwargs)\n"
    )
    (sdk / "__init__.py").write_text('''
import os
from .errors import NotFound
class _Classification:
    def get_catalog_config(self, name):
        raise NotFound(name)
class _API:
    def do(self, method, path, **kwargs):
        with open(os.environ["FAKE_SDK_CALLS"], "a") as handle:
            handle.write(f"{method} {path}\\n")
class WorkspaceClient:
    def __init__(self, **kwargs):
        self.data_classification = _Classification()
        self.api_client = _API()
''')
    calls = tmp_path / f"{target}-{ci or 'local'}-sdk-calls"
    site_packages = next(path for path in sys.path if path.endswith("site-packages"))
    before = _baseline(repo)
    token_env = {}
    if target == "plan":
        token = tmp_path / f"{target}-{ci or 'local'}-exposure-token"
        token.write_text(str(dev.resolve()) + "\n")
        token.chmod(0o600)
        token_env["GENIERAILS_EXPOSURE_TOKEN"] = str(token)

    result = _make(
        repo, tmp_path, target, "ENV=dev",
        PYTHONPATH=os.pathsep.join((str(tmp_path / "fake-sdk"), site_packages)),
        PIP_NO_INDEX="1", DATABRICKS_CONFIG_FILE="/dev/null",
        GIT_CONFIG_GLOBAL="/dev/null", FAKE_SDK_CALLS=str(calls),
        **token_env,
        **({"CI": ci} if ci else {}),
    )

    assert result.returncode == 0, result.stdout + result.stderr
    recorded = calls.read_text().splitlines() if calls.exists() else []
    assert all(call.startswith("GET ") for call in recorded), recorded
    if target == "check":
        _assert_read_only(repo, before)


def test_legacy_check_skips_regeneration_with_a_note(repo, tmp_path):
    env_file = _envs(repo) / "dev" / "env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace('governance_mode = "deterministic"\n', ""))
    split = _envs(repo) / "dev" / "data_access" / "abac.auto.tfvars"
    split.write_text(split.read_text() + "\n# hand edit\n")
    _commit(repo, "legacy")
    before = _baseline(repo)

    result = _make(repo, tmp_path, "check", "ENV=dev", CI="true")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "NOTE: skipped: envs/dev is in legacy mode" in result.stdout
    _assert_read_only(repo, before)


def test_legacy_check_validates_committed_split_config_without_generated(repo, tmp_path):
    dev = _envs(repo) / "dev"
    env_file = dev / "env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace('governance_mode = "deterministic"\n', ""))
    shutil.rmtree(dev / "generated")
    split = dev / "data_access" / "abac.auto.tfvars"
    split.write_text(split.read_text().replace('to_principals = ["pay_group"]',
                                                'to_principals = ["no_such_group"]'))
    _commit(repo, "broken legacy split without generated output")
    before = _baseline(repo)

    result = _make(repo, tmp_path, "check", "ENV=dev")

    out = result.stdout + result.stderr
    assert result.returncode == 2, out
    assert "=== make check ENV=dev: FAILED (validate) ===" in result.stderr
    assert "has not been generated; run make generate" not in out
    _assert_read_only(repo, before)


def test_check_stops_on_the_first_failing_step(repo, tmp_path):
    split = _envs(repo) / "dev" / "data_access" / "abac.auto.tfvars"
    split.write_text(split.read_text() + "\nfgac_policies = [ {\n")
    _commit(repo, "broken")
    before = _baseline(repo)

    result = _make(repo, tmp_path, "check", "ENV=dev")

    assert result.returncode != 0
    assert "=== make check ENV=dev: FAILED (validate) ===" in result.stderr
    assert "--- check 2/6" not in result.stdout
    assert not (tmp_path / "tf.log").exists()
    _assert_read_only(repo, before)


@pytest.mark.parametrize("args, message", [
    (["ENV=account"], "This command is workspace-driven"),
    (["ENV=dev", "ENV_DIR=/tmp/elsewhere"], "is not envs/dev for ENV=dev; drop ENV_DIR"),
])
def test_check_refusals(repo, tmp_path, args, message):
    before = _baseline(repo)
    result = _make(repo, tmp_path, "check", *args)
    assert result.returncode != 0
    assert message in result.stderr
    _assert_read_only(repo, before)


@pytest.mark.parametrize("target", [["check", "ENV=dev"], ["promote-to", "ENV=prod", "CHECK=1"]])
def test_dry_run_runs_nothing(repo, tmp_path, target):
    before = _baseline(repo)
    result = _make(repo, tmp_path, "-n", *target)
    assert result.returncode == 0, result.stderr
    assert "check_env.py" in result.stdout
    assert "===" not in result.stdout
    _assert_read_only(repo, before)


@pytest.mark.parametrize("ci", ["", "true"])
def test_promote_check_passes_on_committed_promote_output_and_writes_nothing(promoted, tmp_path, ci):
    before = _baseline(promoted)

    result = _make(promoted, tmp_path, "promote-to", "ENV=prod", "CHECK=1", **({"CI": ci} if ci else {}))

    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert "OK: promote-to ENV=prod CHECK=1: committed files match." in out
    assert "applying targets cannot run in CI" not in out
    _assert_read_only(promoted, before)
    assert _git(promoted, "status", "--porcelain") == ""


@pytest.mark.parametrize("path", ["prod/generated/abac.auto.tfvars", "prod/generated/expected_classification.json",
                                  "account/abac.auto.tfvars"])
def test_promote_check_fails_when_promote_output_is_hand_edited(promoted, tmp_path, path):
    edited = _envs(promoted) / path
    edited.write_text(edited.read_text().replace("account_last4", "redact"))
    before = _baseline(promoted)

    result = _make(promoted, tmp_path, "promote-to", "ENV=prod", "CHECK=1", CI="true")

    assert result.returncode != 0
    assert f"envs/{path}: differs from the regenerated file" in result.stderr
    assert "Run make promote-to ENV=prod and commit the result." in result.stderr
    _assert_read_only(promoted, before)


def test_promote_check_fails_when_the_promotion_is_not_committed(repo, tmp_path):
    prod = _envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text('governance_mode = "deterministic"\npromote_from = "dev"\n'
                                          'deployer_principal_id = "d0000000-0000-0000-0000-00000000d002"\n'
                                          'catalog_map = { "paycat" = "ppay" }\n')
    _commit(repo, "prod settings only")
    before = _baseline(repo)

    result = _make(repo, tmp_path, "promote-to", "ENV=prod", "CHECK=1")

    assert result.returncode != 0
    assert "envs/prod/generated/abac.auto.tfvars: missing from the commit" in result.stderr
    _assert_read_only(repo, before)


def test_promote_check_never_reads_prod_state_or_credentials(promoted, tmp_path):
    # Local state and credentials (ignored runtime files) never reach the copy;
    # a state-kept policy name would otherwise change promote-to's output.
    prod = _envs(promoted) / "prod"
    (prod / "data_access" / "terraform.tfstate").write_text("not json")
    (prod / "auth.auto.tfvars").write_text('databricks_workspace_host = "https://unreachable.invalid"\n'
                                          'databricks_client_id = "x"\ndatabricks_client_secret = "y"\n')
    before = _baseline(promoted)

    result = _make(promoted, tmp_path, "promote-to", "ENV=prod", "CHECK=1")

    assert result.returncode == 0, result.stdout + result.stderr
    _assert_read_only(promoted, before)


# -- Only committed files decide the result -----------------------------------

def _assert_check_prod_passes_offline(result):
    # Steps 1-5 pass offline; the plan (6) needs prod's live classification config.
    out = result.stdout + result.stderr
    assert "OK: promote-to ENV=prod CHECK=1: committed files match." in out, out
    assert "OK: generated files (envs/prod): committed files match." in out, out
    assert "--- check 6/6: plan ---" in out, out
    assert result.returncode == 0 or "=== make check ENV=prod: FAILED (plan) ===" in out, out


@pytest.fixture
def champion(tmp_path):
    """Dev configured by agent ID only, generated with the ignored discovery present, promoted, committed."""
    repo = _promote(_build(tmp_path, CHAMPION_ENV, DISCOVERED), tmp_path)
    _record(_envs(repo) / "prod")
    _commit(repo, "prod governed tables")
    ignored = _envs(repo) / "dev" / "data_access" / "discovered_uc_tables.auto.tfvars"
    assert "!!" in _git(repo, "status", "--porcelain", "--ignored", str(ignored))
    return repo


def test_generate_commits_the_id_only_agents_tables(champion):
    manifest = json.loads((_envs(champion) / "dev" / "generated" / "governed_tables.json").read_text())
    assert manifest["agent_tables"] == {"s1": ["paycat.s.t"]}
    prod = hcl2.loads((_envs(champion) / "prod" / "env.auto.tfvars").read_text())
    assert [space["uc_tables"] for space in prod["genie_spaces"]] == [["ppay.s.t"]]


def test_clean_clone_of_an_id_only_agent_passes_check_and_promote_check_in_ci(champion, tmp_path):
    clone = tmp_path / "clone"
    subprocess.run(["git", "clone", "-q", str(champion), str(clone)], check=True, capture_output=True)
    assert not (_envs(clone) / "dev" / "data_access" / "discovered_uc_tables.auto.tfvars").exists()
    before = _baseline(clone)

    promote = _make(clone, tmp_path, "promote-to", "ENV=prod", "CHECK=1", CI="true")
    check = _make(clone, tmp_path, "check", "ENV=prod", CI="true")

    assert promote.returncode == 0, promote.stdout + promote.stderr
    assert "Resolved 1 committed table(s) for Payments" in promote.stdout
    _assert_check_prod_passes_offline(check)
    _assert_read_only(clone, before)


def test_id_only_agent_without_committed_tables_fails_closed_even_with_local_discovery(champion, tmp_path):
    # The commit predates the attribution; the ignored discovery is still on disk.
    manifest = _envs(champion) / "dev" / "generated" / "governed_tables.json"
    manifest.write_text(json.dumps({"tables": ["paycat.s.t"]}, indent=2) + "\n")
    _commit(champion, "record without agent tables")
    assert (_envs(champion) / "dev" / "data_access" / "discovered_uc_tables.auto.tfvars").is_file()
    before = _baseline(champion)

    for target in (["promote-to", "ENV=prod", "CHECK=1"], ["check", "ENV=prod"]):
        result = _make(champion, tmp_path, *target, CI="true")
        out = result.stdout + result.stderr
        assert result.returncode != 0, out
        assert ("ERROR: no tables found for agent s1: envs/dev/generated/governed_tables.json has no "
                "agent_tables entry for it; run `make generate ENV=dev` and commit generated/") in out
    _assert_read_only(champion, before)


def test_promotion_itself_refuses_the_ignored_discovery_without_the_committed_entry(champion, tmp_path):
    # No scratch copy here: the ignored file is on disk where remap reads.
    dev = _envs(champion) / "dev"
    (dev / "generated" / "governed_tables.json").write_text(json.dumps({"tables": ["paycat.s.t"]}, indent=2) + "\n")
    _commit(champion, "record without agent tables")
    assert (dev / "data_access" / "discovered_uc_tables.auto.tfvars").is_file()
    refusal = ("ERROR: no tables found for agent s1: envs/dev/generated/governed_tables.json has no "
               "agent_tables entry for it; run `make generate ENV=dev` and commit generated/")
    dest = tmp_path / "remap-dest"
    dest.mkdir()

    direct = subprocess.run([sys.executable, str(SHARED / "scripts" / "remap_env_config.py"), str(dev), str(dest),
                             "paycat=ppay"], text=True, capture_output=True, env=_env(tmp_path))
    before = _baseline(champion)
    promote = _make(champion, tmp_path, "promote-to", "ENV=prod")

    assert direct.returncode != 0 and refusal in direct.stdout, direct.stdout + direct.stderr
    assert not (dest / "env.auto.tfvars").exists()
    assert promote.returncode != 0 and refusal in promote.stdout, promote.stdout + promote.stderr
    _assert_read_only(champion, before)


LOCAL_ONLY = {
    # git-ignored: make generate's discovery, with a table the commit never saw
    "ignored": ("dev/data_access/discovered_uc_tables.auto.tfvars",
                'discovered_uc_tables = ["paycat.s.extra"]\n'
                'discovered_table_agents = { "paycat.s.extra" = ["Payments"] }\n', "paycat.s.extra"),
    # untracked: an agent capture nobody has added yet
    "untracked": ("dev/agents/Stray.space.json", "not json", "Stray"),
}


@pytest.mark.parametrize("kind", sorted(LOCAL_ONLY))
def test_local_only_files_cannot_change_the_result(promoted, tmp_path, kind):
    _record(_envs(promoted) / "prod")
    _commit(promoted, "prod governed tables")
    rel, content, marker = LOCAL_ONLY[kind]
    path = _envs(promoted) / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    status = _git(promoted, "status", "--porcelain", "--ignored", str(path))
    assert status.startswith("!!" if kind == "ignored" else "??"), status
    before = _baseline(promoted)

    promote = _make(promoted, tmp_path, "promote-to", "ENV=prod", "CHECK=1", CI="true")
    check_prod = _make(promoted, tmp_path, "check", "ENV=prod", CI="true")
    check_dev = _make(promoted, tmp_path, "check", "ENV=dev", CI="true")

    assert promote.returncode == 0, promote.stdout + promote.stderr
    _assert_check_prod_passes_offline(check_prod)
    assert check_dev.returncode == 0, check_dev.stdout + check_dev.stderr
    for result in (promote, check_prod, check_dev):
        # check's non-failing NOTE may name an untracked file; its content still never reaches the result.
        assert marker not in (result.stdout + result.stderr).replace(f"aws/envs/{rel}", "")
    _assert_read_only(promoted, before)


@pytest.mark.parametrize("ci", ["", "true"])
def test_check_notes_modified_tracked_env_file_without_changing_exit(repo, tmp_path, ci):
    env_file = _envs(repo) / "dev" / "env.auto.tfvars"
    env_file.write_text(env_file.read_text() + "\n# local review\n")

    result = _make(repo, tmp_path, "check", "ENV=dev", **({"CI": ci} if ci else {}))

    assert result.returncode == 0, result.stdout + result.stderr
    assert "NOTE: make check passed against uncommitted working-tree content" in result.stdout
    assert "aws/envs/dev/env.auto.tfvars" in result.stdout
    assert "untracked files" not in result.stdout
    assert "rehearse and release-plan use the commit" in result.stdout


@pytest.mark.parametrize("ci", ["", "true"])
def test_check_notes_committable_untracked_env_file_without_changing_exit(repo, tmp_path, ci):
    (_envs(repo) / "dev" / "review.txt").write_text("review me\n")

    result = _make(repo, tmp_path, "check", "ENV=dev", **({"CI": ci} if ci else {}))

    assert result.returncode == 0, result.stdout + result.stderr
    assert "NOTE: untracked files in envs/dev were not checked" in result.stdout
    assert "aws/envs/dev/review.txt" in result.stdout
    assert "passed against uncommitted working-tree content" not in result.stdout
    assert "rehearse and release-plan use the commit" in result.stdout


@pytest.mark.parametrize("ci", ["", "true"])
def test_check_has_no_uncommitted_note_on_a_clean_tree(repo, tmp_path, ci):
    result = _make(repo, tmp_path, "check", "ENV=dev", **({"CI": ci} if ci else {}))

    assert result.returncode == 0, result.stdout + result.stderr
    assert "uncommitted" not in result.stdout and "untracked files" not in result.stdout


def test_uncommitted_note_is_silent_outside_git(tmp_path, capsys):
    cloud = tmp_path / "aws"
    (cloud / "envs" / "dev").mkdir(parents=True)
    (cloud / "envs" / "dev" / "local.tfvars").write_text("x = 1\n")

    check_env._note_uncommitted_env_files(cloud, "dev")

    assert capsys.readouterr().out == ""


def test_check_outside_a_git_checkout_is_refused(repo, tmp_path):
    shutil.rmtree(repo / ".git")
    result = _make(repo, tmp_path, "check", "ENV=dev")
    assert result.returncode != 0
    assert "cannot list the files git tracks under" in result.stderr
    assert "make check runs in a git checkout" in result.stderr


def test_check_of_untracked_envs_is_refused(repo, tmp_path):
    _git(repo, "rm", "-r", "-q", "--cached", "aws/envs")
    (repo / ".gitignore").write_text(GITIGNORE + "envs/\n")
    _commit(repo, "envs ignored, as in the GenieRails repo itself")
    result = _make(repo, tmp_path, "check", "ENV=dev")
    assert result.returncode != 0
    err = result.stderr
    assert "(is envs/ git-ignored?); make check reads only committed env files" in err
    assert "shared/examples/dev_to_prod/README.md" in err
    assert "Dev step 1 / Commit it" in err
    assert "git add .gitignore envs/" in err


def test_check_prod_runs_promote_check(promoted, tmp_path):
    edited = _envs(promoted) / "prod" / "generated" / "abac.auto.tfvars"
    edited.write_text(edited.read_text().replace("account_last4", "redact"))
    _commit(promoted, "hand edit prod")
    before = _baseline(promoted)

    result = _make(promoted, tmp_path, "check", "ENV=prod", CI="true")

    assert result.returncode != 0
    assert "envs/prod/generated/abac.auto.tfvars: differs from the regenerated file" in result.stderr
    assert "FAILED (promote-to ENV=prod CHECK=1)" in result.stderr
    _assert_read_only(promoted, before)



# ── symlinks: a scratch copy must never write through one ───────────────────

def _link_out(repo, tmp_path, env, rel):
    """Move envs/<env>/<rel> outside the repo and leave a symlink in its place."""
    inside = _envs(repo) / env / rel if rel else _envs(repo) / env
    outside = tmp_path / "external" / env / (rel or "env")
    outside.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(inside), str(outside))
    inside.symlink_to(outside, target_is_directory=outside.is_dir())
    _commit(repo, f"symlink {env}/{rel}")
    return outside


def _external(outside):
    if outside.is_dir():
        return _snapshot(outside)
    return outside.read_bytes()


@pytest.mark.parametrize("rel, shown", [
    ("", "envs/dev is a symlink"),
    ("generated", "envs/dev/generated is a symlink"),
    ("generated/governed_tables.json", "envs/dev/generated/governed_tables.json is a symlink"),
])
def test_check_refuses_a_symlink_in_the_env_and_leaves_its_target_alone(repo, tmp_path, rel, shown):
    outside = _link_out(repo, tmp_path, "dev", rel)
    external = _external(outside)
    before = _baseline(repo)

    result = _make(repo, tmp_path, "check", "ENV=dev", CI="true")

    assert result.returncode != 0
    assert shown in result.stderr
    assert "Replace the symlink with the file or directory it points to." in result.stderr
    assert "--- check 1/6" not in result.stdout
    assert _external(outside) == external
    _assert_read_only(repo, before)


@pytest.mark.parametrize("rel, shown", [
    ("", "envs/prod is a symlink"),
    ("generated", "envs/prod/generated is a symlink"),
    ("generated/abac.auto.tfvars", "envs/prod/generated/abac.auto.tfvars is a symlink"),
])
def test_promote_check_refuses_a_symlink_in_any_env(promoted, tmp_path, rel, shown):
    outside = _link_out(promoted, tmp_path, "prod", rel)
    external = _external(outside)
    before = _baseline(promoted)

    result = _make(promoted, tmp_path, "promote-to", "ENV=prod", "CHECK=1", CI="true")

    assert result.returncode != 0
    assert shown in result.stderr
    assert "Running promote-to" not in result.stdout
    assert _external(outside) == external
    _assert_read_only(promoted, before)


def test_check_allows_only_the_scaffolded_env_link(repo, tmp_path):
    link = _envs(repo) / "dev" / "data_access" / "env.auto.tfvars"
    assert link.is_symlink() and os.readlink(link) == "../env.auto.tfvars"
    link.unlink()
    link.symlink_to(_envs(repo) / "dev" / "env.auto.tfvars")  # same file, but an absolute link
    _commit(repo, "absolute link")

    result = _make(repo, tmp_path, "check", "ENV=dev")

    assert result.returncode != 0
    assert "envs/dev/data_access/env.auto.tfvars is a symlink" in result.stderr


@pytest.mark.parametrize("ci", ["", "true"])
def test_check_never_runs_the_staged_policy_controller_or_any_apply(repo, tmp_path, ci):
    """In a deterministic env, check's plan only previews the staged phases (step 6); nothing applies."""
    log = tmp_path / "tf_all.log"
    (tmp_path / "bin" / "terraform").write_text(
        FAKE_TERRAFORM.replace("#!/bin/sh\n", f'#!/bin/sh\necho "$(basename "$PWD") $*" >> "{log}"\n', 1))
    before = _baseline(repo)
    result = _make(repo, tmp_path, "check", "ENV=dev", **({"CI": ci} if ci else {}))
    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    # This fixture's values aren't versioned generations, so the preview says it can't plan the phases.
    assert "Staged policy apply: no phase preview" in out
    # The controller's run prints these (and takes the env lock); check never reaches it.
    assert "=== Policy staged apply" not in out and "=== Policy phase" not in out
    commands = log.read_text().splitlines()
    assert not [c for c in commands if c.split()[1:2] in (["apply"], ["import"], ["destroy"])], commands
    assert any(c.startswith("data_access plan") and "-var=policy_stage=preview" in c for c in commands), commands
    _assert_read_only(repo, before)


@pytest.mark.parametrize("secret, warns", [("filled-secret", False), ("", True)])
def test_check_secret_warnings_reflect_the_real_auth_file(repo, tmp_path, secret, warns):
    auth = _envs(repo) / "dev" / "auth.auto.tfvars"
    auth.write_text('databricks_account_id = "account"\n'
                    'databricks_client_id = "client"\n'
                    f'databricks_client_secret = "{secret}"\n'
                    'databricks_workspace_id = "workspace"\n'
                    'databricks_workspace_host = "https://workspace.example"\n')
    result = _make(repo, tmp_path, "check", "ENV=dev")
    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert ("'databricks_client_secret' is empty" in out) is warns


def test_check_removes_the_scratch_copy_when_copying_credentials_fails(repo, tmp_path, monkeypatch):
    secret = "scratch-secret-value"
    for env_name in ("dev", "account"):
        (_envs(repo) / env_name / "auth.auto.tfvars").write_text(f'databricks_client_secret = "{secret}"\n')
    scratch_tmp = tmp_path / "scratch-tmp"
    scratch_tmp.mkdir()
    monkeypatch.setattr(check_env.tempfile, "tempdir", str(scratch_tmp))
    copyfile = check_env.shutil.copyfile

    def failing_copyfile(source, dest, *args, **kwargs):
        # The env's auth file is already copied in when the account's fails.
        if Path(dest).parts[-2:] == ("account", "auth.auto.tfvars"):
            raise OSError("disk full")
        return copyfile(source, dest, *args, **kwargs)

    monkeypatch.setattr(check_env.shutil, "copyfile", failing_copyfile)
    args = check_env.argparse.Namespace(env="dev", cloud_dir=repo / "aws", makefile=repo / "aws" / "Makefile",
                                        make="make")

    with pytest.raises(OSError, match="disk full"):
        check_env._check(args)

    assert list(scratch_tmp.iterdir()) == []


# ── rehearse and release-plan run the same check first ──────────────────────
# The first step after the check is a live call (rehearse: the custom-mask live
# check; release-plan: rehearse-reuse, which takes the source env's lock). Both
# are stubbed to record and stop, so a test sees whether the check let the
# command reach them, and nothing else live can run.

FIRST_LIVE_STEP = {"rehearse": "custom_masks.py live-check", "release-plan": "release_gate.py rehearse-reuse"}


def _make_gated(repo, tmp_path, target, env):
    """make <target> ENV=<env>, first live step stubbed (the deployer SP's auth file is written first)."""
    from shared.tests import script_stubs

    log = tmp_path / "live-steps.log"
    recorder = tmp_path / "live-step"
    recorder.write_text(f'#!/bin/sh\necho "$0 $*" >> "{log}"\nexit 1\n')
    recorder.chmod(0o755)
    # The fixture's agent has no acl_groups: the local reader and group guards
    # (tested on their own) pass here so the command reaches the check.
    stub_bin = script_stubs.install(tmp_path / "stub-bin", {FIRST_LIVE_STEP[target]: str(recorder),
                                                           "table_readers.py check": "true",
                                                           "group_map.py check": "true"})
    environ = _env(tmp_path)
    environ["PATH"] = f"{stub_bin}:{environ['PATH']}"
    result = subprocess.run(["make", "--no-print-directory", "-C", str(repo / "aws"), target, f"ENV={env}"],
                            text=True, capture_output=True, env=environ, timeout=300)
    return result, (log.read_text() if log.exists() else "")


def _copy_tracked_shared(dest):
    """The engine as git tracks it (no local __pycache__, .terraform or other untracked files)."""
    listed = subprocess.run(["git", "-C", str(ROOT), "ls-files", "-z", "--", "shared"],
                            capture_output=True, check=True).stdout
    for rel in filter(None, listed.decode().split("\0")):
        source = ROOT / rel
        if source.is_file() and not source.is_symlink():
            target = dest / Path(rel).relative_to("shared")
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)


def _vendor_shared(repo):
    """Replace the fixture's shared/ symlink with a committed copy, as a customer checkout has."""
    shared = repo / "shared"
    if shared.is_symlink():
        shared.unlink()
        _copy_tracked_shared(shared)
        _commit(repo, "vendor shared/")


def _ready_to_apply(repo, env):
    """What rehearse/release-plan need of the checkout: its own committed shared/ and the deployer's auth."""
    _vendor_shared(repo)
    deployer = hcl2.load(open(_envs(repo) / env / "env.auto.tfvars")).get("deployer_principal_id", "")
    (_envs(repo) / env / "auth.auto.tfvars").write_text(f'databricks_client_id = "{deployer}"\n')


def _no_lock_or_receipt(repo):
    assert not list(_envs(repo).rglob(".governance.lock")), "a lock was taken"
    assert not list(repo.rglob("receipts")), "a receipt was written"


@pytest.mark.parametrize("target, env", [("rehearse", "dev"), ("release-plan", "prod")])
def test_a_broken_committed_config_is_refused_before_any_lock_write_or_live_call(promoted, tmp_path, target, env):
    # Valid config whose committed generated file the commit doesn't regenerate.
    edited = _envs(promoted) / env / "generated" / "abac.auto.tfvars"
    edited.write_text(edited.read_text().replace('"c"' if env == "dev" else "account_last4",
                                                 '"hand edited"' if env == "dev" else "redact", 1))
    _commit(promoted, "hand edit")
    _ready_to_apply(promoted, env)
    before = _baseline(promoted)

    result, live = _make_gated(promoted, tmp_path, target, env)

    assert result.returncode != 0
    # check's own message, then the command's refusal.
    assert ": differs from the regenerated file" in result.stderr
    assert f"=== make check ENV={env}: FAILED (" in result.stderr
    assert (f"{target}: refused: make check ENV={env} failed (above); nothing was locked, written or applied."
            in result.stderr)
    if env == "prod":
        assert "FAILED (promote-to ENV=prod CHECK=1)" in result.stderr
    assert live == "", "the command went past the check"
    assert "environment lock acquired" not in result.stdout
    assert not (tmp_path / "tf.log").exists()
    _no_lock_or_receipt(promoted)
    _assert_read_only(promoted, before)


def test_rehearse_on_a_valid_committed_config_passes_the_check_and_goes_on(repo, tmp_path):
    _ready_to_apply(repo, "dev")
    before = _baseline(repo)

    result, live = _make_gated(repo, tmp_path, "rehearse", "dev")

    out = result.stdout + result.stderr
    assert "=== make check ENV=dev: passed ===" in out, out
    assert "refused: make check" not in out
    # The check's structural plan ran; then rehearse went on to its first live step.
    assert (tmp_path / "tf.log").read_text().split("\n")[:2] == ["plan data_access structural-check",
                                                                  "plan workspace expected-withholding"]
    assert "--env dev" in live, out
    _no_lock_or_receipt(repo)
    _assert_read_only(repo, before)


def test_release_plan_check_runs_after_the_workspace_env_and_mode_guards(repo, tmp_path):
    # A legacy env is refused with release-plan's own message, before any check.
    (_envs(repo) / "dev" / "env.auto.tfvars").write_text(DEV_ENV.replace("deterministic", "legacy"))
    _commit(repo, "legacy")

    result, live = _make_gated(repo, tmp_path, "release-plan", "dev")

    assert result.returncode != 0
    assert "is not in deterministic governance mode" in result.stderr
    assert "make check ENV=dev" not in result.stdout + result.stderr
    assert live == ""


def _broken_head_clean_worktree(repo, env):
    """Codex's repro: commit a hand edit (HEAD is broken), then restore the file uncommitted."""
    rel = "dev/data_access/abac.auto.tfvars" if env == "dev" else "prod/generated/abac.auto.tfvars"
    path = _envs(repo) / rel
    good = path.read_text()
    path.write_text(good.replace('"c"', '"hand edited"', 1) if env == "dev" else good.replace("account_last4", "redact"))
    _commit(repo, "hand edit")
    path.write_text(good)
    return rel


@pytest.mark.parametrize("target, env", [("rehearse", "dev"), ("release-plan", "prod")])
def test_a_broken_commit_under_a_restored_working_tree_is_refused_before_anything(promoted, tmp_path, target, env):
    _vendor_shared(promoted)  # first: vendoring commits everything, the restored file included
    rel = _broken_head_clean_worktree(promoted, env)
    _ready_to_apply(promoted, env)
    before = _baseline(promoted)

    result, live = _make_gated(promoted, tmp_path, target, env)

    assert result.returncode != 0
    assert f"make {target} refused: the checkout is not clean:" in result.stderr
    assert f"aws/envs/{rel}" in result.stderr
    # Refused before the check, any live step, lock, receipt or terraform.
    assert "=== make check" not in result.stdout + result.stderr
    assert live == "" and "environment lock acquired" not in result.stdout
    assert not (tmp_path / "tf.log").exists()
    _no_lock_or_receipt(promoted)
    _assert_read_only(promoted, before)


def _check_env(repo, env, *flags):
    return subprocess.run([sys.executable, str(SHARED / "scripts" / "check_env.py"), *flags, "check", "--env", env,
                           "--cloud-dir", str(repo / "aws"), "--makefile", str(repo / "aws" / "Makefile")],
                          text=True, capture_output=True, env=_env(repo.parent), timeout=300)


@pytest.mark.parametrize("env", ["dev", "prod"])
def test_the_embedded_check_reads_head_not_the_working_tree(promoted, env):
    rel = _broken_head_clean_worktree(promoted, env)
    before = _baseline(promoted)

    committed = _check_env(promoted, env, "--committed")
    working = _check_env(promoted, env)

    assert committed.returncode != 0, committed.stdout + committed.stderr
    assert ": differs from the regenerated file" in committed.stderr
    assert "uncommitted working-tree content" not in committed.stdout
    # Standalone make check keeps checking the tracked working tree, with #238's note.
    assert "differs from the regenerated file" not in working.stderr
    assert f"NOTE: make check passed against uncommitted working-tree content in envs/{env}:" in working.stdout \
        or "FAILED (plan)" in working.stderr, working.stdout + working.stderr
    assert rel.split("/", 1)[1] in working.stdout
    _assert_read_only(promoted, before)


def test_ignored_runtime_files_never_make_the_checkout_dirty(repo, tmp_path):
    _ready_to_apply(repo, "dev")
    (_envs(repo) / "dev" / "data_access").mkdir(exist_ok=True)
    (_envs(repo) / "dev" / "data_access" / "terraform.tfstate").write_text("{}")
    assert _git(repo, "status", "--porcelain") == ""

    result, live = _make_gated(repo, tmp_path, "rehearse", "dev")

    assert "not clean" not in result.stderr
    assert "=== make check ENV=dev: passed ===" in result.stdout + result.stderr
    assert "--env dev" in live


def _external_shared(repo, tmp_path, *, git_tree=True):
    """repo/shared as a committed symlink to an engine copy outside the checkout."""
    outside = tmp_path / "engine" / "shared"
    _copy_tracked_shared(outside)
    if git_tree:
        _git(outside.parent, "init", "-q")
        _git(outside.parent, "add", "-A")
        _git(outside.parent, "commit", "-q", "-m", "engine")
    (repo / "shared").unlink()
    (repo / "shared").symlink_to(outside)
    _commit(repo, "point shared/ at the engine")
    return outside


EXTERNAL = "shared/ resolves outside this checkout"
COMMITTED_HERE = "need the engine committed as regular files in this repo so receipts can fingerprint it"


@pytest.mark.parametrize("state", ["clean", "untracked", "modified", "not-a-git-tree"])
def test_an_external_shared_refuses_rehearse_before_anything(repo, tmp_path, state):
    # The code fingerprint hashes only this checkout's tracked files, so a shared/ that resolves
    # elsewhere can't be bound by a receipt, clean (at any revision) or not. "untracked" is the
    # r2 repro.
    outside = _external_shared(repo, tmp_path, git_tree=state != "not-a-git-tree")
    if state == "untracked":
        (outside / "scripts" / "planted.py").write_text("x = 1\n")
    elif state == "modified":
        (outside / "scripts" / "check_env.py").write_text((outside / "scripts" / "check_env.py").read_text()
                                                          + "\n# edited\n")
    deployer = hcl2.load(open(_envs(repo) / "dev" / "env.auto.tfvars"))["deployer_principal_id"]
    (_envs(repo) / "dev" / "auth.auto.tfvars").write_text(f'databricks_client_id = "{deployer}"\n')
    before = _baseline(repo)

    result, live = _make_gated(repo, tmp_path, "rehearse", "dev")

    assert result.returncode != 0
    assert "make rehearse refused: the checkout is not clean:" in result.stderr
    assert f"{EXTERNAL} ({outside.resolve()}); deterministic rehearse/release-plan {COMMITTED_HERE}" in result.stderr
    assert "=== make check" not in result.stdout + result.stderr
    assert live == "" and "environment lock acquired" not in result.stdout
    assert not (tmp_path / "tf.log").exists()
    _no_lock_or_receipt(repo)
    _assert_read_only(repo, before)


@pytest.mark.parametrize("state", ["clean", "untracked", "not-a-git-tree"])
def test_receipts_and_provenance_use_the_same_engine_test_as_clean_check(repo, tmp_path, state):
    from shared.scripts import release_gate as rg

    outside = _external_shared(repo, tmp_path, git_tree=state != "not-a-git-tree")
    if state == "untracked":
        (outside / "planted.txt").write_text("x\n")
    status = rg.checkout_status(_envs(repo) / "dev")
    assert status["clean_tree"] is False, status
    assert any(EXTERNAL in line and COMMITTED_HERE in line for line in status["dirty"]), status
    with pytest.raises(rg.Refused, match=COMMITTED_HERE):
        rg.clean_check(_envs(repo) / "dev", "dev", "make rehearse")

# What a normal clone's .gitignore ignores under shared/ (the root .gitignore): caches only.
CACHE_IGNORES = "__pycache__/\n*.pyc\n.pytest_cache/\n.terraform/\n.terraform.lock.hcl\n.DS_Store\n"


def _ignore(repo, patterns):
    (repo / ".gitignore").write_text((repo / ".gitignore").read_text() + patterns)
    _commit(repo, "ignore")


def _plant(repo, rel):
    path = repo / "shared" / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x\n")


def test_a_normal_clone_with_caches_is_clean(repo):
    from shared.scripts import release_gate as rg

    _vendor_shared(repo)
    _ignore(repo, CACHE_IGNORES)
    for rel in ("scripts/__pycache__/check_env.cpython-311.pyc", "tests/__pycache__/x.cpython-311.pyc",
                "roots/data_access/.terraform/providers/p", "roots/data_access/.terraform.lock.hcl",
                ".pytest_cache/v/cache/nodeids", "scripts/.DS_Store"):
        _plant(repo, rel)
    assert _git(repo, "status", "--porcelain") == ""
    status = rg.checkout_status(_envs(repo) / "dev")
    assert status["clean_tree"] is True, status


@pytest.mark.parametrize("planted, pattern", [
    ("countries/ZZ.yaml", "ZZ.yaml\n"),             # r4 repro: load_country_overlays(["ZZ"]) reads it
    ("industries/zz.yaml", "zz.yaml\n"),
    ("scripts/.planted.json", ""),                   # the fixture's .*.json
    ("scripts/stray.pyc", ""),                       # a bare .pyc beside the sources is importable
    ("scripts/account-admin.aws.env", "*.env\n"),    # any suffix: deny by default
])
def test_any_other_ignored_file_under_shared_refuses(repo, planted, pattern):
    from shared.scripts import release_gate as rg

    _vendor_shared(repo)
    _ignore(repo, CACHE_IGNORES + pattern)
    _plant(repo, planted)
    assert _git(repo, "status", "--porcelain") == "", "the planted file must be git-ignored"
    status = rg.checkout_status(_envs(repo) / "dev")
    assert status["clean_tree"] is False, status
    assert (f"ignored file under shared/ (not in the code fingerprint; commit or delete it): shared/{planted}"
            in status["dirty"]), status


def _submodule_shared(repo, tmp_path, *, nested_only=False):
    """shared/ as a committed submodule (gitlink), or a nested repo under shared/scripts."""
    engine = tmp_path / "engine-repo"
    _copy_tracked_shared(engine)
    _git(engine, "init", "-q")
    _git(engine, "add", "-A")
    _git(engine, "commit", "-q", "-m", "engine")
    if nested_only:
        _vendor_shared(repo)
        nested = repo / "shared" / "scripts" / "vendored"
        _git(repo, "-c", "protocol.file.allow=always", "submodule", "add", "-q", str(engine), str(nested.relative_to(repo)))
    else:
        (repo / "shared").unlink()
        _git(repo, "rm", "-q", "--cached", "shared")
        _git(repo, "-c", "protocol.file.allow=always", "submodule", "add", "-q", str(engine), "shared")
    _commit(repo, "engine as a submodule")


@pytest.mark.parametrize("where", ["shared", "under-shared"])
def test_an_engine_submodule_refuses_rehearse_before_anything(repo, tmp_path, where):
    _submodule_shared(repo, tmp_path, nested_only=where == "under-shared")
    staged = _git(repo, "ls-files", "-s", "--", "shared")
    assert any(line.startswith("160000 ") for line in staged.splitlines()), staged
    deployer = hcl2.load(open(_envs(repo) / "dev" / "env.auto.tfvars"))["deployer_principal_id"]
    (_envs(repo) / "dev" / "auth.auto.tfvars").write_text(f'databricks_client_id = "{deployer}"\n')
    before = _baseline(repo)

    result, live = _make_gated(repo, tmp_path, "rehearse", "dev")

    assert result.returncode != 0
    assert "make rehearse refused: the checkout is not clean:" in result.stderr
    assert f"holds a git submodule or nested repository" in result.stderr
    assert COMMITTED_HERE in result.stderr
    assert "=== make check" not in result.stdout + result.stderr
    assert live == "" and "environment lock acquired" not in result.stdout
    assert not (tmp_path / "tf.log").exists()
    _no_lock_or_receipt(repo)
    _assert_read_only(repo, before)


def test_an_ignored_country_overlay_refuses_rehearse_before_anything(repo, tmp_path):
    _vendor_shared(repo)
    _ignore(repo, CACHE_IGNORES + "ZZ.yaml\n")
    _plant(repo, "countries/ZZ.yaml")
    deployer = hcl2.load(open(_envs(repo) / "dev" / "env.auto.tfvars"))["deployer_principal_id"]
    (_envs(repo) / "dev" / "auth.auto.tfvars").write_text(f'databricks_client_id = "{deployer}"\n')
    before = _baseline(repo)

    result, live = _make_gated(repo, tmp_path, "rehearse", "dev")

    assert result.returncode != 0
    assert "ignored file under shared/ (not in the code fingerprint; commit or delete it): shared/countries/ZZ.yaml" \
        in result.stderr
    assert "=== make check" not in result.stdout + result.stderr
    assert live == "" and "environment lock acquired" not in result.stdout
    assert not (tmp_path / "tf.log").exists()
    _no_lock_or_receipt(repo)
    _assert_read_only(repo, before)
