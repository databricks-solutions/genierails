"""Rollout step 14 (design 18.1): personas and the files each owns.

- Agent settings live only in agents/ files and every reader merges them
  (generate, the ACL sidecar, promote), so a persona-layout env promotes.
- promote-to keeps the split: an agent-only change produces a diff only in
  the target's agents/ files, which the sample CODEOWNERS gives to data
  citizens alone; a governance change touches governance files.
- Customer class mappings are the env's tag_treatments, never shared/:
  scaffold-treatments writes them, the model applies them, promotion carries them.
- The legacy layout (agents inline in env.auto.tfvars) keeps working
  unchanged, and moves only through the explicit make split-agent-files,
  which gives Terraform exactly the same agents.

Terraform is a stub on PATH, or (the console tests) the real roots with no
provider call; nothing reaches Databricks.
"""

from __future__ import annotations

import fnmatch
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

import hcl2
import pytest

import agent_files
import generate_abac
from agent_files import AgentFileError
from deterministic_governance import validate_config
from governance_policies import DeterministicGovernanceError, build_model, column_types_from_ddl
from scripts import agent_ownership as ownership, coverage_fix, promotion_txn
from scripts import scaffold_treatments as scaffold
from tests import test_deterministic_check_promote as base
from tests.terraform_helpers import TIMEOUT, shared_copy, skip_if_providers_unavailable
from tests.test_check_target import FAKE_TERRAFORM, GITIGNORE, _git

ROOT = base.ROOT
SHARED = base.SHARED
BASE_COMMIT = "16c9797a5030e80a8fe0c79eb60c29ad12218407"  # origin/main before step 14
CODEOWNERS = SHARED / "examples" / "CODEOWNERS.example"
CLOUDS = ("aws", "azure")
DATA_CITIZENS, GOVERNANCE = "@your-org/data-citizens", "@your-org/governance-engineers"
PLATFORM = "@your-org/platform"

DEV_ENV = f'''governance_mode = "deterministic"
deployer_principal_id = "{base.DEPLOYER}"
access_tier_groups = {json.dumps(base.TIERS)}
table_readers = {{ "paycat.s.t" = {json.dumps(base.TIERS)} }}
'''
PROD_ENV = (f'governance_mode = "deterministic"\npromote_from = "dev"\ncatalog_map = {{ "paycat" = "ppay" }}\n'
            f'deployer_principal_id = "{base.DEPLOYER}"\n')
AGENT = f'genie_space_id = "{base.DEV_SPACE_ID}"\nacl_groups = {json.dumps(base.TIERS)}\n'
PAYLOAD = {
    "version": 2,
    "data_sources": {"tables": [{"identifier": "paycat.s.t"}]},
    "instructions": {"text_instructions": [{"id": "01f0ti0000000001", "content": ["Use paycat.s.t for payments."]}]},
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _codeowners_rules() -> list[tuple[re.Pattern, list[str]]]:
    """The sample's rules as (regex, owners), with GitHub's gitignore-style matching."""
    rules = []
    for line in CODEOWNERS.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        pattern, *owners = line.split()
        anchored = pattern.startswith("/") or "/" in pattern.rstrip("/")
        body = pattern.lstrip("/")
        directory = body.endswith("/")
        body = body.rstrip("/")
        regex = ""
        for part in re.split(r"(\*\*/|\*)", body):
            regex += {"**/": "(?:.*/)?", "*": "[^/]*"}.get(part, re.escape(part))
        regex = ("" if anchored else "(?:.*/)?") + regex + ("/.*" if directory else "(?:/.*)?")
        rules.append((re.compile(regex + r"\Z"), owners))
    return rules


def owners(path: str) -> list[str]:
    """CODEOWNERS owners of a repo path: the last matching rule wins."""
    found: list[str] = []
    for regex, rule_owners in _codeowners_rules():
        if regex.match(path):
            found = rule_owners
    return found


def _changed(repo: Path) -> list[str]:
    status = _git(repo, "status", "--porcelain", "--untracked-files=all")
    return sorted(line[3:] for line in status.splitlines() if line.strip())


def _agent_repo(tmp_path: Path, monkeypatch) -> Path:
    """A committed persona-layout dev env: governance in env.auto.tfvars, the agent in agents/."""
    monkeypatch.setattr(base, "DEV_ENV", DEV_ENV)
    repo = tmp_path / "repo"
    dev = base._envs(repo) / "dev"
    (dev / "agents").mkdir(parents=True)
    (base._envs(repo) / "account").mkdir()
    shutil.copy2(ROOT / "aws" / "Makefile", repo / "aws" / "Makefile")
    (repo / "shared").symlink_to(SHARED)
    (dev / "env.auto.tfvars").write_text(DEV_ENV)
    (dev / "agents" / "Payments.auto.tfvars").write_text(AGENT)
    (dev / "agents" / "Payments.space.json").write_text(json.dumps(PAYLOAD, indent=2) + "\n")
    (dev / "auth.auto.tfvars").write_text(base._auth(base.DEPLOYER))
    (base._envs(repo) / "account" / "abac.auto.tfvars").write_text(base.LEGACY_ACCOUNT)
    base._write_generated(dev)
    (repo / ".gitignore").write_text(GITIGNORE)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "python3").write_text(f"#!/bin/sh\nexec {sys.executable} \"$@\"\n")
    (bin_dir / "python3").chmod(0o755)
    (bin_dir / "terraform").write_text(FAKE_TERRAFORM)
    (bin_dir / "terraform").chmod(0o755)
    result = base._make(repo, tmp_path, "promote", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    _record(dev)
    _git(repo, "init", "-q")
    base._commit(repo, "dev")
    return repo


def _record(env_dir: Path) -> None:
    subprocess.run([sys.executable, str(SHARED / "scripts" / "sticky_governance.py"), "record",
                    "--env-dir", str(env_dir), "--config", str(env_dir / "generated" / "abac.auto.tfvars")],
                   check=True)


def _first_promotion(repo: Path, tmp_path: Path) -> subprocess.CompletedProcess:
    prod = base._envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text(PROD_ENV)
    (prod / "auth.auto.tfvars").write_text(base._auth(base.DEPLOYER, host=""))
    result = base._make(repo, tmp_path, "promote-to", "ENV=prod")
    assert result.returncode == 0, result.stdout + result.stderr
    _record(prod)
    base._commit(repo, "first promotion")
    return result


@pytest.fixture
def promoted(tmp_path, monkeypatch):
    repo = _agent_repo(tmp_path, monkeypatch)
    _first_promotion(repo, tmp_path)
    return repo


# ── Every reader merges the agent files ─────────────────────────────────────

def test_generate_and_the_acl_sidecar_read_agent_files(tmp_path):
    env = tmp_path / "envs" / "dev"
    (env / "agents").mkdir(parents=True)
    (env / "env.auto.tfvars").write_text(DEV_ENV)
    (env / "agents" / "Payments.auto.tfvars").write_text(AGENT)
    (env / "agents" / "Payments.space.json").write_text(json.dumps(PAYLOAD))
    (env / "auth.auto.tfvars").write_text(base._auth(base.DEPLOYER))

    cfg = generate_abac.load_auth_config(env / "auth.auto.tfvars")
    assert [(s["name"], s["acl_groups"], s["uc_tables"]) for s in cfg["genie_spaces"]] == [
        ("Payments", base.TIERS, ["paycat.s.t"])]
    assert agent_files.captured_names(env) == {"Payments"}

    generated = env / "generated" / "abac.auto.tfvars"
    generated.parent.mkdir()
    generated.write_text('genie_space_configs = { "Payments" = { title = "Payments" } }\n'
                         'fgac_policies = []\ngroups = {}\n')
    assert generate_abac.autofix_acl_groups(generated, env / "env.auto.tfvars", reject_draft_acls=True) == 1
    sidecar = hcl2.loads((env / "generated" / "genie_space_derived_acl_groups.auto.tfvars").read_text())
    assert sidecar["genie_space_derived_acl_groups"] == {"Payments": base.TIERS}


def test_a_persona_layout_env_promotes_its_agents_as_files(promoted):
    prod = base._envs(promoted) / "prod"
    prod_env = hcl2.loads((prod / "env.auto.tfvars").read_text())
    assert prod_env["genie_spaces"] == [], "no agent setting is written to the governance file"
    assert agent_files.load(prod, "Payments")["acl_groups"] == base.TIERS
    payload = json.loads((prod / "agents" / "Payments.space.json").read_text())
    assert payload["data_sources"]["tables"] == [{"identifier": "ppay.s.t"}]
    assert base.DEV_SPACE_ID not in (prod / "agents" / "Payments.auto.tfvars").read_text()


# ── promote-to keeps the split ──────────────────────────────────────────────

@pytest.mark.parametrize("ci", ["", "true"])
def test_an_agent_only_change_promotes_as_a_diff_in_agent_files_only(promoted, tmp_path, ci):
    dev = base._envs(promoted) / "dev"
    curated = json.loads(json.dumps(PAYLOAD))
    curated["instructions"]["text_instructions"][0]["content"] = ["Fiscal year starts in February."]
    curated["config"] = {"sample_questions": [{"id": "01f0sq0000000001", "question": ["Refunds by month?"]}]}
    (dev / "agents" / "Payments.space.json").write_text(json.dumps(curated, indent=2) + "\n")
    base._commit(promoted, "data citizen: curate Payments in dev, make capture")

    result = base._make(promoted, tmp_path, "promote-to", "ENV=prod")

    assert result.returncode == 0, result.stdout + result.stderr
    changed = _changed(promoted)
    assert changed == ["aws/envs/prod/agents/Payments.space.json"], changed
    assert {owner for path in changed for owner in owners(path)} == {DATA_CITIZENS}
    assert "agent files (data citizens):          envs/prod/agents/Payments.space.json" in result.stdout
    assert "governance files (governance owners): none" in result.stdout
    assert "Agent-only promotion: it needs no governance approval." in result.stdout
    prod_payload = json.loads((base._envs(promoted) / "prod" / "agents" / "Payments.space.json").read_text())
    assert prod_payload["instructions"]["text_instructions"][0]["content"] == ["Fiscal year starts in February."]

    base._commit(promoted, "promotion PR")
    before = base._state(promoted)
    check = base._make(promoted, tmp_path, "promote-to", "ENV=prod", "CHECK=1", **({"CI": ci} if ci else {}))
    assert check.returncode == 0, check.stdout + check.stderr
    assert "OK: promote-to ENV=prod CHECK=1: committed files match." in check.stdout
    assert base._state(promoted) == before


def test_a_governance_change_promotes_into_governance_files(promoted, tmp_path):
    dev = base._envs(promoted) / "dev"
    env_file = dev / "env.auto.tfvars"
    env_file.write_text(env_file.read_text() + 'treatment_versions = { email_partial = { partial = "redacted" } }\n')
    model = build_model(base.CLASSES, column_types_from_ddl(base.DDL), hcl2.loads(env_file.read_text()),
                        deployer_principal=base.DEPLOYER)
    generate_abac.write_deterministic_model(dev / "generated" / "abac.auto.tfvars",
                                            dev / "generated" / "masking_functions.sql", model)
    assert base._make(promoted, tmp_path, "promote", "ENV=dev").returncode == 0
    _record(dev)
    base._commit(promoted, "governance engineer: email partial is redacted")

    result = base._make(promoted, tmp_path, "promote-to", "ENV=prod")

    assert result.returncode == 0, result.stdout + result.stderr
    changed = [p for p in _changed(promoted) if promotion_txn.owner(p.split("/", 3)[3]) != promotion_txn.LAYER]
    assert "aws/envs/prod/env.auto.tfvars" in changed and "aws/envs/prod/generated/abac.auto.tfvars" in changed
    assert not [p for p in changed if "/agents/" in p], "a governance change rewrites no agent file"
    assert {owner for path in changed for owner in owners(path)} == {GOVERNANCE}
    assert "It changes governance files: a governance owner approves the PR." in result.stdout


def test_promote_refuses_agents_a_legacy_target_still_keeps_inline(promoted, tmp_path):
    prod_env = base._envs(promoted) / "prod" / "env.auto.tfvars"
    prod_env.write_text(prod_env.read_text().replace(
        "genie_spaces = [\n]", 'genie_spaces = [\n  { name = "Payments", genie_space_id = "", '
        'uc_tables = ["ppay.s.t"], acl_groups = ["prod_only"] },\n]'))
    shutil.rmtree(base._envs(promoted) / "prod" / "agents")
    base._commit(promoted, "prod still in the legacy layout")
    before = base._state(promoted)

    result = base._make(promoted, tmp_path, "promote-to", "ENV=prod")

    assert result.returncode != 0
    assert "Run make split-agent-files ENV=prod" in result.stderr, result.stdout + result.stderr
    assert base._state(promoted) == before, "prod's own acl_groups are never replaced silently"



# ── An agent owns its table list; governance sees every change to it ────────
# generated/governed_tables.json (governance-owned) lists each file-defined
# agent's tables, so a table an agent adds, even one with no classified
# column, is a governance diff; make check fails while the record is stale.

PLAIN = "paycat.s.plain"  # no classified column, so generate writes no tag for it


def _add_table(env_dir: Path, table: str) -> None:
    """The data citizen adds a table to the agent in the dev UI, then make capture."""
    path = env_dir / "agents" / "Payments.space.json"
    payload = json.loads(path.read_text())
    payload["data_sources"]["tables"].append({"identifier": table})
    path.write_text(json.dumps(payload, indent=2) + "\n")


def _governed(env_dir: Path) -> dict:
    return json.loads((env_dir / "generated" / "governed_tables.json").read_text())


def test_an_agent_adding_a_table_with_no_classified_column_changes_governed_tables_json(promoted, tmp_path):
    dev = base._envs(promoted) / "dev"
    assert _governed(dev) == {"tables": ["paycat.s.t"], "agent_tables": {"Payments": ["paycat.s.t"]}}
    _add_table(dev, PLAIN)

    _record(dev)  # what make generate and make capture run after writing

    assert _governed(dev) == {"tables": ["paycat.s.t"], "agent_tables": {"Payments": [PLAIN, "paycat.s.t"]}}
    assert _changed(promoted) == ["aws/envs/dev/agents/Payments.space.json",
                                  "aws/envs/dev/generated/governed_tables.json"]
    assert owners("aws/envs/dev/generated/governed_tables.json") == [GOVERNANCE]
    dry = subprocess.run(["make", "-n", "--no-print-directory", "-C", str(promoted / "aws"), "capture",
                          "ENV=dev", "SPACE=Payments"], capture_output=True, text=True)
    assert "sticky_governance.py\" record --env-dir" in dry.stdout, dry.stdout + dry.stderr


@pytest.mark.parametrize("ci", ["", "true"])
def test_check_fails_while_governed_tables_json_is_stale(promoted, tmp_path, ci):
    dev = base._envs(promoted) / "dev"
    _add_table(dev, PLAIN)
    base._commit(promoted, "agent adds a table; governed_tables.json not regenerated")
    before = base._state(promoted)

    stale = base._make(promoted, tmp_path, "check", "ENV=dev", **({"CI": ci} if ci else {}))

    assert stale.returncode != 0, stale.stdout + stale.stderr
    assert "generated/governed_tables.json" in stale.stderr, stale.stdout + stale.stderr
    assert base._state(promoted) == before
    _record(dev)
    base._commit(promoted, "regenerated")
    current = base._make(promoted, tmp_path, "check", "ENV=dev", **({"CI": ci} if ci else {}))
    assert current.returncode == 0, current.stdout + current.stderr


@pytest.mark.parametrize("ci", ["", "true"])
def test_check_prod_passes_on_what_an_agent_file_promotion_writes(tmp_path, monkeypatch, ci):
    """promote-to records prod's governed tables itself; make check expects exactly that."""
    repo = _agent_repo(tmp_path, monkeypatch)
    prod = base._envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text(PROD_ENV)
    (prod / "auth.auto.tfvars").write_text(base._auth(base.DEPLOYER, host=""))
    result = base._make(repo, tmp_path, "promote-to", "ENV=prod")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _governed(prod)["agent_tables"] == {"Payments": ["ppay.s.t"]}
    base._commit(repo, "first promotion")

    check = base._make(repo, tmp_path, "check", "ENV=prod", **({"CI": ci} if ci else {}))

    out = check.stdout + check.stderr
    assert "OK: generated files (envs/prod): committed files match." in out, out
    # The plan (6) needs prod's live classification config.
    assert check.returncode == 0 or "=== make check ENV=prod: FAILED (plan) ===" in out, out


def test_an_agent_only_change_adding_a_table_changes_only_agent_and_governance_owned_generated_files(
        promoted, tmp_path):
    dev = base._envs(promoted) / "dev"
    _add_table(dev, PLAIN)
    _record(dev)
    assert _changed(promoted) == ["aws/envs/dev/agents/Payments.space.json",
                                  "aws/envs/dev/generated/governed_tables.json"]
    base._commit(promoted, "data citizen: Payments reads paycat.s.plain")

    result = base._make(promoted, tmp_path, "promote-to", "ENV=prod")

    assert result.returncode == 0, result.stdout + result.stderr
    changed = _changed(promoted)
    assert changed == ["aws/envs/prod/agents/Payments.space.json",
                       "aws/envs/prod/generated/governed_tables.json"], changed
    assert _governed(base._envs(promoted) / "prod")["agent_tables"] == {"Payments": ["ppay.s.plain", "ppay.s.t"]}
    assert owners(changed[1]) == [GOVERNANCE], "the governance-owned record routes the PR to governance"
    assert "It changes governance files: a governance owner approves the PR." in result.stdout
    base._commit(promoted, "promotion PR")
    check = base._make(promoted, tmp_path, "promote-to", "ENV=prod", "CHECK=1")
    assert check.returncode == 0, check.stdout + check.stderr


def test_an_agent_only_change_adding_a_table_grants_no_select_and_no_can_run(promoted, tmp_path):
    dev = base._envs(promoted) / "dev"
    _add_table(dev, PLAIN)
    _record(dev)

    from table_readers import access_problems, describe
    problems = access_problems(dev)
    assert problems and all(PLAIN in problem for problem in problems), problems
    assert {group for group in base.TIERS if any(group in problem for problem in problems)} == set(base.TIERS)
    assert f"  {PLAIN}: (none)" in describe(dev), "no table_readers entry covers it, so no SELECT"
    refused = subprocess.run([sys.executable, str(SHARED / "table_readers.py"), "check", "--env-dir", str(dev),
                              "--command-name", "make release"], capture_output=True, text=True)
    assert refused.returncode == 1 and "CAN_RUN would be granted" in refused.stderr, refused.stderr
    assert "table_readers" not in _governed(dev), "the record grants nothing"

def test_the_promotion_report_classifies_every_env_path():
    assert promotion_txn.owner("agents/Sales.auto.tfvars") == promotion_txn.AGENT
    assert promotion_txn.owner("agents/Sales.space.json") == promotion_txn.AGENT
    for path in ("env.auto.tfvars", "ack.txt", "generated/abac.auto.tfvars", "generated/governed_tables.json",
                 "generated/expected_classification.json", "generated/masking_functions.sql"):
        assert promotion_txn.owner(path) == promotion_txn.GOVERNANCE, path
    for path in ("abac.auto.tfvars", "data_access/abac.auto.tfvars",
                 "generated/genie_space_derived_acl_groups.auto.tfvars"):
        assert promotion_txn.owner(path) == promotion_txn.LAYER, path
    for path in (".genie_space_id_sales", "generated/.runtime/abac.auto.tfvars", "data_access/.coverage_gate.json"):
        assert promotion_txn.owner(path) is None, path


def test_the_sample_codeowners_splits_data_citizens_from_governance_engineers():
    for path in ("aws/envs/dev/agents/Payments.auto.tfvars", "aws/envs/prod/agents/Payments.space.json",
                 "azure/envs/prod/agents/Ops.auto.tfvars"):
        assert owners(path) == [DATA_CITIZENS], path
    for path in ("aws/envs/prod/env.auto.tfvars", "aws/envs/prod/generated/abac.auto.tfvars",
                 "aws/envs/prod/ack.txt", "azure/envs/dev/generated/governed_tables.json",
                 "aws/envs/prod/abac.auto.tfvars", "shared/mask_library.json"):
        assert owners(path) == [GOVERNANCE], path
    for path in ("aws/.github/workflows/pr-check.yml", "azure/.github/workflows/pr-check.yml",
                 ".github/workflows/ci.yml", ".github/CODEOWNERS",
                 "aws/azure-pipelines/pr-check.yml", "azure/azure-pipelines/pr-check.yml"):
        assert owners(path) == [PLATFORM], path


def _codeowners_patterns() -> tuple[list[str], list[str]]:
    """The sample's active rule patterns, and those of its commented example rules."""
    active, examples = [], []
    for line in CODEOWNERS.read_text().splitlines():
        if line.startswith("#"):
            example = re.match(r"#\s*(\S+)\s+@", line)
            if example:
                examples.append(example.group(1))
        elif line.strip():
            active.append(line.split()[0])
    return active, examples


def test_every_sample_codeowners_rule_is_anchored_to_a_supported_cloud_tree():
    active, examples = _codeowners_patterns()
    assert examples, "the sample's optional per-agent example is gone"
    for pattern in active + examples:
        assert pattern.startswith("/") and "**" not in pattern, pattern
        assert pattern.split("/")[1] in ("shared", ".github", *CLOUDS), pattern
    assert "/.github/" in active
    for cloud in CLOUDS:
        for pattern in (f"/{cloud}/envs/", f"/{cloud}/envs/*/agents/", f"/{cloud}/envs/*/ack.txt",
                        f"/{cloud}/.github/", f"/{cloud}/azure-pipelines/"):
            assert pattern in active, pattern
        assert f"/{cloud}/envs/*/agents/Sales.*" in examples, cloud
    # A directory that only happens to be called envs/ is not an env.
    for path in ("tools/envs/dev/agents/Payments.auto.tfvars", "aws/sub/envs/dev/agents/Payments.auto.tfvars",
                 "shared/tests/fixtures/envs/dev/agents/Payments.auto.tfvars"):
        assert owners(path) != [DATA_CITIZENS], path
    assert owners("shared/tests/fixtures/envs/dev/agents/Payments.auto.tfvars") == [GOVERNANCE]


# ── Customer class mappings: the env's tag_treatments ───────────────────────

LOYALTY = {"paycat.s.t.loyalty": ["class.loyalty_id"], "paycat.s.t.email": ["class.email_address"]}
LOYALTY_TYPES = {"paycat.s.t.loyalty": "STRING", "paycat.s.t.email": "STRING"}


def test_tag_treatments_map_a_customer_class_to_a_library_treatment():
    unmapped = build_model(LOYALTY, LOYALTY_TYPES, {"access_tier_groups": base.TIERS})
    assert unmapped.unmapped == [("paycat.s.t.loyalty", "loyalty_id")]
    assert unmapped.columns["paycat.s.t.loyalty"].value.startswith("unmapped_redact__")

    mapped = build_model(LOYALTY, LOYALTY_TYPES, {"access_tier_groups": base.TIERS,
                                                  "tag_treatments": {"class.loyalty_id": "identifier"}})
    assert mapped.unmapped == []
    assert mapped.columns["paycat.s.t.loyalty"].value.startswith("identifier__string__")
    assert mapped.columns["paycat.s.t.email"] == unmapped.columns["paycat.s.t.email"]


@pytest.mark.parametrize("mapping, message", [
    ({"sensitivity=confidential": "redact"}, "tags other than class.* are not read"),
    ({"class.email_address": "redact"}, "the mask library already maps class.email_address"),
    ({"class.loyalty_id": "mask_loyalty"}, "is not a mask-library treatment"),
    ({"class.Loyalty ID": "redact"}, "keys must be class.<name>"),
])
def test_tag_treatments_refuse_what_would_mask_nothing_or_remap_the_library(mapping, message):
    cfg = {"governance_mode": "deterministic", "deployer_principal_id": base.DEPLOYER,
           "access_tier_groups": base.TIERS, "tag_treatments": mapping}
    assert any(message in error for error in validate_config(cfg)), validate_config(cfg)
    with pytest.raises(DeterministicGovernanceError, match=re.escape(message)):
        build_model(LOYALTY, LOYALTY_TYPES, cfg)


def _scaffold_env(tmp_path: Path, name: str, env_text: str, marker: str = "") -> Path:
    env = tmp_path / "envs" / name
    (env / "generated").mkdir(parents=True)
    (env / "env.auto.tfvars").write_text(env_text)
    (env / "generated" / "abac.auto.tfvars").write_text(
        (f"# gr.classification_unmapped: {marker}|class.loyalty_id\n" if marker else "")
        + "tag_assignments = []\nfgac_policies = []\n")
    (env / "generated" / "masking_functions.sql").write_text("-- sql\n")
    return env


def _run_scaffold(env: Path, tmp_path: Path, treatment: str) -> subprocess.CompletedProcess:
    config = tmp_path / "treatment_config.json"
    if not config.exists():
        shutil.copy2(SHARED / "treatment_config.json", config)
    return subprocess.run(
        [sys.executable, str(SHARED / "scripts" / "scaffold_treatments.py"),
         "--tfvars", str(env / "generated" / "abac.auto.tfvars"),
         "--sql", str(env / "generated" / "masking_functions.sql"),
         "--treatment-config", str(config), "--env-file", str(env / "env.auto.tfvars"),
         "--env-name", env.name, *(["--treatment", treatment] if treatment else [])],
        text=True, capture_output=True, cwd=SHARED, timeout=120)


def test_scaffold_treatments_writes_the_envs_tag_treatments_never_shared(tmp_path):
    dev = _scaffold_env(tmp_path, "dev", DEV_ENV + "\n# reviewed\ncountry = \"\"\n", marker="paycat.s.t.loyalty")
    before = hcl2.loads((dev / "env.auto.tfvars").read_text())

    result = _run_scaffold(dev, tmp_path, "identifier")

    assert result.returncode == 0, result.stdout + result.stderr
    after = hcl2.loads((dev / "env.auto.tfvars").read_text())
    assert after["tag_treatments"] == {"class.loyalty_id": "identifier"}
    assert {k: v for k, v in after.items() if k != "tag_treatments"} == before
    assert "# reviewed" in (dev / "env.auto.tfvars").read_text()
    assert (tmp_path / "treatment_config.json").read_bytes() == (SHARED / "treatment_config.json").read_bytes()
    assert "MAPPED class.loyalty_id -> identifier" in result.stdout
    assert "envs/dev/env.auto.tfvars tag_treatments (a governance file; shared/ is unchanged)" in result.stdout
    assert "Next: commit envs/dev/env.auto.tfvars, make generate ENV=dev, make check ENV=dev, " \
        "make rehearse ENV=dev; commit and push after each step that changes envs/" in result.stdout
    model = build_model(LOYALTY, LOYALTY_TYPES, after)
    assert model.unmapped == [] and model.columns["paycat.s.t.loyalty"].value.startswith("identifier__")

    again = _run_scaffold(dev, tmp_path, "identifier")
    assert again.returncode == 0 and "UNCHANGED class.loyalty_id is already mapped to identifier" in again.stdout
    assert hcl2.loads((dev / "env.auto.tfvars").read_text()) == after


def test_scaffold_in_a_promoted_env_writes_the_env_it_is_promoted_from(tmp_path):
    dev = _scaffold_env(tmp_path, "dev", DEV_ENV)
    prod = _scaffold_env(tmp_path, "prod", PROD_ENV, marker="ppay.s.t.loyalty")
    prod_before = (prod / "env.auto.tfvars").read_bytes()

    result = _run_scaffold(prod, tmp_path, "redact")

    assert result.returncode == 0, result.stdout + result.stderr
    assert hcl2.loads((dev / "env.auto.tfvars").read_text())["tag_treatments"] == {"class.loyalty_id": "redact"}
    assert (prod / "env.auto.tfvars").read_bytes() == prod_before, "promote-to owns prod's copy"
    assert "promote-to carries tag_treatments from envs/dev to prod." in result.stdout
    # release-plan needs the promotion on origin/main and a dev rehearsal of that same commit.
    after = result.stdout[result.stdout.index("make promote-to ENV=prod"):]
    steps = ["commit and push envs/prod/", "make rehearse ENV=dev on that commit", "make release-plan ENV=prod",
             "make release ENV=prod"]
    assert [after.index(step) for step in steps] == sorted(after.index(step) for step in steps)


@pytest.mark.parametrize("treatment, message", [
    ("", "deterministic envs map a new class to a mask-library treatment"),
    ("mask_loyalty", "TREATMENT=mask_loyalty is not a mask-library treatment"),
])
def test_scaffold_in_a_deterministic_env_never_writes_stubs_into_shared(tmp_path, treatment, message):
    dev = _scaffold_env(tmp_path, "dev", DEV_ENV, marker="paycat.s.t.loyalty")
    files = {p: p.read_bytes() for p in [*dev.rglob("*"), tmp_path / "treatment_config.json"] if p.is_file()}
    shutil.copy2(SHARED / "treatment_config.json", tmp_path / "treatment_config.json")
    files[tmp_path / "treatment_config.json"] = (tmp_path / "treatment_config.json").read_bytes()

    result = _run_scaffold(dev, tmp_path, treatment)

    assert result.returncode == 1 and message in result.stderr, result.stdout + result.stderr
    assert {p: p.read_bytes() for p in files} == files


def test_promotion_carries_tag_treatments_into_prods_own_derivation(promoted, tmp_path):
    dev = base._envs(promoted) / "dev"
    env_file = dev / "env.auto.tfvars"
    env_file.write_text(env_file.read_text() + 'tag_treatments = { "class.loyalty_id" = "identifier" }\n')
    base._commit(promoted, "map a customer class in dev")

    result = base._make(promoted, tmp_path, "promote-to", "ENV=prod")

    assert result.returncode == 0, result.stdout + result.stderr
    prod_settings = hcl2.loads((base._envs(promoted) / "prod" / "env.auto.tfvars").read_text())
    assert prod_settings["tag_treatments"] == {"class.loyalty_id": "identifier"}
    model = build_model({"ppay.s.t.loyalty": ["class.loyalty_id"]}, {"ppay.s.t.loyalty": "STRING"}, prod_settings,
                        deployer_principal=base.DEPLOYER)
    assert model.unmapped == [] and model.columns["ppay.s.t.loyalty"].value.startswith("identifier__")


def test_the_coverage_gap_fix_points_deterministic_envs_at_tag_treatments(tmp_path):
    _scaffold_env(tmp_path, "dev", DEV_ENV)
    prod = _scaffold_env(tmp_path, "prod", PROD_ENV)
    text = "\n".join(coverage_fix.fix_lines("prod", prod, [("ppay.s.t.loyalty", "class.loyalty_id")]))
    assert "envs/dev/env.auto.tfvars; shared/ is product code" in text
    assert "make scaffold-treatments ENV=prod TREATMENT=<mask-library treatment>" in text
    assert "make promote-to ENV=prod" in text
    # The promotion is a new commit: push it and rehearse dev on it before release-plan.
    after_promote = text[text.index("make promote-to ENV=prod"):]
    assert after_promote.index("commit and push envs/prod/") < after_promote.index("make rehearse ENV=dev")
    assert "shared/treatment_config.json" not in text


def test_the_missing_mask_fix_in_a_deterministic_env_maps_or_adopts_never_materializes(tmp_path):
    _scaffold_env(tmp_path, "dev", DEV_ENV)
    prod = _scaffold_env(tmp_path, "prod", PROD_ENV)
    text = "\n".join(coverage_fix.materialize_lines("prod", prod, ["generic_partial"]))
    assert "make materialize-treatment" not in text
    assert "make scaffold-treatments ENV=prod TREATMENT=<mask-library treatment>" in text
    assert "make adopt-custom-mask ENV=dev TREATMENT=custom_<name>" in text
    assert "tag_treatments (or column_overrides) in envs/dev/env.auto.tfvars" in text
    after_promote = text[text.index("make promote-to ENV=prod"):]
    assert after_promote.index("commit and push envs/prod/") < after_promote.index("make rehearse ENV=dev")


@pytest.mark.parametrize("env", ["dev", "prod"])
def test_materialize_refuses_a_deterministic_env_and_writes_nothing(tmp_path, capsys, env):
    from scripts import materialize_treatment

    dirs = {"dev": _scaffold_env(tmp_path, "dev", DEV_ENV), "prod": _scaffold_env(tmp_path, "prod", PROD_ENV)}
    before = {p: p.read_bytes() for p in (tmp_path / "envs").rglob("*") if p.is_file()}
    with pytest.raises(ValueError) as refused:
        materialize_treatment.source_env_dir(tmp_path / "envs", env, dirs[env])
    message = str(refused.value)
    assert f"materialize-treatment is legacy-only and {env} is deterministic; nothing was written" in message
    assert "make materialize-treatment" not in message
    assert "make adopt-custom-mask ENV=dev TREATMENT=custom_<name>" in message
    shutil.copy2(SHARED / "treatment_config.json", tmp_path / "treatment_config.json")
    for extra in (["--envs-dir", str(tmp_path / "envs")], []):  # make's call, and a direct one
        assert materialize_treatment.main([
            "--tfvars", str(dirs[env] / "generated" / "abac.auto.tfvars"),
            "--sql", str(dirs[env] / "generated" / "masking_functions.sql"),
            "--treatment-config", str(tmp_path / "treatment_config.json"), "--treatment", "redact",
            "--env-dir", str(dirs[env]), "--env-name", env, *extra]) == 1
        assert "is deterministic; nothing was written" in capsys.readouterr().err
    assert {p: p.read_bytes() for p in (tmp_path / "envs").rglob("*") if p.is_file()} == before
    assert materialize_treatment.release_steps(tmp_path / "legacy", "dev", "prod") == "make release ENV=prod"


# ── The legacy layout: unchanged, or moved by make split-agent-files ────────

LEGACY_DEV = f'''governance_mode = "deterministic"
deployer_principal_id = "{base.DEPLOYER}"
access_tier_groups = {json.dumps(base.TIERS)}

# Two agents, not in name order.
genie_spaces = [
  {{ name = "Payments", genie_space_id = "01ef00000000000a", uc_tables = ["paycat.s.t"],
     acl_groups = {json.dumps(base.TIERS)}, sql_warehouse_id = "wh1" }},
  {{ name = "Card Ops", genie_space_id = "01ef00000000000b", uc_tables = ["paycat.s.cards"],
     acl_groups = [] }},
]
uc_tables = ["paycat.s.t", "paycat.s.cards"]
table_readers = {{ "paycat.s.t" = {json.dumps(base.TIERS)} }}
'''


def _legacy_env(tmp_path: Path, text: str = LEGACY_DEV, name: str = "dev") -> Path:
    env = tmp_path / "envs" / name
    env.mkdir(parents=True)
    (env / "env.auto.tfvars").write_text(text)
    return env


def test_split_agent_files_moves_every_inline_agent_with_the_same_terraform_input(tmp_path):
    env = _legacy_env(tmp_path)
    before = agent_files.merged_config(env)

    assert agent_files.split(env) == ["Payments", "Card Ops"]

    settings = hcl2.loads((env / "env.auto.tfvars").read_text())
    assert settings["genie_spaces"] == []
    assert {k: v for k, v in settings.items() if k != "genie_spaces"} == {
        k: v for k, v in hcl2.loads(LEGACY_DEV).items() if k != "genie_spaces"}
    assert agent_files.names(env) == ["Card Ops", "Payments"]
    assert agent_files.load(env, "Card Ops")["acl_groups"] == []
    assert sorted(map(agent_files._as_terraform, agent_files.merged_config(env)["genie_spaces"]),
                  key=lambda s: s["name"]) == sorted(map(agent_files._as_terraform, before["genie_spaces"]),
                                                     key=lambda s: s["name"])
    assert agent_files.split(env) == [], "re-running moves nothing"


@pytest.mark.parametrize("entry, message", [
    ('{ name = "Payments", genie_space_id = "1", declared_footprint = ["c.s.t"] }', "sets declared_footprint"),
    ('{ name = "Payments", genie_space_id = "1", delete = true }', "has delete = true"),
])
def test_split_agent_files_refuses_any_entry_that_would_change_the_plan(tmp_path, entry, message):
    env = _legacy_env(tmp_path, f"genie_spaces = [{entry}]\n")
    before = {p: p.read_bytes() for p in env.rglob("*") if p.is_file()}
    with pytest.raises(AgentFileError, match=re.escape(message)):
        agent_files.split(env)
    assert {p: p.read_bytes() for p in env.rglob("*") if p.is_file()} == before
    assert not (env / "agents").exists()


@pytest.mark.parametrize("entry, expected_file, expected_key, display_name", [
    ('{ genie_space_id = "01ef0c" }', "01ef0c", "01ef0c", ""),
    ('{ name = "GenieRails Dev-to-Prod Walkthrough (louis_genierails_prod.dev_to_prod_demo)", '
     'genie_space_id = "01ef0d" }',
     "genierails_dev_to_prod_walkthrough_louis_genierails_prod_dev_to_prod_demo",
     "genierails_dev_to_prod_walkthrough_louis_genierails_prod_dev_to_prod_demo",
     "GenieRails Dev-to-Prod Walkthrough (louis_genierails_prod.dev_to_prod_demo)"),
])
def test_split_agent_files_preserves_address_and_display_for_legacy_names(
        tmp_path, entry, expected_file, expected_key, display_name):
    env = _legacy_env(tmp_path, f"genie_spaces = [{entry}]\n")
    assert agent_files.split(env) == [expected_file]
    agent = agent_files.load(env, expected_file)
    assert agent["key"] == expected_key
    assert agent["display_name"] == display_name
    assert agent_files.merged_config(env)["genie_spaces"][0]["name"] == display_name


def test_split_agent_files_accepts_explicit_key_to_filename_mapping(tmp_path):
    env = _legacy_env(tmp_path, 'genie_spaces = [{ genie_space_id = "01ef0c" }]\n')
    assert agent_files.split(env, "01ef0c=walkthrough") == ["walkthrough"]
    agent = agent_files.load(env, "walkthrough")
    assert agent["key"] == "01ef0c"
    assert agent["display_name"] == ""


@pytest.mark.parametrize("entries, expected", [
    # Two titles that sanitize alike: the root keeps the first plain, the second --<ID>.
    ('{ name = "A b", genie_space_id = "01ef01" }, { name = "a-b", genie_space_id = "01ef02" }',
     {"A b": "a_b", "a-b": "a_b--01ef02"}),
    # Not created yet (no ID): the second is --<list index>.
    ('{ name = "A b" }, { name = "a-b" }', {"A b": "a_b", "a-b": "a_b--1"}),
])
def test_split_agent_files_keeps_the_disambiguated_key_of_titles_that_sanitize_alike(tmp_path, entries, expected):
    env = _legacy_env(tmp_path, f"genie_spaces = [{entries}]\n")
    assert agent_files.terraform_keys(hcl2.loads((env / "env.auto.tfvars").read_text())["genie_spaces"]) == \
        list(expected.values())
    agent_files.split(env)
    assert {a["name"]: a["key"] for a in agent_files.load_all(env)} == expected


def test_split_agent_files_keeps_promoted_duplicate_titles_apart(tmp_path):
    env = _legacy_env(tmp_path, PROD_ENV + 'genie_spaces = [{ name = "A b" }, { name = "a-b" }]\n', name="prod")
    agent_files.split(env)
    assert {a["name"]: a["key"] for a in agent_files.load_all(env)} == {"A b": "a_b", "a-b": "a_b--1"}
    assert [s["key"] for s in agent_files.merged_config(env)["genie_spaces"]] == ["a_b", "a_b--1"]


@pytest.mark.parametrize("entry", ['{ genie_space_id = "AB/CD" }', '{ genie_space_id = "-01ef" }'])
def test_split_agent_files_refuses_a_key_no_agent_file_can_hold(tmp_path, entry):
    env = _legacy_env(tmp_path, f"genie_spaces = [{entry}]\n")
    before = {p: p.read_bytes() for p in env.rglob("*") if p.is_file()}
    with pytest.raises(AgentFileError, match="keep it inline"):
        agent_files.split(env)
    assert {p: p.read_bytes() for p in env.rglob("*") if p.is_file()} == before
    assert not (env / "agents").exists()


def test_split_agent_files_self_check_compares_the_terraform_key(tmp_path, monkeypatch):
    env = _legacy_env(tmp_path, 'genie_spaces = [{ name = "A b", genie_space_id = "1" }, { name = "a-b" }]\n')
    before = {p: p.read_bytes() for p in env.rglob("*") if p.is_file()}
    render = agent_files._render_agent
    monkeypatch.setattr(agent_files, "_render_agent",
                        lambda space, key: render(space, key.replace("--1", "--2")))
    with pytest.raises(AgentFileError, match="would not match the inline ones"):
        agent_files.split(env)
    assert {p: p.read_bytes() for p in env.rglob("*") if p.is_file()} == before
    assert not (env / "agents").exists()


@pytest.mark.parametrize("ids, expected", [
    (("01ef0a", "01ef0b"), {"Card Ops": "card_ops", "card-ops": "card_ops--01ef0b"}),
    (("", ""), {"Card Ops": "card_ops", "card-ops": "card_ops--1"}),
])
def test_an_already_split_env_with_file_names_that_sanitize_alike_still_loads(tmp_path, ids, expected):
    env = _legacy_env(tmp_path, "")
    (env / "agents").mkdir()
    for name, space_id in zip(expected, ids):
        (env / "agents" / f"{name}.auto.tfvars").write_text(f'genie_space_id = "{space_id}"\nacl_groups = []\n')
    assert {a["name"]: a["key"] for a in agent_files.load_all(env)} == expected


def test_an_agent_files_key_comes_from_its_file_name_never_its_title(tmp_path):
    env = _legacy_env(tmp_path, "")
    (env / "agents").mkdir()
    (env / "agents" / "sales.auto.tfvars").write_text('name = "Sales Analytics"\nacl_groups = []\n')
    agent = agent_files.load(env, "sales")
    assert (agent["key"], agent["display_name"]) == ("sales", "Sales Analytics")
    assert agent_files.merged_config(env)["genie_spaces"][0] == {
        "key": "sales", "name": "Sales Analytics", "genie_space_id": "", "sql_warehouse_id": "",
        "uc_tables": [], "acl_groups": [], "detached": False}


@pytest.mark.parametrize("key", ["a/b", " ", "", "-x", "a b", 7])
def test_an_agent_files_explicit_key_must_be_one_terraform_can_use(tmp_path, key):
    env = _legacy_env(tmp_path, "")
    (env / "agents").mkdir()
    (env / "agents" / "x.auto.tfvars").write_text(f"key = {json.dumps(key)}\nacl_groups = []\n")
    with pytest.raises(AgentFileError, match="key"):
        agent_files.load_all(env)


def test_two_agent_files_with_one_explicit_key_are_refused(tmp_path):
    env = _legacy_env(tmp_path, 'genie_spaces = [{ name = "Ops" }]\n')
    (env / "agents").mkdir()
    (env / "agents" / "x.auto.tfvars").write_text('key = "ops"\nacl_groups = []\n')
    with pytest.raises(AgentFileError, match="share Terraform key 'ops'"):
        agent_files.load_all(env)


def test_split_agent_files_keeps_a_promoted_envs_agent_ids_out_of_git(tmp_path):
    env = _legacy_env(tmp_path, PROD_ENV + 'genie_spaces = [{ name = "Payments", genie_space_id = "prod-1" }]\n',
                      name="prod")
    with pytest.raises(AgentFileError, match="never in git; move it to .genie_space_id_payments"):
        agent_files.split(env)


def test_make_split_agent_files_is_explicit_and_reports_the_move(tmp_path):
    cloud = tmp_path / "aws"
    env = cloud / "envs" / "dev"
    env.mkdir(parents=True)
    (env / "env.auto.tfvars").write_text(LEGACY_DEV)
    shutil.copy2(ROOT / "aws" / "Makefile", cloud / "Makefile")
    (tmp_path / "shared").symlink_to(SHARED)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "python3").write_text(f"#!/bin/sh\nexec {sys.executable} \"$@\"\n")
    (bin_dir / "python3").chmod(0o755)

    environ = base._env(tmp_path)
    environ["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    result = subprocess.run(["make", "--no-print-directory", "-C", str(cloud), "split-agent-files", "ENV=dev"],
                            text=True, capture_output=True, env=environ, timeout=120)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "moved 'Payments' -> envs/dev/agents/Payments.auto.tfvars" in result.stdout
    assert "make plan ENV=dev shows no change" in result.stdout
    assert agent_files.names(env) == ["Card Ops", "Payments"]


def _make_split(tmp_path: Path, text: str, *args: str, **environ: str) -> tuple:
    cloud = tmp_path / "aws"
    env = cloud / "envs" / "dev"
    env.mkdir(parents=True)
    (env / "env.auto.tfvars").write_text(text)
    shutil.copy2(ROOT / "aws" / "Makefile", cloud / "Makefile")
    (tmp_path / "shared").symlink_to(SHARED)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "python3").write_text(f"#!/bin/sh\nexec {sys.executable} \"$@\"\n")
    (bin_dir / "python3").chmod(0o755)
    process_env = base._env(tmp_path, **environ)
    process_env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)
    before = {p.relative_to(env): p.read_bytes() for p in env.rglob("*") if p.is_file()}
    result = subprocess.run(["make", "--no-print-directory", "-C", str(cloud), "split-agent-files", "ENV=dev", *args],
                            text=True, capture_output=True, env=process_env, timeout=120)
    return env, before, result


def test_make_split_agent_files_names_files_with_agent_file_names(tmp_path):
    env, _, result = _make_split(tmp_path, 'genie_spaces = [{ genie_space_id = "01ef0c" }]\n',
                                 "AGENT_FILE_NAMES=01ef0c=walkthrough", KEY="01ef0c=from-the-shell")
    assert result.returncode == 0, result.stdout + result.stderr
    assert agent_files.names(env) == ["walkthrough"]
    assert agent_files.load(env, "walkthrough")["key"] == "01ef0c"


@pytest.mark.parametrize("mapping, message", [
    ("01ef0c", "must be existing-terraform-key=agent-file-name"),
    ("nope=walkthrough", "unknown Terraform key(s): nope"),
    ("01ef0c=../x", "is not allowed"),
])
def test_make_split_agent_files_refuses_a_bad_mapping_writing_nothing(tmp_path, mapping, message):
    env, before, result = _make_split(tmp_path, 'genie_spaces = [{ genie_space_id = "01ef0c" }]\n',
                                      f"AGENT_FILE_NAMES={mapping}")
    assert result.returncode != 0 and message in result.stderr, result.stdout + result.stderr
    guard = Path("generated") / ".governance.guard"  # make's own runtime marker
    assert {p.relative_to(env): p.read_bytes() for p in env.rglob("*")
            if p.is_file() and p.relative_to(env) != guard} == before


def test_make_split_agent_files_refuses_ci_without_writing(tmp_path):
    cloud = tmp_path / "aws"
    env = cloud / "envs" / "dev"
    env.mkdir(parents=True)
    (env / "env.auto.tfvars").write_text(LEGACY_DEV)
    shutil.copy2(ROOT / "aws" / "Makefile", cloud / "Makefile")
    (tmp_path / "shared").symlink_to(SHARED)
    before = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    environ = base._env(tmp_path, CI="true")
    result = subprocess.run(["make", "--no-print-directory", "-C", str(cloud), "split-agent-files", "ENV=dev"],
                            text=True, capture_output=True, env=environ, timeout=120)
    assert result.returncode != 0
    assert "cannot run in CI" in result.stderr
    assert {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before


# Terraform's view: the roots' own locals, evaluated by terraform console
# through terraform_layer.sh (which passes the agent var-file exactly as plan
# does). Every Genie resource is keyed off local.merged_spaces; every grant,
# classification and attribution off the data_access table locals.
WORKSPACE_EXPR = "jsonencode(local.merged_spaces)"
# data_access never reads an entry's key (the workspace root's), so it is left out.
DATA_ACCESS_EXPR = ("jsonencode({ tables = sort(local.full_effective_uc_tables), "
                    "agents = { for t, a in local.table_agents : t => sort(a) }, "
                    "spaces = sort([for s in var.genie_spaces : jsonencode({ for k, v in s : k => v if k != \"key\" })]) })")


@pytest.fixture(scope="module")
def console_shared(tmp_path_factory):
    copy = shared_copy(tmp_path_factory.mktemp("shared"))
    for layer in ("workspace", "data_access"):
        scratch = tmp_path_factory.mktemp(f"init-{layer}")
        init = subprocess.run([str(copy / "scripts" / "terraform_layer.sh"), layer, "test", "print-cmd", "plan"],
                              env={**os.environ, "LAYER_ENV_DIR": str(scratch), "TF_IN_AUTOMATION": "1"},
                              text=True, capture_output=True, timeout=TIMEOUT)
        skip_if_providers_unavailable(init)
        shutil.copytree(scratch / ".terraform", copy / f".tf-{layer}")
    return copy


def _console(shared: Path, env: Path, layer: str, expr: str) -> object:
    layer_dir = env if layer == "workspace" else env / "data_access"
    result = subprocess.run(
        [str(shared / "scripts" / "terraform_layer.sh"), layer, env.name, "console"],
        input=expr + "\n", text=True, capture_output=True, timeout=TIMEOUT,
        env={**os.environ, "LAYER_ENV_DIR": str(layer_dir), "TF_DATA_DIR": str(shared / f".tf-{layer}"),
             "TF_IN_AUTOMATION": "1", "ENVS_DIR": str(env.parent)})
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(json.loads(result.stdout.strip().splitlines()[-1]))


def _console_env(tmp_path: Path, text: str) -> Path:
    env = tmp_path / "envs" / "dev"
    (env / "data_access").mkdir(parents=True)
    (env / "env.auto.tfvars").write_text(text)
    auth = ('databricks_account_id = "a"\ndatabricks_client_id = "sp"\ndatabricks_client_secret = "s"\n'
            'databricks_workspace_id = "1"\ndatabricks_workspace_host = "https://example.invalid"\n')
    (env / "auth.auto.tfvars").write_text(auth)
    (env / "data_access" / "auth.auto.tfvars").write_text(auth)
    (env / "abac.auto.tfvars").write_text('groups = { dev_ops = {}, dev_analysts = {} }\n')
    (env / "data_access" / "abac.auto.tfvars").write_text('groups = { dev_ops = {}, dev_analysts = {} }\n')
    (env / "data_access" / "env.auto.tfvars").symlink_to("../env.auto.tfvars")  # as make setup links it
    return env


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_after_split_agent_files_terraform_sees_exactly_the_same_agents(console_shared, tmp_path):
    env = _console_env(tmp_path, LEGACY_DEV)
    inline = {layer: _console(console_shared, env, layer, expr)
              for layer, expr in (("workspace", WORKSPACE_EXPR), ("data_access", DATA_ACCESS_EXPR))}
    assert set(inline["workspace"]) == {"payments", "card_ops"}

    agent_files.split(env)

    assert agent_files.names(env) and hcl2.loads((env / "env.auto.tfvars").read_text())["genie_spaces"] == []
    for layer, expr in (("workspace", WORKSPACE_EXPR), ("data_access", DATA_ACCESS_EXPR)):
        assert _console(console_shared, env, layer, expr) == inline[layer], layer


# The real workspace root (roots/workspace merged_spaces, which keys every Genie
# resource and .genie_space_id_<key>): splitting keeps every key and value.
REAL_ROOT_SPLITS = {
    "unnamed": ('{ genie_space_id = "01ef0c" }', "", {"01ef0c"}),
    "unnamed, file named": ('{ genie_space_id = "01ef0c" }', "01ef0c=walkthrough", {"01ef0c"}),
    "disallowed title": (
        '{ name = "GenieRails Dev-to-Prod Walkthrough (louis_genierails_prod.dev_to_prod_demo)", '
        'genie_space_id = "01ef0d" }', "",
        {"genierails_dev_to_prod_walkthrough_louis_genierails_prod_dev_to_prod_demo"}),
    "titles that sanitize alike": ('{ name = "A b", genie_space_id = "01ef01" }, '
                                   '{ name = "a-b", genie_space_id = "01ef02" }', "", {"a_b", "a_b--01ef02"}),
    "titles that sanitize alike, not created": ('{ name = "A b" }, { name = "a-b" }', "", {"a_b", "a_b--1"}),
}


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
@pytest.mark.parametrize("case", list(REAL_ROOT_SPLITS))
def test_the_real_workspace_root_keys_every_split_agent_as_before(console_shared, tmp_path, case):
    entries, file_names, keys = REAL_ROOT_SPLITS[case]
    env = _console_env(tmp_path, f"genie_spaces = [{entries}]\n")
    inline = _console(console_shared, env, "workspace", WORKSPACE_EXPR)
    assert set(inline) == keys

    agent_files.split(env, file_names)

    assert _console(console_shared, env, "workspace", WORKSPACE_EXPR) == inline
    # Every script reads and writes the ID file Terraform's create resource uses.
    assert {ownership.id_file(env, name).name for name in agent_files.names(env)} == {
        f".genie_space_id_{key}" for key in keys}


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_retitling_a_split_agent_keeps_its_terraform_key(console_shared, tmp_path):
    env = _console_env(tmp_path, 'genie_spaces = [{ name = "A b" }, { name = "a-b" }]\n')
    agent_files.split(env)
    settings = env / "agents" / "a-b.auto.tfvars"
    settings.write_text(settings.read_text().replace('name           = "a-b"', 'name           = "Card Ops"'))

    spaces = _console(console_shared, env, "workspace", WORKSPACE_EXPR)

    assert set(spaces) == {"a_b", "a_b--1"} and spaces["a_b--1"]["name"] == "Card Ops"


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_the_real_workspace_root_still_evaluates_the_legacy_single_agent(console_shared, tmp_path):
    env = _console_env(tmp_path, 'genie_space_title = "Sales Space"\ngenie_space_id = "01ef0f"\n')
    spaces = _console(console_shared, env, "workspace", WORKSPACE_EXPR)
    assert set(spaces) == {"sales_space"} and spaces["sales_space"]["genie_space_id"] == "01ef0f"


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_the_real_workspace_root_keys_alike_file_names_as_the_scripts_do(console_shared, tmp_path):
    env = _console_env(tmp_path, "")
    (env / "agents").mkdir()
    (env / "agents" / "Card Ops.auto.tfvars").write_text('genie_space_id = "01ef0a"\nacl_groups = []\n')
    (env / "agents" / "card-ops.auto.tfvars").write_text('genie_space_id = "01ef0b"\nacl_groups = []\n')
    spaces = _console(console_shared, env, "workspace", WORKSPACE_EXPR)
    assert set(spaces) == {a["key"] for a in agent_files.load_all(env)} == {"card_ops", "card_ops--01ef0b"}


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_terraform_plans_no_select_for_a_table_an_agent_file_adds(console_shared, tmp_path):
    env = _console_env(tmp_path, 'governance_mode = "deterministic"\n'
                                 'access_tier_groups = ["dev_ops", "dev_analysts"]\n'
                                 'table_readers = { "cat.sch.orders" = ["dev_ops"] }\n')
    (env / "agents").mkdir()
    agent = env / "agents" / "Payments.auto.tfvars"
    agent.write_text('genie_space_id = "01f0aa0000000001"\nacl_groups = ["dev_ops"]\nuc_tables = ["cat.sch.orders"]\n')
    expr = "jsonencode(module.data_access.access_config.grant_keys)"
    assert _console(console_shared, env, "data_access", expr) == ["cat.sch.orders|dev_ops"]

    agent.write_text(agent.read_text().replace('["cat.sch.orders"]', '["cat.sch.orders", "cat.sch.plain"]'))

    assert _console(console_shared, env, "data_access", expr) == ["cat.sch.orders|dev_ops"]
    readers = _console(console_shared, env, "data_access", "jsonencode(module.data_access.access_config.readers)")
    assert readers == {"cat.sch.orders": ["dev_ops"], "cat.sch.plain": []}, "governed, but nobody reads it"


# The legacy layout is untouched: promote-to with step 14 writes the very
# bytes the code before it wrote, so every Terraform input, and the plan, is the same.

def _base_tree(tmp_path: Path) -> Path:
    present = subprocess.run(["git", "cat-file", "-e", f"{BASE_COMMIT}^{{commit}}"], cwd=ROOT, capture_output=True)
    if present.returncode != 0:
        if os.environ.get("REQUIRE_TERRAFORM_TESTS") == "1":
            pytest.fail(f"{BASE_COMMIT} is not in this clone; fetch full history")
        pytest.skip(f"{BASE_COMMIT} is not in this clone (shallow checkout?)")
    archive = subprocess.run(["git", "archive", "--format=tar", BASE_COMMIT, "shared", "aws/Makefile"],
                             cwd=ROOT, capture_output=True, check=True)
    tree = tmp_path / "base"
    with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as tar:
        tar.extractall(tree, filter="data")
    return tree


def _legacy_promotion(tmp_path: Path, shared: Path, makefile: Path) -> dict[str, bytes]:
    repo = tmp_path / "repo"
    dev = base._envs(repo) / "dev"
    dev.mkdir(parents=True)
    (base._envs(repo) / "account").mkdir()
    shutil.copy2(makefile, repo / "aws" / "Makefile")
    (repo / "shared").symlink_to(shared)
    (dev / "env.auto.tfvars").write_text(base.DEV_ENV)
    (dev / "auth.auto.tfvars").write_text(base._auth(base.DEPLOYER))
    (base._envs(repo) / "account" / "abac.auto.tfvars").write_text(base.LEGACY_ACCOUNT)
    base._write_generated(dev)
    (repo / ".gitignore").write_text(GITIGNORE)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    (bin_dir / "python3").write_text(f"#!/bin/sh\nexec {sys.executable} \"$@\"\n")
    (bin_dir / "python3").chmod(0o755)
    (bin_dir / "terraform").write_text(FAKE_TERRAFORM)
    (bin_dir / "terraform").chmod(0o755)
    result = base._make(repo, tmp_path, "promote", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    prod = base._envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text(base.PROD_ENV)
    (prod / "auth.auto.tfvars").write_text(base._auth(base.DEPLOYER, host=""))
    result = base._make(repo, tmp_path, "promote-to", "ENV=prod")
    assert result.returncode == 0, result.stdout + result.stderr
    envs = base._envs(repo)
    files = {str(p.relative_to(envs)): p.read_bytes() for p in sorted(envs.rglob("*"))
             if p.is_file() and ".runtime" not in p.parts}
    # make bootstraps the account env with a copy of shared/env.auto.tfvars.example,
    # whose comments are documentation that may change; its settings may not.
    seed = "account/env.auto.tfvars"
    files[seed] = b"".join(line for line in files[seed].splitlines(keepends=True) if not line.startswith(b"#"))
    return files


def test_a_legacy_layout_env_promotes_and_plans_exactly_as_before(tmp_path):
    tree = _base_tree(tmp_path)
    before = _legacy_promotion(tmp_path / "before", tree / "shared", tree / "aws" / "Makefile")
    after = _legacy_promotion(tmp_path / "after", SHARED, ROOT / "aws" / "Makefile")

    assert "dev/agents" not in " ".join(after) and "prod/agents" not in " ".join(after)
    # The one addition since: the record of the tiers promote-to wrote (decision 8).
    record = json.loads(after.pop("prod/generated/promoted_access_tiers.json"))
    assert record["access_tier_groups"] == hcl2.loads(after["prod/env.auto.tfvars"].decode())["access_tier_groups"]
    assert after == before
