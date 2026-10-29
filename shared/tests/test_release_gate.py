"""Release gating (rollout step 9): rehearse receipts, provenance, manifests, release receipts.

Every test runs against a real git checkout in tmp_path with a bare
``origin`` it fetches from, laid out like a user's repo (envs/ committed,
runtime files ignored). Receipts go to GENIERAILS_STATE_DIR in tmp_path.
"""

from __future__ import annotations

import json
import time
import os
import subprocess
import sys
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
for _path in (SHARED, SHARED / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import generate_abac  # noqa: E402
from governance_policies import build_model  # noqa: E402
from scripts import policy_generations as pg  # noqa: E402
from scripts import release_gate as rg  # noqa: E402
from scripts import runtime_overlay as ro  # noqa: E402
from scripts import staged_policy_apply as spa  # noqa: E402

# What a user's repo ignores (shared/docs/version-control.md).
GITIGNORE = """\
.genierails-state/
envs/*/auth.auto.tfvars
envs/*/abac.auto.tfvars
envs/*/data_access/
envs/*/ddl/_fetched.sql
envs/*/generated/.*
envs/*/generated/genie_space_derived_acl_groups.auto.tfvars
envs/*/classification.auto.tfvars
envs/*/discovered_uc_tables.auto.tfvars
envs/*/.*
*.tfstate
*.tfstate.backup
.terraform/
"""
DEV_ENV = ('governance_mode = "deterministic"\ndeployer_principal_id = "d0000000-0000-0000-0000-00000000d001"\n'
           'access_tier_groups = ["analysts", "viewers"]\n')
PROD_ENV = ('governance_mode = "deterministic"\ndeployer_principal_id = "d0000000-0000-0000-0000-00000000d002"\n'
            'promote_from = "dev"\n'
            'catalog_map = { "dev_cat" = "prod_cat" }\naccess_tier_groups = ["analysts", "viewers"]\n')
EMPTY_ABAC = "tag_policies = [\n]\ntag_assignments = [\n]\nfgac_policies = [\n]\n"


def git_env(tmp_path: Path) -> dict:
    config = tmp_path / "gitconfig"
    if not config.exists():
        config.write_text("[user]\n\tname = Test\n\temail = test@example.com\n[commit]\n\tgpgsign = false\n"
                          "[init]\n\tdefaultBranch = main\n")
    return {"GIT_CONFIG_GLOBAL": str(config), "GIT_CONFIG_NOSYSTEM": "1"}


class Repo:
    """A user's GenieRails repo: shared/ code, aws/envs/{account,dev,prod} committed, a bare origin."""

    def __init__(self, tmp_path: Path, monkeypatch=None):
        self.tmp = tmp_path
        self.root = tmp_path / "repo"
        self.origin = tmp_path / "origin.git"
        self.state = tmp_path / "state"
        self.state.mkdir(exist_ok=True)  # GenieRails never creates a missing state dir
        self.env = git_env(tmp_path) | {rg.STATE_DIR_ENV: str(self.state)}
        if monkeypatch is not None:
            for key, value in self.env.items():
                monkeypatch.setenv(key, value)
        self.cloud = self.root / "aws"
        self.dev = self.cloud / "envs" / "dev"
        self.prod = self.cloud / "envs" / "prod"
        files = {
            "shared/scripts/code.py": "x = 1\n",
            "shared/docs/notes.md": "docs\n",
            "aws/Makefile": "# cloud wrapper\n",
            "aws/.gitignore": GITIGNORE,
            "aws/envs/account/env.auto.tfvars": "manage_groups = false\n",
            "aws/envs/dev/env.auto.tfvars": DEV_ENV,
            "aws/envs/dev/generated/abac.auto.tfvars": EMPTY_ABAC,
            "aws/envs/prod/env.auto.tfvars": PROD_ENV,
            "aws/envs/prod/generated/abac.auto.tfvars": EMPTY_ABAC,
            "aws/envs/prod/generated/masking_functions.sql": "",
        }
        for rel, text in files.items():
            path = self.root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        self.git("init", "-q", "-b", "main")
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "init")
        subprocess.run(["git", "init", "-q", "--bare", str(self.origin)], check=True, env=os.environ | self.env)
        self.git("remote", "add", "origin", str(self.origin))
        self.git("push", "-q", "origin", "main")

    def git(self, *args: str) -> str:
        result = subprocess.run(["git", "-C", str(self.root), *args], text=True, capture_output=True,
                                env=os.environ | self.env)
        assert result.returncode == 0, result.stderr
        return result.stdout.strip()

    def head(self) -> str:
        return self.git("rev-parse", "HEAD")

    def commit(self, files: dict[str, str], *, push: bool = True, message: str = "change") -> str:
        for rel, text in files.items():
            path = self.root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", message)
        if push:
            self.git("push", "-q", "origin", "main")
        return self.head()


@pytest.fixture
def repo(tmp_path, monkeypatch):
    r = Repo(tmp_path, monkeypatch)
    # promote-to from the commit alone: here, the committed prod output is current.
    monkeypatch.setattr(rg, "promoted_digest",
                        lambda cloud, makefile, make, target: (rg.generated_digest(Path(cloud) / "envs" / target
                                                                                   / "generated"), ""))
    return r


def rehearse(repo: Repo, result: str = "passed") -> dict:
    assert rg.rehearse_start(repo.dev, "dev", Path("Makefile"), "make") == "run"
    return rg.rehearse_finish(repo.dev, "dev", result, Path("Makefile"), "make")


# -- rehearse receipts ------------------------------------------------------------

def test_a_passing_rehearse_writes_an_append_only_receipt_keyed_by_commit(repo):
    receipt = rehearse(repo)
    path = Path(receipt["_path"])
    assert path.parent.name == repo.head()
    assert path.is_relative_to(repo.state / "aws" / "dev" / "receipts" / "rehearse")
    stored = json.loads(path.read_text())
    assert stored["commit"] == repo.head() and stored["clean_tree"] is True and stored["result"] == "passed"
    assert stored["fingerprint"] == rg.inputs_fingerprint(repo.root, repo.cloud, "dev")["fingerprint"]
    assert stored["targets"] == {"prod": {"digest": rg.generated_digest(repo.prod / "generated")}}
    assert not os.access(path, os.W_OK)
    with pytest.raises(rg.Refused, match="never overwritten"):
        rg._write_once(path, {"forged": True})
    assert json.loads(path.read_text()) == stored
    # The default store is an ignored dir in the checkout.
    os.environ.pop(rg.STATE_DIR_ENV)
    assert rg.state_root(repo.cloud) == repo.cloud.resolve() / ".genierails-state"


def test_unchanged_dev_inputs_write_a_receipt_without_reapplying_but_recompute_digests(repo, monkeypatch):
    first = rehearse(repo)
    # A prod-only change (an env-owned prod setting): same dev fingerprint.
    commit = repo.commit({"aws/envs/prod/ack.txt": "# none\n"})
    calls = []
    monkeypatch.setattr(rg, "promoted_digest", lambda *a: calls.append(a[-1]) or ("recomputed", ""))
    assert rg.rehearse_start(repo.dev, "dev", Path("Makefile"), "make") == "reused"
    receipt = rg.passing_receipt(repo.cloud, "dev", commit)
    assert receipt["reused_from"] == first["commit"] and receipt["applied"] is False
    assert receipt["targets"]["prod"]["digest"] == "recomputed" and calls == ["prod"]


def test_changed_code_or_dev_inputs_rehearse_again(repo):
    rehearse(repo)
    repo.commit({"shared/scripts/code.py": "x = 2\n"})
    assert rg.rehearse_start(repo.dev, "dev", Path("Makefile"), "make") == "run"
    rg.rehearse_finish(repo.dev, "dev", "passed", Path("Makefile"), "make")
    repo.commit({"aws/envs/dev/env.auto.tfvars": DEV_ENV + 'uc_catalog = "dev_cat"\n'})
    assert rg.rehearse_start(repo.dev, "dev", Path("Makefile"), "make") == "run"


def test_docs_and_tests_are_not_code_for_the_fingerprint(repo):
    rehearse(repo)
    repo.commit({"shared/docs/notes.md": "more docs\n", "shared/tests/test_x.py": "pass\n"})
    assert rg.rehearse_start(repo.dev, "dev", Path("Makefile"), "make") == "reused"


def test_a_failed_rehearse_is_never_reused(repo):
    rehearse(repo, "failed")
    assert rg.rehearse_start(repo.dev, "dev", Path("Makefile"), "make") == "run"


def test_a_run_that_rewrites_a_committed_input_records_a_failure(repo):
    assert rg.rehearse_start(repo.dev, "dev", Path("Makefile"), "make") == "run"
    (repo.dev / "generated" / "abac.auto.tfvars").write_text(EMPTY_ABAC + "# rewritten by an applying run\n")
    receipt = rg.rehearse_finish(repo.dev, "dev", "passed", Path("Makefile"), "make")
    assert receipt["result"] == "failed" and "never rewrite committed files" in receipt["reason"]
    assert receipt["clean_tree"] is False


def test_runtime_files_are_not_inputs(repo):
    before = rg.inputs_fingerprint(repo.root, repo.cloud, "dev")
    for rel in ("data_access/abac.auto.tfvars", "abac.auto.tfvars", "generated/.runtime/abac.auto.tfvars",
                "ddl/_fetched.sql", "terraform.tfstate", ".genie_space_id", "auth.auto.tfvars",
                "generated/genie_space_derived_acl_groups.auto.tfvars", "classification.auto.tfvars"):
        path = repo.dev / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("runtime\n")
    assert rg.inputs_fingerprint(repo.root, repo.cloud, "dev") == before
    assert rg.checkout_status(repo.dev)["clean_tree"] is True


def test_a_dirty_or_uncommitted_checkout_rehearses_but_its_receipt_is_not_clean(repo):
    (repo.dev / "generated" / "abac.auto.tfvars").write_text(EMPTY_ABAC + "# local edit\n")
    receipt = rehearse(repo)
    assert receipt["clean_tree"] is False and any("abac.auto.tfvars" in d for d in receipt["dirty"])
    assert rg.passing_receipt(repo.cloud, "dev", repo.head()) is None


# -- provenance (release-plan) -------------------------------------------------------

def test_provenance_passes_for_a_rehearsed_clean_pushed_commit(repo):
    rehearse(repo)
    prov = rg.provenance(repo.prod, "prod")
    assert prov["commit"] == repo.head() and prov["rollback"] is None and prov["source_env"] == "dev"


def test_provenance_refuses_legacy_source_for_deterministic_destination(repo):
    repo.commit({"aws/envs/dev/env.auto.tfvars": 'uc_tables = ["dev_catalog.s.t"]\n'})
    with pytest.raises(rg.Refused, match="legacy.*deterministic.*legacy-migration.md"):
        rg.provenance(repo.prod, "prod")


def test_provenance_names_a_missing_promote_from_env(repo):
    repo.commit({"aws/envs/prod/env.auto.tfvars": PROD_ENV.replace('promote_from = "dev"',
                                                                    'promote_from = "typo"')})
    with pytest.raises(rg.Refused, match=r"envs/typo/env\.auto\.tfvars was not found"):
        rg.provenance(repo.prod, "prod")


@pytest.mark.parametrize("name,text,expected", [
    ("prod", 'governance_mode = "legacy"\n', rg.PROMOTED),
    ("stg", 'governance_mode = "legacy"\npromote_from = "dev"\n', rg.PROMOTED),
    ("dev", 'governance_mode = "legacy"\n', rg.LEGACY),
    ("dev", 'governance_mode = "deterministic"\n', rg.REHEARSE),
])
def test_env_role_keeps_promotion_separate_from_governance_mode(tmp_path, name, text, expected):
    env = tmp_path / name
    env.mkdir()
    env.joinpath("env.auto.tfvars").write_text(text)
    assert rg.env_role(env) == expected


def test_provenance_refuses_a_stale_receipt(repo):
    rehearse(repo)
    repo.commit({"aws/envs/dev/env.auto.tfvars": DEV_ENV + "# new\n"})
    with pytest.raises(rg.Refused, match="no passing rehearsal of"):
        rg.provenance(repo.prod, "prod")


def test_the_promotion_commit_needs_its_own_dev_rehearse_which_reuses_the_apply(repo):
    """Walkthrough Prod step 3: rehearse dev on the promotion commit before release-plan."""
    first = rehearse(repo)
    promoted = repo.commit({"aws/envs/prod/generated/masking_functions.sql": "-- promoted\n"},
                           message="Promote the rules to prod")
    with pytest.raises(rg.Refused, match=f"no passing rehearsal of {promoted[:12]}"):
        rg.provenance(repo.prod, "prod")
    # Only envs/prod changed: dev isn't applied again, the receipt records the new promote-to digest.
    assert rg.rehearse_start(repo.dev, "dev", Path("Makefile"), "make") == "reused"
    receipt = rg.passing_receipt(repo.cloud, "dev", promoted)
    assert receipt["applied"] is False and receipt["reused_from"] == first["commit"]
    assert receipt["targets"]["prod"]["digest"] == rg.generated_digest(repo.prod / "generated")
    assert receipt["targets"]["prod"]["digest"] != first["targets"]["prod"]["digest"]
    assert rg.provenance(repo.prod, "prod")["commit"] == promoted


# -- rehearse-reuse (release-plan reuses the source env's rehearsal) ----------------------

def _reuse(repo: Repo, env_dir: Path | None = None, env: str = "prod") -> str | None:
    return rg.rehearse_reuse(env_dir or repo.prod, env, Path("Makefile"), "make")


def _dev_receipts(repo: Repo) -> list[dict]:
    return rg.rehearse_receipts(repo.cloud, "dev", any_store=True)


def test_release_plan_reuse_writes_rehearses_receipt_for_a_promotion_commit_without_applying(repo, capsys):
    """The second make rehearse ENV=dev on the promotion commit is no longer needed."""
    first = rehearse(repo)
    promoted = repo.commit({"aws/envs/prod/generated/masking_functions.sql": "-- promoted\n"},
                           message="Promote the rules to prod")
    capsys.readouterr()
    assert _reuse(repo) is None
    out = capsys.readouterr().out
    assert "reusing dev's passing rehearsal of " + first["commit"][:12] + " (nothing is applied)" in out, out
    receipt = rg.passing_receipt(repo.cloud, "dev", promoted)
    assert receipt["applied"] is False and receipt["reused_from"] == first["commit"]
    assert receipt["targets"]["prod"]["digest"] == rg.generated_digest(repo.prod / "generated")
    # The same receipt make rehearse ENV=dev writes when it reuses.
    again = repo.commit({"aws/envs/prod/ack.txt": "# none\n"})
    assert rg.rehearse_start(repo.dev, "dev", Path("Makefile"), "make") == "reused"
    assert set(rg.passing_receipt(repo.cloud, "dev", again)) == set(receipt)
    assert not (rg.env_state(repo.cloud, "dev") / "rehearse.pending.json").exists()
    rg.provenance(repo.prod, "prod")


def test_release_plan_reuse_takes_and_releases_the_source_env_lock(repo, monkeypatch):
    from scripts import environment_lock

    rehearse(repo)
    repo.commit({"aws/envs/prod/ack.txt": "# none\n"})
    held = []
    monkeypatch.setattr(rg, "_write_reused", lambda cloud, env, *a: held.append(
        environment_lock.owns_lock(repo.dev, os.getpid())))
    assert _reuse(repo) is None
    assert held == [True] and not environment_lock.lock_path(repo.dev).exists()


def test_release_plan_reuse_refuses_while_the_source_env_is_locked(repo):
    from scripts import environment_lock

    rehearse(repo)
    repo.commit({"aws/envs/prod/ack.txt": "# none\n"})
    ok, _, _ = environment_lock.acquire(repo.dev, os.getppid(), "rehearse")
    assert ok
    try:
        with pytest.raises(rg.Refused, match="could not lock dev to reuse its rehearsal: .*held by make rehearse"):
            _reuse(repo)
    finally:
        environment_lock.release_lock(repo.dev, os.getppid())
    assert rg.passing_receipt(repo.cloud, "dev", repo.head()) is None


def test_release_plan_reuse_is_a_no_op_when_head_was_rehearsed(repo):
    rehearse(repo)
    before = len(_dev_receipts(repo))
    assert _reuse(repo) is None and len(_dev_receipts(repo)) == before


def test_release_plan_reuse_refuses_changed_source_inputs(repo):
    rehearse(repo)
    repo.commit({"aws/envs/dev/env.auto.tfvars": DEV_ENV + "# new\n"})
    why = _reuse(repo)
    assert why and "env inputs of dev changed since its last passing rehearsal" in why
    assert "run make rehearse ENV=dev on this commit" in why
    assert rg.passing_receipt(repo.cloud, "dev", repo.head()) is None
    with pytest.raises(rg.Refused, match="no passing rehearsal of"):
        rg.provenance(repo.prod, "prod")


def test_release_plan_reuse_refuses_changed_code(repo):
    rehearse(repo)
    repo.commit({"shared/scripts/code.py": "x = 2\n"})
    why = _reuse(repo)
    assert why and why.startswith("code inputs of dev changed") and "make rehearse ENV=dev" in why


@pytest.mark.parametrize("prior", [None, "failed"])
def test_release_plan_reuse_refuses_without_an_earlier_passing_rehearsal(repo, prior):
    if prior:
        rehearse(repo, prior)
        repo.commit({"aws/envs/prod/ack.txt": "# none\n"})
    why = _reuse(repo)
    assert why == "dev has no earlier passing rehearsal to reuse; run make rehearse ENV=dev on this commit (it applies dev)"
    assert rg.passing_receipt(repo.cloud, "dev", repo.head()) is None


@pytest.mark.parametrize("rel, text", [
    ("aws/.gitignore", GITIGNORE + "/envs/prod/abac.auto.tfvars\n"),
    ("shared/scratch.txt", "untracked\n"),
    ("aws/envs/prod/ack.txt", "# uncommitted\n"),
])
def test_release_plan_reuse_refuses_a_dirty_checkout_and_writes_nothing(repo, rel, text):
    rehearse(repo)
    repo.commit({"aws/envs/prod/generated/masking_functions.sql": "-- promoted\n"})
    before = len(_dev_receipts(repo))
    (repo.root / rel).write_text(text)
    why = _reuse(repo)
    assert why and "the checkout is not clean" in why and "re-run make release-plan ENV=prod" in why
    assert len(_dev_receipts(repo)) == before


def test_release_plan_reuse_never_counts_a_receipt_from_another_store(repo):
    """Dev rehearsed only in CI: its receipts (a downloaded artifact) never count on this machine."""
    receipt = rehearse(repo)
    path = Path(receipt["_path"])
    foreign = {k: v for k, v in json.loads(path.read_text()).items()} | {"store": "ci-runner-store"}
    path.chmod(0o600)
    path.unlink()
    rg._write_once(path, rg.seal(foreign))  # a genuine receipt of another store
    repo.commit({"aws/envs/prod/ack.txt": "# none\n"})
    why = _reuse(repo)
    assert why and "written by another store" in why and "make rehearse ENV=dev on this commit" in why
    assert "on this machine" in why
    assert rg.passing_receipt(repo.cloud, "dev", repo.head()) is None


def test_release_plan_reuse_refuses_a_multi_hop_chain(repo):
    rehearse(repo)
    stg_env = PROD_ENV.replace('catalog_map = { "dev_cat" = "prod_cat" }', 'catalog_map = { "dev_cat" = "stg_cat" }')
    repo.commit({"aws/envs/stg/env.auto.tfvars": stg_env,
                 "aws/envs/stg/generated/abac.auto.tfvars": EMPTY_ABAC,
                 "aws/envs/prod/env.auto.tfvars": PROD_ENV.replace('promote_from = "dev"', 'promote_from = "stg"')})
    before = len(_dev_receipts(repo))
    why = _reuse(repo)
    assert why and "envs/stg is itself promoted" in why and "multi-hop" in why
    assert "make promote-to ENV=prod" in why
    assert len(_dev_receipts(repo)) == before and not rg.rehearse_receipts(repo.cloud, "stg", any_store=True)
    with pytest.raises(rg.Refused, match="no passing rehearsal of"):
        rg.provenance(repo.prod, "prod")


def test_release_plan_reuse_leaves_a_legacy_or_missing_source_to_provenance(repo):
    repo.commit({"aws/envs/prod/env.auto.tfvars": PROD_ENV.replace('promote_from = "dev"', 'promote_from = "typo"')})
    assert _reuse(repo) is None
    repo.commit({"aws/envs/prod/env.auto.tfvars": PROD_ENV,
                 "aws/envs/dev/env.auto.tfvars": 'uc_tables = ["dev_catalog.s.t"]\n'})
    assert _reuse(repo) is None and not _dev_receipts(repo)


# -- reuse refuses divergent commits and edited or inconsistent receipts (Codex r1) ---------

def _diverge(repo: Repo) -> tuple[str, str]:
    """A passing dev rehearsal of a commit, then HEAD on a side branch that doesn't contain it."""
    base = repo.head()
    rehearsed = repo.commit({"aws/envs/prod/ack.txt": "# main\n"})
    rehearse(repo)
    repo.git("checkout", "-q", "-b", "side", base)
    side = repo.commit({"aws/envs/prod/ack.txt": "# side\n"}, push=False)
    assert not rg.is_ancestor(repo.root, rehearsed, side)
    assert rg.inputs_fingerprint(repo.root, repo.cloud, "dev")["fingerprint"] == \
        rg.passing_receipt(repo.cloud, "dev", rehearsed)["fingerprint"]
    return rehearsed, side


def test_rehearse_never_reuses_a_receipt_of_a_divergent_commit(repo, capsys):
    rehearsed, side = _diverge(repo)
    capsys.readouterr()
    assert rg.rehearse_start(repo.dev, "dev", Path("Makefile"), "make") == "run"
    out = capsys.readouterr().out
    assert f"{rehearsed[:12]} is not an ancestor of {side[:12]} (a divergent branch)" in out, out
    assert rg.passing_receipt(repo.cloud, "dev", side) is None


def test_release_plan_never_reuses_a_receipt_of_a_divergent_commit(repo):
    rehearsed, side = _diverge(repo)
    before = len(_dev_receipts(repo))
    why = _reuse(repo)
    assert why and f"{rehearsed[:12]} is not an ancestor of {side[:12]}" in why and "make rehearse ENV=dev" in why
    assert len(_dev_receipts(repo)) == before and rg.passing_receipt(repo.cloud, "dev", side) is None


DROP = object()


def _edit_receipt(receipt: dict, **changes) -> dict:
    """Edit a stored receipt in place, by hand (what the seal must catch); DROP removes a field."""
    path = Path(receipt["_path"])
    data = json.loads(path.read_text())
    for key, value in changes.items():
        if value is DROP:
            data.pop(key, None)
        else:
            data[key] = value
    path.chmod(0o600)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    return data


def test_a_failed_receipt_edited_to_passed_is_never_reused(repo, capsys):
    failed = rehearse(repo, "failed")
    _edit_receipt(failed, result="passed", reason=DROP)
    commit = repo.commit({"aws/envs/prod/ack.txt": "# none\n"})
    before = len(_dev_receipts(repo))
    why = _reuse(repo)
    assert why and "its digest doesn't match its content" in why and "make rehearse ENV=dev" in why
    assert len(_dev_receipts(repo)) == before and rg.passing_receipt(repo.cloud, "dev", commit) is None
    capsys.readouterr()
    assert rg.rehearse_start(repo.dev, "dev", Path("Makefile"), "make") == "run"
    assert "its digest doesn't match its content" in capsys.readouterr().out


def test_a_receipt_edited_to_passed_never_satisfies_provenance(repo):
    failed = rehearse(repo, "failed")
    _edit_receipt(failed, result="passed", reason=DROP)
    assert rg.passing_receipt(repo.cloud, "dev", repo.head()) is None
    with pytest.raises(rg.Refused, match="fail validation .edited by hand.. and never count: .*its digest doesn't match"):
        rg.provenance(repo.prod, "prod")


INCONSISTENT = [
    ({"applied": True, "reused_from": "a" * 40}, "applied and reused_from disagree"),
    ({"applied": False, "reused_from": None}, "applied and reused_from disagree"),
    ({"fingerprint_parts": {"code": "x", "env": "y", "account": "z", "fingerprint": "w"}},
     "its fingerprint doesn't match its parts"),
    ({"commit": "b" * 40}, "its commit doesn't match where it is stored"),
    ({"reason": "a step failed"}, "a passing receipt carries a failure reason"),
    ({"env": "prod"}, "not a rehearse receipt of this env and version"),
    ({"dirty": ["uncommitted: x"]}, "not a passing rehearsal of a clean checkout"),
    ({"surprise": 1}, "unexpected fields: unknown surprise"),
    ({"targets": DROP}, "unexpected fields: missing targets"),
]


@pytest.mark.parametrize(("changes", "why"), INCONSISTENT)
def test_a_sealed_but_inconsistent_receipt_is_never_reused(repo, changes, why):
    """Re-sealing after an edit passes the digest; the fields must still fit together."""
    first = rehearse(repo)
    data = {k: v for k, v in _edit_receipt(first, **changes).items()}
    Path(first["_path"]).write_text(json.dumps(rg.seal(data), indent=2, sort_keys=True) + "\n")
    repo.commit({"aws/envs/prod/ack.txt": "# none\n"})
    reason = _reuse(repo)
    assert reason and why in reason, reason


def _reseal(receipt: dict, **changes) -> None:
    data = _edit_receipt(receipt, **changes)
    Path(receipt["_path"]).write_text(json.dumps(rg.seal(data), indent=2, sort_keys=True) + "\n")


@pytest.mark.parametrize(("changes", "why"), [c for c in INCONSISTENT if "commit" not in c[0]])
def test_a_sealed_but_inconsistent_receipt_never_satisfies_provenance(repo, changes, why):
    """Codex r2: provenance runs the same validator as reuse (receipt_problem), not just the seal."""
    first = rehearse(repo)
    _reseal(first, **changes)
    assert rg.passing_receipt(repo.cloud, "dev", repo.head()) is None
    with pytest.raises(rg.Refused) as refused:
        rg.provenance(repo.prod, "prod")
    assert "no passing rehearsal of" in str(refused.value)
    assert f"fail validation (edited by hand?) and never count: {Path(first['_path']).name}: {why}" \
        in str(refused.value), str(refused.value)


def test_a_resealed_receipt_with_an_unknown_field_never_satisfies_provenance(repo):
    first = rehearse(repo)
    assert rg.provenance(repo.prod, "prod")["commit"] == repo.head()
    _reseal(first, unknown=True)
    assert rg.sealed(json.loads(Path(first["_path"]).read_text()))
    with pytest.raises(rg.Refused, match="never count: .*unexpected fields: unknown unknown"):
        rg.provenance(repo.prod, "prod")


def test_a_receipt_stored_under_another_commit_never_satisfies_provenance(repo):
    """A receipt moved (swapped) to another commit's directory, re-sealed: its path and commit disagree."""
    first = rehearse(repo)
    path = Path(first["_path"])
    moved = path.parent.parent / ("b" * 40) / path.name
    moved.parent.mkdir()
    path.chmod(0o600)
    path.rename(moved)
    _reseal(first | {"_path": str(moved)})
    with pytest.raises(rg.Refused, match="never count: .*its commit doesn't match where it is stored"):
        rg.provenance(repo.prod, "prod")
    assert rg.passing_receipt(repo.cloud, "dev", repo.head()) is None


def test_every_receipt_rehearse_writes_is_sealed_and_reusable(repo):
    applied = rehearse(repo)
    assert rg.sealed(applied) and rg.receipt_problem(applied, "dev") is None
    commit = repo.commit({"aws/envs/prod/ack.txt": "# none\n"})
    assert rg.rehearse_start(repo.dev, "dev", Path("Makefile"), "make") == "reused"
    reused = rg.passing_receipt(repo.cloud, "dev", commit)
    assert rg.sealed(reused) and rg.receipt_problem(reused, "dev") is None
    failed = rg.rehearse_finish(repo.dev, "dev", "failed", Path("Makefile"), "make", "boom")
    assert rg.sealed(failed) and rg.receipt_problem(failed, "dev") == "not a passing rehearsal of a clean checkout"


def test_the_rehearse_reuse_cli_prints_why_and_exits_zero_for_provenance_to_refuse(repo, capsys):
    rc = rg.main(["rehearse-reuse", "--env-dir", str(repo.prod), "--env", "prod", "--makefile", "Makefile"])
    assert rc == 0
    err = capsys.readouterr().err
    assert "release-plan: did not reuse a rehearsal for this commit: dev has no earlier passing rehearsal" in err


@pytest.mark.parametrize("rel, text", [
    # make setup ENV=prod adds prod's lines to the cloud .gitignore.
    ("aws/.gitignore", GITIGNORE + "/envs/prod/abac.auto.tfvars\n"),
    ("shared/scratch.txt", "untracked\n"),
    ("aws/notes.txt", "untracked\n"),
])
def test_a_reused_rehearsal_of_an_unclean_checkout_says_so_and_release_plan_names_why(repo, capsys, rel, text):
    rehearse(repo)
    promoted = repo.commit({"aws/envs/prod/generated/masking_functions.sql": "-- promoted\n"})
    (repo.root / rel).write_text(text)
    capsys.readouterr()

    assert rg.rehearse_start(repo.dev, "dev", Path("Makefile"), "make") == "reused"

    out = capsys.readouterr().out
    assert "NOT a clean checkout; release-plan ignores this receipt" in out, out
    assert "uncommitted: " in out and Path(rel).name in out, out
    assert rg.passing_receipt(repo.cloud, "dev", promoted) is None
    with pytest.raises(rg.Refused) as refused:
        rg.provenance(repo.prod, "prod")
    assert "a receipt exists but its checkout wasn't clean: uncommitted: " in str(refused.value)
    assert Path(rel).name in str(refused.value).split("wasn't clean:")[1]


def test_release_plan_does_not_blame_a_dirty_receipt_it_does_not_have(repo):
    rehearse(repo)
    repo.commit({"aws/envs/prod/generated/masking_functions.sql": "-- promoted\n"})
    with pytest.raises(rg.Refused) as refused:
        rg.provenance(repo.prod, "prod")
    assert "no passing rehearsal of" in str(refused.value)
    assert "wasn't clean" not in str(refused.value)


def test_provenance_refuses_a_failed_rehearsal(repo):
    rehearse(repo, "failed")
    with pytest.raises(rg.Refused, match="no passing rehearsal of"):
        rg.provenance(repo.prod, "prod")


def test_provenance_refuses_a_dirty_tree(repo):
    rehearse(repo)
    (repo.root / "shared" / "scripts" / "code.py").write_text("x = 'edited'\n")
    with pytest.raises(rg.Refused, match="checkout is not clean"):
        rg.provenance(repo.prod, "prod")


def test_provenance_refuses_an_untracked_file(repo):
    rehearse(repo)
    (repo.prod / "agents").mkdir()
    (repo.prod / "agents" / "x.auto.tfvars").write_text("acl_groups = []\n")
    with pytest.raises(rg.Refused, match="checkout is not clean"):
        rg.provenance(repo.prod, "prod")


def test_provenance_refuses_a_commit_not_on_origin_main(repo):
    repo.commit({"aws/envs/prod/ack.txt": "# local only\n"}, push=False)
    rehearse(repo)
    with pytest.raises(rg.Refused, match="is not on origin/main"):
        rg.provenance(repo.prod, "prod")


def test_provenance_accepts_committed_head_in_a_repo_with_no_remotes(repo):
    repo.git("remote", "remove", "origin")
    local = repo.commit({"aws/envs/prod/ack.txt": "# local only\n"}, push=False)
    rehearse(repo)
    prov = rg.provenance(repo.prod, "prod")
    assert prov["commit"] == local
    assert prov["remote"] == local and prov["remote_commit"] == local


def test_provenance_refuses_when_origin_is_missing_but_another_remote_exists(repo):
    repo.git("remote", "remove", "origin")
    repo.git("remote", "add", "fork", str(repo.origin))
    repo.commit({"aws/envs/prod/ack.txt": "# local only\n"}, push=False)
    rehearse(repo)
    with pytest.raises(rg.Refused, match="git fetch .* origin main failed"):
        rg.provenance(repo.prod, "prod")


def test_provenance_refuses_when_origin_exists_without_origin_main(repo, tmp_path):
    empty = tmp_path / "empty.git"
    subprocess.run(["git", "init", "-q", "--bare", str(empty)], check=True, env=os.environ | repo.env)
    repo.git("remote", "set-url", "origin", str(empty))
    rehearse(repo)
    with pytest.raises(rg.Refused, match="git fetch .* origin main failed"):
        rg.provenance(repo.prod, "prod")


def test_provenance_fetches_origin_main_fresh(repo, tmp_path):
    rehearse(repo)
    # origin/main moved on elsewhere; a fresh fetch still finds HEAD on it.
    other = tmp_path / "other"
    subprocess.run(["git", "clone", "-q", str(repo.origin), str(other)], check=True, env=os.environ | repo.env)
    (other / "aws" / "envs" / "prod" / "ack.txt").write_text("# newer\n")
    for args in (("add", "-A"), ("commit", "-q", "-m", "newer"), ("push", "-q", "origin", "main")):
        subprocess.run(["git", "-C", str(other), *args], check=True, env=os.environ | repo.env)
    rg.provenance(repo.prod, "prod")
    assert repo.git("rev-parse", "origin/main") == subprocess.run(
        ["git", "-C", str(other), "rev-parse", "HEAD"], text=True, capture_output=True).stdout.strip()


def test_provenance_refuses_a_fingerprint_mismatch(repo):
    receipt = rehearse(repo)
    forged = {k: v for k, v in receipt.items() if k != "_path"} | {
        "fingerprint": "0" * 64, "fingerprint_parts": receipt["fingerprint_parts"] | {"fingerprint": "0" * 64},
        "created_at": rg._now()}
    rg._write_once(Path(receipt["_path"]).with_name("zz-forged.json"), rg.seal(forged))
    with pytest.raises(rg.Refused, match="fingerprint differs from the rehearsal receipt"):
        rg.provenance(repo.prod, "prod")


def test_provenance_refuses_a_prod_generated_digest_mismatch(repo, monkeypatch):
    monkeypatch.setattr(rg, "promoted_digest", lambda *a: ("what-promote-to-produces-now", ""))
    rehearse(repo)
    with pytest.raises(rg.Refused, match="stale promotion") as refused:
        rg.provenance(repo.prod, "prod")
    assert ("open a PR, then re-run make release-plan ENV=prod on its merge commit (it reuses the dev "
            "rehearsal while dev's inputs are unchanged)" in str(refused.value))


def test_provenance_refuses_a_receipt_without_a_digest_for_the_env(repo, monkeypatch):
    monkeypatch.setattr(rg, "promoted_digest", lambda *a: (None, "promote-to failed"))
    rehearse(repo)
    with pytest.raises(rg.Refused, match="no promote-to ENV=prod digest"):
        rg.provenance(repo.prod, "prod")


def _release_receipt_at(repo: Repo, commit: str) -> None:
    rg._write_once(rg.release_dir(repo.cloud, "prod") / f"{rg._now().replace(':', '')}-{commit[:12]}.json",
                   {"kind": "release", "version": rg.VERSION, "commit": commit, "created_at": rg._now()})


def test_provenance_refuses_an_older_commit_than_the_last_release(repo):
    older = repo.head()
    newer = repo.commit({"aws/envs/prod/ack.txt": "# newer\n"})
    _release_receipt_at(repo, newer)
    repo.git("checkout", "-q", "--detach", older)
    rehearse(repo)
    with pytest.raises(rg.Refused, match="older than the last release"):
        rg.provenance(repo.prod, "prod")


def _roll_back_to(repo: Repo, ack: str) -> str:
    """Release v2, then merge a reviewed rollback acknowledgement on main; return the old commit."""
    older = repo.head()
    released = repo.commit({"aws/envs/prod/env.auto.tfvars": PROD_ENV + "# v2\n"})
    _release_receipt_at(repo, released)
    repo.commit({"aws/envs/prod/ack.txt": ack.format(older=older)})
    repo.git("checkout", "-q", "--detach", older)
    return older


def test_provenance_allows_a_rollback_acknowledged_on_origin_main(repo):
    older = _roll_back_to(repo, "prod:rollback:{older}\n")
    rehearse(repo)
    prov = rg.provenance(repo.prod, "prod")
    assert prov["commit"] == older and prov["rollback"]["key"] == f"prod:rollback:{older}"


def test_local_only_rollback_ack_is_read_from_committed_head(repo):
    repo.git("remote", "remove", "origin")
    older = repo.head()
    released = repo.commit({"aws/envs/prod/env.auto.tfvars": PROD_ENV + "# v2\n"}, push=False)
    _release_receipt_at(repo, released)
    repo.commit({"aws/envs/prod/ack.txt": f"prod:rollback:{older}\n"}, push=False)
    assert [e.key for e in rg._committed_acks(repo.root, repo.prod, "prod", repo.head())] == [
        f"prod:rollback:{older}"
    ]
    (repo.prod / "ack.txt").write_text("prod:rollback:" + "ab" * 20 + "\n")
    assert [e.key for e in rg._committed_acks(repo.root, repo.prod, "prod", repo.head())] == [
        f"prod:rollback:{older}"
    ]


def test_provenance_accepts_an_abbreviated_rollback_commit(repo):
    older = repo.head()
    _roll_back_to(repo, f"prod:rollback:{older[:10]}\n")
    rehearse(repo)
    assert rg.provenance(repo.prod, "prod")["rollback"]["key"] == f"prod:rollback:{older[:10]}"


def test_a_rollback_acknowledgement_for_another_commit_does_not_apply(repo):
    _roll_back_to(repo, "prod:rollback:" + "ab" * 20 + "\n")
    rehearse(repo)
    with pytest.raises(rg.Refused, match="older than the last release"):
        rg.provenance(repo.prod, "prod")


def test_a_rollback_acknowledgement_only_on_an_unmerged_branch_does_not_apply(repo):
    older = repo.head()
    released = repo.commit({"aws/envs/prod/env.auto.tfvars": PROD_ENV + "# v2\n"})
    _release_receipt_at(repo, released)
    repo.commit({"aws/envs/prod/ack.txt": f"prod:rollback:{older}\n"}, push=False)  # never merged
    repo.git("checkout", "-q", "--detach", older)
    rehearse(repo)
    with pytest.raises(rg.Refused, match="older than the last release"):
        rg.provenance(repo.prod, "prod")


def test_releasing_the_last_released_commit_again_is_not_a_rollback(repo):
    _release_receipt_at(repo, repo.head())
    rehearse(repo)
    assert rg.provenance(repo.prod, "prod")["rollback"] is None


def test_rehearse_dev_requires_promote_from_for_release_plan(repo):
    other = repo.cloud / "envs" / "bu1"
    (other / "generated").mkdir(parents=True)
    (other / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    with pytest.raises(rg.Refused, match="no promote_from"):
        rg.source_env(other)


# -- account operation provenance -------------------------------------------------------

def test_account_provenance_passes_for_clean_origin_main_head(repo):
    prov = rg.account_provenance(repo.cloud / "envs" / "account")
    assert prov["commit"] == repo.head()
    assert prov["remote"] == "origin/main"


def test_account_provenance_refuses_dirty_checkout(repo):
    (repo.cloud / "envs" / "account" / "env.auto.tfvars").write_text("manage_groups = true\n")
    with pytest.raises(rg.Refused, match="checkout is not clean"):
        rg.account_provenance(repo.cloud / "envs" / "account")


def test_account_provenance_refuses_a_local_only_commit(repo):
    repo.commit({"aws/envs/account/env.auto.tfvars": "manage_groups = false\n# local\n"}, push=False)
    with pytest.raises(rg.Refused, match="not exactly origin/main"):
        rg.account_provenance(repo.cloud / "envs" / "account")


def test_account_provenance_accepts_committed_head_in_a_repo_with_no_remotes(repo):
    repo.git("remote", "remove", "origin")
    local = repo.commit({"aws/envs/account/env.auto.tfvars": "manage_groups = false\n# local\n"}, push=False)
    prov = rg.account_provenance(repo.cloud / "envs" / "account")
    assert prov["commit"] == local
    assert prov["remote"] == local and prov["remote_commit"] == local


def test_account_provenance_refuses_local_head_when_any_remote_exists(repo):
    repo.git("remote", "remove", "origin")
    repo.git("remote", "add", "genierails", str(repo.origin))
    repo.commit({"aws/envs/account/env.auto.tfvars": "manage_groups = false\n# local\n"}, push=False)
    with pytest.raises(rg.Refused, match="git fetch .* origin main failed"):
        rg.account_provenance(repo.cloud / "envs" / "account")


def test_account_provenance_refuses_when_origin_main_moved_ahead(repo, tmp_path):
    other = tmp_path / "other"
    subprocess.run(["git", "clone", "-q", str(repo.origin), str(other)], check=True, env=os.environ | repo.env)
    (other / "aws" / "envs" / "account" / "env.auto.tfvars").write_text("manage_groups = false\n# newer\n")
    for args in (("add", "-A"), ("commit", "-q", "-m", "newer"), ("push", "-q", "origin", "main")):
        subprocess.run(["git", "-C", str(other), *args], check=True, env=os.environ | repo.env)
    with pytest.raises(rg.Refused, match="not exactly origin/main"):
        rg.account_provenance(repo.cloud / "envs" / "account")


def _account_plan_dir(repo: Repo, *, changes: bool = True) -> Path:
    plan_dir = rg.new_plan_dir(repo.cloud / "envs" / "account", "account")
    (plan_dir / "account.tfplan").write_bytes(b"account saved plan")
    (plan_dir / "account.json").write_text(json.dumps({"file": "account.tfplan", "changes": changes}))
    (plan_dir / "account.summary.json").write_text(json.dumps({
        "changes": changes, "resource_changes": {"create": 1, "update": 1},
    }))
    (plan_dir / "tag_sync.json").write_text(json.dumps({"diff": [
        {"key": "gr_treatment", "added": ["new"], "removed": ["old"], "left_in_place": ["theirs"]},
    ], "allowed": ["gr_treatment:old"], "warehouse_id": "wh"}))
    return plan_dir


@pytest.fixture
def account_manifest(repo, monkeypatch):
    def diff(_env_dir, allowed=(), warehouse_id=""):
        # Like the sync: only a named removal (with its warehouse) is removed.
        named = list(allowed) == ["gr_treatment:old"] and warehouse_id == "wh"
        return [{"key": "gr_treatment", "added": ["new"], "removed": ["old"] if named else [],
                 "left_in_place": ["theirs"] if named else ["old", "theirs"]}]
    monkeypatch.setattr(rg, "account_tag_sync_diff", diff)
    state = rg.root_state_paths(repo.cloud / "envs" / "account")["account"]
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"version": 4, "serial": 5, "lineage": "A", "resources": []}))
    prov = rg.account_provenance(repo.cloud / "envs" / "account")
    return rg.account_manifest_write(repo.cloud / "envs" / "account", "account", _account_plan_dir(repo), prov)


def test_account_plan_writes_one_immutable_manifest(repo, account_manifest):
    data = json.loads(account_manifest.read_text())
    assert data["kind"] == "account-manifest"
    assert data["commit"] == repo.head()
    assert data["state_serial"] == {"serial": 5, "lineage": "A"}
    assert data["plan"]["sha256"] == rg.sha256_bytes(b"account saved plan")
    assert data["plan"]["summary"]["resource_changes"] == {"create": 1, "update": 1}
    assert {"path": "env.auto.tfvars", "sha256": rg.sha256_file(repo.cloud / "envs" / "account" / "env.auto.tfvars")} in data["inputs"]
    assert data["tag_sync"] == [{"key": "gr_treatment", "added": ["new"], "removed": ["old"],
                                 "left_in_place": ["theirs"]}]
    assert data["tag_value_removal"] == {"allowed": ["gr_treatment:old"], "allowed_arg": "gr_treatment:old",
                                         "warehouse_id": "wh", "removed": ["gr_treatment:old"]}
    for path in [account_manifest.parent, *account_manifest.parent.rglob("*")]:
        assert path.stat().st_mode & 0o077 == 0, path
        if path.is_file():
            assert not os.access(path, os.W_OK), path
    assert rg.account_manifest_check(repo.cloud / "envs" / "account", "account", account_manifest)["id"] == data["id"]


def test_account_manifest_refuses_wrong_commit(repo, account_manifest):
    repo.commit({"aws/envs/account/env.auto.tfvars": "manage_groups = false\n# later\n"})
    with pytest.raises(rg.Refused, match="manifest planned"):
        rg.account_manifest_check(repo.cloud / "envs" / "account", "account", account_manifest)


def test_apply_account_manifest_check_accepts_local_only_committed_head(repo, monkeypatch):
    repo.git("remote", "remove", "origin")
    repo.commit({"aws/envs/account/env.auto.tfvars": "manage_groups = false\n# local\n"}, push=False)

    def diff(_env_dir, allowed=(), warehouse_id=""):
        return [{"key": "gr_treatment", "added": ["new"], "removed": ["old"], "left_in_place": []}]

    monkeypatch.setattr(rg, "account_tag_sync_diff", diff)
    state = rg.root_state_paths(repo.cloud / "envs" / "account")["account"]
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"version": 4, "serial": 5, "lineage": "A", "resources": []}))
    prov = rg.account_provenance(repo.cloud / "envs" / "account")
    manifest = rg.account_manifest_write(repo.cloud / "envs" / "account", "account", _account_plan_dir(repo), prov)
    assert rg.account_manifest_check(repo.cloud / "envs" / "account", "account", manifest)["commit"] == repo.head()


def test_account_manifest_refuses_state_serial_drift(repo, account_manifest):
    state = rg.root_state_paths(repo.cloud / "envs" / "account")["account"]
    state.write_text(json.dumps({"version": 4, "serial": 6, "lineage": "A", "resources": []}))
    with pytest.raises(rg.Refused, match="account state changed since account-plan"):
        rg.account_manifest_check(repo.cloud / "envs" / "account", "account", account_manifest)


def test_account_manifest_refuses_plan_hash_mismatch(repo, account_manifest):
    plan = account_manifest.parent / "account.tfplan"
    os.chmod(plan, 0o644)
    plan.write_bytes(b"tampered")
    with pytest.raises(rg.Refused, match="saved account plan .* changed"):
        rg.account_manifest_check(repo.cloud / "envs" / "account", "account", account_manifest)


def test_account_manifest_refuses_a_tampered_manifest(repo, account_manifest):
    os.chmod(account_manifest, 0o644)
    data = json.loads(account_manifest.read_text())
    data["plan"]["file"] = "other.tfplan"
    account_manifest.write_text(json.dumps(data))
    with pytest.raises(rg.Refused, match="not account.tfplan"):
        rg.account_manifest_check(repo.cloud / "envs" / "account", "account", account_manifest)


def test_account_manifest_refuses_account_input_hash_drift(repo, account_manifest):
    (repo.cloud / "envs" / "account" / "abac.auto.tfvars").write_text("tag_policies = []\n")
    with pytest.raises(rg.Refused, match="account input file hashes changed"):
        rg.account_manifest_check(repo.cloud / "envs" / "account", "account", account_manifest)


def test_account_input_hashes_pin_auth_endpoint_but_not_auth_secrets(tmp_path):
    env_dir = tmp_path / "envs" / "account"
    env_dir.mkdir(parents=True)
    auth = env_dir / "auth.auto.tfvars"
    auth.write_text('databricks_account_host = "https://accounts.one.example"\n'
                    'databricks_account_id = "123"\n'
                    'databricks_workspace_host = "https://workspace.one.example"\n'
                    'databricks_client_id = "client-one"\n'
                    'databricks_client_secret = "first"\n')
    first = rg.account_input_hashes(env_dir)
    assert [item["path"] for item in first] == [
        "auth.auto.tfvars#databricks_account_host,databricks_account_id,databricks_workspace_host"]
    auth.write_text('databricks_account_host = "https://accounts.one.example"\n'
                    'databricks_account_id = "123"\n'
                    'databricks_workspace_host = "https://workspace.one.example"\n'
                    'databricks_client_id = "client-two"\n'
                    'databricks_client_secret = "second"\n')
    assert rg.account_input_hashes(env_dir) == first
    auth.write_text('databricks_account_host = "https://accounts.one.example"\n'
                    'databricks_account_id = "123"\n'
                    'databricks_workspace_host = "https://workspace.two.example"\n'
                    'databricks_client_id = "client-two"\n'
                    'databricks_client_secret = "second"\n')
    assert rg.account_input_hashes(env_dir) != first


@pytest.mark.parametrize("missing", ["databricks_account_id", "databricks_workspace_host"])
def test_account_input_hashes_fail_closed_without_a_required_endpoint_key(tmp_path, missing):
    env_dir = tmp_path / "envs" / "account"
    env_dir.mkdir(parents=True)
    values = {"databricks_account_id": "123", "databricks_workspace_host": "https://workspace.example"}
    values.pop(missing)
    (env_dir / "auth.auto.tfvars").write_text("".join(f'{key} = "{value}"\n' for key, value in values.items()))
    with pytest.raises(rg.Refused, match=missing):
        rg.account_input_hashes(env_dir)


def test_account_input_hashes_refuse_an_auth_symlink_outside_envs(tmp_path):
    env_dir = tmp_path / "cloud" / "envs" / "account"
    env_dir.mkdir(parents=True)
    outside = tmp_path / "outside.auto.tfvars"
    outside.write_text('databricks_account_id = "123"\n'
                       'databricks_workspace_host = "https://workspace.example"\n')
    (env_dir / "auth.auto.tfvars").symlink_to(outside)
    with pytest.raises(rg.Refused, match="resolves outside"):
        rg.account_input_hashes(env_dir)


def test_account_manifest_refuses_tag_sync_diff_drift(repo, account_manifest, monkeypatch):
    monkeypatch.setattr(rg, "account_tag_sync_diff", lambda *_a: [
        {"key": "gr_treatment", "added": ["new"], "removed": []},
    ])
    with pytest.raises(rg.Refused, match=r"planned removals \['gr_treatment:old'\].*live removals now \[\]"):
        rg.account_manifest_check(repo.cloud / "envs" / "account", "account", account_manifest)


def test_account_manifest_ignores_values_left_in_place_changing(repo, account_manifest, monkeypatch):
    monkeypatch.setattr(rg, "account_tag_sync_diff", lambda *_a: [
        {"key": "gr_treatment", "added": ["new"], "removed": ["old"], "left_in_place": ["theirs", "more"]},
    ])
    rg.account_manifest_check(repo.cloud / "envs" / "account", "account", account_manifest)


def test_account_manifest_refuses_a_different_allow_list_at_apply(repo, account_manifest):
    with pytest.raises(rg.Refused, match="chosen at account-plan, not at apply-account"):
        rg.account_manifest_check(repo.cloud / "envs" / "account", "account", account_manifest,
                                  allow_removal="gr_treatment:old,gr_treatment:theirs")
    rg.account_manifest_check(repo.cloud / "envs" / "account", "account", account_manifest,
                              allow_removal=" gr_treatment:old ,")


def test_account_manifest_without_recorded_removals_is_refused(repo, account_manifest):
    os.chmod(account_manifest, 0o644)
    data = json.loads(account_manifest.read_text())
    del data["tag_value_removal"]
    account_manifest.write_text(json.dumps(data))
    with pytest.raises(rg.Refused, match="records no approved tag-value removals"):
        rg.account_manifest_check(repo.cloud / "envs" / "account", "account", account_manifest)


def test_account_manifest_write_refuses_removals_nobody_allowed(repo, monkeypatch):
    state = rg.root_state_paths(repo.cloud / "envs" / "account")["account"]
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"version": 4, "serial": 5, "lineage": "A", "resources": []}))
    plan_dir = _account_plan_dir(repo)
    (plan_dir / "tag_sync.json").write_text(json.dumps({"diff": [
        {"key": "gr_treatment", "added": [], "removed": ["old"]}], "allowed": [], "warehouse_id": ""}))
    prov = rg.account_provenance(repo.cloud / "envs" / "account")
    with pytest.raises(rg.Refused, match="doesn't remove exactly the values ALLOW_TAG_VALUE_REMOVAL names"):
        rg.account_manifest_write(repo.cloud / "envs" / "account", "account", plan_dir, prov)


def test_account_manifest_summary_puts_removals_first_and_loud(repo, account_manifest):
    lines = rg.account_manifest_summary(account_manifest, json.loads(account_manifest.read_text()))
    text = "\n".join(lines)
    assert "!! TAG VALUES THIS MANIFEST REMOVES (from every workspace on the metastore):" in text
    assert "!!   gr_treatment: ['old']" in text
    assert "Tag values added: gr_treatment: ['new']" in text
    assert "left in place (live, not desired here; never removed): gr_treatment: 1 value(s) ['theirs']" in text
    assert text.index("REMOVES") < text.index("Tag values added")


@pytest.mark.parametrize("text, expected", [
    ("", []), ("k:b,k:a, k:a ", ["k:a", "k:b"]), ("k:a:b", ["k:a:b"]),
])
def test_canonical_removals_is_sorted_and_deduplicated(text, expected):
    assert rg.canonical_removals(text) == expected


@pytest.mark.parametrize("text", ["novalue", ":v", "k:"])
def test_canonical_removals_refuses_malformed_items(text):
    with pytest.raises(rg.Refused, match="is not <key>:<value>"):
        rg.canonical_removals(text)


def test_account_manifest_refuses_already_receipted_manifest(repo, account_manifest):
    rg.account_receipt(repo.cloud / "envs" / "account", "account", account_manifest)
    with pytest.raises(rg.Refused, match="single-use"):
        rg.account_manifest_check(repo.cloud / "envs" / "account", "account", account_manifest)


def test_account_receipt_records_manifest_and_post_apply_state(repo, account_manifest):
    path = rg.account_receipt(repo.cloud / "envs" / "account", "account", account_manifest)
    receipt = json.loads(path.read_text())
    assert receipt["kind"] == "account"
    assert receipt["commit"] == repo.head()
    assert receipt["manifest"]["sha256"] == rg.sha256_file(account_manifest)
    assert receipt["state_serial"] == {"serial": 5, "lineage": "A"}


# -- manifests ------------------------------------------------------------------------

def _plan_dir(repo: Repo, *, final=True) -> Path:
    plan_dir = rg.new_plan_dir(repo.prod, "prod")
    (plan_dir / "account.tfplan").write_bytes(b"account plan")
    (plan_dir / "account.json").write_text('{"file": "account.tfplan", "changes": false}')
    (plan_dir / "workspace.tfplan").write_bytes(b"workspace plan")
    (plan_dir / "workspace.json").write_text('{"file": "workspace.tfplan", "changes": true}')
    da = plan_dir / "data_access"
    da.mkdir()
    (da / "00-grant.tfplan").write_bytes(b"grant plan")
    (da / "phases.json").write_text(json.dumps({
        "transition": {"from": "a", "to": "a", "retain": "", "retire": "", "tier_move": False,
                       "phases": ["create", "grant"], "completed": [], "resumed": False},
        "steps": [{"phase": "grant", "kind": "apply", "file": "00-grant.tfplan",
                   "sha256": rg.sha256_file(da / "00-grant.tfplan"), "changes": True}],
        "final": final}))
    return plan_dir


@pytest.fixture
def manifest(repo):
    state = rg.root_state_paths(repo.prod)["data_access"]
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"version": 4, "serial": 7, "lineage": "L", "resources": []}))
    ro.begin(repo.prod)
    ro.seal(repo.prod)
    rehearse(repo)
    prov = rg.provenance(repo.prod, "prod")
    path = rg.manifest_write(repo.prod, "prod", _plan_dir(repo), prov, "live-1", {"ok": True})
    return path


def test_release_plan_writes_one_immutable_manifest(repo, manifest):
    data = json.loads(manifest.read_text())
    assert data["commit"] == repo.head() and data["live_snapshot"] == "live-1" and data["final"] is True
    assert data["state_serials"]["data_access"] == {"serial": 7, "lineage": "L"}
    assert data["state_serials"]["account"] is None and data["state_serials"]["workspace"] is None
    assert data["plans"]["account"] == {"file": "account.tfplan", "sha256": rg.sha256_bytes(b"account plan"),
                                        "changes": False}
    assert data["plans"]["data_access"]["steps"][0]["sha256"] == rg.sha256_bytes(b"grant plan")
    assert data["effective_digest"] == ro.digest(repo.prod) and data["kept_names"] == {}
    # Saved plans carry the deployer's variables: owner-only, never writable.
    for path in [manifest.parent, *manifest.parent.rglob("*")]:
        assert path.stat().st_mode & 0o077 == 0, path
        if path.is_file():
            assert not os.access(path, os.W_OK), path
    assert not list(manifest.parent.parent.glob(".pending-*"))
    assert rg.manifest_check(repo.prod, "prod", manifest, live="live-1")["id"] == data["id"]


def test_release_manifest_write_accepts_local_only_committed_head(repo):
    repo.git("remote", "remove", "origin")
    repo.commit({"aws/envs/prod/ack.txt": "# local only\n"}, push=False)
    state = rg.root_state_paths(repo.prod)["data_access"]
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"version": 4, "serial": 7, "lineage": "L", "resources": []}))
    ro.begin(repo.prod)
    ro.seal(repo.prod)
    rehearse(repo)
    prov = rg.provenance(repo.prod, "prod")
    manifest = rg.manifest_write(repo.prod, "prod", _plan_dir(repo), prov, "live-1", {"ok": True})
    assert json.loads(manifest.read_text())["provenance"]["remote"] == repo.head()
    assert rg.manifest_check(repo.prod, "prod", manifest, live="live-1")["commit"] == repo.head()


def test_manifest_refused_when_reports_refused(repo):
    rehearse(repo)
    prov = rg.provenance(repo.prod, "prod")
    with pytest.raises(rg.Refused, match="reader or weakening check refused"):
        rg.manifest_write(repo.prod, "prod", _plan_dir(repo), prov, "live", {"ok": False})


def test_manifest_refused_when_the_account_plan_is_not_empty(repo):
    rehearse(repo)
    prov = rg.provenance(repo.prod, "prod")
    plan_dir = _plan_dir(repo)
    (plan_dir / "account.json").write_text('{"file": "account.tfplan", "changes": true}')
    with pytest.raises(rg.Refused, match="make apply-account"):
        rg.manifest_write(repo.prod, "prod", plan_dir, prov, "live", {"ok": True})


def test_manifest_drift_commit(repo, manifest):
    repo.commit({"aws/envs/prod/ack.txt": "# later\n"})
    with pytest.raises(rg.Refused, match="the manifest planned"):
        rg.manifest_check(repo.prod, "prod", manifest, live="live-1")


def test_manifest_drift_dirty_tree(repo, manifest):
    (repo.prod / "env.auto.tfvars").write_text(PROD_ENV + "# edited\n")
    with pytest.raises(rg.Refused, match="checkout is not clean"):
        rg.manifest_check(repo.prod, "prod", manifest, live="live-1")


@pytest.mark.parametrize("root", ["data_access", "workspace", "account"])
def test_manifest_drift_state_serial(repo, manifest, root):
    path = rg.root_state_paths(repo.prod)[root]
    path.parent.mkdir(parents=True, exist_ok=True)
    state = json.loads(path.read_text()) if path.is_file() else {"version": 4, "serial": 0, "lineage": "L"}
    state["serial"] = state["serial"] + 1
    path.write_text(json.dumps(state))
    with pytest.raises(rg.Refused, match=f"the {root} state changed since release-plan"):
        rg.manifest_check(repo.prod, "prod", manifest, live="live-1")


@pytest.mark.parametrize("plan", ["account.tfplan", "workspace.tfplan", "data_access/00-grant.tfplan"])
def test_manifest_drift_plan_hash(repo, manifest, plan):
    path = manifest.parent / plan
    os.chmod(path, 0o644)
    path.write_bytes(b"a different plan")
    with pytest.raises(rg.Refused, match="changed or is missing"):
        rg.manifest_check(repo.prod, "prod", manifest, live="live-1")


def test_manifest_drift_live_snapshot(repo, manifest):
    with pytest.raises(rg.Refused, match="live Unity Catalog changed"):
        rg.manifest_check(repo.prod, "prod", manifest, live="live-2")


# A derive that prints progress to stdout, as derive_assignments.py does.
_CHATTY_DERIVE = '''
import os, sys
from pathlib import Path
args = sys.argv[1:]
out, ddl = Path(args[args.index("--out-dir") + 1]), Path(args[args.index("--write-ddl") + 1])
print("Loaded environment from env.auto.tfvars")
print("[SENSITIVITY] prodcat.s.t.ssn -> pii")
print("Derived 9 gr_treatment assignment(s)", flush=True)
if os.environ.get("FAKE_DERIVE_FAIL"):
    sys.exit(3)
(out / "abac.auto.tfvars").write_text("fgac_policies = []\\n")
(out / "masking_functions.sql").write_text("-- masks\\n")
ddl.write_text(os.environ.get("FAKE_LIVE_DDL", "CREATE TABLE prodcat.s.t (ssn STRING);\\n"))
'''

# The real release_gate.py CLI, with only the derive script and the live policy lister swapped.
_LIVE_SNAPSHOT_CLI = '''
import functools, sys
sys.path[:0] = [sys.argv[1], sys.argv[1] + "/scripts"]
from scripts import release_gate as rg
rg.live_policies = lambda env_dir, catalogs: {}
rg.live_snapshot = functools.partial(rg.live_snapshot, derive_script=sys.argv[2])
sys.exit(rg.main(sys.argv[3:]))
'''


def _live_snapshot_cli(env_dir: Path, tmp_path: Path, *, rederive: bool = True, **env) -> subprocess.CompletedProcess:
    derive = tmp_path / "chatty_derive.py"
    derive.write_text(_CHATTY_DERIVE)
    return subprocess.run(
        [sys.executable, "-c", _LIVE_SNAPSHOT_CLI, str(SHARED), str(derive),
         "live-snapshot", "--env-dir", str(env_dir), "--env", "prod", *(["--rederive"] if rederive else [])],
        text=True, capture_output=True, env={**os.environ, **env}, check=False)


def _expected_digest(tmp_path: Path, ddl: str = "CREATE TABLE prodcat.s.t (ssn STRING);\n") -> str:
    want = tmp_path / "want"
    want.mkdir(exist_ok=True)
    (want / "abac.auto.tfvars").write_text("fgac_policies = []\n")
    (want / "masking_functions.sql").write_text("-- masks\n")
    (want / "_fetched.sql").write_text(ddl)
    return rg.snapshot_digest(want / "abac.auto.tfvars", want / "masking_functions.sql", want / "_fetched.sql", {})


def test_rederived_live_snapshot_stdout_is_exactly_the_digest(repo, tmp_path):
    # make release captures live="$(release_gate.py live-snapshot --rederive)": the derive's
    # progress must not land in it, or it never equals the manifest's digest.
    result = _live_snapshot_cli(repo.prod, tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout == _expected_digest(tmp_path) + "\n"
    assert "Derived 9 gr_treatment" in result.stderr  # diagnostics are kept, on stderr


def test_rederived_live_snapshot_fails_closed_when_the_derive_fails(repo, tmp_path):
    result = _live_snapshot_cli(repo.prod, tmp_path, FAKE_DERIVE_FAIL="1")
    assert result.returncode == 1 and result.stdout == ""
    assert "re-reading live UC for the live snapshot failed" in result.stderr
    assert "Loaded environment from" in result.stderr


def test_release_rederive_matches_the_release_plan_snapshot_and_a_live_change_refuses(repo, manifest, tmp_path):
    # release-plan digests what the last derive wrote; release re-derives into a scratch dir.
    ro.effective(repo.prod, "abac.auto.tfvars").write_text("fgac_policies = []\n")
    ro.effective(repo.prod, "masking_functions.sql").write_text("-- masks\n")
    (repo.prod / "ddl").mkdir(exist_ok=True)
    (repo.prod / "ddl" / "_fetched.sql").write_text("CREATE TABLE prodcat.s.t (ssn STRING);\n")
    planned = _live_snapshot_cli(repo.prod, tmp_path, rederive=False)
    assert planned.returncode == 0 and planned.stdout == _expected_digest(tmp_path) + "\n"
    live = planned.stdout.removesuffix("\n")  # what $(...) keeps
    time.sleep(1.1)  # manifest ids are per second
    current = rg.manifest_write(repo.prod, "prod", _plan_dir(repo), rg.provenance(repo.prod, "prod"), live,
                                {"ok": True})
    unchanged = _live_snapshot_cli(repo.prod, tmp_path)
    assert unchanged.returncode == 0, unchanged.stderr
    rg.manifest_check(repo.prod, "prod", current, live=unchanged.stdout.removesuffix("\n"))
    changed = _live_snapshot_cli(repo.prod, tmp_path, FAKE_LIVE_DDL="CREATE TABLE prodcat.s.t (ssn INT);\n")
    assert changed.returncode == 0 and changed.stdout == _expected_digest(
        tmp_path, "CREATE TABLE prodcat.s.t (ssn INT);\n") + "\n"
    with pytest.raises(rg.Refused, match="live Unity Catalog changed"):
        rg.manifest_check(repo.prod, "prod", current, live=changed.stdout.removesuffix("\n"))


def test_manifest_drift_effective_config(repo, manifest):
    (ro.overlay_dir(repo.prod) / "abac.auto.tfvars").write_text(EMPTY_ABAC + "# re-derived since\n")
    with pytest.raises(rg.Refused, match="effective config"):
        rg.manifest_check(repo.prod, "prod", manifest, live="live-1")


def test_manifest_drift_kept_names(repo, manifest):
    (ro.overlay_dir(repo.prod) / ro.KEPT).write_text('{"gr_mask_prod_cat_x_full": "gr_mask_dev_cat_x_full"}')
    with pytest.raises(rg.Refused, match="kept policy names"):
        rg.manifest_check(repo.prod, "prod", manifest, live="live-1")


def test_manifest_for_another_env_is_refused(repo, manifest):
    with pytest.raises(rg.Refused, match="manifest for prod, not dev"):
        rg.manifest_check(repo.dev, "dev", manifest)


# -- the current manifest (plain make release) ------------------------------------------

def _second_manifest(repo: Repo) -> Path:
    time.sleep(1.1)  # manifest ids are per second
    return rg.manifest_write(repo.prod, "prod", _plan_dir(repo), rg.provenance(repo.prod, "prod"), "live-1",
                             {"ok": True})


def test_release_plan_records_its_manifest_as_the_current_one(repo, manifest):
    assert rg.current_manifest(repo.prod, "prod") == manifest
    pointer = rg.current_pointer(repo.cloud, "prod")
    assert pointer.parent == rg.manifests_dir(repo.cloud, "prod")  # under the state dir, next to the manifests
    assert json.loads(pointer.read_text())["id"] == json.loads(manifest.read_text())["id"]
    assert pointer.stat().st_mode & 0o077 == 0


def test_the_current_manifest_lives_in_the_state_dir(repo, manifest):
    state = Path(os.environ[rg.STATE_DIR_ENV])
    assert rg.current_pointer(repo.cloud, "prod") == state / repo.cloud.name / "prod" / "manifests" / "CURRENT"
    assert not list((repo.cloud / "envs" / "prod").rglob("CURRENT"))  # nothing in the checkout


def test_no_current_manifest_refuses_with_a_pointer_to_release_plan(repo):
    with pytest.raises(rg.Refused, match="has no current release manifest. Run make release-plan ENV=prod"):
        rg.current_manifest(repo.prod, "prod")


def test_a_newer_release_plan_supersedes_the_current_manifest(repo, manifest):
    newer = _second_manifest(repo)
    assert rg.current_manifest(repo.prod, "prod") == newer
    with pytest.raises(rg.Refused, match="was superseded by a newer make release-plan"):
        rg.manifest_check(repo.prod, "prod", manifest, live="live-1")
    assert rg.manifest_check(repo.prod, "prod", newer, live="live-1")["id"] == json.loads(newer.read_text())["id"]


@pytest.mark.parametrize("how", ["release", "phase"])
def test_a_released_manifest_is_never_released_again(repo, manifest, how):
    (rg.release_receipt if how == "release" else rg.phase_record)(repo.prod, "prod", manifest)
    with pytest.raises(rg.Refused, match="was already released .*Run make release-plan ENV=prod"):
        rg.current_manifest(repo.prod, "prod")
    with pytest.raises(rg.Refused, match="was already released"):
        rg.manifest_check(repo.prod, "prod", manifest, live="live-1")


def test_a_manifest_from_before_the_current_pointer_still_releases_explicitly(repo, manifest):
    rg.current_pointer(repo.cloud, "prod").unlink()
    assert rg.manifest_check(repo.prod, "prod", manifest, live="live-1")
    with pytest.raises(rg.Refused, match="has no current release manifest"):
        rg.current_manifest(repo.prod, "prod")


@pytest.mark.parametrize("value", ['{"id": "../elsewhere"}', '{"id": ""}', '{}'])
def test_a_malformed_current_pointer_refuses(repo, manifest, value):
    pointer = rg.current_pointer(repo.cloud, "prod")
    pointer.write_text(value)
    with pytest.raises(rg.Refused, match="names no manifest"):
        rg.current_manifest(repo.prod, "prod")


def test_a_missing_current_manifest_refuses(repo, manifest):
    rg.current_pointer(repo.cloud, "prod").write_text('{"id": "20990101T000000-000000000000"}')
    with pytest.raises(rg.Refused, match="is missing. Run make release-plan ENV=prod"):
        rg.current_manifest(repo.prod, "prod")


def test_the_summary_names_path_commit_and_changes(repo, manifest):
    data = json.loads(manifest.read_text())
    lines = rg.manifest_summary(manifest, data)
    assert lines[0] == f"  Manifest: {manifest}"
    assert lines[1].startswith(f"  Commit:   {repo.head()} (planned ")
    assert "    data_access:  saved plan(s) with changes, phase(s) grant" in lines
    assert "    workspace:    saved plan with changes" in lines


# -- release receipt and maintain ----------------------------------------------------

def test_release_receipt_records_commit_digests_and_manifest(repo, manifest):
    path = rg.release_receipt(repo.prod, "prod", manifest)
    receipt = json.loads(path.read_text())
    assert receipt["commit"] == repo.head()
    assert receipt["manifest"]["sha256"] == rg.sha256_file(manifest)
    assert receipt["digests"]["generated"] == rg.generated_digest(repo.prod / "generated")
    assert receipt["digests"]["live_snapshot"] == "live-1"
    assert rg.maintain_check(repo.prod, "prod")["commit"] == repo.head()


def test_maintain_refuses_without_a_release_receipt(repo):
    with pytest.raises(rg.Refused, match="no release receipt yet"):
        rg.maintain_check(repo.prod, "prod")


def test_maintain_refuses_from_a_commit_other_than_the_last_release(repo, manifest):
    rg.release_receipt(repo.prod, "prod", manifest)
    repo.commit({"aws/envs/prod/ack.txt": "# newer, unreleased\n"})
    with pytest.raises(rg.Refused, match="but the last release is"):
        rg.maintain_check(repo.prod, "prod")


def test_maintain_refuses_mismatched_digests(repo, manifest):
    released = json.loads(rg.release_receipt(repo.prod, "prod", manifest).read_text())
    forged = released | {"digests": released["digests"] | {"generated": "0" * 64}, "created_at": rg._now()}
    rg._write_once(rg.release_dir(repo.cloud, "prod") / "zz-forged.json", forged)
    with pytest.raises(rg.Refused, match="generated digest differs"):
        rg.maintain_check(repo.prod, "prod")


def test_maintain_refuses_a_dirty_checkout(repo, manifest):
    rg.release_receipt(repo.prod, "prod", manifest)
    (repo.root / "shared" / "scripts" / "code.py").write_text("x = 'hotfix'\n")
    with pytest.raises(rg.Refused, match="not clean"):
        rg.maintain_check(repo.prod, "prod")


def test_a_phase_manifest_is_recorded_on_the_final_release_receipt(repo, manifest):
    rg.phase_record(repo.prod, "prod", manifest)
    receipt = json.loads(rg.release_receipt(repo.prod, "prod", manifest).read_text())
    assert receipt["phase_manifests"] == [json.loads(manifest.read_text())["id"]]


# -- the final receipt's phase manifests: one release, whatever commit planned each phase -----------

def _journal(repo: Repo, *, open_run: str = "", history=()) -> None:
    journal = spa.empty_journal()
    journal.update(transition={"run_id": open_run} if open_run else None, history=list(history))
    spa.save_journal(repo.prod, journal)


def _finished(run: str) -> dict:
    return {"run_id": run, "commit": "c", "from": "a", "to": "b", "retired": "", "phases": [],
            "finished_at": rg._now()}


def _abandoned(run: str) -> dict:
    return {"run_id": run, "abandoned": "b", "at": rg._now()}


def _engine_fix(repo: Repo, n: int) -> Path:
    """An engine fix merges mid-release; the next phase is planned at the new commit."""
    repo.commit({"shared/scripts/code.py": f"x = {n}\n"})
    rehearse(repo)
    return _second_manifest(repo)


def _id(path: Path) -> str:
    return json.loads(Path(path).read_text())["id"]


def _receipt(repo: Repo, final: Path) -> dict:
    return json.loads(rg.release_receipt(repo.prod, "prod", final).read_text())


def test_the_final_receipt_links_every_phase_of_the_release_whatever_commit_planned_it(repo, manifest):
    _journal(repo, open_run="run1")
    rg.phase_record(repo.prod, "prod", manifest)  # planned at commit A
    second = _engine_fix(repo, 2)
    rg.phase_record(repo.prod, "prod", second)  # planned at commit B
    final = _engine_fix(repo, 3)  # the final phase, at commit C
    _journal(repo, history=[_finished("run1")])
    receipt = _receipt(repo, final)
    assert receipt["phase_manifests"] == [_id(manifest), _id(second)]
    assert receipt["phase_manifests_unlinked"] == []
    record = json.loads((rg.env_state(repo.cloud, "prod") / "receipts" / "phases" / f"{_id(second)}.json")
                        .read_text())
    assert record["policy_run"] == "run1" and record["manifest_sha256"] == rg.sha256_file(second)


def test_an_earlier_releases_phases_are_not_linked_to_the_next_release(repo, manifest):
    _journal(repo, open_run="run1")
    rg.phase_record(repo.prod, "prod", manifest)
    first_final = _engine_fix(repo, 2)
    _journal(repo, history=[_finished("run1")])
    first = _receipt(repo, first_final)
    assert first["phase_manifests"] == [_id(manifest)]
    time.sleep(1.1)  # receipts and records are timestamped per second
    _journal(repo, open_run="run2", history=[_finished("run1")])
    phase = _engine_fix(repo, 3)
    rg.phase_record(repo.prod, "prod", phase)
    final = _engine_fix(repo, 4)
    _journal(repo, history=[_finished("run1"), _finished("run2")])
    receipt = _receipt(repo, final)
    assert receipt["phase_manifests"] == [_id(phase)]
    assert receipt["phase_manifests_unlinked"] == []
    assert first["release_epoch"] and receipt["release_epoch"] not in (None, first["release_epoch"])
    assert [e["id"] for e in receipt["phase_manifests_earlier"]] == [_id(manifest)]


@pytest.mark.parametrize("ended, reason", [
    (_abandoned, "run run1, which was abandoned"),
    (None, "run run1, which the policy journal doesn't record as finished"),
])
def test_an_abandoned_or_unfinished_runs_phases_are_not_linked(repo, manifest, ended, reason):
    _journal(repo, open_run="run1")
    rg.phase_record(repo.prod, "prod", manifest)  # run1's first phase; then make policy-abandon
    history = [ended("run1")] if ended else []
    _journal(repo, open_run="run2", history=history)
    retry = _second_manifest(repo)  # the same commit: a commit filter can't tell the runs apart
    rg.phase_record(repo.prod, "prod", retry)
    final = _second_manifest(repo)
    _journal(repo, history=history + [_finished("run2")])
    receipt = _receipt(repo, final)
    assert receipt["phase_manifests"] == [_id(retry)]
    [unlinked] = receipt["phase_manifests_unlinked"]
    assert unlinked["id"] == _id(manifest) and reason in unlinked["reason"]


def _tamper_manifest(repo: Repo, phase: Path, record: Path) -> None:
    os.chmod(phase, 0o600)
    phase.write_text(phase.read_text().replace('"live-1"', '"live-forged"'))


def _tamper_record_id(repo: Repo, phase: Path, record: Path) -> None:
    data = json.loads(record.read_text()) | {"id": _id(phase).replace("-", "_")}
    os.chmod(record, 0o600)
    record.write_text(json.dumps(data))


def _tamper_unreadable(repo: Repo, phase: Path, record: Path) -> None:
    os.chmod(record, 0o600)
    record.write_text("{not json")


def _tamper_legacy(repo: Repo, phase: Path, record: Path) -> None:
    data = {k: v for k, v in json.loads(record.read_text()).items() if k not in ("manifest_sha256", "policy_run")}
    os.chmod(record, 0o600)
    record.write_text(json.dumps(data))


@pytest.mark.parametrize("tamper, reason", [
    (_tamper_manifest, "doesn't match the hash the record holds"),
    (_tamper_record_id, "but is stored as"),
    (_tamper_unreadable, "the phase record is unreadable"),
    (_tamper_legacy, "written before phase records held their manifest's hash"),
])
def test_a_phase_record_that_does_not_verify_is_listed_unlinked_never_linked(repo, manifest, tamper, reason):
    _journal(repo, open_run="run1")
    rg.phase_record(repo.prod, "prod", manifest)
    second = _engine_fix(repo, 2)
    record = rg.env_state(repo.cloud, "prod") / "receipts" / "phases" / f"{_id(second)}.json"
    rg.phase_record(repo.prod, "prod", second)
    tamper(repo, second, record)
    final = _engine_fix(repo, 3)
    _journal(repo, history=[_finished("run1")])
    receipt = _receipt(repo, final)
    assert receipt["phase_manifests"] == [_id(manifest)]
    [unlinked] = receipt["phase_manifests_unlinked"]
    assert unlinked["id"] == _id(second) and reason in unlinked["reason"]
    assert unlinked["record"] == str(record)


def test_an_unreadable_journal_still_records_the_applied_phase_but_never_links_it(repo, manifest):
    rg.phase_record(repo.prod, "prod", manifest)  # no journal yet: no policy run was open
    journal = spa.journal_path(repo.prod)
    journal.parent.mkdir(parents=True, exist_ok=True)
    journal.write_text("{not json")
    second = _engine_fix(repo, 2)
    record = json.loads(rg.phase_record(repo.prod, "prod", second).read_text())  # the apply already happened
    assert record["policy_run"] is None
    _journal(repo, history=[_finished("run1")])
    final = _engine_fix(repo, 3)
    receipt = _receipt(repo, final)
    assert receipt["phase_manifests"] == [_id(manifest)]
    [unlinked] = receipt["phase_manifests_unlinked"]
    assert unlinked["id"] == _id(second) and "policy run is unknown" in unlinked["reason"]


def test_a_deleted_earlier_receipt_does_not_let_its_phases_into_the_next_release(repo, manifest):
    _journal(repo, open_run="run1")
    rg.phase_record(repo.prod, "prod", manifest)  # phase A of release 1
    first_final = _engine_fix(repo, 2)
    _journal(repo, history=[_finished("run1")])
    first = rg.release_receipt(repo.prod, "prod", first_final)
    first.unlink()  # lost, or deleted by hand
    _journal(repo, open_run="run2", history=[_finished("run1")])
    phase = _engine_fix(repo, 3)
    rg.phase_record(repo.prod, "prod", phase)  # phase B of release 2
    final = _engine_fix(repo, 4)
    _journal(repo, history=[_finished("run1"), _finished("run2")])
    receipt = _receipt(repo, final)
    assert receipt["phase_manifests"] == [_id(phase)]
    [unlinked] = receipt["phase_manifests_unlinked"]
    assert unlinked["id"] == _id(manifest) and "another release's phase" in unlinked["reason"]


def test_a_foreign_receipt_neither_accounts_for_nor_hides_a_phase(repo, manifest):
    _journal(repo, open_run="run1")
    rg.phase_record(repo.prod, "prod", manifest)
    final = _engine_fix(repo, 2)
    time.sleep(1.1)  # the foreign receipt is newer than the phase
    foreign = rg._write_once(rg.release_dir(repo.cloud, "prod") / "zz-dev.json", {
        "kind": "release", "version": rg.VERSION, "env": "dev", "cloud": repo.cloud.name, "commit": repo.head(),
        "manifest": {"id": _id(manifest), "sha256": rg.sha256_file(manifest)},
        "phase_manifests": [_id(manifest)], "created_at": rg._now()})
    _journal(repo, history=[_finished("run1")])
    receipt = _receipt(repo, final)
    assert receipt["phase_manifests"] == [_id(manifest)]
    assert receipt["phase_manifests_unlinked"] == []
    assert receipt["release_receipts_ignored"] == [
        {"receipt": str(foreign), "reason": "a receipt of aws/dev, not aws/prod"}]


def test_a_clock_that_went_backwards_does_not_hide_a_phase(repo, manifest, monkeypatch):
    rg.release_receipt(repo.prod, "prod", manifest)  # release 1
    _journal(repo, open_run="run2")
    phase = _engine_fix(repo, 2)
    with monkeypatch.context() as m:
        m.setattr(rg, "_now", lambda: "2000-01-01T00:00:00+00:00")  # before release 1's receipt
        rg.phase_record(repo.prod, "prod", phase)
    final = _engine_fix(repo, 3)
    _journal(repo, history=[_finished("run2")])
    receipt = _receipt(repo, final)
    assert receipt["phase_manifests"] == [_id(phase)]
    assert receipt["phase_manifests_unlinked"] == []


def _phases_now(repo: Repo) -> tuple[list[str], list[dict], list[dict], list[dict]]:
    return rg.release_phases(repo.prod, "prod", rg.release_epoch(repo.cloud, "prod"))


def _rewrite(path: Path, change) -> None:
    data = json.loads(path.read_text())
    change(data)
    os.chmod(path, 0o600)
    path.write_text(json.dumps(data))


def _every_record_once(repo: Repo, linked, unlinked, earlier) -> None:
    directory = rg.env_state(repo.cloud, "prod") / "receipts" / "phases"
    found = sorted(p.stem if p.parent == directory else p.relative_to(directory).as_posix()
                   for p in directory.rglob("*.json"))
    assert sorted(linked + [e["id"] for e in unlinked + earlier]) == found


def test_a_copy_of_a_phase_record_elsewhere_is_listed_once_and_never_links(repo, manifest):
    _journal(repo, open_run="run1")
    record = rg.phase_record(repo.prod, "prod", manifest)
    directory = record.parent
    (directory / "duplicate").mkdir()
    copy = directory / "duplicate" / record.name
    copy.write_bytes(record.read_bytes())  # same id, same bytes
    stray = directory / "nested" / "other.json"
    stray.parent.mkdir()
    stray.write_text(json.dumps(json.loads(record.read_text()) | {"id": "other"}))
    final = _engine_fix(repo, 2)
    _journal(repo, history=[_finished("run1")])
    receipt = _receipt(repo, final)
    linked, unlinked, earlier = (receipt["phase_manifests"], receipt["phase_manifests_unlinked"],
                                 receipt["phase_manifests_earlier"])
    assert linked == [_id(manifest)] and earlier == []
    assert [(u["id"], u["record"], u["record_sha256"]) for u in unlinked] == [
        (f"duplicate/{record.name}", str(copy), rg.sha256_file(copy)),
        ("nested/other.json", str(stray), rg.sha256_file(stray))]
    assert unlinked[0]["reason"].startswith(f"duplicate record id {_id(manifest)} at {copy}")
    assert "isn't where phase records are written" in unlinked[1]["reason"]
    ids = linked + [e["id"] for e in unlinked + earlier]
    assert len(ids) == len(set(ids)) and ids.count(_id(manifest)) == 1
    _every_record_once(repo, linked, unlinked, earlier)
    # The next release reports each anomaly once, as accounted for by this receipt, and links nothing.
    linked, unlinked, earlier, _ = _phases_now(repo)
    assert (linked, unlinked) == ([], [])
    assert sorted(e["id"] for e in earlier) == sorted([_id(manifest), f"duplicate/{record.name}", "nested/other.json"])
    _every_record_once(repo, linked, unlinked, earlier)


@pytest.mark.parametrize("listed_as", ["phase_manifests", "phase_manifests_unlinked"])
def test_an_earlier_receipt_naming_a_phase_of_another_release_does_not_hide_it(repo, manifest, listed_as):
    _journal(repo, open_run="run1")
    rg.phase_record(repo.prod, "prod", manifest)  # release 1's phase
    first_final = _engine_fix(repo, 2)
    _journal(repo, history=[_finished("run1")])
    first = rg.release_receipt(repo.prod, "prod", first_final)
    _journal(repo, open_run="run2", history=[_finished("run1")])
    phase = _engine_fix(repo, 3)
    record = rg.phase_record(repo.prod, "prod", phase)  # X: release 2's phase
    entry = {"id": _id(phase), "record": str(record), "record_sha256": rg.sha256_file(record), "reason": "forged"}
    # The older receipt still verifies (env, kind, version, its manifest's hash) but names X.
    _rewrite(first, lambda r: r[listed_as].append(_id(phase) if listed_as == "phase_manifests" else entry))
    assert rg._receipt_problem(repo.cloud, "prod", json.loads(first.read_text())) == ""
    _journal(repo, history=[_finished("run1"), _finished("run2")])
    linked, unlinked, earlier, ignored = _phases_now(repo)
    assert linked == [_id(phase)]
    assert unlinked == [] and ignored == []
    assert earlier == [{"id": _id(manifest), "record": str(rg.env_state(repo.cloud, "prod") / "receipts" / "phases"
                                                          / f"{_id(manifest)}.json"), "receipt": str(first)}]
    _every_record_once(repo, linked, unlinked, earlier)


def test_a_record_without_identity_is_reported_unlinked_once_not_on_every_release(repo, manifest):
    rg.phase_record(repo.prod, "prod", manifest)
    record = rg.env_state(repo.cloud, "prod") / "receipts" / "phases" / f"{_id(manifest)}.json"
    _rewrite(record, lambda r: [r.pop(k) for k in ("manifest_sha256", "policy_run", "release_epoch")])  # pre-upgrade
    first = _receipt(repo, _engine_fix(repo, 2))
    assert [u["id"] for u in first["phase_manifests_unlinked"]] == [_id(manifest)]
    second = rg.release_receipt(repo.prod, "prod", _engine_fix(repo, 3))
    receipt = json.loads(second.read_text())
    assert receipt["phase_manifests_unlinked"] == [] and receipt["phase_manifests"] == []
    assert [e["id"] for e in receipt["phase_manifests_earlier"]] == [_id(manifest)]
    _rewrite(record, lambda r: r.update(commit="0" * 40))  # changed since it was reported: reported again
    linked, unlinked, earlier, _ = _phases_now(repo)
    assert [u["id"] for u in unlinked] == [_id(manifest)] and earlier == []


def test_a_receipt_from_before_release_epochs_accounts_for_the_records_it_linked(repo, manifest):
    rg.phase_record(repo.prod, "prod", manifest)
    record = rg.env_state(repo.cloud, "prod") / "receipts" / "phases" / f"{_id(manifest)}.json"
    _rewrite(record, lambda r: [r.pop(k) for k in ("manifest_sha256", "policy_run", "release_epoch")])
    final = _second_manifest(repo)  # same commit, as the old receipt chose its phases
    old = rg._write_once(rg.release_dir(repo.cloud, "prod") / "old.json", {
        "kind": "release", "version": rg.VERSION, "env": "prod", "cloud": repo.cloud.name, "commit": repo.head(),
        "manifest": {"id": _id(final), "sha256": rg.sha256_file(final)}, "phase_manifests": [_id(manifest)],
        "created_at": rg._now()})
    linked, unlinked, earlier, ignored = _phases_now(repo)
    assert (linked, unlinked, ignored) == ([], [], [])
    assert earlier == [{"id": _id(manifest), "record": str(record), "receipt": str(old)}]


# -- runtime overlay and kept policy names ----------------------------------------------

def test_overlay_is_used_only_when_current_and_deterministic(repo, monkeypatch):
    committed = repo.prod / "generated" / "abac.auto.tfvars"
    assert ro.effective(repo.prod, "abac.auto.tfvars") == committed
    ro.begin(repo.prod)
    assert ro.effective(repo.prod, "abac.auto.tfvars") == committed  # not sealed: not used
    ro.seal(repo.prod)
    assert ro.effective(repo.prod, "abac.auto.tfvars") == ro.overlay_dir(repo.prod) / "abac.auto.tfvars"
    monkeypatch.setenv(ro.NO_OVERLAY_ENV, "1")
    assert ro.effective(repo.prod, "abac.auto.tfvars") == committed
    monkeypatch.delenv(ro.NO_OVERLAY_ENV)
    committed.write_text(EMPTY_ABAC + "# a new commit\n")  # stale base: ignored
    assert ro.effective(repo.prod, "abac.auto.tfvars") == committed


def test_legacy_envs_never_use_an_overlay(repo):
    ro.begin(repo.prod)
    ro.seal(repo.prod)
    (repo.prod / "env.auto.tfvars").write_text('promote_from = "dev"\n')  # governance_mode unset: legacy
    assert ro.effective(repo.prod, "abac.auto.tfvars") == repo.prod / "generated" / "abac.auto.tfvars"
    assert ro.ensure(repo.prod) is False


def _policy(name: str, catalog: str = "prod_cat") -> dict:
    return {"name": name, "catalog": catalog, "policy_type": "POLICY_TYPE_COLUMN_MASK"}


def test_kept_names_keep_a_policy_prod_deploys_under_its_old_name():
    kept = ro.kept_policy_names([_policy("gr_mask_prod_cat_v_full")], {"dev_cat": "prod_cat"},
                                {("prod_cat", "gr_mask_dev_cat_v_full")}, lambda c: set())
    assert kept == {"gr_mask_prod_cat_v_full": "gr_mask_dev_cat_v_full"}
    live = ro.kept_policy_names([_policy("gr_mask_prod_cat_v_full")], {"dev_cat": "prod_cat"},
                                set(), lambda c: {"prod_cat_gr_mask_dev_cat_v_full"})
    assert live == kept
    assert ro.kept_policy_names([_policy("gr_mask_prod_cat_v_full")], {"dev_cat": "prod_cat"}, set(),
                                lambda c: set()) == {}


def test_kept_names_fail_closed_when_policies_cant_be_listed():
    with pytest.raises(ro.OverlayError, match="couldn't be listed"):
        ro.kept_policy_names([_policy("gr_mask_prod_cat_v_full")], {"dev_cat": "prod_cat"}, set(), lambda c: None)


def test_kept_names_refuse_a_collision_with_an_unmanaged_policy():
    with pytest.raises(ro.OverlayError, match="doesn't manage"):
        ro.kept_policy_names([_policy("gr_mask_prod_cat_v_full")], {"dev_cat": "prod_cat"}, set(),
                             lambda c: {"prod_cat_gr_mask_prod_cat_v_full"})


def test_kept_names_are_applied_to_the_overlay_never_to_committed_files(repo):
    model = build_model({"prod_cat.sch.t.email": ["email_address"]}, {"prod_cat.sch.t.email": "STRING"},
                        {"access_tier_groups": ["analysts", "viewers"]})
    committed = repo.prod / "generated" / "abac.auto.tfvars"
    generate_abac.write_deterministic_model(committed, committed.with_name("masking_functions.sql"), model)
    before = committed.read_text()
    name = model.fgac_policies[0]["name"]
    old = name.replace("prod_cat", "dev_cat")
    kept = ro.build_kept_names(repo.prod, state_keys={("prod_cat", old)}, live_names=lambda c: set())
    assert kept == {name: old}
    overlay = ro.overlay_dir(repo.prod) / "abac.auto.tfvars"

    def names(path):
        return [p["name"] for p in ro._load_hcl(path).get("fgac_policies") or []]

    assert names(overlay) == [old]
    assert committed.read_text() == before and names(committed) == [name]
    # A later materialization (no live derive) keeps the same names.
    ro.begin(repo.prod)
    assert ro.ensure(repo.prod)
    assert names(overlay) == [old]
    # Rebuilding from an overlay that already carries kept names is stable.
    assert ro.build_kept_names(repo.prod, state_keys={("prod_cat", old)}, live_names=lambda c: set()) == kept
    assert names(overlay) == [old]
    # Once prod no longer deploys the old name, the promoted name comes back.
    assert ro.build_kept_names(repo.prod, state_keys=set(), live_names=lambda c: set()) == {}
    assert names(overlay) == [name]


def test_kept_names_failure_unseals_the_overlay(repo, monkeypatch):
    model = build_model({"prod_cat.sch.t.email": ["email_address"]}, {"prod_cat.sch.t.email": "STRING"},
                        {"access_tier_groups": ["analysts", "viewers"]})
    committed = repo.prod / "generated" / "abac.auto.tfvars"
    generate_abac.write_deterministic_model(committed, committed.with_name("masking_functions.sql"), model)
    ro.begin(repo.prod)
    ro.seal(repo.prod)
    monkeypatch.setattr("remap_generated_config.live_policy_lister", lambda auth: (lambda c: None))
    assert ro.main(["kept-names", "--env-dir", str(repo.prod)]) == 1
    assert not ro.active(repo.prod)


# -- derive into the overlay, pure promote, verify keys ---------------------------------

def test_derive_out_dir_never_rewrites_committed_files(repo, monkeypatch):
    import derive_assignments as da

    committed = repo.prod / "generated" / "abac.auto.tfvars"
    before = committed.read_text()
    seen = {}

    def fake(config, auth, env, ddl_out=None, *, record_governed=True, classes_out=None):
        seen.update(config=config, record_governed=record_governed)
        config.write_text(config.read_text() + "# derived from live tags\n")
        return 1

    monkeypatch.setattr(da, "derive_assignments", fake)
    out = ro.begin(repo.prod)
    monkeypatch.chdir(repo.prod)
    assert da.main(["--config", "generated/abac.auto.tfvars", "--out-dir", str(out)]) == 0
    assert committed.read_text() == before
    assert seen == {"config": out / "abac.auto.tfvars", "record_governed": False}
    assert (out / "abac.auto.tfvars").read_text().endswith("# derived from live tags\n")


def test_deterministic_promote_is_pure():
    from remap_generated_config import remap_hcl, remap_policy_names

    source = ('fgac_policies = [\n  {\n    name = "gr_mask_dev_cat_v_full"\n    catalog = "dev_cat"\n'
              '    policy_type = "POLICY_TYPE_COLUMN_MASK"\n  },\n]\n')
    pairs = [("dev_cat", "prod_cat")]
    deployed = {("prod_cat", "gr_mask_dev_cat_v_full")}
    text, _ = remap_policy_names(source, remap_hcl(source, pairs), pairs, deployed, lambda c: None, pure=True)
    assert 'name = "gr_mask_prod_cat_v_full"' in text
    # Legacy promote still keeps a deployed name.
    text, _ = remap_policy_names(source, remap_hcl(source, pairs), pairs, deployed, lambda c: None)
    assert 'name = "gr_mask_dev_cat_v_full"' in text


def test_deterministic_verify_keys_are_reported_not_written(tmp_path, capsys):
    from saved_settings import report_verify_keys

    env_file = tmp_path / "env.auto.tfvars"
    env_file.write_text('governance_mode = "deterministic"\n')
    result = tmp_path / "result.json"
    result.write_text(json.dumps({"passed": True, "masked_tables": ["c.s.t"], "mask_keys_complete": True,
                                  "mask_keys_proven_by_table": {"c.s.t": "id"}}))
    assert report_verify_keys(env_file, "", result) == 0
    assert env_file.read_text() == 'governance_mode = "deterministic"\n'
    assert "applying commands don't edit committed files" in capsys.readouterr().out


def test_validate_abac_reads_an_overlay_file_as_the_committed_one():
    from validate_abac import logical_path

    assert logical_path(Path("e/generated/.runtime/abac.auto.tfvars")) == Path("e/generated/abac.auto.tfvars")
    assert logical_path(Path("e/generated/abac.auto.tfvars")) == Path("e/generated/abac.auto.tfvars")


# -- reader and weakening reports --------------------------------------------------------

TIERS = ["analysts", "regional", "viewers"]


def _write_generated(env_dir: Path, *, tiers=TIERS, exempt=(), readers=None) -> dict:
    settings = {"access_tier_groups": list(tiers), "raw_exempt_principals": list(exempt)}
    model = build_model({"prod_cat.sch.t.email": ["email_address"]}, {"prod_cat.sch.t.email": "STRING"}, settings)
    generated = env_dir / "generated"
    (generated / "abac.auto.tfvars").write_text(EMPTY_ABAC)
    (generated / "masking_functions.sql").write_text("")
    generate_abac.write_deterministic_model(generated / "abac.auto.tfvars", generated / "masking_functions.sql",
                                            model)
    env = (f'governance_mode = "deterministic"\naccess_tier_groups = {json.dumps(list(tiers))}\n'
           f"raw_exempt_principals = {json.dumps(list(exempt))}\n")
    if readers is not None:
        env += "table_readers = {\n" + "".join(f'  "{t}" = {json.dumps(r)}\n' for t, r in readers.items()) + "}\n"
    (env_dir / "env.auto.tfvars").write_text(env)
    return pg.desired_generation(env_dir)


def _deploy(env_dir: Path, generation: dict, grants=()) -> None:
    """Prod's data_access state and journal as a release of ``generation`` leaves them."""
    resources = [
        {"module": "module.data_access", "mode": "managed", "type": "databricks_policy_info", "name": "policies",
         "instances": [{"index_key": p["name"], "attributes": {
             "to_principals": p["to_principals"], "except_principals": p.get("except_principals") or [],
             "on_securable_fullname": p["catalog"]}} for p in generation["fgac_policies"]]},
        {"module": "module.data_access", "mode": "managed", "type": "databricks_entity_tag_assignment",
         "name": "treatment", "instances": [{"index_key": f"columns|{c}|gr_treatment", "attributes": {
             "entity_name": c, "tag_value": v, "entity_type": "columns", "tag_key": "gr_treatment"}}
             for c, v in generation["tags"].items()]},
        {"module": "module.data_access", "mode": "managed", "type": "databricks_grant", "name": "table_access",
         "instances": [{"index_key": f"{t}|{p}", "attributes": {}} for t, p in grants]},
    ]
    layer = env_dir / "data_access"
    layer.mkdir(parents=True, exist_ok=True)
    (layer / "terraform.tfstate").write_text(json.dumps({"version": 4, "serial": 1, "resources": resources}))
    pg.save_snapshot(env_dir, generation)
    spa.save_journal(env_dir, {"version": 1, "active": generation["fingerprint"], "pending": [],
                               "transition": None, "history": []})


@pytest.fixture
def released_prod(tmp_path):
    env_dir = tmp_path / "envs" / "prod"
    (env_dir / "generated").mkdir(parents=True)
    _deploy(env_dir, _write_generated(env_dir))
    rg._write_once(rg.release_dir(rg.cloud_dir_of(env_dir), "prod") / "released.json",
                   {"kind": "release", "version": rg.VERSION, "commit": "0123456789", "created_at": rg._now()})
    return env_dir


def test_unchanged_governance_reports_no_weakening_and_reads_no_new_table(released_prod):
    report = rg.reports(released_prod, "prod")
    assert report["ok"], report
    assert report["readers"]["tables"] == [] and report["weakening"]["unacknowledged"] == []


def test_a_new_raw_exemption_is_refused_until_acknowledged(released_prod):
    _write_generated(released_prod, exempt=["etl-sp"])
    report = rg.reports(released_prod, "prod")
    keys = {w["ack_key"] for w in report["weakening"]["unacknowledged"]}
    assert not report["ok"]
    assert {"prod:weaken:prod_cat.sch.t:etl-sp", "prod:weaken:prod_cat.sch.t.email:etl-sp"} <= keys
    (released_prod / "ack.txt").write_text("".join(f"{k}\n" for k in sorted(keys)))
    assert rg.reports(released_prod, "prod")["ok"]


def test_a_tier_move_that_unmasks_a_group_is_a_weakening(released_prod):
    _write_generated(released_prod, tiers=["regional", "analysts", "viewers"])  # regional now sees raw
    report = rg.reports(released_prod, "prod")
    assert not report["ok"]
    assert "prod:weaken:prod_cat.sch.t.email:regional" in {w["ack_key"] for w in report["weakening"]["unacknowledged"]}


def test_without_a_journal_the_deployed_protection_still_comes_from_state(released_prod):
    from scripts import staged_policy_apply as spa_

    spa_.journal_path(released_prod).unlink()
    # Deployed under kept (dev-catalog) names: matched to their values by suffix.
    state_path = released_prod / "data_access" / "terraform.tfstate"
    state = json.loads(state_path.read_text())
    for instance in state["resources"][0]["instances"]:
        instance["index_key"] = instance["index_key"].replace("prod_cat", "dev_cat")
    state_path.write_text(json.dumps(state))
    _write_generated(released_prod, exempt=["etl-sp"])
    from reader_inventory import FakeSource

    source = FakeSource(owners={("CATALOG", "prod_cat"): "deployer", ("SCHEMA", "prod_cat.sch"): "deployer",
                                ("TABLE", "prod_cat.sch.t"): "deployer"}, admins={"deployer"}, deployer="deployer")
    report = rg.reports(released_prod, "prod", reader_source=lambda: source)
    assert report["readers"]["tables"] == ["prod_cat.sch.t"]  # no journal: a first deterministic apply
    assert "prod:weaken:prod_cat.sch.t.email:etl-sp" in {w["ack_key"] for w in report["weakening"]["unacknowledged"]}


def test_a_stale_acknowledgement_is_refused(released_prod):
    (released_prod / "ack.txt").write_text("prod:weaken:prod_cat.sch.t.email:nobody\n")
    report = rg.reports(released_prod, "prod")
    assert not report["ok"] and report["unmatched_acknowledgements"] == ["prod:weaken:prod_cat.sch.t.email:nobody"]


def test_a_rollback_acknowledgement_is_stale_until_its_commit_is_released(released_prod):
    (released_prod / "ack.txt").write_text("prod:rollback:abcdef1\n")
    assert rg.reports(released_prod, "prod")["unmatched_acknowledgements"] == ["prod:rollback:abcdef1"]
    rollback = {"key": "prod:rollback:abcdef1", "consequence": "releases abcdef1, older than the last release"}
    assert rg.reports(released_prod, "prod", rollback=rollback)["ok"]
    rg._write_once(rg.release_dir(rg.cloud_dir_of(released_prod), "prod") / "r.json",
                   {"kind": "release", "version": rg.VERSION, "commit": "abcdef1234", "created_at": rg._now()})
    assert rg.reports(released_prod, "prod")["ok"]


def test_a_new_table_reader_is_widening(released_prod):
    _write_generated(released_prod, readers={"prod_cat.sch.t": ["regional"]})
    report = rg.reports(released_prod, "prod")
    assert "prod:reader:prod_cat.sch.t:regional" in {w["ack_key"] for w in report["weakening"]["unacknowledged"]}


def test_newly_governed_tables_get_the_reader_inventory(tmp_path):
    from reader_inventory import FakeSource, Grant

    env_dir = tmp_path / "envs" / "prod"
    (env_dir / "generated").mkdir(parents=True)
    _write_generated(env_dir)  # nothing deployed yet: a first deterministic apply
    with pytest.raises(rg.Refused, match="needs a SQL warehouse"):
        rg.reports(env_dir, "prod")
    source = FakeSource(
        grant_rows=[Grant("TABLE", "prod_cat.sch.t", "bi-sp", "SELECT"),
                    Grant("TABLE", "prod_cat.sch.t", "analysts", "SELECT")],
        owners={("CATALOG", "prod_cat"): "deployer", ("SCHEMA", "prod_cat.sch"): "deployer",
                ("TABLE", "prod_cat.sch.t"): "deployer"},
        admins={"deployer"}, deployer="deployer")
    report = rg.reports(env_dir, "prod", reader_source=lambda: source)
    assert report["readers"]["tables"] == ["prod_cat.sch.t"] and not report["ok"]
    unacked = {f["ack_key"] for c in report["readers"]["checks"] for f in c["unacknowledged"]}
    assert "prod:reader:prod_cat.sch.t:bi-sp" in unacked and "prod:reader:prod_cat.sch.t:analysts" not in unacked
    (env_dir / "ack.txt").write_text("".join(f"{k}\n" for k in sorted(unacked)))
    assert rg.reports(env_dir, "prod", reader_source=lambda: source)["ok"]


# The live AWS prod first release (run 6): three tables, three tier groups from
# table_readers, the env's deployer SP reading through SELECT, and a human who
# is the metastore admin and owns the catalog, schema and tables.
SHAPE_CATALOG, SHAPE_SCHEMA = "prod_cat", "dev_to_prod_demo"
SHAPE_TABLES = [f"{SHAPE_CATALOG}.{SHAPE_SCHEMA}.{t}" for t in ("customers", "notes", "payments")]
SHAPE_TIERS = ["dev_to_prod_payments_ops", "dev_to_prod_regional_analysts", "dev_to_prod_viewers"]
SHAPE_DEPLOYER = "2169eb19-40e9-44ba-9cbc-50741dcb8e91"
SHAPE_OWNER = "owner@example.com"


def _write_shape(env_dir: Path, readers=None, *, exempt=()) -> dict:
    readers = {t: list(SHAPE_TIERS) for t in SHAPE_TABLES} if readers is None else readers
    settings = {"access_tier_groups": SHAPE_TIERS, "raw_exempt_principals": list(exempt)}
    columns = {f"{t}.email": ["email_address"] for t in SHAPE_TABLES}
    model = build_model(columns, {c: "STRING" for c in columns}, settings, deployer_principal=SHAPE_DEPLOYER)
    generated = env_dir / "generated"
    generated.mkdir(parents=True, exist_ok=True)
    (generated / "abac.auto.tfvars").write_text(EMPTY_ABAC)
    (generated / "masking_functions.sql").write_text("")
    generate_abac.write_deterministic_model(generated / "abac.auto.tfvars", generated / "masking_functions.sql",
                                            model)
    (env_dir / "env.auto.tfvars").write_text(
        f'governance_mode = "deterministic"\naccess_tier_groups = {json.dumps(SHAPE_TIERS)}\n'
        f"raw_exempt_principals = {json.dumps(list(exempt))}\n"
        f'deployer_principal_id = "{SHAPE_DEPLOYER}"\n'
        "table_readers = {\n" + "".join(f'  "{t}" = {json.dumps(r)}\n' for t, r in readers.items()) + "}\n")
    return pg.desired_generation(env_dir)


def _shape_source(extra_grants=(), groups=None, *, listed_tiers=SHAPE_TIERS):
    from reader_inventory import FakeSource, Grant

    schema = f"{SHAPE_CATALOG}.{SHAPE_SCHEMA}"
    grants = [Grant("CATALOG", SHAPE_CATALOG, SHAPE_DEPLOYER, "SELECT"),
              Grant("CATALOG", SHAPE_CATALOG, SHAPE_DEPLOYER, "MANAGE"),
              Grant("SCHEMA", schema, SHAPE_DEPLOYER, "SELECT")]
    grants += [Grant("TABLE", t, SHAPE_DEPLOYER, "SELECT") for t in SHAPE_TABLES]
    owners = {("CATALOG", SHAPE_CATALOG): SHAPE_OWNER, ("SCHEMA", schema): SHAPE_OWNER}
    owners.update({("TABLE", t): SHAPE_OWNER for t in SHAPE_TABLES})
    groups = {**{t: set() for t in listed_tiers}, **(groups or {})}  # the workspace lists the tier groups
    return FakeSource(grant_rows=grants + list(extra_grants), owners=owners, groups=groups,
                      admins={SHAPE_OWNER}, deployer=SHAPE_DEPLOYER)


def _refusals(report) -> list[str]:
    return sorted([f"weakening {w['ack_key']}" for w in report["weakening"]["unacknowledged"]]
                  + [f"reader {f['ack_key']}" for c in report["readers"]["checks"] for f in c["unacknowledged"]])


def _owner_refusals(tables=SHAPE_TABLES) -> list[str]:
    return [f"reader prod:reader:{t}:{SHAPE_OWNER}" for t in tables]


def _complete_release(env_dir: Path, generation: dict, grants) -> None:
    _deploy(env_dir, generation, grants)
    rg._write_once(rg.release_dir(rg.cloud_dir_of(env_dir), "prod") / "release.json",
                   {"kind": "release", "version": rg.VERSION, "commit": "abcdef1234", "created_at": rg._now()})


@pytest.fixture
def first_prod(tmp_path):
    env_dir = tmp_path / "envs" / "prod"
    _write_shape(env_dir)
    return env_dir


def test_a_first_release_refuses_only_the_owner_not_its_own_contract_or_deployer(first_prod):
    report = rg.reports(first_prod, "prod", reader_source=_shape_source)
    # Before: 9 weakenings (the tiers' table_readers SELECT) + 3 deployer + 3 owner reader lines.
    assert _refusals(report) == _owner_refusals() and not report["ok"]
    (first_prod / "ack.txt").write_text("".join(f"prod:reader:{t}:{SHAPE_OWNER}\n" for t in SHAPE_TABLES))
    report = rg.reports(first_prod, "prod", reader_source=_shape_source)
    assert report["ok"], _refusals(report)


def test_every_round_of_a_first_release_keeps_the_contract_baseline(first_prod):
    # Round 1 applied the policies (the journal is active); SELECT is a later round.
    (first_prod / "ack.txt").write_text("".join(f"prod:reader:{t}:{SHAPE_OWNER}\n" for t in SHAPE_TABLES))
    _deploy(first_prod, pg.desired_generation(first_prod))
    report = rg.reports(first_prod, "prod", reader_source=_shape_source)
    # Every round re-inventories, so round 1's owner acknowledgements stay matched.
    assert report["readers"]["tables"] == SHAPE_TABLES and report["ok"], _refusals(report)
    # The SELECT round applied it all: the next round compares with what is deployed.
    _deploy(first_prod, pg.desired_generation(first_prod), [(t, p) for t in SHAPE_TABLES for p in SHAPE_TIERS])
    assert rg.reports(first_prod, "prod", reader_source=_shape_source)["ok"]
    # Once any SELECT is deployed the receipt store alone can't prove a first release, so a SELECT
    # round that stopped part-way fails closed: the rest of the contract is compared as widening.
    _deploy(first_prod, pg.desired_generation(first_prod), [(SHAPE_TABLES[0], SHAPE_TIERS[0])])
    report = rg.reports(first_prod, "prod", reader_source=_shape_source)
    assert len(report["weakening"]["unacknowledged"]) == 8 and not report["ok"]


def test_a_first_release_still_refuses_a_reader_outside_table_readers(first_prod):
    from reader_inventory import Grant

    source = _shape_source([Grant("TABLE", SHAPE_TABLES[1], "bi-sp", "SELECT"),
                            Grant("SCHEMA", f"{SHAPE_CATALOG}.{SHAPE_SCHEMA}", "contractors", "SELECT")],
                           groups={"contractors": {"contractor@example.com"}})
    report = rg.reports(first_prod, "prod", reader_source=lambda: source)
    refused = set(_refusals(report))
    assert f"reader prod:reader:{SHAPE_TABLES[1]}:bi-sp" in refused
    assert {f"reader prod:reader:{t}:{p}" for t in SHAPE_TABLES
            for p in ("contractors", "contractor@example.com")} <= refused


def test_a_first_release_still_refuses_a_contract_reader_outside_the_tiers(tmp_path):
    env_dir = tmp_path / "envs" / "prod"
    # A raw-exempt reader gets raw SELECT: its own reader line and its raw exemption both need an ack.
    _write_shape(env_dir, {SHAPE_TABLES[0]: [SHAPE_TIERS[0], "etl-sp"]}, exempt=["etl-sp"])
    keys = {w["ack_key"] for w in rg.reports(env_dir, "prod", reader_source=_shape_source)["weakening"]["unacknowledged"]}
    assert f"prod:reader:{SHAPE_TABLES[0]}:etl-sp" in keys and f"prod:weaken:{SHAPE_TABLES[0]}:etl-sp" in keys
    assert f"prod:reader:{SHAPE_TABLES[0]}:{SHAPE_TIERS[0]}" not in keys
    # A reader neither a tier nor exempt (validation refuses it first) is never part of the baseline.
    _write_shape(env_dir, {SHAPE_TABLES[0]: ["outsiders"]})
    keys = {w["ack_key"] for w in rg.reports(env_dir, "prod", reader_source=_shape_source)["weakening"]["unacknowledged"]}
    assert f"prod:reader:{SHAPE_TABLES[0]}:outsiders" in keys


def test_widening_the_contract_after_a_complete_release_is_refused(tmp_path):
    env_dir = tmp_path / "envs" / "prod"
    first = {SHAPE_TABLES[0]: [SHAPE_TIERS[0]], SHAPE_TABLES[1]: [SHAPE_TIERS[0]]}
    _complete_release(env_dir, _write_shape(env_dir, first), [(t, SHAPE_TIERS[0]) for t in first])
    assert rg.reports(env_dir, "prod")["ok"]
    widened = {SHAPE_TABLES[0]: SHAPE_TIERS[:2], SHAPE_TABLES[1]: [SHAPE_TIERS[0]],
               SHAPE_TABLES[2]: [SHAPE_TIERS[2]]}
    _write_shape(env_dir, widened)
    report = rg.reports(env_dir, "prod", reader_source=_shape_source)
    assert {w["ack_key"] for w in report["weakening"]["unacknowledged"]} == {
        f"prod:reader:{SHAPE_TABLES[0]}:{SHAPE_TIERS[1]}", f"prod:reader:{SHAPE_TABLES[2]}:{SHAPE_TIERS[2]}"}


def test_a_lost_receipt_and_state_cant_turn_a_released_env_into_a_first_release(tmp_path, capsys):
    import shutil
    from reader_inventory import Grant

    env_dir = tmp_path / "envs" / "prod"
    customers = SHAPE_TABLES[0]
    _complete_release(env_dir, _write_shape(env_dir, {customers: [SHAPE_TIERS[0]]}), [(customers, SHAPE_TIERS[0])])
    (env_dir / "ack.txt").write_text("".join(f"prod:reader:{t}:{SHAPE_OWNER}\n" for t in SHAPE_TABLES))
    released = lambda: _shape_source([Grant("TABLE", customers, SHAPE_TIERS[0], "SELECT")])  # what it applied
    _write_shape(env_dir, {customers: SHAPE_TIERS[:2]})
    widened = f"prod:reader:{customers}:{SHAPE_TIERS[1]}"
    report = rg.reports(env_dir, "prod", reader_source=released)
    assert [w["ack_key"] for w in report["weakening"]["unacknowledged"]] == [widened] and not report["ok"]
    # Only local files are lost: the receipt store and data_access state. Live SELECT still says released.
    shutil.rmtree(rg.release_dir(rg.cloud_dir_of(env_dir), "prod"))
    (env_dir / "data_access" / "terraform.tfstate").unlink()
    report = rg.reports(env_dir, "prod", reader_source=released)
    kept = f"prod:reader:{customers}:{SHAPE_TIERS[0]}"
    assert {w["ack_key"] for w in report["weakening"]["unacknowledged"]} == {kept, widened} and not report["ok"]
    assert report["first_release_refused"] == [f"{SHAPE_TIERS[0]} can read {customers} via SELECT on TABLE {customers}"]
    rg.print_reports(report)
    err = capsys.readouterr().err
    assert "isn't treated as a first release" in err and "GENIERAILS_STATE_DIR" in err
    # The recovery when the receipt can't be restored: review and acknowledge each SELECT.
    with (env_dir / "ack.txt").open("a") as ack:
        ack.write(f"{kept}\n{widened}\n")
    assert rg.reports(env_dir, "prod", reader_source=released)["ok"]


def _lost_receipt_widening(env_dir: Path, ack_lines=()) -> dict:
    """No receipt or state; customers widened to tiers 0 and 1; the owner (and ack_lines) acknowledged."""
    _write_shape(env_dir, {SHAPE_TABLES[0]: SHAPE_TIERS[:2]})
    (env_dir / "ack.txt").write_text("".join(f"{k}\n" for k in [f"prod:reader:{t}:{SHAPE_OWNER}" for t in SHAPE_TABLES]
                                             + list(ack_lines)))
    return {f"prod:reader:{SHAPE_TABLES[0]}:{t}" for t in SHAPE_TIERS[:2]}


def test_a_nested_tier_member_reading_live_is_evidence_of_a_release(tmp_path):
    from reader_inventory import Grant

    env_dir = tmp_path / "envs" / "prod"
    tier_selects = _lost_receipt_widening(env_dir)
    nested = "payments_child"  # check_readers accepts it as covered by tier0
    source = _shape_source([Grant("TABLE", SHAPE_TABLES[0], nested, "SELECT")],
                           groups={SHAPE_TIERS[0]: {nested}, nested: {"payer@example.com"}})
    report = rg.reports(env_dir, "prod", reader_source=lambda: source)
    assert any(e.startswith(f"{nested} can read") for e in report["first_release_refused"])
    assert {w["ack_key"] for w in report["weakening"]["unacknowledged"]} == tier_selects and not report["ok"]


def test_a_case_variant_of_a_tier_reading_live_is_evidence_of_a_release(tmp_path):
    from reader_inventory import Grant

    env_dir = tmp_path / "envs" / "prod"
    variant = SHAPE_TIERS[0].upper()
    # Acknowledging the mixed-case reader line doesn't acknowledge the table_readers SELECT.
    tier_selects = _lost_receipt_widening(env_dir, [f"prod:reader:{SHAPE_TABLES[0]}:{variant}"])
    source = _shape_source([Grant("TABLE", SHAPE_TABLES[0], variant, "SELECT")])
    report = rg.reports(env_dir, "prod", reader_source=lambda: source)
    assert any(e.startswith(f"{variant} can read") for e in report["first_release_refused"])
    assert {w["ack_key"] for w in report["weakening"]["unacknowledged"]} == tier_selects and not report["ok"]


def test_a_tier_whose_members_cant_be_listed_drops_the_first_release_baseline(tmp_path):
    env_dir = tmp_path / "envs" / "prod"
    tier_selects = _lost_receipt_widening(env_dir)
    source = _shape_source(listed_tiers=SHAPE_TIERS[1:])
    report = rg.reports(env_dir, "prod", reader_source=lambda: source)
    assert report["first_release_refused"] == [f"{SHAPE_TIERS[0]}'s members couldn't be listed (not in the workspace groups)"]
    assert {w["ack_key"] for w in report["weakening"]["unacknowledged"]} == tier_selects and not report["ok"]


def test_a_real_first_release_with_no_tier_select_live_keeps_the_contract_baseline(first_prod):
    report = rg.reports(first_prod, "prod", reader_source=_shape_source)
    assert report["first_release_refused"] == [] and _refusals(report) == _owner_refusals()


def test_a_released_env_that_granted_no_select_treats_new_readers_as_widening(tmp_path):
    env_dir = tmp_path / "envs" / "prod"
    _complete_release(env_dir, _write_shape(env_dir, {}), [])  # released with table_readers = {}
    _write_shape(env_dir)
    report = rg.reports(env_dir, "prod", reader_source=_shape_source)
    assert len(report["weakening"]["unacknowledged"]) == 9


def test_only_the_exact_deployer_id_is_accepted_as_a_reader(first_prod):
    from reader_inventory import Grant

    impostors = ["genierails-prod-deployer",  # the deployer SP's display name
                 "2169eb19-40e9-44ba-9cbc-50741dcb8e92",  # another SP
                 "deployers"]  # a group the deployer is in: the group and its other members still refuse
    source = _shape_source([Grant("TABLE", SHAPE_TABLES[0], p, "SELECT") for p in impostors],
                           groups={"deployers": {SHAPE_DEPLOYER, "someone@example.com"}})
    refused = set(_refusals(rg.reports(first_prod, "prod", reader_source=lambda: source)))
    assert {f"reader prod:reader:{SHAPE_TABLES[0]}:{p}" for p in [*impostors, "someone@example.com"]} <= refused
    assert not any(SHAPE_DEPLOYER in line for line in refused)
    # Without a committed deployer_principal_id, the deployer is a reader like any other.
    env_file = first_prod / "env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace(f'deployer_principal_id = "{SHAPE_DEPLOYER}"\n', ""))
    refused = _refusals(rg.reports(first_prod, "prod", reader_source=_shape_source))
    assert [line for line in refused if SHAPE_DEPLOYER in line] == [
        f"reader prod:reader:{t}:{SHAPE_DEPLOYER}" for t in SHAPE_TABLES]


def test_group_map_acknowledgements_are_reconciled_with_the_rest(released_prod, monkeypatch):
    import group_map

    monkeypatch.setattr(group_map, "required_acks",
                        lambda env_dir, env, cfg: {"prod:weaken:group_map:dev_a=prod_a": "remapped"})
    assert rg.reports(released_prod, "prod")["ok"]  # group_map's own check refuses the missing ack
    (released_prod / "ack.txt").write_text("prod:weaken:group_map:dev_a=prod_a\n")
    assert rg.reports(released_prod, "prod")["ok"]
    monkeypatch.setattr(group_map, "required_acks", lambda env_dir, env, cfg: {})
    assert not rg.reports(released_prod, "prod")["ok"]  # now stale


# -- edits git status can't see: skip-worktree / assume-unchanged ------------------------------

HIDDEN = [("skip-worktree", "shared/scripts/code.py"), ("assume-unchanged", "shared/scripts/code.py"),
          ("skip-worktree", "aws/envs/dev/env.auto.tfvars"), ("assume-unchanged", "aws/envs/account/env.auto.tfvars")]


def _hide_an_edit(repo, flag, rel):
    """Codex's repro: flag a tracked file, then edit it; git status stays empty."""
    repo.git("update-index", f"--{flag}", "--", rel)
    path = repo.root / rel
    path.write_text(path.read_text() + "# hidden edit\n")
    assert repo.git("status", "--porcelain") == ""


@pytest.mark.parametrize("flag, rel", HIDDEN, ids=[f"{f}:{r}" for f, r in HIDDEN])
def test_a_flag_hiding_an_edit_makes_the_checkout_unclean_everywhere(repo, flag, rel):
    rehearse(repo)  # a passing clean receipt of HEAD before the hidden edit
    _hide_an_edit(repo, flag, rel)
    message = f"{rel} is marked {flag}, so git status can't show its edits: clear it (git update-index --no-{flag} -- {rel})"
    status = rg.checkout_status(repo.dev)
    assert status["clean_tree"] is False and message in status["dirty"], status
    # clean-check (before rehearse/release-plan's check, lock and live calls) refuses.
    with pytest.raises(rg.Refused, match="checkout is not clean") as refused:
        rg.clean_check(repo.dev, "dev", "make rehearse")
    assert message in str(refused.value)
    # A receipt written now records it as not clean, so it never counts for a release.
    receipt = rg.rehearse_finish(repo.dev, "dev", "passed", Path("Makefile"), "make")
    assert receipt["clean_tree"] is False and message in receipt["dirty"]
    # Provenance refuses too.
    with pytest.raises(rg.Refused, match="checkout is not clean"):
        rg.provenance(repo.prod, "prod")


def test_a_flag_without_an_edit_still_refuses_and_clearing_it_restores_clean(repo):
    repo.git("update-index", "--skip-worktree", "--", "shared/scripts/code.py")
    assert rg.checkout_status(repo.dev)["clean_tree"] is False
    repo.git("update-index", "--no-skip-worktree", "--", "shared/scripts/code.py")
    assert rg.checkout_status(repo.dev)["clean_tree"] is True
