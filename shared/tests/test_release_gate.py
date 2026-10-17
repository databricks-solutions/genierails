"""Release gating (rollout step 9): rehearse receipts, provenance, manifests, release receipts.

Every test runs against a real git checkout in tmp_path with a bare
``origin`` it fetches from, laid out like a user's repo (envs/ committed,
runtime files ignored). Receipts go to GENIERAILS_STATE_DIR in tmp_path.
"""

from __future__ import annotations

import json
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


def test_provenance_refuses_a_stale_receipt(repo):
    rehearse(repo)
    repo.commit({"aws/envs/dev/env.auto.tfvars": DEV_ENV + "# new\n"})
    with pytest.raises(rg.Refused, match="no passing rehearsal of"):
        rg.provenance(repo.prod, "prod")


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
    forged = {k: v for k, v in receipt.items() if k != "_path"} | {"fingerprint": "0" * 64,
                                                                    "created_at": rg._now()}
    rg._write_once(Path(receipt["_path"]).with_name("zz-forged.json"), forged)
    with pytest.raises(rg.Refused, match="fingerprint differs from the rehearsal receipt"):
        rg.provenance(repo.prod, "prod")


def test_provenance_refuses_a_prod_generated_digest_mismatch(repo, monkeypatch):
    monkeypatch.setattr(rg, "promoted_digest", lambda *a: ("what-promote-to-produces-now", ""))
    rehearse(repo)
    with pytest.raises(rg.Refused, match="stale promotion"):
        rg.provenance(repo.prod, "prod")


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

    def fake(config, auth, env, ddl_out=None, *, record_governed=True):
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


def test_group_map_acknowledgements_are_reconciled_with_the_rest(released_prod, monkeypatch):
    import group_map

    monkeypatch.setattr(group_map, "required_acks",
                        lambda env_dir, env, cfg: {"prod:weaken:group_map:dev_a=prod_a": "remapped"})
    assert rg.reports(released_prod, "prod")["ok"]  # group_map's own check refuses the missing ack
    (released_prod / "ack.txt").write_text("prod:weaken:group_map:dev_a=prod_a\n")
    assert rg.reports(released_prod, "prod")["ok"]
    monkeypatch.setattr(group_map, "required_acks", lambda env_dir, env, cfg: {})
    assert not rg.reports(released_prod, "prod")["ok"]  # now stale
