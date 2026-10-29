"""make status / make next (scripts/env_status.py): one table of repo states, each with its exact next command.

Every case runs the script as make does, with a fake ``databricks`` package
first on PYTHONPATH and fake ``databricks``/``terraform`` executables first on
PATH, each of which leaves a marker if anything imports or runs it; and every
case snapshots the repo, its origin and the receipt store before and after,
so status and next are proven read-only (no client, no write, no lock).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import ci_guard, env_status, environment_lock
from scripts import policy_generations as pg
from scripts import release_gate as rg
from tests import script_stubs
from tests.test_release_gate import EMPTY_ABAC, PROD_ENV, Repo

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "shared" / "scripts" / "env_status.py"
SECRET = "zz-client-secret-never-printed"
AUTH = ('databricks_workspace_host = "https://example.cloud.databricks.com"\n'
        f'databricks_client_id = "c0000000-0000-0000-0000-00000000c001"\ndatabricks_client_secret = "{SECRET}"\n')
NONCE = "zz-lock-nonce-never-printed"
LEGACY = 'governance_mode = "legacy"\naccess_tier_groups = ["analysts", "viewers"]\n'
STG_ENV = PROD_ENV.replace("d002", "d003").replace("prod_cat", "stg_cat")
PROD_FROM_STG = PROD_ENV.replace('promote_from = "dev"', 'promote_from = "stg"').replace("dev_cat", "stg_cat")


class World:
    """A customer repo (tests/test_release_gate.py's Repo) and a sandbox that catches any client or Terraform."""

    def __init__(self, tmp_path: Path, repo: Repo):
        self.tmp, self.repo = tmp_path, repo
        self.markers = tmp_path / "markers"
        self.markers.mkdir()
        fake = tmp_path / "fake-py" / "databricks"
        fake.mkdir(parents=True)
        (fake / "__init__.py").write_text(
            f"open({str(self.markers / 'databricks-imported')!r}, 'w').close()\n"
            "raise ImportError('make status/next must not import the Databricks SDK')\n")
        self.bin = tmp_path / "fake-bin"
        self.bin.mkdir()
        for name in ("databricks", "terraform"):
            tool = self.bin / name
            tool.write_text(f"#!/bin/sh\ntouch {self.markers / name}-ran\nexit 1\n")
            tool.chmod(0o755)

    def environ(self, extra: dict | None = None) -> dict:
        """The host environment minus every CI marker (so a CI runner doesn't change the table), plus ``extra``."""
        environ = {k: v for k, v in script_stubs.without_ci(os.environ).items()
                   if k not in ("MAKEFLAGS", "MAKELEVEL", "PYTHONPATH")}
        environ |= self.repo.env | (extra or {})
        environ["PYTHONPATH"] = str(self.tmp / "fake-py")
        environ["PATH"] = f"{self.bin}:{Path(sys.executable).parent}:{environ['PATH']}"
        return environ

    def snapshot(self) -> dict:
        files = {}
        for base in (self.repo.root, self.repo.origin, self.repo.state):
            for path in sorted(base.rglob("*")):
                st = path.lstat()
                key = str(path.relative_to(self.tmp))
                if path.is_symlink():
                    files[key] = ("link", os.readlink(path))
                elif path.is_file():
                    files[key] = (hashlib.sha256(path.read_bytes()).hexdigest(), oct(st.st_mode), st.st_mtime_ns)
                else:
                    files[key] = ("dir", oct(st.st_mode))
        return files

    def run(self, command: str, env: str, *extra: str, via_make: bool = False, ci: dict | None = None,
            makefile: Path | None = None) -> subprocess.CompletedProcess:
        before = self.snapshot()
        if via_make:
            args = ["make", "--no-print-directory", "-f", str(makefile or ROOT / "aws" / "Makefile"), command, f"ENV={env}",
                    f"CLOUD_ROOT={self.repo.cloud}", f"SHARED_ROOT={ROOT / 'shared'}", *extra]
        else:
            args = [sys.executable, str(SCRIPT), command, "--cloud-dir", str(self.repo.cloud), "--env", env, *extra]
        result = subprocess.run(args, cwd=self.repo.cloud, text=True, capture_output=True, env=self.environ(ci))
        assert self.snapshot() == before, "make status/next changed the repo, its origin or the receipt store"
        assert not list(self.markers.iterdir()), "a Databricks client or Terraform was reached"
        for secret in (SECRET, NONCE):
            assert secret not in result.stdout + result.stderr
        return result

    def next(self, env: str, ci: dict | None = None) -> tuple[str, str]:
        result = self.run("next", env, ci=ci)
        assert result.returncode == 0, result.stderr
        command, why = result.stdout.rstrip("\n").split("\n", 1)
        assert why.startswith("  Why: ")
        return command, why[len("  Why: "):]

    def status(self, env: str, ci: dict | None = None) -> dict:
        result = self.run("status", env, "--json", ci=ci)
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)


@pytest.fixture
def repo(tmp_path, monkeypatch):
    r = Repo(tmp_path, monkeypatch)
    monkeypatch.setattr(rg, "promoted_digest",
                        lambda cloud, makefile, make, target: (rg.generated_digest(Path(cloud) / "envs" / target
                                                                                   / "generated"), ""))
    for env in ("dev", "prod"):
        (r.cloud / "envs" / env / "auth.auto.tfvars").write_text(AUTH)
    return r


@pytest.fixture
def world(tmp_path, repo):
    return World(tmp_path, repo)


def rehearse(repo: Repo, env: str = "dev") -> dict:
    env_dir = repo.cloud / "envs" / env
    assert rg.rehearse_start(env_dir, env, Path("Makefile"), "make") == "run"
    return rg.rehearse_finish(env_dir, env, "passed", Path("Makefile"), "make")


def account_applied(repo: Repo) -> None:
    """The first deploy's account layer is done (what make apply-account's receipt records)."""
    path = rg.env_state(repo.cloud, "account") / "receipts" / "account" / "r.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"kind": "account", "version": rg.VERSION, "commit": repo.head(),
                                "manifest": {"id": "a1"}, "created_at": "2026-01-01T00:00:00Z"}))


def account_manifest(repo: Repo, *, changes: bool) -> None:
    directory = rg.manifests_dir(repo.cloud, "account") / "a1"
    directory.mkdir(parents=True)
    (directory / "account.tfplan").write_bytes(b"plan")
    (directory / "manifest.json").write_text(json.dumps({
        "kind": "account-manifest", "version": rg.VERSION, "env": "account", "id": "a1", "commit": repo.head(),
        "created_at": "2026-01-01T00:00:00Z", "plan": {"file": "account.tfplan", "changes": changes},
        "tag_sync": []}))


def manifest(repo: Repo, manifest_id: str = "m1", env: str = "prod") -> Path:
    directory = rg.manifests_dir(repo.cloud, env) / manifest_id
    directory.mkdir(parents=True)
    path = directory / "manifest.json"
    path.write_text(json.dumps({"kind": "manifest", "version": rg.VERSION, "env": env, "id": manifest_id,
                                "commit": repo.head(), "created_at": "2026-01-02T00:00:00Z", "final": True}))
    rg._set_current(repo.cloud, env, manifest_id, "2026-01-02T00:00:00Z")
    return path


def receipt(repo: Repo, kind: str, manifest_id: str, env: str = "prod") -> None:
    where = {"release-phase": "phases", "release": "release"}[kind]
    path = rg.env_state(repo.cloud, env) / "receipts" / where / f"{manifest_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"kind": kind, "version": rg.VERSION, "env": env, "id": manifest_id,
                                "commit": repo.head(), "manifest": {"id": manifest_id},
                                "created_at": "2026-01-03T00:00:00Z"}))


def lock(repo: Repo, env: str, pid: int) -> None:
    path = environment_lock.lock_path(repo.cloud / "envs" / env)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"pid": pid, "host": socket.gethostname(), "owner": "release", "nonce": NONCE,
                                "pid_start": "x", "started_at": "2026-01-04T00:00:00+00:00"}))


def journal(repo: Repo, env: str, to: str) -> None:
    env_dir = repo.cloud / "envs" / env
    pg.write_json(pg.ledger_dir(env_dir) / "journal.json", {
        "version": 1, "active": "", "pending": [], "history": [],
        "transition": {"run_id": "r1", "from": "", "to": to, "status": "failed", "failed_phase": "verify-created",
                       "error": "boom", "commit": repo.head(), "phases": ["create", "verify-created", "grant"],
                       "completed": ["create"], "switch_attempted": False, "restoring": False,
                       "restore_failed": False, "retain": "", "current": "verify-created"}})


def _dead_pid() -> int:
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    child.wait()
    return child.pid


# -- the table ------------------------------------------------------------------------------------------------------

def test_fresh_repo(tmp_path, monkeypatch):
    repo = Repo(tmp_path, monkeypatch)
    repo.git("rm", "-rq", "aws/envs")
    repo.git("commit", "-qm", "no envs")
    assert World(tmp_path, repo).next("dev") == (
        "make setup ENV=dev", "envs/dev/env.auto.tfvars doesn't exist yet; setup writes it and prints this env's steps")


def test_after_setup(tmp_path, monkeypatch):
    repo = Repo(tmp_path, monkeypatch)
    repo.git("rm", "-rq", "aws/envs")
    repo.git("commit", "-qm", "no envs")
    world = World(tmp_path, repo)
    setup = subprocess.run(["make", "--no-print-directory", "-f", str(ROOT / "aws" / "Makefile"), "setup", "ENV=dev",
                            f"CLOUD_ROOT={repo.cloud}", f"SHARED_ROOT={ROOT / 'shared'}", "GITIGNORE=off"], cwd=repo.cloud, text=True,
                           capture_output=True, env=world.environ())
    assert setup.returncode == 0, setup.stdout + setup.stderr
    command, why = world.next("dev")
    assert command == "edit envs/dev/auth.auto.tfvars"
    assert why.startswith("fill in the dev workspace host, client ID and client secret (it is incomplete")
    (repo.dev / "auth.auto.tfvars").write_text(AUTH)
    command, why = world.next("dev")
    assert command == "edit envs/dev/env.auto.tfvars" and "placeholder" in why


def test_after_generate_uncommitted(world, repo):
    (repo.dev / "generated" / "abac.auto.tfvars").write_text(EMPTY_ABAC + "# reviewed\n")
    command, why = world.next("dev")
    assert command == "git add envs/ && git commit"
    assert why.startswith("uncommitted changes (aws/envs/dev/generated/abac.auto.tfvars)")
    assert world.status("dev")["git"]["env_dirty"] == [" M aws/envs/dev/generated/abac.auto.tfvars"]


def test_committed_first_deploy_plans_the_account_layer(world, repo):
    assert world.next("dev")[0] == "make account-plan"
    account_manifest(repo, changes=True)
    assert world.next("dev") == ("make apply-account",
                                 "the approved account manifest a1 for HEAD is not applied yet")
    assert world.status("dev")["account"]["pending"][0]["id"] == "a1"


def test_account_plan_needs_head_exactly_on_origin_main(world, repo):
    repo.commit({"aws/envs/dev/ack.txt": "\n"}, push=False)
    assert world.next("dev") == ("git push origin HEAD:main (or merge its PR, then pull)",
                                 f"HEAD {repo.head()[:12]} isn't on origin/main (as last fetched; status doesn't "
                                 "fetch); make account-plan runs only from exactly origin/main")


def test_account_plan_accepts_committed_head_in_local_only_repo(world, repo):
    repo.git("remote", "remove", "origin")
    repo.commit({"aws/envs/dev/ack.txt": "\n"}, push=False)
    assert world.next("dev")[0] == "make account-plan"
    status = world.status("dev")
    assert status["git"]["local_only"] is True
    assert status["git"]["remote"] == "local main"
    assert status["git"]["remote_ref"] == "refs/heads/main"
    assert status["git"]["head_is_remote"] is True


def test_committed_rehearses(world, repo):
    account_applied(repo)
    assert world.next("dev") == ("make rehearse ENV=dev", f"no passing rehearsal of HEAD {repo.head()[:12]} from "
                                                          "this receipt store; release-plan needs one")
    assert world.next("prod") == ("make rehearse ENV=dev", f"release-plan ENV=prod needs a passing rehearsal of dev "
                                                           f"at HEAD {repo.head()[:12]}; no passing rehearsal of HEAD "
                                                           f"{repo.head()[:12]} from this receipt store; release-plan "
                                                           "needs one")


def test_rehearsed_plans_the_release(world, repo):
    account_applied(repo)
    rehearse(repo)
    expected = ("make release-plan ENV=prod",
                f"dev is rehearsed at HEAD {repo.head()[:12]} and envs/prod/generated matches it")
    assert world.next("dev") == expected
    assert world.next("prod") == expected
    status = world.status("prod")
    assert status["rehearsal"]["passing"] and status["rehearsal"]["target_digest"] == "matches"
    assert status["release"]["first_release"] == {"in_progress": False, "rounds_applied": 0}


def test_rehearsed_with_a_stale_promotion_promotes(world, repo, monkeypatch):
    account_applied(repo)
    monkeypatch.setattr(rg, "promoted_digest", lambda *args: ("other-digest", ""))
    rehearse(repo)
    assert world.next("prod")[0] == "make promote-to ENV=prod"
    assert world.next("dev")[0] == "make promote-to ENV=prod"


def test_promoted_uncommitted_then_committed(world, repo):
    account_applied(repo)
    rehearse(repo)
    (repo.prod / "generated" / "masking_functions.sql").write_text("-- promoted\n")
    assert world.next("prod")[0] == "git add envs/ && git commit"
    head = repo.commit({})
    assert world.next("prod") == ("make rehearse ENV=dev", f"release-plan ENV=prod needs a passing rehearsal of dev "
                                                           f"at HEAD {head[:12]}; no passing rehearsal of HEAD "
                                                           f"{head[:12]} from this receipt store; release-plan "
                                                           "needs one")


def test_rehearsed_but_not_on_origin_main_pushes(world, repo):
    account_applied(repo)
    repo.commit({"aws/envs/prod/ack.txt": "\n"}, push=False)
    rehearse(repo)
    assert world.next("prod")[0] == "git push origin HEAD:main (or merge its PR, then pull)"


def test_no_remote_local_only_repo_plans_release(world, repo):
    account_applied(repo)
    rehearse(repo)
    repo.git("remote", "remove", "origin")
    command, why = world.next("prod")
    assert command == "make release-plan ENV=prod"
    assert why == f"dev is rehearsed at HEAD {repo.head()[:12]} and envs/prod/generated matches it"
    text = world.run("status", "prod").stdout
    assert "local-only repo (local main; no remotes)" in text


def test_local_only_rollback_next_hint_names_local_main(world, repo):
    account_applied(repo)
    repo.git("remote", "remove", "origin")
    older = repo.head()
    newer = repo.commit({"aws/envs/prod/env.auto.tfvars": PROD_ENV + "# v2\n"}, push=False)
    path = rg.release_dir(repo.cloud, "prod") / f"{rg._now().replace(':', '')}-{newer[:12]}.json"
    rg._write_once(path, {"kind": "release", "version": rg.VERSION, "env": "prod", "commit": newer,
                          "created_at": rg._now()})
    repo.git("checkout", "-q", "--detach", older)
    rehearse(repo)
    command, why = world.next("prod")
    assert command == f"merge prod:rollback:{older} into envs/prod/ack.txt on local main"
    assert "older than the last release" in why


def test_missing_origin_with_another_remote_still_pushes_or_fixes_origin(world, repo):
    account_applied(repo)
    rehearse(repo)
    repo.git("remote", "remove", "origin")
    repo.git("remote", "add", "fork", str(repo.origin))
    command, why = world.next("prod")
    assert command == "git remote add origin <your repo URL> && git push origin HEAD:main"
    assert "there is no origin remote, but other remote(s) exist (fork)" in why


def test_mid_first_release(world, repo):
    account_applied(repo)
    rehearse(repo)
    path = manifest(repo, "m1")
    assert world.next("prod") == ("make release ENV=prod", "the current manifest m1 (first release, round 1) plans "
                                                           "HEAD and is not released yet: get it approved, then "
                                                           "release it")
    receipt(repo, "release-phase", "m1")
    assert world.next("prod") == ("make release-plan ENV=prod",
                                  "round 1 of the first release is applied; plan round 2")
    status = world.status("prod")
    assert status["release"]["first_release"] == {"in_progress": True, "rounds_applied": 1}
    assert status["release"]["current_manifest"]["path"] == str(path)


def test_released(world, repo):
    account_applied(repo)
    rehearse(repo)
    manifest(repo, "m1")
    receipt(repo, "release", "m1")
    assert world.next("prod") == ("make maintain ENV=prod",
                                  "prod is released at HEAD; maintain keeps it reconciled (scheduled)")
    assert world.next("dev") == ("make maintain ENV=prod",
                                 "prod is released at HEAD; maintain keeps it reconciled (scheduled)")


def test_held_lock(world, repo):
    account_applied(repo)
    rehearse(repo)
    lock(repo, "prod", os.getpid())
    command, why = world.next("prod")
    assert command == (f"wait for make release (pid {os.getpid()} on {socket.gethostname()}, since "
                       "2026-01-04T00:00:00+00:00) to finish, then make next ENV=prod")
    assert why.endswith("holds the prod env lock; every applying command refuses until it is released")
    assert world.status("prod")["lock"]["state"] == "held"


def test_stale_lock_is_reported_and_skipped(world, repo):
    account_applied(repo)
    rehearse(repo)
    lock(repo, "prod", _dead_pid())
    assert world.next("prod")[0] == "make release-plan ENV=prod"
    assert world.status("prod")["lock"]["state"] == "stale"


def test_legacy_envs(world, repo):
    repo.commit({"aws/envs/dev/env.auto.tfvars": LEGACY,
                 "aws/envs/prod/env.auto.tfvars": 'promote_from = "dev"\n'})
    assert world.next("dev") == ("make rehearse ENV=dev", "legacy envs keep no rehearse receipt, so status can't tell "
                                                          "whether this commit was rehearsed; once it passes, make "
                                                          "promote-to ENV=prod")
    assert world.next("prod") == ("make release ENV=prod", "prod uses legacy governance: release derives, checks, "
                                                           "applies and verifies in one run (legacy envs have no "
                                                           "release-plan)")
    status = world.status("prod")
    assert status["config"]["governance_mode"] == "legacy" and "rehearsal" not in status


def test_a_legacy_env_whose_migration_began_restores_deterministic(world, repo):
    repo.commit({"aws/envs/dev/env.auto.tfvars": LEGACY})
    journal(repo, "dev", "0" * 16)
    command, why = world.next("dev")
    assert command == 'edit envs/dev/env.auto.tfvars: governance_mode = "deterministic"'
    assert "was migrated to deterministic governance (a policy journal" in why


def test_multi_hop_chain(world, repo):
    account_applied(repo)
    repo.commit({"aws/envs/stg/env.auto.tfvars": STG_ENV, "aws/envs/stg/generated/abac.auto.tfvars": EMPTY_ABAC,
                 "aws/envs/prod/env.auto.tfvars": PROD_FROM_STG})
    (repo.cloud / "envs" / "stg" / "auth.auto.tfvars").write_text(AUTH)
    rehearse(repo)
    command, why = world.next("prod")
    assert command == 'edit envs/prod/env.auto.tfvars: promote_from = "dev"'
    assert "make rehearse refuses envs/stg while it has promote_from (chain dev -> stg -> prod)" in why
    assert world.next("stg")[0] == "make release-plan ENV=stg"
    assert world.next("dev")[0] == "make release-plan ENV=stg"
    assert world.status("prod")["config"]["chain"] == ["dev", "stg", "prod"]


def test_open_staged_change_resumes_through_release_plan(world, repo):
    account_applied(repo)
    rehearse(repo)
    journal(repo, "prod", pg.desired_generation(repo.prod)["fingerprint"])
    command, why = world.next("prod")
    assert command == "make release-plan ENV=prod"
    assert why.endswith("(it resumes unfinished policy change r1, failed, done: create)")
    staged = world.status("prod")["staged"]
    assert staged["transition"]["run_id"] == "r1" and staged["config_moved_on"] is False


def test_open_staged_change_after_the_config_moved_on(world, repo):
    account_applied(repo)
    rehearse(repo)
    journal(repo, "prod", "0" * 16)
    staged = world.status("prod")["staged"]
    assert staged["config_moved_on"] is True
    command = world.next("prod")[0]
    assert command == ("make policy-abandon ENV=prod" if staged["abandonable"]
                       else f"git checkout --detach {repo.head()}")


def test_leftover_evidence_folder(world, repo):
    account_applied(repo)
    (repo.prod / "generated" / "evidence").mkdir()
    (repo.prod / "generated" / "evidence" / "compliance-evidence-v1.json").write_text("{}")
    assert world.next("prod")[0] == "move envs/prod/generated/evidence/ out of the checkout"
    assert world.status("prod")["evidence"] == {"path": "envs/prod/generated/evidence", "exists": True,
                                                "files": 1, "untracked": True}


def test_placeholder_deployer_blocks_before_anything_live(world, repo):
    repo.commit({"aws/envs/prod/env.auto.tfvars": PROD_ENV.replace(
        "d0000000-0000-0000-0000-00000000d002", "<prod-deployer>")})
    command, why = world.next("prod")
    assert command == "edit envs/prod/env.auto.tfvars" and "<prod-deployer>" in why


def test_promoted_env_without_credentials_edits_auth_before_release_plan(world, repo):
    account_applied(repo)
    rehearse(repo)
    (repo.prod / "auth.auto.tfvars").unlink()
    command, why = world.next("prod")
    assert command == "edit envs/prod/auth.auto.tfvars"
    assert why.startswith("make release-plan ENV=prod needs the prod workspace host")


# -- output, wiring and refusals -------------------------------------------------------------------------------------

def test_status_text_reports_every_section(world, repo):
    account_applied(repo)
    rehearse(repo)
    manifest(repo, "m1")
    lock(repo, "prod", _dead_pid())
    result = world.run("status", "prod")
    assert result.returncode == 0, result.stderr
    for line in ("Governance:  deterministic", "Role:        promoted from dev", "Chain:       dev -> prod",
                 "Credentials: auth.auto.tfvars configured", f"Git:         HEAD {repo.head()[:12]}, on origin/main",
                 "envs/prod: committed", "Rehearsal:   passing rehearsal of dev covers HEAD",
                 "Manifest:    m1 (awaiting release)", "Released:    never", "First release: not started",
                 "Policy:      no policy journal yet", "Lock:        stale (dead pid", "Account:     no pending",
                 "Next: make release ENV=prod"):
        assert line in result.stdout


def test_make_targets_match_the_script(world, repo):
    account_applied(repo)
    rehearse(repo)
    by_make = world.run("next", "prod", via_make=True)
    assert by_make.returncode == 0, by_make.stderr
    assert by_make.stdout == world.run("next", "prod").stdout
    status = world.run("status", "prod", "JSON=1", via_make=True)
    assert status.returncode == 0, status.stderr
    assert json.loads(status.stdout)["next"]["command"] == "make release-plan ENV=prod"
    text = world.run("status", "prod", via_make=True)
    assert text.stdout.startswith("=== GenieRails status (prod) ===")


@pytest.mark.parametrize("env", ["account", "data_access"])
def test_reserved_envs_are_refused(world, env):
    result = world.run("next", env)
    assert result.returncode == 2
    assert f"Use ENV=<workspace> instead of ENV={env}" in result.stderr


def test_a_missing_state_dir_is_an_error_not_an_empty_store(world, repo, tmp_path):
    repo.env[rg.STATE_DIR_ENV] = str(tmp_path / "no-such-dir")
    command, why = world.next("prod")
    assert command == "make status ENV=prod"
    assert "the receipt store can't be read" in why
    assert not (tmp_path / "no-such-dir").exists()


def test_a_receipt_store_without_an_id_counts_no_receipt_and_creates_none(world, repo):
    account_applied(repo)
    rehearse(repo)
    store = rg.env_state(repo.cloud, "dev") / ".store-id"
    store.unlink()
    assert world.next("prod")[0] == "make rehearse ENV=dev"
    assert not store.exists()


def test_makefile_documents_both_targets():
    text = (ROOT / "shared" / "Makefile.shared").read_text()
    assert "\nstatus: ## Read-only:" in text and "\nnext: ## Read-only:" in text
    assert " status next\n" in text.split("\n\n", 1)[0] + "\n"


# -- CI: never a target make's CI guard refuses --------------------------------------------------------------------

CI_RUNS = [{"CI": "true"}, {"CI": "YES"}, {"CI": "1"}, {"TF_BUILD": "True"}, {"GITLAB_CI": "true"},
           {"JENKINS_URL": "http://jenkins"}, {"BUILDKITE": "true"}, {"CIRCLECI": "true"}]
OPTED_OUT = {"CI": "true", "GENIERAILS_ALLOW_CI_APPLY": "1"}
MAKEFILE_TEXT = (ROOT / "shared" / "Makefile.shared").read_text()


def guard_accepts(world: World, command: str, ci: dict) -> bool:
    """Whether make's real _guard-not-ci-apply (Makefile.shared) lets ``command``'s target run under ``ci``.

    A target without the guard always passes it. The guard target is run on its own, so its dev rehearse
    exemption (goal exactly rehearse) never applies here; that one has its own test below.
    """
    match = re.match(r"^make ([a-z][a-z0-9_-]*)(?: ENV=(\S+))?", command)
    prereqs = re.search(rf"^{re.escape(match.group(1))}:(?!=)([^#\n]*)", MAKEFILE_TEXT, re.MULTILINE) if match else None
    guards = [g for g in ("_guard-not-ci-rebind", "_guard-not-ci-apply") if prereqs and g in prereqs.group(1).split()]
    if not guards:
        return True
    guard = guards[0]
    result = subprocess.run(["make", "--no-print-directory", "-f", str(ROOT / "aws" / "Makefile"), guard,
                             f"ENV={match.group(2) or 'dev'}", f"CLOUD_ROOT={world.repo.cloud}",
                             f"SHARED_ROOT={ROOT / 'shared'}"], cwd=world.repo.cloud, text=True,
                            capture_output=True, env=world.environ(ci))
    return result.returncode == 0


def deployment_machine(command: str) -> str:
    return f"run {command} from the deployment machine, not CI"


@pytest.mark.parametrize("ci", CI_RUNS, ids=lambda ci: ",".join(ci))
def test_ci_never_suggests_release_plan(world, repo, ci):
    account_applied(repo)
    rehearse(repo)
    plain = world.next("prod")[0]
    assert plain == "make release-plan ENV=prod" and not guard_accepts(world, plain, ci)
    command, why = world.next("prod", ci)
    assert command == deployment_machine("make release-plan ENV=prod")
    assert why.endswith("but make release-plan refuses applying targets in CI "
                        f"({ci_guard.ci_markers(ci)[0]}) unless GENIERAILS_ALLOW_CI_APPLY=1 "
                        "(isolated integration-test workflows only)")
    assert world.next("dev", ci)[0] == deployment_machine("make release-plan ENV=prod")


def test_ci_never_suggests_release_maintain_apply_account_rehearse_or_abandon(world, repo):
    ci = {"CI": "true"}
    account_manifest(repo, changes=True)
    assert world.next("dev", ci)[0] == deployment_machine("make apply-account")
    assert not guard_accepts(world, "make apply-account", ci)
    account_applied(repo)
    assert world.next("dev", ci)[0] == deployment_machine("make rehearse ENV=dev")
    rehearse(repo)
    journal(repo, "prod", "0" * 16)
    staged = world.status("prod")["staged"]
    moved = world.next("prod", ci)[0]
    if staged["abandonable"]:
        assert moved == deployment_machine("make policy-abandon ENV=prod")
        assert not guard_accepts(world, "make policy-abandon ENV=prod", ci)
    else:
        assert moved == f"git checkout --detach {repo.head()}"
    pg.ledger_dir(repo.prod).joinpath("journal.json").unlink()
    manifest(repo, "m1")
    assert world.next("prod", ci)[0] == deployment_machine("make release ENV=prod")
    receipt(repo, "release", "m1")
    assert world.next("prod", ci)[0] == deployment_machine("make maintain ENV=prod")
    for target in ("release ENV=prod", "maintain ENV=prod", "rehearse ENV=dev"):
        assert not guard_accepts(world, f"make {target}", ci)


def test_ci_keeps_steps_make_runs_in_ci(world, repo):
    """account-plan, promote-to, setup and commits carry no CI guard, so CI gets them unchanged."""
    ci = {"CI": "true"}
    assert world.next("dev", ci)[0] == "make account-plan"
    assert guard_accepts(world, "make account-plan", ci)
    account_applied(repo)
    (repo.prod / "generated" / "masking_functions.sql").write_text("-- promoted\n")
    assert world.next("prod", ci)[0] == "git add envs/ && git commit"


def test_ci_opt_out_suggests_what_make_then_accepts(world, repo):
    account_applied(repo)
    rehearse(repo)
    assert world.next("prod", OPTED_OUT) == world.next("prod")
    assert world.next("prod", OPTED_OUT)[0] == "make release-plan ENV=prod"
    assert guard_accepts(world, "make release-plan ENV=prod", OPTED_OUT)
    status = world.status("prod", OPTED_OUT)["ci"]
    assert status == {"detected": True, "markers": ["CI=true"], "allow_ci_apply": True,
                      "dev_rehearse_allowed": False}


@pytest.mark.parametrize("ci", [{"CI": "false"}, {"CI": ""}, {"TF_BUILD": ""}, {"GENIERAILS_ALLOW_CI_APPLY": "1"}],
                         ids=["CI=false", "CI=empty", "TF_BUILD=empty", "opt-out-only"])
def test_not_ci_is_not_ci(world, repo, ci):
    account_applied(repo)
    rehearse(repo)
    assert world.next("prod", ci)[0] == "make release-plan ENV=prod"
    assert guard_accepts(world, "make release-plan ENV=prod", ci)
    assert world.status("prod", ci)["ci"]["detected"] is False


def test_ci_status_field(world, repo):
    status = world.status("prod", {"TF_BUILD": "True", "CI": "true"})["ci"]
    assert status["detected"] is True and status["markers"] == ["CI=true", "TF_BUILD"]
    assert status["allow_ci_apply"] is False
    assert world.status("prod")["ci"] == {"detected": False, "markers": [], "allow_ci_apply": False}
    text = world.run("status", "prod", ci={"CI": "true"}).stdout
    assert "  CI:          detected (CI=true); applying targets refuse" in text
    assert "  CI:          not detected" in world.run("status", "prod").stdout
    assert "GENIERAILS_ALLOW_CI_APPLY=1: applying targets may run" in world.run("status", "prod", ci=OPTED_OUT).stdout


def test_ci_dev_rehearse_exemption_follows_the_guard(world, repo):
    """make rehearse ENV=dev with locked remote state runs in CI (state_layout.ci_allowed): next keeps it."""
    shutil.copyfile(ROOT / "aws" / "Makefile", repo.cloud / "Makefile")
    repo.commit({"aws/envs/dev/remote_state.hcl": 'backend = "s3"\nbucket = "b"\nregion = "r"\n'
                                                  'dynamodb_table = "locks"\n'})
    account_applied(repo)
    ci = {"CI": "true"}
    by_make = world.run("next", "dev", ci=ci, via_make=True, makefile=repo.cloud / "Makefile")
    assert by_make.returncode == 0, by_make.stderr
    assert by_make.stdout.splitlines()[0] == "make rehearse ENV=dev"
    guard = subprocess.run([sys.executable, str(ROOT / "shared" / "scripts" / "ci_guard.py"), "apply",
                            "--cloud-dir", str(repo.cloud), "--cloud-root", str(repo.cloud),
                            "--env-dir", str(repo.dev), "--env", "dev"], text=True, capture_output=True,
                           env=world.environ(ci | {"GENIERAILS_MAKE_GOALS": "rehearse"}))
    assert guard.returncode == 0, guard.stderr
    # A Makefile from another checkout with CLOUD_ROOT pointed here fails ci_allowed: guard and next both refuse.
    other = world.run("next", "dev", ci=ci, via_make=True)
    assert other.stdout.splitlines()[0] == deployment_machine("make rehearse ENV=dev")
    other_guard = subprocess.run(["make", "--no-print-directory", "-f", str(ROOT / "aws" / "Makefile"), "rehearse",
                                  "ENV=dev", f"CLOUD_ROOT={repo.cloud}", f"SHARED_ROOT={ROOT / 'shared'}"],
                                 cwd=repo.cloud, text=True, capture_output=True, env=world.environ(ci))
    assert other_guard.returncode and "applying targets cannot run in CI" in other_guard.stderr
    # A legacy dev never rehearses in CI, remote state or not.
    repo.commit({"aws/envs/dev/env.auto.tfvars": LEGACY})
    legacy = world.run("next", "dev", ci=ci, via_make=True, makefile=repo.cloud / "Makefile")
    assert legacy.stdout.splitlines()[0] == deployment_machine("make rehearse ENV=dev")


def test_make_and_next_share_one_ci_detection():
    """No inline CI check is left in Makefile.shared: every one runs scripts/ci_guard.py, which make next imports."""
    text = (ROOT / "shared" / "Makefile.shared").read_text()
    assert "TF_BUILD" not in text and "ci_value" not in text and "in-ci" not in text
    # The Databricks-compute guard (scripts/databricks_compute.py) runs first, as its prerequisite.
    assert ("\n_guard-not-ci-apply: _guard-not-databricks-compute\n"
            "\t@ci_out=\"$$(GENIERAILS_MAKE_GOALS=\"$(MAKECMDGOALS)\" $(_CI_GUARD) apply ") in text
    assert "\n_guard-not-ci-rebind:\n\t@ci_out=\"$$($(_CI_GUARD) rebind)\"; ci_rc=$$?; \\\n\t$(_CI_GUARD_ANSWER)\n" in text
    assert "CI_MARKERS" not in (ROOT / "shared" / "scripts" / "staged_policy_apply.py").read_text()
    guarded = env_status.ci_guarded_targets()
    assert {"rehearse", "release", "release-plan", "maintain", "apply-account", "policy-abandon"} <= set(guarded)
    assert guarded["policy-rebind"] == "rebind"
    assert {"_release-manifest", "_apply-layer", "_destroy-layer"} <= set(guarded)  # underscore targets too
    assert not {"account-plan", "promote-to", "setup", "generate", "check", "status", "next"} & set(guarded)


@pytest.mark.parametrize("ci", [*CI_RUNS, OPTED_OUT, {"CI": "false"}, {"CI": "true", "GENIERAILS_ALLOW_CI_APPLY": "0"}],
                         ids=lambda ci: ",".join(f"{k}={v}" for k, v in ci.items()))
def test_the_real_guard_and_ci_guard_agree(world, ci):
    expected = ci_guard.apply_allowed(ci)
    assert guard_accepts(world, "make release ENV=prod", ci) is expected


def test_next_never_suggests_a_separate_make_check():
    """rehearse and release-plan run make check themselves (deterministic envs), so make next never names it."""
    source = (Path(__file__).resolve().parents[1] / "scripts" / "env_status.py").read_text()
    assert "make check" not in source
