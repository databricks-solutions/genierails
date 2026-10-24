"""make wiring of release gating (rollout step 9): rehearse, release-plan, release (current or MANIFEST=), maintain.

Real make runs the real recipes and release_gate.py against a git checkout
in tmp_path (tests/test_release_gate.py's Repo). Recursive makes, Terraform
and every live check are stubs that record their calls, so nothing reaches
Databricks.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

import pytest

from scripts import environment_lock
from scripts import release_gate as rg
from scripts import sticky_governance
from tests import script_stubs
from tests import test_prod_coverage_gap_fix as uc
from tests.test_release_gate import EMPTY_ABAC, PROD_ENV, Repo

ROOT = Path(__file__).resolve().parents[2]
LEGACY_ENV = 'promote_from = "dev"\n'

MAKE_STUB = r'''#!{python}
"""Recursive make: records its arguments; _release-plan-roots saves fake plans."""
import json, hashlib, os, sys
from pathlib import Path
args = sys.argv[1:]
with open({log!r}, "a") as log:
    log.write(json.dumps(args) + "\n")
if {fail!r} and {fail!r} in args:
    sys.exit(1)
if set(args) & (set({passthrough!r}) | {{"_account-plan-locked"}}):
    # Run the real recipe (its own recursive makes come back to this stub via MAKEFLAGS).
    os.execvp("make", ["make", *args])
if "_release-plan-roots" in args:
    plan_dir = Path(next(a.split("=", 1)[1] for a in args if a.startswith("PLAN_DIR=")))
    (plan_dir / "account.tfplan").write_bytes(b"account")
    (plan_dir / "account.json").write_text('{{"file": "account.tfplan", "changes": false}}')
    (plan_dir / "workspace.tfplan").write_bytes(b"workspace")
    (plan_dir / "workspace.json").write_text('{{"file": "workspace.tfplan", "changes": true}}')
    if Path({imports!r}).exists():  # imports and stale-tag cleanups import_existing.sh --record found
        (plan_dir / "imports.jsonl").write_text(Path({imports!r}).read_text())
    da = plan_dir / "data_access"
    da.mkdir()
    (da / "00-grant.tfplan").write_bytes(b"grant")
    (da / "phases.json").write_text(json.dumps({{
        "transition": {{"from": "a", "to": "a", "retain": "", "retire": "", "tier_move": False,
                       "phases": ["create", "grant"], "completed": [], "resumed": False}},
        "steps": [{{"phase": "grant", "kind": "apply", "file": "00-grant.tfplan",
                   "sha256": hashlib.sha256(b"grant").hexdigest(), "changes": True}}],
        "final": True}}))
'''

RECORDER = r'''#!{python}
import json, sys
from pathlib import Path
with open({log!r}, "a") as log:
    log.write(json.dumps([{name!r}] + sys.argv[1:]) + "\n")
args = sys.argv[1:]
if {name!r} == "runner" and len(args) >= 3 and args[0] == "account" and args[2] == "plan":
    for arg in args:
        if arg.startswith("-out="):
            Path(arg.split("=", 1)[1]).write_bytes(b"account saved plan")
    sys.exit(2)
if {name!r} == "runner" and len(args) >= 3 and args[0] == "account" and args[2] == "show":
    print(json.dumps({{"resource_changes": [{{"change": {{"actions": ["create"]}}}}]}}))
    sys.exit(0)
if {name!r} == "reports" and "--plan-dir" in args:
    Path(args[args.index("--plan-dir") + 1], "reports.json").write_text(json.dumps({{"ok": True}}))
if {name!r} == "live":
    print(Path({live!r}).read_text().strip())
sys.exit(int(Path({rc!r}).read_text()) if Path({rc!r}).exists() else 0)
'''


class Make:
    def __init__(self, tmp_path: Path, repo: Repo, cloud: str = "aws", fail: str = "", passthrough=()):
        self.tmp, self.repo, self.cloud = tmp_path, repo, cloud
        self.log = tmp_path / "calls.jsonl"
        self.live = tmp_path / "live.txt"
        self.live.write_text("live-1\n")
        self.imports = tmp_path / "imports.jsonl"
        self.make = self._write("make-stub", MAKE_STUB.format(python=sys.executable, log=str(self.log), fail=fail,
                                                              passthrough=list(passthrough),
                                                              imports=str(self.imports)))
        self.stubs = {name: self._recorder(name) for name in
                      ("staged", "gate", "runner", "reports", "live", "inventory", "classification", "audit")}

    def _write(self, name: str, text: str) -> Path:
        path = self.tmp / name
        path.write_text(text)
        path.chmod(0o755)
        return path

    def _recorder(self, name: str) -> Path:
        return self._write(f"{name}-stub", RECORDER.format(python=sys.executable, log=str(self.log), name=name,
                                                           live=str(self.live), rc=str(self.tmp / f"{name}.rc")))

    def exit_code(self, name: str, rc: int) -> None:
        (self.tmp / f"{name}.rc").write_text(str(rc))

    def script_stubs(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        """The live checks, stubbed below make (the helpers themselves can't be overridden)."""
        return {
            "table_readers.py check": "true", "group_map.py": "true", "deployer_identity.py": "true",
            "saved_settings.py verify-key": "true", "staged_policy_apply.py confirm": "true",
            "external_mask_inventory.py": str(self.stubs["inventory"]),
            "classification_completeness.py check": str(self.stubs["classification"]),
            "release_gate.py live-snapshot": str(self.stubs["live"]),
            "release_gate.py reports": str(self.stubs["reports"]),
            "staged_policy_apply.py": str(self.stubs["staged"]), "coverage_gate.py": str(self.stubs["gate"]),
            "audit_schema_drift.py": str(self.stubs["audit"]), **(extra or {}),
        }

    def __call__(self, target: str, env: str, *extra: str, stub_make: bool = True, ci: bool = False,
                 stubs: dict[str, str] | None = None, environ_extra: dict[str, str] | None = None,
                 cwd: Path | None = None, root_runner: Path | None = None):
        env_dir = self.repo.cloud / "envs" / env
        args = ["make", "--no-print-directory", target, f"ENV={env}", f"ENV_DIR={env_dir}",
                f"ACCOUNT_ENV_DIR={self.repo.cloud / 'envs' / 'account'}",
                f"ROOT_RUNNER={root_runner or self.stubs['runner']}", *extra]
        if stub_make:
            args.append(f"MAKE={self.make}")
        bin_dir = script_stubs.install(self.tmp / "stub-bin", self.script_stubs(stubs))
        environ = {k: v for k, v in script_stubs.without_ci(os.environ).items()
                   if k not in ("MAKEFLAGS", "MAKELEVEL", "VERIFY_KEY_COLUMN", "APPLY_FLAGS", "MANIFEST")}
        environ |= self.repo.env | ({"CI": "true"} if ci else {}) | (environ_extra or {})
        environ["PATH"] = f"{bin_dir}:{environ['PATH']}"
        return subprocess.run(args, cwd=cwd or ROOT / self.cloud, text=True, capture_output=True, env=environ)

    def calls(self) -> list[list[str]]:
        if not self.log.exists():
            return []
        # Drop the scratch promote-to the receipt digest runs (make -C <scratch> promote-to).
        return [c for c in map(json.loads, self.log.read_text().splitlines()) if c[:1] != ["-C"]]

    def names(self) -> list[str]:
        return [next((a for a in c if not a.startswith("-") and "=" not in a), "?") for c in self.calls()]

    def reset(self) -> None:
        self.log.unlink(missing_ok=True)


@pytest.fixture
def repo(tmp_path, monkeypatch):
    return Repo(tmp_path, monkeypatch)


@pytest.fixture
def make(tmp_path, repo):
    return Make(tmp_path, repo)


def _no_lock(repo: Repo, env: str) -> bool:
    return not (repo.cloud / "envs" / env / "generated" / ".governance.lock").exists()


# -- plain release in a deterministic env ---------------------------------------------

@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_plain_release_in_a_deterministic_env_refuses_with_a_pointer_to_release_plan(tmp_path, repo, cloud):
    result = Make(tmp_path, repo, cloud)("release", "prod", stub_make=False)
    assert result.returncode != 0
    assert "has no current release manifest. Run make release-plan ENV=prod" in result.stderr
    assert "MANIFEST=<path> names one explicitly" in result.stderr
    assert _no_lock(repo, "prod")
    assert not rg.release_receipts(repo.cloud, "prod")


def test_release_hands_a_deterministic_env_to_the_manifest_release(make, repo):
    result = make("release", "prod", "MANIFEST=/m/manifest.json")
    assert result.returncode == 0, result.stderr
    assert make.calls() == [["--no-print-directory", "_release-manifest", "ENV=prod",
                             f"ENV_DIR={repo.prod}", "MANIFEST=/m/manifest.json"]]


def test_legacy_release_is_unchanged(make, repo):
    (repo.prod / "env.auto.tfvars").write_text(LEGACY_ENV)
    result = make("release", "prod")
    assert result.returncode == 0, result.stdout + result.stderr
    names = make.names()
    assert "_release-manifest" not in names
    # The classification check is stubbed below make now, so it is recorded too.
    assert names[:5] == ["classification", "_verify-access-keys-readonly", "derive-assignments",
                         "validate-generated", "coverage-gate"]
    assert not rg.release_receipts(repo.cloud, "prod")


# -- release-plan -----------------------------------------------------------------------

def test_release_plan_refuses_a_legacy_env(make, repo):
    (repo.prod / "env.auto.tfvars").write_text(LEGACY_ENV)
    result = make("release-plan", "prod")
    assert result.returncode != 0 and "not in deterministic governance mode" in result.stderr
    assert make.calls() == []


def test_release_plan_refuses_an_unrehearsed_commit_before_locking_or_running_anything(make, repo):
    result = make("release-plan", "prod")
    assert result.returncode != 0
    assert "no passing rehearsal of" in result.stderr
    assert make.calls() == [] and _no_lock(repo, "prod")


def test_release_plan_refuses_a_dirty_checkout(make, repo):
    assert make("rehearse", "dev").returncode == 0
    (repo.prod / "env.auto.tfvars").write_text(PROD_ENV + "# local edit\n")
    make.reset()
    result = make("release-plan", "prod")
    assert result.returncode != 0 and "checkout is not clean" in result.stderr and make.calls() == []


def test_release_plan_writes_one_manifest_under_the_lock(make, repo):
    assert make("rehearse", "dev").returncode == 0
    make.reset()
    result = make("release-plan", "prod")
    assert result.returncode == 0, result.stdout + result.stderr
    names = make.names()
    # The live checks run before the lock; everything after them runs under it.
    assert names == ["inventory", "classification", "derive-assignments", "validate-generated", "coverage-gate",
                     "promote", "verify-access-keys", "audit-rulebook", "reports", "_release-plan-roots",
                     "live"], names
    manifest = Path(result.stdout.split("make release ENV=prod MANIFEST=")[-1].strip())
    data = json.loads(manifest.read_text())
    assert data["commit"] == repo.head() and data["live_snapshot"] == "live-1" and data["final"] is True
    assert "    make release ENV=prod\n" in result.stdout and rg.current_manifest(repo.prod, "prod") == manifest
    assert data["provenance"]["source_env"] == "dev" and data["reports"] == {"ok": True}
    assert _no_lock(repo, "prod")


def test_release_plan_refused_by_the_reports_writes_no_manifest(make, repo):
    assert make("rehearse", "dev").returncode == 0
    make.exit_code("reports", 1)
    result = make("release-plan", "prod")
    assert result.returncode != 0 and "reader or weakening check" in result.stderr
    assert not list(rg.manifests_dir(repo.cloud, "prod").glob("*/manifest.json"))
    assert not list(rg.manifests_dir(repo.cloud, "prod").glob(".pending-*"))


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_release_plan_refuses_in_ci(tmp_path, repo, cloud):
    result = Make(tmp_path, repo, cloud)("release-plan", "prod", ci=True)
    assert result.returncode != 0 and "applying targets cannot run in CI" in result.stderr


# -- account-plan / apply-account -------------------------------------------------------

def _account_planned(make: Make) -> Path:
    result = make("account-plan", "account")
    assert result.returncode == 0, result.stdout + result.stderr
    make.reset()
    return Path(next(line.split(":", 1)[1].strip() for line in result.stdout.splitlines()
                     if "Manifest (immutable):" in line))


def test_account_plan_writes_an_immutable_manifest_from_clean_origin_main(make, repo):
    result = make("account-plan", "account")
    assert result.returncode == 0, result.stdout + result.stderr
    manifest = Path(next(line.split(":", 1)[1].strip() for line in result.stdout.splitlines()
                         if "Manifest (immutable):" in line))
    data = json.loads(manifest.read_text())
    assert data["kind"] == "account-manifest"
    assert data["commit"] == repo.head()
    assert data["plan"]["sha256"] == rg.sha256_bytes(b"account saved plan")
    assert data["plan"]["summary"]["resource_changes"] == {"create": 1}
    assert data["inputs"]
    assert data["tag_sync"] == []
    runner = [c for c in make.calls() if c[0] == "runner"]
    assert runner[0][:6] == ["runner", "account", "account", "plan", "-input=false", "-detailed-exitcode"]
    assert runner[0][6].startswith("-out=") and Path(runner[0][6].split("=", 1)[1]).name == "account.tfplan"
    assert runner[1][:5] == ["runner", "account", "account", "show", "-json"]
    assert Path(runner[1][5]).name == "account.tfplan"
    assert not (manifest.parent / "account.show.json").exists()


def test_plain_account_plan_locks_the_account_env(make, repo):
    result = make("account-plan", "dev")  # the public command's default workspace env
    assert result.returncode == 0, result.stdout + result.stderr
    assert "environment lock acquired (account, account-plan)" in result.stdout
    assert "environment lock acquired (dev, account-plan)" not in result.stdout


def test_account_plan_refuses_under_apply_account_lock_and_keeps_pending_plan(make, repo):
    manifest = _account_planned(make)
    plan = manifest.parent / "account.tfplan"
    account = repo.cloud / "envs" / "account"
    ok, message, _nonce = environment_lock.acquire(account, os.getpid(), "apply-account")
    assert ok, message
    try:
        result = make("account-plan", "dev")
    finally:
        environment_lock.release_lock(account, os.getpid())
    assert result.returncode != 0
    assert "account is locked" in result.stderr and "held by make apply-account" in result.stderr
    assert plan.is_file()
    assert not [call for call in make.calls() if call[:4] == ["runner", "account", "account", "plan"]]


def test_real_layer_runner_keeps_account_plan_show_json_clean(tmp_path, repo):
    """The real wrapper's diagnostics must not corrupt account-plan's JSON input."""
    secret = "regression-client-secret-must-not-persist"
    terraform = tmp_path / "terraform"
    terraform.write_text(f'''#!/usr/bin/env python3
import json, pathlib, sys
args = sys.argv[1:]
if args[:1] == ["plan"]:
    out = next(a.split("=", 1)[1] for a in args if a.startswith("-out="))
    pathlib.Path(out).write_text({secret!r})
    sys.exit(2)
if args[:2] == ["show", "-json"]:
    print(json.dumps({{"variables": {{"databricks_client_secret": {{"value": {secret!r}}}}},
                      "resource_changes": [{{"change": {{"actions": ["create"]}}}}]}}))
''')
    terraform.chmod(0o755)
    make = Make(tmp_path, repo)
    checkout_account = ROOT / "aws/envs/account"
    existed = checkout_account.exists()
    result = make("account-plan", "account", root_runner=ROOT / "shared/scripts/terraform_layer.sh",
                  environ_extra={"PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}"})
    assert checkout_account.exists() is existed
    assert result.returncode == 0, result.stdout + result.stderr
    manifest = Path(next(line.split(":", 1)[1].strip() for line in result.stdout.splitlines()
                         if "Manifest (immutable):" in line))
    assert json.loads((manifest.parent / "account.summary.json").read_text())["resource_changes"] == {"create": 1}
    assert "+ terraform show -json" in result.stderr
    assert "+ terraform" not in result.stdout
    assert secret not in "".join(p.read_text(errors="ignore") for p in manifest.parent.iterdir()
                                 if p.name != "account.tfplan")


def test_account_plan_and_apply_account_use_the_same_overridden_account_env_dir(tmp_path, repo):
    """ACCOUNT_ENV_DIR (here the tmp checkout's envs/account, not this checkout's
    CLOUD_ROOT/envs/account) is where account-plan plans and writes its manifest,
    and where apply-account selects that manifest and applies its plan."""
    calls = tmp_path / "terraform.jsonl"
    terraform = tmp_path / "terraform"
    terraform.write_text(f'''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open({str(calls)!r}, "a") as log:
    log.write(json.dumps({{"args": args, "layer_env_dir": os.environ.get("LAYER_ENV_DIR")}}) + "\\n")
if args[:1] == ["plan"]:
    pathlib.Path(next(a.split("=", 1)[1] for a in args if a.startswith("-out="))).write_text("saved plan")
    sys.exit(2)
if args[:2] == ["show", "-json"]:
    print(json.dumps({{"resource_changes": [{{"change": {{"actions": ["create"]}}}}]}}))
''')
    terraform.chmod(0o755)
    override = repo.cloud / "envs" / "account"
    checkout_account = ROOT / "aws/envs/account"
    existed = checkout_account.exists()
    make = Make(tmp_path, repo, passthrough=("_apply-account-locked",))
    runner = ROOT / "shared/scripts/terraform_layer.sh"
    path = {"PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}"}
    planned = make("account-plan", "account", root_runner=runner, environ_extra=path)
    assert planned.returncode == 0, planned.stdout + planned.stderr
    manifest = Path(next(line.split(":", 1)[1].strip() for line in planned.stdout.splitlines()
                         if "Manifest (immutable):" in line))
    assert manifest.is_relative_to(rg.env_state(repo.cloud, "account"))
    applied = make("apply-account", "account", root_runner=runner, environ_extra=path)
    assert applied.returncode == 0, applied.stdout + applied.stderr
    assert f"Selected account manifest {manifest.parent.name} " in applied.stderr
    assert rg.account_receipts(repo.cloud, "account")[-1]["manifest"]["path"] == str(manifest)
    runs = [json.loads(line) for line in calls.read_text().splitlines()]
    commands = [run["args"][0] for run in runs if run["args"][0] != "init"]
    assert commands == ["plan", "show", "apply"]
    assert {run["layer_env_dir"] for run in runs} == {str(override)}
    plan = next(run["args"] for run in runs if run["args"][0] == "plan")
    assert f"-var-file={override / 'env.auto.tfvars'}" in plan
    assert checkout_account.exists() is existed


def test_apply_account_deletes_the_secret_bearing_saved_plan(tmp_path, repo):
    make = Make(tmp_path, repo, passthrough=("_apply-account-locked",))
    manifest = _account_planned(make)
    plan = manifest.parent / "account.tfplan"
    os.chmod(plan, 0o600)
    plan.write_text("persisted-client-secret")
    data = json.loads(manifest.read_text())
    os.chmod(manifest, 0o600)
    data["plan"]["sha256"] = rg.sha256_file(plan)
    manifest.write_text(json.dumps(data))
    os.chmod(manifest, 0o400)
    result = make("apply-account", "account", f"MANIFEST={manifest}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert rg.account_receipts(repo.cloud, "account")
    assert not plan.exists()
    assert "persisted-client-secret" not in "".join(
        p.read_text(errors="ignore") for p in rg.env_state(repo.cloud, "account").rglob("*") if p.is_file())


def test_apply_account_records_success_when_saved_plan_deletion_fails(tmp_path, repo):
    make = Make(tmp_path, repo, passthrough=("_apply-account-locked",))
    manifest = _account_planned(make)
    plan = manifest.parent / "account.tfplan"
    bin_dir = tmp_path / "delete-fail-bin"
    bin_dir.mkdir()
    rm = bin_dir / "rm"
    rm.write_text(f'''#!/bin/sh
if [ "$#" -eq 2 ] && [ "$1" = -f ] && [ "$2" = {shlex.quote(str(plan))} ]; then
  exit 1
fi
exec /bin/rm "$@"
''')
    rm.chmod(0o755)
    result = make("apply-account", "account", f"MANIFEST={manifest}",
                  environ_extra={"PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"})
    assert result.returncode == 0, result.stdout + result.stderr
    assert rg.account_receipts(repo.cloud, "account")
    assert plan.exists()
    assert str(plan) in result.stderr
    assert "contains credentials" in result.stderr and "delete it" in result.stderr


def test_apply_account_keeps_saved_plan_when_receipt_write_fails(tmp_path, repo):
    make = Make(tmp_path, repo, passthrough=("_apply-account-locked",))
    manifest = _account_planned(make)
    plan = manifest.parent / "account.tfplan"
    receipt_dir = rg.env_state(repo.cloud, "account") / "receipts" / "account"
    receipt_dir.parent.mkdir(parents=True, exist_ok=True)
    receipt_dir.write_text("blocks the receipt directory")
    result = make("apply-account", "account", f"MANIFEST={manifest}")
    assert result.returncode != 0
    assert plan.exists()
    assert not rg.account_receipts(repo.cloud, "account")


def test_account_plan_refuses_dirty_checkout_before_planning(make, repo):
    (repo.cloud / "envs" / "account" / "env.auto.tfvars").write_text("manage_groups = false\n# local\n")
    result = make("account-plan", "account")
    assert result.returncode != 0 and "checkout is not clean" in result.stderr
    assert not [c for c in make.calls() if c[0] == "runner"]


def test_account_plan_refuses_when_head_is_not_origin_main(make, repo):
    repo.commit({"aws/envs/account/env.auto.tfvars": "manage_groups = false\n# local\n"}, push=False)
    result = make("account-plan", "account")
    assert result.returncode != 0 and "not exactly origin/main" in result.stderr
    assert not [c for c in make.calls() if c[0] == "runner"]


def test_apply_account_refuses_without_any_manifest(make, repo):
    result = make("apply-account", "account")
    assert result.returncode != 0
    assert "no unreceipted account manifest for HEAD" in result.stderr
    assert not [c for c in make.calls() if c[0] == "runner"]


def test_apply_account_refuses_in_ci(make, repo):
    result = make("apply-account", "account", "MANIFEST=/m/manifest.json", ci=True)
    assert result.returncode != 0 and "applying targets cannot run in CI" in result.stderr
    assert make.calls() == []


def test_apply_account_applies_only_the_saved_plan_under_the_account_lock_and_writes_receipt(tmp_path, repo):
    make = Make(tmp_path, repo, passthrough=("_apply-account-locked",))
    manifest = _account_planned(make)
    result = make("apply-account", "account")
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"Selected account manifest" in result.stderr
    calls = make.calls()
    apply_calls = [c for c in calls if c[:4] == ["runner", "account", "account", "apply-plan"]]
    assert len(apply_calls) == 1
    applied_plan = Path(apply_calls[0][4])
    assert applied_plan.name == "account.tfplan" and applied_plan.parent != manifest.parent
    assert not applied_plan.exists()
    assert not [c for c in calls if c[0] == "runner" and c[3:4] in (["apply"], ["plan"])]
    receipts = rg.account_receipts(repo.cloud, "account")
    assert receipts and receipts[-1]["commit"] == repo.head()
    assert receipts[-1]["manifest"]["path"] == str(manifest)


def test_apply_account_explicit_manifest_override_still_works(tmp_path, repo):
    make = Make(tmp_path, repo, passthrough=("_apply-account-locked",))
    manifest = _account_planned(make)
    result = make("apply-account", "account", f"MANIFEST={manifest}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Selected account manifest" in result.stdout


@pytest.mark.parametrize("drift", ["commit", "not-origin-main", "origin-moved-ahead", "dirty", "serial", "plan", "manifest", "inputs"])
def test_apply_account_refuses_a_stale_or_tampered_manifest_and_applies_nothing(tmp_path, repo, drift):
    make = Make(tmp_path, repo, passthrough=("_apply-account-locked",))
    manifest = _account_planned(make)
    if drift == "commit":
        repo.commit({"aws/envs/account/env.auto.tfvars": "manage_groups = false\n# later\n"})
    elif drift == "not-origin-main":
        repo.commit({"aws/envs/account/env.auto.tfvars": "manage_groups = false\n# local\n"}, push=False)
    elif drift == "origin-moved-ahead":
        other = tmp_path / "other"
        subprocess.run(["git", "clone", "-q", str(repo.origin), str(other)], check=True, env=os.environ | repo.env)
        (other / "aws" / "envs" / "prod" / "ack.txt").write_text("# newer\n")
        for args in (("add", "-A"), ("commit", "-q", "-m", "newer"), ("push", "-q", "origin", "main")):
            subprocess.run(["git", "-C", str(other), *args], check=True, env=os.environ | repo.env)
    elif drift == "dirty":
        (repo.cloud / "envs" / "account" / "env.auto.tfvars").write_text("manage_groups = false\n# local\n")
    elif drift == "serial":
        state = rg.root_state_paths(repo.cloud / "envs" / "account")["account"]
        state.parent.mkdir(parents=True, exist_ok=True)
        state.write_text(json.dumps({"version": 4, "serial": 3, "lineage": "A"}))
    elif drift == "plan":
        plan = manifest.parent / "account.tfplan"
        os.chmod(plan, 0o644)
        plan.write_bytes(b"tampered")
    elif drift == "manifest":
        os.chmod(manifest, 0o644)
        data = json.loads(manifest.read_text())
        data["plan"]["file"] = "other.tfplan"
        manifest.write_text(json.dumps(data))
    else:
        (repo.cloud / "envs" / "account" / "abac.auto.tfvars").write_text("tag_policies = []\n")
    result = make("apply-account", "account", f"MANIFEST={manifest}")
    assert result.returncode != 0
    assert "Run make account-plan again" in result.stderr
    assert not [c for c in make.calls() if c[0] == "runner" and c[3:4] == ["apply-plan"]]
    assert not rg.account_receipts(repo.cloud, "account")


def test_apply_account_refuses_when_the_real_auth_endpoint_changes_after_plan(tmp_path, repo):
    make = Make(tmp_path, repo, passthrough=("_apply-account-locked",))
    auth = repo.cloud / "envs" / "account" / "auth.auto.tfvars"
    auth.write_text('databricks_account_host = "https://accounts.cloud.databricks.com"\n'
                    'databricks_account_id = "acc-123"\n'
                    'databricks_workspace_host = "https://workspace.one.example"\n'
                    'databricks_client_id = "client"\n'
                    'databricks_client_secret = "secret-not-hashed"\n')
    manifest = _account_planned(make)
    auth.write_text(auth.read_text().replace("workspace.one.example", "workspace.two.example"))
    result = make("apply-account", "account", f"MANIFEST={manifest}")
    assert result.returncode != 0
    assert "account input file hashes changed" in result.stderr
    assert not [c for c in make.calls() if c[:4] == ["runner", "account", "account", "apply-plan"]]
    assert not rg.account_receipts(repo.cloud, "account")


def test_apply_account_pins_the_setup_layout_symlinked_auth_endpoint(tmp_path, repo):
    make = Make(tmp_path, repo, passthrough=("_apply-account-locked",))
    account_auth = repo.cloud / "envs" / "account" / "auth.auto.tfvars"
    workspace_auth = repo.cloud / "envs" / "dev" / "auth.auto.tfvars"
    workspace_auth.write_text('databricks_account_id = "acc-123"\n'
                              'databricks_workspace_host = "https://workspace.one.example"\n'
                              'databricks_client_id = "client"\n'
                              'databricks_client_secret = "secret-not-hashed"\n')
    account_auth.unlink(missing_ok=True)
    account_auth.symlink_to("../dev/auth.auto.tfvars")
    manifest = _account_planned(make)
    inputs = json.loads(manifest.read_text())["inputs"]
    assert any(item["path"].startswith("auth.auto.tfvars#databricks_") for item in inputs)
    workspace_auth.write_text(workspace_auth.read_text().replace("workspace.one.example", "workspace.two.example"))
    result = make("apply-account", "account", f"MANIFEST={manifest}")
    assert result.returncode != 0
    assert "account input file hashes changed" in result.stderr
    assert not [c for c in make.calls() if c[:4] == ["runner", "account", "account", "apply-plan"]]
    assert not rg.account_receipts(repo.cloud, "account")


def test_apply_account_selects_the_newest_manifest_after_the_older_plan_was_dropped(make, repo):
    first = _account_planned(make)
    second = _account_planned(make)
    assert rg.select_account_manifest(repo.cloud / "envs" / "account", "account") == second
    assert not (first.parent / "account.tfplan").exists()


def test_apply_account_explicitly_refuses_a_superseded_account_manifest(tmp_path, repo):
    make = Make(tmp_path, repo, passthrough=("_apply-account-locked",))
    first = _account_planned(make)
    second = _account_planned(make)
    result = make("apply-account", "account", f"MANIFEST={first}")
    assert result.returncode != 0
    assert f"superseded by a newer account-plan ({second})" in result.stderr
    assert "changed or is missing" not in result.stderr


def test_apply_account_refuses_superseded_manifest_when_failed_delete_kept_its_plan(tmp_path, repo):
    make = Make(tmp_path, repo, passthrough=("_apply-account-locked",))
    first = _account_planned(make)
    old_plan = first.parent / "account.tfplan"
    os.chmod(first.parent, 0o500)
    try:
        second_result = make("account-plan", "account")
    finally:
        os.chmod(first.parent, 0o700)
    assert second_result.returncode == 0, second_result.stdout + second_result.stderr
    second = Path(next(line.split(":", 1)[1].strip() for line in second_result.stdout.splitlines()
                       if "Manifest (immutable):" in line))
    assert old_plan.is_file() and "contains credentials" in second_result.stderr

    make.reset()
    ambiguous = make("apply-account", "account")
    assert ambiguous.returncode != 0
    assert "multiple unreceipted account manifests" in ambiguous.stderr
    assert first.parent.name in ambiguous.stderr and second.parent.name in ambiguous.stderr

    make.reset()
    explicit = make("apply-account", "account", f"MANIFEST={first}")
    assert explicit.returncode != 0
    assert f"superseded by a newer account-plan ({second})" in explicit.stderr
    assert not rg.account_receipts(repo.cloud, "account")
    assert not [call for call in make.calls() if call[:4] == ["runner", "account", "account", "apply-plan"]]


def test_account_bootstrap_targets_do_not_use_the_account_manifest_flow():
    text = (ROOT / "shared" / "Makefile.shared").read_text()
    bootstrap = text[text.index("\nbootstrap-sp:"):text.index("\n# Set up the Databricks side")]
    pipelines = text[text.index("\nbootstrap-pipelines:"):text.index("\ntest-unit:")]
    assert "apply-account" not in bootstrap + pipelines
    assert "account-plan" not in bootstrap + pipelines


# -- release MANIFEST= ---------------------------------------------------------------

def _planned(make: Make) -> Path:
    assert make("rehearse", "dev").returncode == 0
    result = make("release-plan", "prod")
    assert result.returncode == 0, result.stdout + result.stderr
    make.reset()
    return Path(result.stdout.split("make release ENV=prod MANIFEST=")[-1].strip())


def test_release_applies_only_the_saved_plans_then_verifies_and_writes_a_receipt(make, repo):
    manifest = _planned(make)
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}")
    assert result.returncode == 0, result.stdout + result.stderr
    calls = make.calls()
    staged = [c for c in calls if c[0] == "staged"]
    assert staged and staged[0][1:2] == ["apply-saved"]
    assert staged[0][staged[0].index("--plan-dir") + 1] == str(manifest.parent / "data_access")
    runner = [c for c in calls if c[0] == "runner"]
    assert runner == [["runner", "workspace", "prod", "apply-plan", str(manifest.parent / "workspace.tfplan")]]
    assert not [c for c in calls if c[0] == "runner" and ("plan" in c or "apply" in c)]
    assert "_verify-access-gate" in make.names()
    receipt = rg.last_release(repo.cloud, "prod")
    assert receipt["commit"] == repo.head() and receipt["manifest"]["path"] == str(manifest)
    assert "Release the current manifest" not in result.stdout  # an explicit MANIFEST= is unchanged


def _applied(make: Make) -> list[list[str]]:
    return [c for c in make.calls() if c[0] in ("staged", "runner")]


def test_plain_release_prints_the_current_manifest_then_applies_it(make, repo):
    manifest = _planned(make)
    result = make("_release-manifest", "prod")
    assert result.returncode == 0, result.stdout + result.stderr
    out = result.stdout
    summary = out.index("=== Release the current manifest (prod) ===")
    assert f"  Manifest: {manifest}\n" in out and f"  Commit:   {repo.head()} (planned " in out
    assert "    data_access:  saved plan(s) with changes, phase(s) grant" in out
    assert "    workspace:    saved plan with changes" in out
    # The summary comes before anything is applied, and the checks and apply path are today's.
    assert summary < out.index("match.") < out.index("=== Apply saved plan (workspace:prod) ===") \
        < out.index("=== Release complete (prod) ===")
    staged = [c for c in make.calls() if c[0] == "staged"]
    assert staged[0][1:2] == ["apply-saved"]
    assert staged[0][staged[0].index("--plan-dir") + 1] == str(manifest.parent / "data_access")
    assert [c for c in make.calls() if c[0] == "runner"] == [
        ["runner", "workspace", "prod", "apply-plan", str(manifest.parent / "workspace.tfplan")]]
    assert rg.last_release(repo.cloud, "prod")["manifest"]["path"] == str(manifest)


def test_plain_release_through_the_release_target_uses_the_current_manifest(tmp_path, repo):
    make = Make(tmp_path, repo, passthrough=("_release-manifest",))
    manifest = _planned(make)
    result = make("release", "prod")
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"  Manifest: {manifest}\n" in result.stdout
    assert rg.last_release(repo.cloud, "prod")["manifest"]["path"] == str(manifest)


def _fake_uc(tmp_path: Path, table: str) -> dict[str, str]:
    """test_prod_coverage_gap_fix's fake Databricks SDK, answering ``table``'s DDL and class.* tags."""
    sdk = tmp_path / "sdk" / "databricks" / "sdk"
    (sdk / "service").mkdir(parents=True)
    (sdk.parent / "__init__.py").write_text("")
    (sdk / "__init__.py").write_text(uc.FAKE_SDK)
    (sdk / "config.py").write_text("class Config:\n    def __init__(self, **kwargs): self.__dict__.update(kwargs)\n")
    (sdk / "useragent.py").write_text("")  # else generate_abac pip-upgrades the real SDK
    (sdk / "errors.py").write_text("class NotFound(Exception):\n    pass\n")
    (sdk / "service" / "__init__.py").write_text("")
    (sdk / "service" / "sql.py").write_text(uc.FAKE_SQL)
    live = tmp_path / "uc.json"
    live.write_text(json.dumps({"tables": {table: uc.COLUMNS}, "tags": uc._tags(table.split(".")[0])}))
    return {"PYTHONPATH": str(tmp_path / "sdk"), "FAKE_UC": str(live), "PIP_NO_INDEX": "1"}


def test_plan_between_release_plan_and_release_does_not_stale_the_manifest(tmp_path, repo):
    """S9. The plan really re-derives prod from live tags and re-splits it; only the overlay may change."""
    table = uc.PROD_TABLE
    (repo.prod / "env.auto.tfvars").write_text(PROD_ENV + f'uc_tables = ["{table}"]\nsql_warehouse_id = "wh"\n')
    (repo.prod / "generated" / "abac.auto.tfvars").write_text(
        'groups = { analysts = { description = "A" }, viewers = { description = "V" } }\n' + EMPTY_ABAC)
    sticky_governance.save_governed_tables(repo.prod, [table])  # what promote-to records
    repo.commit({})
    (repo.prod / "auth.auto.tfvars").write_text('databricks_workspace_host = "https://prod.example.invalid"\n'
                                                'databricks_client_id = "c"\ndatabricks_client_secret = "s"\n')

    def committed_generated():  # the ignored .live_refresh.json and .runtime/ are the plan's to write
        return {p.name: p.read_bytes() for p in (repo.prod / "generated").iterdir()
                if p.is_file() and not p.name.startswith(".")}

    committed = committed_generated()
    live = _fake_uc(tmp_path, table)
    # release-plan derives and splits for real too, so its manifest pins what the live tags give.
    make = Make(tmp_path, repo, passthrough=("derive-assignments", "promote"))
    assert make("rehearse", "dev").returncode == 0
    result = make("release-plan", "prod", environ_extra=live)
    assert result.returncode == 0, result.stdout + result.stderr
    manifest = Path(result.stdout.split("make release ENV=prod MANIFEST=")[-1].strip())
    gate = make._write("gate-full", '#!/bin/sh\n[ "$1" = needs-derive ] && echo full\nexit 0\n')
    result = make("plan", "prod", stub_make=False, stubs={"coverage_gate.py": str(gate)}, environ_extra=live)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Derived 2 gr_treatment assignment(s)" in result.stdout
    assert "=== Split generated/ to account + data_access + workspace (prod) ===" in result.stdout
    # The live derive (not the committed, empty assignments) reached the overlay and the split.
    derived = f'entity_name = "{table}.cvv"'
    assert derived in (repo.prod / "generated" / ".runtime" / "abac.auto.tfvars").read_text()
    assert derived in (repo.prod / "data_access" / "abac.auto.tfvars").read_text()
    assert committed_generated() == committed
    assert repo.git("status", "--porcelain") == ""
    make.reset()
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert rg.last_release(repo.cloud, "prod")["manifest"]["path"] == str(manifest)


def test_plain_release_without_a_current_manifest_refuses_and_applies_nothing(make, repo):
    result = make("_release-manifest", "prod")
    assert result.returncode != 0
    assert "has no current release manifest. Run make release-plan ENV=prod" in result.stderr
    assert "Release the current manifest" not in result.stdout
    assert _applied(make) == [] and _no_lock(repo, "prod") and not rg.release_receipts(repo.cloud, "prod")


def test_plain_release_refuses_a_current_manifest_that_was_already_released(make, repo):
    manifest = _planned(make)
    assert make("_release-manifest", "prod").returncode == 0
    make.reset()
    for extra in ((), (f"MANIFEST={manifest}",)):
        result = make("_release-manifest", "prod", *extra)
        assert result.returncode != 0
        assert "was already released" in result.stderr and "Run make release-plan ENV=prod" in result.stderr
        assert _applied(make) == [] and _no_lock(repo, "prod")
    assert len(rg.release_receipts(repo.cloud, "prod")) == 1


def test_plain_release_refuses_a_current_phase_manifest_already_applied(make, repo):
    _planned(make)
    make.exit_code("staged", 10)
    assert make("_release-manifest", "prod").returncode == 0
    make.reset()
    result = make("_release-manifest", "prod")
    assert result.returncode != 0 and "was already released" in result.stderr
    assert _applied(make) == []


def test_a_newer_release_plan_supersedes_the_older_manifest(make, repo):
    older = _planned(make)
    time.sleep(1.1)  # manifest ids are per second
    result = make("release-plan", "prod")
    assert result.returncode == 0, result.stdout + result.stderr
    newer = Path(result.stdout.split("make release ENV=prod MANIFEST=")[-1].strip())
    assert newer != older
    make.reset()
    result = make("_release-manifest", "prod", f"MANIFEST={older}")
    assert result.returncode != 0
    assert "was superseded by a newer make release-plan" in result.stderr and str(newer) in result.stderr
    assert _applied(make) == [] and _no_lock(repo, "prod")
    result = make("_release-manifest", "prod")
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"  Manifest: {newer}\n" in result.stdout
    assert rg.last_release(repo.cloud, "prod")["manifest"]["path"] == str(newer)


def test_plain_release_runs_todays_staleness_checks(make, repo):
    _planned(make)
    make.live.write_text("live-2\n")
    result = make("_release-manifest", "prod")
    assert result.returncode != 0
    assert "=== Release the current manifest (prod) ===" in result.stdout
    assert "live Unity Catalog changed" in result.stderr and "Run make release-plan ENV=prod again" in result.stderr
    assert _applied(make) == [] and not rg.release_receipts(repo.cloud, "prod") and _no_lock(repo, "prod")


@pytest.mark.parametrize("drift", ["commit", "serial", "live", "plan"])
def test_release_refuses_a_drifted_manifest_and_applies_nothing(make, repo, drift):
    manifest = _planned(make)
    if drift == "commit":
        repo.commit({"aws/envs/prod/ack.txt": "# later\n"})
    elif drift == "serial":
        state = rg.root_state_paths(repo.prod)["workspace"]
        state.parent.mkdir(parents=True, exist_ok=True)
        state.write_text(json.dumps({"version": 4, "serial": 3, "lineage": "L"}))
    elif drift == "live":
        make.live.write_text("live-2\n")
    else:
        plan = manifest.parent / "workspace.tfplan"
        os.chmod(plan, 0o644)
        plan.write_bytes(b"other")
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}")
    assert result.returncode != 0
    assert "Run make release-plan ENV=prod again" in result.stderr
    assert not [c for c in make.calls() if c[0] in ("staged", "runner")]
    assert not rg.release_receipts(repo.cloud, "prod") and _no_lock(repo, "prod")


def test_a_phase_manifest_records_the_phase_and_writes_no_release_receipt(make, repo):
    manifest = _planned(make)
    make.exit_code("staged", 10)
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}")
    assert result.returncode == 0, result.stderr
    assert "make release-plan ENV=prod" in result.stdout
    assert not rg.release_receipts(repo.cloud, "prod")
    assert [r["id"] for r in rg.phase_records(repo.cloud, "prod", repo.head())]
    assert not [c for c in make.calls() if c[0] == "runner"]  # the workspace waits for the last phase


CLASSIFICATION_IMPORT = {"kind": "import", "layer": "data_access", "id": "catalogs/cat/config",
                         "address": 'module.data_access.databricks_data_classification_catalog_config.classification["cat"]'}
CLEANUP = {"kind": "cleanup", "layer": "data_access", "entity_type": "tables", "entity_name": "cat.sch.t",
           "tag_key": "sensitivity", "statement": "ALTER TABLE `cat`.`sch`.`t` UNSET TAGS ('sensitivity')",
           "warehouse_id": "wh"}
# What an import-only manifest must never carry: it skips the agent preflight, tag sync and verification.
FORGED = {
    "treatment-cleanup": CLEANUP | {"entity_type": "columns", "entity_name": "cat.sch.t.ssn", "tag_key": "gr_treatment",
                                    "statement": "ALTER TABLE `cat`.`sch`.`t` ALTER COLUMN `ssn` UNSET TAGS ('gr_treatment')"},
    "treatment-cleanup-upper": CLEANUP | {"entity_type": "columns", "entity_name": "cat.sch.t.ssn", "tag_key": "GR_TREATMENT",
                                          "statement": "ALTER TABLE `cat`.`sch`.`t` ALTER COLUMN `ssn` UNSET TAGS ('GR_TREATMENT')"},
    "treatment-tag-import-mixed-case": CLASSIFICATION_IMPORT | {
        "address": 'module.data_access.databricks_entity_tag_assignment.assignments["columns|cat.sch.t.ssn|Gr_Treatment|redact"]',
        "id": "columns,cat.sch.t.ssn,Gr_Treatment"},
    "arbitrary-sql": CLEANUP | {"statement": "GRANT SELECT ON CATALOG cat TO `account users`"},
    "unsupported-address": CLASSIFICATION_IMPORT | {"address": 'module.data_access.databricks_grant.anything["cat"]'},
    "foreign-warehouse": CLEANUP | {"warehouse_id": "someone-elses"},
}


@pytest.mark.parametrize("forged", FORGED, ids=list(FORGED))
def test_release_plan_refuses_a_record_entry_it_never_writes(make, repo, forged):
    make.imports.write_text(json.dumps(CLASSIFICATION_IMPORT) + "\n" + json.dumps(FORGED[forged]) + "\n")
    assert make("rehearse", "dev").returncode == 0
    result = make("release-plan", "prod")
    assert result.returncode != 0
    assert "entry 2:" in result.stderr and "never records" in result.stderr
    assert not rg.manifests_dir(repo.cloud, "prod").exists() or not list(rg.manifests_dir(repo.cloud, "prod").iterdir())


@pytest.mark.parametrize("forged", FORGED, ids=list(FORGED))
def test_an_import_manifest_with_an_entry_release_plan_never_records_is_refused(make, repo, forged):
    """A manifest whose record and hash agree (forged, or written before the check) still runs nothing."""
    make.imports.write_text(json.dumps(CLASSIFICATION_IMPORT) + "\n")
    manifest = _planned(make)
    record = manifest.parent / "imports.jsonl"
    entries = [CLASSIFICATION_IMPORT, FORGED[forged]]
    for path in (record, manifest):
        os.chmod(path, 0o644)
    record.write_text("".join(json.dumps(e) + "\n" for e in entries))
    body = json.loads(manifest.read_text())
    body["imports"] |= {"count": 2, "sha256": rg.sha256_file(record), "entries": entries}
    manifest.write_text(json.dumps(body))
    importer = make._recorder("importer")
    (repo.prod / "data_access").mkdir(exist_ok=True)
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", f"IMPORT_EXISTING_SCRIPT={importer}")
    assert result.returncode != 0 and "entry 2:" in result.stderr and "never records" in result.stderr
    assert not [c for c in make.calls() if c[0] in ("importer", "staged", "runner")]
    assert not rg.phase_records(repo.cloud, "prod", repo.head())


def test_an_import_manifest_runs_only_its_recorded_imports_under_the_lock(make, repo):
    """release-plan recorded imports instead of running them; release runs exactly those, then stops."""
    make.imports.write_text(json.dumps(CLASSIFICATION_IMPORT) + "\n")
    manifest = _planned(make)
    assert json.loads(manifest.read_text())["imports"]["count"] == 1
    importer = make._recorder("importer")
    (repo.prod / "data_access").mkdir(exist_ok=True)
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", f"IMPORT_EXISTING_SCRIPT={importer}")
    assert result.returncode == 0, result.stdout + result.stderr
    calls = make.calls()
    assert [c for c in calls if c[0] == "importer"] == [
        ["importer", "--skip-treatment-tags", f"--apply-recorded={manifest.parent / 'imports.jsonl'}"]]
    # Nothing else: no saved plan, no tag-policy sync, no workspace apply, no release receipt.
    assert not [c for c in calls if c[0] in ("staged", "runner")] and "sync-tags" not in make.names()
    assert [r["id"] for r in rg.phase_records(repo.cloud, "prod", repo.head())]
    assert not rg.release_receipts(repo.cloud, "prod") and _no_lock(repo, "prod")
    assert "make release-plan ENV=prod" in result.stdout


def test_an_import_manifest_whose_record_changed_is_refused(make, repo):
    make.imports.write_text(json.dumps(CLASSIFICATION_IMPORT) + "\n")
    manifest = _planned(make)
    record = manifest.parent / "imports.jsonl"
    os.chmod(record, 0o644)
    record.write_text(json.dumps(CLASSIFICATION_IMPORT | {"id": "catalogs/other/config"}) + "\n")
    importer = make._recorder("importer")
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", f"IMPORT_EXISTING_SCRIPT={importer}")
    assert result.returncode != 0 and "the recorded imports" in result.stderr
    assert not [c for c in make.calls() if c[0] in ("importer", "staged", "runner")]


def test_a_failed_saved_plan_writes_no_release_receipt(make, repo):
    manifest = _planned(make)
    make.exit_code("staged", 1)
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}")
    assert result.returncode != 0 and not rg.release_receipts(repo.cloud, "prod")


# -- maintain --------------------------------------------------------------------------

def test_maintain_refuses_before_any_release(make, repo):
    result = make("maintain", "prod")
    assert result.returncode != 0 and "no release receipt yet" in result.stderr
    assert make.calls() == [] and _no_lock(repo, "prod")


def test_maintain_runs_only_from_the_released_commit(make, repo):
    manifest = _planned(make)
    assert make("_release-manifest", "prod", f"MANIFEST={manifest}").returncode == 0
    make.reset()
    result = make("maintain", "prod")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "apply-governance" in make.names()
    repo.commit({"aws/envs/prod/ack.txt": "# newer, unreleased\n"})
    make.reset()
    result = make("maintain", "prod")
    assert result.returncode != 0 and "but the last release is" in result.stderr and make.calls() == []


def test_legacy_maintain_is_unchanged(make, repo):
    (repo.prod / "env.auto.tfvars").write_text(LEGACY_ENV)
    result = make("maintain", "prod")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "apply-governance" in make.names()


def test_maintain_verify_key_failure_stops_before_mask_proof_and_apply(make, repo):
    (repo.prod / "env.auto.tfvars").write_text(LEGACY_ENV)
    proof = make._recorder("proof")
    result = make(
        "maintain",
        "prod",
        "VERIFY_SPEC=/tmp/verify-spec.json",
        stubs={
            "resolve_env_config.py verify-key": "false",
            "release_helpers.py require-mask-proof": str(proof),
        },
    )
    assert result.returncode != 0, result.stdout + result.stderr
    assert "maintain: could not resolve the verification key" in result.stderr
    assert "proof" not in make.names()
    assert "apply-governance" not in make.names()


# -- rehearse --------------------------------------------------------------------------

def test_rehearse_writes_a_receipt_and_reuses_it_without_reapplying(make, repo):
    result = make("rehearse", "dev")
    assert result.returncode == 0, result.stdout + result.stderr
    assert make.names() == ["_derive-before-exposure", "validate-generated", "coverage-gate", "apply",
                            "_verify-access-gate"]
    first = rg.passing_receipt(repo.cloud, "dev", repo.head())
    assert first and first["applied"] is True and first["targets"]["prod"]["digest"]
    commit = repo.commit({"aws/envs/prod/ack.txt": "# prod-only change\n"})
    make.reset()
    result = make("rehearse", "dev")
    assert result.returncode == 0, result.stdout + result.stderr
    assert make.calls() == [] and "without re-applying" in result.stdout
    assert rg.passing_receipt(repo.cloud, "dev", commit)["reused_from"] == first["commit"]
    assert _no_lock(repo, "dev")


@pytest.mark.parametrize(
    ("extra", "expected"),
    [
        ((), ["_verify-access-gate", "ENV=dev", "VERIFY_FROM_TFVARS=", "VERIFY_ENV_FILE=", "VERIFY_PRE_GRANT="]),
        (("VERIFY_KEY_COLUMN=customer_id",),
         ["_verify-access-gate", "ENV=dev", "VERIFY_FROM_TFVARS=", "VERIFY_ENV_FILE=", "VERIFY_PRE_GRANT=",
          "VERIFY_KEY_COLUMN=customer_id"]),
    ],
    ids=["autopick", "explicit-key"],
)
def test_deterministic_rehearse_pins_verify_access_arguments(make, extra, expected):
    result = make("rehearse", "dev", *extra)
    assert result.returncode == 0, result.stdout + result.stderr
    assert [call for call in make.calls() if call[:1] == ["_verify-access-gate"]] == [expected]


def test_a_failed_rehearse_step_writes_a_failed_receipt(tmp_path, repo):
    make = Make(tmp_path, repo, fail="coverage-gate")
    result = make("rehearse", "dev")
    assert result.returncode != 0
    receipts = rg.rehearse_receipts(repo.cloud, "dev")
    assert [r["result"] for r in receipts] == ["failed"]
    assert make.names() == ["_derive-before-exposure", "validate-generated", "coverage-gate"]


def test_legacy_rehearse_writes_no_receipt(make, repo):
    (repo.dev / "env.auto.tfvars").write_text('genie_space_id = "x"\n')
    result = make("rehearse", "dev")
    assert result.returncode == 0, result.stdout + result.stderr
    assert not rg.rehearse_receipts(repo.cloud, "dev")


# -- applying runs never rewrite committed files ----------------------------------------

DERIVE_STUB = r'''#!{python}
import sys
from pathlib import Path
args = sys.argv[1:]
open({log!r}, "a").write(" ".join(args) + "\n")
config = Path(args[args.index("--config") + 1])
if "--out-dir" in args:
    config = Path(args[args.index("--out-dir") + 1]) / config.name
config.write_text(config.read_text() + "# derived from live tags\n")
'''


@pytest.mark.parametrize("mode", ["deterministic", "legacy"])
def test_derive_assignments_writes_the_overlay_in_deterministic_envs_only(tmp_path, repo, mode):
    if mode == "legacy":
        (repo.dev / "env.auto.tfvars").write_text('genie_space_id = "x"\n')
        repo.commit({})
    log = tmp_path / "derive.log"
    derive = tmp_path / "derive.py"
    derive.write_text(DERIVE_STUB.format(python=sys.executable, log=str(log)))
    derive.chmod(0o755)
    committed = repo.dev / "generated" / "abac.auto.tfvars"
    before = committed.read_text()
    environ = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL")} | repo.env
    bin_dir = script_stubs.install(tmp_path / "stub-bin", {"derive_assignments.py": str(derive)})
    environ["PATH"] = f"{bin_dir}:{environ['PATH']}"
    result = subprocess.run(["make", "--no-print-directory", "derive-assignments", "ENV=dev", f"ENV_DIR={repo.dev}",
                             f"ACCOUNT_ENV_DIR={repo.cloud / 'envs' / 'account'}"], cwd=ROOT / "aws", text=True,
                            capture_output=True, env=environ)
    assert result.returncode == 0, result.stdout + result.stderr
    if mode == "deterministic":
        assert "--out-dir" in log.read_text()
        assert committed.read_text() == before
        overlay = repo.dev / "generated" / ".runtime" / "abac.auto.tfvars"
        assert overlay.read_text().endswith("# derived from live tags\n")
        assert repo.git("status", "--porcelain") == ""
    else:
        assert "--out-dir" not in log.read_text()
        assert committed.read_text().endswith("# derived from live tags\n")  # as before step 9


def test_the_same_env_split_reads_the_overlay_and_writes_nothing_committed(tmp_path, repo):
    from scripts import runtime_overlay as ro

    repo.commit({"aws/envs/dev/generated/abac.auto.tfvars":
                 'groups = {\n  "analysts" = { description = "Analysts" }\n}\n' + EMPTY_ABAC,
                 "aws/envs/dev/generated/masking_functions.sql": ""})
    ro.begin(repo.dev)
    ro.seal(repo.dev)
    environ = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL")} | repo.env
    result = subprocess.run(["make", "--no-print-directory", "promote", "ENV=dev", f"ENV_DIR={repo.dev}",
                             f"ACCOUNT_ENV_DIR={repo.cloud / 'envs' / 'account'}",
                             "SOURCE_ENV=dev", f"SOURCE_ENV_DIR={repo.dev}", "DEST_ENV=", "DEST_ENV_DIR="],
                            cwd=ROOT / "aws", text=True, capture_output=True, env=environ)
    assert result.returncode == 0, result.stdout + result.stderr
    assert (repo.dev / "data_access" / "abac.auto.tfvars").is_file()
    assert repo.git("status", "--porcelain") == ""


def test_a_deterministic_promotion_never_reads_prod_state_or_lists_live_policies():
    text = (ROOT / "shared" / "Makefile.shared").read_text()
    promote = text[text.index("\npromote:"):text.index("\n# make promote-to")]
    assert 'name_flags=(--state-env-dir "$$dest_env_dir" --auth' in promote
    assert ('if [ "$$($(_IS_DETERMINISTIC) "$(SOURCE_ENV_DIR)/env.auto.tfvars" 2>/dev/null)" = 1 ]; then '
            "name_flags=(--pure); fi") in promote
    assert '"$${name_flags[@]}"' in promote


def test_make_check_regenerates_without_the_overlay():
    from scripts import check_env

    seen = {}

    class Done(Exception):
        pass

    def fake_run(command, env):
        seen.update(env)
        raise Done

    scratch = check_env.Scratch.__new__(check_env.Scratch)
    scratch.cloud, scratch.make_bin = Path("/x"), "make"
    original = subprocess.run
    subprocess.run = fake_run
    try:
        with pytest.raises(Done):
            scratch.make("promote", "ENV=dev")
    finally:
        subprocess.run = original
    assert seen["GENIERAILS_NO_OVERLAY"] == "1"


@pytest.mark.parametrize("cloud", ["aws", "azure"])
@pytest.mark.parametrize("target", ["release-plan", "_release-manifest", "rehearse", "maintain"])
def test_dry_runs_of_release_targets_execute_nothing(tmp_path, repo, cloud, target):
    make = Make(tmp_path, repo, cloud)
    env = "dev" if target == "rehearse" else "prod"
    result = make(target, env, "-n", "MANIFEST=/m/manifest.json")
    assert result.returncode == 0, result.stderr
    assert make.calls() == [] and not rg.state_root(repo.cloud).exists()
    assert shlex.split  # the recipe is printed, not run


# -- the real lock opens the side-door gate for the commands that hold it ----------------

def test_rehearse_applies_dev_through_the_gate_with_its_own_lock(tmp_path, repo):
    """rehearse's real _ENV_LOCK exports its nonce; the real apply recipe it runs passes the gate."""
    make = Make(tmp_path, repo, passthrough=("apply",))
    result = make("rehearse", "dev")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "refused" not in result.stderr
    assert [c for c in make.calls() if "_apply-layer" in c]  # recorded from inside the real apply
    assert rg.passing_receipt(repo.cloud, "dev", repo.head())["applied"] is True


def test_maintain_applies_prod_governance_through_the_gate_with_its_own_lock(tmp_path, repo):
    plain = Make(tmp_path, repo)
    manifest = _planned(plain)
    assert plain("_release-manifest", "prod", f"MANIFEST={manifest}").returncode == 0
    make = Make(tmp_path, repo, passthrough=("apply-governance",))
    result = make("maintain", "prod")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "refused" not in result.stderr
    assert [c for c in make.calls() if "_apply-layer" in c and "LAYER=data_access" in c]


# -- migrate-plan ----------------------------------------------------------------------

INVENTORY_FAILS = """#!/bin/sh
echo "ERROR: external-mask inventory failed: governed_tables.json is missing" >&2
exit 2
"""
INVENTORY_FINDS = """#!/bin/sh
echo "ERROR: a mask GenieRails didn't create can resolve on a governed column" >&2
echo "  - cat.sch.t.email: rogue_mask" >&2
exit 1
"""


@pytest.mark.parametrize("inventory, expected, not_expected", [
    (INVENTORY_FAILS, "the external-mask inventory failed: governed_tables.json is missing; fix that and re-run",
     "block the cutover until they are removed"),
    (INVENTORY_FINDS, "masks GenieRails didn't create block the cutover until they are removed",
     "inventory failed"),
])
def test_migrate_plan_tells_a_failed_inventory_from_found_external_masks(make, repo, inventory, expected,
                                                                         not_expected):
    stub = make._write("inventory-case", inventory)
    result = make("migrate-plan", "prod", stubs={"external_mask_inventory.py": str(stub),
                                                 "migration.py report": "true"})
    assert result.returncode != 0, result.stdout + result.stderr  # still refused either way
    assert expected in result.stderr and not_expected not in result.stderr
    assert "ERROR:" in result.stderr  # the inventory's own output is still shown


def test_migrate_plan_passes_when_the_inventory_and_the_report_pass(make, repo):
    result = make("migrate-plan", "prod", stubs={"migration.py report": "true"})
    assert result.returncode == 0, result.stdout + result.stderr
    assert "inventory failed" not in result.stderr and "block the cutover" not in result.stderr


# -- adopted custom-mask cases (design 18.4.1): read-only, before the env lock ----------

CASES_STUB = r'''#!{python}
"""custom_masks.py live-check: records whether the env lock was held when the cases ran."""
import json, sys
from pathlib import Path
args = sys.argv[1:]
env_dir = Path(args[args.index("--env-dir") + 1])
held = "locked" if (env_dir / "generated" / ".governance.lock").exists() else "unlocked"
with open({log!r}, "a") as log:
    log.write(json.dumps(["cases", held, env_dir.name]) + "\n")
sys.exit(int(Path({rc!r}).read_text()) if Path({rc!r}).exists() else 0)
'''


def _cases_stub(make: Make) -> dict[str, str]:
    stub = make._write("cases-stub", CASES_STUB.format(python=sys.executable, log=str(make.log),
                                                      rc=str(make.tmp / "cases.rc")))
    return {"custom_masks.py live-check": str(stub)}


def test_rehearse_runs_the_custom_mask_cases_before_the_lock_and_before_any_step(make, repo):
    stubs = _cases_stub(make)
    result = make("rehearse", "dev", stubs=stubs)
    assert result.returncode == 0, result.stdout + result.stderr
    assert make.calls()[0] == ["cases", "unlocked", "dev"]
    assert make.names() == ["cases", "_derive-before-exposure", "validate-generated", "coverage-gate", "apply",
                            "_verify-access-gate"]


def test_a_failing_custom_mask_case_stops_rehearse_before_the_lock_and_the_receipt(make, repo):
    stubs = _cases_stub(make)
    make.exit_code("cases", 1)
    result = make("rehearse", "dev", stubs=stubs)
    assert result.returncode != 0
    assert make.calls() == [["cases", "unlocked", "dev"]]
    assert not rg.rehearse_receipts(repo.cloud, "dev") and _no_lock(repo, "dev")


def test_release_plan_and_release_run_the_custom_mask_cases_before_the_lock(make, repo):
    stubs = _cases_stub(make)
    assert make("rehearse", "dev", stubs=stubs).returncode == 0
    make.reset()
    result = make("release-plan", "prod", stubs=stubs)
    assert result.returncode == 0, result.stdout + result.stderr
    assert make.calls()[:2] == [["inventory", "--env-dir", str(repo.prod)], ["cases", "unlocked", "prod"]]
    manifest = Path(result.stdout.split("make release ENV=prod MANIFEST=")[-1].strip())
    make.reset()
    result = make("_release-manifest", "prod", f"MANIFEST={manifest}", stubs=stubs)
    assert result.returncode == 0, result.stdout + result.stderr
    assert ["cases", "unlocked", "prod"] in make.calls()
    assert make.names().index("cases") < make.names().index("live")
    # A failure stops release-plan before the lock: nothing under it ran and no manifest was written.
    make.exit_code("cases", 1)
    make.reset()
    result = make("release-plan", "prod", stubs=stubs)
    assert result.returncode != 0
    assert make.names() == ["inventory", "cases"] and _no_lock(repo, "prod")


# -- a failed verify-key stops every release path ---------------------------------------

@pytest.mark.parametrize("target", ["release-plan", "_release-manifest", "legacy-release"])
def test_a_failed_verify_key_stops_before_the_mask_proof_and_applies_nothing(make, repo, target):
    proof = make._recorder("proof")
    stubs = {"resolve_env_config.py verify-key": "false", "release_helpers.py require-mask-proof": str(proof)}
    if target == "_release-manifest":
        _planned(make)
    elif target == "legacy-release":
        (repo.prod / "env.auto.tfvars").write_text(LEGACY_ENV)
    result = make("release" if target == "legacy-release" else target, "prod", stubs=stubs)
    assert result.returncode != 0, result.stdout + result.stderr
    names = make.names()
    assert "proof" not in names
    assert not [c for c in make.calls() if c[0] in ("staged", "runner")]
    assert "_release-plan-roots" not in names and "derive-assignments" not in names
    assert not rg.release_receipts(repo.cloud, "prod") and _no_lock(repo, "prod")


@pytest.mark.parametrize("mode", ["legacy", "deterministic"])
def test_maintain_stops_when_the_verify_config_path_cannot_be_resolved(make, repo, mode):
    if mode == "legacy":
        (repo.prod / "env.auto.tfvars").write_text(LEGACY_ENV)
    else:
        assert make("_release-manifest", "prod", f"MANIFEST={_planned(make)}").returncode == 0
        make.reset()
    proof = make._recorder("proof")
    stubs = {"resolve_env_config.py verify-key": str(make._write("key", "#!/bin/sh\necho id\n")),
             "release_helpers.py require-mask-proof": str(proof)}
    # Control: with the path resolved, the proof gets it and governance applies.
    result = make("maintain", "prod", "VERIFY_SPEC=masked", stubs=stubs)
    assert result.returncode == 0, result.stdout + result.stderr
    proofs = [c for c in make.calls() if c[0] == "proof"]
    assert proofs and proofs[0][proofs[0].index("--tfvars") + 1] == str(repo.prod / "generated" / "abac.auto.tfvars")
    assert "apply-governance" in make.names()
    make.reset()

    result = make("maintain", "prod", "VERIFY_SPEC=masked", stubs=stubs | {"runtime_overlay.py path": "false"})
    assert result.returncode != 0, result.stdout + result.stderr
    assert "maintain: could not resolve the generated ABAC config; governance was not applied." in result.stderr
    assert "proof" not in make.names() and "apply-governance" not in make.names()
    assert _no_lock(repo, "prod")
