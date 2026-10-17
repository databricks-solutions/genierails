"""Rollout step 12b: GENIERAILS_STATE_DIR, dev remote state and the CI exception.

No Terraform, cloud or Databricks call: Terraform is a fake on PATH, and every
make or runner subprocess gets a minimal environment (no credentials, a
throwaway HOME).
"""

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import environment_lock, release_gate as rg, state_layout

ROOT = Path(__file__).resolve().parents[2]
SHARED = ROOT / "shared"
RUNNER = SHARED / "scripts" / "terraform_layer.sh"
S3 = 'backend = "s3"\nbucket = "b"\nregion = "r"\ndynamodb_table = "locks"\n'
AZURE = 'backend = "azurerm"\nresource_group_name = "r"\nstorage_account_name = "s"\ncontainer_name = "c"\n'
FAKE_TERRAFORM = """#!/usr/bin/env bash
printf '%s\\n' "$*" >> "$FAKE_TF_LOG"
case "$1 $2" in
  "state pull") [ -n "${FAKE_TF_FAIL:-}" ] && { echo "backend unreachable" >&2; exit 1; }; cat "$FAKE_TF_STATE" ;;
esac
exit 0
"""


def _remote(cloud: str, env_dir: Path, text: str | None = None) -> Path:
    env_dir.mkdir(parents=True, exist_ok=True)
    (env_dir / "remote_state.hcl").write_text(text or (S3 if cloud == "aws" else AZURE))
    return env_dir


def _python_path(tmp_path: Path) -> str:
    """PATH with python3 (and python) first resolving to this interpreter, so subprocesses
    use the same Python and python-hcl2 as the test. Only those two names: the
    interpreter's own bin dir may also hold a make that would shadow GNU Make."""
    bin_dir = tmp_path / "pybin"
    if not bin_dir.is_dir():
        bin_dir.mkdir()
        for name in ("python3", "python"):
            (bin_dir / name).symlink_to(sys.executable)
    return f"{bin_dir}{os.pathsep}{os.environ['PATH']}"


def _hermetic_env(tmp_path: Path, **extra) -> dict:
    """PATH and a throwaway HOME only: no cloud, Databricks or CI variables leak in."""
    home = tmp_path / "home"
    home.mkdir(exist_ok=True)
    return {"PATH": _python_path(tmp_path), "HOME": str(home), "LANG": "C"} | extra


@pytest.fixture
def fake_terraform(tmp_path):
    bin_dir = tmp_path / "fakebin"
    bin_dir.mkdir()
    (bin_dir / "terraform").write_text(FAKE_TERRAFORM)
    (bin_dir / "terraform").chmod(0o755)
    log, state = tmp_path / "tf.log", tmp_path / "pulled.json"
    log.touch()
    state.write_text("")
    return {"PATH": f"{bin_dir}{os.pathsep}{_python_path(tmp_path)}", "FAKE_TF_LOG": str(log),
            "FAKE_TF_STATE": str(state), "log": log, "state": state}


def _use_fake(monkeypatch, fake):
    for key in ("PATH", "FAKE_TF_LOG", "FAKE_TF_STATE"):
        monkeypatch.setenv(key, fake[key])


def _run_layer(tmp_path, fake, layer, env_name, command, layer_env_dir, envs_dir, **extra):
    env = _hermetic_env(tmp_path, PATH=fake["PATH"], FAKE_TF_LOG=fake["FAKE_TF_LOG"],
                        FAKE_TF_STATE=fake["FAKE_TF_STATE"], LAYER_ENV_DIR=str(layer_env_dir),
                        ENVS_DIR=str(envs_dir), **extra)
    return subprocess.run([str(RUNNER), layer, env_name, command], env=env, text=True, capture_output=True)


def _cloud_copy(tmp_path: Path, cloud: str) -> Path:
    """A cloud dir with the real Makefile (SHARED_ROOT passed on the command line)."""
    cloud_dir = tmp_path / cloud
    (cloud_dir / "envs").mkdir(parents=True)
    shutil.copyfile(ROOT / cloud / "Makefile", cloud_dir / "Makefile")
    return cloud_dir


def _make(tmp_path, cloud_dir, *args, **env):
    return subprocess.run(["make", "--no-print-directory", "-C", str(cloud_dir), *args, f"SHARED_ROOT={SHARED}"],
                          env=_hermetic_env(tmp_path, **env), text=True, capture_output=True, timeout=60)


# -- B1: the CI exception is exactly make rehearse ENV=dev on <cloud>/envs/dev ---

@pytest.fixture
def goals_rehearse(monkeypatch):
    monkeypatch.setenv("GENIERAILS_MAKE_GOALS", "rehearse")


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_ci_exception_is_only_canonical_unpromoted_dev(tmp_path, cloud, goals_rehearse):
    cloud_dir = tmp_path / cloud
    dev = _remote(cloud, cloud_dir / "envs" / "dev")
    assert state_layout.ci_allowed(dev, "dev", cloud_dir, cloud_dir)
    for name in ("stg", "qa", "prod"):
        other = _remote(cloud, cloud_dir / "envs" / name)
        assert not state_layout.ci_allowed(other, name, cloud_dir)
        assert not state_layout.ci_allowed(other, "dev", cloud_dir)  # ENV=dev aimed at another env's dir
    outside = _remote(cloud, tmp_path / "elsewhere" / "envs" / "dev")
    assert not state_layout.ci_allowed(outside, "dev", cloud_dir)
    alias = tmp_path / "alias"
    alias.symlink_to(dev)
    assert not state_layout.ci_allowed(alias, "dev", cloud_dir)
    envs_alias = tmp_path / "envs-alias"
    envs_alias.symlink_to(cloud_dir / "envs")
    assert not state_layout.ci_allowed(envs_alias / "dev", "dev", cloud_dir)
    assert not state_layout.ci_allowed(dev, "dev", cloud_dir, tmp_path / "other-cloud-root")
    (dev / "env.auto.tfvars").write_text('promote_from = "qa"\n')
    assert not state_layout.ci_allowed(dev, "dev", cloud_dir)


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_ci_exception_needs_valid_locked_remote_state(tmp_path, cloud, goals_rehearse):
    cloud_dir = tmp_path / cloud
    dev = cloud_dir / "envs" / "dev"
    dev.mkdir(parents=True)
    assert not state_layout.ci_allowed(dev, "dev", cloud_dir)  # local state
    _remote(cloud, dev, 'backend = "s3"\nbucket = "b"\nregion = "r"\n' if cloud == "aws" else 'backend = "azurerm"\n')
    assert not state_layout.ci_allowed(dev, "dev", cloud_dir)


@pytest.mark.parametrize("cloud", ["aws", "azure"])
@pytest.mark.parametrize("env_name", ["stg", "qa", "prod"])
def test_real_make_refuses_rehearse_of_other_envs_in_ci(tmp_path, cloud, env_name):
    cloud_dir = _cloud_copy(tmp_path, cloud)
    _remote(cloud, cloud_dir / "envs" / env_name)
    proc = _make(tmp_path, cloud_dir, "rehearse", f"ENV={env_name}", CI="true")
    assert proc.returncode and "applying targets cannot run in CI" in proc.stderr


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_real_make_refuses_env_dev_aimed_at_another_dir_in_ci(tmp_path, cloud):
    cloud_dir = _cloud_copy(tmp_path, cloud)
    prod = _remote(cloud, cloud_dir / "envs" / "prod")
    proc = _make(tmp_path, cloud_dir, "rehearse", "ENV=dev", f"ENV_DIR={prod}", CI="true")
    assert proc.returncode and "applying targets cannot run in CI" in proc.stderr


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_real_make_passes_the_guard_then_refuses_legacy_dev_in_ci(tmp_path, cloud):
    cloud_dir = _cloud_copy(tmp_path, cloud)
    _remote(cloud, cloud_dir / "envs" / "dev")
    proc = _make(tmp_path, cloud_dir, "rehearse", "ENV=dev", CI="true")
    assert proc.returncode
    assert "applying targets cannot run in CI" not in proc.stderr
    assert "uses legacy governance, which the CI dev pipeline doesn't run" in proc.stderr


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_runner_reads_the_layer_dirs_remote_state_not_one_rebuilt_from_envs_dir(tmp_path, cloud, fake_terraform):
    dev = _remote(cloud, tmp_path / cloud / "envs" / "dev")
    elsewhere = tmp_path / "other-envs"
    (elsewhere / "dev").mkdir(parents=True)
    proc = _run_layer(tmp_path, fake_terraform, "workspace", "dev", "state-list", dev, elsewhere)
    assert proc.returncode == 0, proc.stderr
    init = fake_terraform["log"].read_text().splitlines()[0]
    assert "-backend-config=key=genierails/dev/workspace.tfstate" in init
    assert "-backend-config=path=" not in init


@pytest.mark.parametrize("command", ["apply", "apply-plan", "destroy", "import", "state-rm", "state-mv"])
def test_runner_refuses_local_backend_writes_in_ci(tmp_path, fake_terraform, command):
    dev = tmp_path / "aws" / "envs" / "dev"
    dev.mkdir(parents=True)
    proc = _run_layer(tmp_path, fake_terraform, "workspace", "dev", command, dev, dev.parent, CI="true")
    assert proc.returncode == 2 and "the backend is local" in proc.stderr
    assert fake_terraform["log"].read_text() == ""


def test_runner_never_falls_back_to_local_state_on_an_invalid_backend(tmp_path, fake_terraform):
    dev = _remote("aws", tmp_path / "aws" / "envs" / "dev", 'backend = "s3"\nbucket = "b"\n')
    proc = _run_layer(tmp_path, fake_terraform, "workspace", "dev", "plan", dev, dev.parent)
    assert proc.returncode != 0 and "incomplete" in proc.stderr
    assert fake_terraform["log"].read_text() == ""


# -- B2: the generated remote root ---------------------------------------------

def test_remote_root_is_rebuilt_from_the_current_checkout(tmp_path, monkeypatch):
    shared = tmp_path / "checkout" / "shared"
    source = shared / "roots" / "workspace"
    source.mkdir(parents=True)
    (shared / "modules" / "m").mkdir(parents=True)
    (shared / "modules" / "m" / "main.tf").write_text("# first\n")
    (shared / "scripts").mkdir()
    (shared / "deploy_masking_functions.py").write_text("first")
    (source / "main.tf").write_text('terraform {\n  backend "local" {}\n}\n')
    (source / "old.tf").write_text("# removed upstream later\n")
    env = _remote("aws", tmp_path / "aws" / "envs" / "dev")
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))

    root = state_layout.prepare_root(env, "dev", "workspace", source)
    assert 'backend "s3" {}' in (root / "main.tf").read_text()
    assert (root / "../../deploy_masking_functions.py").resolve().read_text() == "first"
    (root / ".terraform.lock.hcl").write_text("# provider pins\n")  # what terraform init writes

    (shared / "deploy_masking_functions.py").write_text("second")
    (shared / "modules" / "m" / "main.tf").write_text("# second\n")
    (source / "old.tf").unlink()
    root = state_layout.prepare_root(env, "dev", "workspace", source)
    assert (root / "../../deploy_masking_functions.py").resolve().read_text() == "second"
    assert (root.parents[1] / "modules" / "m" / "main.tf").read_text() == "# second\n"
    assert not (root / "old.tf").exists()
    assert (root / ".terraform.lock.hcl").read_text() == "# provider pins\n"
    assert not any(p.is_symlink() for p in root.parents[2].rglob("*"))

    moved = tmp_path / "checkout-b" / "shared"  # the earlier checkout is gone
    shutil.copytree(shared, moved)
    shutil.rmtree(tmp_path / "checkout")
    root = state_layout.prepare_root(env, "dev", "workspace", moved / "roots" / "workspace")
    assert (root / "../../deploy_masking_functions.py").resolve().read_text() == "second"


@pytest.mark.parametrize("layer", ["data_access", "workspace"])
def test_remote_root_of_the_real_layers_has_what_they_reference(tmp_path, monkeypatch, layer):
    env = _remote("aws", tmp_path / "aws" / "envs" / "dev")
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    root = state_layout.prepare_root(env, "dev", layer, SHARED / "roots" / layer)
    project_root = (root / ".." / "..").resolve()
    assert (project_root / "deploy_masking_functions.py").is_file()
    assert (project_root / "scripts" / "genie_space.sh").is_file()
    assert (project_root / "modules" / layer).is_dir()
    assert 'backend "s3" {}' in (root / "main.tf").read_text()
    assert not (project_root / "tests").exists()


def test_remote_env_without_state_dir_writes_nothing_under_the_env_dir(tmp_path, fake_terraform):
    dev = _remote("aws", tmp_path / "aws" / "envs" / "dev")
    before = sorted(p.name for p in dev.rglob("*"))
    proc = _run_layer(tmp_path, fake_terraform, "workspace", "dev", "state-list", dev, dev.parent)
    assert proc.returncode == 0, proc.stderr
    assert sorted(p.name for p in dev.rglob("*")) == before
    assert (tmp_path / "aws" / ".genierails-state" / "dev" / "roots" / "workspace").is_dir()
    assert environment_lock.lock_path(dev) == tmp_path / "aws/.genierails-state/dev/locks/.governance.lock"


# -- B3: the sample dev pipeline's account plan reads the real account state ----

@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_ci_account_plan_refuses_without_account_remote_state(tmp_path, cloud):
    cloud_dir = _cloud_copy(tmp_path, cloud)
    (cloud_dir / "envs" / "account").mkdir()
    proc = _make(tmp_path, cloud_dir, "_guard-account-plan-empty", CI="true")
    assert proc.returncode and "account plan needs" in proc.stderr and "remote_state.hcl" in proc.stderr


def test_account_state_reads_the_account_backend(tmp_path, fake_terraform, monkeypatch):
    envs = tmp_path / "aws" / "envs"
    dev = _remote("aws", envs / "dev")
    _remote("aws", envs / "account", S3.replace('bucket = "b"', 'bucket = "account-bucket"'))
    fake_terraform["state"].write_text(json.dumps({"serial": 9, "lineage": "A", "resources": []}))
    _use_fake(monkeypatch, fake_terraform)
    assert state_layout.state_json(dev, "dev", "account")["serial"] == 9
    init = fake_terraform["log"].read_text().splitlines()[0]
    assert "-backend-config=bucket=account-bucket" in init
    assert "-backend-config=key=genierails/account/account.tfstate" in init


# -- B4: one state reader, and it fails closed ---------------------------------

def _write_state(path: Path, *, serial=1) -> None:
    state = {"version": 4, "serial": serial, "lineage": "L", "outputs": {
        "sql_warehouse_id": {"value": "wh-123"}}, "resources": [
        {"mode": "managed", "type": "databricks_entity_tag_assignment", "name": "tags", "instances": [
            {"index_key": "c", "attributes": {"entity_type": "columns", "tag_key": "pii", "entity_name": "c.s.t.col"}}]},
        {"mode": "managed", "type": "null_resource", "name": "genie_space_create", "instances": [
            {"index_key": "sales", "attributes": {}}]}]}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state))


@pytest.fixture
def state_dir_env(tmp_path, monkeypatch):
    env = tmp_path / "aws" / "envs" / "dev"
    (env / "data_access").mkdir(parents=True)
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    layers = tmp_path / "state" / "aws" / "dev" / "layers"
    _write_state(layers / "data_access" / "terraform.tfstate", serial=3)
    _write_state(layers / "workspace" / "terraform.tfstate", serial=4)
    return env


def test_every_reader_follows_the_state_dir(state_dir_env):
    from group_map import _ever_applied
    from scripts.genie_adopt_preflight import legacy_agents
    from scripts.resolve_env_config import _warehouse_from_local_state
    from scripts.staged_policy_apply import _read_state
    from scripts.sticky_governance import _state_tags
    from table_readers import applied_grants

    env = state_dir_env
    serials = rg.state_serials(env)
    assert serials["data_access"] == {"serial": 3, "lineage": "L"}
    assert serials["workspace"] == {"serial": 4, "lineage": "L"}
    assert _state_tags(env, "pii") == [("tags", "c", "c.s.t.col")]
    assert _read_state(env)["serial"] == 3
    assert [key for key, _ in legacy_agents(env)] == ["sales"]
    assert _warehouse_from_local_state(env) == "wh-123"
    assert _ever_applied(env)
    assert applied_grants(env) == []


def test_a_state_left_in_the_checkout_is_a_split_not_an_empty_env(tmp_path, monkeypatch):
    env = tmp_path / "aws" / "envs" / "dev"
    _write_state(env / "data_access" / "terraform.tfstate")
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    with pytest.raises(RuntimeError, match="migrate-state-dir"):
        state_layout.state_json(env, "dev", "data_access")
    with pytest.raises(rg.Refused, match="migrate-state-dir"):
        rg.state_serials(env)


def test_unreadable_or_unpullable_state_fails_closed(tmp_path, fake_terraform, monkeypatch):
    env = tmp_path / "aws" / "envs" / "dev"
    (env / "data_access").mkdir(parents=True)
    (env / "data_access" / "terraform.tfstate").write_text("{truncated")
    with pytest.raises(RuntimeError, match="cannot read"):
        state_layout.state_json(env, "dev", "data_access")
    remote = _remote("aws", tmp_path / "aws" / "envs" / "stg")
    _use_fake(monkeypatch, fake_terraform)
    monkeypatch.setenv("FAKE_TF_FAIL", "1")
    with pytest.raises(RuntimeError, match="backend unreachable"):
        state_layout.state_json(remote, "stg", "workspace")
    with pytest.raises(rg.Refused, match="backend unreachable"):
        rg.state_serials(remote)


def test_remote_state_is_read_through_terraform_state_pull(tmp_path, fake_terraform, monkeypatch):
    env = _remote("azure", tmp_path / "azure" / "envs" / "dev")
    _remote("azure", tmp_path / "azure" / "envs" / "account")
    _use_fake(monkeypatch, fake_terraform)
    assert state_layout.state_json(env, "dev", "data_access") is None  # nothing applied yet
    fake_terraform["state"].write_text(json.dumps({"serial": 12, "lineage": "R"}))
    assert rg.state_serials(env)["data_access"] == {"serial": 12, "lineage": "R"}
    assert "state pull" in fake_terraform["log"].read_text()


# Every production mention of a state file, and why it is not a read of state.
ALLOWED_STATE_MENTIONS = {
    "shared/Makefile.shared": (
        r'ls "\$\(CLOUD_ROOT\)"/terraform\.tfstate\*',  # pre-layer root state: refuse to run
        r'ls "\$\(ENV_DIR\)"/terraform\.tfstate\*',
        r'for path in "\$\(CLOUD_ROOT\)"/terraform\.tfstate\*',
        # the pre-layer make migrate-state (in-checkout layout only; it refuses otherwise)
        r'\[ ! -f "\$\(ACCOUNT_ENV_DIR\)/terraform\.tfstate" \]',
        r'\[ ! -f "\$\$target_gov_dir/terraform\.tfstate" \]',
        r'for name in abac\.auto\.tfvars masking_functions\.sql terraform\.tfstate',
        r'\[ -f "\$\(ENV_DIR\)/\$\(DATA_ACCESS_SUBDIR\)/terraform\.tfstate" \]',
        r'\[ -f "\$\(ENV_DIR\)/terraform\.tfstate" \]',
    ),
    "shared/scripts/terraform_layer.sh": (r'-backend-config="path=\$STATE_BASE/terraform\.tfstate"',
                                          # where state mv/rm write Terraform's backup (never read)
                                          r'"-backup=\$STATE_BASE/terraform\.tfstate\.'),
    "shared/scripts/sticky_governance.py": (r'^STATE = Path\("data_access/terraform\.tfstate"\)$',),  # messages
    "shared/scripts/remap_generated_config.py": (r'help="Destination data_access terraform\.tfstate\."',),
    # The roots read state with file() only from the snapshot terraform_layer.sh
    # passes (workspace_state_file, data_access_state_file); these are the
    # in-checkout defaults when it passes none.
    "shared/roots/workspace/main.tf": (
        r'^_data_access_state_file = var\.data_access_state_file != "" \? var\.data_access_state_file : '
        r'"\$\{local\.data_access_dir\}/terraform\.tfstate"$',
        r'^_own_state_file = var\.workspace_state_file != "" \? var\.workspace_state_file : '
        r'"\$\{var\.env_dir\}/terraform\.tfstate"$',
    ),
    "shared/roots/data_access/main.tf": (
        r'^_own_state_file = var\.data_access_state_file != "" \? var\.data_access_state_file : '
        r'"\$\{var\.env_dir\}/terraform\.tfstate"$',
    ),
}


def test_no_state_file_is_read_outside_state_layout():
    offenders = []
    terraform = [p for p in [*(SHARED / "roots").rglob("*.tf"), *(SHARED / "modules").rglob("*.tf")]
                 if "tests" not in p.parts]  # module test fixtures write state on purpose
    for path in sorted([*SHARED.rglob("*.py"), *SHARED.rglob("*.sh"), SHARED / "Makefile.shared", *terraform]):
        rel = path.relative_to(ROOT).as_posix()
        if (rel.startswith(("shared/tests/", "shared/examples/")) or path.name in {
                "state_layout.py", "run_integration_tests.py"}):  # the helper; the live e2e harness
            continue
        allowed = ALLOWED_STATE_MENTIONS.get(rel, ())
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if "terraform.tfstate" in line and not any(re.search(p, line.strip()) for p in allowed):
                offenders.append(f"{rel}:{number}: {line.strip()[:120]}")
    assert offenders == [], "read Terraform state through scripts/state_layout.py state_json:\n" + "\n".join(offenders)


# -- B5: a receipt counts only in the store that wrote it -----------------------

def test_a_receipt_copied_from_another_store_is_ignored(tmp_path, monkeypatch):
    cloud = tmp_path / "aws"
    (cloud / "envs" / "dev").mkdir(parents=True)
    commit = "a" * 40
    (tmp_path / "ci-runner").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "ci-runner"))
    receipt = {"kind": "rehearse", "version": rg.VERSION, "env": "dev", "commit": commit, "result": "passed",
               "clean_tree": True, "fingerprint": "f", "created_at": rg._now(), "store": rg.store_id(cloud, "dev")}
    written = rg._write_rehearse(cloud, "dev", receipt)
    assert rg.passing_receipt(cloud, "dev", commit)

    (tmp_path / "deployment-machine").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "deployment-machine"))
    copied = rg.rehearse_dir(cloud, "dev") / commit / written.name  # e.g. a downloaded CI artifact
    copied.parent.mkdir(parents=True)
    shutil.copyfile(written, copied)
    assert rg.passing_receipt(cloud, "dev", commit) is None
    assert rg.reusable_receipt(cloud, "dev", "f") is None
    rg._write_rehearse(cloud, "dev", receipt | {"store": "", "created_at": rg._now() + "x"})  # a forged one
    assert rg.passing_receipt(cloud, "dev", commit) is None
    assert len(rg.rehearse_receipts(cloud, "dev", any_store=True)) == 2


def test_remote_state_config_is_part_of_the_env_fingerprint(tmp_path):
    env = _remote("aws", tmp_path / "aws" / "envs" / "dev")
    before = rg.env_fingerprint(env)
    (env / "remote_state.hcl").write_text(S3.replace('"b"', '"other"'))
    assert rg.env_fingerprint(env) != before


# -- B7: migration -------------------------------------------------------------

def _legacy_env(tmp_path) -> Path:
    env = tmp_path / "aws" / "envs" / "dev"
    (env / "data_access").mkdir(parents=True)
    (env / "terraform.tfstate").write_text('{"serial": 1}')
    (env / "data_access" / "terraform.tfstate").write_text('{"serial": 2}')
    receipts = tmp_path / "aws" / ".genierails-state" / "dev" / "receipts" / "rehearse"
    receipts.mkdir(parents=True)
    (receipts / "one.json").write_text("immutable")
    (receipts.parent.parent / ".store-id").write_text("store-1\n")
    return env


def test_migration_moves_state_and_records_and_leaves_a_marker(tmp_path, monkeypatch):
    env = _legacy_env(tmp_path)
    target = tmp_path / "state" / "aws" / "dev"
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    assert state_layout.migrate(env, "dev") == 4
    assert (target / "layers/workspace/terraform.tfstate").read_text() == '{"serial": 1}'
    assert (target / "layers/data_access/terraform.tfstate").read_text() == '{"serial": 2}'
    assert (target / "receipts/rehearse/one.json").read_text() == "immutable"
    assert (target / ".store-id").read_text() == "store-1\n"  # receipts stay valid in their new home
    assert not (env / "terraform.tfstate").exists() and (env / ".state-moved").is_file()
    assert state_layout.state_json(env, "dev", "workspace") == {"serial": 1}

    monkeypatch.delenv("GENIERAILS_STATE_DIR")
    with pytest.raises(RuntimeError, match="state was moved"):
        state_layout.state_json(env, "dev", "workspace")
    with pytest.raises(RuntimeError, match="state was moved"):
        environment_lock.lock_path(env)
    proc = subprocess.run([str(RUNNER), "workspace", "dev", "plan"], text=True, capture_output=True,
                          env=_hermetic_env(tmp_path, LAYER_ENV_DIR=str(env), ENVS_DIR=str(env.parent)))
    assert proc.returncode == 2 and "state was moved" in proc.stderr


@pytest.mark.parametrize("held", ["in-checkout", "state-dir"])
def test_migration_refuses_while_either_lock_is_held(tmp_path, monkeypatch, held):
    env = _legacy_env(tmp_path)
    state_dir = str(tmp_path / "state")
    Path(state_dir).mkdir()  # GenieRails never creates a missing state dir
    lock_dir = "" if held == "in-checkout" else state_dir
    ok, _, _ = environment_lock.acquire(env, os.getpid(), "rehearse", state_dir=lock_dir)
    assert ok
    monkeypatch.setenv("GENIERAILS_STATE_DIR", state_dir)
    try:
        with pytest.raises(RuntimeError, match="locked, nothing was moved"):
            state_layout.migrate(env, "dev")
        assert (env / "terraform.tfstate").is_file() and not (env / ".state-moved").exists()
    finally:
        environment_lock.release_lock(env, os.getpid(), state_dir=lock_dir)


def test_migration_resumes_after_a_crash_and_never_overwrites(tmp_path, monkeypatch):
    env = _legacy_env(tmp_path)
    target = tmp_path / "state" / "aws" / "dev" / "layers"
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    # A crash after copying the workspace state, before removing its source,
    # and another mid-copy of the data_access state.
    (target / "workspace").mkdir(parents=True)
    shutil.copy2(env / "terraform.tfstate", target / "workspace" / "terraform.tfstate")
    (target / "data_access").mkdir()
    (target / "data_access" / "terraform.tfstate.migrating").write_text('{"ser')
    state_layout.migrate(env, "dev")
    assert not (env / "terraform.tfstate").exists()
    assert (target / "data_access" / "terraform.tfstate").read_text() == '{"serial": 2}'

    (env / "terraform.tfstate").write_text('{"serial": 99}')
    with pytest.raises(RuntimeError, match="refusing to overwrite"):
        state_layout.migrate(env, "dev")
    assert (target / "workspace" / "terraform.tfstate").read_text() == '{"serial": 1}'


def test_migration_needs_the_state_dir(tmp_path):
    with pytest.raises(RuntimeError, match="set GENIERAILS_STATE_DIR"):
        state_layout.migrate(_legacy_env(tmp_path), "dev")


def test_state_dir_isolates_state_receipts_and_lock(tmp_path, monkeypatch):
    env = tmp_path / "aws" / "envs" / "dev"
    env.mkdir(parents=True)
    (tmp_path / "outside").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "outside"))
    assert state_layout.env_state(env, "dev") == tmp_path / "outside" / "aws" / "dev"
    assert rg.rehearse_dir(tmp_path / "aws", "dev").is_relative_to(tmp_path / "outside")
    ok, _, _ = environment_lock.acquire(env, os.getpid(), "rehearse")
    assert ok and (tmp_path / "outside/aws/dev/locks/.governance.lock").is_file()
    assert not (env / "generated" / ".governance.lock").exists()
    environment_lock.release_lock(env, os.getpid())


# -- backends ------------------------------------------------------------------

def test_backend_allowlist_accepts_s3_lockfile_and_refuses_secrets(tmp_path):
    env = _remote("aws", tmp_path / "aws" / "envs" / "dev",
                  'backend = "s3"\nbucket = "b"\nregion = "r"\nuse_lockfile = true\n')
    assert state_layout.validate(state_layout.config(env), "aws") == (True, "")
    for key in ("access_key", "secret_key", "token", "sas_token", "client_secret", "access_key_id"):
        (env / "remote_state.hcl").write_text(
            f'backend = "s3"\nbucket = "b"\nregion = "r"\nuse_lockfile = true\n{key} = "x"\n')
        ok, reason = state_layout.validate(state_layout.config(env), "aws")
        assert not ok and "forbidden/unknown" in reason and key in reason
    azure = _remote("azure", tmp_path / "azure" / "envs" / "dev", AZURE + 'access_key = "x"\n')
    assert not state_layout.validate(state_layout.config(azure), "azure")[0]


def test_backend_args_name_each_layer_and_env(tmp_path):
    env = _remote("aws", tmp_path / "aws" / "envs" / "dev")
    assert state_layout.backend_args(env, "dev", "data_access") == [
        "-backend-config=bucket=b", "-backend-config=dynamodb_table=locks", "-backend-config=encrypt=true",
        "-backend-config=key=genierails/dev/data_access.tfstate", "-backend-config=region=r"]


def test_make_check_scratch_never_sees_remote_state_or_the_state_dir():
    from scripts import check_env

    assert "remote_state.hcl" in check_env.RUNTIME_NAMES
    assert "GENIERAILS_STATE_DIR" in check_env.LEAKED_VARS


# -- pipeline samples ----------------------------------------------------------

def _dev_samples(cloud):
    return (ROOT / cloud / ".github/workflows/dev-rehearse.yml", ROOT / cloud / "azure-pipelines/dev-rehearse.yml")


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_dev_samples_rehearse_only_the_newest_commit_in_their_paths_with_remote_state(cloud):
    for path in _dev_samples(cloud):
        text = path.read_text()
        assert 'newest="$(git log -1 --format=%H FETCH_HEAD -- . ../shared)"' in text, path
        assert '"$(git rev-parse HEAD)" != "$newest"' in text, path
        assert "envs/dev/remote_state.hcl" in text and "envs/account/remote_state.hcl" in text, path
        assert text.index("FETCH_HEAD") < text.rindex("make rehearse ENV=dev"), path
        header = text[:text.index("\non:" if ".github" in path.parts else "\ntrigger:")]
        assert "persistent checkout" not in header and "Until that exists" not in header, path


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_prod_scope_fails_loudly_when_it_cannot_diff(cloud):
    for path in (ROOT / cloud / ".github/workflows/prod-release.yml", ROOT / cloud / "azure-pipelines/prod-release.yml"):
        text = path.read_text()
        assert 'changed="$(git diff --name-only "$base" HEAD -- envs/prod ../shared)" || exit $?' in text, path
        assert "grep -q ." not in text, path


# -- readers merged from main (#130 migration, #132 scheduled job move) ----------

def _job_state(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"version": 4, "resources": [{"mode": "managed", "type": "databricks_job",
                                                            "name": "scheduled_governance", "instances": [{}]}]}))


def test_migration_reads_follow_the_state_dir(state_dir_env, tmp_path):
    from scripts import migration

    layers = tmp_path / "state" / "aws" / "dev" / "layers"
    state = json.loads((layers / "data_access" / "terraform.tfstate").read_text())
    state["resources"].append({"module": "module.data_access", "mode": "managed", "type": "databricks_entity_tag_assignment",
                               "name": "treatment", "instances": [{"index_key": "columns|c.s.t.col|pii", "attributes": {}}]})
    (layers / "data_access" / "terraform.tfstate").write_text(json.dumps(state))
    assert "stable keys" in migration.migrated(state_dir_env)


def test_scheduled_job_move_uses_the_state_dir_files(tmp_path, fake_terraform, monkeypatch):
    from scripts import move_scheduled_job

    env = tmp_path / "aws" / "envs" / "dev"
    (env / "data_access").mkdir(parents=True)
    layers = tmp_path / "state" / "aws" / "dev" / "layers"
    _job_state(layers / "workspace" / "terraform.tfstate")
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    _use_fake(monkeypatch, fake_terraform)
    assert move_scheduled_job.move(env, env / "data_access") == 0
    mv = fake_terraform["log"].read_text()
    assert f"-state={layers / 'workspace' / 'terraform.tfstate'}" in mv
    assert f"-state-out={layers / 'data_access' / 'terraform.tfstate'}" in mv


def test_scheduled_job_still_in_a_remote_workspace_state_refuses(tmp_path, fake_terraform, monkeypatch, capsys):
    from scripts import move_scheduled_job

    env = _remote("aws", tmp_path / "aws" / "envs" / "dev")
    _job_state(fake_terraform["state"])
    _use_fake(monkeypatch, fake_terraform)
    assert move_scheduled_job.move(env, env / "data_access") == 1
    assert "remote workspace state" in capsys.readouterr().err
    fake_terraform["state"].write_text("")
    assert move_scheduled_job.move(env, env / "data_access") == 0


def test_a_nested_state_read_reuses_the_callers_remote_root(tmp_path, fake_terraform):
    # The runner reads the workspace state itself (the scheduled-job move, the
    # agent files) through a nested state-pull run; rebuilding the root there
    # would delete the directory the outer command is about to run in.
    bin_dir = Path(fake_terraform["PATH"].split(os.pathsep)[0])
    (bin_dir / "terraform").write_text(FAKE_TERRAFORM.replace(
        'printf \'%s\\n\' "$*"', 'printf \'%s %s\\n\' "$(pwd -P 2>/dev/null || echo DELETED)" "$*"'))
    dev = _remote("aws", tmp_path / "aws" / "envs" / "dev")
    proc = _run_layer(tmp_path, fake_terraform, "workspace", "dev", "plan", dev, dev.parent)
    assert proc.returncode == 0, proc.stderr
    calls = fake_terraform["log"].read_text().splitlines()
    assert any(" state pull" in line for line in calls)  # the nested read happened
    plan = [line for line in calls if " plan " in f"{line} "][-1]
    assert not plan.startswith("DELETED"), calls
    assert Path(plan.split(" ", 1)[0]).is_dir()


def test_agent_readers_follow_the_state_dir_and_fail_closed(tmp_path, monkeypatch):
    import agent_files
    from scripts import agent_ownership

    env = tmp_path / "aws" / "envs" / "prod"
    env.mkdir(parents=True)
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    state = {"resources": [{"module": "module.workspace", "mode": "managed", "type": "terraform_data",
                            "name": "genie_space", "instances": [{"index_key": "sales", "attributes": {"id": "t"}}]}]}
    layer = tmp_path / "state" / "aws" / "prod" / "layers" / "workspace"
    layer.mkdir(parents=True)
    (layer / "terraform.tfstate").write_text(json.dumps(state))
    assert agent_ownership.create_instance(env, "sales") == {"id": "t"}
    (layer / "terraform.tfstate").unlink()
    (env / "terraform.tfstate").write_text(json.dumps(state))  # left in the checkout: a split
    with pytest.raises(agent_files.AgentFileError, match="migrate-state-dir"):
        agent_files.state_keys(env)
    with pytest.raises(agent_ownership.AgentError, match="migrate-state-dir"):
        agent_ownership.create_instance(env, "sales")


# A remote env reads only its workspace state (it never moves the job itself).
@pytest.mark.parametrize("remote, failing", [(False, "workspace"), (False, "data_access"), (True, "workspace")])
def test_scheduled_job_move_fails_closed_on_unreadable_state(tmp_path, fake_terraform, monkeypatch, remote, failing):
    from scripts import move_scheduled_job

    env = _remote("aws", tmp_path / "aws" / "envs" / "dev") if remote else tmp_path / "aws" / "envs" / "dev"
    (env / "data_access").mkdir(parents=True, exist_ok=True)
    job = {"resources": [{"mode": "managed", "type": "databricks_job", "name": "scheduled_governance",
                          "instances": [{}]}]}

    def state_json(env_dir, env_name, layer):
        if layer == failing:
            raise state_layout.StateError(f"cannot read the {layer} state")
        return job if layer == "workspace" else None

    monkeypatch.setattr(move_scheduled_job.state_layout, "state_json", state_json)
    _use_fake(monkeypatch, fake_terraform)
    assert move_scheduled_job.move(env, env / "data_access") != 0
    assert fake_terraform["log"].read_text() == ""  # no Terraform command ran


def _fd_path(fd: int) -> str:
    try:
        import fcntl

        return os.fsdecode(fcntl.fcntl(fd, fcntl.F_GETPATH, bytes(1024)).rstrip(b"\0"))  # macOS
    except (AttributeError, OSError):
        return os.readlink(f"/proc/self/fd/{fd}")  # Linux


def test_migration_makes_the_marker_durable_before_removing_any_source(tmp_path, monkeypatch):
    env = _legacy_env(tmp_path)
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    events = []
    real_fsync, real_fsync_dir, real_unlink = os.fsync, state_layout._fsync_dir, Path.unlink

    def fsync(fd):
        events.append(("fsync", Path(_fd_path(fd)).resolve()))
        real_fsync(fd)

    def fsync_dir(path):
        events.append(("fsync-dir", Path(path).resolve()))
        real_fsync_dir(path)

    def unlink(self, *args, **kwargs):
        if self.resolve() in {(env / "terraform.tfstate").resolve(), (env / "data_access/terraform.tfstate").resolve()}:
            events.append(("unlink-source", self.resolve()))
            raise OSError("power lost")  # the crash: nothing after this point happens
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(state_layout.os, "fsync", fsync)
    monkeypatch.setattr(state_layout, "_fsync_dir", fsync_dir)
    monkeypatch.setattr(Path, "unlink", unlink)
    with pytest.raises(OSError, match="power lost"):
        state_layout.migrate(env, "dev")
    marker = (env / ".state-moved").resolve()
    first_unlink = next(i for i, e in enumerate(events) if e[0] == "unlink-source")
    assert ("fsync", marker) in events[:first_unlink], events
    assert events.index(("fsync", marker)) < events.index(("fsync-dir", env.resolve())) < first_unlink, events
    assert marker.is_file()


# python-hcl2 8 changed its parse output (state_layout.config, generate_abac);
# everything that installs it for GenieRails pins the 7.x line.
HCL2_PIN = "python-hcl2>=7.3.1,<8"
HCL2_INSTALLERS = (
    *sorted(ROOT.glob("*/.github/workflows/*.yml")), *sorted(ROOT.glob("*/azure-pipelines/*.yml")),
    *sorted((ROOT / ".github" / "workflows").glob("*.yml")),
    SHARED / "roots/data_access/scheduled_governance.tf", SHARED / "roots/workspace/scheduled_governance.tf",
    SHARED / "scheduled_governance.auto.tfvars.example", SHARED / "scripts/run_integration_tests.py",
    SHARED / "scripts/setup_test_data.py", SHARED / "scripts/sync_tag_policies.py",
    SHARED / "scripts/wait_for_tag_policies.py", SHARED / "docs/prerequisites.md",
    SHARED / "docs/integration-testing.md",
)


def test_every_sample_and_installer_pins_python_hcl2_below_8():
    assert len([p for p in HCL2_INSTALLERS if "workflows" in p.parts or "azure-pipelines" in p.parts]) >= 17
    unpinned = []
    for path in HCL2_INSTALLERS:
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if (path.suffix == ".md" or line.lstrip().startswith("#")) and "pip install" not in line:
                continue  # prose, tables and comments name the package
            for match in re.finditer(r"python-hcl2[^\s'\",\]|]*(?:,<\d+)?", line):
                if match.group(0) != HCL2_PIN:
                    unpinned.append(f"{path.relative_to(ROOT)}:{number}: {line.strip()}")
            if "pip install" in line and "python-hcl2" in line and f"'{HCL2_PIN}'" not in line:
                unpinned.append(f"{path.relative_to(ROOT)}:{number}: unquoted in a shell (> redirects): {line.strip()}")
    assert unpinned == [], "\n".join(unpinned)


# Each module that reads state lazily, and a call that reaches that read.
LAZY_STATE_READERS = {
    "scripts.coverage_gate": "m._layer_state(env / 'data_access')",
    "scripts.staged_policy_apply": "m._read_state(env)",
    "scripts.remap_env_config": "m._deployed_space_keys(str(env))",
    "scripts.runtime_overlay": "__import__('state_layout').state_json(env, 'dev', 'workspace')",
    "scripts.migration": "m._read_state(env, 'workspace')",
}


@pytest.mark.parametrize("module", sorted(LAZY_STATE_READERS))
def test_state_readers_import_the_helper_without_help_from_other_modules(tmp_path, module):
    # Each reader in a fresh interpreter with only shared/ on its path, as
    # pytest or another module imports it: no earlier import may have put
    # scripts/ on sys.path for it.
    env = tmp_path / "aws" / "envs" / "dev"
    (env / "data_access").mkdir(parents=True)
    code = (f"import importlib, sys; from pathlib import Path; sys.path[:0] = [{str(SHARED)!r}]; "
            f"m = importlib.import_module({module!r}); env = Path({str(env)!r}); {LAZY_STATE_READERS[module]}")
    proc = subprocess.run([sys.executable, "-I", "-c", code], text=True, capture_output=True,
                          env=_hermetic_env(tmp_path))
    assert proc.returncode == 0, proc.stderr


# -- the runner's own variables can't be overridden --------------------------

RUNNER_VARS = ("env_dir", "workspace_state_file", "data_access_state_file", "genie_id_dir")
OVERRIDE_FORMS = ("-var=NAME=", "-var NAME=", "--var=NAME=", "-var-file=FILE", "-var-file FILE",
                  "env tfvars", "TF_CLI_ARGS", "TF_CLI_ARGS_plan")


@pytest.mark.parametrize("layer", ["workspace", "data_access"])
@pytest.mark.parametrize("name", RUNNER_VARS)
@pytest.mark.parametrize("form", OVERRIDE_FORMS)
def test_a_caller_cannot_set_the_runners_state_variables(tmp_path, fake_terraform, layer, name, form):
    # A later -var wins in Terraform, so a caller (APPLY_FLAGS, direct args,
    # a var file, TF_CLI_ARGS) could point a root at a forged snapshot or at
    # another env's state. The runner refuses before any Terraform command.
    dev = tmp_path / "aws" / "envs" / "dev"
    (dev / "data_access").mkdir(parents=True)
    layer_dir = dev / "data_access" if layer == "data_access" else dev
    forged = tmp_path / "forged.json"
    forged.write_text("{}")
    args, extra = ["plan"], {}
    var_file = tmp_path / "caller.tfvars"
    var_file.write_text(f'{name} = "{forged}"\n')
    if form.startswith(("-var=", "--var=")):
        args.append(form.replace("NAME", name) + str(forged))
    elif form == "-var NAME=":
        args += ["-var", f"{name}={forged}"]
    elif form == "-var-file=FILE":
        args.append(f"-var-file={var_file}")
    elif form == "-var-file FILE":
        args += ["-var-file", str(var_file)]
    elif form == "env tfvars":
        (layer_dir / "abac.auto.tfvars").write_text(f'{name} = "{forged}"\n')
    else:
        extra[form] = f"-var={name}={forged}"
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
    env = _hermetic_env(tmp_path, PATH=fake_terraform["PATH"], FAKE_TF_LOG=fake_terraform["FAKE_TF_LOG"],
                        FAKE_TF_STATE=fake_terraform["FAKE_TF_STATE"], LAYER_ENV_DIR=str(layer_dir),
                        ENVS_DIR=str(dev.parent), GENIERAILS_STATE_DIR=str(tmp_path / "state"), **extra)
    proc = subprocess.run([str(RUNNER), layer, "dev", *args], env=env, text=True, capture_output=True)
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert f"sets {name}" in proc.stderr and "set only by" in proc.stderr
    assert fake_terraform["log"].read_text() == ""  # refused before terraform init


def test_an_unreadable_caller_var_file_refuses(tmp_path, fake_terraform):
    dev = tmp_path / "aws" / "envs" / "dev"
    dev.mkdir(parents=True)
    proc = _run_layer(tmp_path, fake_terraform, "workspace", "dev", "plan", dev, dev.parent)
    assert proc.returncode == 0, proc.stderr
    bad = tmp_path / "bad.tfvars"
    bad.write_text("this is { not hcl")
    env = _hermetic_env(tmp_path, PATH=fake_terraform["PATH"], FAKE_TF_LOG=fake_terraform["FAKE_TF_LOG"],
                        FAKE_TF_STATE=fake_terraform["FAKE_TF_STATE"], LAYER_ENV_DIR=str(dev), ENVS_DIR=str(dev.parent))
    proc = subprocess.run([str(RUNNER), "workspace", "dev", "plan", f"-var-file={bad}"], env=env, text=True,
                          capture_output=True)
    assert proc.returncode == 2 and "can't be read" in proc.stderr


@pytest.mark.parametrize("layout", ["in-checkout", "state dir"])
def test_the_runners_variables_come_after_every_caller_argument(tmp_path, fake_terraform, layout):
    dev = tmp_path / "aws" / "envs" / "dev"
    (dev / "data_access").mkdir(parents=True)
    extra = {"GENIERAILS_STATE_DIR": str(tmp_path / "state")} if layout == "state dir" else {}
    (tmp_path / "state").mkdir()  # GenieRails never creates a missing state dir
    env = _hermetic_env(tmp_path, PATH=fake_terraform["PATH"], FAKE_TF_LOG=fake_terraform["FAKE_TF_LOG"],
                        FAKE_TF_STATE=fake_terraform["FAKE_TF_STATE"], LAYER_ENV_DIR=str(dev),
                        ENVS_DIR=str(dev.parent), **extra)
    proc = subprocess.run([str(RUNNER), "workspace", "dev", "plan", "-var=policy_stage=switch", "-no-color"],
                          env=env, text=True, capture_output=True)
    assert proc.returncode == 0, proc.stderr  # ordinary caller variables still pass
    plan = [c for c in fake_terraform["log"].read_text().splitlines() if c.startswith("plan ")][-1].split()
    runner_vars = [i for i, a in enumerate(plan) if a.split("=", 2)[1:2] and a.startswith("-var=")
                   and a.split("=", 2)[1] in RUNNER_VARS]
    caller = [plan.index("-var=policy_stage=switch"), plan.index("-no-color")]
    assert runner_vars and min(runner_vars) > max(caller), plan
    expected = 4 if layout == "state dir" else 1  # + genie_id_dir (the state dir's records/)
    assert len(runner_vars) == expected, plan
