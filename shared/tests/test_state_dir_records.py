"""Runtime records under GENIERAILS_STATE_DIR: a fresh checkout sees what an earlier one left.

The deployment machine runs each command from a fresh, clean checkout (design
section 17), so the staged policy journal, its generation snapshots, the
migration record and group_map's applied record must live in the state dir,
not in the gitignored envs/<env>/ of whichever checkout wrote them.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests import test_staged_policy_apply as tspa
import group_map
from scripts import migration
from scripts import policy_generations as pg
from scripts import staged_policy_apply as spa
from scripts import environment_lock
from scripts import release_gate as rg
from scripts import state_layout

SHARED = Path(__file__).resolve().parents[1]
ROOT = SHARED.parent
STAGED = SHARED / "scripts" / "staged_policy_apply.py"
LEDGER = Path("data_access") / ".policy_generations"


def _state_path(env_dir: Path) -> Path:
    return state_layout.root_state_paths(env_dir)["data_access"]


@pytest.fixture
def state_dir(tmp_path, monkeypatch):
    path = tmp_path / "state"
    path.mkdir()
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(path))
    # The fake layer and the harness's "Unity Catalog" read the state where the
    # runner keeps it (the state dir), not envs/<env>/data_access/.
    monkeypatch.setattr(tspa, "live_state", lambda env_dir: json.loads(_state_path(env_dir).read_text())
                        if _state_path(env_dir).is_file() else {"resources": [], "functions": []})
    return path


def _harness(checkout: Path, monkeypatch, tmp_path) -> tspa.Harness:
    env_dir = checkout / "aws" / "envs" / "dev"
    env_dir.mkdir(parents=True, exist_ok=True)
    h = tspa.Harness(env_dir, monkeypatch, tmp_path)
    runner = Path(pg.RUNNER)
    runner.write_text(tspa.FAKE_RUNNER.replace(
        'state_path = os.path.join(layer_dir, "terraform.tfstate")',
        'state_path = os.environ.get("FAKE_TF_STATE") or os.path.join(layer_dir, "terraform.tfstate")'))
    monkeypatch.setenv("FAKE_TF_STATE", str(_state_path(env_dir)))
    _state_path(env_dir).parent.mkdir(parents=True, exist_ok=True)
    return h


def _fresh_checkout(source_env: Path, checkout: Path) -> Path:
    """A clean checkout of the same commit: the committed env files, plus the layer split each
    applying command re-derives from them; no ignored dot files."""
    env_dir = checkout / "aws" / "envs" / "dev"
    for sub in ("generated", "data_access"):
        (env_dir / sub).mkdir(parents=True)
        for name in ("abac.auto.tfvars", "masking_functions.sql"):
            shutil.copy2(source_env / sub / name, env_dir / sub / name)
    shutil.copy2(source_env / "env.auto.tfvars", env_dir / "env.auto.tfvars")
    return env_dir


def _status(env_dir: Path, **extra) -> subprocess.CompletedProcess:
    environ = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL")} | extra
    return subprocess.run([sys.executable, str(STAGED), "status", "--env-dir", str(env_dir), "--env-name", "dev",
                           "--commit", "x"], text=True, capture_output=True, env=environ)


def _failed_change(state_dir, tmp_path, monkeypatch):
    """Checkout A releases a generation, then a change fails at switch: an unfinished journal."""
    a = _harness(tmp_path / "a", monkeypatch, tmp_path)
    old = tspa.released(a)
    new = tspa.write_env(a.env_dir, versions={"email_partial": {"partial": "redacted"}})
    assert a.apply(FAKE_TF_FAIL_APPLY="switch") == 1
    return a, old, new


# -- a fresh checkout sees the journal ----------------------------------------------

def test_the_ledger_is_written_to_the_state_dir_not_the_checkout(state_dir, tmp_path, monkeypatch):
    a, _old, new = _failed_change(state_dir, tmp_path, monkeypatch)
    records = state_dir / "aws" / "dev" / state_layout.RECORDS / LEDGER
    assert json.loads((records / "journal.json").read_text())["transition"]["to"] == new["fingerprint"]
    assert (records / f"{new['fingerprint']}.json").is_file() and (records / "stage.json").is_file()
    assert not (a.env_dir / LEDGER).exists()


def test_a_fresh_checkout_sees_and_resumes_a_failed_change(state_dir, tmp_path, monkeypatch):
    a, old, new = _failed_change(state_dir, tmp_path, monkeypatch)
    b_env = _fresh_checkout(a.env_dir, tmp_path / "b")

    # make policy-status's command, from the fresh checkout.
    result = _status(b_env)
    assert result.returncode == 0, result.stderr
    assert "no policy journal yet" not in result.stdout
    assert f"active generation:  {old['fingerprint']}" in result.stdout
    assert f"-> {new['fingerprint']}" in result.stdout and "status:    failed at switch" in result.stdout

    # rehearse/release run the same controller: it resumes the recorded change.
    b = _harness(tmp_path / "b", monkeypatch, tmp_path)
    assert b.apply() == 0
    assert b.calls[0] != "create" and "switch" in b.calls
    journal = spa.load_journal(b_env)
    assert journal["transition"] is None and journal["active"] == new["fingerprint"]
    assert journal["pending"][0]["fingerprint"] == old["fingerprint"]
    assert not (b_env / LEDGER).exists() and not b.invariant_problems


def test_make_policy_status_reads_the_state_dir_journal(state_dir, tmp_path, monkeypatch):
    a, _old, new = _failed_change(state_dir, tmp_path, monkeypatch)
    b_env = _fresh_checkout(a.env_dir, tmp_path / "b")
    environ = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL")}
    result = subprocess.run(["make", "--no-print-directory", "policy-status", "ENV=dev", f"ENV_DIR={b_env}"],
                            cwd=ROOT / "aws", text=True, capture_output=True, env=environ)
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"-> {new['fingerprint']}" in result.stdout and "failed at switch" in result.stdout


# -- refusals -------------------------------------------------------------------------

def _in_checkout_change(tmp_path, monkeypatch):
    """The pre-state-dir layout: a failed change journaled in the checkout."""
    monkeypatch.delenv(state_layout.STATE_DIR_ENV, raising=False)
    h = tspa.Harness(tmp_path / "c" / "aws" / "envs" / "dev", monkeypatch, tmp_path)
    h.env_dir.mkdir(parents=True)
    tspa.released(h)
    tspa.write_env(h.env_dir, versions={"email_partial": {"partial": "redacted"}})
    assert h.apply(FAKE_TF_FAIL_APPLY="switch") == 1
    assert (h.env_dir / LEDGER / "journal.json").is_file()
    return h


@pytest.mark.parametrize("where", ["checkout-only", "both"])
def test_a_journal_left_in_the_checkout_is_refused_not_ignored(tmp_path, monkeypatch, where):
    h = _in_checkout_change(tmp_path, monkeypatch)
    state = tmp_path / "state"
    records = state / "aws" / "dev" / state_layout.RECORDS / LEDGER
    records.mkdir(parents=True)
    if where == "both":
        shutil.copy2(h.env_dir / LEDGER / "journal.json", records / "journal.json")
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(state))

    result = _status(h.env_dir)
    assert result.returncode == 3, result.stdout + result.stderr
    assert "still in the checkout" in result.stderr and "make migrate-state-dir ENV=dev" in result.stderr
    assert "no policy journal yet" not in result.stdout
    for read in (spa.load_journal, migration.load_record, migration.migrated):
        with pytest.raises(spa.Refused if read is spa.load_journal else migration.Refused,
                           match="migrate-state-dir"):
            read(h.env_dir)
    with pytest.raises(state_layout.RecordError, match="migrate-state-dir"):
        pg.load_snapshot(h.env_dir, "abc")
    with pytest.raises(spa.Refused, match="migrate-state-dir"):
        h.apply()
    assert (h.env_dir / LEDGER / "journal.json").is_file()  # nothing was moved or dropped


def test_a_group_map_record_left_in_the_checkout_is_refused(tmp_path, monkeypatch):
    env_dir = tmp_path / "aws" / "envs" / "dev"
    env_dir.mkdir(parents=True)
    (env_dir / group_map.APPLIED_RECORD).write_text('{"group_map": {"a": "b"}}\n')
    (tmp_path / "state").mkdir()
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(tmp_path / "state"))
    with pytest.raises(state_layout.RecordError, match="migrate-state-dir"):
        group_map.required_acks(env_dir, "dev", {"group_map": {"a": "b"}})
    with pytest.raises(state_layout.RecordError, match="migrate-state-dir"):
        group_map.record_applied(env_dir, {"group_map": {"a": "b"}})


def test_an_unreadable_state_dir_fails_closed(tmp_path, monkeypatch):
    env_dir = tmp_path / "aws" / "envs" / "dev"
    env_dir.mkdir(parents=True)
    not_a_dir = tmp_path / "file"
    not_a_dir.write_text("")
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(not_a_dir))
    with pytest.raises(spa.Refused, match="is not a directory"):
        spa.load_journal(env_dir)
    result = _status(env_dir)
    assert result.returncode == 3 and "is not a directory" in result.stderr

    if os.geteuid() == 0:
        pytest.skip("root reads any directory")
    records = tmp_path / "state" / "aws" / "dev" / state_layout.RECORDS
    records.mkdir(parents=True)
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(tmp_path / "state"))
    records.chmod(0)
    try:
        with pytest.raises(spa.Refused, match="can't be read"):
            spa.load_journal(env_dir)
    finally:
        records.chmod(0o700)


def test_a_mistyped_state_dir_refuses_every_reader_and_creates_nothing(tmp_path, monkeypatch):
    env_dir = tmp_path / "aws" / "envs" / "dev"
    env_dir.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    (tmp_path / "state").mkdir()  # the real one; the variable names a typo of it
    typo = tmp_path / "statee"
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(typo))

    with pytest.raises(state_layout.StateError, match="does not exist"):
        state_layout.state_json(env_dir, "dev", "workspace")
    for read in (spa.load_journal, migration.load_record):
        with pytest.raises((spa.Refused, migration.Refused), match="does not exist"):
            read(env_dir)
    with pytest.raises(state_layout.RecordError, match="does not exist"):
        group_map._applied(env_dir)
    with pytest.raises(RuntimeError, match="does not exist"):
        environment_lock.acquire(env_dir, os.getpid(), "rehearse")
    with pytest.raises(RuntimeError, match="does not exist"):
        rg.env_state(rg.cloud_dir_of(env_dir), "dev")
    result = _status(env_dir)
    assert result.returncode == 3 and "does not exist" in result.stderr, result.stdout + result.stderr
    assert "no policy journal yet" not in result.stdout
    for command in ("layer-dir", "records-dir"):
        cli = subprocess.run([sys.executable, str(SHARED / "scripts" / "state_layout.py"), command,
                              "--env-dir", str(env_dir), "--env", "dev", *(["--layer", "workspace"]
                                                                          if command == "layer-dir" else [])],
                             text=True, capture_output=True)
        assert cli.returncode == 2 and "does not exist" in cli.stderr, (command, cli.stdout, cli.stderr)
    environ = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL")}
    made = subprocess.run(["make", "--no-print-directory", "policy-status", "ENV=dev", f"ENV_DIR={env_dir}"],
                          cwd=ROOT / "aws", text=True, capture_output=True, env=environ)
    assert made.returncode != 0 and "does not exist" in made.stderr, made.stdout + made.stderr
    assert not typo.exists(), "nothing may create a mistyped state dir"


def test_an_existing_state_dir_without_records_reads_as_empty(tmp_path, monkeypatch):
    env_dir = tmp_path / "aws" / "envs" / "dev"
    env_dir.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    (tmp_path / "state").mkdir()
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(tmp_path / "state"))
    assert spa.load_journal(env_dir) is None and migration.load_record(env_dir) is None
    assert group_map._applied(env_dir) is None
    assert state_layout.state_json(env_dir, "dev", "workspace") is None
    assert state_layout.records_dir(env_dir) == (tmp_path / "state").resolve() / "aws" / "dev" / "records"
    result = _status(env_dir)
    assert result.returncode == 0 and "no policy journal yet" in result.stdout, result.stderr
    assert not (tmp_path / "state" / "aws").exists(), "reading creates nothing"


def test_only_migrate_state_dir_creates_a_missing_state_dir(tmp_path, monkeypatch):
    env_dir, files = _records_env(tmp_path)
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(tmp_path / "new" / "state"))
    with pytest.raises(spa.Refused, match="does not exist"):
        spa.load_journal(env_dir)
    assert state_layout.migrate(env_dir, "dev") == len(files)
    assert (tmp_path / "new" / "state").is_dir()
    assert spa.load_journal(env_dir)["active"] == "g1"


def test_without_the_state_dir_records_stay_in_the_checkout(tmp_path, monkeypatch):
    monkeypatch.delenv(state_layout.STATE_DIR_ENV, raising=False)
    env_dir = tmp_path / "aws" / "envs" / "dev"
    env_dir.mkdir(parents=True)
    assert pg.ledger_dir(env_dir) == env_dir / LEDGER
    assert group_map.applied_record_path(env_dir) == env_dir / group_map.APPLIED_RECORD
    (env_dir / state_layout.MOVED_MARKER).write_text("moved\n")
    with pytest.raises(spa.Refused, match="state was moved"):
        spa.load_journal(env_dir)


# -- make migrate-state-dir moves them ------------------------------------------------

def _records_env(tmp_path: Path) -> tuple[Path, dict[Path, bytes]]:
    env_dir = tmp_path / "aws" / "envs" / "dev"
    ledger = env_dir / LEDGER
    (ledger / "verify").mkdir(parents=True)
    files = {
        LEDGER / "journal.json": json.dumps(spa.empty_journal() | {"active": "g1", "pending": [
            {"fingerprint": "g0", "superseded_by": "r1", "confirmed": False}]}).encode(),
        LEDGER / "g1.json": b'{"fingerprint": "g1"}\n',
        LEDGER / "stage.json": b'{"stage": "switch"}\n',
        LEDGER / "migration.json": b'{"status": "adopting", "legacy": "g1"}\n',
        LEDGER / "verify" / "switch.json": b"{}\n",
        Path(group_map.APPLIED_RECORD): b'{"group_map": {}}\n',
    }
    for rel, data in files.items():
        (env_dir / rel).write_bytes(data)
    (env_dir / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    return env_dir, files


def test_migrate_state_dir_moves_every_record(tmp_path, monkeypatch):
    env_dir, files = _records_env(tmp_path)
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(tmp_path / "state"))
    assert state_layout.migrate(env_dir, "dev") == len(files)
    records = tmp_path / "state" / "aws" / "dev" / state_layout.RECORDS
    for rel, data in files.items():
        assert (records / rel).read_bytes() == data
    assert not (env_dir / LEDGER).exists()
    assert not (env_dir / group_map.APPLIED_RECORD).exists()
    assert (env_dir / state_layout.MOVED_MARKER).is_file()
    # Every reader now finds them, from this checkout or a fresh one.
    fresh = tmp_path / "fresh" / "aws" / "envs" / "dev"
    fresh.mkdir(parents=True)
    for env in (env_dir, fresh):
        assert spa.load_journal(env)["pending"][0]["fingerprint"] == "g0"
        assert migration.load_record(env)["status"] == "adopting"
        assert pg.load_snapshot(env, "g1") == {"fingerprint": "g1"}
        assert group_map._applied(env) == {}


def test_make_migrate_state_dir_then_policy_status_from_a_fresh_checkout(tmp_path):
    env_dir, _files = _records_env(tmp_path)
    shutil.copyfile(ROOT / "aws" / "Makefile", tmp_path / "aws" / "Makefile")
    (tmp_path / "state").mkdir()
    environ = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL")} | {
        state_layout.STATE_DIR_ENV: str(tmp_path / "state")}

    def make(cloud_dir, *args):
        return subprocess.run(["make", "--no-print-directory", "-C", str(cloud_dir), *args, f"SHARED_ROOT={SHARED}"],
                              text=True, capture_output=True, env=environ, timeout=120)

    before = make(tmp_path / "aws", "policy-status", "ENV=dev")
    assert before.returncode != 0 and "make migrate-state-dir ENV=dev" in before.stderr
    moved = make(tmp_path / "aws", "migrate-state-dir", "ENV=dev")
    assert moved.returncode == 0, moved.stdout + moved.stderr
    fresh = tmp_path / "fresh" / "aws"
    (fresh / "envs" / "dev").mkdir(parents=True)
    shutil.copyfile(ROOT / "aws" / "Makefile", fresh / "Makefile")
    shutil.copy2(env_dir / "env.auto.tfvars", fresh / "envs" / "dev" / "env.auto.tfvars")
    after = make(fresh, "policy-status", "ENV=dev")
    assert after.returncode == 0, after.stdout + after.stderr
    assert "active generation:  g1" in after.stdout and "waiting to retire:  g0" in after.stdout


def test_record_migration_resumes_after_a_crash_and_never_overwrites(tmp_path, monkeypatch):
    env_dir, files = _records_env(tmp_path)
    records = tmp_path / "state" / "aws" / "dev" / state_layout.RECORDS
    monkeypatch.setenv(state_layout.STATE_DIR_ENV, str(tmp_path / "state"))
    # A crash after copying the journal, before removing its source, and another
    # mid-copy of a snapshot: readers refuse the half-moved env until it's rerun.
    (records / LEDGER).mkdir(parents=True)
    shutil.copy2(env_dir / LEDGER / "journal.json", records / LEDGER / "journal.json")
    (records / LEDGER / "g1.json.migrating").write_text('{"fing')
    with pytest.raises(spa.Refused, match="migrate-state-dir"):
        spa.load_journal(env_dir)
    state_layout.migrate(env_dir, "dev")
    assert (records / LEDGER / "g1.json").read_bytes() == files[LEDGER / "g1.json"]
    assert spa.load_journal(env_dir)["active"] == "g1"

    (env_dir / LEDGER).mkdir(parents=True)
    (env_dir / LEDGER / "journal.json").write_text('{"version": 1, "active": "other"}')
    with pytest.raises(RuntimeError, match="refusing to overwrite"):
        state_layout.migrate(env_dir, "dev")
    assert json.loads((records / LEDGER / "journal.json").read_text())["active"] == "g1"
    assert (env_dir / LEDGER / "journal.json").is_file()
