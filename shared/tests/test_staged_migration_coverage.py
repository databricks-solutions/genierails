"""A staged legacy migration through the coverage check (the Azure dev rehearse loop).

The live refresh records the versioned values the switch applies, while the
create phase of a legacy migration deliberately keeps the legacy values live.
The coverage check re-run on the create render compared the two and stopped
the phase, so every rehearse looped. Here: the check accepts exactly the
retained legacy values while a recorded migration is in its create phase,
still requires the versioned values at the switch, and refuses a mismatch
without an intact migration record; no staged phase rewrites the committed
split config (it renders into generated/.runtime/data_access); and terraform
state mv/rm backups land next to the env's state, never in shared/roots.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import coverage_gate as cg
from scripts import migration
from scripts import policy_generations as pg
from scripts import staged_policy_apply as spa
from scripts.runtime_overlay import effective
from tests import fake_uc, script_stubs
from tests.test_migration import COLUMN, ENV, LEGACY_POLICY, LEGACY_VALUE, legacy_policy, opt_in, write_legacy_state

ROOT = Path(__file__).resolve().parents[2]
SHARED = ROOT / "shared"
RUNNER = SHARED / "scripts" / "terraform_layer.sh"
DDL = "CREATE TABLE cat.sch.t (\n  email STRING\n);\n"
COMMITTED = ("generated/abac.auto.tfvars", "generated/masking_functions.sql",
             "data_access/abac.auto.tfvars", "data_access/masking_functions.sql")


def _committed(env_dir: Path) -> dict[str, bytes]:
    return {name: (env_dir / name).read_bytes() for name in COMMITTED if (env_dir / name).is_file()}


def _refresh(env_dir: Path) -> None:
    """What derive-assignments leaves: live DDL, and the refresh of the desired (versioned) assignments."""
    ddl = env_dir / "ddl" / "_fetched.sql"
    ddl.parent.mkdir(exist_ok=True)
    ddl.write_text(DDL)
    cg.write_refresh_record(env_dir / cg.REFRESH_RELPATH, mode="full", ddl_path=ddl,
                            config_path=effective(env_dir, "abac.auto.tfvars"))


class Migrating:
    """An env in the create phase of a recorded legacy migration, as the Azure dev rehearse left it."""

    def __init__(self, tmp_path: Path):
        self.env_dir = tmp_path / "envs" / ENV
        self.env_dir.mkdir(parents=True)
        write_legacy_state(self.env_dir)
        opt_in(self.env_dir)
        journal = migration.adopt(self.env_dir, ENV, terraform=lambda *_: 0)
        self.legacy = pg.load_snapshot(self.env_dir, journal["active"])
        self.desired = pg.desired_generation(self.env_dir)
        pg.save_snapshot(self.env_dir, self.desired)
        journal["transition"] = {"run_id": "r1", "from": self.legacy["fingerprint"], "to": self.desired["fingerprint"],
                                 "retain": "", "status": "failed", "failed_phase": "create"}
        spa.save_journal(self.env_dir, journal)
        _refresh(self.env_dir)

    def render(self, stage: str, keep_new_tags: bool = False) -> Path:
        rendered = pg.plan_stage(stage, self.desired, self.legacy, None, keep_new_tags=keep_new_tags)["rendered"]
        return pg.write_stage(self.env_dir / "data_access", rendered) / "abac.auto.tfvars"

    def judge(self, tfvars: Path, stage: str, *, old: str | None = None, keep_new_tags: bool = False):
        staged = {"stage": stage, "old_fingerprint": self.legacy["fingerprint"] if old is None else old,
                  "keep_new_tags": keep_new_tags}
        return cg.live_refresh(self.env_dir, tfvars, staged)


@pytest.fixture
def migrating(tmp_path):
    return Migrating(tmp_path)


# -- (1) the coverage check knows the staged phases ---------------------------------
def test_the_create_render_keeps_legacy_values_and_differs_from_the_refresh(migrating):
    """The loop's precondition: the plain check refuses the create render."""
    tfvars = migrating.render("create")
    assert pg._load(tfvars)["tag_assignments"][0]["tag_value"] == LEGACY_VALUE
    assert migrating.desired["tags"][COLUMN].startswith(f"{LEGACY_VALUE}__")
    refreshed_at, reason = cg.live_refresh(migrating.env_dir, tfvars)
    assert refreshed_at is None and "don't match the last live refresh" in reason


def test_create_of_a_recorded_legacy_migration_accepts_the_retained_legacy_values(migrating):
    refreshed_at, detail = migrating.judge(migrating.render("create"), "create")
    assert refreshed_at is not None, detail
    assert f"create keeps legacy generation {migrating.legacy['fingerprint']}" in detail


@pytest.mark.parametrize("stage", ["restore", "abandon"])
def test_the_recovery_phases_that_keep_legacy_values_are_accepted_too(migrating, stage):
    refreshed_at, detail = migrating.judge(migrating.render(stage, keep_new_tags=stage == "restore"), stage,
                                           keep_new_tags=stage == "restore")
    assert refreshed_at is not None, detail


def test_switch_still_requires_the_versioned_values(migrating):
    switch = migrating.render("switch")
    assert migrating.judge(switch, "switch")[0] is not None  # the refresh's own values
    create = migrating.render("create")
    refreshed_at, reason = migrating.judge(create, "switch")
    assert refreshed_at is None and "the switch phase applies the desired values" in reason


def test_outside_a_staged_phase_a_mismatch_still_refuses(migrating):
    assert cg.live_refresh(migrating.env_dir, migrating.render("create"))[0] is None


def _forge_record(env_dir: Path, **change) -> None:
    path = migration.record_path(env_dir)
    path.write_text(json.dumps(json.loads(path.read_text()) | change))


@pytest.mark.parametrize("forge, reason", [
    (lambda m: migration.record_path(m.env_dir).unlink(), "no adopted migration"),
    (lambda m: _forge_record(m.env_dir, status="adopting"), "no adopted migration"),
    (lambda m: _forge_record(m.env_dir, legacy="legacy-0000000000000000"), "no adopted migration"),
    (lambda m: spa.journal_path(m.env_dir).unlink(), "records no transition"),
    (lambda m: spa.save_journal(m.env_dir, spa.load_journal(m.env_dir) | {"transition": None}),
     "records no transition"),
    # The snapshot edited to make other values look retained.
    (lambda m: pg.write_json(pg.ledger_dir(m.env_dir) / f"{m.legacy['fingerprint']}.json",
                             m.legacy | {"tags": {COLUMN: "redact"}}), "doesn't match its fingerprint"),
    (lambda m: (pg.ledger_dir(m.env_dir) / f"{m.legacy['fingerprint']}.json").unlink(), "no snapshot"),
])
def test_a_forged_or_missing_migration_record_refuses(migrating, forge, reason):
    tfvars = migrating.render("create")
    forge(migrating)
    refreshed_at, detail = migrating.judge(tfvars, "create")
    assert refreshed_at is None and reason in detail, detail


def test_a_deterministic_generation_is_not_a_legacy_migration(migrating):
    refreshed_at, detail = migrating.judge(migrating.render("create"), "create",
                                           old=migrating.desired["fingerprint"])
    assert refreshed_at is None and "is not a legacy migration's" in detail


def test_a_create_render_with_any_other_value_refuses(migrating):
    tfvars = migrating.render("create")
    tfvars.write_text(tfvars.read_text().replace(f'"{LEGACY_VALUE}"', '"redact"'))
    refreshed_at, detail = migrating.judge(tfvars, "create")
    assert refreshed_at is None and "neither the live refresh's nor the legacy values" in detail


def test_a_refresh_of_another_config_refuses(migrating):
    tfvars = migrating.render("create")
    record = json.loads((migrating.env_dir / cg.REFRESH_RELPATH).read_text())
    (migrating.env_dir / cg.REFRESH_RELPATH).write_text(json.dumps(record | {"tag_assignments_sha256": "0" * 64}))
    refreshed_at, detail = migrating.judge(tfvars, "create")
    assert refreshed_at is None and "generated config's tag_assignments don't match" in detail


# -- (2) staged phases write only to generated/.runtime -----------------------------
def _fake_terraform(tmp_path: Path) -> tuple[dict, Path]:
    log = tmp_path / "terraform.log"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "terraform").write_text(f"#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{log}\"\n")
    (bin_dir / "terraform").chmod(0o755)
    return os.environ | {"PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}, log


def test_the_runner_reads_a_staged_render_instead_of_the_split_config(tmp_path):
    layer = tmp_path / "envs" / "dev" / "data_access"
    layer.mkdir(parents=True)
    (layer / "abac.auto.tfvars").write_text("fgac_policies = []\n")
    (layer / "env.auto.tfvars").write_text('sql_warehouse_id = "wh"\n')
    staged = tmp_path / "envs" / "dev" / "generated" / ".runtime" / "data_access"
    staged.mkdir(parents=True)
    (staged / "abac.auto.tfvars").write_text("fgac_policies = []\n")
    environ, log = _fake_terraform(tmp_path)
    environ |= {"LAYER_ENV_DIR": str(layer), pg.STAGE_CONFIG_ENV: str(staged)}
    result = subprocess.run([RUNNER, "data_access", "dev", "plan", "-var=policy_stage=create"],
                            text=True, capture_output=True, env=environ)
    assert result.returncode == 0, result.stdout + result.stderr
    args = shlex.split(log.read_text().splitlines()[1])
    assert f"-var-file={staged / 'abac.auto.tfvars'}" in args
    assert f"-var-file={layer / 'abac.auto.tfvars'}" not in args
    assert f"-var-file={layer / 'env.auto.tfvars'}" in args  # only the rendered file is replaced
    assert args[-1] == f"-var=masking_sql_file={staged / 'masking_functions.sql'}"
    # Without the variable nothing changes.
    environ.pop(pg.STAGE_CONFIG_ENV)
    log.unlink()
    assert subprocess.run([RUNNER, "data_access", "dev", "plan"], capture_output=True, env=environ).returncode == 0
    args = shlex.split(log.read_text().splitlines()[1])
    assert f"-var-file={layer / 'abac.auto.tfvars'}" in args and not [a for a in args if "masking_sql_file" in a]


def test_the_runner_refuses_a_staged_dir_without_a_render(tmp_path):
    layer = tmp_path / "data_access"
    layer.mkdir()
    environ, _ = _fake_terraform(tmp_path)
    environ |= {"LAYER_ENV_DIR": str(layer), pg.STAGE_CONFIG_ENV: str(tmp_path / "missing")}
    result = subprocess.run([RUNNER, "data_access", "dev", "plan"], text=True, capture_output=True, env=environ)
    assert result.returncode == 2 and "has no abac.auto.tfvars" in result.stderr


def test_the_data_access_root_takes_the_staged_masking_sql():
    text = (SHARED / "roots" / "data_access" / "main.tf").read_text()
    assert 'variable "masking_sql_file"' in text
    assert ('masking_sql_file                         = var.masking_sql_file != "" ? var.masking_sql_file : '
            '"${var.env_dir}/masking_functions.sql"') in text


# -- (3) state mv/rm backups stay next to the state ---------------------------------
MINIMAL_ROOT = ('resource "terraform_data" "a" {}\n\nvariable "env_dir" {\n  type = string\n}\n\n'
                'variable "data_access_state_file" {\n  type    = string\n  default = ""\n}\n')


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
@pytest.mark.parametrize("state_dir", [False, True])
def test_state_mv_backups_go_to_the_env_state_dir_never_into_shared_roots(tmp_path, state_dir):
    from tests.terraform_helpers import shared_copy

    shared = shared_copy(tmp_path)
    root = shared / "roots" / "data_access"
    shutil.rmtree(root)
    root.mkdir()
    (root / "main.tf").write_text(MINIMAL_ROOT)  # terraform_data only: no provider download
    env_dir = tmp_path / "cloud" / "envs" / "dev"
    (env_dir / "data_access").mkdir(parents=True)
    environ = {k: v for k, v in os.environ.items() if k not in ("CI", "GENIERAILS_STATE_DIR")}
    environ |= {"LAYER_ENV_DIR": str(env_dir / "data_access"), "ENVS_DIR": str(env_dir.parent),
                "TF_IN_AUTOMATION": "1"}
    if state_dir:
        (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
        environ["GENIERAILS_STATE_DIR"] = str(tmp_path / "state")
    runner = shared / "scripts" / "terraform_layer.sh"

    def run(*args):
        result = subprocess.run([runner, "data_access", "dev", *args], text=True, capture_output=True,
                                env=environ, timeout=300)
        assert result.returncode == 0, result.stdout + result.stderr
        return result

    run("apply", "-auto-approve", "-input=false")
    run("state-mv", "terraform_data.a", "terraform_data.b")
    backups = sorted(p for p in tmp_path.rglob("*.backup"))
    roots = [p for p in backups if shared in p.parents]
    assert roots == [], roots
    owner = (tmp_path / "state") if state_dir else env_dir / "data_access"
    assert [p for p in backups if owner in p.parents] and len(backups) == 1, backups
    assert backups[0].name.startswith("terraform.tfstate.") and backups[0].name.endswith(".backup")
    if state_dir:
        assert not list(env_dir.rglob("*.backup"))  # nothing state-like in the checkout
    # The backup holds the state before the move.
    assert "terraform_data" in backups[0].read_text() and '"a"' in backups[0].read_text()


@pytest.mark.parametrize("name", ["terraform.tfstate.1792065006.backup",
                                  "terraform.tfstate.20261015T115024Z.4242.backup"])
def test_timestamped_state_backups_are_ignored_upstream_and_in_a_customer_repo(tmp_path, name):
    from scripts import customer_gitignore

    assert "*.tfstate.*.backup" in (ROOT / ".gitignore").read_text().splitlines()
    assert "*.tfstate.*.backup" in customer_gitignore.COMMON.splitlines()
    environ = os.environ | {"GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"}
    for label, gitignore in (("upstream", (ROOT / ".gitignore").read_text()),
                             ("customer", customer_gitignore.block(tmp_path))):
        repo = tmp_path / label
        (repo / "shared" / "roots" / "data_access").mkdir(parents=True)
        (repo / "envs" / "dev" / "data_access").mkdir(parents=True)
        (repo / ".gitignore").write_text(gitignore)
        subprocess.run(["git", "init", "-q", str(repo)], check=True, env=environ)
        for where in ("shared/roots/data_access", "envs/dev/data_access"):
            (repo / where / name).write_text("{}")
            result = subprocess.run(["git", "-C", str(repo), "check-ignore", "-q", f"{where}/{name}"], env=environ)
            assert result.returncode == 0, f"{label}: {where}/{name} is not ignored"
        # And the staged phases' renders.
        render = "envs/dev/generated/.runtime/data_access/abac.auto.tfvars"
        assert subprocess.run(["git", "-C", str(repo), "check-ignore", "-q", render], env=environ).returncode == 0


# -- end to end: make rehearse on a pending legacy migration ---------------------------
# Below make as scripts/staged_policy_apply.py: the real Controller and in-process hooks
# (tests/test_migration.py's MigrationHarness), with the coverage re-check left on.
CONTROLLER = r'''
import json, os, sys
from pathlib import Path
sys.path.insert(0, {shared!r})
import pytest
from scripts import migration, policy_generations as pg, staged_policy_apply as spa
from tests.test_migration import MigrationHarness

args = sys.argv[1:]
env_dir = Path(args[args.index("--env-dir") + 1])
assert os.environ.get("GENIERAILS_COVERAGE_GATE_RUNNER"), "make sets the coverage re-check runner"
h = MigrationHarness(env_dir, pytest.MonkeyPatch(), Path(os.environ["HARNESS_TMP"]))
if migration.deployment(env_dir)["legacy"]:
    h.legacy = migration.legacy_generation(migration.deployment(env_dir))
with open(os.environ["FAKE_UC_EVENTS"], "a") as f:
    f.write(json.dumps({{"src": "controller", "argv": args}}) + "\n")
try:
    rc = getattr(h.controller(), args[0])()
except spa.Refused as exc:
    print(f"ERROR: staged policy apply refused: {{exc}}", file=sys.stderr)
    rc = 3
with open(os.environ["FAKE_UC_EVENTS"], "a") as f:
    f.write(json.dumps({{"src": "hooks", "calls": h.calls}}) + "\n")
if h.invariant_problems:
    print(h.invariant_problems, file=sys.stderr)
    rc = rc or 1
sys.exit(rc)
'''

# Recursive make below rehearse: runs the steps under test, records and skips the rest.
MAKE = r'''#!{python}
import json, os, sys
args = sys.argv[1:]
with open(os.environ["FAKE_UC_EVENTS"], "a") as f:
    f.write(json.dumps({{"src": "make", "argv": args}}) + "\n")
env_dir = os.environ["REHEARSE_ENV_DIR"]
if "_derive-before-exposure" in args:
    # derive-assignments in a deterministic env: the ignored overlay and the live refresh record.
    sys.path.insert(0, {shared!r})
    from scripts import runtime_overlay
    from tests.test_staged_migration_coverage import _refresh
    from pathlib import Path
    runtime_overlay.begin(Path(env_dir))
    runtime_overlay.seal(Path(env_dir))
    _refresh(Path(env_dir))
elif "apply" in args:
    os.execvp("make", ["make", "--no-print-directory", "_apply-layer", "LAYER=data_access", "TARGET_ENV=dev",
                       f"LAYER_ENV_DIR={{env_dir}}/data_access", "IMPORT_CLASSIFICATION=1"])
elif "_prepare-classification" in args:
    os.execvp("make", ["make", *args])
'''

# ROOT_RUNNER: terraform console answers the coverage check's inputs from the config
# terraform_layer.sh would read (the staged render when GENIERAILS_LAYER_CONFIG_DIR is
# set); everything else goes to tests/fake_uc.py's runner.
CONSOLE_RUNNER = r'''#!{python}
import base64, hashlib, json, os, sys
layer, env_name, command, *args = sys.argv[1:]
if command != "console":
    os.execv(sys.executable, [sys.executable, {inner!r}, *sys.argv[1:]])
config = os.environ.get("GENIERAILS_LAYER_CONFIG_DIR") or os.environ["LAYER_ENV_DIR"]
digest = hashlib.sha256()
for name in ("abac.auto.tfvars", "masking_functions.sql"):
    path = os.path.join(config, name)
    digest.update(open(path, "rb").read() if os.path.exists(path) else b"")
with open(os.environ["FAKE_UC_EVENTS"], "a") as f:
    f.write(json.dumps({{"src": "console", "config": config}}) + "\n")
# Legacy grants are kept under a changing protection, so a failed check stops the phase.
inputs = {{"fingerprint": digest.hexdigest(), "grant_tables": [], "acknowledged_columns": [], "needs_gate": True,
          "new_grants": [], "grant_keys": [], "max_age": "24h"}}
print("+ terraform console")
print('"' + base64.b64encode(json.dumps(inputs).encode()).decode() + '"')
'''


def _with_tag_attributes(env_dir: Path) -> None:
    path = env_dir / "data_access" / "terraform.tfstate"
    state = json.loads(path.read_text())
    for resource in state["resources"]:
        if resource["type"] == "databricks_entity_tag_assignment":
            for instance in resource["instances"]:
                instance["attributes"] |= {"entity_type": "columns", "tag_key": "gr_treatment"}
    path.write_text(json.dumps(state))


def test_make_rehearse_of_a_pending_legacy_migration_passes_create_and_switch(tmp_path):
    env_dir = tmp_path / "envs" / ENV
    env_dir.mkdir(parents=True)
    # As legacy GenieRails deployed it: the mask has its match alias, and its function
    # stays in the committed split SQL (outside the deterministic library block).
    write_legacy_state(env_dir, policies={LEGACY_POLICY: legacy_policy() | {
        "match_columns": [{"condition": f"hasTagValue('gr_treatment', '{LEGACY_VALUE}')", "alias": "col"}]}})
    _with_tag_attributes(env_dir)
    opt_in(env_dir)
    sql = env_dir / "data_access" / "masking_functions.sql"
    sql.write_text("USE CATALOG cat;\nUSE SCHEMA sch;\n\nCREATE OR REPLACE FUNCTION mask_email(email STRING)\n"
                   "RETURNS STRING\nRETURN CONCAT('***@', SPLIT(email, '@')[1]);\n\n" + sql.read_text())
    # Tag policies are account-scoped: the shared account layer still allows the
    # legacy value (its generation is live), the split data_access config has none.
    layer_abac = env_dir / "data_access" / "abac.auto.tfvars"
    text = layer_abac.read_text()
    tag_policies, rest = text.split("tag_assignments = [", 1)
    (tmp_path / "envs" / "account").mkdir()
    (tmp_path / "envs" / "account" / "abac.auto.tfvars").write_text(
        tag_policies.replace("values = [", f'values = ["{LEGACY_VALUE}", '))
    layer_abac.write_text("tag_assignments = [" + rest)
    auth = 'databricks_workspace_host = "https://fake.cloud.databricks.com"\ndatabricks_client_id = "11111111-2222-3333-4444-555555555555"\n'
    (env_dir / "auth.auto.tfvars").write_text(auth)
    (env_dir / "data_access" / "auth.auto.tfvars").write_text(auth)
    with (env_dir / "env.auto.tfvars").open("a") as f:
        f.write('deployer_principal_id = "11111111-2222-3333-4444-555555555555"\n')
    layer_abac = env_dir / "data_access" / "abac.auto.tfvars"  # the promote split carries the account groups
    layer_abac.write_text("groups = {\n  raw = {}\n  regional = {}\n  masked = {}\n}\n" + layer_abac.read_text())
    live = {"column_tags": [[COLUMN, "gr_treatment", LEGACY_VALUE]], "table_tags": [],
            "warehouses": [{"id": "wh", "name": "Shared", "state": "RUNNING"}]}
    fake = fake_uc.install(tmp_path, live)
    (tmp_path / "harness").mkdir()
    controller = tmp_path / "controller.py"
    controller.write_text(CONTROLLER.format(shared=str(SHARED)))
    make = tmp_path / "make-stub"
    make.write_text(MAKE.format(python=sys.executable, shared=str(SHARED)))
    make.chmod(0o755)
    runner = tmp_path / "console-runner"
    runner.write_text(CONSOLE_RUNNER.format(python=sys.executable, inner=fake["FAKE_UC_RUNNER"]))
    runner.chmod(0o755)
    catalogs = tmp_path / "catalogs"
    catalogs.write_text("#!/bin/sh\necho ''\n")
    catalogs.chmod(0o755)
    # Only the checks around the migration are stubbed; coverage_gate.py is the real one.
    bin_dir = script_stubs.install(tmp_path / "stub-bin", {
        "staged_policy_apply.py confirm": "true",
        "staged_policy_apply.py": script_stubs.python_file(controller),
        "release_gate.py rehearse-start": "true", "release_gate.py rehearse-finish": "true",
        "release_gate.py env-role": "true",
        "deployer_identity.py": "true", "table_readers.py": "true", "group_map.py": "true",
        "exposure_token.py": "true", "saved_settings.py": "true",
        "prepare_classification_config.py": str(catalogs),
    })
    environ = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "APPLY_FLAGS", "CI")}
    environ |= fake | {"HARNESS_TMP": str(tmp_path / "harness"), "PATH": f"{bin_dir}:{os.environ['PATH']}",
                       "GIT_CONFIG_GLOBAL": "/dev/null", "REHEARSE_ENV_DIR": str(env_dir),
                       "PYTHONPATH": f"{fake['PYTHONPATH']}{os.pathsep}{SHARED}"}
    committed = _committed(env_dir)

    result = subprocess.run(
        ["make", "--no-print-directory", "rehearse", f"ENV={ENV}", f"ENV_DIR={env_dir}",
         f"ROOT_RUNNER={runner}", f"MAKE={make}"],
        cwd=ROOT / "aws", text=True, capture_output=True, env=environ, timeout=900)
    out = result.stdout + result.stderr
    assert result.returncode == 0, out

    events = fake_uc.events(fake)
    hooks = [c for e in events if e["src"] == "hooks" for c in e["calls"]]
    assert hooks[:2] == ["create", "inventory:create/restore"] and "switch" in hooks and hooks[-1] == "grant", hooks
    assert "=== Migration (dev): adopting the legacy deployment" in out
    # The re-check judged each phase's render (create kept the legacy values), not the split config.
    staged = pg.stage_config_dir(env_dir)
    consoles = [e["config"] for e in events if e["src"] == "console"]
    assert str(staged) in consoles and "create keeps legacy generation legacy-" in out
    journal = spa.load_journal(env_dir)
    desired = pg.desired_generation(env_dir)
    assert journal["transition"] is None and journal["active"] == desired["fingerprint"]
    tags = {a["entity_name"]: a["tag_value"] for r in json.loads((env_dir / "data_access" / "terraform.tfstate")
            .read_text())["resources"] if r["name"] == "treatment" for a in (i["attributes"] for i in r["instances"])}
    assert tags == {COLUMN: desired["tags"][COLUMN]} and tags[COLUMN] != LEGACY_VALUE
    assert json.loads((env_dir / "data_access" / cg.GATE_FILENAME).read_text())["status"] == "pass"
    # The committed config is byte for byte what it was; the renders are in the ignored runtime dir.
    assert _committed(env_dir) == committed
    assert sorted(p.name for p in staged.iterdir()) == ["abac.auto.tfvars", "masking_functions.sql"]
    assert not list((ROOT / "shared" / "roots").rglob("*.tfstate.*.backup"))


def test_a_failed_staged_phase_leaves_the_committed_config_byte_identical(migrating, monkeypatch, tmp_path):
    """The coverage re-check stops the phase after its render: the split config is still untouched."""
    from tests.test_migration import MigrationHarness

    committed = _committed(migrating.env_dir)
    # A refresh of another config: the create re-check fails closed.
    record = json.loads((migrating.env_dir / cg.REFRESH_RELPATH).read_text())
    (migrating.env_dir / cg.REFRESH_RELPATH).write_text(json.dumps(record | {"tag_assignments_sha256": "0" * 64}))
    h = MigrationHarness(migrating.env_dir, monkeypatch, tmp_path)
    runner = tmp_path / "console-runner"
    runner.write_text(CONSOLE_RUNNER.format(python=sys.executable, inner=str(pg.RUNNER)))
    runner.chmod(0o755)
    events = tmp_path / "events.jsonl"
    environ = os.environ | {"FAKE_UC_EVENTS": str(events)}
    hook_env = environ | {"GENIERAILS_ENV_DIR": str(migrating.env_dir), "GENIERAILS_ENV_NAME": ENV,
                          "GENIERAILS_POLICY_FINGERPRINT": migrating.desired["fingerprint"],
                          "GENIERAILS_OLD_POLICY_FINGERPRINT": migrating.legacy["fingerprint"],
                          "GENIERAILS_COVERAGE_GATE_RUNNER": str(runner)}
    monkeypatch.setenv("FAKE_UC_EVENTS", str(events))
    with pytest.raises(pg.StageError, match="coverage check stopped the stage"):
        pg.run_hook("create", hook_env)
    assert _committed(migrating.env_dir) == committed
    assert (pg.stage_config_dir(migrating.env_dir) / "abac.auto.tfvars").is_file()
    assert not h.invariant_problems
