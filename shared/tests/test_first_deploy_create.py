"""A first deterministic deploy, through every staged phase, with business grants planned.

The live AWS shape (run 8e19fa134092): a fresh deterministic env, nothing
applied; make generate, make check, then make rehearse ENV=dev stopped at the
first phase with ``ERROR: the create plan destroys protected resources;
nothing was applied``, listing six grants it would *create*: USE_CATALOG and
USE_SCHEMA for the three tier groups. Only table SELECT waited for the grant
phase (roots/data_access withhold_new_table_grants); catalog and schema USE
were planned in every phase, and the create phase refuses every business
grant create (policy_generations.plan_violations), so a first deploy could
never pass create. Its coverage check also judged a render that tags no
column yet, so it failed as "vacuous" and called every classified column
unprotected.

Now USE opens with SELECT in the grant phase (withhold_new_use_grants), the
create phase of a first deploy is checked against the treatment tags its
switch applies, and the refusal names creates as creates. Runs the real
controller, hooks, coverage check and live check on the fake layer of
tests/test_staged_policy_apply.py with the business grants on
(fake_access.json); the fake reads which stages withhold from the real
roots/data_access/main.tf.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import coverage_gate as cg
from scripts import policy_generations as pg
from scripts import staged_policy_apply as spa
from tests.test_policy_abandon_coverage import GATED_RUNNER, OLD_SP, _make_abandon, _promoted
from tests.test_policy_rebind import GRANTS, access_file, grant_keys
from tests.test_staged_policy_apply import Harness, live_policies, live_state, live_tags, write_env

SHARED = Path(__file__).resolve().parents[1]
EMAIL_DDL = "CREATE TABLE cat.sch.t (\n  email STRING\n);\n"
CI_MARKERS = ("CI", "TF_BUILD", "JENKINS_URL", "GITLAB_CI", "BUILDKITE", "CIRCLECI", "GENIERAILS_ALLOW_CI_APPLY")
TIERS = ("raw", "regional", "masked")
USE = {g for g in GRANTS if g.split("|")[0] in ("catalog_access", "schema_access")}
SELECT = {g for g in GRANTS if g.startswith("table_access|")}
BUSINESS = USE | SELECT
DEPLOYER_GRANT = {"terraform_sp_manage_catalog|cat"}
FIRST_DEPLOY = ["create", "inventory:create/restore", "switch", "inventory:switch", "verify-access:switch", "grant"]


class Recording(Harness):
    """The harness, recording what is live (and what the coverage check said) after every hook."""

    def __init__(self, *args):
        super().__init__(*args)
        self.after: dict[str, dict] = {}

    def run(self, argv, env=None, check=False):
        result = super().run(argv, env, check)
        if argv[0].startswith("/hooks/"):
            state = live_state(self.env_dir)
            gate_path = self.env_dir / "data_access" / cg.GATE_FILENAME
            self.after[argv[0].rsplit("/", 1)[1]] = {
                "rc": result.returncode, "grants": grant_keys(state),
                "policies": {p["name"] for p in live_policies(state)}, "tags": live_tags(state),
                "gate": json.loads(gate_path.read_text()) if gate_path.is_file() else None,
            }
        return result


@pytest.fixture
def harness(tmp_path, monkeypatch):
    for marker in CI_MARKERS:
        monkeypatch.delenv(marker, raising=False)
    env_dir = tmp_path / "envs" / "dev"
    env_dir.mkdir(parents=True)
    h = Recording(env_dir, monkeypatch, tmp_path)
    yield h
    assert not h.invariant_problems, h.invariant_problems


def _refresh(env_dir: Path, ddl_text: str = EMAIL_DDL, column_classes: dict | None = None) -> None:
    """What make's live refresh (derive-assignments, full mode) leaves before the controller runs."""
    from scripts.runtime_overlay import effective

    ddl = env_dir / "ddl" / "_fetched.sql"
    ddl.parent.mkdir(exist_ok=True)
    ddl.write_text(ddl_text)
    cg.write_refresh_record(env_dir / cg.REFRESH_RELPATH, mode="full", ddl_path=ddl,
                            config_path=effective(env_dir, "abac.auto.tfvars"),
                            column_classes=column_classes)


def fresh(h, tmp_path: Path, monkeypatch, *, ddl_text: str = EMAIL_DDL,
          column_classes: dict | None = None, **env) -> dict:
    """A fresh deterministic env as make generate leaves it: nothing applied, the real coverage check on."""
    gen = write_env(h.env_dir, deployer=OLD_SP, **env)
    _promoted(h, gen)
    access_file(h).write_text(json.dumps({"grants": sorted(GRANTS), "client_id": OLD_SP, "gone": []}))
    _refresh(h.env_dir, ddl_text, column_classes)
    runner = tmp_path / "gated-runner"
    runner.write_text(GATED_RUNNER.format(python=sys.executable, inner=str(pg.RUNNER)))
    runner.chmod(0o755)
    monkeypatch.setattr(pg, "RUNNER", runner)
    monkeypatch.setenv("GENIERAILS_COVERAGE_GATE_RUNNER", str(runner))  # as make apply sets it
    assert live_state(h.env_dir)["resources"] == [] and spa.load_journal(h.env_dir) is None
    return gen


# -- (1) the first deploy, phase by phase ---------------------------------------------------------
def test_a_first_deploy_runs_create_switch_grant_with_access_last(harness, tmp_path, monkeypatch, capfd):
    gen = fresh(harness, tmp_path, monkeypatch)
    assert harness.apply() == 0, capfd.readouterr().err
    assert harness.calls == FIRST_DEPLOY
    after = harness.after

    # create: the policies and the functions, the deployer's own grant; no tag, no business grant.
    assert after["create"]["policies"] == {p["name"] for p in gen["fgac_policies"]}
    assert after["create"]["tags"] == {}
    assert after["create"]["grants"] == DEPLOYER_GRANT
    # Its coverage check passed on what it does: no "vacuous" pass refused, no column called unprotected.
    gate = after["create"]["gate"]
    assert gate["status"] == "pass" and gate["kept_from"] == "" and gate["uncovered_columns"] == sorted(gen["tags"])

    # switch: every column tagged, masks live; still no business grant.
    assert after["switch"]["tags"] == gen["tags"]
    assert after["switch"]["policies"] == after["create"]["policies"]
    assert after["switch"]["grants"] == DEPLOYER_GRANT

    # grant, after verify-switched: USE and SELECT for the tier groups, nothing else changed.
    assert after["grant"]["grants"] == BUSINESS | DEPLOYER_GRANT
    assert (after["grant"]["tags"], after["grant"]["policies"]) == (after["switch"]["tags"],
                                                                    after["switch"]["policies"])
    assert after["grant"]["gate"]["status"] == "pass" and "uncovered_columns" not in after["grant"]["gate"]

    journal = harness.journal()
    assert journal["active"] == gen["fingerprint"] and journal["transition"] is None
    out = capfd.readouterr()  # the coverage check runs as a subprocess
    assert "destroys protected resources" not in out.err and "vacuous" not in out.out + out.err
    assert "classified but unprotected" not in out.out + out.err
    assert "First deploy: the create phase tags no column yet" in out.out


def test_no_business_group_gets_any_grant_before_the_masks_are_live_and_verified(harness, tmp_path, monkeypatch):
    fresh(harness, tmp_path, monkeypatch, columns=("email", "phone"), classes=("email_address",))
    assert harness.apply() == 0
    for stage in ("create", "switch"):
        assert not harness.after[stage]["grants"] & BUSINESS, stage
        assert not {g.rsplit("|", 1)[-1] for g in harness.after[stage]["grants"]} & set(TIERS), stage
    # The only phase that grants comes after verify-switched.
    assert harness.calls.index("grant") > harness.calls.index("verify-access:switch")
    assert harness.after["grant"]["grants"] >= BUSINESS


def test_the_staged_plans_hold_use_with_select_until_grant(harness, tmp_path, monkeypatch):
    """What each phase's plan would create, as the hooks saw it (release-plan saves these)."""
    fresh(harness, tmp_path, monkeypatch)
    plans = {}
    real = pg._plan_json

    def recorded(env_name, layer_dir, stage, plan_file, *extra):
        plan = real(env_name, layer_dir, stage, plan_file, *extra)
        plans[stage] = {f"{c['name']}|{c['index']}" for c in plan["resource_changes"]
                        if c["type"] == "databricks_grant" and "create" in c["change"]["actions"]}
        return plan

    monkeypatch.setattr(pg, "_plan_json", recorded)
    assert harness.apply() == 0
    assert plans == {"create": DEPLOYER_GRANT, "switch": set(), "grant": BUSINESS}


def test_the_fake_layer_reads_the_withholding_stages_from_the_real_root():
    import re

    from tests.test_staged_policy_apply import FAKE_RUNNER

    source = "def withholding_stages" + FAKE_RUNNER.split("def withholding_stages", 1)[1].split("USE_GRANTS =", 1)[0]
    found = {"re": re, "os": os}
    exec(source, found)
    assert found["WITHHOLD"] == {"create", "tighten", "switch", "restore", "retire", "abandon", "rebind"}
    assert found["WITHHOLD_USE"] == {"create", "tighten", "switch", "restore", "retire", "abandon"}


# -- (2) the regression: the exact refusal, and what the message says now ---------------------------
def test_main_s_create_plan_of_a_first_deploy_is_refused_as_creates_not_destroys(harness, tmp_path, monkeypatch,
                                                                                capsys):
    """A root that plans USE in create (main before withhold_new_use_grants): the live error, reworded."""
    fresh(harness, tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_TF_NO_WITHHOLD_USE", "1")
    assert harness.apply() == 1
    err = capsys.readouterr().err
    assert "ERROR: the create plan creates business access before the grant phase; nothing was applied:" in err
    assert "destroys protected resources" not in err
    refused = [line for line in err.splitlines() if line.startswith("  - ")]
    assert len(refused) == len(USE) == 6
    assert all(": create (a business grant: access opens only in the grant phase" in line for line in refused)
    assert {line.split("[", 1)[1].split("]", 1)[0].strip('"') for line in refused} == {
        g.split("|", 1)[1] for g in USE}
    # Nothing was applied, and the journal records the failed create (the live run's state).
    assert live_state(harness.env_dir)["resources"] == []
    t = harness.journal()["transition"]
    assert (t["from"], t["status"], t["failed_phase"], t["completed"]) == ("", "failed", "create", [])


def test_the_heading_names_each_kind_of_refused_change():
    def change(rtype, name, *actions, index="k"):
        return {"type": rtype, "name": name, "index": index, "address": f"module.data_access.{rtype}.{name}[\"{index}\"]",
                "change": {"actions": list(actions)}}

    creates = {"resource_changes": [change("databricks_grant", "catalog_access", "create")]}
    destroys = {"resource_changes": [change("databricks_policy_info", "policies", "delete")]}
    both = {"resource_changes": creates["resource_changes"] + destroys["resource_changes"]}
    heading = {name: pg.violations_heading("create", pg.classified_violations(plan, "create"))
               for name, plan in (("creates", creates), ("destroys", destroys), ("both", both))}
    assert heading["creates"] == ("ERROR: the create plan creates business access before the grant phase; "
                                  "nothing was applied:")
    assert heading["destroys"] == "ERROR: the create plan destroys protected resources; nothing was applied:"
    assert heading["both"] == ("ERROR: the create plan creates business access before the grant phase and "
                               "destroys protected resources; nothing was applied:")
    assert pg.plan_violations(creates, "create") == [
        'module.data_access.databricks_grant.catalog_access["k"]: create (a business grant: access opens only '
        "in the grant phase, after the switch is verified)"]
    retag = {"resource_changes": [change("databricks_entity_tag_assignment", "treatment", "update")]}
    assert pg.violations_heading("abandon", pg.classified_violations(retag, "abandon")) == (
        "ERROR: the abandon plan makes changes the abandon phase may not; nothing was applied:")


# -- (3) a staged det->det change still refuses business grant creates in create ---------------------
def _released_with_access(h, tmp_path, monkeypatch) -> dict:
    gen = fresh(h, tmp_path, monkeypatch)
    assert h.apply() == 0 and h.controller().confirm() == 0
    assert grant_keys(live_state(h.env_dir)) == BUSINESS | DEPLOYER_GRANT
    return gen


def _new_reader(h, group: str = "auditors") -> set[str]:
    access = json.loads(access_file(h).read_text())
    added = {f"catalog_access|cat|{group}", f"schema_access|cat.sch|{group}", f"table_access|cat.sch.t|{group}"}
    access["grants"] = sorted(set(access["grants"]) | added)
    access_file(h).write_text(json.dumps(access))
    return added


def test_a_det_to_det_change_still_refuses_a_business_grant_create_in_create(harness, tmp_path, monkeypatch,
                                                                            capsys):
    old = _released_with_access(harness, tmp_path, monkeypatch)
    added = _new_reader(harness)
    new = write_env(harness.env_dir, deployer=OLD_SP, versions={"email_partial": {"partial": "redacted"}})
    _promoted(harness, old, new)
    _refresh(harness.env_dir)
    before = live_state(harness.env_dir)
    harness.calls.clear()
    capsys.readouterr()
    # A plan that does create USE in create (a raw or older root) is refused before anything applies.
    monkeypatch.setenv("FAKE_TF_NO_WITHHOLD_USE", "1")
    assert harness.apply() == 1
    err = capsys.readouterr().err
    assert "ERROR: the create plan creates business access before the grant phase" in err
    assert sorted(line for line in err.splitlines() if line.startswith("  - ")) == sorted(
        f'  - module.data_access.databricks_grant.{g.split("|")[0]}["{g.split("|", 1)[1]}"]: create (a business '
        "grant: access opens only in the grant phase, after the switch is verified)"
        for g in added if not g.startswith("table_access|"))
    assert live_state(harness.env_dir) == before and harness.calls == ["create"]
    t = harness.journal()["transition"]
    assert (t["from"], t["failed_phase"]) == (old["fingerprint"], "create")

    # With the root withholding new USE, the same change runs, and the new reader opens only at grant.
    monkeypatch.delenv("FAKE_TF_NO_WITHHOLD_USE")
    assert harness.apply() == 0
    for stage in ("create", "switch"):
        assert not harness.after[stage]["grants"] & added, stage
    assert harness.after["grant"]["grants"] >= added
    assert harness.journal()["active"] == new["fingerprint"]


@pytest.mark.parametrize("stage", ["create", "tighten", "restore", "abandon"])
def test_the_create_phase_guard_is_unchanged_for_every_business_grant(stage):
    for name in ("catalog_access", "schema_access", "table_access"):
        plan = {"resource_changes": [{"type": "databricks_grant", "name": name, "index": "k",
                                      "address": f"module.data_access.databricks_grant.{name}[\"k\"]",
                                      "change": {"actions": ["create"]}}]}
        violations = pg.classified_violations(plan, stage)
        assert violations and violations[0][0] in ("create", "change"), (stage, name)


# -- (4) a first deploy that failed at create (main's live run) recovers -----------------------------
def _failed_like_main(h, tmp_path, monkeypatch) -> dict:
    """The live run's journal and state: create refused, nothing applied."""
    gen = fresh(h, tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_TF_NO_WITHHOLD_USE", "1")
    assert h.apply() == 1
    monkeypatch.delenv("FAKE_TF_NO_WITHHOLD_USE")
    t = h.journal()["transition"]
    assert (t["status"], t["failed_phase"], t["completed"], t["from"]) == ("failed", "create", [], "")
    assert live_state(h.env_dir)["resources"] == []
    h.calls.clear()
    h.after.clear()
    return gen


def test_the_failed_create_points_at_resume_and_abandon(harness, tmp_path, monkeypatch, capsys):
    capsys.readouterr()
    gen = write_env(harness.env_dir, deployer=OLD_SP)
    _promoted(harness, gen)
    access_file(harness).write_text(json.dumps({"grants": sorted(GRANTS), "client_id": OLD_SP, "gone": []}))
    monkeypatch.setenv("FAKE_TF_NO_WITHHOLD_USE", "1")
    assert harness.apply() == 1
    err = capsys.readouterr().err
    run_id = harness.journal()["transition"]["run_id"]
    assert (f"Next: fix what stopped it (above), then re-run the same command: it resumes run {run_id} at create. "
            "To drop the change instead, run make policy-abandon ENV=dev.") in err


def test_re_running_the_same_command_resumes_the_failed_first_deploy(harness, tmp_path, monkeypatch, capsys):
    gen = _failed_like_main(harness, tmp_path, monkeypatch)
    run_id = harness.journal()["transition"]["run_id"]
    capsys.readouterr()
    assert harness.apply() == 0
    out = capsys.readouterr().out
    assert f"resuming run {run_id} (failed at create)" in out
    assert harness.calls == FIRST_DEPLOY
    assert harness.after["create"]["grants"] == DEPLOYER_GRANT
    assert harness.after["grant"]["grants"] == BUSINESS | DEPLOYER_GRANT
    journal = harness.journal()
    assert journal["active"] == gen["fingerprint"] and journal["transition"] is None
    assert journal["history"][-1]["run_id"] == run_id


def test_a_failed_first_deploy_whose_config_moved_on_is_abandoned_and_redeployed(harness, tmp_path, monkeypatch):
    old = _failed_like_main(harness, tmp_path, monkeypatch)
    t = harness.journal()["transition"]
    moved = write_env(harness.env_dir, deployer=OLD_SP, versions={"email_partial": {"partial": "redacted"}})
    _promoted(harness, old, moved)
    _refresh(harness.env_dir)
    with pytest.raises(spa.Refused) as refused:
        harness.apply()
    assert "Next: run make policy-abandon ENV=dev, then re-run" in str(refused.value)
    assert harness.controller()._may_abandon(t, pg.desired_generation(harness.env_dir))

    assert harness.controller().abandon() == 0
    # Nothing was live, so nothing was planned or applied: an abandon plan of the empty state would only create.
    assert harness.calls == [] and live_state(harness.env_dir)["resources"] == []
    journal = harness.journal()
    assert journal["transition"] is None and journal["active"] == ""
    assert journal["history"][-1] == {"run_id": t["run_id"], "abandoned": t["to"], "at": journal["history"][-1]["at"],
                                      "applied": False}

    assert harness.apply() == 0
    assert harness.calls == FIRST_DEPLOY
    assert harness.journal()["active"] == moved["fingerprint"]
    assert harness.after["grant"]["grants"] == BUSINESS | DEPLOYER_GRANT


def test_make_policy_abandon_drops_the_failed_first_deploy(harness, tmp_path, monkeypatch):
    """The real make policy-abandon target (its live refresh, promote and controller) on the live run's shape."""
    _failed_like_main(harness, tmp_path, monkeypatch)
    t = harness.journal()["transition"]
    result = _make_abandon(harness, tmp_path, Path(os.environ["GENIERAILS_COVERAGE_GATE_RUNNER"]))
    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert f"Abandoned generation {t['to']}: run {t['run_id']} applied no policy or treatment tag" in out
    assert harness.journal()["transition"] is None
    assert live_state(harness.env_dir)["resources"] == []


def test_abandon_of_a_first_deploy_with_policies_live_still_plans(harness, tmp_path, monkeypatch):
    """Only a first deploy with nothing live skips the abandon plan."""
    fresh(harness, tmp_path, monkeypatch)
    harness.fail_verify = set()
    assert harness.apply(FAKE_TF_FAIL_APPLY="switch") == 1  # create applied; switch failed and restored
    t = harness.journal()["transition"]
    assert "create" in t["completed"] and live_policies(live_state(harness.env_dir))
    assert not harness.controller()._nothing_applied(t)


# -- (5) the coverage check of a first deploy's create ------------------------------------------------
def _journaled_first_deploy(h, gen) -> None:
    pg.save_snapshot(h.env_dir, gen)
    spa.save_journal(h.env_dir, spa.empty_journal() | {"transition": {
        "run_id": "r1", "from": "", "to": gen["fingerprint"], "retain": "", "status": "running"}})


def _render(h, gen, stage="create") -> Path:
    rendered = pg.plan_stage(stage, gen, None, None)["rendered"]
    return pg.write_stage(h.env_dir / "data_access", rendered) / "abac.auto.tfvars"


def _runner_answer(path: Path, answer: dict) -> Path:
    encoded = base64.b64encode(json.dumps(answer).encode()).decode()
    runner = path / "runner"
    runner.write_text(f"#!/bin/sh\necho '+ terraform console'\necho '\"{encoded}\"'\n")
    runner.chmod(0o755)
    return runner


def _run_create_gate(h, tmp_path: Path, gen: dict, *, validation_dir: Path | None = None) -> int:
    staged = pg.plan_stage("create", gen, None, None)
    rendered_dir = pg.write_stage(h.env_dir / "data_access", staged["rendered"])
    runner = _runner_answer(tmp_path, {
        "fingerprint": "create-fp",
        "grant_tables": ["cat.sch.t"],
        "acknowledged_columns": [],
        "needs_gate": False,
        "new_grants": ["cat.sch.t|raw"],
    })
    return cg.run_gate(
        h.env_dir, h.env_dir.name, runner, "", False, rendered_dir,
        {"stage": "create", "old_fingerprint": "", "keep_new_tags": False},
        validation_dir,
    )


def _write_live_generation_state(env_dir: Path, gen: dict) -> None:
    policy = gen["fgac_policies"][0]
    column, value = next(iter(gen["tags"].items()))
    state = {
        "version": 4,
        "resources": [
            {
                "module": "module.data_access",
                "mode": "managed",
                "type": "databricks_policy_info",
                "name": "policies",
                "instances": [{
                    "index_key": policy["name"],
                    "attributes": {
                        "to_principals": policy.get("to_principals") or [],
                        "except_principals": policy.get("except_principals") or [],
                        "policy_type": policy.get("policy_type"),
                        "catalog": policy.get("catalog"),
                        "match_condition": policy.get("match_condition"),
                    },
                }],
            },
            {
                "module": "module.data_access",
                "mode": "managed",
                "type": "databricks_entity_tag_assignment",
                "name": "treatment",
                "instances": [{
                    "index_key": f"columns|{column}|gr_treatment",
                    "attributes": {"entity_name": column, "tag_value": value},
                }],
            },
        ],
    }
    path = env_dir / "data_access" / "terraform.tfstate"
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(state))


def _write_release_receipt(env_dir: Path, env: str = "prod") -> None:
    from scripts import release_gate as rg

    cloud = rg.cloud_dir_of(env_dir)
    manifest = {"id": "already-released"}
    manifest_path = rg.manifests_dir(cloud, env) / manifest["id"] / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
    receipt = {
        "kind": "release",
        "version": rg.VERSION,
        "env": env,
        "cloud": cloud.name,
        "manifest": {"id": manifest["id"], "sha256": rg.sha256_file(manifest_path)},
    }
    receipt_path = rg.release_dir(cloud, env) / "20990101T000000Z-released.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, sort_keys=True) + "\n")


def _release_receipt_dir(env_dir: Path) -> Path:
    from scripts import release_gate as rg

    directory = rg.release_dir(rg.cloud_dir_of(env_dir), env_dir.name)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _active_old_generation_without_new_treatment(h, tmp_path: Path, monkeypatch) -> tuple[dict, dict]:
    gen = fresh(h, tmp_path, monkeypatch, column_classes={"cat.sch.t.email": ["email_address"]})
    old = dict(gen, tags={})
    old["fingerprint"] = pg.model_fingerprint(
        {"fgac_policies": old["fgac_policies"], "tag_assignments": []},
        old["functions_sql"],
    )
    assert old["fingerprint"] != gen["fingerprint"]
    assert pg.snapshot_intact(old, old["fingerprint"])
    pg.save_snapshot(h.env_dir, old)
    pg.save_snapshot(h.env_dir, gen)
    spa.save_journal(h.env_dir, spa.empty_journal() | {
        "active": old["fingerprint"],
        "transition": {
            "run_id": "r1",
            "from": old["fingerprint"],
            "to": gen["fingerprint"],
            "retain": "",
            "status": "running",
        },
    })
    return gen, old


def _run_old_generation_gate(h, tmp_path: Path, gen: dict, old: dict, stage: str) -> tuple[int, dict]:
    staged = pg.plan_stage(stage, gen, old, None)
    rendered_dir = pg.write_stage(h.env_dir / "data_access", staged["rendered"])
    validation_dir = None
    if pg._normal(staged["coverage_rendered"]) != pg._normal(staged["rendered"]):
        validation_dir = pg.write_stage(
            h.env_dir / "data_access",
            staged["coverage_rendered"],
            out_dir=pg.coverage_config_dir(h.env_dir),
        )
    runner = _runner_answer(tmp_path, {
        "fingerprint": "old-gen-fp",
        "grant_tables": ["cat.sch.t"],
        "acknowledged_columns": [],
        "needs_gate": False,
        "new_grants": ["cat.sch.t|raw"],
    })
    rc = cg.run_gate(
        h.env_dir, "prod", runner, "", False, rendered_dir,
        {"stage": stage, "old_fingerprint": old["fingerprint"], "keep_new_tags": False},
        validation_dir,
    )
    return rc, json.loads((h.env_dir / "data_access" / cg.GATE_FILENAME).read_text())


def test_the_create_render_of_a_first_deploy_is_judged_as_one(harness, tmp_path, monkeypatch):
    gen = fresh(harness, tmp_path, monkeypatch)
    _journaled_first_deploy(harness, gen)
    tfvars = _render(harness, gen)
    assert not [i for i in pg._load(tfvars)["tag_assignments"] if i["tag_key"] == "gr_treatment"]
    staged = {"stage": "create", "old_fingerprint": "", "keep_new_tags": False}
    refreshed_at, detail, kept = cg._live_refresh(harness.env_dir, tfvars, staged)
    assert refreshed_at is not None, detail
    assert kept == {"kept_from": "", "uncovered_columns": sorted(gen["tags"])}
    assert "create of a first deploy keeps no column tagged" in detail and "opens no access" in detail
    pending = cg.pending_treatments(harness.env_dir, staged)
    assert sorted(i["entity_name"] for i in pending) == sorted(gen["tags"])
    # A phase that opens access with those columns untagged still refuses.
    refreshed_at, detail, _ = cg._live_refresh(harness.env_dir, tfvars, staged | {"opens_access": True})
    assert refreshed_at is None and "this phase opens access" in detail


def test_release_plan_create_of_a_first_prod_release_needs_no_policy_journal(harness, tmp_path, monkeypatch):
    gen = fresh(harness, tmp_path, monkeypatch,
                column_classes={"cat.sch.t.email": ["email_address"]})
    tfvars = _render(harness, gen)
    assert spa.load_journal(harness.env_dir) is None

    staged = {"stage": "create", "old_fingerprint": "", "keep_new_tags": False}
    refreshed_at, detail, kept = cg._live_refresh(harness.env_dir, tfvars, staged)
    assert refreshed_at is not None, detail
    assert kept == {"kept_from": "", "uncovered_columns": sorted(gen["tags"])}
    assert "create of a first deploy keeps no column tagged" in detail
    pending = cg.pending_treatments(harness.env_dir, staged)
    assert sorted(i["entity_name"] for i in pending) == sorted(gen["tags"])

    assert _run_create_gate(harness, tmp_path, gen) == 0
    gate = json.loads((harness.env_dir / "data_access" / cg.GATE_FILENAME).read_text())
    assert gate["status"] == "pass"


def test_first_deploy_create_gate_validates_the_generated_end_state(harness, tmp_path, monkeypatch):
    gen = fresh(harness, tmp_path, monkeypatch,
                column_classes={"cat.sch.t.email": ["email_address"]})
    _journaled_first_deploy(harness, gen)
    staged = pg.plan_stage("create", gen, None, None)
    assert staged["rendered"]["tags"] == {}
    assert staged["coverage_rendered"]["tags"] == gen["tags"]
    validation_dir = pg.write_stage(
        harness.env_dir / "data_access", staged["coverage_rendered"], out_dir=pg.coverage_config_dir(harness.env_dir),
    )

    assert _run_create_gate(harness, tmp_path, gen, validation_dir=validation_dir) == 0
    gate = json.loads((harness.env_dir / "data_access" / cg.GATE_FILENAME).read_text())
    assert gate["status"] == "pass"
    assert gate["uncovered_columns"] == ["cat.sch.t.email"]


def test_first_deploy_create_gate_still_withholds_for_a_real_gap(harness, tmp_path, monkeypatch, capfd):
    gen = fresh(harness, tmp_path, monkeypatch,
                column_classes={"cat.sch.t.email": ["email_address"]})
    _journaled_first_deploy(harness, gen)
    broken = dict(pg.plan_stage("create", gen, None, None)["coverage_rendered"])
    broken["fgac_policies"] = []
    validation_dir = pg.write_stage(harness.env_dir / "data_access", broken, out_dir=pg.coverage_config_dir(harness.env_dir))

    assert _run_create_gate(harness, tmp_path, gen, validation_dir=validation_dir) == 0
    output = capfd.readouterr()
    gate = json.loads((harness.env_dir / "data_access" / cg.GATE_FILENAME).read_text())
    assert gate["status"] == "fail"
    assert "classified but unprotected columns" in output.out
    assert "1 new SELECT grant(s) are withheld (cat.sch.t|raw)" in output.err


def test_unjournaled_first_release_create_still_withholds_for_a_real_gap(harness, tmp_path, monkeypatch, capfd):
    gen = fresh(harness, tmp_path, monkeypatch,
                column_classes={"cat.sch.t.email": ["email_address"]})
    broken = dict(pg.plan_stage("create", gen, None, None)["coverage_rendered"])
    broken["fgac_policies"] = []
    validation_dir = pg.write_stage(
        harness.env_dir / "data_access", broken, out_dir=pg.coverage_config_dir(harness.env_dir),
    )

    assert spa.load_journal(harness.env_dir) is None
    assert _run_create_gate(harness, tmp_path, gen, validation_dir=validation_dir) == 0
    output = capfd.readouterr()
    gate = json.loads((harness.env_dir / "data_access" / cg.GATE_FILENAME).read_text())
    assert gate["status"] == "fail"
    assert "classified but unprotected columns" in output.out
    assert "1 new SELECT grant(s) are withheld (cat.sch.t|raw)" in output.err


def test_unjournaled_empty_old_generation_refuses_when_prod_already_has_live_generation(
        harness, tmp_path, monkeypatch):
    gen = fresh(harness, tmp_path, monkeypatch)
    _write_live_generation_state(harness.env_dir, gen)
    tfvars = _render(harness, gen)
    staged = {"stage": "create", "old_fingerprint": "", "keep_new_tags": False}

    refreshed_at, detail, kept = cg._live_refresh(harness.env_dir, tfvars, staged)
    assert refreshed_at is None and kept == {}
    assert "already holds deterministic policies or treatment tags" in detail
    assert cg.pending_treatments(harness.env_dir, staged) is None


def test_unjournaled_empty_old_generation_refuses_after_a_prod_release_receipt(harness, tmp_path, monkeypatch):
    gen = fresh(harness, tmp_path, monkeypatch)
    _write_release_receipt(harness.env_dir, harness.env_dir.name)
    tfvars = _render(harness, gen)
    staged = {"stage": "create", "old_fingerprint": "", "keep_new_tags": False}

    refreshed_at, detail, kept = cg._live_refresh(harness.env_dir, tfvars, staged)
    assert refreshed_at is None and kept == {}
    assert "a prior release receipt exists" in detail
    assert cg.pending_treatments(harness.env_dir, staged) is None


def test_unjournaled_empty_old_generation_refuses_with_invalid_newest_release_receipt(
        harness, tmp_path, monkeypatch):
    gen = fresh(harness, tmp_path, monkeypatch)
    (_release_receipt_dir(harness.env_dir) / "20990101T000000Z-broken.json").write_text("{not json")
    tfvars = _render(harness, gen)
    staged = {"stage": "create", "old_fingerprint": "", "keep_new_tags": False}

    refreshed_at, detail, kept = cg._live_refresh(harness.env_dir, tfvars, staged)
    assert refreshed_at is None and kept == {}
    assert "records no first deploy" in detail and "newest release receipt is invalid" in detail
    assert cg.pending_treatments(harness.env_dir, staged) is None


def test_unjournaled_empty_old_generation_refuses_when_first_release_proof_raises(
        harness, tmp_path, monkeypatch):
    gen = fresh(harness, tmp_path, monkeypatch)
    tfvars = _render(harness, gen)

    def boom(*_args, **_kwargs):
        raise spa.Refused("remote state pull failed")

    monkeypatch.setattr(spa, "_read_state", boom)
    staged = {"stage": "create", "old_fingerprint": "", "keep_new_tags": False}

    refreshed_at, detail, kept = cg._live_refresh(harness.env_dir, tfvars, staged)
    assert refreshed_at is None and kept == {}
    assert "records no first deploy" in detail and "could not prove this is the env's first release" in detail
    assert cg.pending_treatments(harness.env_dir, staged) is None


def test_unjournaled_empty_old_generation_is_not_first_release_for_tighten(harness, tmp_path, monkeypatch):
    gen = fresh(harness, tmp_path, monkeypatch)
    tfvars = _render(harness, gen)
    staged = {"stage": "tighten", "old_fingerprint": "", "keep_new_tags": False}

    refreshed_at, detail, kept = cg._live_refresh(harness.env_dir, tfvars, staged)
    assert refreshed_at is None and kept == {}
    assert "records no first deploy" in detail
    assert "only recognized for the create phase" in detail
    assert cg.pending_treatments(harness.env_dir, staged) is None


@pytest.mark.parametrize("stage", ["create", "tighten"])
def test_create_and_tighten_gate_validate_end_state_tags_for_active_old_generation(
        harness, tmp_path, monkeypatch, capfd, stage):
    gen, old = _active_old_generation_without_new_treatment(harness, tmp_path, monkeypatch)
    rc, gate = _run_old_generation_gate(harness, tmp_path, gen, old, stage)
    output = capfd.readouterr()
    assert rc == 0
    assert gate["status"] == "pass", (gate, output.out[-3000:], output.err[-1500:])
    assert gate["uncovered_columns"] == ["cat.sch.t.email"]


def test_coverage_rendered_uses_end_state_tags_only_for_pre_switch_validation_phases(
        harness, tmp_path, monkeypatch):
    gen, old = _active_old_generation_without_new_treatment(harness, tmp_path, monkeypatch)
    phases = {}

    for stage in pg.APPLY_STAGES:
        staged = pg.plan_stage(stage, gen, old, None)
        coverage_tags = staged["coverage_rendered"]["tags"]
        rendered_tags = staged["rendered"]["tags"]
        phases[stage] = {
            "rendered_tags_are_desired": rendered_tags == gen["tags"],
            "coverage_tags_are_desired": coverage_tags == gen["tags"],
            "coverage_differs_from_rendered": pg._normal(staged["coverage_rendered"]) != pg._normal(
                staged["rendered"]),
        }

    assert phases == {
        "create": {"rendered_tags_are_desired": False, "coverage_tags_are_desired": True,
                   "coverage_differs_from_rendered": True},
        "tighten": {"rendered_tags_are_desired": False, "coverage_tags_are_desired": True,
                    "coverage_differs_from_rendered": True},
        "switch": {"rendered_tags_are_desired": True, "coverage_tags_are_desired": True,
                   "coverage_differs_from_rendered": False},
        "restore": {"rendered_tags_are_desired": False, "coverage_tags_are_desired": True,
                    "coverage_differs_from_rendered": True},
        "retire": {"rendered_tags_are_desired": True, "coverage_tags_are_desired": True,
                   "coverage_differs_from_rendered": False},
        "abandon": {"rendered_tags_are_desired": False, "coverage_tags_are_desired": False,
                    "coverage_differs_from_rendered": False},
    }


@pytest.mark.parametrize("journal", [
    {"active": "0" * 16},  # an active generation: not a first deploy
    {"transition": {"from": "0" * 16}},  # a transition from a generation
    {"transition": {"to": "0" * 16}},  # a transition to another generation
])
def test_an_empty_old_generation_without_a_first_deploy_on_record_fails_closed(harness, tmp_path, monkeypatch,
                                                                               journal):
    gen = fresh(harness, tmp_path, monkeypatch)
    _journaled_first_deploy(harness, gen)
    record = spa.load_journal(harness.env_dir)
    if journal is None:
        spa.journal_path(harness.env_dir).unlink()
    else:
        record.update({k: v for k, v in journal.items() if k != "transition"})
        if "transition" in journal:
            record["transition"] |= journal["transition"]
        spa.save_journal(harness.env_dir, record)
    tfvars = _render(harness, gen)
    staged = {"stage": "create", "old_fingerprint": "", "keep_new_tags": False}
    refreshed_at, detail, kept = cg._live_refresh(harness.env_dir, tfvars, staged)
    assert refreshed_at is None and "records no first deploy" in detail and kept == {}
    assert cg.pending_treatments(harness.env_dir, staged) is None


def _validate(h, tfvars: Path, pending: list | None) -> subprocess.CompletedProcess:
    command = [sys.executable, str(SHARED / "validate_abac.py"), "--coverage-gate", str(tfvars),
               str(tfvars.parent / "masking_functions.sql"), "--ddl", str(h.env_dir / "ddl" / "_fetched.sql")]
    context = h.env_dir / "context.json"
    context.write_text(json.dumps({"first_exposure_tables": ["cat.sch.t"], "acknowledged_columns": []}))
    command += ["--exposure-context", str(context)]
    if pending is not None:
        path = h.env_dir / "pending.json"
        path.write_text(json.dumps(pending))
        command += ["--pending-treatments", str(path)]
    return subprocess.run(command, text=True, capture_output=True, cwd=h.env_dir / "data_access")


def test_the_create_render_alone_is_vacuous_but_is_checked_against_the_switch_tags(harness, tmp_path,
                                                                                  monkeypatch):
    gen = fresh(harness, tmp_path, monkeypatch)
    _journaled_first_deploy(harness, gen)
    tfvars = _render(harness, gen)
    # What main judged, and the live run reported: a vacuous pass refused, and the governed column
    # called untagged (the render carries no gr_treatment yet).
    alone = _validate(harness, tfvars, None)
    assert alone.returncode == 1 and "refusing a vacuous pass" in alone.stdout, alone.stdout
    assert "NO class.*/sensitivity tag and NO gr_treatment" in alone.stdout
    assert "cat.sch.t.email (looks like: email)" in alone.stdout
    pending = cg.pending_treatments(harness.env_dir, {"stage": "create", "old_fingerprint": ""})
    checked = _validate(harness, tfvars, pending)
    assert checked.returncode == 0, checked.stdout
    assert "vacuous" not in checked.stdout and "NO gr_treatment" not in checked.stdout
    assert "Coverage check: 1 classified/treatment column(s) fully protected" in checked.stdout


def test_a_genuinely_uncovered_sensitive_column_still_fails_the_first_deploy_create(harness, tmp_path,
                                                                                   monkeypatch):
    """An untagged sensitive-looking column (no class.* tag, so no treatment) still blocks."""
    gen = fresh(harness, tmp_path, monkeypatch, ddl_text="CREATE TABLE cat.sch.t (\n  email STRING,\n  ssn STRING\n);\n")
    _journaled_first_deploy(harness, gen)
    tfvars = _render(harness, gen)
    pending = cg.pending_treatments(harness.env_dir, {"stage": "create", "old_fingerprint": ""})
    checked = _validate(harness, tfvars, pending)
    assert checked.returncode == 1 and "cat.sch.t.ssn" in checked.stdout, checked.stdout
    # And a pending treatment whose policy the render lacks is unprotected.
    tfvars.write_text(tfvars.read_text().replace("hasTagValue('gr_treatment', ", "hasTagValue('gr_other', "))
    broken = _validate(harness, tfvars, pending)
    assert broken.returncode == 1 and "no covering column-mask policy" in broken.stdout, broken.stdout


def test_a_det_to_det_create_is_not_checked_against_pending_tags(harness, tmp_path, monkeypatch):
    old = _released_with_access(harness, tmp_path, monkeypatch)
    new = write_env(harness.env_dir, deployer=OLD_SP, versions={"email_partial": {"partial": "redacted"}})
    pg.save_snapshot(harness.env_dir, new)
    journal = spa.load_journal(harness.env_dir)
    journal["transition"] = {"run_id": "r2", "from": old["fingerprint"], "to": new["fingerprint"], "retain": ""}
    spa.save_journal(harness.env_dir, journal)
    assert cg.pending_treatments(harness.env_dir, {"stage": "create", "old_fingerprint": old["fingerprint"]}) is None
    assert cg.pending_treatments(harness.env_dir, {"stage": "create", "old_fingerprint": ""}) is None


# -- (6) the root: USE waits with SELECT (terraform test, mock providers) -------------------------------
@pytest.mark.skipif(__import__("shutil").which("terraform") is None, reason="terraform not installed")
def test_the_root_plans_business_use_only_in_the_grant_phase(tmp_path):
    from tests.terraform_helpers import shared_copy, tf, tf_env, tf_init

    root = shared_copy(tmp_path) / "roots" / "data_access"
    env = tf_env(tmp_path)
    tf_init(root, env=env)
    result = tf(root, "test", "-no-color", "-filter=tests/staged_use_grants.tftest.hcl", env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    for run in ("first_deploy_create_plans_no_business_use", "tighten_plans_no_business_use",
                "switch_plans_no_business_use", "restore_plans_no_business_use",
                "retire_plans_no_business_use", "abandon_plans_no_business_use",
                "grant_opens_use_with_select", "rebind_is_not_withheld", "preview_plans_use",
                "legacy_mode_is_not_withheld", "create_keeps_applied_use_and_withholds_new"):
        assert f'run "{run}"... pass' in result.stdout, result.stdout
