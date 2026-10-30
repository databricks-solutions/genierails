"""A first deterministic prod release, end to end through every release-plan/release round.

Real make runs the real release-plan, _release-plan-roots and
_release-manifest recipes (and release_gate.py) against a git checkout in
tmp_path (tests/test_release_gate_make.py's harness). The staged policy
controller, the live agent coverage check, the coverage gate and the root
runner are one fake Unity Catalog ("the world") that keeps what is live
between rounds: masks (create/switch), SELECT (grant) and Genie CAN_RUN
(the saved workspace plan). Like modules/coverage_gate_check, a workspace
plan holds CAN_RUN only if the live agent coverage check passed when it was
planned.

A first release stages create -> switch -> grant, and the workspace is planned
with the last phase. Planned then, before grant applies SELECT, the coverage
check fails and the saved workspace plan withholds CAN_RUN, so the release
ended in "access was withheld". The workspace now waits for a release-plan
after the SELECT is live: the release finishes in one more round, never
granting CAN_RUN before the masks and SELECT are in place.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

from scripts import release_gate as rg
from tests import script_stubs
from tests.test_release_gate import Repo
from tests.test_release_gate_make import Make

WORLD = r'''
"""The fake Unity Catalog: <world.json> <role> <args...>."""
import hashlib, json, sys
from pathlib import Path

world_path, role, args = Path(sys.argv[1]), sys.argv[2], sys.argv[3:]
world = json.loads(world_path.read_text())


def save():
    world_path.write_text(json.dumps(world))


def arg(name):
    return args[args.index(name) + 1]


def event(name):
    world["events"].append(name)
    if name == "select" and not world["masks"]:
        world["violations"].append("SELECT opened before the masks were live")
    if name == "can_run" and not (world["masks"] and world["select"]):
        world["violations"].append("CAN_RUN granted before the masks and SELECT were live")


def staged_plan(out):
    out.mkdir(parents=True, exist_ok=True)
    first = ["create", "verify-created", "switch", "verify-switched", "grant"]
    phases = first if not world["finished"] else ["create", "grant"]
    done = world["completed"]
    remaining = [p for p in phases if p not in done]
    steps, stopped = [], False
    for phase in remaining:
        if phase.startswith("verify-"):
            steps.append({"phase": phase, "kind": "verify"})
            continue
        if stopped:
            break
        changes = not world["finished"]
        plan = out / f"{len(steps):02d}-{phase}.tfplan"
        plan.write_text(phase)
        steps.append({"phase": phase, "kind": "apply", "file": plan.name,
                      "sha256": hashlib.sha256(plan.read_bytes()).hexdigest(), "changes": changes})
        stopped = changes
    planned = {s["phase"] for s in steps}
    (out / "phases.json").write_text(json.dumps({
        "transition": {"from": "", "to": "g1", "retain": "", "retire": "", "tier_move": False,
                       "phases": phases, "completed": list(done), "resumed": bool(done)},
        "steps": steps, "final": all(p in planned for p in remaining)}))
    return 0


def staged_apply(plan_dir):
    saved = json.loads((plan_dir / "phases.json").read_text())
    for step in saved["steps"]:
        if step["kind"] == "apply" and step["changes"]:
            if step["phase"] in ("create", "switch"):
                world["masks"] = True
                event("masks")
            elif step["phase"] == "grant":
                world["select"] = True
                event("select")
        world["completed"].append(step["phase"])
    if saved["final"]:
        world["finished"], world["completed"] = True, []
        return 0
    return 10


rc = 0
if role == "staged" and args[0] == "plan":
    rc = staged_plan(Path(arg("--out-dir")))
elif role == "staged" and args[0] == "apply-saved":
    rc = staged_apply(Path(arg("--plan-dir")))
elif role == "agents" and args[0] == "run":
    world["coverage"] = world["masks"] and world["select"]
    if not world["coverage"]:
        print("WARNING: new Genie CAN_RUN is withheld for sales (no SELECT)", file=sys.stderr)
    rc = 1 if "--strict" in args and not world["coverage"] else 0
elif role == "gate" and args[0] == "can-run-check":
    world["can_run_desired"] = True
elif role == "gate" and args[0] == "withheld" and arg("--layer") == "workspace":
    rc = 1 if world["can_run_desired"] and not world["can_run"] else 0
elif role == "runner" and args[:1] == ["account"] and "plan" in args:
    Path(next(a for a in args if a.startswith("-out="))[5:]).write_text("account")
elif role == "runner" and args[:1] == ["workspace"] and "plan" in args:
    Path(next(a for a in args if a.startswith("-out="))[5:]).write_text(
        json.dumps({"can_run": world["coverage"]}))
    rc = 2
elif role == "runner" and args[:1] == ["workspace"] and "apply-plan" in args:
    if json.loads(Path(args[-1]).read_text())["can_run"]:
        world["can_run"] = True
        event("can_run")
save()
sys.exit(rc)
'''


@pytest.fixture
def world(tmp_path, monkeypatch):
    return build_world(tmp_path, monkeypatch)


def build_world(tmp_path, monkeypatch, passthrough=()):
    """(make, repo, world.json, script stubs); ``passthrough``: more recursive makes to run for real."""
    repo = Repo(tmp_path, monkeypatch)
    # Agents in prod, so the workspace layer (CAN_RUN) is part of the release.
    repo.commit({"aws/envs/prod/abac.auto.tfvars": "genie_spaces = []\n",
                 "aws/envs/prod/agents/sales.auto.tfvars": "# an agent\n"})
    (repo.prod / "data_access").mkdir()  # the split layer dir (empty, so the checkout stays clean)
    make = Make(tmp_path, repo, passthrough=("_release-plan-roots", "_release-manifest", *passthrough))
    state = tmp_path / "world.json"
    state.write_text(json.dumps({"masks": False, "select": False, "can_run": False, "coverage": False,
                                 "can_run_desired": False, "finished": False, "completed": [],
                                 "events": [], "violations": []}))
    fake = tmp_path / "world.py"
    fake.write_text(WORLD)
    for role in ("staged", "gate", "runner", "agents"):
        path = tmp_path / f"world-{role}"
        path.write_text(f"#!/bin/sh\nexec {sys.executable} {fake} {state} {role} \"$@\"\n")
        path.chmod(0o755)
        make.stubs[role] = path
    stubs = {"agent_coverage.py": str(make.stubs["agents"]), "agent_ownership.py": "true",
             "sticky_governance.py": "true", "genie_adopt_preflight.py": "true"}
    return make, repo, state, stubs


def test_a_first_release_opens_can_run_only_after_masks_and_select_and_never_ends_withheld(world):
    make, repo, state, stubs = world
    assert make("rehearse", "dev").returncode == 0
    plans, rounds = [], []
    for _ in range(6):
        time.sleep(1.1)  # manifest ids are per second
        plan = make("release-plan", "prod", "IMPORT_EXISTING_SCRIPT=true", stubs=stubs)
        assert plan.returncode == 0, plan.stdout + plan.stderr
        plans.append(plan)
        release = make("release", "prod", "IMPORT_EXISTING_SCRIPT=true", stubs=stubs)
        rounds.append(release)
        assert "access was withheld" not in release.stderr, release.stdout + release.stderr
        assert release.returncode == 0, release.stdout + release.stderr
        if "=== Release complete (prod) ===" in release.stdout:
            break
        assert "=== Release phase applied (prod) ===" in release.stdout, release.stdout
    else:
        pytest.fail("the first release never completed")
    world_state = json.loads(state.read_text())
    assert world_state["violations"] == []
    assert world_state["events"] == ["masks", "masks", "select", "can_run"]
    # create, switch, grant, then the workspace with the SELECT live.
    assert len(rounds) == 4
    assert "the workspace is planned once its SELECT is live" in plans[2].stdout
    assert "deferred: the saved grant phase opens SELECT first" in rounds[2].stdout
    assert "This manifest opened the SELECT the Genie agents need" in rounds[2].stdout
    assert rg.last_release(repo.cloud, "prod") is not None
