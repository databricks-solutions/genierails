#!/usr/bin/env python3
"""Staged policy changes for deterministic governance (rollout step 6).

Every data_access apply of a deterministic env runs through this controller
(make apply, apply-governance, rehearse, release and maintain). A change to
policies, functions, treatment values or tier principals is applied in
separate Terraform applies under the env lock, each followed by a live
verification, in this order:

1. ``create``: new functions and policies; nothing removed or retagged.
2. ``verify-created``: the live policies, functions and tags match the render
   (the effective-policies API) and every column still gives each tier exactly
   its old output (verify-access with the old tier settings).
3. ``tighten`` and ``verify-tightened`` (tier moves only): principals added
   before any are removed, so nobody is ever unmasked.
4. ``switch``: retag columns and move principals to the desired generation.
5. ``verify-switched``: live match plus verify-access with the desired settings.
6. ``retire``, ``verify-retired``, ``drop-functions``: only for a generation an
   **earlier** run made obsolete and a later successful run confirmed
   (``confirm``); never the generation this run switched away from.
7. ``grant``: new SELECT (table_readers). Every phase above withholds new
   grants (``policy_stage``), so access opens only once its protection is
   applied and verified.

Two-phase release (rollout step 9). ``plan --out-dir D`` (make release-plan)
saves the next phases' plans instead of applying them: it plans each remaining
apply phase in order, recording a phase whose plan changes nothing and
planning on, and stops after the first phase that changes something (with the
verify phases right after it). Terraform can't plan a phase against state an
earlier unapplied phase would leave, so a staged policy change takes one
manifest (and approval) per changing phase; a release that changes no policy
is one manifest. ``apply-saved --plan-dir D`` (make release MANIFEST=...)
applies exactly those saved plans and runs those verifications, refusing when
the journal or the generated config moved since they were planned. Exit 10
means phases remain for the next release-plan.

The journal (``<env>/data_access/.policy_generations/journal.json``, or under
``$GENIERAILS_STATE_DIR/<cloud>/<env>/records/`` when that is set, so a fresh
checkout finds it: ``policy_generations.ledger_dir``) records the active generation, the obsolete one waiting
for retirement and the transition in progress. A failed tighten or switch
phase restores the previous protection at once. Any unfinished transition
makes the next run resume it, or refuse when the generated config has moved
on; ``abandon`` drops one that never switched. Legacy envs are never staged:
the controller exits 0 and touches nothing.

Exit codes: 0 done, 1 a phase failed (the journal says what is live), 2 a
usage or internal error, 3 refused before applying anything, 10 (apply-saved)
the saved phases are applied and more remain.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

SHARED = Path(__file__).resolve().parents[1]
if str(SHARED) not in sys.path:
    sys.path.insert(0, str(SHARED))

from scripts import environment_lock  # noqa: E402
from scripts import policy_generations as pg  # noqa: E402

JOURNAL_VERSION = 1
JOURNAL_NAME = "journal.json"
HOOK_DIR = SHARED / "scripts" / "policy_hooks"
INVENTORY = SHARED / "scripts" / "staged_policy_verify.py"
APPLY_PHASES = {"create": "create", "tighten": "tighten", "switch": "switch", "retire": "retire",
                "drop-functions": "drop-functions", "grant": "grant"}
# verify phase -> (stages the last applied hook may be, run verify-access too)
VERIFY_PHASES = {
    "verify-created": (("create", "restore"), True),
    "verify-tightened": (("tighten",), False),
    "verify-switched": (("switch",), True),
    "verify-retired": (("retire",), False),
}
# A failure here may leave columns on the new value or principals moved, so
# the previous protection is restored before the run stops.
RESTORE_ON_FAILURE = {"tighten", "verify-tightened", "switch", "verify-switched"}
HISTORY_LIMIT = 20


class Refused(RuntimeError):
    """The run stops before applying anything."""


PHASES_FILE = "phases.json"
MORE_PHASES = 10


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def governance_mode(env_dir: Path) -> str:
    return str(pg._load(env_dir / "env.auto.tfvars").get("governance_mode") or "legacy")


def journal_path(env_dir: Path) -> Path:
    """The journal, wherever the ledger lives; a ledger that can't be located safely refuses."""
    from scripts.state_layout import RecordError

    try:
        return pg.ledger_dir(env_dir) / JOURNAL_NAME
    except RecordError as exc:
        raise Refused(f"the policy journal can't be read: {exc}") from exc


def empty_journal() -> dict:
    return {"version": JOURNAL_VERSION, "active": "", "pending": [], "transition": None, "history": []}


def load_journal(env_dir: Path) -> dict | None:
    """The journal, None when there is none; an unreadable one is never ignored."""
    path = journal_path(env_dir)
    if not path.exists():
        return None
    try:
        journal = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise Refused(f"the policy journal {path} is unreadable ({exc}); restore it from a backup") from exc
    if not isinstance(journal, dict) or journal.get("version") != JOURNAL_VERSION:
        raise Refused(f"the policy journal {path} has an unknown format; refusing to guess what is live")
    return journal


def save_journal(env_dir: Path, journal: dict) -> None:
    pg.write_json(journal_path(env_dir), journal)


def live_from_state(state: dict | None) -> dict:
    """The deterministic mask policies and treatment tags a data_access state holds."""
    policies: dict[str, dict] = {}
    tags: dict[str, str] = {}
    for resource in (state or {}).get("resources") or []:
        if resource.get("module") != "module.data_access" or resource.get("mode", "managed") != "managed":
            continue
        for instance in resource.get("instances") or []:
            if instance.get("deposed"):
                continue
            key = str(instance.get("index_key") or "")
            attrs = instance.get("attributes") or {}
            if resource.get("type") == "databricks_policy_info" and resource.get("name") == "policies" \
                    and key.startswith("gr_mask_"):
                policies[key] = {"to": sorted(attrs.get("to_principals") or []),
                                 "except": sorted(attrs.get("except_principals") or [])}
            elif resource.get("type") == "databricks_entity_tag_assignment" and resource.get("name") == "treatment":
                tags[str(attrs.get("entity_name") or key.split("|")[1])] = str(attrs.get("tag_value") or "")
    return {"policies": policies, "tags": tags}


def _expected_live(generations: list[dict]) -> dict[str, dict]:
    """Policy name -> principals, the first generation's definition winning."""
    out: dict[str, dict] = {}
    for generation in reversed(generations):
        for policy in generation["fgac_policies"]:
            out[policy["name"]] = {"to": sorted(policy.get("to_principals") or []),
                                   "except": sorted(policy.get("except_principals") or [])}
    return out


def state_problems(live: dict, active: dict | None, pending: list[dict]) -> list[str]:
    """How the data_access state differs from what the journal says is live."""
    problems = []
    expected_tags = dict((active or {}).get("tags") or {})
    for column in sorted(live["tags"].keys() | expected_tags.keys()):
        have, want = live["tags"].get(column), expected_tags.get(column)
        if have != want:
            problems.append(f"{column}: state has treatment {have or 'none'}, the journal expects {want or 'none'}")
    expected = _expected_live([g for g in [active] + pending if g])
    for name in sorted(live["policies"].keys() - expected.keys()):
        problems.append(f"policy {name} is in the state but in no recorded generation")
    for name in sorted({p["name"] for p in (active or {}).get("fgac_policies") or []} - live["policies"].keys()):
        problems.append(f"policy {name} of the active generation is missing from the state")
    for name in sorted(live["policies"].keys() & expected.keys()):
        if live["policies"][name] != expected[name]:
            problems.append(f"policy {name} has principals {live['policies'][name]}, "
                            f"the journal expects {expected[name]}")
    return problems


def _read_state(env_dir: Path) -> dict | None:
    from scripts.state_layout import state_json
    try:
        return state_json(env_dir, env_dir.name, "data_access")
    except RuntimeError as exc:
        raise Refused(str(exc)) from exc


@contextmanager
def env_lock(env_dir: Path, owner: str):
    """The env lock: reused when an enclosing make release/maintain holds it, else taken."""
    if environment_lock.held_by_ancestor(env_dir) is not None:
        yield
        return
    ok, message, nonce = environment_lock.acquire(env_dir, os.getpid(), owner)
    if not ok:
        raise Refused(message)
    var = environment_lock.nonce_var(env_dir)
    previous = os.environ.get(var)
    os.environ[var] = nonce  # what a make _ENV_LOCK holder exports to its children
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(var, None)
        else:
            os.environ[var] = previous
        environment_lock.release_lock(env_dir, os.getpid())


def _hcl_value(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return json.dumps(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, dict):
        return "{ " + ", ".join(f"{json.dumps(str(k))} = {_hcl_value(v)}" for k, v in value.items()) + " }"
    return "[" + ", ".join(_hcl_value(v) for v in value) + "]"


class Controller:
    def __init__(self, env_dir: Path, env_name: str = "", *, hook_dir: Path = HOOK_DIR,
                 verify_access_cmd: str = "", inventory_cmd: list[str] | None = None,
                 commit: str = "", environ: dict | None = None, run=subprocess.run):
        self.env_dir = env_dir.resolve()
        self.env_name = env_name or self.env_dir.name
        self.hook_dir = hook_dir
        self.verify_access_cmd = verify_access_cmd
        self.inventory_cmd = inventory_cmd or [sys.executable, str(INVENTORY)]
        self.commit = commit
        self.environ = dict(os.environ if environ is None else environ)
        self.run_cmd = run

    # -- journal -------------------------------------------------------------
    def _save(self, journal: dict) -> None:
        if journal.get("transition"):
            journal["transition"]["updated_at"] = _now()
        save_journal(self.env_dir, journal)

    def _snapshot(self, fingerprint: str) -> dict | None:
        preview = getattr(self, "_preview_snapshots", {})
        return preview[fingerprint] if fingerprint in preview else pg.load_snapshot(self.env_dir, fingerprint)

    def _adopt(self, desired: dict) -> dict:
        """A journal for an env that has none: fresh, the legacy deployment, or exactly the desired generation live."""
        from scripts import migration

        if migration.deployment(self.env_dir)["legacy"]:
            # A legacy env that opted in: its deployment is the live generation (design section 8).
            try:
                return migration.adopt(self.env_dir, self.env_name)
            except migration.Refused as exc:
                raise Refused(str(exc)) from exc
        journal = empty_journal()
        live = live_from_state(_read_state(self.env_dir))
        if not live["policies"] and not live["tags"]:
            return journal
        problems = state_problems(live, desired, [])
        if problems:
            raise Refused(
                "the data_access state holds deterministic policies or treatment tags that no policy "
                "journal records, and they differ from the generated config:\n  - " + "\n  - ".join(problems[:20])
                + f"\nRestore {journal_path(self.env_dir)} from a backup; GenieRails won't guess what is live.")
        pg.save_snapshot(self.env_dir, desired)
        journal["active"] = desired["fingerprint"]
        journal["history"].append({"adopted": desired["fingerprint"], "at": _now()})
        print(f"Policy journal: adopted the live generation {desired['fingerprint']} (matches the state).")
        return journal

    def _new_transition(self, journal: dict, desired: dict) -> dict:
        active = journal.get("active") or ""
        pending = list(journal.get("pending") or [])
        if len(pending) > 1:
            raise Refused(f"the journal lists {len(pending)} generations waiting to retire; expected at most one")
        old = self._snapshot(active)
        held = pending[0] if pending else None
        retain = self._snapshot(held["fingerprint"]) if held else None
        # A migration preview (make plan) runs before the legacy tags are moved; the adoption checks the state.
        previewed = bool(old) and old["fingerprint"] in getattr(self, "_preview_snapshots", {})
        problems = [] if previewed else state_problems(live_from_state(_read_state(self.env_dir)), old,
                                                       [retain] if retain else [])
        if problems:
            raise Refused(
                "the data_access state doesn't match the policy journal (applied outside the staged "
                "controller, or the journal is stale):\n  - " + "\n  - ".join(problems[:20])
                + "\nNothing was applied.")
        change = desired["fingerprint"] != active
        if change and held and not held["confirmed"]:
            raise Refused(
                f"generation {held['fingerprint']} is still live from the change of run {held['superseded_by']}, "
                "which no successful release has confirmed yet. Re-run that release (same commit) to "
                "completion first; a new policy change would need a fourth live generation.")
        eligible = held if held and held["confirmed"] and held["fingerprint"] not in {active, desired["fingerprint"]} \
            else None
        tier_move = change and pg.has_tier_move(desired, old, retain)
        phases = ["create"]
        if change or eligible:
            phases.append("verify-created")
        if tier_move:
            phases += ["tighten", "verify-tightened"]
        if change:
            phases += ["switch", "verify-switched"]
        if eligible:
            phases += ["retire", "verify-retired", "drop-functions"]
        # New SELECT opens last, after everything above is applied and verified.
        phases.append("grant")
        return {
            "run_id": uuid.uuid4().hex[:12], "commit": self.commit, "from": active,
            "to": desired["fingerprint"], "retain": held["fingerprint"] if held else "",
            "retire": eligible["fingerprint"] if eligible else "", "tier_move": tier_move,
            "phases": phases, "completed": [], "current": "", "status": "running",
            "failed_phase": "", "error": "", "switch_attempted": False, "restoring": False,
            "restore_failed": False, "started_at": _now(), "updated_at": _now(),
        }

    # -- running hooks ---------------------------------------------------------
    def _hook_env(self, t: dict) -> dict:
        return self.environ | {
            "GENIERAILS_ENV_DIR": str(self.env_dir), "GENIERAILS_ENV_NAME": self.env_name,
            "GENIERAILS_POLICY_FINGERPRINT": t["to"], "GENIERAILS_OLD_POLICY_FINGERPRINT": t["from"],
            "GENIERAILS_RETIRE_FINGERPRINT": t["retain"],
            "GENIERAILS_POLICY_KEEP_NEW_TAGS": "1" if t["switch_attempted"] else "",
            "GENIERAILS_POLICY_RUN_ID": t["run_id"],
        }

    def _hook(self, t: dict, stage: str) -> int:
        return self.run_cmd([str(self.hook_dir / stage)], env=self._hook_env(t), check=False).returncode

    def _inventory(self, t: dict, stages: tuple[str, ...]) -> int:
        cmd = self.inventory_cmd + ["--env-dir", str(self.env_dir), "--stage", ",".join(stages)]
        return self.run_cmd(cmd, env=self._hook_env(t), check=False).returncode

    def verification_inputs(self, t: dict, stages: tuple[str, ...]) -> tuple[Path, Path] | None:
        """The tfvars and env file verify-access checks the applied stage against.

        After create (or restore) only columns that already had a value are
        checked, against the tier settings of the generation they still carry;
        after switch, every column against the desired settings. None when no
        column is checked.
        """
        record = pg.load_stage_record(self.env_dir)
        if not record or record.get("stage") not in stages:
            raise Refused(f"expected a {'/'.join(stages)} stage to be applied, found "
                          f"{(record or {}).get('stage') or 'none'}")
        rendered = record["rendered"]
        before_switch = record["stage"] in ("create", "restore")
        basis = self._snapshot(t["from"]) if before_switch and t["from"] else self._snapshot(t["to"])
        tags = dict(rendered["tags"])
        if before_switch:
            tags = {c: v for c, v in tags.items() if c in (basis or {}).get("tags", {})}
        if not tags:
            return None
        verify_dir = pg.ledger_dir(self.env_dir) / "verify"
        verify_dir.mkdir(parents=True, exist_ok=True)
        # The applied stage's render (policy_generations.write_stage), else the split config.
        layer_tfvars = pg.stage_config_dir(self.env_dir) / "abac.auto.tfvars"
        if not layer_tfvars.is_file():
            layer_tfvars = self.env_dir / "data_access" / "abac.auto.tfvars"
        tfvars = verify_dir / f"{record['stage']}.abac.auto.tfvars"
        tfvars.write_text(layer_tfvars.read_text())
        _write_subset(tfvars, tags)
        env_cfg = pg._load(self.env_dir / "env.auto.tfvars")
        settings = (basis or {}).get("settings")
        if settings is None:
            if t["tier_move"] and before_switch:
                raise Refused(f"generation {t['from']} has no recorded tier settings, so its old "
                              "protection can't be verified across a tier move")
            settings = {}
        env_cfg.update(settings)
        env_file = verify_dir / f"{record['stage']}.env.auto.tfvars"
        env_file.write_text("".join(f"{key} = {_hcl_value(value)}\n" for key, value in env_cfg.items()))
        return tfvars, env_file

    def _verify(self, t: dict, phase: str) -> int:
        stages, with_access = VERIFY_PHASES[phase]
        rc = self._inventory(t, stages)
        if rc or not with_access:
            return rc
        inputs = self.verification_inputs(t, stages)
        if inputs is None:
            print(f"  {phase}: no column carries a value yet; the live policy check above is the verification")
            return 0
        if not self.verify_access_cmd:
            print(f"ERROR: {phase} needs a verify-access command (--verify-access-cmd)", file=sys.stderr)
            return 2
        env = self._hook_env(t) | {"GENIERAILS_VERIFY_TFVARS": str(inputs[0]),
                                   "GENIERAILS_VERIFY_ENV_FILE": str(inputs[1])}
        return self.run_cmd(["/bin/sh", "-c", self.verify_access_cmd], env=env, check=False).returncode

    def _phase(self, t: dict, phase: str) -> int:
        if phase in APPLY_PHASES:
            return self._hook(t, APPLY_PHASES[phase])
        return self._verify(t, phase)

    # -- the protocol ------------------------------------------------------------
    def _restore(self, journal: dict, t: dict) -> bool:
        """Put the previous protection back: (tighten, when principals may be moved,) then restore."""
        t["restoring"] = True
        self._save(journal)
        steps = (["tighten"] if t["switch_attempted"] and t["tier_move"] else []) + ["restore"]
        for step in steps:
            print(f"=== Policy staged apply: restoring the previous protection ({step}) ===", flush=True)
            if self._hook(t, step):
                break
        else:
            if not self._inventory(t, ("restore",)):
                t.update(restoring=False, restore_failed=False, completed=["create"])
                self._save(journal)
                print("  The previous protection is back in place and verified live.")
                return True
        t["restore_failed"] = True
        self._save(journal)
        print("ERROR: restoring the previous protection failed; every column still carries the old or the "
              "new protection (the stages never remove a live mask). Re-run the same command to finish the "
              "restore and resume.", file=sys.stderr)
        return False

    def _fail(self, journal: dict, t: dict, phase: str, rc: int) -> int:
        t.update(status="failed", failed_phase=phase, error=f"{phase} exited {rc}")
        self._save(journal)
        print(f"ERROR: policy phase {phase} failed (exit {rc}).", file=sys.stderr)
        if phase in RESTORE_ON_FAILURE:
            self._restore(journal, t)
        print(self.describe_transition(t), file=sys.stderr)
        return 1

    def _finish(self, journal: dict, t: dict) -> None:
        pending = [e for e in journal.get("pending") or [] if e["fingerprint"] not in {t["to"], t["retire"]}]
        if t["from"] and t["from"] != t["to"]:
            pending.append({"fingerprint": t["from"], "superseded_by": t["run_id"], "confirmed": False})
        journal.update(active=t["to"], pending=pending, transition=None)
        journal["history"] = (journal.get("history") or [])[-(HISTORY_LIMIT - 1):] + [{
            "run_id": t["run_id"], "commit": t["commit"], "from": t["from"], "to": t["to"],
            "retired": t["retire"], "phases": t["phases"], "finished_at": _now()}]
        self._save(journal)

    def run(self) -> int:
        if governance_mode(self.env_dir) != "deterministic":
            print("Staged policy apply: legacy governance mode; nothing is staged and nothing changes.")
            return 0
        with env_lock(self.env_dir, "staged-policy-apply"):
            desired = pg.desired_generation(self.env_dir)
            journal = load_journal(self.env_dir) or self._adopt(desired)
            t = journal.get("transition")
            if t:
                if t["to"] != desired["fingerprint"]:
                    raise Refused(self._unfinished_message(t, desired))
                print(f"=== Policy staged apply: resuming run {t['run_id']} "
                      f"({t['status']} at {t['failed_phase'] or t['current'] or 'start'}) ===")
                if t["restoring"] or t["restore_failed"]:
                    if not self._restore(journal, t):
                        return 1
            else:
                t = self._new_transition(journal, desired)
                journal["transition"] = t
                self._save(journal)
                print(f"=== Policy staged apply ({self.env_name}): {' -> '.join(t['phases'])} ===")
            for index, phase in enumerate(t["phases"]):
                if phase in t["completed"]:
                    continue
                t.update(current=phase, status="running")
                if phase in ("switch", "verify-switched"):
                    t["switch_attempted"] = True
                self._save(journal)
                print(f"=== Policy phase {index + 1}/{len(t['phases'])}: {phase} ({self.env_name}) ===", flush=True)
                rc = self._phase(t, phase)
                if rc:
                    return self._fail(journal, t, phase, rc)
                t["completed"].append(phase)
                self._save(journal)
            self._finish(journal, t)
            print(f"Staged policy apply: generation {t['to']} is live and verified"
                  + (f"; {t['from']} stays until a later release succeeds" if t["from"] and t["from"] != t["to"] else "")
                  + (f"; retired {t['retire']}" if t["retire"] else "") + ".")
            return 0

    # -- two-phase release -------------------------------------------------------
    def _transition_for(self, journal: dict, desired: dict) -> dict:
        t = journal.get("transition")
        if t and t["to"] != desired["fingerprint"]:
            raise Refused(self._unfinished_message(t, desired))
        return t or self._new_transition(journal, desired)

    def plan(self, out_dir: Path) -> int:
        """Save the plans of the next phases (make release-plan); applies nothing."""
        if governance_mode(self.env_dir) != "deterministic":
            print("Staged policy apply: legacy governance mode; nothing to plan.")
            return 0
        from scripts.policy_generations import PLAN_BASIS_ENV, PLAN_OUT_ENV, load_sidecar

        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        with env_lock(self.env_dir, "staged-policy-plan"):
            desired = pg.desired_generation(self.env_dir)
            journal = load_journal(self.env_dir)
            if journal is None and self._legacy_pending():
                return self._plan_adoption(out_dir, desired)
            journal = journal or self._adopt(desired)
            t = self._transition_for(journal, desired)
            remaining = [p for p in t["phases"] if p not in t["completed"]]
            steps: list[dict] = []
            basis = ""
            stopped = False
            for index, phase in enumerate(remaining):
                if stopped:
                    if phase in VERIFY_PHASES:
                        steps.append({"phase": phase, "kind": "verify"})
                        continue
                    break
                if phase in VERIFY_PHASES:
                    steps.append({"phase": phase, "kind": "verify"})
                    continue
                plan_file = out_dir / f"{len(steps):02d}-{phase}.tfplan"
                env = self._hook_env(t) | {PLAN_OUT_ENV: str(plan_file)} | ({PLAN_BASIS_ENV: basis} if basis else {})
                print(f"=== Policy phase {phase} ({self.env_name}): saving its plan ===", flush=True)
                rc = self.run_cmd([str(self.hook_dir / APPLY_PHASES[phase])], env=env, check=False).returncode
                if rc:
                    print(f"ERROR: planning policy phase {phase} failed (exit {rc}); nothing was applied.",
                          file=sys.stderr)
                    return 1
                side = load_sidecar(plan_file)
                steps.append({"phase": phase, "kind": "apply", "file": plan_file.name,
                              "sha256": _sha256(plan_file), "changes": bool(side.get("changes"))})
                basis = str(plan_file)
                stopped = bool(side.get("changes"))
            planned = {s["phase"] for s in steps}
            final = all(p in planned for p in remaining)
            summary = {
                "transition": {key: t[key] for key in ("from", "to", "retain", "retire", "tier_move", "phases",
                                                       "completed")} | {"resumed": bool(journal.get("transition"))},
                "steps": steps, "final": final, "planned_at": _now(),
            }
            pg.write_json(out_dir / PHASES_FILE, summary)
            print(f"Staged policy plan ({self.env_name}): "
                  + " -> ".join(f"{s['phase']}{'' if s['kind'] == 'verify' or s['changes'] else ' (no changes)'}"
                                for s in steps)
                  + ("" if final else f"; later phases ({', '.join(p for p in remaining if p not in planned)}) "
                                      "need another release-plan once these are applied"))
            return 0

    def apply_saved(self, plan_dir: Path) -> int:
        """Apply exactly the saved phases of a manifest (make release MANIFEST=...)."""
        if governance_mode(self.env_dir) != "deterministic":
            print("Staged policy apply: legacy governance mode; nothing is staged and nothing changes.")
            return 0
        from scripts.policy_generations import SAVED_PLAN_ENV

        plan_dir = Path(plan_dir)
        try:
            saved = json.loads((plan_dir / PHASES_FILE).read_text())
        except (OSError, ValueError) as exc:
            raise Refused(f"no saved policy phases in {plan_dir} ({exc})") from exc
        planned = saved["transition"]
        with env_lock(self.env_dir, "staged-policy-apply"):
            desired = pg.desired_generation(self.env_dir)
            if desired["fingerprint"] != planned["to"]:
                raise Refused(f"the generated config is generation {desired['fingerprint']}, the manifest planned "
                              f"{planned['to']}; run make release-plan again")
            if [s["kind"] for s in saved["steps"]] == ["adopt"]:
                return self._apply_adoption(saved["steps"][0])
            journal = load_journal(self.env_dir) or self._adopt(desired)
            t = self._transition_for(journal, desired)
            for key in ("from", "to", "retain", "retire", "tier_move", "phases"):
                if t[key] != planned[key]:
                    raise Refused(f"the policy journal moved since release-plan ({key}: planned {planned[key]!r}, "
                                  f"now {t[key]!r}); run make release-plan again")
            if sorted(t["completed"]) != sorted(planned["completed"]):
                raise Refused("policy phases were applied since release-plan; run make release-plan again")
            if not journal.get("transition"):
                journal["transition"] = t
                self._save(journal)
            print(f"=== Policy staged apply ({self.env_name}, saved plans): "
                  f"{' -> '.join(s['phase'] for s in saved['steps'])} ===")
            for step in saved["steps"]:
                phase = step["phase"]
                t.update(current=phase, status="running")
                if phase in ("switch", "verify-switched"):
                    t["switch_attempted"] = True
                self._save(journal)
                print(f"=== Policy phase {phase} ({self.env_name}) ===", flush=True)
                if step["kind"] == "apply":
                    env = self._hook_env(t) | {SAVED_PLAN_ENV: str(plan_dir / step["file"])}
                    rc = self.run_cmd([str(self.hook_dir / APPLY_PHASES[phase])], env=env, check=False).returncode
                else:
                    rc = self._verify(t, phase)
                if rc:
                    return self._fail(journal, t, phase, rc)
                t["completed"].append(phase)
                self._save(journal)
            remaining = [p for p in t["phases"] if p not in t["completed"]]
            if remaining:
                t.update(current="", status="waiting")
                self._save(journal)
                print(f"Staged policy apply: the saved phases are applied and verified; {', '.join(remaining)} "
                      f"remain. Run make release-plan ENV={self.env_name} again for the next phase.")
                return MORE_PHASES
            self._finish(journal, t)
            print(f"Staged policy apply: generation {t['to']} is live and verified (saved plans).")
            return 0

    # -- migration from legacy (make release-plan / release MANIFEST=) --------------
    def _legacy_pending(self) -> bool:
        from scripts import migration

        return migration.deployment(self.env_dir)["legacy"]

    def _adoption_step(self) -> dict:
        """What adopting the legacy deployment does: checked, but nothing changes."""
        from scripts import migration
        from scripts.runtime_overlay import effective

        dep = migration.deployment(self.env_dir)
        legacy = migration.legacy_generation(dep)
        try:
            migration.check(self.env_dir, self.env_name, legacy)
        except migration.Refused as exc:
            raise Refused(str(exc)) from exc
        forget = migration.forgotten_tags(dep, pg._load(effective(self.env_dir, "abac.auto.tfvars")))
        return {"phase": "adopt", "kind": "adopt", "legacy": legacy["fingerprint"],
                "moves": [list(m) for m in dep["moves"]], "forget": forget}

    def _plan_adoption(self, out_dir: Path, desired: dict) -> int:
        """release-plan of a pending migration: the state moves are this manifest's only operation.

        Nothing is moved, journaled or recorded here; release MANIFEST= runs
        exactly these moves under the env lock, and the next release-plan plans
        the create phase against the moved state.
        """
        step = self._adoption_step()
        summary = {
            "transition": {"from": step["legacy"], "to": desired["fingerprint"], "retain": "", "retire": "",
                           "tier_move": False, "phases": ["adopt"], "completed": [], "resumed": False},
            "steps": [step], "final": False, "planned_at": _now(),
        }
        pg.write_json(Path(out_dir) / PHASES_FILE, summary)
        print(f"Staged policy plan ({self.env_name}): adopt the legacy deployment {step['legacy']} "
              f"({len(step['moves'])} terraform state mv, {len(step['forget'])} state rm, nothing applied); "
              "the create phase needs another release-plan once this manifest is applied")
        for source, target in step["moves"]:
            print(f"  terraform state mv '{source}' '{target}'")
        return 0

    def _apply_adoption(self, planned: dict) -> int:
        """release MANIFEST= of an adoption manifest: exactly the approved moves, then stop."""
        from scripts import migration

        if load_journal(self.env_dir) is not None:
            raise Refused("the legacy deployment was adopted since release-plan; run make release-plan again")
        now = self._adoption_step()
        for key in ("legacy", "moves", "forget"):
            if now[key] != planned[key]:
                raise Refused(f"the data_access state changed since release-plan ({key}); nothing was moved. "
                              "Run make release-plan again")
        try:
            journal = migration.adopt(self.env_dir, self.env_name)
        except migration.Refused as exc:
            raise Refused(str(exc)) from exc
        self._save(journal)
        print(f"Staged policy apply: adopted the legacy deployment {planned['legacy']} (the approved state moves); "
              f"run make release-plan ENV={self.env_name} again for the create phase.")
        return MORE_PHASES

    def abandon(self) -> int:
        """Drop an unfinished transition that never switched, back to the active generation."""
        if governance_mode(self.env_dir) != "deterministic":
            print("Staged policy apply: legacy governance mode; nothing to abandon.")
            return 0
        with env_lock(self.env_dir, "policy-abandon"):
            journal = load_journal(self.env_dir)
            t = (journal or {}).get("transition")
            if not t:
                print("Staged policy apply: no unfinished policy change; nothing to abandon.")
                return 0
            if t["switch_attempted"]:
                raise Refused("this change already retagged columns or moved principals; it can't be "
                              "abandoned. Re-run the command that started it to resume it.")
            print(f"=== Policy staged apply: abandoning run {t['run_id']} (generation {t['to']}) ===")
            if self._hook(t, "abandon") or self._inventory(t, ("abandon",)):
                t.update(status="failed", error="abandon failed")
                self._save(journal)
                print("ERROR: abandoning failed; the journal still records the change.", file=sys.stderr)
                return 1
            journal["transition"] = None
            journal["history"] = (journal.get("history") or [])[-(HISTORY_LIMIT - 1):] + [
                {"run_id": t["run_id"], "abandoned": t["to"], "at": _now()}]
            self._save(journal)
            print(f"Abandoned generation {t['to']}; {t['from'] or 'nothing'} stays live.")
            return 0

    def confirm(self) -> int:
        """After a successful release: generations it superseded may retire on a later one."""
        if governance_mode(self.env_dir) != "deterministic":
            return 0
        journal = load_journal(self.env_dir)
        if journal is None:
            return 0
        if journal.get("transition"):
            print("ERROR: a staged policy change is unfinished; nothing confirmed.", file=sys.stderr)
            return 1
        desired = pg.desired_generation(self.env_dir)
        if journal.get("active") != desired["fingerprint"]:
            print(f"ERROR: the live generation {journal.get('active') or 'none'} is not the generated "
                  f"{desired['fingerprint']}; nothing confirmed.", file=sys.stderr)
            return 1
        changed = [e for e in journal.get("pending") or [] if not e["confirmed"]]
        for entry in changed:
            entry["confirmed"] = True
        if changed:
            self._save(journal)
            print("Policy journal: generation(s) " + ", ".join(e["fingerprint"] for e in changed)
                  + " can retire after the next successful release.")
        return 0

    def describe(self) -> int:
        """What the next apply would stage (read-only, never fails: make plan and make check call it)."""
        if governance_mode(self.env_dir) != "deterministic":
            print("Staged policy apply: legacy governance mode; applies are not staged.")
            return 0
        try:
            return self._describe()
        except (Refused, pg.StageError, OSError, ValueError, RuntimeError) as exc:
            print(f"Staged policy apply: no phase preview ({exc}); an apply would stop on this first.")
            return 0

    def _describe(self) -> int:
        desired = pg.desired_generation(self.env_dir)
        journal = load_journal(self.env_dir)
        t = (journal or {}).get("transition")
        if t:
            print(self.describe_transition(t))
            if t["to"] != desired["fingerprint"]:
                print("  The generated config has moved on: the next apply refuses until this is resumed or abandoned.")
            return 0
        try:
            plan = self._new_transition(journal or self._adopt_preview(desired), desired)
        except Refused as exc:
            print(f"Staged policy apply would refuse: {exc}")
            return 0
        print(f"Staged policy apply ({self.env_name}) would run: {' -> '.join(plan['phases'])}"
              + (" (tier move)" if plan["tier_move"] else ""))
        return 0

    def _adopt_preview(self, desired: dict) -> dict:
        from scripts import migration

        journal = empty_journal()
        dep = migration.deployment(self.env_dir)
        if dep["legacy"]:
            legacy = migration.legacy_generation(dep)
            migration.check(self.env_dir, self.env_name, legacy)
            print(f"Staged policy apply ({self.env_name}): migrates from the legacy deployment "
                  f"(generation {legacy['fingerprint']}); see make migrate-plan ENV={self.env_name}")
            self._preview_snapshots = {legacy["fingerprint"]: legacy}
            journal["active"] = legacy["fingerprint"]
            return journal
        live = live_from_state(_read_state(self.env_dir))
        if live["policies"] or live["tags"]:
            journal["active"] = desired["fingerprint"]
        return journal

    def describe_transition(self, t: dict) -> str:
        lines = [f"Unfinished policy change (run {t['run_id']}, commit {t['commit'] or '?'}): "
                 f"{t['from'] or 'nothing'} -> {t['to']}",
                 f"  phases:    {' -> '.join(t['phases'])}",
                 f"  completed: {', '.join(t['completed']) or 'none'}",
                 f"  status:    {t['status']}" + (f" at {t['failed_phase']} ({t['error']})" if t['failed_phase'] else "")]
        if t["restore_failed"]:
            lines.append("  live:      a restore of the previous protection failed; the next run finishes it first")
        elif not t["switch_attempted"]:
            lines.append(f"  live:      the previous protection ({t['from'] or 'none'}); no column was retagged")
        else:
            lines.append(f"  live:      the previous protection ({t['from'] or 'none'}), restored")
        return "\n".join(lines)

    def _unfinished_message(self, t: dict, desired: dict) -> str:
        options = [f"re-run the same command at commit {t['commit'] or '?'} (generation {t['to']}) to resume it"]
        if not t["switch_attempted"]:
            options.append(f"or drop it with make policy-abandon ENV={self.env_name}")
        return (self.describe_transition(t) + f"\nThe generated config is now generation {desired['fingerprint']}. "
                "A new policy change can't start until this one is finished: " + "; ".join(options) + ".")


def _write_subset(tfvars: Path, tags: dict) -> None:
    """Rewrite ``tfvars`` (a copy of the layer config) with only ``tags`` as treatment tags."""
    from generate_abac import _find_bracket_section, _render_tag_assignment_block, _replace_bracket_section

    text = tfvars.read_text()
    cfg = pg._load(tfvars)
    assignments = [
        item for item in cfg.get("tag_assignments") or []
        if not (item.get("entity_type") == "columns" and item.get("tag_key") == pg.TAG_KEY)
    ] + [{"entity_type": "columns", "entity_name": c, "tag_key": pg.TAG_KEY, "tag_value": v}
         for c, v in sorted(tags.items())]
    if _find_bracket_section(text, "tag_assignments") is None:
        text = text.rstrip() + "\n\ntag_assignments = [\n]\n"
    tfvars.write_text(_replace_bracket_section(text, "tag_assignments",
                                               [_render_tag_assignment_block(a) for a in assignments]))


def _sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _git_head() -> str:
    out = subprocess.run(["git", "-C", str(SHARED), "rev-parse", "HEAD"], text=True, capture_output=True, check=False)
    return out.stdout.strip() if out.returncode == 0 else ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("run", "confirm", "abandon", "status", "describe", "plan",
                                            "apply-saved"))
    parser.add_argument("--env-dir", type=Path, required=True)
    parser.add_argument("--env-name", default="")
    parser.add_argument("--hook-dir", type=Path, default=HOOK_DIR)
    parser.add_argument("--verify-access-cmd", default="",
                        help="shell command verifying every tier's output; gets GENIERAILS_VERIFY_TFVARS "
                             "and GENIERAILS_VERIFY_ENV_FILE")
    parser.add_argument("--inventory-cmd", default="", help="override the live policy check (argv, shell-split)")
    parser.add_argument("--commit", default="")
    parser.add_argument("--out-dir", type=Path, help="plan: where the saved phase plans go")
    parser.add_argument("--plan-dir", type=Path, help="apply-saved: the saved phase plans to apply")
    args = parser.parse_args(argv)
    controller = Controller(
        args.env_dir, args.env_name, hook_dir=args.hook_dir, verify_access_cmd=args.verify_access_cmd,
        inventory_cmd=shlex.split(args.inventory_cmd) if args.inventory_cmd else None,
        commit=args.commit or _git_head(),
    )
    try:
        if args.command == "status":
            journal = load_journal(controller.env_dir)
            if governance_mode(controller.env_dir) != "deterministic":
                print("legacy governance mode: applies are not staged")
            elif journal is None:
                print("no policy journal yet (no staged apply has run)")
            else:
                print(f"active generation:  {journal['active'] or 'none'}")
                for entry in journal.get("pending") or []:
                    print(f"waiting to retire:  {entry['fingerprint']} (superseded by run {entry['superseded_by']}, "
                          f"{'confirmed' if entry['confirmed'] else 'not yet confirmed'})")
                if journal.get("transition"):
                    print(controller.describe_transition(journal["transition"]))
            return 0
        if args.command == "plan":
            if not args.out_dir:
                parser.error("plan needs --out-dir")
            return controller.plan(args.out_dir)
        if args.command == "apply-saved":
            if not args.plan_dir:
                parser.error("apply-saved needs --plan-dir")
            return controller.apply_saved(args.plan_dir)
        return getattr(controller, args.command)()
    except Refused as exc:
        print(f"ERROR: staged policy apply refused: {exc}", file=sys.stderr)
        return 3
    except (pg.StageError, OSError, ValueError) as exc:
        print(f"ERROR: staged policy apply: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
