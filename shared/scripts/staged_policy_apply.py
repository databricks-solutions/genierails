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
   applied and verified. verify-created and verify-switched therefore run
   verify-access with ``GENIERAILS_VERIFY_PRE_GRANT=1``: each temporary test
   principal itself (never a tier group) gets USE/SELECT on the tables it
   checks, revoked when verify-access exits, so a tier's masks are checked
   for real before its group can read anything.

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
on (pointing at abandon); ``abandon`` drops one that never switched, or one
whose switch was rolled back (the journal's ``restored`` marker) when a fresh
live check shows every column and principal back on the previous generation,
also after the config moved on (class.* or treatment-setting drift: it keeps
the previous generation's recorded values, once its snapshot re-hashes to its
fingerprint). Abandon saves a
refreshed plan that may only delete the change's policies, checks live after
it, applies exactly that plan (hash checked) and checks live again, all under
the env lock. ``rebind`` (make policy-rebind) is the step before abandon when
principals of a rolled-back change were deleted and recreated: it re-applies
the restore render with the generated config's principals, protection first
(policies and functions, checked live), then the grants the deleted
principals lost (checked live and with verify-access), each from a saved plan.
Legacy envs are never staged: the controller exits 0 and touches nothing.

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
AGENT_COVERAGE = SHARED / "scripts" / "agent_coverage.py"
APPLY_PHASES = {"create": "create", "tighten": "tighten", "switch": "switch", "retire": "retire",
                "drop-functions": "drop-functions", "grant": "grant"}
# verify phase -> (stages the last applied hook may be, run verify-access too)
VERIFY_PHASES = {
    "verify-created": (("create", "restore"), True),
    "verify-tightened": (("tighten",), False),
    "verify-switched": (("switch",), True),
    "verify-retired": (("retire",), False),
    "verify-rebound": (("rebind",), True),
}
# A failure here may leave columns on the new value or principals moved, so
# the previous protection is restored before the run stops.
RESTORE_ON_FAILURE = {"tighten", "verify-tightened", "switch", "verify-switched"}
# Verify phases that run before the grant phase: the tier groups may not hold
# USE/SELECT yet, so verify-access grants its own test principals temporary
# access to the checked tables (GENIERAILS_VERIFY_PRE_GRANT). verify-rebound
# runs after rebind-access re-created the lost grants and checks those.
PRE_GRANT_VERIFY = {"verify-created", "verify-switched"}
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
    sql_hash = ""
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
                                 "except": sorted(attrs.get("except_principals") or []),
                                 "binding": _state_policy_binding(attrs)}
            elif resource.get("type") == "databricks_entity_tag_assignment" and resource.get("name") == "treatment":
                tags[str(attrs.get("entity_name") or key.split("|")[1])] = str(attrs.get("tag_value") or "")
            elif resource.get("type") == "terraform_data" and resource.get("name") == "masking_functions":
                sql_hash = _trigger_value(attrs, "sql_hash") or sql_hash
    return {"policies": policies, "tags": tags, "sql_hash": sql_hash}


def _trigger_value(attrs: dict, key: str) -> str:
    for field in ("triggers_replace", "triggers", "input"):
        value = attrs.get(field)
        if isinstance(value, dict) and value.get(key):
            return str(value[key])
    return ""


def _state_policy_binding(attrs: dict) -> dict:
    conditions = [m.get("condition") for m in attrs.get("match_columns") or [] if isinstance(m, dict)]
    return {
        "column_mask": attrs.get("column_mask") or {},
        "match_condition": str(attrs.get("match_condition") or next((c for c in conditions if c), "")),
        "row_filter": attrs.get("row_filter") or None,
        "when_condition": str(attrs.get("when_condition") or ""),
        "on_securable_fullname": str(attrs.get("on_securable_fullname") or attrs.get("catalog") or ""),
        "on_securable_type": str(attrs.get("on_securable_type") or attrs.get("securable_type") or ""),
    }


def _desired_policy_binding(policy: dict) -> dict:
    return {
        "column_mask": {"function_name": ".".join(
            str(policy.get(k) or "") for k in ("function_catalog", "function_schema", "function_name")
        )},
        "match_condition": str(policy.get("match_condition") or ""),
        "row_filter": policy.get("row_filter") or None,
        "when_condition": str(policy.get("when_condition") or ""),
        "on_securable_fullname": str(policy.get("on_securable_fullname") or policy.get("catalog") or ""),
        "on_securable_type": str(policy.get("on_securable_type") or ""),
    }


def _normal_binding(binding: dict) -> dict:
    out = {k: v for k, v in binding.items() if v not in ("", None, [], {})}
    if isinstance(out.get("column_mask"), dict):
        mask = {k: v for k, v in out["column_mask"].items() if v not in ("", None, [], {})}
        if mask.get("function_name"):
            mask["function_name"] = str(mask["function_name"]).lower()
        out["column_mask"] = mask
    return out


def _with_state_principals(policy: dict, principals: dict) -> dict:
    return dict(policy) | {
        "to_principals": list(principals.get("to") or []),
        "except_principals": list(principals.get("except") or []),
    }


def _principal_only_adoption(env_dir: Path, live: dict, desired: dict) -> tuple[dict | None, list[str]]:
    """A synthetic current generation when state differs from desired only by principals."""
    problems: list[str] = []
    desired_by_name = {p["name"]: p for p in desired.get("fgac_policies") or []}
    live_names = set(live["policies"])
    desired_names = set(desired_by_name)
    for name in sorted(live_names - desired_names):
        problems.append(f"policy {name} is in the state but not in the generated config")
    for name in sorted(desired_names - live_names):
        problems.append(f"policy {name} is in the generated config but missing from the state")
    for column in sorted(set(live["tags"]) | set(desired.get("tags") or {})):
        have, want = live["tags"].get(column), desired.get("tags", {}).get(column)
        if have != want:
            problems.append(f"{column}: state has treatment {have or 'none'}, generated config has {want or 'none'}")
    if live.get("sql_hash"):
        desired_hash = pg.masking_sql_hash(desired.get("functions_sql") or "")
        if live["sql_hash"] != desired_hash:
            problems.append(f"masking function SQL digest differs (state {live['sql_hash']}, generated {desired_hash})")
    changed_principals = False
    adopted_policies = []
    for name in sorted(live_names & desired_names):
        live_policy = live["policies"][name]
        desired_policy = desired_by_name[name]
        live_binding = _normal_binding(live_policy.get("binding") or {})
        desired_binding = _normal_binding(_desired_policy_binding(desired_policy))
        for key in sorted(set(live_binding) | set(desired_binding)):
            if live_binding.get(key) != desired_binding.get(key):
                problems.append(f"policy {name} changes its {key}")
        want = {"to": sorted(desired_policy.get("to_principals") or []),
                "except": sorted(desired_policy.get("except_principals") or [])}
        changed_principals = changed_principals or live_policy["to"] != want["to"] \
            or live_policy["except"] != want["except"]
        adopted_policies.append(_with_state_principals(desired_policy, live_policy))
    if problems:
        return None, problems
    if not changed_principals:
        return None, []
    adopted = dict(desired) | {"fgac_policies": adopted_policies}
    # Recompute so the journal's active generation really names the live principals.
    cfg = {"fgac_policies": adopted_policies, "tag_assignments": [
        {"entity_type": "columns", "entity_name": column, "tag_key": pg.TAG_KEY, "tag_value": value}
        for column, value in sorted((desired.get("tags") or {}).items())
    ]}
    from governance_policies import model_fingerprint
    adopted["fingerprint"] = model_fingerprint(cfg, adopted.get("functions_sql") or "")
    return adopted, []


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
        live_principals = {k: live["policies"][name][k] for k in ("to", "except")}
        if live_principals != expected[name]:
            problems.append(f"policy {name} has principals {live_principals}, "
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
                 agent_coverage_cmd: list[str] | None = None,
                 commit: str = "", environ: dict | None = None, run=subprocess.run,
                 verify_spec: str = "", verify_key: str = "", account_tfvars: Path | None = None):
        self.env_dir = env_dir.resolve()
        self.env_name = env_name or self.env_dir.name
        self.hook_dir = hook_dir
        self.verify_access_cmd = verify_access_cmd
        self.inventory_cmd = inventory_cmd or [sys.executable, str(INVENTORY)]
        self.agent_coverage_cmd = agent_coverage_cmd or [sys.executable, str(AGENT_COVERAGE), "check-masks"]
        self.commit = commit
        self.environ = dict(os.environ if environ is None else environ)
        self.run_cmd = run
        # The VERIFY_SPEC every verify-access below runs, its key and account config.
        self.verify_spec = verify_spec
        self.verify_key = verify_key
        self.account_tfvars = account_tfvars

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
            adopted, adoption_problems = _principal_only_adoption(self.env_dir, live, desired)
            if adopted:
                pg.save_snapshot(self.env_dir, adopted)
                journal["active"] = adopted["fingerprint"]
                journal["history"].append({
                    "adopted": adopted["fingerprint"], "at": _now(),
                    "reason": "principal-only drift adopted from state",
                    "to": desired["fingerprint"],
                })
                print(f"Policy journal: adopted live principal-only drift {adopted['fingerprint']} "
                      f"from state; generated config remains {desired['fingerprint']}.")
                return journal
            raise Refused(
                "the data_access state holds deterministic policies or treatment tags that no policy "
                "journal records, and they differ from the generated config:\n  - " + "\n  - ".join(problems[:20])
                + ("\nThis is not principal-only drift: " + "; ".join(adoption_problems[:5])
                   if adoption_problems else "\nGenieRails won't guess what is live."))
        pg.save_snapshot(self.env_dir, desired)
        journal["active"] = desired["fingerprint"]
        journal["history"].append({"adopted": desired["fingerprint"], "at": _now()})
        print(f"Policy journal: adopted the live generation {desired['fingerprint']} (matches the state).")
        return journal

    def _adopt_principal_drift(self, journal: dict, desired: dict) -> dict:
        """If state drifted only in principals, record that live state as the current generation."""
        if journal.get("transition"):
            return journal
        active = journal.get("active") or ""
        pending = list(journal.get("pending") or [])
        if pending:
            return journal
        old = self._snapshot(active)
        if active and old is None:
            return journal
        live = live_from_state(_read_state(self.env_dir))
        problems = state_problems(live, old, [])
        if not problems:
            return journal
        adopted, adoption_problems = _principal_only_adoption(self.env_dir, live, desired)
        if not adopted:
            if adoption_problems:
                raise Refused(
                    "the data_access state doesn't match the policy journal, and it is not principal-only "
                    "drift that GenieRails can adopt:\n  - " + "\n  - ".join(adoption_problems[:20])
                    + "\nNothing was applied.")
            return journal
        pg.save_snapshot(self.env_dir, adopted)
        journal["active"] = adopted["fingerprint"]
        journal["pending"] = []
        journal["history"] = (journal.get("history") or [])[-(HISTORY_LIMIT - 1):] + [{
            "adopted": adopted["fingerprint"], "at": _now(),
            "reason": "principal-only drift adopted from state",
            "from": active, "to": desired["fingerprint"],
        }]
        self._save(journal)
        print(f"Policy journal: adopted live principal-only drift {adopted['fingerprint']} "
              f"from state; generated config remains {desired['fingerprint']}.")
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
            "restore_failed": False, "restored": None, "started_at": _now(), "updated_at": _now(),
        }

    # -- running hooks ---------------------------------------------------------
    def _hook_env(self, t: dict) -> dict:
        # The env lock's nonce (env_lock exports it once taken): the hooks' Terraform calls derive
        # the token terraform_layer.sh needs for policy_stage from it (environment_lock.stage_token).
        var = environment_lock.nonce_var(self.env_dir)
        return self.environ | ({var: os.environ[var]} if os.environ.get(var) else {}) | {
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
        verify_dir = pg.ledger_dir(self.env_dir) / "verify"
        # The applied stage's render (policy_generations.write_stage), else the split config.
        layer_tfvars = pg.stage_config_dir(self.env_dir) / "abac.auto.tfvars"
        if not layer_tfvars.is_file():
            layer_tfvars = self.env_dir / "data_access" / "abac.auto.tfvars"
        return self._verification_files(t, record["stage"], record["rendered"], layer_tfvars, verify_dir)

    def _verification_files(self, t: dict, stage: str, rendered: dict, layer_tfvars: Path,
                            verify_dir: Path) -> tuple[Path, Path] | None:
        before_switch = stage in ("create", "restore", "rebind")
        basis = self._snapshot(t["from"]) if before_switch and t["from"] else self._snapshot(t["to"])
        tags = dict(rendered["tags"])
        if before_switch:
            tags = {c: v for c, v in tags.items() if c in (basis or {}).get("tags", {})}
        if not tags:
            return None
        verify_dir.mkdir(parents=True, exist_ok=True)
        tfvars = verify_dir / f"{stage}.abac.auto.tfvars"
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
        env_file = verify_dir / f"{stage}.env.auto.tfvars"
        env_file.write_text("".join(f"{key} = {_hcl_value(value)}\n" for key, value in env_cfg.items()))
        return tfvars, env_file

    def refuse_weaker_spec(self, t: dict) -> None:
        """Refuse, before create is applied, a VERIFY_SPEC verify-created would refuse.

        verify-created holds the spec to the generation the columns still carry
        (its tiers and functions), not the desired one make checked it against,
        so a spec written for the desired generation can fail there only after
        create applied. Its inputs come from the same render the create hook
        applies (policy_generations.render_stage).
        """
        if not self.verify_spec.strip() or "verify-created" not in t["phases"] or "create" in t["completed"]:
            return
        import tempfile

        from scripts.release_helpers import require_mask_proof
        from scripts.resolve_env_config import resolve_verify_key

        desired = pg.desired_generation(self.env_dir)
        old = self._snapshot(t["from"]) if t["from"] else None
        retire = self._snapshot(t["retain"]) if t["retain"] not in {"", t["to"], t["from"]} else None
        rendered = pg.render_stage("create", desired, old, retire)
        with tempfile.TemporaryDirectory() as tmp:
            layer = pg.write_stage(self.env_dir / "data_access", rendered, out_dir=Path(tmp) / "create")
            inputs = self._verification_files(t, "create", rendered, layer / "abac.auto.tfvars", Path(tmp))
            if inputs is None:
                return
            if require_mask_proof(self.env_dir, self.env_name, resolve_verify_key(self.env_dir, self.verify_key),
                                  self.verify_spec, self.account_tfvars, tfvars=inputs[0], env_file=inputs[1]):
                raise Refused(f"verify-created would refuse VERIFY_SPEC against generation {t['from']}, which "
                              "the columns carry until switch (above); omit VERIFY_SPEC for this change")

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
                                   "GENIERAILS_VERIFY_ENV_FILE": str(inputs[1]),
                                   "GENIERAILS_VERIFY_PRE_GRANT": "1" if phase in PRE_GRANT_VERIFY else ""}
        return self.run_cmd(["/bin/sh", "-c", self.verify_access_cmd], env=env, check=False).returncode

    def _phase(self, t: dict, phase: str) -> int:
        if phase in APPLY_PHASES:
            return self._hook(t, APPLY_PHASES[phase])
        return self._verify(t, phase)

    # -- the protocol ------------------------------------------------------------
    def _restore(self, journal: dict, t: dict) -> bool:
        """Put the previous protection back: (tighten, when principals may be moved,) then restore."""
        t.update(restoring=True, restored=None)
        self._save(journal)
        steps = (["tighten"] if t["switch_attempted"] and t["tier_move"] else []) + ["restore"]
        for step in steps:
            print(f"=== Policy staged apply: restoring the previous protection ({step}) ===", flush=True)
            if self._hook(t, step):
                break
        else:
            if not self._inventory(t, ("restore",)):
                record = pg.load_stage_record(self.env_dir) or {}
                rendered = record.get("rendered") or {}
                # What the live check proved, so abandon can tell a rolled-back switch from one in flight.
                restored = {"at": _now(), "after": t["failed_phase"], "generation": t["from"],
                            "tags": dict(rendered.get("tags") or {}),
                            "policies": sorted(p["name"] for p in rendered.get("fgac_policies") or [])}
                t.update(restoring=False, restore_failed=False, completed=["create"], restored=restored)
                self._save(journal)
                kept = self._kept_new_columns(t)
                if not t["from"]:
                    print(f"  A first deploy has no previous protection to go back to: the restore keeps "
                          f"generation {t['to']}'s masks and treatment tags on {len(kept)} column(s), verified "
                          "live (fail-closed). No business USE or SELECT was granted.")
                elif kept:
                    print(f"  The previous protection is back in place and verified live; {len(kept)} newly "
                          f"governed column(s) keep generation {t['to']}'s masks (fail-closed).")
                else:
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
        restored = phase in RESTORE_ON_FAILURE and self._restore(journal, t)
        print(self.describe_transition(t), file=sys.stderr)
        if restored:
            try:
                abandon = self._may_abandon(t, pg.desired_generation(self.env_dir))
            except (pg.StageError, OSError, ValueError):
                abandon = False
            print(f"Next: fix what stopped it (above), then re-run the same command: it resumes run {t['run_id']} "
                  f"after create ({_remaining(t)})."
                  + (f" To drop the change instead, run make policy-abandon ENV={self.env_name}." if abandon else ""),
                  file=sys.stderr)
        if phase == "create" and not t["switch_attempted"]:
            try:
                abandon = self._may_abandon(t, pg.desired_generation(self.env_dir))
            except (pg.StageError, OSError, ValueError):
                abandon = False
            print(f"Next: fix what stopped it (above), then re-run the same command: it resumes run {t['run_id']} "
                  "at create." + (f" To drop the change instead, run make policy-abandon ENV={self.env_name}."
                                  if abandon else ""), file=sys.stderr)
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
            journal = self._adopt_principal_drift(journal, desired)
            t = journal.get("transition")
            if t:
                if t["to"] != desired["fingerprint"]:
                    raise Refused(self._unfinished_message(t, desired))
                print(f"=== Policy staged apply: resuming run {t['run_id']} "
                      f"({t['status']} at {t['failed_phase'] or t['current'] or 'start'}) ===")
                if t["restoring"] or t["restore_failed"]:
                    if not self._restore(journal, t):
                        return 1
                self.refuse_weaker_spec(t)
            else:
                t = self._new_transition(journal, desired)
                self.refuse_weaker_spec(t)
                journal["transition"] = t
                self._save(journal)
                print(f"=== Policy staged apply ({self.env_name}): {' -> '.join(t['phases'])} ===")
            for index, phase in enumerate(t["phases"]):
                if phase in t["completed"]:
                    continue
                t.update(current=phase, status="running")
                if phase in ("switch", "verify-switched"):
                    t.update(switch_attempted=True, restored=None)
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
            journal = self._adopt_principal_drift(journal, desired)
            t = self._transition_for(journal, desired)
            self.refuse_weaker_spec(t)
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
                with _owner_only(plan_file):
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
                unfinished = (load_journal(self.env_dir) or {}).get("transition")
                if unfinished and unfinished["to"] != desired["fingerprint"]:
                    raise Refused(self._unfinished_message(unfinished, desired))
                raise Refused(f"the generated config is generation {desired['fingerprint']}, the manifest planned "
                              f"{planned['to']}; run make release-plan again")
            if [s["kind"] for s in saved["steps"]] == ["adopt"]:
                return self._apply_adoption(saved["steps"][0])
            journal = load_journal(self.env_dir) or self._adopt(desired)
            journal = self._adopt_principal_drift(journal, desired)
            t = self._transition_for(journal, desired)
            for key in ("from", "to", "retain", "retire", "tier_move", "phases"):
                if t[key] != planned[key]:
                    raise Refused(f"the policy journal moved since release-plan ({key}: planned {planned[key]!r}, "
                                  f"now {t[key]!r}); run make release-plan again")
            if sorted(t["completed"]) != sorted(planned["completed"]):
                raise Refused("policy phases were applied since release-plan; run make release-plan again")
            self.refuse_weaker_spec(t)
            if not journal.get("transition"):
                journal["transition"] = t
                self._save(journal)
            print(f"=== Policy staged apply ({self.env_name}, saved plans): "
                  f"{' -> '.join(s['phase'] for s in saved['steps'])} ===")
            for step in saved["steps"]:
                phase = step["phase"]
                t.update(current=phase, status="running")
                if phase in ("switch", "verify-switched"):
                    t.update(switch_attempted=True, restored=None)
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

    def _restore_unproven(self, t: dict, stages: tuple[str, ...] = ("restore", "rebind", "abandon")) -> str:
        """Why a switched transition can't be abandoned ('' when, short of the live check, it can).

        Only a switch that was rolled back qualifies: the journal records a
        verified restore, the last applied stage is that restore (or a partly
        done abandon) of this transition, and it puts every column on exactly
        the previous generation's value and every retained policy on its
        previous principals. A rebind of that restore (make policy-rebind) is
        still that restore. abandon then checks Unity Catalog still matches it
        once the abandon plan is saved, so it ends where it would for a change
        that never switched.

        A journal written before the marker existed has no ``restored`` key;
        there only a successful, verified restore leaves completed == ["create"]
        after a failed switch with no restore in progress or failed, so that
        exact shape stands in for the marker. Every other check still runs.
        """
        if t["restoring"] or t["restore_failed"]:
            return "no verified restore of the previous protection is recorded"
        if not t.get("restored"):
            if not _pre_marker_restored(t):
                return "no verified restore of the previous protection is recorded"
        record = pg.load_stage_record(self.env_dir)
        if not record or record.get("stage") not in stages \
                or record.get("desired") != t["to"] or record.get("old") != t["from"]:
            return (f"the last applied stage is {(record or {}).get('stage') or 'none'}, not the restore "
                    "of this change")
        old = self._snapshot(t["from"]) if t["from"] else None
        retain = self._snapshot(t["retain"]) if t["retain"] else None
        if (t["from"] and old is None) or (t["retain"] and retain is None):
            return "the previous generation's snapshot is missing, so what it protects can't be checked"
        rendered = record["rendered"]
        old_tags = dict((old or {}).get("tags") or {})
        if dict(rendered["tags"]) != old_tags:
            moved = sorted(c for c in rendered["tags"].keys() | old_tags.keys()
                           if rendered["tags"].get(c) != old_tags.get(c))
            return f"{len(moved)} column(s) {KEPT_NEW_COLUMNS} ({', '.join(moved[:5])})"
        previous = _expected_live([g for g in (old, retain) if g])
        for policy in rendered["fgac_policies"]:
            want = previous.get(policy["name"])
            have = {"to": sorted(policy.get("to_principals") or []),
                    "except": sorted(policy.get("except_principals") or [])}
            if want is not None and have != want:
                return f"policy {policy['name']} has principals {have}, the previous generation {want}"
        return ""

    def _created_policies(self, t: dict) -> set[str]:
        """The policies only the unfinished generation has: all that abandon may delete."""
        generations = {}
        for fingerprint in (t["to"], t["from"], t["retain"]):
            if fingerprint:
                generations[fingerprint] = self._snapshot(fingerprint)
                if generations[fingerprint] is None:
                    raise Refused(f"the snapshot of generation {fingerprint} is missing, so the policies the "
                                  "unfinished change created can't be told apart; nothing was applied")
        kept = {p["name"] for fp in (t["from"], t["retain"]) if fp for p in generations[fp]["fgac_policies"]}
        return {p["name"] for p in generations[t["to"]]["fgac_policies"]} - kept

    def _abandon_refusal(self, t: dict, desired: dict) -> str:
        """Why abandon would refuse ``t`` before making its plan ('' when it goes on to plan).

        abandon raises exactly this, and the abandon hints (_may_abandon) offer
        it only when this is ''. What abandon checks after the plan is saved, the
        live check that Unity Catalog still matches the restore, isn't predicted:
        it reads Unity Catalog, and it refuses only if live governance was
        changed outside GenieRails since the verified restore.
        """
        if t.get("rebind_incomplete"):
            return (f"make policy-rebind of run {t['run_id']} didn't finish "
                    f"({t['rebind_incomplete']['reason']}). {t['rebind_incomplete']['next']}")
        split = self._split_drift_message("policy-abandon")
        if split:
            return split
        if self._moved_treatments(t, desired):
            # Abandon keeps the previous generation's recorded values whatever the config says now, so
            # after drift that record must still be the one the change started from.
            problem = self._previous_snapshot_problem(t)
            if problem:
                return (f"this change can't be abandoned: the generated config's treatment values differ from the "
                        f"unfinished change {t['to']}, and {problem}, so what abandon would keep live can't be "
                        "checked. Nothing was applied. Next: restore the snapshot from where the env's records are "
                        f"kept (git history or GENIERAILS_STATE_DIR), then re-run make policy-abandon "
                        f"ENV={self.env_name}.")
        if t["switch_attempted"]:
            reason = self._restore_unproven(t)
            if KEPT_NEW_COLUMNS in reason:
                return self._kept_columns_message(t, "abandoned", reason)
            if reason:
                return _unabandonable(reason)
        try:
            self._created_policies(t)
        except Refused as exc:
            return str(exc)
        return ""

    def _nothing_applied(self, t: dict) -> bool:
        """A first deploy whose data_access state holds no deterministic policy or treatment tag.

        No generation was live before it (``from`` is empty) and it never
        switched, so nothing it applied could protect or expose a column.
        """
        if t["from"] or t["retain"] or t["switch_attempted"]:
            return False
        live = live_from_state(_read_state(self.env_dir))
        return not live["policies"] and not live["tags"]

    def _may_abandon(self, t: dict, desired: dict) -> bool:
        """Whether make policy-abandon would take ``t`` up, for the hints that offer it.

        Conservative: a check that can't run (a snapshot or record unreadable)
        counts as a refusal, so the hint falls back to resume only.
        """
        try:
            return not self._abandon_refusal(t, desired)
        except (Refused, pg.StageError, OSError, ValueError, KeyError, TypeError):
            return False

    def abandon(self) -> int:
        """Drop an unfinished transition that never switched, or whose switch was rolled back and still is."""
        if governance_mode(self.env_dir) != "deterministic":
            print("Staged policy apply: legacy governance mode; nothing to abandon.")
            return 0
        with env_lock(self.env_dir, "policy-abandon"):
            journal = load_journal(self.env_dir)
            t = (journal or {}).get("transition")
            if not t:
                print("Staged policy apply: no unfinished policy change; nothing to abandon.")
                return 0
            desired = pg.desired_generation(self.env_dir)
            refusal = self._abandon_refusal(t, desired)
            if refusal:
                raise Refused(refusal)
            switched = t["switch_attempted"]
            if switched and "restored" not in t:
                print("  The journal predates the restore marker; treating the recorded failed switch as restored "
                      "after a fresh live check.")
            moved = self._moved_treatments(t, desired)
            created = self._created_policies(t)
            print(f"=== Policy staged apply: abandoning run {t['run_id']} (generation {t['to']}) ===")
            if self._nothing_applied(t):
                # A first deploy that stopped before any policy or treatment tag was applied: there is
                # nothing to delete, and an abandon plan against the empty state would only create.
                journal["transition"] = None
                journal["history"] = (journal.get("history") or [])[-(HISTORY_LIMIT - 1):] + [
                    {"run_id": t["run_id"], "abandoned": t["to"], "at": _now(), "applied": False}]
                self._save(journal)
                print(f"Abandoned generation {t['to']}: run {t['run_id']} applied no policy or treatment tag "
                      "(the data_access state holds none), so nothing was planned or applied; nothing is live.")
                return 0
            # 1. A refreshed, saved plan of the abandon render; the hook refuses any change but
            #    deleting this change's policies (policy_generations.plan_violations).
            plan_dir = pg.ledger_dir(self.env_dir) / "abandon"
            plan_dir.mkdir(parents=True, exist_ok=True)
            os.chmod(plan_dir, 0o700)
            plan_file = plan_dir / f"{t['run_id']}.tfplan"
            for stale in (plan_file, pg.sidecar_path(plan_file)):
                stale.unlink(missing_ok=True)
            env = self._hook_env(t) | {pg.PLAN_OUT_ENV: str(plan_file)}
            with _owner_only(plan_file):
                rc = self.run_cmd([str(self.hook_dir / "abandon")], env=env, check=False).returncode
            if rc:
                _drop_plans(plan_file)  # refused: it can never be applied
                raise Refused("the abandon plan was refused or couldn't be made (see above); nothing was applied")
            planned = _sha256(plan_file)
            side = pg.load_sidecar(plan_file)
            if (side.get("stage"), side.get("desired"), side.get("old")) != ("abandon", t["to"], t["from"]):
                raise Refused(f"the saved abandon plan {plan_file} isn't of this change; nothing was applied")
            foreign = sorted(set(side.get("policy_deletes") or []) - created)
            if foreign:
                raise Refused(f"the abandon plan deletes policies the unfinished change didn't create "
                              f"({', '.join(foreign)}); nothing was applied")
            # 2. Live protection, after the plan: still the restore that plan was made against.
            if switched:
                rc = self._inventory(t, (pg.load_stage_record(self.env_dir)["stage"],))
                if rc:
                    raise Refused(_unabandonable(
                        "the live check found Unity Catalog differs from the restored protection" if rc == 1
                        else f"the live check couldn't run (exit {rc}), so the restore can't be confirmed"))
                print(f"  The switch of run {t['run_id']} was rolled back after {t['failed_phase']} and "
                      f"the live check confirms the previous protection ({t['from'] or 'none'}).")
            # 3. Exactly the saved plan (release's saved-plan pattern: the hash recorded when it was made).
            if _sha256(plan_file) != planned:
                raise Refused(f"the saved abandon plan {plan_file} changed after it was made; nothing was applied")
            env = self._hook_env(t) | {pg.SAVED_PLAN_ENV: str(plan_file)}
            if self.run_cmd([str(self.hook_dir / "abandon")], env=env, check=False).returncode:
                t.update(status="failed", error="abandon apply failed")
                self._save(journal)
                print("ERROR: applying the abandon plan failed; the journal still records the change.",
                      file=sys.stderr)
                return 1
            # 4. Verified live, or the change stays recorded.
            if self._inventory(t, ("abandon",)):
                t.update(status="failed", error="abandon applied, but the live check after it failed",
                         abandon_incomplete=True)
                self._save(journal)
                print(f"ERROR: ABANDON INCOMPLETE. The abandon plan was applied, but live governance doesn't match "
                      f"the previous protection ({t['from'] or 'none'}): see the live check above. Run "
                      f"{t['run_id']} is still recorded as unfinished, not abandoned. Next: put back what the "
                      "check lists (a column retagged outside GenieRails goes back to its previous gr_treatment "
                      f"value), then re-run make policy-abandon ENV={self.env_name}.", file=sys.stderr)
                return 1
            journal["transition"] = None
            journal["history"] = (journal.get("history") or [])[-(HISTORY_LIMIT - 1):] + [
                {"run_id": t["run_id"], "abandoned": t["to"], "at": _now()}]
            self._save(journal)
            # The journal records the abandon first; a failed abandon keeps its plan.
            _drop_plans(plan_file, *(pg.ledger_dir(self.env_dir) / "rebind").glob(f"{t['run_id']}-*.tfplan"))
            print(f"Abandoned generation {t['to']}; {t['from'] or 'nothing'} stays live.")
            if moved:
                print(f"  {len(moved)} column(s) have another treatment in the generated config than the abandoned "
                      f"change ({', '.join(moved[:10])}); the next apply stages that as a new change.")
            return 0

    # -- re-pointing a rolled-back change (make policy-rebind) ----------------------
    def rebind(self) -> int:
        """Re-point a rolled-back change at recreated principals: protection first, then the lost grants.

        Deleting a principal leaves the policies naming it as ``unknown`` and
        drops its grants, and abandon only deletes the change's policies, so
        it refuses. Rebind re-applies the restore render with the generated
        config's principals (which may differ from the change only there,
        policy_generations.rebind_problems), in two saved plans, under the env
        lock:

        1. ``rebind-protect``: the policies' principals and the function
           redeploy; every grant the plan would create is held back. Then the
           live check (the restore's policies and treatment values, with the
           new principals) and the live agent coverage check (every
           classified column of each agent's tables has a GenieRails mask).
        2. ``rebind-access``: the grants, after the coverage gate on the same
           render. Then the live check again and verify-access with the
           previous generation's tier settings.

        Each plan's sha256 is recorded when it is made and re-checked before
        its apply. A failure after the first apply leaves the change marked
        ``rebind_incomplete`` with the next step; abandon refuses until a
        rebind finishes.
        """
        if governance_mode(self.env_dir) != "deterministic":
            print("Staged policy apply: legacy governance mode; nothing to rebind.")
            return 0
        from scripts.release_gate import PROMOTED, env_role

        # No GENIERAILS_ALLOW_CI_APPLY opt-out: an integration workflow has no rolled-back live change to repair.
        if _in_ci(self.environ, opt_out=False):
            raise Refused("policy-rebind applies to the data_access layer; it can't run in CI, even with "
                          "GENIERAILS_ALLOW_CI_APPLY=1. Run it on the deployment machine.")
        if env_role(self.env_dir) == PROMOTED:
            raise Refused(f"ENV={self.env_name} is promoted: its applies come only from a make release-plan "
                          "manifest, and policy-rebind doesn't make one yet. It runs on the rehearse env "
                          "(dev) only; nothing was applied.")
        with env_lock(self.env_dir, "policy-rebind"):
            journal = load_journal(self.env_dir)
            t = (journal or {}).get("transition")
            if not t:
                print("Staged policy apply: no unfinished policy change; nothing to rebind.")
                return 0
            if t.get("abandon_incomplete"):
                raise Refused("an abandon of this change was applied but its live check failed; finish it with "
                              f"make policy-abandon ENV={self.env_name}")
            if not t["switch_attempted"]:
                raise Refused("policy-rebind re-points a change whose switch was rolled back; this one never "
                              f"switched. Resume it, or drop it with make policy-abandon ENV={self.env_name}.")
            reason = self._restore_unproven(t, ("restore", "rebind"))
            if KEPT_NEW_COLUMNS in reason:
                raise Refused(self._kept_columns_message(t, "re-pointed", reason))
            if reason:
                raise Refused(f"this change can't be re-pointed ({reason}); nothing was applied.")
            if "restored" not in t:
                print("  The journal predates the restore marker; treating the recorded failed switch as restored "
                      "after a fresh live check.")
            self._refuse_split_drift("policy-rebind")
            desired = pg.desired_generation(self.env_dir)
            if desired["fingerprint"] == t["to"]:
                raise Refused(f"the generated config is the unfinished change's own generation {t['to']}; "
                              "there is nothing to re-point. Re-run the command that started it to resume it.")
            moved = self._moved_treatments(t, desired)
            if moved:
                raise Refused(
                    f"{len(moved)} column(s) have another treatment in the generated config than in the unfinished "
                    f"change {t['to']} ({', '.join(moved[:10])}): their live class.* tags changed since it started, "
                    "or its treatment settings were edited. A rebind only re-points principals, so nothing was "
                    "applied. Next: put those columns' class.* tags (or the treatment settings) back as they were "
                    f"when the change started, then re-run make policy-rebind ENV={self.env_name}. If no principal "
                    f"it names was deleted, run make policy-abandon ENV={self.env_name}, then re-run.")
            plan_dir = pg.ledger_dir(self.env_dir) / "rebind"
            # Grants earlier protect phases of this rebind held back, each proven by its saved plan.
            held = self._validated_held(t, plan_dir)
            env = self._hook_env(t) | {"GENIERAILS_REBIND_FINGERPRINT": desired["fingerprint"],
                                       "GENIERAILS_REBIND_TABLE_GRANTS": json.dumps(_table_keys(held))}
            print(f"=== Policy rebind ({self.env_name}): run {t['run_id']} (generation {t['to']}) with the "
                  f"principals of {desired['fingerprint']} ===", flush=True)
            plan_dir.mkdir(parents=True, exist_ok=True)
            os.chmod(plan_dir, 0o700)

            # 1. Protection: policies' principals and functions; no grant.
            plan_file, planned = self._rebind_plan(t, env, "rebind-protect", plan_dir)
            if plan_file is None:
                raise Refused("the rebind-protect plan was refused or couldn't be made (see above); "
                              "nothing was applied")
            rerun = f"re-run make policy-rebind ENV={self.env_name}"
            # The grants this protect plan holds back, bound to the plan: once it is applied, Terraform's
            # state no longer records the lost ones, so the access phase may create exactly these (and a
            # re-run, after validating the saved plan, carries the lost table SELECT on).
            record = self._held_record(t, plan_dir, plan_file, planned)
            t["rebind_held"] = [r for r in t.get("rebind_held") or [] if r["sha256"] != planned] + [record]
            held = sorted(set(held) | set(record["grants"]))
            env["GENIERAILS_REBIND_TABLE_GRANTS"] = json.dumps(_table_keys(held))
            env["GENIERAILS_REBIND_ALLOWED_GRANTS"] = json.dumps(held)
            t["rebind_incomplete"] = {"phase": "rebind-protect", "generation": desired["fingerprint"],
                                      "at": _now(), "reason": "it was interrupted",
                                      "next": f"Re-run make policy-rebind ENV={self.env_name}."}
            self._save(journal)
            if not self._rebind_apply(env, "rebind-protect", plan_file, planned):
                return self._rebind_failed(journal, t, "rebind-protect", "applying the protect plan failed",
                                           f"No grant was applied. Check the error above, then {rerun}.")
            rc = self._inventory(t, ("rebind",))
            if rc:
                return self._rebind_failed(
                    journal, t, "rebind-protect", "the live check after the protect phase failed" if rc == 1
                    else f"the live check after the protect phase couldn't run (exit {rc})",
                    f"No grant was applied. Fix what the live check lists, then {rerun}.")
            rc = self.run_cmd(self.agent_coverage_cmd + ["--env-dir", str(self.env_dir), "--env-name", self.env_name],
                              env=env, check=False).returncode
            if rc:
                return self._rebind_failed(
                    journal, t, "rebind-protect", "the live agent coverage check found an agent table column "
                    "without a GenieRails mask" if rc == 1 else f"the live agent coverage check couldn't run (exit {rc})",
                    f"No grant was applied. Fix what the check lists, then {rerun}.")
            print("  The protection is re-pointed and checked live; planning the grants.", flush=True)

            # 2. Access: the grants the deleted principals lost (after the coverage gate on this render).
            t["rebind_incomplete"].update(phase="rebind-access", at=_now())
            self._save(journal)
            plan_file, planned = self._rebind_plan(t, env, "rebind-access", plan_dir)
            if plan_file is None:
                return self._rebind_failed(journal, t, "rebind-access", "the access plan was refused or couldn't "
                                           "be made", f"The protection is re-pointed and verified. Fix what the "
                                           f"plan check lists, then {rerun}.")
            if not self._rebind_apply(env, "rebind-access", plan_file, planned):
                return self._rebind_failed(journal, t, "rebind-access", "applying the access plan failed",
                                           f"The protection is re-pointed and verified. {rerun[0].upper()}{rerun[1:]}.")
            rc = self._verify(t, "verify-rebound")
            if rc:
                return self._rebind_failed(journal, t, "rebind-access", "the live check or verify-access after "
                                           "the access phase failed", f"Fix what the check lists, then {rerun}.")
            t.pop("rebind_incomplete", None)
            t.pop("rebind_held", None)
            t["rebound"] = {"at": _now(), "generation": desired["fingerprint"]}
            self._save(journal)
            # The journal no longer needs the held-back record's plan copy; a failed rebind keeps them all.
            _drop_plans(*plan_dir.glob(f"{t['run_id']}-*.tfplan"))
            print(f"Policy rebind: run {t['run_id']} is on the previous protection ({t['from'] or 'none'}) with "
                  f"the current principals, verified live. Next: make policy-abandon ENV={self.env_name}.")
            return 0

    def _held_record(self, t: dict, plan_dir: Path, plan_file: Path, planned: str) -> dict:
        """The protect plan's held grants, kept with a copy of the plan and a digest binding them to it."""
        import shutil

        kept = plan_dir / f"{t['run_id']}-held-{planned[:16]}.tfplan"
        if not kept.exists():
            with _owner_only(kept):
                shutil.copyfile(plan_file, kept)
        grants = sorted(pg.saved_plan_variables(self.env_dir, self.env_name, kept).get("held_access_grants") or [])
        if grants != sorted(pg.load_sidecar(plan_file).get("held") or []) or _sha256(kept) != planned:
            raise Refused(f"the saved rebind-protect plan {plan_file} doesn't hold back what its record says; "
                          "nothing was applied")
        record = {"plan": kept.name, "sha256": planned, "grants": grants}
        return record | {"digest": _held_digest(t, record)}

    def _validated_held(self, t: dict, plan_dir: Path) -> list[str]:
        """The grants the journal says earlier protect plans held back, each checked against that plan."""
        held = set()
        for record in t.get("rebind_held") or []:
            kept = plan_dir / str(record.get("plan") or "")
            problem = ""
            if not record.get("plan") or not kept.is_file() or _sha256(kept) != record.get("sha256"):
                problem = "its saved protect plan is missing or changed"
            elif record.get("digest") != _held_digest(t, record):
                problem = "its digest doesn't match"
            elif sorted(pg.saved_plan_variables(self.env_dir, self.env_name, kept).get("held_access_grants")
                        or []) != sorted(record.get("grants") or []):
                problem = "the saved protect plan held back other grants"
            if problem:
                raise Refused(f"the journal's record of the grants rebind held back ({record.get('plan')}) can't "
                              f"be trusted: {problem}. Nothing was applied; restore the journal from a backup.")
            held |= set(record["grants"])
        return sorted(held)

    def _rebind_plan(self, t: dict, env: dict, stage: str, plan_dir: Path) -> tuple[Path | None, str]:
        """A saved plan of one rebind phase and its sha256 (None when the hook refused it)."""
        plan_file = plan_dir / f"{t['run_id']}-{stage}.tfplan"
        for stale in (plan_file, pg.sidecar_path(plan_file)):
            stale.unlink(missing_ok=True)
        with _owner_only(plan_file):
            rc = self.run_cmd([str(self.hook_dir / stage)], env=env | {pg.PLAN_OUT_ENV: str(plan_file)},
                              check=False).returncode
        if rc:
            _drop_plans(plan_file)  # refused: it can never be applied
            return None, ""
        planned = _sha256(plan_file)
        side = pg.load_sidecar(plan_file)
        if (side.get("stage"), side.get("desired"), side.get("old"), side.get("rebind")) != (
                stage, t["to"], t["from"], env["GENIERAILS_REBIND_FINGERPRINT"]):
            raise Refused(f"the saved {stage} plan {plan_file} isn't of this rebind")
        return plan_file, planned

    def _rebind_apply(self, env: dict, stage: str, plan_file: Path, planned: str) -> bool:
        """Apply exactly the saved plan whose hash was recorded when it was made."""
        if _sha256(plan_file) != planned:
            print(f"ERROR: the saved {stage} plan {plan_file} changed after it was made; not applied.",
                  file=sys.stderr)
            return False
        return not self.run_cmd([str(self.hook_dir / stage)], env=env | {pg.SAVED_PLAN_ENV: str(plan_file)},
                                check=False).returncode

    def _rebind_failed(self, journal: dict, t: dict, phase: str, reason: str, step: str) -> int:
        t["rebind_incomplete"].update(phase=phase, at=_now(), reason=reason, next=step)
        t["error"] = f"policy-rebind {phase}: {reason}"
        self._save(journal)
        print(f"ERROR: REBIND INCOMPLETE at {phase}: {reason}. {step}", file=sys.stderr)
        return 1

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
                print("  The generated config has moved on: the next apply refuses until this is resumed or "
                      f"abandoned (make policy-abandon ENV={self.env_name}, then re-run)." if self._may_abandon(t, desired) else
                      "  The generated config has moved on: the next apply refuses until this is resumed (re-run "
                      f"the same command at commit {t['commit'] or '?'}).")
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
        if t.get("rebind_incomplete"):
            lines.append(f"  live:      a policy-rebind stopped at {t['rebind_incomplete']['phase']} "
                         f"({t['rebind_incomplete']['reason']}); {t['rebind_incomplete']['next']}")
        elif t.get("abandon_incomplete"):
            lines.append("  live:      an abandon was applied but its live check failed; it is not abandoned")
        elif t["restore_failed"]:
            lines.append("  live:      a restore of the previous protection failed; the next run finishes it first")
        elif t["restoring"]:
            lines.append("  live:      a restore of the previous protection was interrupted; the next run finishes it "
                         "first (every column carries the old or the new mask)")
        elif not t["switch_attempted"]:
            lines.append(f"  live:      the previous protection ({t['from'] or 'none'}); no column was retagged")
        elif not t.get("restored") and not _pre_marker_restored(t):
            lines.append(f"  live:      a switch to {t['to']} that wasn't rolled back: every column carries the old "
                         f"or the new mask; the next run resumes at {t['current'] or t['failed_phase'] or 'switch'}")
        else:
            kept = self._kept_new_columns(t)
            after = (t.get("restored") or {}).get("after") or t["failed_phase"]
            if not t["from"]:
                lines.append(f"  live:      generation {t['to']}'s masks and treatment tags on {len(kept)} column(s), "
                             f"kept by the restore after {after}: a first deploy has no previous protection "
                             "(fail-closed; no business USE or SELECT is granted until the grant phase)")
            elif kept:
                lines.append(f"  live:      the previous protection ({t['from']}), restored after {after}; "
                             f"{len(kept)} newly governed column(s) keep generation {t['to']}'s masks (fail-closed)")
            else:
                lines.append(f"  live:      the previous protection ({t['from']}), restored after {after}")
            if kept:
                lines.append(f"  next:      re-run the same command to resume ({_remaining(t)}); it can't be "
                             "abandoned while columns keep its masks")
        return "\n".join(lines)

    def _kept_new_columns(self, t: dict) -> list[str]:
        """Columns a verified restore left on the change's value: newly governed, so kept masked."""
        restored = t.get("restored") or {}
        if not restored:
            return []
        try:
            old = self._snapshot(t["from"]) if t["from"] else None
        except (pg.StageError, OSError, ValueError):
            old = None
        old_tags = dict((old or {}).get("tags") or {})
        return sorted(c for c, v in (restored.get("tags") or {}).items() if old_tags.get(c) != v)

    def _unfinished_message(self, t: dict, desired: dict) -> str:
        resume = f"re-run the same command at commit {t['commit'] or '?'} (generation {t['to']}) to resume it"
        moved = self._moved_treatments(t, desired)
        drift = (f"\n{len(moved)} column(s) have another treatment in the generated config than in generation "
                 f"{t['to']} ({', '.join(moved[:10])}): their live class.* tags changed since the change started, or "
                 "its treatment settings were edited." if moved else "")
        if self._may_abandon(t, desired):
            # Abandon re-checks a rolled-back switch live before it drops anything.
            step = f"run make policy-abandon ENV={self.env_name}, then re-run; or {resume}"
            if t["switch_attempted"]:
                step += (". If principals it names were deleted and recreated, run make policy-rebind "
                         f"ENV={self.env_name} first")
        else:
            step = resume + (" (with those columns back as they were)" if moved else "")
        return (self.describe_transition(t) + f"\nThe generated config is now generation {desired['fingerprint']}. "
                "A new policy change can't start until this one is finished." + drift + f"\nNext: {step}.")

    def _moved_treatments(self, t: dict, desired: dict) -> list[str]:
        """Columns whose treatment value differs between the unfinished change and the generated config."""
        try:
            changed = self._snapshot(t["to"]) or {}
        except pg.StageError:
            return []
        want, have = dict(desired.get("tags") or {}), dict(changed.get("tags") or {})
        return sorted(c for c in want.keys() | have.keys() if want.get(c) != have.get(c))

    def _previous_snapshot_problem(self, t: dict) -> str:
        """Why the snapshot of the generation the change started from can't be trusted, or ""."""
        if not t["from"]:
            return ""
        try:
            snapshot = self._snapshot(t["from"])
        except (pg.StageError, OSError, ValueError) as exc:
            return f"the snapshot of generation {t['from']} can't be read ({exc})"
        if not pg.snapshot_intact(snapshot, t["from"]):
            return (f"the snapshot of generation {t['from']} ({pg.ledger_dir(self.env_dir) / (t['from'] + '.json')}) "
                    "doesn't match its fingerprint (edited since it was recorded)")
        return ""

    def _refuse_split_drift(self, command: str) -> None:
        """Refuse before any plan when the split's non-treatment assignments aren't the generated config's."""
        message = self._split_drift_message(command)
        if message:
            raise Refused(message)

    def _split_drift_message(self, command: str) -> str:
        problems = pg.split_assignment_problems(self.env_dir)
        if not problems:
            return ""
        return ("the split data_access config's non-treatment tag assignments differ from the generated config's, "
                "so the coverage check and the plan would judge different assignments; nothing was applied:\n  - "
                + "\n  - ".join(problems[:20])
                + f"\nRun make promote ENV={self.env_name} to re-split it (make {command} promotes after its live "
                "refresh), then re-run the same command.")

    def _kept_columns_message(self, t: dict, action: str, reason: str) -> str:
        return (f"this change can't be {action}: {reason}. The restore kept the mask the change gave them; dropping "
                "or re-pointing the change would leave them on policies it deletes, and GenieRails never untags a "
                "governed column on its own. Nothing was applied. Next: resume the change by re-running the command "
                f"that started it (commit {t['commit'] or '?'}) with the generated config on generation {t['to']}. "
                "If principals it names were deleted and recreated under new IDs, no GenieRails command can finish "
                "or drop it yet: see 'Changes that can't be dropped' in shared/docs/deterministic-policies.md.")


def _in_ci(environ: dict, *, opt_out: bool = True) -> bool:
    """CI, as make's _guard-not-ci-apply sees it (GENIERAILS_ALLOW_CI_APPLY=1 opts out when ``opt_out``)."""
    try:
        from scripts import ci_guard

        return ci_guard.status_line(environ, allow_opt_out=opt_out) != ci_guard.STATUS_NOT_CI
    except Exception:  # fail closed: a missing or failing CI check counts as CI
        return True


def _table_keys(grants) -> list[str]:
    return sorted(g.split("|", 1)[1] for g in grants if g.startswith("table_access|"))


def _held_digest(t: dict, record: dict) -> str:
    """Binds a held-grant record to its run, its change and its saved protect plan."""
    import hashlib

    bound = {"run_id": t["run_id"], "from": t["from"], "to": t["to"], "plan": record.get("plan"),
             "sha256": record.get("sha256"), "grants": sorted(record.get("grants") or [])}
    return hashlib.sha256(json.dumps(bound, sort_keys=True).encode()).hexdigest()


# _restore_unproven's reason when the verified restore kept newly governed columns on the change's value.
KEPT_NEW_COLUMNS = "keep the new generation's treatment"


def _unabandonable(reason: str) -> str:
    return ("this change already retagged columns or moved principals; it can't be "
            f"abandoned ({reason}). Re-run the command that started it to resume it.")


def _remaining(t: dict) -> str:
    return " -> ".join(p for p in t["phases"] if p not in t["completed"])


def _pre_marker_restored(t: dict) -> bool:
    """A journal from before the ``restored`` marker, in the one shape a verified restore of a switch leaves."""
    return ("restored" not in t and t["completed"] == ["create"]
            and t["failed_phase"] in ("switch", "verify-switched")
            and not t["restoring"] and not t["restore_failed"])


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


@contextmanager
def _owner_only(plan: Path):
    """Write a saved plan owner-only from creation (umask 077), and owner-only afterwards whatever the writer returned.

    A saved plan carries the deployer's credentials; the hook may write it and then refuse it.
    """
    previous = os.umask(0o077)
    try:
        yield
    finally:
        os.umask(previous)
        if plan.exists():
            os.chmod(plan, 0o600)


def _drop_plans(*plans: Path) -> None:
    """Delete applied saved plans (they carry the deployer's credentials) once the journal records them."""
    for plan in plans:
        try:
            plan.unlink(missing_ok=True)
        except OSError as exc:
            print(f"WARNING: the journal records this change, but the saved plan {plan} contains credentials and "
                  f"could not be deleted ({exc.strerror}); delete it.", file=sys.stderr)


def _git_head() -> str:
    out = subprocess.run(["git", "-C", str(SHARED), "rev-parse", "HEAD"], text=True, capture_output=True, check=False)
    return out.stdout.strip() if out.returncode == 0 else ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("run", "confirm", "abandon", "rebind", "status", "describe", "plan",
                                            "apply-saved"))
    parser.add_argument("--env-dir", type=Path, required=True)
    parser.add_argument("--env-name", default="")
    parser.add_argument("--hook-dir", type=Path, default=HOOK_DIR)
    parser.add_argument("--verify-access-cmd", default="",
                        help="shell command verifying every tier's output; gets GENIERAILS_VERIFY_TFVARS "
                             "and GENIERAILS_VERIFY_ENV_FILE")
    parser.add_argument("--inventory-cmd", default="", help="override the live policy check (argv, shell-split)")
    parser.add_argument("--verify-spec", default="", help="VERIFY_SPEC, if given: refused before create when "
                                                         "verify-created would refuse it")
    parser.add_argument("--verify-key", default="", help="VERIFY_KEY_COLUMN, if given")
    parser.add_argument("--account-tfvars", type=Path, default=None,
                        help="Account abac.auto.tfvars (group names), as verify-access uses")
    parser.add_argument("--commit", default="")
    parser.add_argument("--out-dir", type=Path, help="plan: where the saved phase plans go")
    parser.add_argument("--plan-dir", type=Path, help="apply-saved: the saved phase plans to apply")
    args = parser.parse_args(argv)
    if args.command not in ("status", "describe"):
        # A hand-edited catalog_map that maps a catalog to itself (S2) is refused
        # before anything is planned or applied, however this is reached.
        from scripts.saved_settings import env_identity_catalog_map_error

        error = env_identity_catalog_map_error(args.env_dir / "env.auto.tfvars")
        if error:
            print(f"ERROR: {error}", file=sys.stderr)
            return 1
    controller = Controller(
        args.env_dir, args.env_name, hook_dir=args.hook_dir, verify_access_cmd=args.verify_access_cmd,
        inventory_cmd=shlex.split(args.inventory_cmd) if args.inventory_cmd else None,
        commit=args.commit or _git_head(),
        verify_spec=args.verify_spec, verify_key=args.verify_key, account_tfvars=args.account_tfvars,
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
