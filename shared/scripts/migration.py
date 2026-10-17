#!/usr/bin/env python3
"""Move a legacy deployment to deterministic governance (design section 8).

An env opts in by committing ``governance_mode = "deterministic"`` with its
deterministic ``make generate`` output, ``table_readers`` and the reviewed
``ack.txt`` entries. Until then nothing here runs: an env that never opts in
plans no change.

report (``make migrate-plan ENV=<env>``)
    Read-only. From the env's Terraform state (what GenieRails deployed) and
    the committed config, it reports column by column and principal by
    principal what each sees today and after, the mask functions (legacy SQL
    against the library's), the policy principals under the tier rule, every
    SELECT and CAN_RUN the cutover would revoke, the agents missing
    ``acl_groups``, the frozen row filters and the generated tag state moves.
    It writes a proposed ``table_readers`` that reproduces every SELECT
    GenieRails grants today (an ignored runtime file; nothing committed).

adopt (the staged policy controller, scripts/staged_policy_apply.py)
    The cutover runs through the normal applying commands (``make rehearse``
    in dev; ``make release-plan`` / ``make release MANIFEST=`` elsewhere, one
    manifest and approval per phase, with release-plan's provenance). When the
    controller finds no policy journal but a legacy deployment in the state,
    it adopts that deployment as the live generation instead of refusing:
    the refusals below are checked, the legacy treatment tags are moved to
    their stable resource keys with ``terraform state mv`` (never destroyed),
    and the legacy masks become the "old" generation. The staged protocol then
    creates the new functions and policies under new names, verifies every
    tier, retags columns in place (one tag per column, so no column is ever
    unmasked or doubly masked) and keeps the legacy policies until a later
    successful release retires them.

Refusals: a weakening (scripts/weakening.py) or a revocation not acknowledged
in ``envs/<env>/ack.txt`` (``<env>:weaken:...`` and
``<env>:revoke:<table>:<principal>`` / ``<env>:revoke:<agent>:<group>``), a
legacy row filter the config no longer carries unchanged (they stay frozen
until declared ``row_filters`` replace them), an agent without
``acl_groups``, and a legacy mask that doesn't match on ``gr_treatment``.

Exit codes: 0 ok (report: the cutover may proceed), 1 refused, 2 error.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping

SHARED = Path(__file__).resolve().parents[1]
for _path in (SHARED, SHARED / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from governance_policies import TAG_KEY, parse_treatment_value  # noqa: E402

LEGACY_PREFIX = "legacy-"
RECORD = "migration.json"
MODULE = "module.data_access"
MASK = "POLICY_TYPE_COLUMN_MASK"
# Legacy treatments (treatment_config.json) whose mask is a full redaction.
REDACTED_LEGACY = {"redact", "compensation_redacted"}
_SQL_FUNCTION = re.compile(r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:[\w`]+\.){0,2}`?(\w+)`?\s*\(.*?\)"
                           r".*?RETURN\s+(.*?);\s*(?:\n|$)", re.IGNORECASE | re.DOTALL)


class Refused(RuntimeError):
    """The migration stops before changing anything; the message says what to fix."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _load_hcl(path: Path) -> dict:
    import hcl2

    return hcl2.loads(Path(path).read_text()) if Path(path).is_file() else {}


def _read_state(env_dir: Path, layer: str) -> dict:
    """A layer's state through scripts/state_layout.py ({} before the first apply)."""
    from state_layout import state_json

    try:
        return state_json(Path(env_dir), Path(env_dir).name, layer) or {}
    except RuntimeError as exc:
        raise Refused(str(exc)) from exc


def _instances(state: Mapping, rtype: str, name: str, module: str = MODULE):
    """(index key, attributes) of a resource's current instances."""
    for resource in state.get("resources") or []:
        if (resource.get("module"), resource.get("mode", "managed"), resource.get("type"), resource.get("name")) \
                != (module, "managed", rtype, name):
            continue
        for instance in resource.get("instances") or []:
            if instance.get("deposed") or instance.get("status") == "tainted":
                continue
            yield str(instance.get("index_key") or ""), dict(instance.get("attributes") or {})


def _first(value):
    if isinstance(value, list):
        return value[0] if value else {}
    return value or {}


def policy_from_state(key: str, attrs: Mapping) -> dict:
    """A deployed databricks_policy_info as the fgac_policies entry that produced it."""
    match = _first(attrs.get("match_columns"))
    mask, row_filter = _first(attrs.get("column_mask")), _first(attrs.get("row_filter"))
    target = mask or row_filter
    parts = str(target.get("function_name") or "").split(".")
    policy_type = str(attrs.get("policy_type") or "") or (MASK if mask else "POLICY_TYPE_ROW_FILTER")
    policy = {
        "name": key, "policy_type": policy_type,
        "catalog": str(attrs.get("on_securable_fullname") or attrs.get("catalog") or ""),
        "to_principals": list(attrs.get("to_principals") or []),
        "except_principals": list(attrs.get("except_principals") or []),
        "comment": str(attrs.get("comment") or ""),
        "match_condition": str(match.get("condition") or attrs.get("match_condition") or ""),
        "when_condition": str(attrs.get("when_condition") or ""),
        "match_alias": str(match.get("alias") or attrs.get("match_alias") or ""),
        "function_name": parts[-1] if parts else "",
        "function_catalog": parts[0] if len(parts) == 3 else "",
        "function_schema": parts[1] if len(parts) == 3 else "",
    }
    return {k: v for k, v in policy.items() if v not in ("", None) or k in ("to_principals", "except_principals")}


def _policy_value(policy: Mapping) -> str:
    from scripts.policy_generations import policy_value

    return policy_value(policy)


def tag_address(key: str, name: str) -> str:
    return f'{MODULE}.databricks_entity_tag_assignment.{name}["{key}"]'


def deployment(env_dir: Path) -> dict:
    """What GenieRails deployed in an env, read from its Terraform state."""
    env_dir = Path(env_dir)
    state = _read_state(env_dir, "data_access")
    masks, others = {}, {}
    for key, attrs in _instances(state, "databricks_policy_info", "policies"):
        policy = policy_from_state(key, attrs)
        if policy["policy_type"] != MASK:
            others[key] = policy
        elif not parse_treatment_value(_policy_value(policy)):
            masks[key] = policy
    tags, moves, other_tags = {}, [], []
    for key, attrs in _instances(state, "databricks_entity_tag_assignment", "assignments"):
        if attrs.get("entity_type", key.split("|")[0]) != "columns":
            continue
        column = str(attrs.get("entity_name") or key.split("|")[1])
        if str(attrs.get("tag_key") or key.split("|")[2]) == TAG_KEY:
            tags[column] = str(attrs.get("tag_value") or key.split("|")[3])
            moves.append((tag_address(key, "assignments"), tag_address(f"columns|{column}|{TAG_KEY}", "treatment")))
        else:
            other_tags.append(key)
    # Already moved by an interrupted adoption: a stable key with a legacy value.
    for key, attrs in _instances(state, "databricks_entity_tag_assignment", "treatment"):
        value = str(attrs.get("tag_value") or "")
        if not parse_treatment_value(value):
            tags[str(attrs.get("entity_name") or key.split("|")[1])] = value
    grants = sorted(k for k, _ in _instances(state, "databricks_grant", "table_access") if "|" in k)
    use_grants = sorted(f"{name}:{k}" for name in ("catalog_access", "schema_access")
                        for k, _ in _instances(state, "databricks_grant", name))
    workspace = _read_state(env_dir, "workspace")
    can_run: dict[str, list[str]] = {}
    for name in ("genie_space_acls", "genie_space_acls_created"):
        for key, attrs in _instances(workspace, "null_resource", name, "module.workspace"):
            groups = str((attrs.get("triggers") or {}).get("groups") or "")
            can_run.setdefault(key, [])
            can_run[key] += [g for g in groups.split(",") if g and g not in can_run[key]]
    return {"masks": masks, "frozen": others, "tags": tags, "moves": moves, "other_tags": sorted(other_tags),
            "grants": grants, "use_grants": use_grants, "can_run": can_run,
            "legacy": bool(masks or tags)}


def legacy_fingerprint(policies: list, tags: Mapping) -> str:
    """The fingerprint of a legacy generation: its masks (sorted by name) and treatment tags."""
    body = json.dumps({"policies": list(policies), "tags": dict(tags)}, sort_keys=True)
    return LEGACY_PREFIX + hashlib.sha256(body.encode()).hexdigest()[:16]


def legacy_generation(dep: Mapping) -> dict:
    """The legacy deployment as the staged controller's live ("old") generation.

    Its functions are never dropped (functions_sql is empty): they stay in
    Unity Catalog, unused once the legacy policies are retired. Verification
    of a column still on a legacy value uses the legacy checks.
    """
    policies = [dep["masks"][name] for name in sorted(dep["masks"])]
    return {
        "fingerprint": legacy_fingerprint(policies, dep["tags"]),
        "fgac_policies": policies, "tags": dict(dep["tags"]), "functions_sql": "",
        "settings": {"governance_mode": "legacy"},
        "legacy": {"grants": list(dep["grants"]), "can_run": dict(dep["can_run"]),
                   "frozen": dict(dep["frozen"])},
    }


# -- where a migration stands -------------------------------------------------

def deterministic(env_dir: Path) -> bool:
    return _load_hcl(Path(env_dir) / "env.auto.tfvars").get("governance_mode", "legacy") == "deterministic"


def record_path(env_dir: Path) -> Path:
    from scripts.policy_generations import ledger_dir
    from scripts.state_layout import RecordError

    try:
        return ledger_dir(Path(env_dir)) / RECORD
    except RecordError as exc:
        raise Refused(f"the migration record can't be read: {exc}") from exc


def load_record(env_dir: Path) -> dict | None:
    path = record_path(env_dir)
    try:
        return json.loads(path.read_text()) if path.is_file() else None
    except ValueError as exc:
        raise Refused(f"the migration record {path} is unreadable: {exc}") from exc


def adopted(record: Mapping | None) -> bool:
    """A record of a finished adoption (an "adopting" one was interrupted and resumes)."""
    return bool(record) and record.get("status", "adopted") == "adopted"


def migrated(env_dir: Path) -> str:
    """Why an env's state is no longer legacy-applicable ("" when it is), read from the state itself.

    Legacy mode never writes the stable-key treatment tag resource, a policy
    journal or a migration record, so any of them means a migration began.
    """
    from scripts.staged_policy_apply import journal_path

    env_dir = Path(env_dir)
    if record_path(env_dir).is_file():
        return f"a migration record ({record_path(env_dir)})"
    if journal_path(env_dir).is_file():
        return f"a policy journal ({journal_path(env_dir)})"
    state = _read_state(env_dir, "data_access")
    moved = [k for k, _ in _instances(state, "databricks_entity_tag_assignment", "treatment")]
    return f"treatment tags at stable keys in the data_access state ({moved[0]}, ...)" if moved else ""


def pending(env_dir: Path) -> bool:
    """A deterministic env whose state still holds an un-adopted legacy deployment."""
    from scripts.staged_policy_apply import load_journal

    return deterministic(env_dir) and load_journal(Path(env_dir)) is None and deployment(env_dir)["legacy"]


def baseline(env_dir: Path) -> dict | None:
    """The legacy generation while it is what GenieRails last switched in, else None."""
    from scripts import policy_generations as pg
    from scripts.staged_policy_apply import load_journal

    env_dir = Path(env_dir)
    if not deterministic(env_dir):
        return None
    journal = load_journal(env_dir)
    if journal is None:
        dep = deployment(env_dir)
        return legacy_generation(dep) if dep["legacy"] else None
    active = str(journal.get("active") or "")
    return pg.load_snapshot(env_dir, active) if active.startswith(LEGACY_PREFIX) else None


# -- before and after ---------------------------------------------------------

def legacy_view(generation: Mapping) -> dict:
    """The legacy generation as release_gate's before-state (who sees what today)."""
    from governance_policies import base_treatment

    policies = []
    for policy in generation["fgac_policies"]:
        value = _policy_value(policy)
        policies.append({"name": policy["name"], "value": value, "to": list(policy.get("to_principals") or []),
                         "except": list(policy.get("except_principals") or []),
                         "level": "redacted" if base_treatment(value) in REDACTED_LEGACY else "partial"})
    readers: dict[str, set[str]] = {}
    for key in (generation.get("legacy") or {}).get("grants") or []:
        table, principal = key.split("|", 1)
        readers.setdefault(table.lower(), set()).add(principal)
    return {"tags": dict(generation["tags"]), "policies": policies, "settings": {},
            "table_readers": readers, "has_generation": False}


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def wanted_can_run(env_dir: Path, cfg: Mapping) -> dict[str, list[str] | None]:
    """Agent key (as the workspace layer keys it) -> its acl_groups (None when missing)."""
    from table_readers import agents

    out = {}
    for space, agent in zip([s for s in cfg.get("genie_spaces") or [] if isinstance(s, dict)],
                            agents(Path(env_dir), cfg)):
        key = _slug(str(space.get("name") or "")) or str(space.get("genie_space_id") or "")
        out[key] = agent["acl_groups"]
    return out


def revocations(env_dir: Path, env: str, generation: Mapping, cfg: Mapping) -> dict[str, str]:
    """Ack key -> consequence, for every SELECT and CAN_RUN GenieRails grants today and the config drops."""
    from table_readers import normalize, readers_for

    legacy = generation.get("legacy") or {}
    readers = normalize(cfg.get("table_readers"), str(cfg.get("uc_catalog") or ""))
    out = {}
    for key in legacy.get("grants") or []:
        table, principal = key.split("|", 1)
        if principal not in readers_for(table, readers):
            out[f"{env}:revoke:{table.lower()}:{principal}"] = (
                f"{principal} loses SELECT on {table} (granted today; no table_readers entry keeps it)")
    wanted = wanted_can_run(env_dir, cfg)
    for agent, groups in sorted((legacy.get("can_run") or {}).items()):
        for group in groups:
            if group not in (wanted.get(agent) or []):
                out[f"{env}:revoke:{agent}:{group}"] = f"{group} loses CAN_RUN on agent {agent}"
    return out


def without_revoked(weak, env: str, revoked: Mapping[str, str]):
    """The weakening result minus each lost SELECT a revocation already reports.

    Here the after side is the committed config, so a table missing from its
    table_readers is a known revocation (<env>:revoke:<table>:<principal>), not
    unknown data. Any missing-reader weakening without that revoke key stays.
    """
    from weakening import CheckResult

    def kept(items):
        return tuple(w for w in items if not (w.category == "missing_table_readers_after"
                                              and f"{env}:revoke:{w.object_name}:{w.principal}" in revoked))
    return CheckResult(kept(weak.all_weakenings), kept(weak.acknowledged), kept(weak.unacknowledged))


def proposed_table_readers(generation: Mapping) -> dict[str, list[str]]:
    readers: dict[str, list[str]] = {}
    for key in (generation.get("legacy") or {}).get("grants") or []:
        table, principal = key.split("|", 1)
        readers.setdefault(table.lower(), [])
        if principal not in readers[table.lower()]:
            readers[table.lower()].append(principal)
    return {table: sorted(p) for table, p in sorted(readers.items())}


def render_table_readers(readers: Mapping[str, list[str]]) -> str:
    lines = [f"  {json.dumps(t)} = [{', '.join(json.dumps(p) for p in ps)}]" for t, ps in readers.items()]
    return ("# Proposed by make migrate-plan: reproduces every SELECT GenieRails grants today.\n"
            "table_readers = {\n" + "\n".join(lines) + ("\n" if lines else "") + "}\n")


def _comparable(policy: Mapping) -> dict:
    keys = ("policy_type", "catalog", "to_principals", "except_principals", "match_condition", "when_condition",
            "match_alias", "function_name", "function_catalog", "function_schema")
    return {k: (sorted(policy.get(k) or []) if k.endswith("principals") else str(policy.get(k) or ""))
            for k in keys}


def frozen_problems(generation: Mapping, desired_cfg: Mapping) -> list[str]:
    """Legacy row filters (and other non-mask policies) stay exactly as deployed."""
    frozen = (generation.get("legacy") or {}).get("frozen") or {}
    desired = {str(p.get("name")): p for p in desired_cfg.get("fgac_policies") or [] if p.get("policy_type") != MASK}
    problems = []
    for name, policy in sorted(frozen.items()):
        if name not in desired:
            problems.append(f"legacy {policy['policy_type']} {name} is missing from the config: legacy row "
                            "filters stay frozen until declared row_filters replace them; restore it "
                            "(re-run make generate on the legacy generated config)")
        elif _comparable(desired[name]) != _comparable(policy):
            problems.append(f"legacy {policy['policy_type']} {name} differs from what is deployed: legacy row "
                            "filters stay frozen as they are")
    return problems


def shape_problems(generation: Mapping) -> list[str]:
    return [f"legacy column mask {p['name']} doesn't match on {TAG_KEY}; re-apply the env with the current "
            "legacy code first (make generate, then make apply) so every mask is a treatment mask"
            for p in generation["fgac_policies"] if not _policy_value(p)]


def analyze(env_dir: Path, env: str, generation: Mapping, *, desired_abac: Path | None = None) -> dict:
    """Every check the cutover needs, against the committed config (pure apart from file reads)."""
    from ack_file import AckFileError, parse_file
    from weakening import check_weakenings
    from scripts import release_gate as rg
    from scripts.runtime_overlay import effective

    env_dir = Path(env_dir)
    cfg = _load_hcl(env_dir / "env.auto.tfvars")
    desired_cfg = _load_hcl(desired_abac or effective(env_dir, "abac.auto.tfvars"))
    try:
        entries = parse_file(env_dir / "ack.txt", env) if (env_dir / "ack.txt").is_file() else ()
    except AckFileError as exc:
        raise Refused(f"envs/{env}/ack.txt: {exc}") from exc
    keys = {e.key for e in entries}
    before, after = legacy_view(generation), rg._desired(env_dir, desired_abac)
    states = rg.governance_states(before, after)
    revoked = revocations(env_dir, env, generation, cfg)
    weak = without_revoked(check_weakenings(*states, env, keys), env, revoked)
    problems = shape_problems(generation) + frozen_problems(generation, desired_cfg)
    undeterministic = sorted({v for v in after["tags"].values() if not parse_treatment_value(v)})
    if undeterministic:
        problems.append(f"the generated config is still legacy (treatment values {undeterministic[:3]}): run "
                        f"make generate ENV={env} with governance_mode = \"deterministic\" and commit it")
    missing = sorted(a for a, groups in wanted_can_run(env_dir, cfg).items() if groups is None)
    problems += [f"agent {a} has no acl_groups: list the groups that may run it ([] means nobody)" for a in missing]
    consequences = weak.consequences | revoked
    stale = sorted(e.key for e in entries if e.kind in ("weaken", "revoke") and e.key not in consequences)
    return {
        "before": before, "after": after, "states": states, "weakening": weak, "revocations": revoked,
        "unacked_revocations": sorted(k for k in revoked if k not in keys), "problems": problems,
        "stale": stale, "consequences": consequences,
        "ok": not problems and weak.ok and all(k in keys for k in revoked),
    }


def check(env_dir: Path, env: str, generation: Mapping) -> dict:
    """analyze(), raising Refused with every reason when the cutover may not start."""
    result = analyze(env_dir, env, generation)
    if result["ok"]:
        return result
    lines = list(result["problems"])
    lines += [f"unacknowledged weakening {w.ack_key}: {w.consequence}" for w in result["weakening"].unacknowledged]
    lines += [f"unacknowledged revocation {k}: {result['revocations'][k]}" for k in result["unacked_revocations"]]
    raise Refused("the migration from legacy governance refused:\n  - " + "\n  - ".join(lines)
                  + f"\nNothing was changed. Run make migrate-plan ENV={env} for the full report; add the reviewed "
                  f"acknowledgements to envs/{env}/ack.txt (or fix the config) in a PR.")


# -- adoption -------------------------------------------------------------------

def _terraform_state(env_dir: Path, env_name: str) -> Callable[..., int]:
    from scripts.policy_generations import RUNNER

    layer = Path(env_dir) / "data_access"

    def run(command: str, *addresses: str) -> int:
        return subprocess.run([str(RUNNER), "data_access", env_name, command, *addresses],
                              env={**os.environ, "LAYER_ENV_DIR": str(layer)}, check=False).returncode
    return run


def forgotten_tags(dep: Mapping, desired_cfg: Mapping) -> list[str]:
    """Legacy column sensitivity tags the config drops: forgotten in state, left on the column."""
    kept = {f"{i.get('entity_type')}|{i.get('entity_name')}|{i.get('tag_key')}|{i.get('tag_value')}"
            for i in desired_cfg.get("tag_assignments") or []}
    return [key for key in dep["other_tags"] if key not in kept]


def adopt(env_dir: Path, env: str, *, terraform: Callable[..., int] | None = None) -> dict:
    """Check, move the legacy tags to their stable keys, and adopt the legacy generation.

    Returns the journal to start from (active = the legacy generation). The
    state moves are idempotent, so an interrupted adoption resumes.
    """
    from scripts import policy_generations as pg
    from scripts.runtime_overlay import effective
    from scripts.staged_policy_apply import empty_journal

    env_dir = Path(env_dir)
    dep = deployment(env_dir)
    generation = legacy_generation(dep)
    result = check(env_dir, env, generation)
    terraform = terraform or _terraform_state(env_dir, env)
    forget = forgotten_tags(dep, _load_hcl(effective(env_dir, "abac.auto.tfvars")))
    print(f"=== Migration ({env}): adopting the legacy deployment as generation {generation['fingerprint']} ===",
          flush=True)
    # The no-way-back marker lands before the first move, so a crash part-way
    # still leaves the env refusing legacy applies (release_gate.legacy_guard).
    pg.write_json(record_path(env_dir), {
        "status": "adopting", "legacy": generation["fingerprint"], "started_at": _now(),
        "moves": [t for _, t in dep["moves"]], "forget": forget,
    })
    for source, target in dep["moves"]:
        print(f"  terraform state mv '{source}' '{target}'", flush=True)
        if terraform("state-mv", source, target):
            raise Refused(f"terraform state mv {source} failed; the state is as the moves above left it. "
                          "Re-run the same command to resume")
    for key in forget:
        print(f"  terraform state rm '{tag_address(key, 'assignments')}' (the tag stays on the column)")
        if terraform("state-rm", tag_address(key, "assignments")):
            raise Refused(f"terraform state rm {key} failed; re-run the same command to resume")
    pg.save_snapshot(env_dir, generation)
    pg.write_json(record_path(env_dir), {
        "status": "adopted", "legacy": generation["fingerprint"], "adopted_at": _now(),
        "moved": [t for _, t in dep["moves"]], "forgotten": forget,
        "acknowledged": result["consequences"],
    })
    journal = empty_journal()
    journal["active"] = generation["fingerprint"]
    journal["history"].append({"migrated_from_legacy": generation["fingerprint"], "at": _now()})
    print(f"  Legacy masks stay live until a later successful release retires them; "
          f"{len(result['revocations'])} acknowledged revocation(s) apply with the cutover.")
    return journal


def accounted(env_dir: Path, env: str) -> tuple[dict[str, str], list[str]]:
    """(consequences ack.txt entries may match, unacknowledged revocation keys) for release-plan.

    Before adoption the revocations are computed and enforced; afterwards the
    acknowledgements the adoption accepted stay accounted for, so they never
    turn stale.
    """
    env_dir = Path(env_dir)
    record = load_record(env_dir)
    if adopted(record):
        return dict(record.get("acknowledged") or {}), []
    if not pending(env_dir):
        return {}, []
    from ack_file import parse_file

    keys = {e.key for e in parse_file(env_dir / "ack.txt", env)} if (env_dir / "ack.txt").is_file() else set()
    revoked = revocations(env_dir, env, legacy_generation(deployment(env_dir)),
                          _load_hcl(env_dir / "env.auto.tfvars"))
    return revoked, sorted(k for k in revoked if k not in keys)


# -- the dry-run report ---------------------------------------------------------

def _functions(env_dir: Path) -> dict[str, str]:
    """Unqualified function name -> its RETURN expression, from the env's SQL files."""
    out = {}
    for path in (env_dir / "data_access" / "masking_functions.sql", env_dir / "generated" / "masking_functions.sql"):
        if path.is_file():
            for name, body in _SQL_FUNCTION.findall(path.read_text()):
                out.setdefault(name.lower(), " ".join(body.split()))
    return out


def _short(text: str, width: int = 100) -> str:
    text = " ".join(text.splitlines())
    return text if len(text) <= width else text[: width - 3] + "..."


def _derive_live_desired(env_dir: Path, out_dir: Path) -> Path:
    """Derive a promoted env's assignments from live class.* tags into scratch output."""
    script = SHARED / "scripts" / "derive_assignments.py"
    result = subprocess.run(
        [sys.executable, str(script), "--auth-file", "auth.auto.tfvars", "--env-file", "env.auto.tfvars",
         "--config", "generated/abac.auto.tfvars", "--out-dir", str(out_dir)],
        cwd=env_dir, check=False,
    )
    if result.returncode:
        raise Refused("re-reading live class.* tags for the migration comparison failed (above); "
                      "no raw fallback was assumed")
    return out_dir / "abac.auto.tfvars"


def report(env_dir: Path, env: str, *, desired_abac: Path | None = None) -> tuple[list[str], dict]:
    """(printed lines, JSON report) of the dry run; never changes anything."""
    env_dir = Path(env_dir)
    dep = deployment(env_dir)
    generation = legacy_generation(dep)
    lines = [f"=== Migration dry run ({env}): legacy -> deterministic governance (read-only) ==="]
    data: dict = {"env": env, "legacy_generation": generation["fingerprint"], "deterministic": deterministic(env_dir),
                  "proposed_table_readers": proposed_table_readers(generation)}
    record = load_record(env_dir)
    if record is not None and not adopted(record):
        lines.append(f"An adoption started at {record.get('started_at')} was interrupted; the next rehearse or "
                     f"release-plan ENV={env} resumes it. The report below is against the current state.")
    if adopted(record):
        lines.append(f"Already migrated: the legacy deployment was adopted at {record.get('adopted_at')} "
                     f"(generation {record.get('legacy')}); make policy-status ENV={env} shows where it stands.")
        return lines, data | {"ok": True, "migrated": True}
    if not dep["legacy"]:
        lines.append("No legacy GenieRails masks or treatment tags in this env's state: nothing to migrate.")
        return lines, data | {"ok": True}
    lines.append(f"Today: {len(dep['masks'])} legacy mask polic(ies), {len(dep['tags'])} treatment-tagged column(s), "
                 f"{len(dep['grants'])} SELECT grant(s), {sum(map(len, dep['can_run'].values()))} CAN_RUN grant(s).")
    lines.append("")
    lines.append("Proposed table_readers (reproduces every SELECT GenieRails grants today):")
    lines += ["  " + line for line in render_table_readers(data["proposed_table_readers"]).splitlines()]
    cfg = _load_hcl(env_dir / "env.auto.tfvars")
    tiers, exempt = list(cfg.get("access_tier_groups") or []), list(cfg.get("raw_exempt_principals") or [])
    outside = sorted({p for ps in data["proposed_table_readers"].values() for p in ps} - set(tiers) - set(exempt))
    if outside:
        lines.append(f"  Not in access_tier_groups or raw_exempt_principals, so they can't be readers: "
                     f"{', '.join(outside)}. Add each to a tier (or raw_exempt_principals), or leave it out and "
                     "acknowledge its revocation.")
    if not deterministic(env_dir) and (cfg.get("promote_from") or env_dir.name == "prod"):
        source = str(cfg.get("promote_from") or "dev")
        lines += ["", f"Next: opt {source} in first. Then set governance_mode = \"deterministic\" and deployer_principal_id in",
                  f"  envs/{env}/env.auto.tfvars (and group_map if {env}'s group names differ), run make",
                  f"  rebuild-governed-tables ENV={env}, then make promote-to ENV={env}: it brings access_tier_groups and,",
                  f"  where {env} has none yet, table_readers (compare them with the proposal above) and each agent's",
                  f"  acl_groups from {source}. Then re-run make migrate-plan ENV={env} for the column-by-column comparison",
                  "  and the refusals."]
        return lines, data | {"ok": False, "ready": False}
    if not deterministic(env_dir):
        lines += ["", f"Next: set governance_mode = \"deterministic\", deployer_principal_id and access_tier_groups in envs/{env}/env.auto.tfvars,",
                  f"  add the table_readers above, run make rebuild-governed-tables ENV={env} (the first deterministic",
                  "  generate refuses without it), then make generate, then re-run",
                  f"  make migrate-plan ENV={env} for the column-by-column comparison and the refusals."]
        return lines, data | {"ok": False, "ready": False}

    committed = _load_hcl(env_dir / "generated" / "abac.auto.tfvars")
    if desired_abac is None and not any(i.get("tag_key") == TAG_KEY
                                       for i in committed.get("tag_assignments") or []):
        with tempfile.TemporaryDirectory(prefix=f"genierails-migrate-{env}-") as tmp:
            return report(env_dir, env, desired_abac=_derive_live_desired(env_dir, Path(tmp)))

    result = analyze(env_dir, env, generation, desired_abac=desired_abac)
    before_state, after_state = result["states"]
    functions = _functions(env_dir)
    desired = result["after"]
    lines += ["", "Columns (what each principal sees today -> after):"]
    by_value_old = {p["value"]: p for p in legacy_view(generation)["policies"]}
    for column in sorted(set(before_state.columns) | set(after_state.columns)):
        old_value, new_value = generation["tags"].get(column, ""), desired["tags"].get(column, "")
        lines.append(f"  {column}: {old_value or 'untagged'} -> {new_value or 'untagged'}")
        old_policy = next((p for p in generation["fgac_policies"] if _policy_value(p) == old_value
                           and p["catalog"] == column.split(".")[0]), None)
        if old_policy:
            fn = old_policy.get("function_name", "")
            lines.append(f"    mask today: {fn}: {_short(functions.get(fn.lower(), '(SQL not in this checkout)'))}")
        for policy in desired["policies"]:
            if policy["value"] == new_value and policy["name"].startswith(f"gr_mask_{column.split('.')[0]}_"):
                fn = policy["name"].replace(f"gr_mask_{column.split('.')[0]}_", "gr_mask_", 1)
                lines.append(f"    after ({fn.rsplit('_', 1)[1]}): {fn}: "
                             f"{_short(functions.get(fn.lower(), '(library)'))}")
        for who in sorted(set(before_state.columns.get(column, {})) | set(after_state.columns.get(column, {}))):
            old = before_state.columns.get(column, {}).get(who)
            new = after_state.columns.get(column, {}).get(who)
            mark = "" if (old and new and old.level == new.level) else "   <- changes"
            lines.append(f"    {who}: {old.consequence if old else '?'} -> {new.consequence if new else '?'}{mark}")
    lines += ["", "Policies (principals under the tier rule):"]
    for value, policy in sorted(by_value_old.items()):
        lines.append(f"  legacy {policy['name']} ({value}): TO {policy['to']} EXCEPT {policy['except']}")
    for policy in sorted(desired["policies"], key=lambda p: p["name"]):
        lines.append(f"  new    {policy['name']}: TO {policy['to']} EXCEPT {policy['except']}")
    lines.append("  The legacy policies stay live until a later successful release retires them; their "
                 "functions are never dropped.")
    lines += ["", "Revocations (granted today, dropped by the config):"]
    lines += [f"  {'ACKNOWLEDGED' if k not in result['unacked_revocations'] else 'NEEDS ACK   '} {k}: {c}"
              for k, c in sorted(result["revocations"].items())] or ["  none"]
    removed_use = [g for g in dep["use_grants"] if not _use_kept(g, desired["table_readers"])]
    if removed_use:
        lines.append(f"  Also removed (no data by themselves): {', '.join(removed_use)}")
    lines += ["", "Weakenings:"]
    lines += [f"  ACKNOWLEDGED {w.ack_key}: {w.consequence}" for w in result["weakening"].acknowledged]
    lines += [f"  NEEDS ACK    {w.ack_key}: {w.consequence}" for w in result["weakening"].unacknowledged]
    if not result["weakening"].all_weakenings:
        lines.append("  none")
    lines += ["", "Frozen legacy row filters (kept as deployed until declared row_filters replace them):"]
    lines += [f"  {name}" for name in sorted(dep["frozen"])] or ["  none"]
    lines += ["", "Tag state moves (run by the cutover under the env lock; tags are never destroyed):"]
    lines += [f"  terraform state mv '{s}' '{t}'" for s, t in dep["moves"]] or ["  none"]
    forget = forgotten_tags(dep, _load_hcl(desired_abac or _effective_abac(env_dir)))
    lines += [f"  terraform state rm '{tag_address(k, 'assignments')}'  (legacy sensitivity tag, left on the column)"
              for k in forget]
    if result["stale"]:
        lines += ["", "ack.txt entries that match nothing (refused):"] + [f"  {k}" for k in result["stale"]]
    if result["problems"]:
        lines += ["", "Refused:"] + [f"  - {p}" for p in result["problems"]]
    needed = [w.ack_key for w in result["weakening"].unacknowledged] + result["unacked_revocations"]
    if needed:
        lines += ["", f"To accept, add to envs/{env}/ack.txt (reviewed, code-owned):"] + [f"  {k}" for k in needed]
    ok = result["ok"] and not result["stale"]
    lines += ["", "Result: " + ("the cutover may proceed: merge, then make rehearse ENV=dev, or make release-plan "
                                f"ENV={env} for a promoted env (one manifest and approval per phase)." if ok else
                                "refused (above); nothing was changed.")]
    data |= {
        "ok": ok, "ready": True, "problems": result["problems"], "stale_acknowledgements": result["stale"],
        "revocations": result["revocations"], "unacknowledged_revocations": result["unacked_revocations"],
        "weakenings": [{"key": w.ack_key, "before": w.before, "after": w.after,
                        "acknowledged": w in result["weakening"].acknowledged}
                       for w in result["weakening"].all_weakenings],
        "columns": {c: {who: {"before": (before_state.columns.get(c, {}).get(who) or _NONE).consequence,
                              "after": (after_state.columns.get(c, {}).get(who) or _NONE).consequence}
                        for who in set(before_state.columns.get(c, {})) | set(after_state.columns.get(c, {}))}
                    for c in set(before_state.columns) | set(after_state.columns)},
        "tag_moves": dep["moves"], "forgotten_tags": forget, "frozen_row_filters": sorted(dep["frozen"]),
        "removed_use_grants": removed_use,
    }
    return lines, data


class _Unknown:
    consequence = "?"


_NONE = _Unknown()


def _effective_abac(env_dir: Path) -> Path:
    from scripts.runtime_overlay import effective

    return effective(env_dir, "abac.auto.tfvars")


def _use_kept(grant: str, readers: Mapping[str, set]) -> bool:
    kind, key = grant.split(":", 1)
    securable, principal = key.split("|", 1)
    depth = 1 if kind == "catalog_access" else 2
    return any(principal in ps and ".".join(t.split(".")[:depth]) == securable.lower() for t, ps in readers.items())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    cmd = sub.add_parser("report", help="the read-only dry run (make migrate-plan)")
    cmd.add_argument("--env-dir", type=Path, required=True)
    cmd.add_argument("--env", required=True)
    cmd.add_argument("--out-dir", type=Path, help="where report.json and table_readers.proposed.hcl go")
    args = parser.parse_args(argv)
    try:
        lines, data = report(args.env_dir, args.env)
    except (Refused, OSError, ValueError) as exc:
        print(f"ERROR: migration dry run: {exc}", file=sys.stderr)
        return 2
    print("\n".join(lines))
    if args.out_dir:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        (args.out_dir / "report.json").write_text(json.dumps(data, indent=2, sort_keys=True, default=list) + "\n")
        (args.out_dir / "table_readers.proposed.hcl").write_text(render_table_readers(data["proposed_table_readers"]))
        print(f"Report: {args.out_dir / 'report.json'}; proposed table_readers: "
              f"{args.out_dir / 'table_readers.proposed.hcl'}")
    return 0 if data.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
