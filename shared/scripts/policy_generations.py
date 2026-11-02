#!/usr/bin/env python3
"""Versioned policy generations and the staged-apply hooks for deterministic governance.

``scripts/staged_policy_apply.py`` (step 6) drives these hooks; this module
owns what each stage renders and what its plan may do. A **generation** is one
deterministic model: its column-mask policies (with principals), gr_treatment
tag values and functions, identified by ``governance_policies.model_fingerprint``.
Function, policy and treatment value names are content-versioned (see
governance_policies.py), so two generations never fight over one name.

Hooks are argv executables in ``scripts/policy_hooks/``, one per stage. Each
renders its stage, checks the render (no tagged column without a mask, no
principal with two outside a tier move), plans it, refuses any destroy the
stage doesn't allow, and applies exactly that plan. A render never rewrites
the layer's split config (``data_access/abac.auto.tfvars`` and
``masking_functions.sql``, committed in a legacy or migrating env): it goes to
the ignored ``envs/<env>/generated/.runtime/data_access/``, and every
Terraform run of the stage reads it from there (``GENIERAILS_LAYER_CONFIG_DIR``,
scripts/terraform_layer.sh). They read:

* ``GENIERAILS_ENV_DIR``: the env (``envs/<env>``); required.
* ``GENIERAILS_ENV_NAME``: defaults to the env dir's name.
* ``GENIERAILS_POLICY_FINGERPRINT``: must equal ``fingerprint`` of the
  generated config, so a hook never applies a model the controller didn't
  record (``abandon`` reads that generation from its snapshot instead).
* ``GENIERAILS_OLD_POLICY_FINGERPRINT``: the active generation, kept live.
* ``GENIERAILS_RETIRE_FINGERPRINT``: a generation an earlier release made
  obsolete; kept live until ``retire`` removes it.
* ``GENIERAILS_POLICY_KEEP_NEW_TAGS=1``: a switch was attempted, so newly
  governed columns may already carry their new value; keep it.
* ``GENIERAILS_COVERAGE_GATE_RUNNER``: when set, the coverage check re-runs on
  each render, so its result matches what the stage applies. It is told the
  stage and the active generation, so the stages that keep the active
  generation's tag values (create, tighten, restore, abandon), a recorded
  legacy migration's or a deterministic one's, aren't judged against the
  desired values the live refresh derived (scripts/coverage_gate.py
  staged_kept_assignments). A rebind is judged as its restore, told the
  generation it re-points to; the grant re-check is told it opens access.
* ``GENIERAILS_POLICY_PLAN_OUT=<file>`` (make release-plan): render, check and
  plan the stage into ``<file>``, record the render and whether the plan
  changes anything in ``<file>.json``, and apply nothing.
* ``GENIERAILS_POLICY_PLAN_BASIS=<file>``: with PLAN_OUT, the grant stage
  re-applies the render planned into that earlier plan file instead of the
  last applied one.
* ``GENIERAILS_POLICY_SAVED_PLAN=<file>`` (make release MANIFEST=...): apply
  exactly that saved plan, after checking the stage still renders what was
  planned; a plan that changes nothing is not applied. Nothing is re-planned.
* ``GENIERAILS_REBIND_FINGERPRINT``: the rebind hooks only; must equal the
  generated config's ``fingerprint``.
* ``GENIERAILS_REBIND_TABLE_GRANTS``: the rebind hooks only; a JSON list of
  the ``table_access`` keys an earlier protect phase held back. Terraform's
  refresh drops a grant whose principal was deleted from state, so these are
  passed back as already applied (``rebind_table_grants``) and granted again.

Stages (every one keeps all retained generations' functions):

* ``create``: replacement policies are added; retained policies keep their
  principals; every column keeps its old treatment value. A new policy on a
  value columns already carry (a new middle tier) waits for ``tighten``.
  Nothing is removed and nothing is retagged.
* ``tighten`` (tier moves only): every policy shared with the desired
  generation takes the union of its live and desired ``TO`` principals and the
  intersection of its ``EXCEPT`` principals, and the deferred policies are
  added. Coverage only grows: nobody masked before or after is unmasked, and
  only principals being moved can briefly hit ``MULTIPLE_MASKS``.
* ``switch``: columns are retagged to the desired values and policies take
  their desired principals. Policies of retained generations whose value no
  column carries any more stay until ``retire``; a retained policy on a value
  still in use that the desired generation drops leaves here (after tighten,
  the desired policies already cover everyone it covered).
* ``restore``: back to the ``create`` render (old values and principals) after
  a failed tighten or switch; newly governed columns keep their new value.
* ``retire``: the retire generation's policies leave.
* ``abandon``: a transition that never switched (or whose switch was rolled
  back) is dropped: back to the active generation, without the replacement's
  policies. Its plan may only delete those policies; any other change, such
  as a column retagged since the restore, refuses (``plan_violations``). The
  controller saves that plan, checks live, and applies exactly it.
* ``drop-functions``: after ``retire``, drop the functions only the retired
  generation used, once no live policy calls them.
* ``grant``: last, once every phase above is applied and verified: re-apply
  the last stage's render with new SELECT allowed. Every earlier phase plans
  ``policy_stage`` values that withhold new grants (business SELECT, and the
  business groups' USE_CATALOG and USE_SCHEMA; grants already applied stay),
  so access never opens before its protection is verified: a first deploy's
  create plans only the policies, the functions and the deployer's own
  grant. Its plan may change no policy or treatment tag.
* ``rebind-protect`` and ``rebind-access`` (make policy-rebind): after a
  rolled-back switch, when principals the policies name were deleted and
  recreated (Unity Catalog then shows them as ``unknown``), re-apply the
  restore render with the generated config's principals. The generated config
  may differ from the unfinished change only in its policies' principals
  (``rebind_problems``). ``rebind-protect`` holds back every grant its plan
  would create (``held_access_grants``), so it re-points policies and
  redeploys the functions only; ``rebind-access``, once that is checked live,
  creates those grants (table SELECT only where it was applied before:
  ``GENIERAILS_REBIND_TABLE_GRANTS``). ``rebind_violations`` refuses anything
  else.

Snapshots of every generation a hook applies are kept next to the layer's
state, in ``<env>/data_access/.policy_generations/<fingerprint>.json`` (with
GENIERAILS_STATE_DIR set, under ``<dir>/<cloud>/<env>/records/`` instead:
``ledger_dir``); the last applied stage is recorded in ``stage.json`` there
for verification.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SHARED = Path(__file__).resolve().parents[1]
if str(SHARED) not in sys.path:
    sys.path.insert(0, str(SHARED))

from governance_policies import (  # noqa: E402
    TAG_KEY, model_fingerprint, parse_treatment_value, replace_sql_library,
    sql_library_block,
)

APPLY_STAGES = ("create", "tighten", "switch", "restore", "retire", "abandon")
REBIND_STAGES = ("rebind-protect", "rebind-access")
STAGES = APPLY_STAGES + ("drop-functions", "grant") + REBIND_STAGES
RUNNER = SHARED / "scripts" / "terraform_layer.sh"
COVERAGE_GATE = SHARED / "scripts" / "coverage_gate.py"
DEPLOY_FUNCTIONS = SHARED / "deploy_masking_functions.py"
LEDGER = Path(".policy_generations")
STAGE_RECORD = "stage.json"
# Where a stage's render goes (ignored, like every generated/.*), and the
# variable that points terraform_layer.sh at it.
STAGE_CONFIG = Path("generated") / ".runtime" / "data_access"
STAGE_CONFIG_ENV = "GENIERAILS_LAYER_CONFIG_DIR"
# The stages whose render keeps the active generation's tag values.
KEEP_TAG_STAGES = ("create", "tighten", "restore", "abandon")
ALL_USERS = "account users"
OUTSIDER = "<anyone outside the tiers>"
# Resources a staged apply may never destroy: a live policy, a treatment tag
# (a column would lose its mask), or the resource whose destroy drops functions.
PROTECTED_TYPES = {
    "databricks_policy_info", "databricks_entity_tag_assignment", "terraform_data.masking_functions_drop",
}
# Deleting a policy restarts this wait (its trigger hashes the policy map); it changes no access.
ENFORCEMENT_WAIT = "time_sleep.wait_for_policy_enforcement"
# Business access is only ever created once the new protection is switched in.
BUSINESS_GRANTS = {"catalog_access", "schema_access", "table_access"}
# The grants a rebind's protect phase holds back (the root's held_access_grants).
ACCESS_GRANTS = BUSINESS_GRANTS | {"terraform_sp_manage_catalog"}
MASKING_FUNCTIONS = "terraform_data.masking_functions"
FUNCTION_DROP = "terraform_data.masking_functions_drop"
# What a masking_functions replace deploys and where (its triggers since the
# paths left them); a replace that keeps all four only re-keys it.
DEPLOY_TRIGGERS = ("sql_hash", "warehouse_id", "host", "client_id")
# Checkout paths, which older states recorded in masking_functions' triggers
# and masking_functions_drop's input (as absolute paths).
# (relative_base only appeared in unreleased state; it is dropped again.)
PATH_KEYS = frozenset({"sql_file", "script", "auth_file", "relative_base"})
DROP_TARGET = ("warehouse_id", "host")
# Which functions masking_functions_drop drops: older states lack it, and
# recording it the first time changes no target.
DROP_IDENTITY = "sql_hash"
REKEY_NOTE = ("terraform_data.masking_functions is replaced only to stop recording checkout paths "
              "(sql_file, script, auth_file): the SQL, warehouse, host and client are unchanged, so it "
              "re-runs CREATE OR REPLACE of the same functions and drops nothing. This happens once.")
# What a rebind may change on a mask policy; everything else must be planned unchanged.
REBIND_POLICY_FIELDS = ("to_principals", "except_principals")
POLICY_BINDING = ("name", "on_securable_type", "on_securable_fullname", "policy_type", "for_securable_type",
                  "column_mask", "row_filter", "match_columns", "when_condition")
SETTINGS_KEYS = ("access_tier_groups", "raw_exempt_principals", "treatment_versions",
                 "column_overrides", "tier_access_overrides")
_VALUE_IN_CONDITION = re.compile(r"hasTagValue\(\s*'" + TAG_KEY + r"'\s*,\s*'([^']+)'\s*\)")


class StageError(RuntimeError):
    pass


def _load(path: Path) -> dict:
    import hcl2
    if not path.is_file():
        return {}
    try:
        return hcl2.loads(path.read_text())
    except Exception:
        if path.name != "auth.auto.tfvars":
            raise
    # Raised outside the handler with no parser text, which can echo a
    # malformed secret (also via __context__).
    raise RuntimeError(f"{path} is not valid HCL") from None


def desired_generation(env_dir: Path) -> dict:
    """The generation the generated config describes, with the settings behind it.

    The effective config: the runtime overlay of a live derivation when there
    is one (scripts/runtime_overlay.py), else the committed generated/.
    """
    from scripts.runtime_overlay import effective

    cfg = _load(effective(env_dir, "abac.auto.tfvars"))
    sql_path = effective(env_dir, "masking_functions.sql")
    functions = sql_library_block(sql_path.read_text() if sql_path.is_file() else "")
    masks = [p for p in cfg.get("fgac_policies") or [] if p.get("policy_type") == "POLICY_TYPE_COLUMN_MASK"]
    tags = {
        item["entity_name"]: item["tag_value"] for item in cfg.get("tag_assignments") or []
        if item.get("entity_type") == "columns" and item.get("tag_key") == TAG_KEY
    }
    undeterministic = sorted(v for v in tags.values() if not parse_treatment_value(v))
    if undeterministic:
        raise StageError(
            "generated config is not a deterministic generation (treatment values "
            f"{undeterministic[:3]}); run make generate or derive-assignments in deterministic mode"
        )
    env_cfg = _load(env_dir / "env.auto.tfvars")
    return {
        "fingerprint": model_fingerprint(cfg, functions),
        "fgac_policies": masks, "tags": tags, "functions_sql": functions,
        # What verification of this generation expects of each tier.
        "settings": {key: env_cfg[key] for key in SETTINGS_KEYS if key in env_cfg},
    }


def ledger_dir(env_dir: Path) -> Path:
    """The env's ledger: in the checkout, or under GENIERAILS_STATE_DIR when set (state_layout.record_path)."""
    from scripts.state_layout import record_path

    return record_path(Path(env_dir), Path("data_access") / LEDGER)


def write_json(path: Path, value: dict) -> Path:
    """Write ``value`` atomically and durably before returning.

    The migration's ``adopting`` record is the no-way-back marker.  Its caller
    must be able to start the first state move knowing both the file contents
    and the directory entry survived a host crash.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as handle:
        handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return path


def save_snapshot(env_dir: Path, generation: dict) -> Path:
    return write_json(ledger_dir(env_dir) / f"{generation['fingerprint']}.json", generation)


def load_snapshot(env_dir: Path, fingerprint: str) -> dict | None:
    if not fingerprint:
        return None
    path = ledger_dir(env_dir) / f"{fingerprint}.json"
    if not path.is_file():
        raise StageError(
            f"no snapshot of live generation {fingerprint} in {path.parent}; refusing to render "
            "a graph that could destroy its policies"
        )
    return json.loads(path.read_text())


def snapshot_intact(snapshot: dict | None, fingerprint: str) -> bool:
    """Whether a generation's snapshot still hashes to its fingerprint (not edited since it was recorded).

    A legacy generation hashes its masks and treatment tags
    (migration.legacy_fingerprint); a deterministic one its policies,
    treatment tags and function SQL (model_fingerprint).
    """
    from scripts import migration

    try:
        tags = dict(snapshot["tags"])
        if fingerprint.startswith(migration.LEGACY_PREFIX):
            return migration.legacy_fingerprint(snapshot["fgac_policies"], tags) == fingerprint
        recorded = {"fgac_policies": snapshot["fgac_policies"], "tag_assignments": [
            {"entity_type": "columns", "entity_name": column, "tag_key": TAG_KEY, "tag_value": value}
            for column, value in tags.items()]}
        return model_fingerprint(recorded, snapshot["functions_sql"]) == fingerprint
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def _statements(block: str) -> list[tuple[str, str, str]]:
    out, catalog, schema = [], "", ""
    for raw in (block or "").split(";\n"):
        stmt = raw.strip().rstrip(";")
        if stmt.upper().startswith("USE CATALOG "):
            catalog = stmt
        elif stmt.upper().startswith("USE SCHEMA "):
            schema = stmt
        elif stmt:
            out.append((catalog, schema, stmt))
    return out


def _join_statements(statements) -> str:
    out, current = [], None
    for catalog, schema, stmt in sorted(set(statements)):
        if (catalog, schema) != current:
            out.append(f"{catalog};\n{schema};")
            current = (catalog, schema)
        out.append(stmt + ";")
    return "\n\n".join(out) + ("\n" if out else "")


def _merge_functions(*blocks: str) -> str:
    """The union of several generations' function blocks, one statement each."""
    # CREATE OR REPLACE of identical text is idempotent.
    return _join_statements(s for block in blocks for s in _statements(block))


def function_name(stmt: str) -> str:
    match = re.match(r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+([\w.`]+)", stmt, re.IGNORECASE)
    return match.group(1).replace("`", "").lower() if match else ""


def policy_value(policy: dict) -> str:
    """The treatment value a deterministic mask policy matches ("" if none)."""
    match = _VALUE_IN_CONDITION.search(str(policy.get("match_condition") or ""))
    return match.group(1) if match else ""


def _principals(policy: dict) -> tuple[list[str], list[str]]:
    return list(policy.get("to_principals") or []), list(policy.get("except_principals") or [])


def tightened(live: dict, wanted: dict) -> dict:
    """``live`` covering everyone either ``live`` or ``wanted`` covers."""
    live_to, live_except = _principals(live)
    wanted_to, wanted_except = _principals(wanted)
    keep = set(wanted_except)
    return dict(live) | {
        "to_principals": list(dict.fromkeys(live_to + wanted_to)),
        "except_principals": [p for p in live_except if p in keep],
    }


def _covers(policy: dict, principal: str) -> bool:
    to, excepted = _principals(policy)
    if principal == OUTSIDER:
        return ALL_USERS in to
    return (ALL_USERS in to or principal in to) and principal not in excepted


def masks_by_principal(policies: list[dict], value: str, universe: set[str]) -> dict[str, list[str]]:
    """Which mask policies on ``value`` apply to each principal (direct names; no nesting)."""
    on_value = [p for p in policies if policy_value(p) == value]
    return {who: sorted(p["name"] for p in on_value if _covers(p, who)) for who in universe}


def _universe(*generations) -> set[str]:
    names = {OUTSIDER}
    for generation in generations:
        for policy in (generation or {}).get("fgac_policies") or []:
            to, excepted = _principals(policy)
            names.update(to + excepted)
    names.discard(ALL_USERS)
    names.discard(UNKNOWN)
    return names


def _tag_key(column: str) -> str:
    return f"columns|{column}|{TAG_KEY}"


def plan_stage(stage: str, desired: dict, old: dict | None, retire: dict | None,
               *, keep_new_tags: bool = False) -> dict:
    """What ``stage`` renders, what its plan may delete, and why it is unsafe (if it is)."""
    if stage not in APPLY_STAGES:
        raise StageError(f"unknown stage {stage!r}")
    old_tags = dict((old or {}).get("tags") or {})
    desired_tags = dict(desired["tags"])
    create_tags = dict(old_tags)
    if keep_new_tags:
        create_tags.update({c: v for c, v in desired_tags.items() if c not in old_tags})
    retained = [g for g in (old, retire) if g]
    desired_defs = {p["name"]: p for p in desired["fgac_policies"]}
    retained_defs: dict[str, dict] = {}
    for generation in reversed(retained):  # the active generation's definition wins
        retained_defs.update({p["name"]: p for p in generation["fgac_policies"]})
    retained_values = {policy_value(p) for p in retained_defs.values()}
    # A new policy on a value columns already carry changes who those columns
    # mask, so it is a tier move: it waits for tighten.
    deferred = {
        name for name, policy in desired_defs.items()
        if name not in retained_defs and policy_value(policy) in retained_values
        and policy_value(policy) in set(create_tags.values())
    }
    live = dict(desired_defs) | retained_defs
    allowed_policies: set[str] = set()
    allowed_tags: set[str] = set()

    if stage in ("create", "restore"):
        policies = {n: p for n, p in live.items() if n not in deferred}
        tags = create_tags
        if stage == "restore":
            allowed_policies = set(deferred)
            allowed_tags = {_tag_key(c) for c in desired_tags.keys() - create_tags.keys()}
    elif stage == "tighten":
        policies = dict(live)
        for name in desired_defs.keys() & retained_defs.keys():
            policies[name] = tightened(retained_defs[name], desired_defs[name])
        tags = create_tags
    elif stage == "abandon":
        policies = dict(retained_defs)
        tags = dict(old_tags)
        allowed_policies = desired_defs.keys() - retained_defs.keys()
    else:  # switch, retire
        kept_gens = [old] if stage == "retire" else retained
        in_use = set(desired_tags.values())
        policies = dict(desired_defs)
        for generation in kept_gens:
            for policy in (generation or {}).get("fgac_policies") or []:
                if policy["name"] not in policies and policy_value(policy) not in in_use:
                    policies[policy["name"]] = policy
        tags = desired_tags
        allowed_policies = set(retained_defs) - set(policies)
        if stage == "switch":
            # Only policies on values still in use leave here; obsolete ones wait for retire.
            allowed_policies = {n for n in allowed_policies if policy_value(retained_defs[n]) in in_use}
        allowed_tags = {_tag_key(c) for c in (old_tags.keys() | create_tags.keys()) - desired_tags.keys()}

    functions = _merge_functions(*(g["functions_sql"] for g in [desired, old, retire] if g))
    rendered = {"fgac_policies": [policies[name] for name in sorted(policies)], "tags": tags,
                "functions_sql": functions}
    return {
        "rendered": rendered,
        "allowed_policy_deletes": sorted(allowed_policies),
        "allowed_tag_deletes": sorted(allowed_tags),
        "problems": stage_problems(stage, rendered, desired, old),
    }


def render_stage(stage: str, desired: dict, old: dict | None, retire: dict | None,
                 *, keep_new_tags: bool = False) -> dict:
    """Policies, tags and functions the data_access layer holds for ``stage``."""
    return plan_stage(stage, desired, old, retire, keep_new_tags=keep_new_tags)["rendered"]


def has_tier_move(desired: dict, old: dict | None, retire: dict | None = None,
                  *, keep_new_tags: bool = False) -> bool:
    """Whether the change moves principals on a value columns keep (needs tighten)."""
    create = render_stage("create", desired, old, retire, keep_new_tags=keep_new_tags)
    tighten = render_stage("tighten", desired, old, retire, keep_new_tags=keep_new_tags)
    return create["fgac_policies"] != tighten["fgac_policies"]


def stage_problems(stage: str, rendered: dict, desired: dict, old: dict | None) -> list[str]:
    """Why a render would leave a governed column unmasked or doubly masked.

    Pure, on direct principal names (the disjointness test of the policy
    builder covers nested groups). Every tagged column's value must have a
    policy; every principal gets at most one mask, except, while tightening,
    principals whose mask differs between the old and desired generation;
    and tightening never uncovers anyone the old or desired generation masks.
    """
    problems = []
    policies = rendered["fgac_policies"]
    universe = _universe(rendered, desired, old)
    old_policies = (old or {}).get("fgac_policies") or []

    # A policy applies only on its catalog, so the same value in two catalogs is two separate checks.
    def on(catalog: str, items: list[dict]) -> list[dict]:
        return [p for p in items if p.get("catalog", catalog) == catalog]

    for column, value in sorted(rendered["tags"].items()):
        if not any(policy_value(p) == value for p in on(column.split(".")[0], policies)):
            problems.append(f"{column} would carry {value}, which no policy masks")
    for catalog, value in sorted({(c.split(".")[0], v) for c, v in rendered["tags"].items()}):
        now = masks_by_principal(on(catalog, policies), value, universe)
        before = masks_by_principal(on(catalog, old_policies), value, universe)
        after = masks_by_principal(on(catalog, desired["fgac_policies"]), value, universe)
        for who in sorted(universe):
            mover = bool(old) and before[who] != after[who]
            if len(now[who]) > 1 and not (stage == "tighten" and mover):
                problems.append(f"{who} would get {len(now[who])} masks on {value} ({', '.join(now[who])})")
            if stage == "tighten" and not now[who] and (before[who] or after[who]):
                problems.append(f"tightening would unmask {value} for {who}")
    return problems


def stage_config_dir(env_dir: Path) -> Path:
    return Path(env_dir) / STAGE_CONFIG


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def write_stage(layer_dir: Path, rendered: dict, out_dir: Path | None = None) -> Path:
    """Write a rendered stage, from the layer's split config, into ``out_dir``.

    ``out_dir`` defaults to the env's ignored stage dir (``stage_config_dir``);
    the split config in ``layer_dir`` is only read. Returns ``out_dir``.
    """
    from generate_abac import (
        _find_bracket_section, _render_fgac_policy_block, _render_tag_assignment_block,
        _replace_bracket_section,
    )

    out_dir = Path(out_dir) if out_dir else stage_config_dir(Path(layer_dir).parent)
    path = layer_dir / "abac.auto.tfvars"
    text = path.read_text()
    cfg = _load(path)
    assignments = [
        item for item in cfg.get("tag_assignments") or []
        if not (item.get("entity_type") == "columns" and item.get("tag_key") == TAG_KEY)
    ] + [
        {"entity_type": "columns", "entity_name": column, "tag_key": TAG_KEY, "tag_value": value}
        for column, value in sorted(rendered["tags"].items())
    ]
    policies = [
        p for p in cfg.get("fgac_policies") or [] if p.get("policy_type") != "POLICY_TYPE_COLUMN_MASK"
    ] + rendered["fgac_policies"]
    for section, items, render in (
        ("tag_assignments", assignments, _render_tag_assignment_block),
        ("fgac_policies", policies, _render_fgac_policy_block),
    ):
        if _find_bracket_section(text, section) is None:
            text = text.rstrip() + f"\n\n{section} = [\n]\n"
        text = _replace_bracket_section(text, section, [render(item) for item in items])
    sql_path = layer_dir / "masking_functions.sql"
    sql = sql_path.read_text() if sql_path.is_file() else ""
    _write_text(out_dir / "abac.auto.tfvars", text)
    _write_text(out_dir / "masking_functions.sql", replace_sql_library(sql, rendered["functions_sql"]))
    return out_dir


def split_assignment_problems(env_dir: Path) -> list[str]:
    """The non-treatment tag assignments the split data_access config and the effective config disagree on.

    write_stage takes those from the split (the last promote), while the
    live refresh records the effective config's; when they differ, the
    coverage check and the stage's plan judge different assignments.
    """
    from scripts.runtime_overlay import effective
    from scripts.state_layout import data_access_subdir

    def others(path: Path) -> set[str]:
        return {"|".join(str(item.get(f, "")) for f in ("entity_type", "entity_name", "tag_key", "tag_value"))
                for item in _load(path).get("tag_assignments") or []
                if not (item.get("entity_type") == "columns" and item.get("tag_key") == TAG_KEY)}

    split = others(Path(env_dir) / data_access_subdir() / "abac.auto.tfvars")
    generated = others(effective(Path(env_dir), "abac.auto.tfvars"))
    return ([f"{key} is in the generated config only" for key in sorted(generated - split)]
            + [f"{key} is in the split data_access config only" for key in sorted(split - generated)])


def plan_violations(plan: dict, stage: str, allowed_policy_deletes=frozenset(),
                    allowed_tag_deletes=frozenset()) -> list[str]:
    """Changes in a plan that the stage may not make.

    No stage replaces a policy, a treatment tag or the function drop. Deletes
    are allowed only for the policies and treatment tags the stage itself
    removes (see plan_stage), and business grants are created only once the
    new protection is switched in. ``abandon`` may only delete those policies:
    any other change (a treatment tag retagged since the restore, a policy's
    principals or match, a function, a grant, a create) is refused, apart from
    the enforcement wait that a policy delete restarts and a path-only re-key
    of the masking functions (``masking_rekey``).
    """
    return [text for _, text in classified_violations(plan, stage, allowed_policy_deletes, allowed_tag_deletes)]


def violations_heading(stage: str, violations: list[tuple[str, str]]) -> str:
    """The refusal's first line, naming what the plan does: creates as creates, destroys as destroys."""
    kinds = {kind for kind, _ in violations}
    parts = [text for kind, text in (
        ("create", "creates business access before the grant phase"),
        ("destroy", "destroys protected resources"),
        ("change", f"makes changes the {stage} phase may not"),
    ) if kind in kinds]
    return f"ERROR: the {stage} plan {' and '.join(parts) or 'is refused'}; nothing was applied:"


def classified_violations(plan: dict, stage: str, allowed_policy_deletes=frozenset(),
                          allowed_tag_deletes=frozenset()) -> list[tuple[str, str]]:
    """plan_violations, each as (kind, text): a business grant ``create``, a protected ``destroy``,
    or another ``change`` the stage may not make."""
    problems = []
    for change in plan.get("resource_changes") or []:
        actions = (change.get("change") or {}).get("actions") or []
        address = str(change.get("address") or "")
        rtype = str(change.get("type") or "")
        key = str(change.get("index") or "")
        if stage == "abandon" and set(actions) - {"no-op", "read"}:
            if actions == ["delete"] and rtype == "databricks_policy_info" and key in allowed_policy_deletes:
                continue
            if address.endswith(ENFORCEMENT_WAIT):
                continue
            if masking_rekey(change):  # the one-time move of the paths out of state
                continue
            problems.append(("destroy" if "delete" in actions and _is_protected(address, rtype) else "change",
                             f"{address}: {'/'.join(actions)} (abandon only deletes the unfinished change's policies)"))
            continue
        if ("create" in actions and rtype == "databricks_grant" and change.get("name") in BUSINESS_GRANTS
                and stage in ("create", "tighten", "restore", "abandon")):
            problems.append(("create", f"{address}: {'/'.join(actions)} (a business grant: access opens only in "
                                       "the grant phase, after the switch is verified)"))
        if stage == "grant" and rtype in ("databricks_policy_info", "databricks_entity_tag_assignment") \
                and set(actions) - {"no-op", "read"}:
            problems.append(("destroy" if "delete" in actions else "change",
                             f"{address}: {'/'.join(actions)} (the grant phase changes access only)"))
            continue
        if "delete" not in actions:
            continue
        if not _is_protected(address, rtype):
            continue
        pure_delete = "create" not in actions
        if pure_delete and rtype == "databricks_policy_info" and key in allowed_policy_deletes:
            continue
        if pure_delete and rtype == "databricks_entity_tag_assignment" and key in allowed_tag_deletes:
            continue
        problems.append(("destroy", f"{address}: {'/'.join(actions)}"))
    return problems


def _is_protected(address: str, rtype: str) -> bool:
    return rtype in PROTECTED_TYPES or any(address.endswith(t) for t in PROTECTED_TYPES if "." in t)


def _runner(env_name: str, layer_dir: Path, *args: str, capture: bool = False) -> str:
    """terraform_layer.sh on the layer, reading the stage's render (never the split config)."""
    from scripts.environment_lock import STAGE_TOKEN_ENV, stage_token

    result = subprocess.run(
        [str(RUNNER), "data_access", env_name, *args],
        env=os.environ | {"LAYER_ENV_DIR": str(layer_dir), STAGE_CONFIG_ENV: str(stage_config_dir(layer_dir.parent)),
                          # policy_stage and the rebind variables pass terraform_layer.sh only with this.
                          STAGE_TOKEN_ENV: stage_token(layer_dir.parent)},
        text=True, stdout=subprocess.PIPE if capture else None, check=False,
    )
    if result.returncode:
        raise StageError(f"terraform {args[0]} failed (exit {result.returncode})")
    return result.stdout or ""


def _rerun_coverage_gate(env_dir: Path, env_name: str, runner: str, stage: str = "",
                         old_fp: str = "", keep_new_tags: bool = False, *, rebind_fp: str = "",
                         opens_access: bool = False) -> None:
    """Re-judge the coverage check on the render, so its result matches what is applied."""
    command = [sys.executable, str(COVERAGE_GATE), "run", "--env-dir", str(env_dir),
               "--env-name", env_name, "--runner", runner, "--layer-config-dir", str(stage_config_dir(env_dir))]
    if stage:
        command += ["--stage", stage, "--old-fingerprint", old_fp] + (["--keep-new-tags"] if keep_new_tags else [])
        command += (["--rebind-fingerprint", rebind_fp] if rebind_fp else []) + (
            ["--opens-access"] if opens_access else [])
    result = subprocess.run(command, check=False)
    if result.returncode:
        raise StageError(f"the coverage check stopped the stage (exit {result.returncode})")


def stage_record_path(env_dir: Path) -> Path:
    return ledger_dir(env_dir) / STAGE_RECORD


def load_stage_record(env_dir: Path) -> dict | None:
    path = stage_record_path(env_dir)
    return json.loads(path.read_text()) if path.is_file() else None


PLAN_OUT_ENV = "GENIERAILS_POLICY_PLAN_OUT"
PLAN_BASIS_ENV = "GENIERAILS_POLICY_PLAN_BASIS"
SAVED_PLAN_ENV = "GENIERAILS_POLICY_SAVED_PLAN"


def sidecar_path(plan_file: Path | str) -> Path:
    plan_file = Path(plan_file)
    return plan_file.with_name(plan_file.name + ".json")


def _normal(value):
    return json.loads(json.dumps(value, sort_keys=True))


def plan_changes(plan: dict) -> bool:
    """Whether a Terraform plan changes any resource."""
    return any(set((c.get("change") or {}).get("actions") or []) - {"no-op", "read"}
               for c in plan.get("resource_changes") or [])


def planned_policy_deletes(plan: dict) -> list[str]:
    """The policies a plan deletes."""
    return sorted(str(c.get("index") or "") for c in plan.get("resource_changes") or []
                  if c.get("type") == "databricks_policy_info"
                  and "delete" in ((c.get("change") or {}).get("actions") or []))


def _plan_json(env_name: str, layer_dir: Path, stage: str, plan_file: Path, *extra: str) -> dict:
    _runner(env_name, layer_dir, "plan", "-input=false", f"-var=policy_stage={stage}", *extra, f"-out={plan_file}")
    shown = _runner(env_name, layer_dir, "show", "-json", str(plan_file), capture=True)
    return json.loads(next(line for line in reversed(shown.splitlines()) if line.startswith("{")))


def saved_plan_variables(env_dir: Path, env_name: str, plan_file: Path) -> dict:
    """The variables a saved data_access plan was made with (terraform show -json)."""
    shown = _runner(env_name, Path(env_dir) / "data_access", "show", "-json", str(plan_file), capture=True)
    plan = json.loads(next(line for line in reversed(shown.splitlines()) if line.startswith("{")))
    return {name: (value or {}).get("value") for name, value in (plan.get("variables") or {}).items()}


def load_sidecar(plan_file: Path | str) -> dict:
    path = sidecar_path(plan_file)
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise StageError(f"no record of the saved plan {plan_file} ({exc})") from exc


def _apply_saved(env_name: str, layer_dir: Path, stage: str, saved: Path, rendered: dict, expect: dict) -> None:
    """Apply a saved stage plan, refusing when the stage no longer renders what was planned."""
    side = load_sidecar(saved)
    for key, value in expect.items():
        if side.get(key) != value:
            raise StageError(f"the saved {stage} plan was made for {key} {side.get(key)!r}, now {value!r}; "
                             "run make release-plan again")
    if side.get("stage") != stage or _normal(side.get("rendered")) != _normal(rendered):
        raise StageError(f"the {stage} stage no longer renders what release-plan planned; "
                         "run make release-plan again")
    write_stage(layer_dir, rendered)
    if side.get("changes"):
        _runner(env_name, layer_dir, "apply-plan", str(saved))
    else:
        print(f"Policy stage {stage}: the saved plan changes nothing; not applied")


def run_hook(stage: str, environ: dict | None = None) -> int:
    environ = dict(os.environ if environ is None else environ)
    env_dir = Path(environ.get("GENIERAILS_ENV_DIR") or "")
    if not env_dir.is_dir():
        raise StageError("GENIERAILS_ENV_DIR must name the env directory")
    if stage == "drop-functions":
        return drop_functions(env_dir, environ)
    if stage == "grant":
        return grant(env_dir, environ)
    if stage in REBIND_STAGES:
        return rebind(env_dir, environ, stage)
    env_name = environ.get("GENIERAILS_ENV_NAME") or env_dir.name
    layer_dir = env_dir / "data_access"
    expected = environ.get("GENIERAILS_POLICY_FINGERPRINT", "")
    if stage == "abandon":
        if not expected:
            raise StageError("abandon needs GENIERAILS_POLICY_FINGERPRINT (the transition to drop)")
        desired = load_snapshot(env_dir, expected)
    else:
        desired = desired_generation(env_dir)
        if expected != desired["fingerprint"]:
            raise StageError(
                f"generated config is generation {desired['fingerprint']}, but the controller is "
                f"applying {expected or '<unset>'}; regenerate or restart the release"
            )
    old_fp = environ.get("GENIERAILS_OLD_POLICY_FINGERPRINT", "")
    old = load_snapshot(env_dir, old_fp)
    retire_fp = environ.get("GENIERAILS_RETIRE_FINGERPRINT", "")
    retire = load_snapshot(env_dir, retire_fp) if retire_fp not in {"", desired["fingerprint"], old_fp} else None
    if stage != "abandon":
        save_snapshot(env_dir, desired)
    keep_new_tags = environ.get("GENIERAILS_POLICY_KEEP_NEW_TAGS") == "1"
    staged = plan_stage(stage, desired, old, retire, keep_new_tags=keep_new_tags)
    if staged["problems"]:
        print(f"ERROR: the {stage} render would weaken a governed column; nothing was applied:",
              file=sys.stderr)
        for problem in staged["problems"]:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    record = {"stage": stage, "desired": desired["fingerprint"], "old": old_fp,
              "retire": retire["fingerprint"] if retire else "", "keep_new_tags": keep_new_tags,
              "rendered": staged["rendered"]}
    if environ.get(SAVED_PLAN_ENV):
        _apply_saved(env_name, layer_dir, stage, Path(environ[SAVED_PLAN_ENV]), staged["rendered"],
                     {"desired": desired["fingerprint"], "old": old_fp, "retire": record["retire"]})
        write_json(stage_record_path(env_dir), record | {
            "applied_at": datetime.now(timezone.utc).isoformat(timespec="seconds")})
        print(f"Policy stage {stage}: applied the saved plan of generation {desired['fingerprint']}")
        return 0
    write_stage(layer_dir, staged["rendered"])
    if environ.get("GENIERAILS_COVERAGE_GATE_RUNNER"):
        _rerun_coverage_gate(env_dir, env_name, environ["GENIERAILS_COVERAGE_GATE_RUNNER"], stage, old_fp,
                             keep_new_tags)
    plan_out = environ.get(PLAN_OUT_ENV)
    with tempfile.TemporaryDirectory() as tmp:
        plan_file = Path(plan_out) if plan_out else Path(tmp) / f"{stage}.tfplan"
        plan = _plan_json(env_name, layer_dir, stage, plan_file)
        violations = classified_violations(plan, stage, set(staged["allowed_policy_deletes"]),
                                           set(staged["allowed_tag_deletes"]))
        if violations:
            print(violations_heading(stage, violations), file=sys.stderr)
            for _, problem in violations:
                print(f"  - {problem}", file=sys.stderr)
            return 1
        for note in rekey_notes(plan):
            print(f"Policy stage {stage}: {note}")
        if plan_out:
            write_json(sidecar_path(plan_file), record | {"changes": plan_changes(plan),
                                                          "policy_deletes": planned_policy_deletes(plan)})
            print(f"Policy stage {stage}: planned generation {desired['fingerprint']} into {plan_file}"
                  + ("" if plan_changes(plan) else " (no changes)"))
            return 0
        _runner(env_name, layer_dir, "apply-plan", str(plan_file))
    write_json(stage_record_path(env_dir), record | {
        "applied_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })
    if stage == "restore":
        # What is live now: the previous generation, plus the change's masks on columns it newly governs.
        kept = sorted(c for c, v in staged["rendered"]["tags"].items() if (old or {}).get("tags", {}).get(c) != v)
        print(f"Policy stage restore: applied the previous protection ({old_fp or 'none'})"
              + (f"; {len(kept)} newly governed column(s) keep generation {desired['fingerprint']}'s masks "
                 "and treatment tags" if kept else ""))
        return 0
    print(f"Policy stage {stage}: applied generation {desired['fingerprint']}"
          + (f" (retained {old['fingerprint']})" if old else ""))
    return 0


def grant(env_dir: Path, environ: dict) -> int:
    """Re-apply the last verified render with new SELECT allowed; nothing else may change."""
    env_name = environ.get("GENIERAILS_ENV_NAME") or env_dir.name
    layer_dir = env_dir / "data_access"
    expected = environ.get("GENIERAILS_POLICY_FINGERPRINT", "")
    plan_out = environ.get(PLAN_OUT_ENV)
    basis = environ.get(PLAN_BASIS_ENV) if plan_out else ""
    record = load_sidecar(basis) if basis else load_stage_record(env_dir)
    if not record or record.get("desired") != expected or desired_generation(env_dir)["fingerprint"] != expected:
        raise StageError("grant: the last applied stage is not of the generation the controller is applying")
    if environ.get(SAVED_PLAN_ENV):
        _apply_saved(env_name, layer_dir, "grant", Path(environ[SAVED_PLAN_ENV]), record["rendered"],
                     {"desired": expected})
        print(f"Policy stage grant: access applied on generation {expected} (saved plan)")
        return 0
    write_stage(layer_dir, record["rendered"])
    if environ.get("GENIERAILS_COVERAGE_GATE_RUNNER"):
        # The render is the last stage's, so it is judged as that stage.
        _rerun_coverage_gate(env_dir, env_name, environ["GENIERAILS_COVERAGE_GATE_RUNNER"],
                             str(record.get("stage") or ""), str(record.get("old") or ""),
                             bool(record.get("keep_new_tags")), opens_access=True)
    with tempfile.TemporaryDirectory() as tmp:
        plan_file = Path(plan_out) if plan_out else Path(tmp) / "grant.tfplan"
        plan = _plan_json(env_name, layer_dir, "grant", plan_file)
        problems = plan_violations(plan, "grant")
        if problems:
            print("ERROR: the grant plan changes protection; nothing was applied:", file=sys.stderr)
            for problem in problems:
                print(f"  - {problem}", file=sys.stderr)
            return 1
        if plan_out:
            write_json(sidecar_path(plan_file), {"stage": "grant", "desired": expected,
                                                 "rendered": record["rendered"], "changes": plan_changes(plan)})
            print(f"Policy stage grant: planned into {plan_file}" + ("" if plan_changes(plan) else " (no changes)"))
            return 0
        _runner(env_name, layer_dir, "apply-plan", str(plan_file))
    print(f"Policy stage grant: access applied on generation {expected}")
    return 0


# -- rebind (make policy-rebind) ----------------------------------------------------
def _binding(policy: dict) -> dict:
    return {k: v for k, v in policy.items() if k not in REBIND_POLICY_FIELDS}


UNKNOWN = "unknown"  # how Unity Catalog shows a deleted principal in a policy


def principal_replacements(desired: dict, changed: dict, approved: set[str]) -> tuple[list[str], dict[str, str]]:
    """The recreated principals the generated config puts in place of the change's (old -> new), and why not.

    For each policy and each of ``TO``/``EXCEPT``, the generated config may
    swap exactly one principal of the change's generation for one
    ``approved`` principal (the env's tier groups, raw_exempt_principals and
    committed deployer) that the change never named, consistently across
    every policy, and the replaced principal may appear nowhere in the
    config. Nothing may be added or removed otherwise: adding an ``EXCEPT``
    or dropping a ``TO`` would loosen a mask.
    """
    problems, mapping = [], {}
    want = {p["name"]: p for p in desired["fgac_policies"]}
    have = {p["name"]: p for p in changed["fgac_policies"]}
    named = {x for p in changed["fgac_policies"] for f in REBIND_POLICY_FIELDS for x in p.get(f) or []}
    still = {x for p in desired["fgac_policies"] for f in REBIND_POLICY_FIELDS for x in p.get(f) or []}
    for name in sorted(want.keys() & have.keys()):
        for field in REBIND_POLICY_FIELDS:
            removed = sorted(set(have[name].get(field) or []) - set(want[name].get(field) or []))
            added = sorted(set(want[name].get(field) or []) - set(have[name].get(field) or []))
            if not removed and not added:
                continue
            if len(removed) != 1 or len(added) != 1:
                problems.append(f"policy {name} changes its {field} beyond replacing one recreated principal "
                                f"(removes {removed}, adds {added})")
                continue
            old, new = removed[0], added[0]
            if new not in approved or new in named or old in still or mapping.get(old, new) != new:
                problems.append(f"policy {name} replaces {old} with {new} in its {field}, which isn't a recreated "
                                "tier group, exempt principal or deployer of this env")
                continue
            mapping[old] = new
    if len(set(mapping.values())) != len(mapping):
        problems.append(f"two principals are replaced by the same one ({mapping})")
    return problems, mapping


def rebind_problems(desired: dict, changed: dict, retained: list[dict], approved: set[str] = frozenset()) -> list[str]:
    """Why the generated config isn't the unfinished change with only recreated principals replaced.

    ``changed`` is the unfinished change's generation (its snapshot). Its
    policies, conditions, functions and treatment values must be exactly the
    generated config's, and its principals may differ only by replacing
    deleted principals with recreated ones (principal_replacements). A
    principal the generated config replaced (a recreated deployer under a new
    ID) may not be named by a retained generation's policy: the rebind keeps
    those policies as their generation recorded them, so it can re-point them
    only at principals recreated under the same name.
    """
    problems = []
    want = {p["name"]: p for p in desired["fgac_policies"]}
    have = {p["name"]: p for p in changed["fgac_policies"]}
    for name in sorted(want.keys() ^ have.keys()):
        problems.append(f"policy {name} is {'added' if name in want else 'removed'} by the generated config")
    for name in sorted(want.keys() & have.keys()):
        a, b = _binding(want[name]), _binding(have[name])
        moved = sorted(k for k in a.keys() | b.keys() if a.get(k) != b.get(k))
        if moved:
            problems.append(f"policy {name} changes its {', '.join(moved)}")
    retagged = sorted(c for c in desired["tags"].keys() | changed["tags"].keys()
                      if desired["tags"].get(c) != changed["tags"].get(c))
    if retagged:
        problems.append(f"{len(retagged)} column(s) change their treatment ({', '.join(retagged[:5])})")
    if desired["functions_sql"] != changed["functions_sql"]:
        problems.append("the masking functions change")
    principal_problems, mapping = principal_replacements(desired, changed, set(approved))
    problems += principal_problems
    replaced = set(mapping)
    for name in want.keys() & have.keys():
        for field in REBIND_POLICY_FIELDS:
            replaced |= set(have[name].get(field) or []) - set(want[name].get(field) or [])
    for generation in retained:
        for policy in generation["fgac_policies"]:
            for who in sorted(replaced & {*(policy.get("to_principals") or []), *(policy.get("except_principals") or [])}):
                problems.append(f"policy {policy['name']} of generation {generation['fingerprint']} names {who}, "
                                "which the generated config replaced")
    return problems


def rebind_approved(env_dir: Path, desired: dict) -> set[str]:
    """The principals a rebind may put in place of deleted ones: tier groups, exempt principals, deployer."""
    settings = desired.get("settings") or {}
    deployer = str(_load(Path(env_dir) / "env.auto.tfvars").get("deployer_principal_id") or "")
    return set(settings.get("access_tier_groups") or []) | set(settings.get("raw_exempt_principals") or []) \
        | ({deployer} if deployer else set())


def restore_record_problems(record: dict | None, rendered: dict, changed_fp: str, old_fp: str) -> list[str]:
    """How a rebind render differs from the restore it re-applies, beyond principals."""
    if not record or record.get("stage") not in ("restore", "rebind") \
            or record.get("desired") != changed_fp or record.get("old") != old_fp:
        return [f"the last applied stage is {(record or {}).get('stage') or 'none'}, not the restore of this change"]
    problems = []
    before = {p["name"]: _binding(p) for p in record["rendered"]["fgac_policies"]}
    after = {p["name"]: _binding(p) for p in rendered["fgac_policies"]}
    for name in sorted(before.keys() ^ after.keys()):
        problems.append(f"policy {name} is {'added to' if name in after else 'removed from'} the restore render")
    problems += [f"policy {name} binds differently from the restore render"
                 for name in sorted(before.keys() & after.keys()) if before[name] != after[name]]
    if dict(record["rendered"]["tags"]) != dict(rendered["tags"]):
        problems.append("treatment values differ from the restore render")
    if record["rendered"]["functions_sql"] != rendered["functions_sql"]:
        problems.append("masking functions differ from the restore render")
    return problems


def mapped_principal_problems(recorded: list[dict], rendered: list[dict], mapping: dict[str, str]) -> list[str]:
    """Rendered policies whose principals aren't the verified restore's with the replacements applied."""
    before = {p["name"]: p for p in recorded}
    problems = []
    for policy in rendered:
        old = before.get(policy["name"])
        for field in REBIND_POLICY_FIELDS if old else ():
            want = sorted(mapping.get(x, x) for x in old.get(field) or [])
            if sorted(policy.get(field) or []) != want:
                problems.append(f"policy {policy['name']}'s {field} is {sorted(policy.get(field) or [])}, not the "
                                f"verified restore's with the recreated principals ({want})")
    return problems


def _plan_policy(values: dict | None) -> dict:
    values = values or {}
    conditions = [m.get("condition") for m in values.get("match_columns") or [] if isinstance(m, dict)]
    return {"to_principals": list(values.get("to_principals") or []),
            "except_principals": list(values.get("except_principals") or []),
            "match_condition": next((c for c in conditions if c), "")}


def coverage_loss_problems(plan: dict, recorded: list[dict], mapping: dict[str, str], principals: set[str]) -> list[str]:
    """How a protect plan would loosen a live mask.

    For every updated policy: a ``TO`` entry may leave only as a deleted
    principal (``unknown``) being replaced, and an ``EXCEPT`` entry may be
    added only for a deleted one and only if the verified restore excepted
    it (with the recreated principals). Then, per treatment value, everyone
    (the env's tier groups, exempt principals and deployer, and every
    principal named before or after) masked live before the apply must stay
    masked after it, unless the verified restore left them unmasked: the
    rebind may only put back what was verified before the deletion.
    """
    problems = []
    recorded_by_name = {p["name"]: p for p in recorded}
    before_all, after_all = [], []
    for change in plan.get("resource_changes") or []:
        if change.get("type") != "databricks_policy_info":
            continue
        body = change.get("change") or {}
        actions = body.get("actions") or []
        before, after = _plan_policy(body.get("before")), _plan_policy(body.get("after") or body.get("before"))
        after["match_condition"] = after["match_condition"] or before["match_condition"]
        if body.get("before"):
            before_all.append(before | {"name": str(change.get("index") or "")})
        if body.get("after") or "delete" not in actions:
            after_all.append(after | {"name": str(change.get("index") or "")})
        if actions != ["update"]:
            continue
        label = f"{change.get('address')}"
        gone_to = [x for x in before["to_principals"] if x not in after["to_principals"] and x != UNKNOWN]
        if gone_to:
            problems.append(f"{label} drops {gone_to} from TO (only a deleted principal may be replaced)")
        rec = recorded_by_name.get(str(change.get("index") or "")) or {}
        allowed = {mapping.get(x, x) for x in rec.get("except_principals") or []}
        added = [x for x in after["except_principals"] if x not in before["except_principals"]]
        if [x for x in added if x not in allowed] or len(added) > before["except_principals"].count(UNKNOWN):
            problems.append(f"{label} adds {added} to EXCEPT beyond the deleted principals it replaces "
                            f"(the verified restore excepted {sorted(allowed)})")
    mapped = [p | {f: [mapping.get(x, x) for x in p.get(f) or []] for f in REBIND_POLICY_FIELDS} for p in recorded]
    universe = set(principals) | {x for p in before_all + after_all + mapped for f in REBIND_POLICY_FIELDS
                                  for x in p.get(f) or []}
    universe = (universe - {UNKNOWN, ALL_USERS}) | {OUTSIDER}
    for value in sorted({policy_value(p) for p in before_all + after_all} - {""}):
        was = masks_by_principal(before_all, value, universe)
        now = masks_by_principal(after_all, value, universe)
        verified = masks_by_principal(mapped, value, universe)
        for who in sorted(universe):
            if was[who] and not now[who] and verified[who]:
                problems.append(f"the protect plan would unmask {value} for {who}, which the verified restore masked")
    return problems


def _values(values: dict | None, field: str) -> dict:
    value = (values or {}).get(field)
    value = value.get("value", value) if isinstance(value, dict) and "type" in value else value
    return value if isinstance(value, dict) else {}


def _trigger(values: dict | None, field: str):
    return _values(values, "triggers_replace").get(field)


def _path_only(before: dict, after: dict, kept: tuple[str, ...]) -> bool:
    """Every key in ``kept`` is set and unchanged, and every other change is a checkout path."""
    if not all(before.get(k) and before.get(k) == after.get(k) for k in kept):
        return False
    return all(before.get(k) == after.get(k) for k in before.keys() | after.keys() if k not in PATH_KEYS)


def masking_rekey(change: dict) -> bool:
    """Whether a planned change only moves checkout paths out of the masking resources' state.

    masking_functions once recorded sql_file, script and auth_file in its
    triggers, so every checkout planned a replace, and the first plan without
    them replaces it once more. A replace that keeps sql_hash, warehouse_id,
    host and client_id re-runs CREATE OR REPLACE of the same functions on the
    same warehouse, and (with no destroy-time provisioner on masking_functions:
    redeploy_drops_functions) drops nothing. masking_functions_drop's input
    moving from absolute to relative paths (and first recording sql_hash) is an
    in-place update that keeps the warehouse and host. Anything else, including any delete of
    masking_functions_drop (the destroy that drops the functions), is not.
    """
    address = str(change.get("address") or "")
    body = change.get("change") or {}
    actions = sorted(body.get("actions") or [])
    before, after = body.get("before"), body.get("after")
    if address.endswith(MASKING_FUNCTIONS) and actions == ["create", "delete"]:
        return (_path_only(_values(before, "triggers_replace"), _values(after, "triggers_replace"),
                           DEPLOY_TRIGGERS) and not redeploy_drops_functions())
    if address.endswith(FUNCTION_DROP) and actions == ["update"]:
        before, after = _values(before, "input"), _values(after, "input")
        if DROP_IDENTITY not in before:
            after = {k: v for k, v in after.items() if k != DROP_IDENTITY}
        return _path_only(before, after, DROP_TARGET)
    return False


def rekey_notes(plan: dict) -> list[str]:
    """The note for a plan that re-keys the masking functions (masking_rekey), else none."""
    return [REKEY_NOTE] if any(masking_rekey(c) and str(c.get("address") or "").endswith(MASKING_FUNCTIONS)
                               for c in plan.get("resource_changes") or []) else []


def redeploy_drops_functions(module_main: Path = SHARED / "modules" / "data_access" / "main.tf") -> bool:
    """Whether replacing terraform_data.masking_functions runs a drop (a destroy-time provisioner).

    Terraform takes destroy provisioners from the current config, so this
    reads the module the plan was made with (unreadable counts as a drop).
    Since the drop moved to masking_functions_drop, a replace only re-runs
    CREATE OR REPLACE.
    """
    try:
        text = Path(module_main).read_text()
    except OSError:
        return True
    match = re.search(r'resource\s+"terraform_data"\s+"masking_functions"\s*\{', text)
    if not match:
        return True
    depth, end = 0, match.end() - 1
    for end in range(match.end() - 1, len(text)):
        depth += {"{": 1, "}": -1}.get(text[end], 0)
        if depth == 0:
            break
    block = re.sub(r"#[^\n]*", "", text[match.end():end])
    return bool(re.search(r"\bwhen\s*=\s*destroy\b", block)) or depth != 0


def _policy_binding_changes(change: dict) -> list[str]:
    before, after = (change.get("change") or {}).get("before") or {}, (change.get("change") or {}).get("after") or {}
    return sorted(f for f in POLICY_BINDING if f in before or f in after if before.get(f) != after.get(f))


def held_grants(plan: dict) -> tuple[list[str], list[str]]:
    """The grants a plan would create, as held_access_grants entries, and any other grant change."""
    held, problems = [], []
    for change in plan.get("resource_changes") or []:
        actions = (change.get("change") or {}).get("actions") or []
        if change.get("type") != "databricks_grant" or not set(actions) - {"no-op", "read"}:
            continue
        if actions == ["create"] and change.get("name") in ACCESS_GRANTS:
            held.append(f"{change['name']}|{change.get('index') or ''}")
        else:
            problems.append(f"{change.get('address')}: {'/'.join(actions)} (a rebind only creates grants again)")
    return sorted(held), problems


def rebind_violations(plan: dict, stage: str, allowed_grants=frozenset(), *, drops=None,
                      coverage: dict | None = None) -> list[str]:
    """Changes a rebind phase's plan may not make.

    ``rebind-protect`` may only update a mask policy's ``TO``/``EXCEPT``
    principals, redeploy the masking functions and restart the enforcement
    wait. The redeploy (``masking_functions`` replaced) may not change a
    function's SQL (``sql_hash``) or run a drop: ``masking_functions_drop``
    is only updated in place, and ``masking_functions`` has no destroy-time
    provisioner (``redeploy_drops_functions``), so it re-runs CREATE OR
    REPLACE of the same definitions and no function a live policy uses is
    dropped. Its principal updates may not loosen a live mask
    (coverage_loss_problems, with ``coverage``: the verified restore's
    policies, the principal replacements and the env's principals).
    ``rebind-access`` may only create the grants in ``allowed_grants``
    (``<resource>|<key>``: exactly what the protect phase held back, as the
    controller validated it against the saved protect plans). Anything else
    refuses: a treatment tag change, a policy create or delete, a policy's
    condition or function, another resource.
    """
    problems = []
    for change in plan.get("resource_changes") or []:
        actions = (change.get("change") or {}).get("actions") or []
        address = str(change.get("address") or "")
        rtype = str(change.get("type") or "")
        if not set(actions) - {"no-op", "read"}:
            continue
        label = f"{address}: {'/'.join(actions)}"
        if stage == "rebind-access":
            if rtype == "databricks_grant" and change.get("name") in ACCESS_GRANTS and actions == ["create"] \
                    and f"{change.get('name')}|{change.get('index') or ''}" in allowed_grants:
                continue
            problems.append(f"{label} (the access phase only creates the grants the deleted principals lost)")
            continue
        if rtype == "databricks_policy_info" and actions == ["update"]:
            moved = _policy_binding_changes(change)
            if moved:
                problems.append(f"{label} changes its {', '.join(moved)} (a rebind only re-points principals)")
            continue
        if address.endswith(MASKING_FUNCTIONS) and sorted(actions) in (["update"], ["create", "delete"]):
            body = (change.get("change") or {})
            before, after = _trigger(body.get("before"), "sql_hash"), _trigger(body.get("after"), "sql_hash")
            if not before or before != after:
                problems.append(f"{label} changes the function SQL (sql_hash {before or 'unknown'} -> "
                                f"{after or 'unknown'}); a rebind only redeploys the same functions")
            elif "delete" in actions and (drops or redeploy_drops_functions)():
                problems.append(f"{label}: replacing it runs a destroy-time drop of the masking functions "
                                "that live policies use")
            continue
        if address.endswith(FUNCTION_DROP) and actions == ["update"]:
            continue
        if address.endswith(ENFORCEMENT_WAIT) and actions != ["delete"]:
            continue
        problems.append(f"{label} (the protect phase only re-points policies, redeploys the functions and "
                        "restarts the enforcement wait)")
    if stage == "rebind-protect":
        if coverage is None:
            problems.append("the protect plan can't be checked for loosened masks (no verified restore given)")
        else:
            problems += coverage_loss_problems(plan, coverage["recorded"], coverage["mapping"], coverage["principals"])
    return problems


def planned_summary(plan: dict) -> list[str]:
    """``<actions> <address>`` for every resource a plan changes."""
    return sorted(f"{'/'.join((c.get('change') or {}).get('actions') or [])} {c.get('address')}"
                  for c in plan.get("resource_changes") or []
                  if set((c.get("change") or {}).get("actions") or []) - {"no-op", "read"})


def rebind(env_dir: Path, environ: dict, stage: str) -> int:
    """Plan (PLAN_OUT) or apply exactly (SAVED_PLAN) one phase of make policy-rebind."""
    from scripts.staged_policy_apply import _in_ci

    if _in_ci(environ, opt_out=False):
        raise StageError(f"{stage} can't run in CI (GENIERAILS_ALLOW_CI_APPLY doesn't apply to policy-rebind)")
    env_name = environ.get("GENIERAILS_ENV_NAME") or env_dir.name
    layer_dir = env_dir / "data_access"
    changed_fp = environ.get("GENIERAILS_POLICY_FINGERPRINT", "")
    old_fp = environ.get("GENIERAILS_OLD_POLICY_FINGERPRINT", "")
    retire_fp = environ.get("GENIERAILS_RETIRE_FINGERPRINT", "")
    if not changed_fp:
        raise StageError(f"{stage} needs GENIERAILS_POLICY_FINGERPRINT (the unfinished change)")
    desired = desired_generation(env_dir)
    if environ.get("GENIERAILS_REBIND_FINGERPRINT") != desired["fingerprint"]:
        raise StageError(f"generated config is generation {desired['fingerprint']}, but policy-rebind is "
                         f"re-pointing to {environ.get('GENIERAILS_REBIND_FINGERPRINT') or '<unset>'}")
    changed, old = load_snapshot(env_dir, changed_fp), load_snapshot(env_dir, old_fp)
    retire = load_snapshot(env_dir, retire_fp) if retire_fp not in {"", changed_fp, old_fp} else None
    keep_new_tags = environ.get("GENIERAILS_POLICY_KEEP_NEW_TAGS") == "1"
    # The restore render, with the generated config's principals for the change's own policies.
    staged = plan_stage("restore", desired, old, retire, keep_new_tags=keep_new_tags)
    rendered = staged["rendered"]
    approved = rebind_approved(env_dir, desired)
    problems = rebind_problems(desired, changed, [g for g in (old, retire) if g], approved) + staged["problems"]
    mapping = principal_replacements(desired, changed, approved)[1]
    record = load_stage_record(env_dir)
    off = sorted(c for c in rendered["tags"].keys() | ((old or {}).get("tags") or {}).keys()
                 if rendered["tags"].get(c) != ((old or {}).get("tags") or {}).get(c))
    if off:
        problems.append(f"{len(off)} column(s) would not be on the previous generation ({', '.join(off[:5])})")
    problems += restore_record_problems(record, rendered, changed_fp, old_fp)
    if not problems:
        problems += mapped_principal_problems(record["rendered"]["fgac_policies"], rendered["fgac_policies"], mapping)
    if problems:
        print(f"ERROR: {stage}: the generated config isn't the unfinished change with only its principals moved; "
              "nothing was applied:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    expect = {"desired": changed_fp, "old": old_fp, "retire": retire["fingerprint"] if retire else "",
              "rebind": desired["fingerprint"]}
    if environ.get(SAVED_PLAN_ENV):
        _apply_saved(env_name, layer_dir, stage, Path(environ[SAVED_PLAN_ENV]), rendered, expect)
        write_json(stage_record_path(env_dir), expect | {
            "stage": "rebind", "phase": stage, "keep_new_tags": keep_new_tags, "rendered": rendered,
            "applied_at": datetime.now(timezone.utc).isoformat(timespec="seconds")})
        print(f"Policy stage {stage}: applied the saved plan (generation {changed_fp}, principals of "
              f"{desired['fingerprint']})")
        return 0
    plan_out = environ.get(PLAN_OUT_ENV)
    if not plan_out:
        raise StageError(f"{stage} only plans (GENIERAILS_POLICY_PLAN_OUT) or applies a saved plan "
                         "(GENIERAILS_POLICY_SAVED_PLAN)")
    write_stage(layer_dir, rendered)
    if environ.get("GENIERAILS_COVERAGE_GATE_RUNNER"):
        _rerun_coverage_gate(env_dir, env_name, environ["GENIERAILS_COVERAGE_GATE_RUNNER"], "restore", old_fp,
                             keep_new_tags, rebind_fp=desired["fingerprint"])
    plan_file = Path(plan_out)
    table_grants = sorted(json.loads(environ.get("GENIERAILS_REBIND_TABLE_GRANTS") or "[]"))
    allowed = set(json.loads(environ.get("GENIERAILS_REBIND_ALLOWED_GRANTS") or "[]"))
    kept = f"-var=rebind_table_grants={json.dumps(table_grants)}"
    held: list[str] = []
    problems = []
    if stage == "rebind-protect":
        # Terraform's -target pulls in dependencies (every policy depends on the grants), so the
        # protect phase holds back exactly the grants a plan of the whole render would create.
        with tempfile.TemporaryDirectory() as tmp:
            held, problems = held_grants(_plan_json(env_name, layer_dir, "rebind", Path(tmp) / "probe.tfplan", kept))
    if not problems:
        plan = _plan_json(env_name, layer_dir, "rebind", plan_file, kept,
                          f"-var=held_access_grants={json.dumps(held)}")
        problems = rebind_violations(plan, stage, allowed, coverage={
            "recorded": record["rendered"]["fgac_policies"], "mapping": mapping, "principals": approved})
    if problems:
        print(f"ERROR: the {stage} plan makes changes a rebind may not; nothing was applied:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    summary = planned_summary(plan)
    write_json(sidecar_path(plan_file), expect | {
        "stage": stage, "keep_new_tags": keep_new_tags, "rendered": rendered, "held": held,
        "table_grants": table_grants, "changes": plan_changes(plan), "summary": summary})
    print(f"Policy stage {stage}: planned into {plan_file}" + ("" if summary else " (no changes)"))
    for line in summary:
        print(f"  {line}")
    for note in rekey_notes(plan):
        print(f"Policy stage {stage}: {note}")
    if held:
        print(f"  held back until the access phase: {len(held)} grant(s)")
    return 0


def functions_to_drop(desired: dict, old: dict | None, retire: dict) -> str:
    """SQL (with its USE context) of the functions only ``retire`` defines."""
    kept = {function_name(stmt) for g in (desired, old) if g for _, _, stmt in _statements(g["functions_sql"])}
    return _join_statements(s for s in _statements(retire["functions_sql"]) if function_name(s[2]) not in kept)


def _live_function_users(env_dir: Path, catalogs: set[str]) -> set[str]:
    """Function names (lowercase, unqualified) live column-mask policies on ``catalogs`` call."""
    from scripts.audit_schema_drift import _get_sdk_client

    w = _get_sdk_client(env_dir / "data_access")
    used = set()
    for catalog in sorted(catalogs):
        for policy in w.policies.list_policies("CATALOG", catalog):
            mask = getattr(policy, "column_mask", None)
            name = str(getattr(mask, "function_name", "") or "") if mask else ""
            if name:
                used.add(name.rsplit(".", 1)[-1].lower())
    return used


def _resolve_warehouse(env_dir: Path, environ: dict) -> str:
    """The warehouse verify-access uses (explicit, configured, or the applied one)."""
    result = subprocess.run(
        [sys.executable, str(SHARED / "scripts" / "resolve_env_config.py"), "warehouse", "--env-dir", str(env_dir),
         "--terraform-runner", str(RUNNER), "--env-name", environ.get("GENIERAILS_ENV_NAME") or env_dir.name],
        text=True, capture_output=True, check=False,
    )
    if result.returncode or not result.stdout.strip():
        raise StageError("drop-functions: no SQL warehouse resolved; set WAREHOUSE_ID or sql_warehouse_id")
    return result.stdout.strip().splitlines()[-1]


def masking_sql_hash(sql: str) -> str:
    """The normalized hash masking_functions_drop records for this SQL (normalize_masking_sql.py)."""
    import hashlib

    from modules.data_access.normalize_masking_sql import normalized_definitions
    return hashlib.sha256(normalized_definitions(sql).encode()).hexdigest()


def drop_functions(env_dir: Path, environ: dict, *, live_users=None, run=subprocess.run) -> int:
    """Drop the retired generation's functions once no live policy calls them."""
    desired = desired_generation(env_dir)
    if environ.get("GENIERAILS_POLICY_FINGERPRINT", "") != desired["fingerprint"]:
        raise StageError("drop-functions: the generated config changed since the controller started")
    retire = load_snapshot(env_dir, environ.get("GENIERAILS_RETIRE_FINGERPRINT", ""))
    if retire is None:
        print("Policy stage drop-functions: no retired generation; nothing to drop")
        return 0
    old = load_snapshot(env_dir, environ.get("GENIERAILS_OLD_POLICY_FINGERPRINT", ""))
    sql = functions_to_drop(desired, old, retire)
    names = {function_name(stmt).rsplit(".", 1)[-1] for _, _, stmt in _statements(sql)}
    if environ.get(PLAN_OUT_ENV):
        # Its plan is the DROP statements it would run (no Terraform here).
        plan_file = Path(environ[PLAN_OUT_ENV])
        plan_file.write_text(sql)
        write_json(sidecar_path(plan_file), {"stage": "drop-functions", "desired": desired["fingerprint"],
                                             "rendered": {"sql": sql}, "changes": bool(names)})
        print(f"Policy stage drop-functions: planned {len(names)} function drop(s) into {plan_file}")
        return 0
    if environ.get(SAVED_PLAN_ENV):
        saved = Path(environ[SAVED_PLAN_ENV])
        if saved.read_text() != sql or load_sidecar(saved).get("desired") != desired["fingerprint"]:
            raise StageError("drop-functions: the functions to drop changed since release-plan; "
                             "run make release-plan again")
    if not names:
        print("Policy stage drop-functions: every retired function is still used; nothing to drop")
        return 0
    catalogs = {p["catalog"] for g in (desired, old, retire) if g for p in g["fgac_policies"]}
    in_use = names & (live_users or _live_function_users)(env_dir, catalogs)
    if in_use:
        print("ERROR: live policies still call " + ", ".join(sorted(in_use)) + "; nothing was dropped",
              file=sys.stderr)
        return 1
    layer = env_dir / "data_access"
    auth = _load(layer / "auth.auto.tfvars")
    warehouse = environ.get("GENIERAILS_WAREHOUSE_ID", "") or _resolve_warehouse(env_dir, environ)
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as handle:
        handle.write(sql)
        sql_file = handle.name
    try:
        # The deployer drops only SQL whose identity it is told: this is the
        # hash of what the generations above say to drop.
        result = run([sys.executable, str(DEPLOY_FUNCTIONS), "--sql-file", sql_file, "--warehouse-id", warehouse,
                      "--auth-file", str(layer / "auth.auto.tfvars"),
                      "--host", str(auth.get("databricks_workspace_host") or ""), "--drop",
                      "--expect-sql-hash", masking_sql_hash(sql)], check=False)
    finally:
        os.unlink(sql_file)
    if result.returncode:
        print(f"ERROR: dropping retired functions failed (exit {result.returncode})", file=sys.stderr)
        return 1
    print(f"Policy stage drop-functions: dropped {len(names)} function(s) of generation {retire['fingerprint']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    fp = sub.add_parser("fingerprint", help="print the generated config's generation fingerprint")
    fp.add_argument("--env-dir", type=Path, required=True)
    hook = sub.add_parser("hook", help="run one staged-apply hook (environment-driven)")
    hook.add_argument("stage", choices=STAGES)
    args = parser.parse_args(argv)
    try:
        if args.command == "fingerprint":
            print(desired_generation(args.env_dir)["fingerprint"])
            return 0
        return run_hook(args.stage)
    except (StageError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
