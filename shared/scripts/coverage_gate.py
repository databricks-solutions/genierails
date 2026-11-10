#!/usr/bin/env python3
"""Coverage check for the data_access layer, enforced by Terraform.

`run` gates the split data_access config exactly as Terraform will apply it:

  1. Ask Terraform (terraform console, same var files and -var flags as the
     apply) for the gate inputs: their fingerprint and the tables it would
     grant.
  2. Read the data_access state for the tables already granted. Tables about
     to be granted for the first time get the first-exposure check: an
     untagged sensitive-looking column blocks unless it is acknowledged in
     coverage_acknowledged_columns. A missing state means nothing is granted
     yet, so every table is checked.
  3. Run validate_abac.py --coverage-gate on the data_access config.
  4. Check the live-refresh record derive-assignments writes after it re-read
     Unity Catalog (generated/.live_refresh.json): it must match the DDL and
     the tags being gated, else the gate fails. Its time becomes the result's
     refreshed_at. One exception, for a staged phase only (``--stage``): the
     phases before the switch (create, tighten, restore, abandon) keep the
     active generation's treatment values live, while the refresh recorded
     the desired values the switch applies. There the tags must be exactly
     the desired config the refresh recorded, with each treatment tag at the
     value the active generation recorded (staged_kept_assignments): an
     adopted legacy migration's (staged_migration_assignments), or a
     deterministic generation's (staged_generation_assignments). Without a
     policy journal moving from that generation and a snapshot that matches
     its fingerprint (and, for a legacy one, an adopted migration record), the
     mismatch fails as before. The result records the generation kept
     (kept_from) and the governed columns the phase leaves untagged. A first
     deploy keeps nothing (kept_from empty), so its create render, which tags
     no column yet, is validated against the treatment tags its switch
     applies (pending_treatments) rather than as a vacuous pass.
  5. Ask Terraform for the fingerprint again and write the result to
     envs/<env>/$DATA_ACCESS_SUBDIR/.coverage_gate.json (data_access by default).

modules/data_access plans business SELECT only while that file records a pass
for the fingerprint Terraform computes at plan time AND a live refresh no
older than coverage_gate_max_age. So a raw terraform or terraform_layer.sh run
can't grant with a missing, failed, stale or old gate. Terraform can't re-read
live UC itself: it can only verify that a recent refreshed pass exists for the
current local inputs. This catches drift and
skipped steps; it is not a defence against someone who hand-forges the files.

`needs-derive` prints which live refresh make must run before a plan/apply:
"full" (derive-assignments: live class.* tags and DDL), "ddl" (live DDL only,
for envs whose tags come from make generate), or "none" when the env has no
governance config to grant through.

Business access has no on/off switch: the retired business_access_enabled
variable is ignored wherever it is set (env.auto.tfvars or -var flags).
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import signal
import shlex
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import hcl2

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))
RUNNER = SHARED_ROOT / "scripts" / "terraform_layer.sh"
VALIDATOR = SHARED_ROOT / "validate_abac.py"
GATE_FILENAME = ".coverage_gate.json"
CAN_RUN_FILENAME = ".can_run_check.json"
AGENT_COVERAGE_FILENAME = ".agent_coverage.json"  # scripts/agent_coverage.py
ACL_RESOURCES = ("genie_space_acls", "genie_space_acls_created")
REFRESH_RELPATH = Path("generated") / ".live_refresh.json"
REFRESH_VERSION = 1
GATE_VERSION = 1
# Longer than terraform_layer.sh's default 600s init-lock wait, so lock
# contention reports the lock-specific timeout rather than a console timeout.
SUBPROCESS_TIMEOUT = os.environ.get("COVERAGE_GATE_TIMEOUT_SECONDS", "660")
TABLE_GRANT = ("module.data_access", "databricks_grant", "table_access")
# Set by scripts/policy_generations.py: a staged phase's render, which
# terraform_layer.sh reads instead of the split data_access config.
LAYER_CONFIG_ENV = "GENIERAILS_LAYER_CONFIG_DIR"
INPUTS_EXPRESSION = "base64encode(jsonencode(module.data_access.coverage_gate_inputs))"


class GateError(Exception):
    """The gate can't establish its inputs; callers treat this as a failure."""


class CommandTimeout(GateError):
    """A coverage subprocess exceeded its bounded runtime."""


def _run_bounded(command: list[str], *, label: str, **kwargs) -> subprocess.CompletedProcess:
    """Run a child in its own process group and kill the whole group on timeout."""
    try:
        timeout = float(SUBPROCESS_TIMEOUT)
        if timeout <= 0 or not math.isfinite(timeout):
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise GateError(
            "COVERAGE_GATE_TIMEOUT_SECONDS must be a positive number "
            f"(got {SUBPROCESS_TIMEOUT!r})"
        ) from exc
    input_value = kwargs.pop("input", None)
    if input_value is not None:
        kwargs["stdin"] = subprocess.PIPE
    previous_sigterm = signal.getsignal(signal.SIGTERM)

    def _raise_on_sigterm(_signum, _frame):
        raise SystemExit(143)

    signal.signal(signal.SIGTERM, _raise_on_sigterm)
    try:
        try:
            process = subprocess.Popen(command, start_new_session=True, **kwargs)
            stdout, stderr = process.communicate(input=input_value, timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            stdout, stderr = process.communicate()
            raise CommandTimeout(
                f"{label} timed out after {timeout:g}s; its process group was killed "
                "and the coverage result was invalidated"
            )
        except BaseException:
            if "process" in locals():
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
            raise
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)


def data_access_subdir() -> str:
    """The data_access layer's directory under envs/<env>/: Makefile.shared's
    ``DATA_ACCESS_SUBDIR`` (exported into recipes). The Terraform layer is
    still named ``data_access``."""
    return os.environ.get("DATA_ACCESS_SUBDIR") or "data_access"


def _load_tfvars(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        return hcl2.loads(path.read_text())
    except Exception as exc:
        if path.name != "auth.auto.tfvars":
            raise GateError(f"cannot parse {path}: {exc}") from exc
    # Raised outside the handler with no parser text, which can echo a
    # malformed secret (also via __context__).
    raise GateError(f"{path} is not valid HCL") from None


RETIRED_FLAG = "business_access_enabled"


def retired_flag_sources(env_file: Path, apply_flags: str, environ: dict) -> list[str]:
    """Where the retired business_access_enabled is still set: the env file,
    a -var in APPLY_FLAGS (either form), or TF_VAR_business_access_enabled.
    Never raises: the deprecation warning must not fail a run."""
    sources = []
    try:
        if RETIRED_FLAG in _load_tfvars(env_file):
            sources.append(str(env_file))
    except GateError:
        pass
    try:
        args = shlex.split(apply_flags or "")
    except ValueError:
        args = (apply_flags or "").split()
    for index, arg in enumerate(args):
        assignment = arg[len("-var="):] if arg.startswith("-var=") else (
            args[index + 1] if arg == "-var" and index + 1 < len(args) else "")
        if assignment.split("=", 1)[0].strip() == RETIRED_FLAG:
            sources.append("APPLY_FLAGS")
            break
    if f"TF_VAR_{RETIRED_FLAG}" in environ:
        sources.append(f"TF_VAR_{RETIRED_FLAG}")
    return sources


def console_flags(apply_flags: str) -> list[str]:
    """Keep only the variable flags terraform console accepts."""
    args = shlex.split(apply_flags or "")
    kept: list[str] = []
    index = 0
    while index < len(args):
        arg = args[index]
        if arg.startswith(("-var=", "-var-file=")):
            kept.append(arg)
        elif arg in ("-var", "-var-file") and index + 1 < len(args):
            kept += [arg, args[index + 1]]
            index += 1
        index += 1
    return kept


def needs_derive(env_dir: Path, apply_flags: str) -> tuple[str, str | None]:
    """Which live refresh must precede a plan/apply: full, ddl or none.

    apply_flags is accepted for older callers and ignored: no flag changes
    whether business access is gated.
    """
    env = _load_tfvars(env_dir / "env.auto.tfvars")
    generated = (env_dir / "generated" / "abac.auto.tfvars").is_file()
    if not generated and not (env_dir / data_access_subdir() / "abac.auto.tfvars").is_file():
        return "none", None
    if env.get("enable_classification") is not True:
        return "ddl", "enable_classification is false; tags come from make generate, so only the DDL is re-read"
    if not generated:
        return "ddl", "no generated/abac.auto.tfvars to derive tags into, so only the DDL is re-read"
    return "full", "business access requires a coverage check of live tags in a native-classification env"


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""


def deployment_binding(source: dict | Path) -> str:
    """Match roots/data_access's binding from tfvars or already-loaded vars."""
    if isinstance(source, Path):
        variables = {}
        for path in (source / "auth.auto.tfvars", source / "env.auto.tfvars"):
            variables.update(_load_tfvars(path))
    else:
        variables = source
    host = str(variables.get("databricks_workspace_host", "")).strip().lower()
    if host.endswith("/"):
        host = host[:-1]
    binding_json = json.dumps({
        "workspace_host": host,
        "workspace_id": str(variables.get("databricks_workspace_id", "")).strip(),
    }, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    # Terraform jsonencode escapes these HTML-sensitive characters.
    binding_json = binding_json.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    return hashlib.sha256(binding_json.encode()).hexdigest()


def tag_assignments_digest(assignments: list) -> str:
    """Order-independent digest of tag assignments (generated vs. split config)."""
    keys = sorted(
        "|".join(str(item.get(field, "")) for field in ("entity_type", "entity_name", "tag_key", "tag_value"))
        for item in assignments or []
    )
    return hashlib.sha256("\n".join(keys).encode()).hexdigest()


def write_refresh_record(path: Path, *, mode: str, ddl_path: Path, config_path: Path | None,
                         column_classes: dict | None = None) -> None:
    """Record a successful live refresh (written by derive_assignments.py).

    ``column_classes`` (a deterministic env's live class.* tags per column)
    lets the coverage check judge each column from its classes, not from
    what the config says about them.
    """
    record = {
        "version": REFRESH_VERSION,
        "mode": mode,
        "refreshed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ddl_sha256": file_sha256(ddl_path),
    }
    if config_path is not None:
        record["tag_assignments_sha256"] = tag_assignments_digest(
            _load_tfvars(config_path).get("tag_assignments") or []
        )
    if column_classes:
        record["column_classes"] = {column: sorted(classes) for column, classes in sorted(column_classes.items())}
    write_result(path, record)


def refresh_column_classes(env_dir: Path) -> dict:
    """The live class.* tags per column the last live refresh recorded ({} if none)."""
    try:
        classes = json.loads((env_dir / REFRESH_RELPATH).read_text()).get("column_classes") or {}
    except (OSError, ValueError, AttributeError):
        return {}  # _live_refresh fails the gate on a missing or unreadable record
    return classes if isinstance(classes, dict) else {}


def staged_migration_assignments(env_dir: Path, stage: str, old_fingerprint: str,
                                 keep_new_tags: bool = False) -> tuple[list, list]:
    """(desired, staged) tag assignments of a phase that keeps a legacy migration's values.

    ``desired`` is the effective generated config's; ``staged`` is what the
    ``stage`` render holds: the same, with every treatment tag at the value
    the adopted legacy generation recorded (and, after an attempted switch,
    newly governed columns at their desired value), as
    policy_generations.plan_stage renders it. Raises GateError, failing
    closed, unless the stage keeps the old values and the migration is on
    record: an adopted migration.json naming ``old_fingerprint``, a policy
    journal whose active generation and transition are that generation, and
    a snapshot whose content still hashes to it.
    """
    from governance_policies import TAG_KEY
    from scripts import migration, policy_generations as pg
    from scripts import staged_policy_apply as spa
    from scripts.runtime_overlay import effective

    if stage not in pg.KEEP_TAG_STAGES:
        raise GateError(f"the {stage} phase applies the desired values")
    if not old_fingerprint.startswith(migration.LEGACY_PREFIX):
        raise GateError(f"the active generation {old_fingerprint or '<none>'} is not a legacy migration's")
    try:
        record = migration.load_record(env_dir)
        journal = spa.load_journal(env_dir)
        snapshot = pg.load_snapshot(env_dir, old_fingerprint)
        target = pg.desired_generation(env_dir)["fingerprint"]
    except (migration.Refused, spa.Refused, pg.StageError, OSError, ValueError) as exc:
        raise GateError(str(exc)) from exc
    if not migration.adopted(record) or record.get("legacy") != old_fingerprint:
        raise GateError(f"no adopted migration of legacy generation {old_fingerprint} is recorded "
                        f"({migration.record_path(env_dir)})")
    transition = (journal or {}).get("transition") or {}
    # abandon drops a transition the generated config may have moved on from.
    if (journal or {}).get("active") != old_fingerprint or transition.get("from") != old_fingerprint \
            or (stage != "abandon" and transition.get("to") != target):
        raise GateError(f"the policy journal records no transition from {old_fingerprint} to {target}")
    try:
        policies, old_tags = snapshot["fgac_policies"], dict(snapshot["tags"])
        intact = migration.legacy_fingerprint(policies, old_tags) == old_fingerprint
    except (KeyError, TypeError, ValueError):
        intact = False
    if not intact:
        raise GateError(f"the snapshot of {old_fingerprint} doesn't match its fingerprint (edited since adoption)")

    def treatment(item: dict) -> bool:
        return item.get("entity_type") == "columns" and item.get("tag_key") == TAG_KEY

    desired = list(_load_tfvars(effective(env_dir, "abac.auto.tfvars")).get("tag_assignments") or [])
    tags = dict(old_tags)
    if keep_new_tags:
        tags.update({i.get("entity_name"): i.get("tag_value") for i in desired
                     if treatment(i) and i.get("entity_name") not in old_tags})
    staged = [i for i in desired if not treatment(i)] + [
        {"entity_type": "columns", "entity_name": column, "tag_key": TAG_KEY, "tag_value": value}
        for column, value in sorted(tags.items())]
    return desired, staged


def _is_treatment(item: dict) -> bool:
    from governance_policies import TAG_KEY

    return item.get("entity_type") == "columns" and item.get("tag_key") == TAG_KEY


def _unrecorded_first_release(env_dir: Path, env_name: str, stage: str, journal_seen: bool) -> tuple[bool, str]:
    """Whether release-plan can treat an unjournaled empty generation as the env's first release."""
    if stage != "create":
        return False, "unjournaled first release is only recognized for the create phase"
    if journal_seen:
        return False, "the policy journal exists but records no first deploy"
    try:
        from scripts import release_gate as rg
        from scripts import staged_policy_apply as spa

        live = spa.live_from_state(spa._read_state(env_dir))
        receipt, problem = rg.newest_release_receipt(rg.cloud_dir_of(env_dir), env_name)
    except Exception as exc:
        return False, f"could not prove this is the env's first release ({exc})"
    if live.get("policies") or live.get("tags"):
        return False, "the data_access state already holds deterministic policies or treatment tags"
    if problem:
        return False, f"the newest release receipt is invalid ({problem})"
    if receipt:
        return False, f"a prior release receipt exists ({receipt.get('_path')})"
    return True, "no live deterministic generation and no prior release receipt"


def staged_generation_assignments(env_dir: Path, stage: str, old_fingerprint: str, keep_new_tags: bool = False,
                                  rebind_fingerprint: str = "", opens_access: bool = False,
                                  env_name: str | None = None) -> tuple[list, list, dict]:
    """(desired, staged, kept) tag assignments of a phase that keeps a deterministic generation's values.

    The deterministic counterpart of staged_migration_assignments: ``staged``
    is what policy_generations.plan_stage renders for ``stage`` from the
    effective config the refresh recorded (``desired``): its non-treatment
    assignments, every treatment tag at the value generation
    ``old_fingerprint`` recorded and, after an attempted switch
    (``keep_new_tags``, except for abandon), newly governed columns at their
    desired value. ``kept`` is what the result records: the generation kept
    (``kept_from``) and the columns the refresh governs that the render
    leaves untagged (``uncovered_columns``). Raises GateError, failing closed,
    unless:

    * the stage keeps the old values (pg.KEEP_TAG_STAGES);
    * the policy journal's active generation and transition start are
      ``old_fingerprint``, and the transition ends at the effective config's
      generation (not checked for abandon, which drops a transition the
      config may have moved on from). A rebind (``rebind_fingerprint``,
      judged as restore) instead needs the effective config to be that
      generation and to differ from the transition's only by recreated
      principals (pg.rebind_problems);
    * the snapshot of ``old_fingerprint`` still hashes to it;
    * a phase that can open access (``rebind_fingerprint`` or
      ``opens_access``: the rebind phases, a grant re-check) leaves no
      governed column untagged. The other phases refuse every business
      grant create (pg.plan_violations), so a column the refresh governs but
      the render doesn't tag (a new column before the switch) gains no
      reader through them.

    A first deploy has no active generation (``old_fingerprint`` empty): its
    create (or abandon) keeps no column tagged, which the journal must
    record as a transition from nothing (first_deploy). Every governed
    column is then uncovered in that phase, so a phase that opens access
    refuses as above.
    """
    from governance_policies import TAG_KEY
    from scripts import policy_generations as pg
    from scripts import staged_policy_apply as spa
    from scripts.runtime_overlay import effective

    if stage not in pg.KEEP_TAG_STAGES:
        raise GateError(f"the {stage} phase applies the desired values")
    try:
        loaded_journal = spa.load_journal(env_dir)
        journal = loaded_journal or {}
        snapshot = pg.load_snapshot(env_dir, old_fingerprint) if old_fingerprint else None
        generation = pg.desired_generation(env_dir)
    except (spa.Refused, pg.StageError, OSError, ValueError) as exc:
        raise GateError(str(exc)) from exc
    target = generation["fingerprint"]
    transition = journal.get("transition") or {}
    if not old_fingerprint:
        if not first_deploy(journal, stage, target):
            first_release, first_release_reason = _unrecorded_first_release(
                env_dir, env_name or env_dir.name, stage, loaded_journal is not None)
            if not first_release:
                raise GateError("no active generation is recorded for the phase to keep, and the policy journal "
                                f"records no first deploy of {target}; {first_release_reason}")
        if rebind_fingerprint:
            raise GateError("a first deploy has no restore to re-point")
        snapshot = {"tags": {}}
    elif journal.get("active") != old_fingerprint or transition.get("from") != old_fingerprint:
        raise GateError(f"the policy journal records no transition from {old_fingerprint}")
    if rebind_fingerprint:
        if stage != "restore":
            raise GateError(f"a rebind is judged as the restore it re-applies, not as {stage}")
        if target != rebind_fingerprint:
            raise GateError(f"the generated config is generation {target}, not {rebind_fingerprint}, which the "
                            "rebind re-points to")
        changed_fp, retain_fp = transition.get("to") or "", transition.get("retain") or ""
        try:
            changed = pg.load_snapshot(env_dir, changed_fp)
            retire = pg.load_snapshot(env_dir, retain_fp) if retain_fp not in {"", changed_fp, old_fingerprint} \
                else None
        except (pg.StageError, OSError, ValueError) as exc:
            raise GateError(str(exc)) from exc
        if changed is None:
            raise GateError("the policy journal records no unfinished change to re-point")
        problems = pg.rebind_problems(generation, changed, [g for g in (snapshot, retire) if g],
                                      pg.rebind_approved(env_dir, generation))
        if problems:
            raise GateError(f"the generated config isn't the unfinished change {changed_fp} with only its "
                            f"principals moved: {'; '.join(problems[:3])}")
    elif old_fingerprint and stage != "abandon" and transition.get("to") != target:
        raise GateError(f"the policy journal records no transition from {old_fingerprint} to {target}")
    if old_fingerprint and not pg.snapshot_intact(snapshot, old_fingerprint):
        raise GateError(f"the snapshot of {old_fingerprint} doesn't match its fingerprint (edited since it was "
                        "recorded)")

    old_tags = dict(snapshot["tags"])
    desired = list(_load_tfvars(effective(env_dir, "abac.auto.tfvars")).get("tag_assignments") or [])
    tags = dict(old_tags)
    if keep_new_tags and stage != "abandon":
        tags.update({i.get("entity_name"): i.get("tag_value") for i in desired
                     if _is_treatment(i) and i.get("entity_name") not in old_tags})
    staged = [i for i in desired if not _is_treatment(i)] + [
        {"entity_type": "columns", "entity_name": column, "tag_key": TAG_KEY, "tag_value": value}
        for column, value in sorted(tags.items())]
    uncovered = sorted({i.get("entity_name") for i in desired if _is_treatment(i)} - tags.keys())
    if uncovered and (rebind_fingerprint or opens_access):
        raise GateError(f"{len(uncovered)} column(s) the live refresh governs have no treatment tag in this render "
                        f"({', '.join(uncovered[:5])}), and this phase opens access; the refresh's columns must "
                        "all be covered")
    return desired, staged, {"kept_from": old_fingerprint, "uncovered_columns": uncovered}


def first_deploy(journal: dict, stage: str, target: str) -> bool:
    """Whether the journal records a first deploy of ``target`` that ``stage`` belongs to.

    Nothing is active and the transition starts from nothing (the controller
    found no live deterministic policy or treatment tag), and, but for
    abandon, ends at the effective config's generation.
    """
    transition = (journal or {}).get("transition") or {}
    return (not (journal or {}).get("active") and bool(transition) and not transition.get("from")
            and (stage == "abandon" or transition.get("to") == target))


def pending_treatments(env_dir: Path, staged: dict | None, env_name: str | None = None) -> list | None:
    """The treatment tags a first deploy's create render is checked against, else None.

    That render holds the generation's policies and functions but tags no
    column (policy_generations.plan_stage: nothing is live to keep), so
    checking its own tags would find nothing to cover ("vacuous") and report
    every classified column as unprotected. It is checked instead against the
    treatment tags the live refresh recorded, which its switch applies: every
    column the switch will tag must have this render's policy and function,
    and the untagged-column check runs as it will at the switch. The phase
    opens no access (new SELECT and USE wait for grant, which checks the
    switch's render).
    """
    if not staged or staged.get("stage") != "create" or staged.get("old_fingerprint") \
            or staged.get("rebind_fingerprint") or staged.get("opens_access"):
        return None
    try:
        desired, _, _ = staged_generation_assignments(
            env_dir, "create", "", bool(staged.get("keep_new_tags")), env_name=env_name)
    except GateError:
        return None
    return [item for item in desired if _is_treatment(item)]


def staged_kept_assignments(env_dir: Path, stage: str, old_fingerprint: str, keep_new_tags: bool = False,
                            rebind_fingerprint: str = "", opens_access: bool = False,
                            env_name: str | None = None) -> tuple[list, list, dict]:
    """(desired, staged, kept) of a phase that keeps the active generation's values: legacy or deterministic."""
    from scripts import migration

    if old_fingerprint.startswith(migration.LEGACY_PREFIX):
        desired, staged = staged_migration_assignments(env_dir, stage, old_fingerprint, keep_new_tags)
        return desired, staged, {}
    return staged_generation_assignments(env_dir, stage, old_fingerprint, keep_new_tags,
                                         rebind_fingerprint, opens_access, env_name)


def live_refresh(env_dir: Path, tfvars: Path, staged: dict | None = None,
                 env_name: str | None = None) -> tuple[str | None, str]:
    """The refreshed_at of a live refresh that matches the gated inputs, or why not.

    ``staged`` (stage, old_fingerprint, keep_new_tags, and optionally
    rebind_fingerprint and opens_access) names the staged phase that rendered
    ``tfvars``; see staged_kept_assignments.
    """
    return _live_refresh(env_dir, tfvars, staged, env_name)[:2]


def _live_refresh(env_dir: Path, tfvars: Path, staged: dict | None = None,
                  env_name: str | None = None) -> tuple[str | None, str, dict]:
    """live_refresh, with what a pass records about the generation a staged phase kept."""
    path = env_dir / REFRESH_RELPATH
    hint = "make runs derive-assignments first; re-run the same make command"
    if not path.is_file():
        return None, f"no live refresh of tags/DDL recorded ({path}); {hint}", {}
    try:
        record = json.loads(path.read_text())
        refreshed_at = str(record["refreshed_at"])
        datetime.strptime(refreshed_at, "%Y-%m-%dT%H:%M:%SZ")
        mode = record["mode"]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return None, f"unreadable live-refresh record {path}: {exc}", {}
    if mode not in ("full", "ddl"):
        return None, f"unknown live-refresh mode {mode!r} in {path}", {}
    if record.get("ddl_sha256") != file_sha256(env_dir / "ddl" / "_fetched.sql"):
        return None, f"ddl/_fetched.sql changed since the last live refresh; {hint}", {}
    gated = tag_assignments_digest(_load_tfvars(tfvars).get("tag_assignments") or [])
    if mode == "full" and record.get("tag_assignments_sha256") != gated:
        if not staged:
            return None, ("the data_access tag_assignments don't match the last live refresh "
                          f"(promote hasn't re-split it, or it was edited); {hint}"), {}
        from scripts.migration import LEGACY_PREFIX

        stage, old = staged["stage"], staged["old_fingerprint"]
        legacy = old.startswith(LEGACY_PREFIX)
        try:
            desired, kept, facts = staged_kept_assignments(env_dir, **staged, env_name=env_name)
        except GateError as exc:
            if legacy:
                return None, (f"the {stage} render's tag_assignments don't match the last live refresh, and it "
                              f"isn't a phase that keeps a recorded legacy migration's values: {exc}"), {}
            return None, (f"the {stage} render's tag_assignments don't match the last live refresh, and it "
                          f"isn't a phase that keeps the active generation's recorded values: {exc}"), {}
        if record.get("tag_assignments_sha256") != tag_assignments_digest(desired):
            return None, f"the generated config's tag_assignments don't match the last live refresh; {hint}", {}
        if gated != tag_assignments_digest(kept):
            if legacy:
                return None, (f"the {stage} render's tag_assignments are neither the live refresh's nor the "
                              f"legacy values generation {old} keeps until the switch"), {}
            return None, (f"the {stage} render's tag_assignments are neither the live refresh's nor the values "
                          f"generation {old} keeps until the switch"), {}
        if legacy:
            return refreshed_at, f"{mode}; {stage} keeps legacy generation {old}'s values", {}
        uncovered = facts["uncovered_columns"]
        detail = (f"{mode}; {stage} keeps generation {old}'s values (kept_from {old})" if old else
                  f"{mode}; {stage} of a first deploy keeps no column tagged")
        if uncovered:
            detail += (f"; {len(uncovered)} column(s) the live refresh governs stay untagged in this phase, which "
                       f"opens no access ({', '.join(uncovered[:5])})")
        return refreshed_at, detail, facts
    return refreshed_at, mode, {}


def query_inputs(runner: Path, env_name: str, layer_dir: Path, flags: list[str],
                 config_dir: Path | None = None) -> dict:
    """Evaluate output.coverage_gate_inputs with the apply's var files and flags."""
    environ = {**os.environ, "LAYER_ENV_DIR": str(layer_dir)}
    if config_dir:
        environ[LAYER_CONFIG_ENV] = str(config_dir)
    try:
        result = _run_bounded(
            [str(runner), "data_access", env_name, "console", *flags],
            label="terraform console (data_access)", input=INPUTS_EXPRESSION + "\n",
            env=environ,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    except CommandTimeout:
        invalidate(layer_dir.parent, "terraform console timed out")
        raise
    if result.returncode != 0:
        raise GateError(
            "terraform console could not evaluate the data_access gate inputs:\n"
            + (result.stderr or result.stdout).strip()
        )
    # The runner writes diagnostics to stderr, so stdout contains the value.
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    try:
        encoded = lines[-1]
        if not (encoded.startswith('"') and encoded.endswith('"')):
            raise ValueError(encoded)
        inputs = json.loads(base64.b64decode(encoded[1:-1], validate=True))
        for key in ("fingerprint", "grant_tables", "acknowledged_columns"):
            inputs[key]
    except (IndexError, ValueError, KeyError, TypeError) as exc:
        raise GateError(f"unexpected terraform console output: {exc}") from exc
    return inputs


def _is_for_this_deployment(state: dict, layer_dir: Path) -> bool:
    """A data_access state recorded for this workspace (host and ID); a copied
    or pre-binding state counts as nothing applied."""
    applied = (state.get("outputs", {}).get("coverage_gate", {}) or {}).get("value") or {}
    return str(applied.get("deployment_binding", "")) == deployment_binding(layer_dir)


def _layer_state(layer_dir: Path, layer: str | None = None) -> dict | None:
    from scripts.state_layout import state_json
    layer_dir = Path(layer_dir)
    resolved_layer = layer or ("data_access" if layer_dir.name == data_access_subdir() else "workspace")
    env_dir = layer_dir.parent if resolved_layer == "data_access" else layer_dir
    try:
        return state_json(env_dir, env_dir.name, resolved_layer)
    except RuntimeError as exc:
        raise GateError(f"cannot read {resolved_layer} state: {exc}") from exc


def granted_tables(layer_dir: Path) -> set[str]:
    """Tables the data_access state already grants business SELECT on.

    Read through scripts/state_layout.py (local, GENIERAILS_STATE_DIR or
    remote). No state means nothing is granted yet (every table is a first
    exposure); a state that should exist but can't be read fails the gate.
    """
    state = _layer_state(layer_dir)
    if state is None:
        return set()
    try:
        if not _is_for_this_deployment(state, layer_dir):
            return set()
        tables = set()
        for resource in state.get("resources", []):
            if (resource.get("module"), resource.get("type"), resource.get("name")) != TABLE_GRANT:
                continue
            if resource.get("mode", "managed") != "managed":
                continue
            for instance in resource.get("instances", []):
                if instance.get("status", "") == "tainted" or instance.get("deposed", "") != "":
                    continue
                tables.add(str(instance["index_key"]).split("|", 1)[0].lower())
        return tables
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        raise GateError(f"cannot read data_access state: {exc}") from exc


def write_result(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f"{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(record, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _failed(gate_path: Path, record: dict, inputs: dict, env_name: str, reason: str, *,
            note: str = "", structural: bool = False) -> int:
    """Record a failed gate. Removing, keeping and withholding may still proceed.

    Without a pass, Terraform leaves new grants out of the plan (withholds
    them) and applies the rest: removals and grants already in place. Its
    needs_gate (coverage_gate_inputs) is true only when a grant already in
    place would be kept with weakened or unrecorded protection, which fails
    the plan without a pass; then, or with an older Terraform output without
    needs_gate, this stops here.
    """
    record.update(status="fail", reason=reason)
    write_result(gate_path, record)
    if inputs.get("needs_gate") is False:
        new = inputs.get("new_grants") or []
        if structural:
            print(STRUCTURAL_NOTE.format(env=env_name, result="did not pass") +
                  (f" This plan withholds {len(new)} new SELECT grant(s)." if new else ""))
            return 0
        withheld = (f" {len(new)} new SELECT grant(s) are withheld ({', '.join(new)}); make exits "
                    "non-zero after the apply." if new else "")
        print(f"WARNING: coverage check FAILED for data_access:{env_name}: {reason}\n"
              "  Proceeding because this change weakens the protection (tags, policies, masks) of no "
              "grant already in place: removals and existing grants apply." + withheld +
              " Fix the check before opening anything." + (f"\n  {note}" if note else ""), file=sys.stderr)
        return 0
    print(f"coverage check FAILED for data_access:{env_name}: {reason}\n"
          "  This change keeps business SELECT with weaker (or unrecorded) protection, so nothing is "
          "applied. Fix the errors above, then re-run the same make command.",
          file=sys.stderr)
    return 1


# local.genie_exposure_blocker itself is unknown under terraform console (its
# refresh-time checks need plantimestamp()), so read the rest of it and apply
# those two checks here (refresh_problem), as modules/coverage_gate_check does.
# local.exposure_gate is the gate CAN_RUN rests on: the data_access coverage
# check (legacy) or the live agent coverage check (deterministic,
# scripts/agent_coverage.py).
CAN_RUN_EXPRESSION = (
    "base64encode(jsonencode({"
    "groups = module.workspace.genie_space_acls_groups, "
    "blocker = local.genie_exposure_static_blocker, "
    "refreshed_at = local.exposure_gate.refreshed_at, "
    "fresh_until = local.exposure_gate.fresh_until, "
    "problems = local.exposure_gate.problems, "
    "widening = local.genie_space_can_run_widening, "
    "applied = local.applied_can_run_groups, "
    "missing = local.genie_space_missing_grants, "
    "deterministic = local.deterministic}))"
)
# modules/coverage_gate_check future_skew.
FUTURE_SKEW = timedelta(minutes=5)


def refresh_problem(state: dict, now: datetime) -> str:
    """The gate module's refresh-time checks against this clock, or "" if fresh."""
    try:
        refreshed_at, fresh_until = (
            datetime.fromisoformat(state[key].replace("Z", "+00:00")) for key in ("refreshed_at", "fresh_until"))
        # A missing, malformed or future (beyond clock skew) refresh is not a live refresh.
        if refreshed_at > now + FUTURE_SKEW:
            return state["problems"]["unrefreshed"]
    except (ValueError, TypeError, AttributeError):
        return state["problems"]["unrefreshed"]
    return state["problems"]["expired"] if fresh_until < now else ""


def can_run_check(env_dir: Path, env_name: str, runner: Path, apply_flags: str) -> int:
    """Mirror what the workspace layer withholds from CAN_RUN before make applies it.

    Asks Terraform (terraform console on the workspace root) which agents'
    ACLs add CAN_RUN groups beyond what the last apply left in place. While
    exposure is blocked, Terraform withholds those groups and this says so;
    keeping, shrinking or clearing ACLs (and every other agent) proceeds, so
    revocation never waits on the gate. Records the desired groups for
    `withheld`.
    """
    try:
        result = _run_bounded(
            [str(runner), "workspace", env_name, "console", *console_flags(apply_flags)],
            label="terraform console (workspace CAN_RUN)", input=CAN_RUN_EXPRESSION + "\n",
            env={**os.environ, "LAYER_ENV_DIR": str(env_dir)}, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    except CommandTimeout:
        invalidate(env_dir, "workspace CAN_RUN terraform console timed out")
        raise
    if result.returncode != 0:
        raise GateError("terraform console could not evaluate the workspace CAN_RUN check:\n"
                        + (result.stderr or result.stdout).strip())
    try:
        line = [l.strip() for l in result.stdout.splitlines() if l.strip()][-1]
        if not (line.startswith('"') and line.endswith('"')):
            raise ValueError(line)
        state = json.loads(base64.b64decode(line[1:-1], validate=True))
        groups = state["groups"]
        blocker = state["blocker"] or refresh_problem(state, datetime.now(timezone.utc))
        widening, missing = state["widening"], state["missing"]
    except (IndexError, ValueError, KeyError, TypeError) as exc:
        raise GateError(f"unexpected terraform console output: {exc}") from exc
    # What the config asks for; `withheld` compares it with the state after
    # the apply.
    write_result(env_dir / CAN_RUN_FILENAME, {"version": 1, "desired": groups})
    # Terraform (modules/workspace) leaves these groups out of the ACLs and
    # applies everything else.
    withheld = {
        key: widening.get(key, ["unknown"])
        for key, csv in groups.items()
        if csv and widening.get(key, ["unknown"]) and (blocker or missing.get(key, ["unknown"]))
    }
    if withheld:
        reason = blocker or ("the live agent coverage check failed for those agents"
                             if state.get("deterministic") else
                             "the data_access state lacks the SELECT grants those groups need")
        details = "; ".join(f"{key}: +{', '.join(added)}" for key, added in sorted(withheld.items()))
        print(f"WARNING: Genie CAN_RUN withheld for workspace:{env_name}: these groups wait ({details}) "
              f"while {reason}.\n  Everything else in this change (keeping, shrinking or clearing "
              "CAN_RUN, other agents) applies, and make exits non-zero after the apply. Fix the "
              "coverage check (make apply / make apply-governance) to open them.", file=sys.stderr)
    if blocker:
        # What this apply removes, from the CAN_RUN the last apply left in place.
        try:
            removed = {
                key: names for key, previous in state["applied"].items()
                if (names := sorted(set(previous) - set((groups.get(key) or "").split(","))))
            }
        except (KeyError, AttributeError, TypeError):
            removed = None  # an answer without it makes no claim either way
        if removed is None:
            kept = "Existing Genie CAN_RUN access is kept unless this config removes it"
        elif removed:
            details = "; ".join(f"{key}: -{', '.join(names)}" for key, names in sorted(removed.items()))
            kept = f"Existing Genie CAN_RUN access is kept except what this config removes ({details})"
        else:
            kept = "Existing Genie CAN_RUN access is kept and nothing is revoked"
        print(f"WARNING: Genie exposure is blocked for workspace:{env_name} ({blocker}).\n"
              f"  {kept}; only new CAN_RUN groups wait. This clears on the next make release "
              "(or make apply) once the coverage check passes.", file=sys.stderr)
    return 0


# make check's structural plan (scripts/check_env.py step 6) runs this gate in
# its scratch copy against empty state, without the live tags or DDL a release
# reads, and _plan-layer ignores its exit code: make check's coverage verdict
# is its own step 4 (make coverage-gate), which still fails check. So there
# the validator report is held back and one line says what the result is.
STRUCTURAL_NOTE = ("  Coverage check of make check's structural plan (data_access:{env}): {result}. This is not "
                   "make check's coverage verdict (its coverage check step is): the scratch copy reads no live "
                   "tags or DDL, and release and maintain re-check them. VERBOSE=1 shows the report.")


def structural_check(env_dir: Path) -> bool:
    """Whether this run is make check's structural plan (its one-use token is present; not consumed here)."""
    from scripts.state_layout import STRUCTURAL_CHECK_TOKEN_ENV, structural_check_token_valid

    return structural_check_token_valid(env_dir, os.environ.get(STRUCTURAL_CHECK_TOKEN_ENV))


def deferred_treatments(env_dir: Path, staged: dict | None, tfvars: Path) -> tuple[int, int] | None:
    """(tagged, governed) for a staged phase whose render tags fewer columns than its switch applies, else None.

    A create, tighten, restore or abandon render keeps the active generation's
    treatment tags (policy_generations.plan_stage); the generated config's
    other gr_treatment tags arrive at the switch. Wording only: the decision
    is the check's, as before.
    """
    from scripts import policy_generations as pg
    from scripts.runtime_overlay import effective

    if not staged or staged.get("stage") not in pg.KEEP_TAG_STAGES:
        return None
    try:
        governed = {i.get("entity_name") for i in
                    _load_tfvars(effective(env_dir, "abac.auto.tfvars")).get("tag_assignments") or []
                    if _is_treatment(i)}
        tagged = {i.get("entity_name") for i in _load_tfvars(tfvars).get("tag_assignments") or []
                  if _is_treatment(i)}
    except (GateError, OSError, ValueError):
        return None
    missing = governed - tagged
    return (len(governed & tagged), len(governed)) if missing else None


def run_gate(env_dir: Path, env_name: str, runner: Path, apply_flags: str, verbose: bool,
             config_dir: Path | None = None, staged: dict | None = None,
             validation_config_dir: Path | None = None) -> int:
    """Gate the data_access config: the split one, or a staged phase's render in ``config_dir``."""
    layer_dir = env_dir / data_access_subdir()
    tfvars = (config_dir or layer_dir) / "abac.auto.tfvars"
    validation_tfvars = (validation_config_dir or config_dir or layer_dir) / "abac.auto.tfvars"
    validation_sql = (validation_config_dir or config_dir or layer_dir) / "masking_functions.sql"
    gate_path = layer_dir / GATE_FILENAME
    if not tfvars.is_file():
        if config_dir:
            raise GateError(f"the staged render {tfvars} is missing")
        print(f"=== Skipping coverage check (data_access:{env_name}): no {tfvars} ===")
        return 0
    flags = console_flags(apply_flags)
    inputs = query_inputs(runner, env_name, layer_dir, flags, config_dir)

    print(f"=== Coverage Check (data_access:{env_name}) ===")
    granted = granted_tables(layer_dir)
    grant_tables = sorted({t.lower() for t in inputs["grant_tables"]})
    first = [t for t in grant_tables if t not in granted]
    if first:
        print(f"  First exposure: {len(first)} table(s) not yet granted: {', '.join(first)}")
    record = {
        "version": GATE_VERSION,
        "env": env_name,
        "fingerprint": inputs["fingerprint"],
        # Informational: the max age is part of the fingerprint, and Terraform
        # judges expiry with the value it is planning (or applied) with.
        "max_age": inputs.get("max_age"),
        "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "first_exposure_tables": first,
        "granted_tables": sorted(granted),
        # What the config grants; `withheld` compares it with the state after
        # the apply.
        "grant_keys": inputs.get("grant_keys"),
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump({
            "first_exposure_tables": first,
            "acknowledged_columns": inputs["acknowledged_columns"],
            "acknowledge_file": str(env_dir / "env.auto.tfvars"),
            "column_classes": refresh_column_classes(env_dir),
        }, handle)
        context = Path(handle.name)
    pending = pending_treatments(env_dir, staged, env_name)
    pending_file = None
    note = ""
    structural = structural_check(env_dir)
    if pending is not None:
        print(f"  First deploy: the create phase tags no column yet; its policies and functions are checked "
              f"against the {len(pending)} treatment tag(s) its switch applies")
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(pending, handle)
            pending_file = Path(handle.name)
    else:
        deferred = deferred_treatments(env_dir, staged, validation_tfvars)
        if deferred:
            tagged, governed = deferred
            stage = staged["stage"]
            print(f"  {stage.capitalize()} phase: this render tags {tagged} of the {governed} column(s) the "
                  f"generated config governs; gr_treatment tags are applied at the switch, so the check below "
                  "judges the render before them. New SELECT stays withheld until the grant phase, whose "
                  "check judges the switched render.")
            note = (f"This is the {stage} render: {governed - tagged} gr_treatment tag(s) are applied at the "
                    "switch, so columns it reports untagged may be those; new SELECT stays withheld until the "
                    "grant phase's check passes.")
    try:
        command = [
            sys.executable, str(VALIDATOR), "--coverage-gate", str(validation_tfvars),
            str(validation_sql),
            "--ddl", str(env_dir / "ddl" / "_fetched.sql"),
            "--exposure-context", str(context),
            "--summary-label", f"coverage check (data_access:{env_name})",
        ]
        if pending_file:
            command += ["--pending-treatments", str(pending_file)]
        if verbose:
            command.append("--verbose")
        sys.stdout.flush()
        # make check's structural plan: hold the report back unless it is wanted or decides the plan.
        quiet = structural and not verbose
        try:
            validation = _run_bounded(command, label="coverage validator", cwd=layer_dir,
                                      **({"stdout": subprocess.PIPE, "stderr": subprocess.STDOUT, "text": True}
                                         if quiet else {}))
        except CommandTimeout as exc:
            return _failed(gate_path, record, inputs, env_name, str(exc))
        if quiet and (validation.returncode == 0 or inputs.get("needs_gate") is not False):
            sys.stdout.write(validation.stdout or "")  # a pass, or a failure that stops the plan: shown as is
            sys.stdout.flush()
    finally:
        context.unlink(missing_ok=True)
        if pending_file:
            pending_file.unlink(missing_ok=True)

    after = query_inputs(runner, env_name, layer_dir, flags, config_dir)
    if after["fingerprint"] != inputs["fingerprint"]:
        record.update(status="fail", reason="inputs changed while the coverage check ran")
        write_result(gate_path, record)
        print("coverage check: inputs changed while the check ran; re-run the same make command.",
              file=sys.stderr)
        return 1
    if validation.returncode != 0:
        return _failed(gate_path, record, inputs, env_name,
                       "coverage check failed (validate_abac.py --coverage-gate; see the report above)",
                       note=note, structural=structural)
    refreshed_at, detail, kept = _live_refresh(env_dir, tfvars, staged, env_name)
    if refreshed_at is None:
        return _failed(gate_path, record, inputs, env_name, detail, structural=structural)
    print(f"  Live refresh ({detail}) at {refreshed_at}")
    record.update(status="pass", refreshed_at=refreshed_at, **kept)
    write_result(gate_path, record)
    return 0


def _current_instances(state: dict | None, module: str, rtype: str, names: tuple[str, ...]) -> dict:
    """index_key -> attributes of current (not tainted or deposed) instances."""
    found: dict = {}
    for resource in (state or {}).get("resources", []):
        if (resource.get("module"), resource.get("mode", "managed"), resource.get("type")) != (module, "managed", rtype):
            continue
        if resource.get("name") not in names:
            continue
        for instance in resource.get("instances", []):
            if instance.get("status") == "tainted" or instance.get("deposed") or instance.get("index_key") is None:
                continue
            found[str(instance["index_key"])] = instance.get("attributes") or {}
    return found


def withheld_access(layer: str, layer_dir: Path) -> list[str]:
    """Access the config asks for that the layer's state doesn't hold."""
    state = _layer_state(layer_dir)
    if layer == "data_access":
        try:
            desired = json.loads((layer_dir / GATE_FILENAME).read_text()).get("grant_keys")
        except (OSError, ValueError, AttributeError):
            return []
        try:
            ours = state is not None and _is_for_this_deployment(state, layer_dir)
        except GateError:
            ours = False
        applied = _current_instances(state, *TABLE_GRANT[:2], (TABLE_GRANT[2],)) if ours else {}
        return sorted(f"SELECT {key}" for key in desired or [] if key not in applied)
    try:
        desired = json.loads((layer_dir / CAN_RUN_FILENAME).read_text())["desired"]
    except (OSError, ValueError, KeyError, TypeError):
        return []
    applied = _current_instances(state, "module.workspace", "null_resource", ACL_RESOURCES)
    missing = []
    for key, csv in sorted((desired or {}).items()):
        have = set(str(applied.get(key, {}).get("triggers", {}).get("groups", "")).split(","))
        missing += [f"CAN_RUN {key}: {group}" for group in (csv or "").split(",") if group and group not in have]
    return missing


def report_withheld(layer: str, layer_dir: Path, env_name: str) -> int:
    """Exit non-zero, after an apply, when it withheld access the config asks for."""
    missing = withheld_access(layer, layer_dir)
    if not missing:
        return 0
    print(f"{layer}:{env_name}: applied, but this access is withheld until the coverage check passes "
          f"(removals and existing access were applied):\n  " + "\n  ".join(missing) +
          f"\n  Fix the coverage check, then re-run make apply ENV={env_name}.", file=sys.stderr)
    return 1


def skip_key(layer: str, layer_dir: Path) -> str:
    """What else make's "inputs unchanged" apply skip must depend on, or "noskip".

    data_access: the gate result (status, fingerprint), and never skip while
    the state isn't this deployment's or records an apply without a pass: a later pass must reach the
    state (and so the workspace layer's CAN_RUN check). workspace: the
    data_access state it reads, and never skip while CAN_RUN is withheld.
    """
    if layer == "data_access":
        state = _layer_state(layer_dir) or {}
        try:
            ours = _is_for_this_deployment(state, layer_dir)
        except GateError:
            ours = False
        if not ours or (state.get("outputs", {}).get("coverage_gate", {}).get("value") or {}).get("status") != "pass":
            return "noskip"
        try:
            gate = json.loads((layer_dir / GATE_FILENAME).read_text())
            return f"gate {gate.get('status')} {gate.get('fingerprint')}"
        except (OSError, ValueError, AttributeError):
            return "noskip"
    if withheld_access(layer, layer_dir):
        return "noskip"
    outputs = (_layer_state(layer_dir / data_access_subdir(), "data_access") or {}).get("outputs", {})
    key = "data_access " + hashlib.sha256(json.dumps(
        [outputs.get("coverage_gate"), outputs.get("table_grant_resource_keys")], sort_keys=True,
    ).encode()).hexdigest()
    # Deterministic envs gate CAN_RUN on the live agent coverage check; a new
    # verdict (not just a newer time) must reach Terraform.
    agent_gate = layer_dir / AGENT_COVERAGE_FILENAME
    if agent_gate.exists():
        try:
            record = json.loads(agent_gate.read_text())
            verdict = [record.get("status"), record.get("fingerprint"),
                       {k: (v or {}).get("status") for k, v in (record.get("agents") or {}).items()}]
        except (OSError, ValueError, AttributeError):
            return "noskip"
        key += " agents " + hashlib.sha256(json.dumps(verdict, sort_keys=True).encode()).hexdigest()
    return key


def invalidate(env_dir: Path, reason: str) -> None:
    """Mark the recorded result failed (a live refresh is starting or failed)."""
    path = env_dir / data_access_subdir() / GATE_FILENAME
    if not path.exists():
        (env_dir / REFRESH_RELPATH).unlink(missing_ok=True)
        return
    try:
        record = json.loads(path.read_text())
        if not isinstance(record, dict):
            raise ValueError("not an object")
    except (OSError, ValueError):
        record = {}
    record.update(status="fail", reason=reason)
    record.pop("refreshed_at", None)
    write_result(path, record)
    (env_dir / REFRESH_RELPATH).unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run the data_access coverage check and record the result")
    run.add_argument("--env-dir", required=True, type=Path)
    run.add_argument("--env-name", required=True)
    run.add_argument("--runner", type=Path, default=RUNNER)
    run.add_argument("--apply-flags", default="")
    run.add_argument("--verbose", action="store_true")
    # Set by scripts/policy_generations.py for a staged phase's re-check.
    run.add_argument("--layer-config-dir", type=Path, help="gate a staged phase's render instead of the split config")
    run.add_argument(
        "--validation-config-dir", type=Path,
        help="validate coverage against this config while gating the staged render's Terraform fingerprint",
    )
    run.add_argument("--stage", default="", help="the staged phase that rendered --layer-config-dir")
    run.add_argument("--old-fingerprint", default="", help="the active generation the phase keeps live")
    run.add_argument("--keep-new-tags", action="store_true", help="a switch was attempted (the phase's KEEP_NEW_TAGS)")
    run.add_argument("--rebind-fingerprint", default="",
                     help="make policy-rebind: the generation whose principals the restore render takes")
    run.add_argument("--opens-access", action="store_true",
                     help="the render is applied with new grants allowed (the grant phase)")
    derive = sub.add_parser("needs-derive", help="print the live refresh make must run first: full, ddl or none")
    derive.add_argument("--env-dir", required=True, type=Path)
    derive.add_argument("--apply-flags", default="")
    stale = sub.add_parser("invalidate", help="mark the recorded result failed (and drop the refresh record) before a live refresh")
    stale.add_argument("--env-dir", required=True, type=Path)
    retired = sub.add_parser("warn-retired-flag", help="print one deprecation line if business_access_enabled is still set (never fails)")
    retired.add_argument("--env-file", required=True, type=Path)
    retired.add_argument("--label", default="")
    retired.add_argument("--file-only", action="store_true",
                         help="check only --env-file (the account layer never reads APPLY_FLAGS or TF_VAR_)")
    check = sub.add_parser("can-run-check", help="report the CAN_RUN a workspace apply withholds while exposure is blocked")
    check.add_argument("--env-dir", required=True, type=Path)
    check.add_argument("--env-name", required=True)
    check.add_argument("--runner", type=Path, default=RUNNER)
    check.add_argument("--apply-flags", default="")
    held = sub.add_parser("withheld", help="after an apply: exit 1 if it withheld access the config asks for")
    held.add_argument("--layer", required=True, choices=("data_access", "workspace"))
    held.add_argument("--layer-dir", required=True, type=Path)
    held.add_argument("--env-name", required=True)
    skip = sub.add_parser("skip-key", help="print what make's unchanged-inputs apply skip also depends on (or noskip)")
    skip.add_argument("--layer", required=True, choices=("data_access", "workspace"))
    skip.add_argument("--layer-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "withheld":
            return report_withheld(args.layer, args.layer_dir, args.env_name)
        if args.command == "skip-key":
            try:
                print(skip_key(args.layer, args.layer_dir))
            except GateError as exc:  # an unreadable state never lets an apply skip
                print(f"coverage check: {exc}; not skipping the apply", file=sys.stderr)
                print("noskip")
            return 0
        if args.command == "warn-retired-flag":
            # APPLY_FLAGS comes through the environment, so its quoting survives.
            if args.file_only:
                sources = retired_flag_sources(args.env_file, "", {})
            else:
                sources = retired_flag_sources(args.env_file, os.environ.get("GENIERAILS_APPLY_FLAGS", ""), os.environ)
            if sources:
                where = ", ".join(args.label or source if source == str(args.env_file) else source
                                  for source in sources)
                print(f"WARNING: {RETIRED_FLAG} ({where}) is deprecated and ignored (business access follows "
                      "the coverage check; setting it false does not revoke access). Remove it; to withdraw "
                      "access, remove the groups or acl_groups entries.", file=sys.stderr)
            return 0
        if args.command == "can-run-check":
            return can_run_check(args.env_dir.resolve(), args.env_name, args.runner, args.apply_flags)
        if args.command == "invalidate":
            invalidate(args.env_dir, "a live refresh of tags/DDL started and has not been checked since")
            return 0
        if args.command == "needs-derive":
            mode, reason = needs_derive(args.env_dir, args.apply_flags)
            print(mode)
            if reason:
                print(f"live refresh before exposure ({mode}): {reason}", file=sys.stderr)
            return 0
        staged = ({"stage": args.stage, "old_fingerprint": args.old_fingerprint,
                   "keep_new_tags": args.keep_new_tags, "rebind_fingerprint": args.rebind_fingerprint,
                   "opens_access": args.opens_access} if args.stage else None)
        return run_gate(args.env_dir.resolve(), args.env_name, args.runner, args.apply_flags, args.verbose,
                        args.layer_config_dir.resolve() if args.layer_config_dir else None, staged,
                        args.validation_config_dir.resolve() if args.validation_config_dir else None)
    except GateError as exc:
        print(f"coverage check: ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
