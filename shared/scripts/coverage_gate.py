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
     refreshed_at.
  5. Ask Terraform for the fingerprint again and write the result to
     envs/<env>/data_access/.coverage_gate.json.

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
RUNNER = SHARED_ROOT / "scripts" / "terraform_layer.sh"
VALIDATOR = SHARED_ROOT / "validate_abac.py"
DATA_ACCESS_SUBDIR = "data_access"
GATE_FILENAME = ".coverage_gate.json"
CAN_RUN_FILENAME = ".can_run_check.json"
ACL_RESOURCES = ("genie_space_acls", "genie_space_acls_created")
REFRESH_RELPATH = Path("generated") / ".live_refresh.json"
REFRESH_VERSION = 1
GATE_VERSION = 1
SUBPROCESS_TIMEOUT = int(os.environ.get("COVERAGE_GATE_TIMEOUT_SECONDS", "300"))
TABLE_GRANT = ("module.data_access", "databricks_grant", "table_access")
INPUTS_EXPRESSION = "base64encode(jsonencode(module.data_access.coverage_gate_inputs))"


class GateError(Exception):
    """The gate can't establish its inputs; callers treat this as a failure."""


class CommandTimeout(GateError):
    """A coverage subprocess exceeded its bounded runtime."""


def _run_bounded(command: list[str], *, label: str, **kwargs) -> subprocess.CompletedProcess:
    """Run a child in its own process group and kill the whole group on timeout."""
    input_value = kwargs.pop("input", None)
    if input_value is not None:
        kwargs["stdin"] = subprocess.PIPE
    process = subprocess.Popen(command, start_new_session=True, **kwargs)
    try:
        stdout, stderr = process.communicate(input=input_value, timeout=SUBPROCESS_TIMEOUT)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate()
        raise CommandTimeout(
            f"{label} timed out after {SUBPROCESS_TIMEOUT}s; its process group was killed "
            "and the coverage result was invalidated"
        )
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def _load_tfvars(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        return hcl2.loads(path.read_text())
    except Exception as exc:
        raise GateError(f"cannot parse {path}: {exc}") from exc


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
    if not generated and not (env_dir / DATA_ACCESS_SUBDIR / "abac.auto.tfvars").is_file():
        return "none", None
    if env.get("enable_classification") is not True:
        return "ddl", "enable_classification is false; tags come from make generate, so only the DDL is re-read"
    if not generated:
        return "ddl", "no generated/abac.auto.tfvars to derive tags into, so only the DDL is re-read"
    return "full", "business access requires a coverage check of live tags in a native-classification env"


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""


def tag_assignments_digest(assignments: list) -> str:
    """Order-independent digest of tag assignments (generated vs. split config)."""
    keys = sorted(
        "|".join(str(item.get(field, "")) for field in ("entity_type", "entity_name", "tag_key", "tag_value"))
        for item in assignments or []
    )
    return hashlib.sha256("\n".join(keys).encode()).hexdigest()


def write_refresh_record(path: Path, *, mode: str, ddl_path: Path, config_path: Path | None) -> None:
    """Record a successful live refresh (written by derive_assignments.py)."""
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
    write_result(path, record)


def live_refresh(env_dir: Path, tfvars: Path) -> tuple[str | None, str]:
    """The refreshed_at of a live refresh that matches the gated inputs, or why not."""
    path = env_dir / REFRESH_RELPATH
    hint = "make runs derive-assignments first; re-run the same make command"
    if not path.is_file():
        return None, f"no live refresh of tags/DDL recorded ({path}); {hint}"
    try:
        record = json.loads(path.read_text())
        refreshed_at = str(record["refreshed_at"])
        datetime.strptime(refreshed_at, "%Y-%m-%dT%H:%M:%SZ")
        mode = record["mode"]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return None, f"unreadable live-refresh record {path}: {exc}"
    if mode not in ("full", "ddl"):
        return None, f"unknown live-refresh mode {mode!r} in {path}"
    if record.get("ddl_sha256") != file_sha256(env_dir / "ddl" / "_fetched.sql"):
        return None, f"ddl/_fetched.sql changed since the last live refresh; {hint}"
    if mode == "full" and record.get("tag_assignments_sha256") != tag_assignments_digest(
        _load_tfvars(tfvars).get("tag_assignments") or []
    ):
        return None, ("the data_access tag_assignments don't match the last live refresh "
                      f"(promote hasn't re-split it, or it was edited); {hint}")
    return refreshed_at, mode


def query_inputs(runner: Path, env_name: str, layer_dir: Path, flags: list[str]) -> dict:
    """Evaluate output.coverage_gate_inputs with the apply's var files and flags."""
    try:
        result = _run_bounded(
            [str(runner), "data_access", env_name, "console", *flags],
            label="terraform console (data_access)", input=INPUTS_EXPRESSION + "\n",
            env={**os.environ, "LAYER_ENV_DIR": str(layer_dir)},
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
    # The runner echoes its commands to stdout; the value is the last line.
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


def deployment_binding(layer_dir: Path) -> str:
    """roots/data_access local.deployment_binding, from the layer's tfvars."""
    variables = {}
    for path in (layer_dir / "auth.auto.tfvars", layer_dir / "env.auto.tfvars"):
        variables.update(_load_tfvars(path))
    return hashlib.sha256(json.dumps({
        "workspace_host": str(variables.get("databricks_workspace_host", "")).strip().rstrip("/").lower(),
        "workspace_id": str(variables.get("databricks_workspace_id", "")).strip(),
    }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _is_for_this_deployment(state: dict, layer_dir: Path) -> bool:
    """A data_access state recorded for this workspace (host and ID); a copied
    or pre-binding state counts as nothing applied."""
    applied = (state.get("outputs", {}).get("coverage_gate", {}) or {}).get("value") or {}
    return str(applied.get("deployment_binding", "")) == deployment_binding(layer_dir)


def granted_tables(layer_dir: Path) -> set[str]:
    """Tables the data_access state already grants business SELECT on.

    The layer runner always uses the local backend at this path. No state file
    means nothing is granted yet (every table is a first exposure); a state
    that exists but can't be read fails the gate.
    """
    state_path = layer_dir / "terraform.tfstate"
    if not state_path.exists():
        return set()
    try:
        state = json.loads(state_path.read_text())
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
        raise GateError(f"cannot read data_access state {state_path}: {exc}") from exc


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


def _failed(gate_path: Path, record: dict, inputs: dict, env_name: str, reason: str) -> int:
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
        withheld = (f" {len(new)} new SELECT grant(s) are withheld ({', '.join(new)}); make exits "
                    "non-zero after the apply." if new else "")
        print(f"WARNING: coverage check FAILED for data_access:{env_name}: {reason}\n"
              "  Proceeding because this change weakens the protection (tags, policies, masks) of no "
              "grant already in place: removals and existing grants apply." + withheld +
              " Fix the check before opening anything.", file=sys.stderr)
        return 0
    print(f"coverage check FAILED for data_access:{env_name}: {reason}\n"
          "  This change keeps business SELECT with weaker (or unrecorded) protection, so nothing is "
          "applied. Fix the errors above, then re-run the same make command.",
          file=sys.stderr)
    return 1


# local.genie_exposure_blocker itself is unknown under terraform console (its
# refresh-time checks need plantimestamp()), so read the rest of it and apply
# those two checks here (refresh_problem), as modules/coverage_gate_check does.
CAN_RUN_EXPRESSION = (
    "base64encode(jsonencode({"
    "groups = module.workspace.genie_space_acls_groups, "
    "blocker = local.genie_exposure_static_blocker, "
    "refreshed_at = module.coverage_gate_check.refreshed_at, "
    "fresh_until = module.coverage_gate_check.fresh_until, "
    "problems = module.coverage_gate_check.problems, "
    "widening = local.genie_space_can_run_widening, "
    "applied = local.applied_can_run_groups, "
    "missing = local.genie_space_missing_grants}))"
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
        reason = blocker or "the data_access state lacks the SELECT grants those groups need"
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


def run_gate(env_dir: Path, env_name: str, runner: Path, apply_flags: str, verbose: bool) -> int:
    layer_dir = env_dir / DATA_ACCESS_SUBDIR
    tfvars = layer_dir / "abac.auto.tfvars"
    gate_path = layer_dir / GATE_FILENAME
    if not tfvars.is_file():
        print(f"=== Skipping coverage check (data_access:{env_name}): no {tfvars} ===")
        return 0
    flags = console_flags(apply_flags)
    inputs = query_inputs(runner, env_name, layer_dir, flags)

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
        }, handle)
        context = Path(handle.name)
    try:
        command = [
            sys.executable, str(VALIDATOR), "--coverage-gate", str(tfvars),
            str(layer_dir / "masking_functions.sql"),
            "--ddl", str(env_dir / "ddl" / "_fetched.sql"),
            "--exposure-context", str(context),
            "--summary-label", f"coverage check (data_access:{env_name})",
        ]
        if verbose:
            command.append("--verbose")
        sys.stdout.flush()
        try:
            validation = _run_bounded(command, label="coverage validator", cwd=layer_dir)
        except CommandTimeout as exc:
            return _failed(gate_path, record, inputs, env_name, str(exc))
    finally:
        context.unlink(missing_ok=True)

    after = query_inputs(runner, env_name, layer_dir, flags)
    if after["fingerprint"] != inputs["fingerprint"]:
        record.update(status="fail", reason="inputs changed while the coverage check ran")
        write_result(gate_path, record)
        print("coverage check: inputs changed while the check ran; re-run the same make command.",
              file=sys.stderr)
        return 1
    if validation.returncode != 0:
        return _failed(gate_path, record, inputs, env_name,
                       "coverage check failed (validate_abac.py --coverage-gate; see the report above)")
    refreshed_at, detail = live_refresh(env_dir, tfvars)
    if refreshed_at is None:
        return _failed(gate_path, record, inputs, env_name, detail)
    print(f"  Live refresh ({detail}) at {refreshed_at}")
    record.update(status="pass", refreshed_at=refreshed_at)
    write_result(gate_path, record)
    return 0


def _state(path: Path) -> dict | None:
    """A layer's local state, or None if missing or unreadable."""
    try:
        state = json.loads(path.read_text())
        return state if isinstance(state, dict) else None
    except (OSError, ValueError):
        return None


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
    state = _state(layer_dir / "terraform.tfstate")
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
        state = _state(layer_dir / "terraform.tfstate") or {}
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
    outputs = (_state(layer_dir / DATA_ACCESS_SUBDIR / "terraform.tfstate") or {}).get("outputs", {})
    return "data_access " + hashlib.sha256(json.dumps(
        [outputs.get("coverage_gate"), outputs.get("table_grant_resource_keys")], sort_keys=True,
    ).encode()).hexdigest()


def invalidate(env_dir: Path, reason: str) -> None:
    """Mark the recorded result failed (a live refresh is starting or failed)."""
    path = env_dir / DATA_ACCESS_SUBDIR / GATE_FILENAME
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
            print(skip_key(args.layer, args.layer_dir))
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
        return run_gate(args.env_dir.resolve(), args.env_name, args.runner,
                        args.apply_flags, args.verbose)
    except GateError as exc:
        print(f"coverage check: ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
