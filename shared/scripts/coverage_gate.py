#!/usr/bin/env python3
"""Coverage gate for the data_access layer, enforced by Terraform.

`run` gates the split data_access config exactly as Terraform will apply it:

  1. Ask Terraform (terraform console, same var files and -var flags as the
     apply) for the gate inputs: their fingerprint, whether business SELECT is
     requested, and the tables it would grant.
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
current local inputs. Like the certification receipt, this catches drift and
skipped steps; it is not a defence against someone who hand-forges the files.

`needs-derive` prints which live refresh make must run before a plan/apply
that opens business access: "full" (derive-assignments: live class.* tags and
DDL), "ddl" (live DDL only, for envs whose tags come from make generate), or
"none" when access stays closed.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shlex
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import hcl2

SHARED_ROOT = Path(__file__).resolve().parent.parent
RUNNER = SHARED_ROOT / "scripts" / "terraform_layer.sh"
VALIDATOR = SHARED_ROOT / "validate_abac.py"
DATA_ACCESS_SUBDIR = "data_access"
GATE_FILENAME = ".coverage_gate.json"
REFRESH_RELPATH = Path("generated") / ".live_refresh.json"
REFRESH_VERSION = 1
GATE_VERSION = 1
GATE_FLAG = "business_access_enabled"
TABLE_GRANT = ("module.data_access", "databricks_grant", "table_access")
INPUTS_EXPRESSION = "base64encode(jsonencode(module.data_access.coverage_gate_inputs))"


class GateError(Exception):
    """The gate can't establish its inputs; callers treat this as a failure."""


def _load_tfvars(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        return hcl2.loads(path.read_text())
    except Exception as exc:
        raise GateError(f"cannot parse {path}: {exc}") from exc


def _flag_override(apply_flags: str) -> bool | None:
    """The last -var=business_access_enabled=... in APPLY_FLAGS, if any."""
    value = None
    args = shlex.split(apply_flags or "")
    for index, arg in enumerate(args):
        assignment = None
        if arg.startswith("-var="):
            assignment = arg[len("-var="):]
        elif arg == "-var" and index + 1 < len(args):
            assignment = args[index + 1]
        if assignment and assignment.split("=", 1)[0].strip() == GATE_FLAG:
            value = assignment.split("=", 1)[1].strip().strip("\"'").lower() == "true"
    return value


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
    """Which live refresh must precede a plan/apply: full, ddl or none."""
    env = _load_tfvars(env_dir / "env.auto.tfvars")
    requested = _flag_override(apply_flags)
    if requested is None:
        requested = env.get(GATE_FLAG) is True
    if not requested:
        return "none", None
    if env.get("enable_classification") is not True:
        return "ddl", "enable_classification is false; tags come from make generate, so only the DDL is re-read"
    if not (env_dir / "generated" / "abac.auto.tfvars").is_file():
        return "ddl", "no generated/abac.auto.tfvars to derive tags into, so only the DDL is re-read"
    return "full", "business access is being opened in a native-classification env"


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
    result = subprocess.run(
        [str(runner), "data_access", env_name, "console", *flags],
        input=INPUTS_EXPRESSION + "\n",
        env={**os.environ, "LAYER_ENV_DIR": str(layer_dir)},
        text=True,
        capture_output=True,
    )
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
        for key in ("fingerprint", "business_access_enabled", "grant_tables", "acknowledged_columns"):
            inputs[key]
    except (IndexError, ValueError, KeyError, TypeError) as exc:
        raise GateError(f"unexpected terraform console output: {exc}") from exc
    return inputs


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
        tables = set()
        for resource in state.get("resources", []):
            if (resource.get("module"), resource.get("type"), resource.get("name")) != TABLE_GRANT:
                continue
            if resource.get("mode", "managed") != "managed":
                continue
            for instance in resource.get("instances", []):
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
    """Record a failed gate. Keeping or revoking SELECT may still proceed.

    Terraform's own needs_gate (coverage_gate_inputs) is false when every
    planned grant already exists with unchanged protection, which is exactly
    when the table_access precondition doesn't need a pass. Then the change
    can only keep or revoke SELECT, so a failing gate (e.g. Unity Catalog
    unreachable) must not stop it. Anything else, or an older Terraform
    output without needs_gate, stops here.
    """
    record.update(status="fail", reason=reason)
    write_result(gate_path, record)
    if inputs.get("needs_gate") is False:
        print(f"WARNING: coverage gate FAILED for data_access:{env_name}: {reason}\n"
              "  Proceeding only because this change adds no SELECT grant and changes no "
              "protection (tags, policies, masks, DDL) of the grants already in place; it "
              "can only keep or revoke access. Fix the gate before opening anything.",
              file=sys.stderr)
        return 0
    print(f"coverage gate FAILED for data_access:{env_name}: {reason}\n"
          "  Business SELECT stays closed. Fix the errors above, then re-run the same make command.",
          file=sys.stderr)
    return 1


CAN_RUN_EXPRESSION = (
    "base64encode(jsonencode({"
    "enabled = var.business_access_enabled, "
    "groups = module.workspace.genie_space_acls_groups, "
    "blocker = local.genie_exposure_blocker, "
    "widening = local.genie_space_can_run_widening, "
    "missing = local.genie_space_missing_grants}))"
)


def can_run_check(env_dir: Path, env_name: str, runner: Path, apply_flags: str) -> int:
    """Mirror the workspace CAN_RUN precondition before make applies the layer.

    Asks Terraform (terraform console on the workspace root) which agents'
    ACLs add CAN_RUN groups beyond what the last apply left in place. While
    exposure is blocked, those are refused here; keeping, shrinking or
    clearing ACLs proceeds, so revocation never waits on the gate.
    """
    result = subprocess.run(
        [str(runner), "workspace", env_name, "console", *console_flags(apply_flags)],
        input=CAN_RUN_EXPRESSION + "\n",
        env={**os.environ, "LAYER_ENV_DIR": str(env_dir)},
        text=True, capture_output=True,
    )
    if result.returncode != 0:
        raise GateError("terraform console could not evaluate the workspace CAN_RUN check:\n"
                        + (result.stderr or result.stdout).strip())
    try:
        line = [l.strip() for l in result.stdout.splitlines() if l.strip()][-1]
        if not (line.startswith('"') and line.endswith('"')):
            raise ValueError(line)
        state = json.loads(base64.b64decode(line[1:-1], validate=True))
        enabled, groups = state["enabled"], state["groups"]
        blocker, widening, missing = state["blocker"], state["widening"], state["missing"]
    except (IndexError, ValueError, KeyError, TypeError) as exc:
        raise GateError(f"unexpected terraform console output: {exc}") from exc
    if not enabled:
        return 0
    refused = {
        key: widening.get(key, ["unknown"])
        for key, csv in groups.items()
        if csv and widening.get(key, ["unknown"]) and (blocker or missing.get(key, ["unknown"]))
    }
    if refused:
        reason = blocker or "the data_access state lacks the SELECT grants those groups need"
        details = "; ".join(f"{key}: +{', '.join(added)}" for key, added in sorted(refused.items()))
        print(f"Genie CAN_RUN blocked for workspace:{env_name}: these ACLs add groups ({details}) "
              f"while {reason}.\n  Removing or keeping CAN_RUN would apply; fix the coverage gate "
              "(make apply / make apply-governance) before opening it.", file=sys.stderr)
        return 1
    if blocker:
        print(f"WARNING: Genie exposure is blocked for workspace:{env_name} ({blocker}); applying "
              "only ACLs that keep, shrink or clear the CAN_RUN already in place.", file=sys.stderr)
    return 0


def run_gate(env_dir: Path, env_name: str, runner: Path, apply_flags: str, verbose: bool) -> int:
    layer_dir = env_dir / DATA_ACCESS_SUBDIR
    tfvars = layer_dir / "abac.auto.tfvars"
    gate_path = layer_dir / GATE_FILENAME
    if not tfvars.is_file():
        print(f"=== Skipping coverage gate (data_access:{env_name}): no {tfvars} ===")
        return 0
    flags = console_flags(apply_flags)
    inputs = query_inputs(runner, env_name, layer_dir, flags)
    if not inputs["business_access_enabled"]:
        print(f"=== Coverage gate (data_access:{env_name}): not required, business_access_enabled = false ===")
        return 0

    print(f"=== Coverage Gate (data_access:{env_name}) ===")
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
            "--summary-label", f"coverage-gate (data_access:{env_name})",
        ]
        if verbose:
            command.append("--verbose")
        sys.stdout.flush()
        validation = subprocess.run(command, cwd=layer_dir)
    finally:
        context.unlink(missing_ok=True)

    after = query_inputs(runner, env_name, layer_dir, flags)
    if after["fingerprint"] != inputs["fingerprint"]:
        record.update(status="fail", reason="inputs changed while the gate ran")
        write_result(gate_path, record)
        print("coverage gate: inputs changed while the gate ran; re-run the same make command.",
              file=sys.stderr)
        return 1
    if validation.returncode != 0:
        return _failed(gate_path, record, inputs, env_name,
                       "validate_abac.py --coverage-gate failed (see the report above)")
    refreshed_at, detail = live_refresh(env_dir, tfvars)
    if refreshed_at is None:
        return _failed(gate_path, record, inputs, env_name, detail)
    print(f"  Live refresh ({detail}) at {refreshed_at}")
    record.update(status="pass", refreshed_at=refreshed_at)
    write_result(gate_path, record)
    return 0


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
    run = sub.add_parser("run", help="gate the data_access layer and record the result")
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
    check = sub.add_parser("can-run-check", help="refuse a workspace apply that opens or widens CAN_RUN while exposure is blocked")
    check.add_argument("--env-dir", required=True, type=Path)
    check.add_argument("--env-name", required=True)
    check.add_argument("--runner", type=Path, default=RUNNER)
    check.add_argument("--apply-flags", default="")
    args = parser.parse_args(argv)
    try:
        if args.command == "can-run-check":
            return can_run_check(args.env_dir.resolve(), args.env_name, args.runner, args.apply_flags)
        if args.command == "invalidate":
            invalidate(args.env_dir, "a live refresh of tags/DDL started and has not been gated since")
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
        print(f"coverage gate: ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
