#!/usr/bin/env python3
"""Scheduled read-only governance checks.

The default ``check`` flow runs the detection halves of native-first maintain:
schema drift and rulebook drift. It never invokes make, Terraform, assignment
derivation, or an apply, and it does not write to the workspace.

The legacy flow remains available as ``--step all`` (or its individual steps):

  audit    -> scripts/audit_schema_drift.py            (== make audit-schema)
  delta    -> generate_abac.py --delta --auth-file ...  (== make generate-delta)
  coverage -> validate_abac.py <config the delta wrote> (== make validate-generated / validate)

The legacy scripts resolve config via relative paths from the environment
directory (envs/<env>/), so this wrapper `chdir`s there once and shells out to
them using the same interpreter. Running all three steps in a SINGLE process
(``--step all``) is what lets ``coverage`` see the file
``delta`` just regenerated — they share one working tree.

The scheduled Databricks Job defined in roots/workspace/scheduled_governance.tf
uses ``--step check``. Legacy per-step modes remain available for existing
callers.

Exit codes:
  - check returns non-zero when either read-only audit reports findings.
  - A drift-only run (audit found drift, delta re-derived it, coverage passed)
    still returns non-zero: the last non-zero step code is remembered, so the
    scheduled run goes red and notifies the team to review + apply the delta.
  - coverage returns non-zero if there is no config to validate (a misconfigured
    env is a hard failure, never a silent pass).
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
SHARED_ROOT = SCRIPTS_DIR.parent
REPO_ROOT = SHARED_ROOT.parent

LEGACY_STEPS = ("audit", "delta", "coverage")


def _resolve_env_dir(env_dir_arg: str) -> Path:
    """Resolve the target env directory (absolute, or repo-relative like
    'aws/envs/prod') to an absolute path."""
    p = Path(env_dir_arg)
    if not p.is_absolute():
        p = (REPO_ROOT / p).resolve()
    return p


def _validate_env_layout(env_dir: Path) -> str | None:
    """Return an error when env_dir is not <cloud>/envs/<env>."""
    if env_dir.parent.name != "envs":
        return f"env directory must have an 'envs' parent: {env_dir}"
    cloud_root = env_dir.parent.parent
    if not (cloud_root / "Makefile").is_file():
        return f"cloud root has no Makefile: {cloud_root}"
    return None


def _overlay(src: Path, dst: Path, label: str) -> None:
    """Copy src onto dst, skipping a source that already is the destination."""
    dst.mkdir(parents=True, exist_ok=True)
    if src.resolve() == dst.resolve():
        # A local run can pass its own envs root as --config-source.
        print(f"+ {label} config already in place: {dst}", flush=True)
        return
    print(f"+ materialize {label} config: {src} -> {dst}", flush=True)
    shutil.copytree(src, dst, dirs_exist_ok=True, symlinks=True)


def _materialize_env_dir(config_source: str, env_dir: Path) -> None:
    """Copy the env configuration from a runtime-visible source into env_dir.

    The repo's `envs/` directories are .gitignore'd (they hold per-deployment
    config + secrets), so a Git checkout of this repo does NOT contain
    envs/<env>/. When the job runs from a fresh checkout, the operator points it
    at a path the job runtime can see — a Unity Catalog Volume, workspace files,
    or DBFS mount holding auth.auto.tfvars, env.auto.tfvars, data_access/,
    generated/, etc. — and we copy that tree into env_dir before scanning.

    Overlays onto env_dir if it already exists (source wins).
    """
    src = Path(config_source)
    if not src.is_dir():
        raise FileNotFoundError(
            f"config source not found or not a directory: {src}")
    env_source = src / env_dir.name
    account_source = src / "account"
    if env_source.is_dir() and account_source.is_dir():
        _overlay(env_source, env_dir, "env")
        _overlay(account_source, env_dir.parent / "account", "account")
        return

    _overlay(src, env_dir, "env")


def _run(cmd: list[str], cwd: Path) -> int:
    print(f"+ (cd {cwd} && {' '.join(cmd)})", flush=True)
    return subprocess.run(cmd, cwd=str(cwd)).returncode


def _audit(env_dir: Path) -> int:
    return _run(
        [sys.executable, str(SCRIPTS_DIR / "audit_schema_drift.py")],
        cwd=env_dir,
    )


def _rulebook(env_dir: Path) -> int:
    return _run(
        [sys.executable, str(SCRIPTS_DIR / "audit_schema_drift.py"),
         "--mode", "rulebook"],
        cwd=env_dir,
    )


def _check(env_dir: Path) -> int:
    """Run both read-only native-first detection checks."""
    account_config = env_dir.parent / "account" / "abac.auto.tfvars"
    skip_rulebook = not account_config.is_file()
    if skip_rulebook:
        print("\n" + "=" * 72, file=sys.stderr)
        print("WARNING: RULEBOOK AUDIT SKIPPED", file=sys.stderr)
        print("WARNING: skipping rulebook audit because promoted account config "
              f"is missing at {account_config}. The old per-environment config "
              "source still supports schema drift only. Repoint "
              "scheduled_governance_config_source at the envs root containing "
              f"account/ and {env_dir.name}/ to enable rulebook checks.",
              file=sys.stderr)
        print("=" * 72 + "\n", file=sys.stderr)

    audit_rc = _audit(env_dir)
    if not skip_rulebook:
        rulebook_rc = _rulebook(env_dir)
    else:
        rulebook_rc = 0

    errors = [rc for rc in (audit_rc, rulebook_rc) if rc not in (0, 1)]
    if errors:
        print("\nERROR: scheduled governance check could not complete. Review "
              "the audit error above and fix the runtime configuration or "
              "credentials; this is not a governance finding.", file=sys.stderr)

    if audit_rc == 1 or rulebook_rc == 1:
        env = env_dir.name
        print(f"\nRun `make maintain ENV={env}` from your GenieRails checkout.",
              file=sys.stderr)
        if audit_rc == 1:
            print("For untagged sensitive-looking columns, review native UC "
                  "classification / auto-tagging or tag them in UC; maintain "
                  "never LLM-tags columns.", file=sys.stderr)
        if rulebook_rc == 1:
            print(f"For rulebook gaps, add the rule in dev, re-promote, then "
                  f"run `make certify ENV={env}`.", file=sys.stderr)
    if errors:
        return errors[0]
    return rulebook_rc or audit_rc


def _delta(env_dir: Path, auth_file: str, catalog: str = "") -> int:
    cmd = [sys.executable, str(SHARED_ROOT / "generate_abac.py"),
           "--delta", "--auth-file", auth_file]
    if catalog:
        cmd += ["--catalog", catalog]
    return _run(cmd, cwd=env_dir)


def _coverage(env_dir: Path) -> int:
    """Validate the config layer the delta step actually writes to.

    generate_abac.py --delta merges into generated/abac.auto.tfvars when it
    exists, otherwise into the split data_access/abac.auto.tfvars — so this
    mirrors that choice (== make validate-generated / make validate). If there
    is no ABAC config in either layer, that is a misconfigured env: fail loudly
    rather than silently pass.
    """
    generated = env_dir / "generated" / "abac.auto.tfvars"
    split_da = env_dir / "data_access" / "abac.auto.tfvars"

    if generated.exists():
        cmd = [sys.executable, str(SHARED_ROOT / "validate_abac.py"), str(generated)]
        masking = env_dir / "generated" / "masking_functions.sql"
        if masking.exists():
            cmd.append(str(masking))
        return _run(cmd, cwd=env_dir)

    if split_da.exists():
        cmd = [sys.executable, str(SHARED_ROOT / "validate_abac.py"), str(split_da)]
        masking = env_dir / "data_access" / "masking_functions.sql"
        if masking.exists():
            cmd.append(str(masking))
        return _run(cmd, cwd=env_dir)

    print(f"ERROR: coverage check found no ABAC config to validate in {env_dir} "
          f"(looked for generated/abac.auto.tfvars and data_access/abac.auto.tfvars).",
          file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env-dir", required=True,
                        help="Target environment directory (absolute, or repo-relative like 'aws/envs/prod').")
    parser.add_argument("--step", choices=("check", *LEGACY_STEPS, "all"),
                        default="check",
                        help="Flow to run. Default 'check' runs read-only schema and rulebook audits. "
                             "'all' is the legacy audit -> delta -> coverage flow.")
    parser.add_argument("--auth-file", default="auth.auto.tfvars",
                        help="Legacy delta only: auth tfvars filename (default: auth.auto.tfvars).")
    parser.add_argument("--catalog", default="",
                        help="Legacy delta only: catalog passed to generate_abac.py --delta. "
                             "Empty = auto-derive from the env's uc_tables.")
    parser.add_argument("--config-source", default="",
                        help="Runtime-visible path (UC Volume / workspace files / DBFS mount) holding "
                             "account/ and <env>/ config to materialize before check. Legacy mode also "
                             "accepts a flat target-env source. Required for a fresh Git checkout.")
    args = parser.parse_args()

    env_dir = _resolve_env_dir(args.env_dir)
    layout_error = _validate_env_layout(env_dir)
    if layout_error:
        print(f"ERROR: {layout_error}", file=sys.stderr)
        return 2

    if args.config_source:
        try:
            _materialize_env_dir(args.config_source, env_dir)
        except FileNotFoundError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2

    if not env_dir.is_dir():
        print(f"ERROR: env directory not found: {env_dir}\n"
              f"       The repo's envs/ is .gitignore'd, so a Git-checked-out job will not\n"
              f"       contain it. Pass --config-source <volume/workspace path> to materialize\n"
              f"       the env config at runtime.", file=sys.stderr)
        return 2

    if args.catalog and args.step not in ("delta", "all"):
        print("WARNING: --catalog is legacy-only and is ignored by the "
              f"'{args.step}' step.", file=sys.stderr)

    if args.step == "check":
        print("=" * 60)
        print(f"  Scheduled governance: read-only native check  (env: {env_dir.name})")
        print("=" * 60)
        return _check(env_dir)

    steps = LEGACY_STEPS if args.step == "all" else (args.step,)
    rc = 0
    for step in steps:
        print("=" * 60)
        print(f"  Scheduled governance step: {step}  (env: {env_dir.name})")
        print("=" * 60)
        if step == "audit":
            step_rc = _audit(env_dir)
        elif step == "delta":
            step_rc = _delta(env_dir, args.auth_file, args.catalog)
        else:
            step_rc = _coverage(env_dir)
        # For a single-step invocation, mirror the wrapped script's exit code.
        # For --step all, keep going but remember the last non-zero code.
        if step_rc != 0:
            rc = step_rc
    return rc


if __name__ == "__main__":
    sys.exit(main())
