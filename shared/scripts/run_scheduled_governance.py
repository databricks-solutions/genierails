#!/usr/bin/env python3
"""Scheduled steady-state governance wrapper.

This is glue only — it invokes the EXISTING steady-state entrypoints from the
target environment directory, exactly as the `make` targets do. It contains no
classification / re-derive logic of its own:

  audit    -> scripts/audit_schema_drift.py            (== make audit-schema)
  delta    -> generate_abac.py --delta --auth-file ...  (== make generate-delta)
  coverage -> validate_abac.py <config the delta wrote> (== make validate-generated / validate)

The steady-state scripts resolve config via relative paths from the environment
directory (envs/<env>/), so this wrapper `chdir`s there once and shells out to
them using the same interpreter. Running all three steps in a SINGLE process
(``--step all``, the default) is what lets ``coverage`` see the file
``delta`` just regenerated — they share one working tree.

It is meant to be driven by the scheduled Databricks Job defined in
roots/workspace/scheduled_governance.tf as a single ``--step all`` task, but the
per-step modes also run standalone for local testing.

Exit codes:
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

STEPS = ("audit", "delta", "coverage")


def _resolve_env_dir(env_dir_arg: str) -> Path:
    """Resolve the target env directory (absolute, or repo-relative like
    'aws/envs/prod') to an absolute path."""
    p = Path(env_dir_arg)
    if not p.is_absolute():
        p = (REPO_ROOT / p).resolve()
    return p


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
    env_dir.mkdir(parents=True, exist_ok=True)
    print(f"+ materialize env config: {src} -> {env_dir}", flush=True)
    shutil.copytree(src, env_dir, dirs_exist_ok=True, symlinks=True)


def _run(cmd: list[str], cwd: Path) -> int:
    print(f"+ (cd {cwd} && {' '.join(cmd)})", flush=True)
    return subprocess.run(cmd, cwd=str(cwd)).returncode


def _audit(env_dir: Path) -> int:
    return _run(
        [sys.executable, str(SCRIPTS_DIR / "audit_schema_drift.py")],
        cwd=env_dir,
    )


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
    parser.add_argument("--step", choices=(*STEPS, "all"), default="all",
                        help="Which steady-state step to run. Default 'all' runs audit -> delta -> coverage "
                             "in ONE process so coverage sees the config delta just wrote.")
    parser.add_argument("--auth-file", default="auth.auto.tfvars",
                        help="Auth tfvars filename passed to generate_abac.py --delta (default: auth.auto.tfvars).")
    parser.add_argument("--catalog", default="",
                        help="Optional catalog threaded to generate_abac.py --delta (--catalog). "
                             "Empty = auto-derive from the env's uc_tables.")
    parser.add_argument("--config-source", default="",
                        help="Runtime-visible path (UC Volume / workspace files / DBFS mount) holding "
                             "the env config to copy into --env-dir before scanning. Required when the "
                             "Git checkout does not already contain the env dir (envs/ is .gitignore'd).")
    args = parser.parse_args()

    env_dir = _resolve_env_dir(args.env_dir)

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

    steps = STEPS if args.step == "all" else (args.step,)
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
