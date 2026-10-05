#!/usr/bin/env python3
"""Resolve verification/evidence settings from an environment configuration."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from scripts.footprint import load_hcl

WAREHOUSE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
TERRAFORM_TIMEOUT_SECONDS = 30


def _valid_warehouse_id(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        return ""
    value = value.strip()
    if not WAREHOUSE_ID_RE.fullmatch(value):
        raise ValueError(f"invalid SQL warehouse ID returned: {value!r}")
    return value


def resolve_verify_key(env_dir: Path, explicit: str = "") -> str:
    """Prefer an explicit make value, then the persisted environment value."""
    return explicit.strip() or str(
        load_hcl(env_dir / "env.auto.tfvars").get("verify_key_column") or ""
    ).strip()


def resolve_warehouse(
    env_dir: Path,
    explicit: str = "",
    *,
    terraform_runner: Path | None = None,
    env_name: str = "",
) -> str:
    """Resolve a warehouse deterministically, failing on per-space ambiguity."""
    if explicit.strip():
        return _valid_warehouse_id(explicit)
    config = load_hcl(env_dir / "env.auto.tfvars")
    shared = str(config.get("sql_warehouse_id") or "").strip()
    if shared:
        return _valid_warehouse_id(shared)
    # Empty per-space values mean "inherit the shared/auto-created warehouse".
    # They do not conflict with one unique explicitly configured per-space ID.
    per_space = {
        str(space.get("sql_warehouse_id") or "").strip()
        for space in config.get("genie_spaces") or []
        if str(space.get("sql_warehouse_id") or "").strip()
    }
    if len(per_space) > 1:
        raise ValueError(
            "ambiguous warehouse configuration: genie_spaces contain multiple "
            f"sql_warehouse_id values: {', '.join(sorted(per_space))}"
        )
    if per_space:
        return _valid_warehouse_id(next(iter(per_space)))
    if terraform_runner and env_name:
        if not terraform_runner.is_file():
            raise ValueError(f"Terraform runner not found: {terraform_runner}")
        try:
            result = subprocess.run(
                [str(terraform_runner), "workspace", env_name, "output", "-json", "sql_warehouse_id"],
                env={**os.environ, "LAYER_ENV_DIR": str(env_dir)},
                text=True,
                capture_output=True,
                timeout=TERRAFORM_TIMEOUT_SECONDS,
            )
        except FileNotFoundError as exc:
            raise ValueError(f"Terraform runner not found: {terraform_runner}") from exc
        except subprocess.TimeoutExpired as exc:
            raise ValueError(
                f"Terraform warehouse output timed out after {TERRAFORM_TIMEOUT_SECONDS}s"
            ) from exc
        if result.returncode != 0:
            detail = result.stderr.strip() or "no diagnostic output"
            raise ValueError(f"Terraform warehouse output failed: {detail}")
        terraform_value = None
        for line in reversed(result.stdout.splitlines()):
            try:
                terraform_value = json.loads(line)
                break
            except json.JSONDecodeError:
                continue
        if terraform_value is None:
            raise ValueError("Terraform warehouse output was missing or invalid JSON")
        value = _valid_warehouse_id(terraform_value)
        if value:
            return value
    raise ValueError(
        "no SQL warehouse could be resolved; set WAREHOUSE_ID, sql_warehouse_id, "
        "one unique genie_spaces[].sql_warehouse_id, or apply the workspace layer"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("setting", choices=("verify-key", "warehouse"))
    parser.add_argument("--env-dir", type=Path, required=True)
    parser.add_argument("--explicit", default="")
    parser.add_argument("--terraform-runner", type=Path)
    parser.add_argument("--env-name", default="")
    args = parser.parse_args(argv)
    try:
        if args.setting == "verify-key":
            value = resolve_verify_key(args.env_dir, args.explicit)
        else:
            value = resolve_warehouse(
                args.env_dir, args.explicit,
                terraform_runner=args.terraform_runner, env_name=args.env_name,
            )
    except ValueError as exc:
        parser.error(str(exc))
    print(value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
