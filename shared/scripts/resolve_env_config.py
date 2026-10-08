#!/usr/bin/env python3
"""Resolve verification/evidence settings from an environment configuration."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from scripts.footprint import load_hcl

WAREHOUSE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")


def _valid_warehouse_id(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        return ""
    value = value.strip()
    if not WAREHOUSE_ID_RE.fullmatch(value):
        raise ValueError(f"invalid SQL warehouse ID returned: {value!r}")
    return value


def _warehouse_from_local_state(env_dir: Path) -> str:
    """Read the workspace output without Terraform init, subprocesses, or locks.

    Every root in this repository declares the local backend, and
    terraform_layer.sh pins workspace state to ``env_dir/terraform.tfstate``.
    """
    state_path = env_dir / "terraform.tfstate"
    if not state_path.is_file():
        return ""
    try:
        state = json.loads(state_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"could not read Terraform state {state_path}: {exc}") from exc
    output = (state.get("outputs") or {}).get("sql_warehouse_id") or {}
    if not isinstance(output, dict):
        raise ValueError("Terraform sql_warehouse_id output has an invalid state shape")
    return _valid_warehouse_id(output.get("value"))


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
    allow_unset: bool = False,
) -> str:
    """Resolve a warehouse deterministically, failing on per-space ambiguity.

    ``allow_unset`` returns "" instead of failing when nothing is configured or
    applied yet (the admin-only key check before the first apply).
    """
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
    # terraform_runner/env_name remain accepted for CLI compatibility, but no
    # subprocess is used: invoking terraform_layer.sh would run init and take a
    # lock that a timeout could strand.
    value = _warehouse_from_local_state(env_dir)
    if value or allow_unset:
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
    parser.add_argument("--allow-unset", action="store_true",
                        help="warehouse: print an empty line instead of failing when none is set yet.")
    args = parser.parse_args(argv)
    try:
        if args.setting == "verify-key":
            value = resolve_verify_key(args.env_dir, args.explicit)
        else:
            value = resolve_warehouse(
                args.env_dir, args.explicit,
                terraform_runner=args.terraform_runner, env_name=args.env_name,
                allow_unset=args.allow_unset,
            )
    except ValueError as exc:
        parser.error(str(exc))
    print(value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
