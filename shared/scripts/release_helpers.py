#!/usr/bin/env python3
"""Small helpers for the unified production release."""

import argparse
import sys
from pathlib import Path

import hcl2


def clear_old_receipts(env_dir: Path) -> None:
    for name in (".certified.json", ".certified.pending.json"):
        (env_dir / "generated" / name).unlink(missing_ok=True)


def column_mask_policies(env_dir: Path) -> list[str]:
    """Column-mask policy names in the env's generated and promoted config."""
    names = []
    for path in (env_dir / "generated" / "abac.auto.tfvars", env_dir / "data_access" / "abac.auto.tfvars"):
        if path.is_file():
            names += [str(policy.get("name", "?")) for policy in hcl2.load(path.open()).get("fgac_policies") or []
                      if policy.get("policy_type") == "POLICY_TYPE_COLUMN_MASK"]
    return sorted(set(names))


SHARED = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED))  # verify_effective_access: the spec verify-access runs


def _refuse(env: str, reason: str) -> int:
    print(f"release: {reason}; nothing was applied.\n"
          f"  Set verify_key_column in envs/{env}/env.auto.tfvars (or in the source env's\n"
          f"  env.auto.tfvars and re-promote; promote carries it), or pass VERIFY_KEY_COLUMN=<col>,\n"
          f"  or a VERIFY_SPEC=<file> whose column_masks give a key_column for every masked column.",
          file=sys.stderr)
    return 1


def _promoted_tfvars(env_dir: Path) -> Path:
    return env_dir / "data_access" / "abac.auto.tfvars"


def require_mask_proof(env_dir: Path, env: str, key: str, spec: str,
                       account_tfvars: Path | None = None) -> int:
    """Refuse a release whose verify-access could not prove every mask.

    The masked columns to prove are every column a column-mask policy's tags
    match (required_mask_columns), whatever principals a check could use.
    Without a key verify-access skips every mask comparison; with a key it
    derives checks, but drops a mask with no concrete masked tier (e.g.
    "account users" without account groups); an explicit VERIFY_SPEC replaces
    the derived checks altogether. Each must cover every required column. Run
    before the release lock and again after release promotes the live-derived
    config (prod's tag assignments only exist then), before it applies.
    """
    from verify_effective_access import (
        load_spec_from_file, load_spec_from_tfvars, required_mask_columns_from_tfvars,
        unchecked_mask_columns,
    )

    masks = column_mask_policies(env_dir)
    if not masks:
        return 0
    tfvars = _promoted_tfvars(env_dir)
    try:
        required = required_mask_columns_from_tfvars(tfvars) if tfvars.is_file() else set()
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return _refuse(env, f"could not read the masked columns from {tfvars} ({exc})")

    if not spec.strip():
        if not key.strip():
            return _refuse(env, f"no row-pairing key for {env}, so verify-access could not prove "
                                f"its {len(masks)} column mask(s)")
        if not required:
            return 0  # nothing tagged yet; re-checked once release has derived the assignments
        # The checks verify-access will derive, with the account groups it uses.
        if account_tfvars is not None and not account_tfvars.is_file():
            return _refuse(env, f"the account config {account_tfvars} that verify-access reads groups "
                                f"from is missing, so it could not check {len(required)} masked column(s)")
        try:
            checks = load_spec_from_tfvars(tfvars, account_tfvars, key_column=key.strip()).column_masks
        except (OSError, ValueError, KeyError, TypeError) as exc:
            return _refuse(env, f"verify-access could not derive its checks from {tfvars} ({exc})")
        unchecked = unchecked_mask_columns(required, checks)
        if unchecked:
            return _refuse(env, f"verify-access would derive no check for masked column(s) "
                                f"{', '.join(unchecked)}: their policies have no concrete masked group "
                                "to test (e.g. 'account users' with no groups in the account config)")
        return 0

    # verify-access reads VERIFY_SPEC from shared/; resolve it the same way.
    path = Path(spec.strip())
    path = path if path.is_absolute() else SHARED / path
    try:
        checks = load_spec_from_file(path).column_masks
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        return _refuse(env, f"VERIFY_SPEC {path} could not be read ({exc})")
    if not checks:
        return _refuse(env, f"VERIFY_SPEC {path} has no column-mask checks, so it proves none of "
                            f"the {len(masks)} column mask(s)")
    keyless = sorted(f"{c.table}.{c.column}" for c in checks if not c.key_column.strip())
    if keyless:
        return _refuse(env, f"VERIFY_SPEC {path} has mask checks without a key_column: {', '.join(keyless)}")
    unchecked = unchecked_mask_columns(required, checks)
    if unchecked:
        return _refuse(env, f"VERIFY_SPEC {path} does not check masked column(s): {', '.join(unchecked)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("clear-old-receipts", "failed", "require-mask-proof"))
    parser.add_argument("env_dir", type=Path)
    parser.add_argument("--env", default="prod")
    parser.add_argument("--reason", default="the release failed")
    parser.add_argument("--key", default="", help="Resolved verify key column (explicit or saved).")
    parser.add_argument("--spec", default="", help="VERIFY_SPEC, if given.")
    parser.add_argument("--account-tfvars", type=Path, default=None,
                        help="Account abac.auto.tfvars (group names), as verify-access uses.")
    args = parser.parse_args()
    if args.command == "clear-old-receipts":
        clear_old_receipts(args.env_dir)
        return 0
    if args.command == "require-mask-proof":
        return require_mask_proof(args.env_dir, args.env, args.key, args.spec, args.account_tfvars)
    env_file = args.env_dir / "env.auto.tfvars"
    print(f"release: {args.reason}.\n  Business access (table SELECT / Genie CAN_RUN) for {args.env} may be PARTLY APPLIED;\n"
          f"  Terraform granted only what passed the coverage check. To withdraw access, remove the groups\n"
          f"  (or set acl_groups = []) in {env_file}, then run: make apply ENV={args.env}\n"
          f"  Then fix the cause and re-run make release ENV={args.env}.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
