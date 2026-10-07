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


def _derived_mask_columns(env_dir: Path, account_tfvars: Path | None) -> set[tuple[str, str]]:
    """(table, column) of every mask check verify-access derives from the promoted config."""
    from verify_effective_access import load_spec_from_tfvars

    tfvars = env_dir / "data_access" / "abac.auto.tfvars"
    if not tfvars.is_file():
        return set()
    account = account_tfvars if account_tfvars and account_tfvars.is_file() else None
    spec = load_spec_from_tfvars(tfvars, account)
    return {(c.table.lower(), c.column.lower()) for c in spec.column_masks}


def require_mask_proof(env_dir: Path, env: str, key: str, spec: str,
                       account_tfvars: Path | None = None) -> int:
    """Refuse a release whose verify-access could not prove every mask.

    verify-access pairs rows by a key column to prove masking. Without a key
    it skips every mask comparison, and an explicit VERIFY_SPEC replaces the
    derived checks altogether, so a spec without keyed mask checks (row
    filters only, say) proves no mask either. Run before the release lock and
    again after release promotes the live-derived config, before it applies.
    """
    masks = column_mask_policies(env_dir)
    if not masks:
        return 0
    if not spec.strip():
        if key.strip():
            return 0  # verify-access derives a keyed check for every masked column
        return _refuse(env, f"no row-pairing key for {env}, so verify-access could not prove "
                            f"its {len(masks)} column mask(s)")

    from verify_effective_access import load_spec_from_file

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
    try:
        required = _derived_mask_columns(env_dir, account_tfvars)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return _refuse(env, f"could not derive the masked columns to check VERIFY_SPEC against ({exc})")
    missing = sorted(f"{t}.{c}" for t, c in required - {(c.table.lower(), c.column.lower()) for c in checks})
    if missing:
        return _refuse(env, f"VERIFY_SPEC {path} does not check masked column(s): {', '.join(missing)}")
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
