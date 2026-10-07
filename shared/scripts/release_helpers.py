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


def require_mask_proof(env_dir: Path, env: str, key: str, spec: str) -> int:
    """Refuse a release whose verify-access could only skip its mask checks.

    verify-access pairs rows by a key column to prove masking; without one it
    skips every mask comparison, and a release would then report success
    without having proven a single mask. Checked before anything is applied.
    """
    if key.strip() or spec.strip():
        return 0
    masks = column_mask_policies(env_dir)
    if not masks:
        return 0
    print(f"release: no row-pairing key for {env}, so verify-access could not prove its "
          f"{len(masks)} column mask(s); nothing was applied.\n"
          f"  Set verify_key_column in {env_dir / 'env.auto.tfvars'} (or in the source env's\n"
          f"  env.auto.tfvars and re-promote; promote carries it), or pass\n"
          f"  VERIFY_KEY_COLUMN=<col> (VERIFY_SPEC=<file> for tables without a shared key).",
          file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("clear-old-receipts", "failed", "require-mask-proof"))
    parser.add_argument("env_dir", type=Path)
    parser.add_argument("--env", default="prod")
    parser.add_argument("--reason", default="the release failed")
    parser.add_argument("--key", default="", help="Resolved verify key column (explicit or saved).")
    parser.add_argument("--spec", default="", help="VERIFY_SPEC, if given.")
    args = parser.parse_args()
    if args.command == "clear-old-receipts":
        clear_old_receipts(args.env_dir)
        return 0
    if args.command == "require-mask-proof":
        return require_mask_proof(args.env_dir, args.env, args.key, args.spec)
    env_file = args.env_dir / "env.auto.tfvars"
    print(f"release: {args.reason}.\n  Business access (table SELECT / Genie CAN_RUN) for {args.env} may be PARTLY APPLIED;\n"
          f"  Terraform granted only what passed the coverage check. To withdraw access, remove the groups\n"
          f"  (or set acl_groups = []) in {env_file}, then run: make apply ENV={args.env}\n"
          f"  Then fix the cause and re-run make release ENV={args.env}.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
