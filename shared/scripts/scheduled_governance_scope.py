#!/usr/bin/env python3
"""Refuse the Terraform-managed scheduled-governance job in a deterministic env.

GenieRails creates that job only for legacy envs (deprecated there). A
deterministic env schedules make maintain / make audit-rulebook / make
audit-schema itself (docs/scheduled-governance.md). make runs this first in
setup, validate, check, plan, apply, promote-to, rehearse, release-plan,
release and maintain, before anything is planned or written.

Usage:
    python scheduled_governance_scope.py <path/to/env.auto.tfvars> [<more env files>...]

A missing or unparsable file passes: the other env checks report it.
"""

import importlib
import sys
from pathlib import Path

try:
    from hcl2_guard import import_hcl2
except ModuleNotFoundError:  # imported as scripts.scheduled_governance_scope
    import_hcl2 = importlib.import_module("scripts.hcl2_guard").import_hcl2


def _enabled(value) -> bool:
    return value is True or (isinstance(value, str) and value.strip().lower() == "true")


def error(cfg: dict, env_name: str) -> str | None:
    """The refusal for a deterministic env that enables the job, else None."""
    if cfg.get("governance_mode", "legacy") != "deterministic":
        return None
    if not _enabled(cfg.get("enable_scheduled_governance")):
        return None
    return (
        f"envs/{env_name}/env.auto.tfvars: enable_scheduled_governance = true is not supported in "
        "deterministic mode (GenieRails creates the scheduled-governance job only for legacy envs). "
        f"Remove it, and schedule make maintain ENV={env_name} (from the last released commit, clean "
        f"tree, on the deployment machine) or make audit-rulebook ENV={env_name} / make audit-schema "
        f"ENV={env_name} with your own scheduler. See shared/docs/scheduled-governance.md.")


def main(argv: list[str] | None = None) -> int:
    rc = 0
    for arg in argv if argv is not None else sys.argv[1:]:
        path = Path(arg)
        try:
            hcl2 = import_hcl2()
            cfg = hcl2.loads(path.read_text())
        except Exception:
            continue
        message = error(cfg, path.resolve().parent.name)
        if message:
            print(f"ERROR: {message}", file=sys.stderr)
            rc = 1
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
