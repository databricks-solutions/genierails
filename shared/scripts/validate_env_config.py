#!/usr/bin/env python3
"""Validate environment-owned deterministic-governance configuration."""
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.hcl2_guard import import_hcl2  # noqa: E402
from agent_files import AgentFileError, merged_config  # noqa: E402
from deterministic_governance import validate_config  # noqa: E402
from scripts.custom_masks import env_errors  # noqa: E402
from scripts.scheduled_governance_scope import error as scheduled_governance_error  # noqa: E402


def main() -> int:
    path = Path(sys.argv[1])
    try:
        hcl2 = import_hcl2()
        cfg = hcl2.loads(path.read_text())
        # The per-agent files (agents/*.auto.tfvars) are validated with it.
        cfg = merged_config(path.parent, cfg)
    except AgentFileError as exc:
        print(f"env config validation: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"env config validation: {path}: {exc}", file=sys.stderr)
        return 1
    # The env's adopted custom masks (custom_masks/, tested_sha256 recomputed).
    library, errors, warnings = env_errors(path.parent, cfg)
    errors += validate_config(
        cfg,
        ack_unclassified=os.environ.get("ACK_UNCLASSIFIED"),
        ack_weaken=os.environ.get("ACK_WEAKEN"),
        library=library,
    ) if library is not None else []
    # GenieRails creates the scheduled-governance job only for legacy envs.
    scheduled = scheduled_governance_error(cfg, path.resolve().parent.name)
    if scheduled:
        errors.append(scheduled)
    for warning in warnings:
        print(f"env config validation: WARNING: {warning}", file=sys.stderr)
    if errors:
        for error in errors:
            print(f"env config validation: {error}", file=sys.stderr)
        return 1
    print("env config validation: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
