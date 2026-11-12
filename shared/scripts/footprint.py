#!/usr/bin/env python3
"""Resolve the effective UC table footprint for an environment."""

from __future__ import annotations

import argparse
import importlib
from pathlib import Path

try:
    from hcl2_guard import import_hcl2
except ModuleNotFoundError:  # imported as scripts.footprint
    import_hcl2 = importlib.import_module("scripts.hcl2_guard").import_hcl2


def load_env_config(env_dir: str | Path) -> dict:
    """envs/<env>/env.auto.tfvars with the per-agent files merged in (shared/agent_files.py)."""
    import sys

    shared = str(Path(__file__).resolve().parents[1])
    if shared not in sys.path:
        sys.path.insert(0, shared)
    from agent_files import AgentFileError, merged_config

    try:
        return merged_config(Path(env_dir))
    except AgentFileError as exc:
        raise FootprintError(str(exc)) from exc


class FootprintError(ValueError):
    """A footprint file cannot be parsed or has an unsafe shape."""


def load_hcl(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        hcl2 = import_hcl2()
        with path.open() as handle:
            value = hcl2.load(handle)
    except Exception as exc:
        if path.name != "auth.auto.tfvars":
            raise FootprintError(f"cannot parse {path}: {exc}") from exc
        value = None
    if value is None:
        # Raised outside the handler with no parser text, which can echo a
        # malformed secret (also via __context__).
        raise FootprintError(f"cannot parse {path}: not valid HCL") from None
    if not isinstance(value, dict):
        raise FootprintError(f"invalid {path}: expected top-level object")
    return value


def load_discovered_footprint(env_dir: str | Path) -> tuple[list[str], dict[str, list[str]]]:
    """Load and validate tool-owned discovered tables and attribution."""
    path = Path(env_dir) / "data_access" / "discovered_uc_tables.auto.tfvars"
    try:
        config = load_hcl(path)
        tables = config.get("discovered_uc_tables", [])
        agents = config.get("discovered_table_agents", {})
        if not isinstance(tables, list) or any(not isinstance(t, str) for t in tables):
            raise FootprintError("discovered_uc_tables must be a list of strings")
        if not isinstance(agents, dict) or any(
            not isinstance(table, str)
            or not isinstance(owners, list)
            or any(not isinstance(owner, str) for owner in owners)
            for table, owners in agents.items()
        ):
            raise FootprintError(
                "discovered_table_agents must map table strings to lists of agent names"
            )
        unknown_attribution = [table for table in agents if table not in tables]
        if unknown_attribution:
            raise FootprintError(
                "discovered_table_agents contains table keys absent from "
                "discovered_uc_tables: " + ", ".join(unknown_attribution)
            )
    except FootprintError as exc:
        raise FootprintError(
            f"invalid discovered footprint {path}: {exc}; re-run "
            "`make generate ENV=<env>`"
        ) from exc
    return list(dict.fromkeys(tables)), agents


def resolve_footprint(
    env_dir: str | Path, env_file: str | Path | None = None, *, discovered: bool = True
) -> list[str]:
    """Return top-level, per-space, and (unless ``discovered`` is false)
    discovered tables in stable order."""
    env_dir = Path(env_dir)
    config = load_hcl(Path(env_file)) if env_file is not None else load_env_config(env_dir)
    tables = list(config.get("uc_tables") or [])
    for space in config.get("genie_spaces") or []:
        tables.extend(space.get("uc_tables") or [])
    if discovered:
        tables.extend(load_discovered_footprint(env_dir)[0])
    return list(dict.fromkeys(table for table in tables if table))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("env_dir")
    parser.add_argument("--catalogs", action="store_true")
    args = parser.parse_args(argv)
    try:
        values = resolve_footprint(args.env_dir)
    except FootprintError as exc:
        print(f"ERROR: {exc}")
        return 1
    if args.catalogs:
        values = sorted({table.split(".")[0] for table in values if table.count(".") >= 2})
    print(", ".join(values) if values else "(none detected)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
