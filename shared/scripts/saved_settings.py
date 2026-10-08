#!/usr/bin/env python3
"""Settings make saves into an env's env.auto.tfvars so later runs need fewer flags.

verify-key      after a rehearse/release whose verify-access passed a mask check
                paired by an explicit VERIFY_KEY_COLUMN (per its result file),
                save it as verify_key_column.
promote-resolve pick the source env and catalog map for make promote-to from
                FROM / CATALOG_MAP, else the destination's saved promote_from /
                catalog_map; prints "<source> <map>".
promote-save    after a successful promote-to, save them in the destination.

Writes replace the setting in place (or append it), leave every other line
untouched, and are atomic; nothing is ever written into the source env.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from pathlib import Path

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

import hcl2  # noqa: E402

from access_tier_groups import _assignment_spans, _hcl_string, display_path  # noqa: E402

RESERVED_ENVS = ("account", "data_access")
# Env names are plain directory names under this cloud's envs/ (no paths).
ENV_NAME = re.compile(r"^[a-z][a-z0-9_-]*$")


def _parse(text: str, path: Path) -> dict:
    try:
        return hcl2.loads(text)
    except Exception as exc:
        raise ValueError(f"cannot parse {display_path(path)}: {exc}") from exc


def _load(path: Path) -> dict:
    return _parse(path.read_text(), path) if path.is_file() else {}


def _saved(config: dict, name: str) -> str:
    value = config.get(name)
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value or "").strip()


def set_settings(path: Path, values: dict[str, str], comment: str) -> bool:
    """Write string ``values`` into ``path``; returns False when already saved."""
    target = Path(path).resolve()
    text = target.read_text()
    before = _parse(text, target)
    if all(_saved(before, name) == value for name, value in values.items()):
        return False
    updated = text
    appended = []
    for name, value in values.items():
        line = f"{name} = {_hcl_string(value)}"
        spans = _assignment_spans(updated, name)
        if len(spans) > 1:
            raise ValueError(f"{name} is assigned more than once in {display_path(target)}")
        if spans:
            start, end = spans[0]
            updated = updated[:start] + line + updated[end:]
        else:
            appended.append(line)
    if appended:
        updated += "" if updated.endswith("\n") or not updated else "\n"
        updated += f"\n# {comment}\n" + "".join(f"{line}\n" for line in appended)
    after = _parse(updated, target)
    before.update(values)
    if after != before:
        raise ValueError(f"could not safely update {display_path(target)}; set it by hand")
    with tempfile.NamedTemporaryFile(
        mode="w", dir=target.parent, prefix=f".{target.name}.", delete=False
    ) as tmp:
        tmp.write(updated)
        tmp_path = Path(tmp.name)
    os.chmod(tmp_path, target.stat().st_mode & 0o777)
    os.replace(tmp_path, target)
    return True


def key_proven(result_file: Path | None, value: str) -> bool:
    """True when verify-access's result shows a passing mask check paired by ``value``."""
    if result_file is None:
        return False
    try:
        result = json.loads(result_file.read_text())
        return result.get("passed") is True and int(
            (result.get("mask_checks_passed_by_key") or {}).get(value, 0)) > 0
    except (OSError, ValueError, TypeError, AttributeError):
        return False


def save_verify_key(env_file: Path, value: str, result_file: Path | None = None) -> int:
    value = value.strip()
    if not value:
        return 0
    if not key_proven(result_file, value):
        print(
            f"NOTE: VERIFY_KEY_COLUMN={value} not saved: verify-access proved no mask check "
            "paired by it (only row filters ran, or no result)."
        )
        return 0
    if not env_file.is_file():
        print(f"NOTE: {display_path(env_file)} not found; VERIFY_KEY_COLUMN not saved.")
        return 0
    try:
        previous = _saved(_load(env_file), "verify_key_column")
        changed = set_settings(
            env_file, {"verify_key_column": value},
            "Row-pairing key for verify-access (saved after a passing run).",
        )
    except ValueError as exc:
        print(f"NOTE: VERIFY_KEY_COLUMN not saved: {exc}.")
        return 0
    if not changed:
        return 0
    if previous:
        print(
            f"NOTE: verify_key_column in {display_path(env_file)} changed from "
            f"{previous!r} to {value!r} (the VERIFY_KEY_COLUMN you passed)."
        )
    else:
        print(
            f"Saved VERIFY_KEY_COLUMN={value} as verify_key_column in "
            f"{display_path(env_file)}; later runs (and promote) can omit it."
        )
    return 0


def normalize_catalog_map(value: str) -> str:
    pairs: dict[str, str] = {}
    for pair in value.split(","):
        if not pair.strip():
            continue
        src, sep, dest = pair.partition("=")
        src, dest = src.strip(), dest.strip()
        if not sep or not src or not dest or any(c.isspace() for c in src + dest):
            raise ValueError(f"CATALOG_MAP entry {pair.strip()!r} is not <src_catalog>=<dest_catalog>")
        if src in pairs:
            raise ValueError(f"CATALOG_MAP maps {src!r} more than once")
        pairs[src] = dest
    if not pairs:
        raise ValueError("CATALOG_MAP is empty")
    return ",".join(f"{src}={dest}" for src, dest in pairs.items())


def _env_name(label: str, value: str) -> str:
    if not ENV_NAME.fullmatch(value):
        raise ValueError(
            f"{label}={value!r} is not an env name (lowercase letters, digits, '_' or '-', "
            "starting with a letter; no paths)"
        )
    return value


def source_env_dir(envs_dir: Path, source: str) -> Path:
    """The source env as a real directory directly under envs/, or raise ValueError."""
    path = envs_dir / source
    root = envs_dir.resolve()
    if path.is_symlink() or not path.is_dir() or path.resolve().parent != root:
        raise ValueError(f"source env {source!r} not found (no envs/{source}/ directory)")
    if not (path / "env.auto.tfvars").is_file():
        raise ValueError(f"source env {source!r} not found (no envs/{source}/env.auto.tfvars)")
    return path


def destination_env_dir(envs_dir: Path, env: str, env_dir: Path) -> Path:
    """ENV_DIR must be exactly envs/<ENV> (lexically and resolved) and not a symlink."""
    expected = Path(os.path.normpath(os.path.abspath(envs_dir / env)))
    given = Path(os.path.normpath(os.path.abspath(env_dir)))
    if given != expected:
        raise ValueError(f"ENV_DIR={env_dir} is not envs/{env} for ENV={env}; drop ENV_DIR")
    if given.is_symlink():
        raise ValueError(f"envs/{env} is a symlink; promote-to writes only a real envs/{env} directory")
    if given.exists() and (
        not given.is_dir() or given.resolve() != envs_dir.resolve() / env
    ):
        raise ValueError(f"envs/{env} does not resolve to envs/{env} under {envs_dir}")
    return given


def resolve_promote(env: str, env_dir: Path, envs_dir: Path, source: str, catalog_map: str) -> str:
    """Return "<source> <catalog_map>" for promote-to, or raise ValueError."""
    _env_name("ENV", env)
    env_dir = destination_env_dir(envs_dir, env, env_dir)
    env_file = env_dir / "env.auto.tfvars"
    saved = _load(env_file)
    saved_from = _saved(saved, "promote_from")
    saved_map = _saved(saved, "catalog_map")
    source, catalog_map = source.strip(), catalog_map.strip()
    if source:
        _env_name("FROM", source)
    first_use = f'make promote-to ENV={env} FROM=<source_env> CATALOG_MAP="<src_catalog>=<{env}_catalog>"'
    if not source and not saved_from:
        raise ValueError(f"no source env saved for {env}; the first run needs FROM and CATALOG_MAP:\n  {first_use}")
    if not catalog_map and source and saved_from and source != saved_from:
        raise ValueError(
            f"FROM={source} differs from the saved promote_from ({saved_from}); "
            f"pass CATALOG_MAP for the new source too:\n  {first_use.replace('<source_env>', source)}"
        )
    if not catalog_map and not saved_map:
        raise ValueError(f"no catalog map saved for {env}; pass CATALOG_MAP:\n  {first_use}")
    reused = [name for name, given in (("FROM", source), ("CATALOG_MAP", catalog_map)) if not given]
    source = _env_name("FROM" if source else f"promote_from in {display_path(env_file)}", source or saved_from)
    catalog_map = normalize_catalog_map(catalog_map or saved_map)
    if source == env:
        raise ValueError(f"FROM must name another env (FROM={source} is the destination ENV={env})")
    if source in RESERVED_ENVS:
        raise ValueError(f"FROM must be a workspace env, not {source}")
    source_env_dir(envs_dir, source)
    for name, given, kept in (("FROM", source, saved_from), ("CATALOG_MAP", catalog_map, saved_map)):
        if kept and given != kept:
            print(f"  {name}={given} overrides the saved {kept!r}; saved after a successful promote.", file=sys.stderr)
    if reused:
        print(
            f"  Using saved {' and '.join(reused)} from {display_path(env_file)}: "
            f"FROM={source} CATALOG_MAP={catalog_map}", file=sys.stderr,
        )
    return f"{source} {catalog_map}"


def save_promote(env_file: Path, source: str, catalog_map: str) -> int:
    try:
        changed = set_settings(
            env_file, {"promote_from": source, "catalog_map": catalog_map},
            "Saved by make promote-to (source env + catalog map for the next promote).",
        )
    except (OSError, ValueError) as exc:
        print(f"NOTE: FROM/CATALOG_MAP not saved: {exc}.")
        return 0
    if changed:
        env = env_file.resolve().parent.name
        print(
            f"  Saved FROM={source} CATALOG_MAP={catalog_map} in {display_path(env_file)}; "
            f"next time just: make promote-to ENV={env}"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    key = sub.add_parser("verify-key")
    key.add_argument("--env-file", type=Path, required=True)
    key.add_argument("--value", default="")
    key.add_argument("--result-file", type=Path)
    resolve = sub.add_parser("promote-resolve")
    resolve.add_argument("--env", required=True)
    resolve.add_argument("--env-dir", type=Path, required=True)
    resolve.add_argument("--envs-dir", type=Path, required=True)
    resolve.add_argument("--from", dest="source", default="")
    resolve.add_argument("--catalog-map", default="")
    save = sub.add_parser("promote-save")
    save.add_argument("--env-file", type=Path, required=True)
    save.add_argument("--from", dest="source", required=True)
    save.add_argument("--catalog-map", required=True)
    args = parser.parse_args(argv)

    if args.command == "verify-key":
        return save_verify_key(args.env_file, args.value, args.result_file)
    if args.command == "promote-save":
        return save_promote(args.env_file, args.source, args.catalog_map)
    try:
        print(resolve_promote(args.env, args.env_dir, args.envs_dir, args.source, args.catalog_map))
    except ValueError as exc:
        print(f"ERROR: promote-to: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
