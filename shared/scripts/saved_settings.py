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
UC_CATALOG_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


def _hcl_value(value: object) -> str:
    if isinstance(value, dict):
        entries = ", ".join(f"{_hcl_string(str(k))} = {_hcl_string(str(v))}" for k, v in value.items())
        return f"{{ {entries} }}"
    return _hcl_string(str(value))


def set_settings(path: Path, values: dict[str, object], comment: str) -> bool:
    """Write ``values`` into ``path``; returns False when already saved."""
    target = Path(path).resolve()
    text = target.read_text()
    before = _parse(text, target)
    if all(before.get(name) == value for name, value in values.items()):
        return False
    updated = text
    appended = []
    for name, value in values.items():
        line = f"{name} = {_hcl_value(value)}"
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


def catalog_map_dict(value: object) -> dict[str, str]:
    """Accept the preferred HCL map or the legacy comma-separated string."""
    if isinstance(value, dict):
        raw_pairs = list(value.items())
    elif isinstance(value, str):
        raw_pairs = []
        for pair in value.split(","):
            if not pair.strip():
                continue
            src, sep, dest = pair.partition("=")
            if not sep:
                raise ValueError(f"catalog_map entry {pair.strip()!r} is not <src_catalog>=<dest_catalog>")
            raw_pairs.append((src, dest))
    elif value is None:
        raw_pairs = []
    else:
        raise ValueError("catalog_map must be an HCL map or a string of src=dest pairs")

    pairs: dict[str, str] = {}
    for raw_src, raw_dest in raw_pairs:
        src, dest = str(raw_src).strip(), str(raw_dest).strip()
        if any(char in src + dest for char in "<>"):
            raise ValueError("catalog_map contains an unfilled <...> placeholder")
        if not src or not dest or any(c.isspace() for c in src + dest):
            raise ValueError(f"catalog_map entry {src + '=' + dest!r} is not <src_catalog>=<dest_catalog>")
        invalid = [name for name in (src, dest) if not UC_CATALOG_NAME.fullmatch(name)]
        if invalid:
            raise ValueError(
                f"catalog_map catalog name {invalid[0]!r} is not a valid UC identifier "
                "(letters, digits and underscores; must not start with a digit)"
            )
        if src in pairs:
            raise ValueError(f"catalog_map maps {src!r} more than once")
        pairs[src] = dest
    if not pairs:
        raise ValueError("catalog_map is missing or empty")
    duplicates = sorted({dest for dest in pairs.values() if list(pairs.values()).count(dest) > 1})
    if duplicates:
        raise ValueError(f"catalog_map maps more than one source catalog to: {', '.join(duplicates)}")
    return pairs


def normalize_catalog_map(value: object) -> str:
    pairs = catalog_map_dict(value)
    return ",".join(f"{src}={dest}" for src, dest in pairs.items())


def validate_source_catalogs(source_dir: Path, catalog_map: str, env_file: Path) -> None:
    from scripts.footprint import FootprintError, resolve_footprint
    from genie_space_placeholder import placeholder_error

    source_file = source_dir / "env.auto.tfvars"
    placeholder = placeholder_error(_load(source_file), source_file)
    if placeholder:
        raise ValueError(placeholder)

    try:
        tables = resolve_footprint(source_dir)
    except FootprintError as exc:
        raise ValueError(str(exc)) from exc
    catalogs = sorted({table.split(".", 1)[0] for table in tables if table.count(".") >= 2})
    if not catalogs:
        for subdir in ("generated", "data_access"):
            config_path = source_dir / subdir / "abac.auto.tfvars"
            if not config_path.is_file():
                continue
            try:
                config = _load(config_path)
            except ValueError:
                continue
            catalogs = sorted({
                str(assignment.get("entity_name") or "").split(".", 1)[0]
                for assignment in config.get("tag_assignments") or []
                if str(assignment.get("entity_name") or "").count(".") >= 2
            } - {""})
            if catalogs:
                break
    if not catalogs:
        raise ValueError(f"no catalogs are used by source env {source_dir.name!r}; run make generate first")
    pairs = catalog_map_dict(catalog_map)
    unknown = sorted(set(pairs) - set(catalogs))
    missing = sorted(set(catalogs) - set(pairs))
    setting = f"catalog_map in {display_path(env_file)}"
    if unknown:
        raise ValueError(f"{setting} has unknown source catalog(s): {', '.join(unknown)}")
    if missing:
        raise ValueError(f"{setting} is missing source catalog(s): {', '.join(missing)}")


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
    saved_map = saved.get("catalog_map")
    source, catalog_map = source.strip(), catalog_map.strip()
    if source:
        _env_name("FROM", source)
    first_use = f'make promote-to ENV={env} FROM=<source_env> CATALOG_MAP="<src_catalog>=<{env}_catalog>"'
    if not source and not saved_from:
        raise ValueError(f"promote_from in {display_path(env_file)} is missing; set it or pass FROM")
    if not catalog_map and source and saved_from and source != saved_from:
        raise ValueError(
            f"FROM={source} differs from the saved promote_from ({saved_from}); "
            f"pass CATALOG_MAP for the new source too:\n  {first_use.replace('<source_env>', source)}"
        )
    if not catalog_map and not saved_map:
        raise ValueError(f"catalog_map in {display_path(env_file)} is missing or empty; set it or pass CATALOG_MAP")
    reused = [name for name, given in (("FROM", source), ("CATALOG_MAP", catalog_map)) if not given]
    source = _env_name("FROM" if source else f"promote_from in {display_path(env_file)}", source or saved_from)
    try:
        catalog_map = normalize_catalog_map(catalog_map or saved_map)
    except ValueError as exc:
        raise ValueError(f"catalog_map in {display_path(env_file)}: {exc}") from exc
    if source == env:
        raise ValueError(f"FROM must name another env (FROM={source} is the destination ENV={env})")
    if source in RESERVED_ENVS:
        raise ValueError(f"FROM must be a workspace env, not {source}")
    source_dir = source_env_dir(envs_dir, source)
    validate_source_catalogs(source_dir, catalog_map, env_file)
    try:
        normalized_saved_map = normalize_catalog_map(saved_map) if saved_map else ""
        saved_map_display = repr(normalized_saved_map)
    except ValueError:
        # An explicit valid CATALOG_MAP overrides even a stale/template saved value.
        normalized_saved_map = str(saved_map)
        saved_map_display = normalized_saved_map
    for name, given, kept, kept_display in (
        ("FROM", source, saved_from, repr(saved_from)),
        ("CATALOG_MAP", catalog_map, normalized_saved_map, saved_map_display),
    ):
        if kept and given != kept:
            print(f"  {name}={given} overrides the saved {kept_display}; saved after a successful promote.", file=sys.stderr)
    if reused:
        print(
            f"  Using saved {' and '.join(reused)} from {display_path(env_file)}: "
            f"FROM={source} CATALOG_MAP={catalog_map}", file=sys.stderr,
        )
    return f"{source} {catalog_map}"


def save_promote(env_file: Path, source: str, catalog_map: str) -> int:
    try:
        changed = set_settings(
            env_file, {"promote_from": source, "catalog_map": catalog_map_dict(catalog_map)},
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
        prefix = "" if str(exc).startswith("replace <") else "promote-to: "
        print(f"ERROR: {prefix}{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
