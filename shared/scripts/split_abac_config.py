#!/usr/bin/env python3
"""Split a generated ABAC draft into account, governance, and workspace tfvars."""

from __future__ import annotations

import errno
import json
import os
import re
import secrets
import stat
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import hcl2
except ImportError:
    print("ERROR: python-hcl2 is required. Install with:")
    print("  pip install python-hcl2")
    sys.exit(2)

from governance_policies import is_library_treatment_value  # noqa: E402
from tag_vocabulary import REGISTRY  # noqa: E402


ACCOUNT_KEYS = (
    "groups",
    "group_members",
    "tag_policies",
)

DATA_ACCESS_KEYS = (
    "groups",
    "tag_assignments",
    "fgac_policies",
    "genie_space_acl_groups",
    "genie_space_id_to_name",
)

WORKSPACE_KEYS = (
    "groups",
    # New multi-space format.
    "genie_space_configs",
    "genie_space_id_to_name",
    # Legacy single-space keys (kept for backward compatibility with old generated configs).
    "genie_space_title",
    "genie_space_description",
    "genie_sample_questions",
    "genie_instructions",
    "genie_benchmarks",
    "genie_sql_filters",
    "genie_sql_expressions",
    "genie_sql_measures",
    "genie_join_specs",
)

IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_hcl(path: Path) -> dict:
    with open(path) as f:
        return hcl2.load(f)


def load_generated_config(source_path: Path) -> dict:
    """Load the authored draft plus its tool-owned derived ACL sidecar."""
    full_cfg = load_hcl(source_path)
    derived_path = source_path.with_name(
        "genie_space_derived_acl_groups.auto.tfvars"
    )
    if derived_path.exists():
        derived_cfg = load_hcl(derived_path)
        full_cfg["genie_space_derived_acl_groups"] = derived_cfg.get(
            "genie_space_derived_acl_groups", {}
        )
        full_cfg["genie_space_legacy_mode"] = derived_cfg.get(
            "genie_space_legacy_mode", False
        )
    return full_cfg


def quote_key(key: str) -> str:
    if IDENT_RE.match(key):
        return key
    return json.dumps(key)


def resolve_space_acl(
    space_name: str,
    space: dict,
    derived_acls: dict,
    *,
    allow_legacy_explicit: bool = False,
) -> list[str]:
    """Resolve tool-owned ACLs, with explicit draft fields allowed only in legacy mode."""
    if space_name in derived_acls:
        value = derived_acls[space_name]
    elif allow_legacy_explicit and "acl_groups" in space:
        value = space["acl_groups"]
    elif allow_legacy_explicit and "genie_acl_groups" in space:
        value = space["genie_acl_groups"]
    else:
        advice = (
            "set the legacy top-level genie_acl_groups list"
            if allow_legacy_explicit
            else "set acl_groups on its genie_spaces[] entry in env.auto.tfvars"
        )
        raise ValueError(
            f"Genie space {space_name!r} has no resolved ACL. Run `make generate` "
            f"to re-derive ACLs or {advice}."
        )
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(
            f"Genie space {space_name!r} ACL must be a list of group names "
            "(explicit [] means no business access); interpolation/null is not allowed."
        )
    return list(value)


def render_scalar(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value)


def render_value(value, indent: int = 0) -> str:
    pad = " " * indent
    if isinstance(value, dict):
        if not value:
            return "{}"
        lines = ["{"]
        for key, item in value.items():
            rendered = render_value(item, indent + 2)
            lines.append(f"{pad}  {quote_key(key)} = {rendered}")
        lines.append(f"{pad}}}")
        return "\n".join(lines)
    if isinstance(value, list):
        if not value:
            return "[]"
        if all(not isinstance(item, (dict, list)) for item in value):
            return "[" + ", ".join(render_scalar(item) for item in value) + "]"
        lines = ["["]
        for item in value:
            rendered = render_value(item, indent + 2)
            lines.append(f"{pad}  {rendered},")
        lines.append(f"{pad}]")
        return "\n".join(lines)
    return render_scalar(value)


def merge_groups(existing: dict, new: dict) -> dict:
    merged = dict(existing or {})
    for name, cfg in (new or {}).items():
        old_cfg = merged.get(name, {})
        old_desc = (
            old_cfg.get("description", "") if isinstance(old_cfg, dict) else ""
        )
        new_desc = cfg.get("description", "") if isinstance(cfg, dict) else ""
        merged[name] = {"description": old_desc or new_desc}
    return merged


def merge_group_members(existing: dict, new: dict) -> dict:
    merged = {k: list(v) for k, v in (existing or {}).items()}
    for group, members in (new or {}).items():
        current = merged.setdefault(group, [])
        for member in members:
            if member not in current:
                current.append(member)
    return merged


def merge_tag_policies(existing: list, new: list) -> list:
    """Union tag policies by key; duplicate keys union their values.

    Existing keys from other environments are preserved so that promoting
    an older env never silently drops policies added by a newer environment.
    """

    def _normalize_policy(policy: dict) -> dict | None:
        key = policy.get("key")
        if not key:
            return None
        canonical_key = REGISTRY.canonical_key(key)
        values: list[str] = []
        for raw_value in policy.get("values", []) or []:
            canonical_value = REGISTRY.canonical_value(canonical_key, raw_value)
            # Deterministic gr_treatment values aren't in the registry; any env
            # may add them to the shared account tag policy.
            if (REGISTRY.is_allowed_value(canonical_key, canonical_value) is False
                    and not is_library_treatment_value(canonical_key, canonical_value)):
                raise ValueError(
                    f"tag_policy '{canonical_key}' contains unknown canonical value "
                    f"'{canonical_value}'"
                )
            if canonical_value not in values:
                values.append(canonical_value)
        return {
            **policy,
            "key": canonical_key,
            "values": values,
        }

    def _merge_policy(target: dict, source: dict) -> dict:
        merged = dict(target or {})
        merged["key"] = source["key"]
        merged["description"] = (
            source.get("description")
            or merged.get("description")
            or ""
        )
        merged_values = list(merged.get("values", []) or [])
        for value in source.get("values", []) or []:
            if value not in merged_values:
                merged_values.append(value)
        merged["values"] = merged_values
        return merged

    merged: dict[str, dict] = {}
    for tp in (existing or []):
        tp = _normalize_policy(tp)
        if not tp:
            continue
        key = tp.get("key")
        if not key:
            continue
        merged[key] = _merge_policy(merged.get(key, {}), tp)
    for tp in (new or []):
        tp = _normalize_policy(tp)
        if not tp:
            continue
        key = tp.get("key")
        if not key:
            continue
        merged[key] = _merge_policy(merged.get(key, {}), tp)
    return list(merged.values())


def build_account_config(full_cfg: dict, existing_cfg: dict | None) -> dict:
    existing_cfg = existing_cfg or {}
    cfg: dict = {}

    groups = merge_groups(
        existing_cfg.get("groups", {}),
        full_cfg.get("groups", {}),
    )
    if groups:
        cfg["groups"] = groups

    group_members = merge_group_members(
        existing_cfg.get("group_members", {}),
        full_cfg.get("group_members", {}),
    )
    if group_members:
        cfg["group_members"] = group_members

    tag_policies = merge_tag_policies(
        existing_cfg.get("tag_policies", []),
        full_cfg.get("tag_policies", []),
    )
    if tag_policies:
        cfg["tag_policies"] = tag_policies

    return cfg


def _strip_var_refs(space_cfg: dict) -> dict:
    """Remove ${var.*} string values from a genie_space_configs entry.

    The LLM sometimes hallucinates Terraform variable references for optional
    list fields (benchmarks, sql_filters, etc.).  These are illegal in .tfvars
    files, so strip them.
    """
    return {
        k: v for k, v in space_cfg.items()
        if k in {"acl_groups", "genie_acl_groups"}
        or not (isinstance(v, str) and "${var." in v)
    }


def _convert_legacy_to_genie_space_configs(cfg: dict) -> dict | None:
    """Convert legacy single-space keys to genie_space_configs format.

    Legacy keys: genie_space_title, genie_instructions, genie_benchmarks,
    genie_sql_filters, genie_sql_expressions, genie_sql_measures, etc.

    Returns a genie_space_configs dict, or None if no legacy keys found.
    """
    title = cfg.get("genie_space_title", "")
    if isinstance(title, list):
        title = title[0] if title else ""
    if not title:
        return None

    def _val(key, default=""):
        v = cfg.get(key, default)
        if isinstance(v, list) and len(v) == 1 and isinstance(v[0], (str, list)):
            v = v[0]
        return v

    space_config = {
        "title": title,
        "description": _val("genie_space_description", ""),
        "instructions": _val("genie_instructions", ""),
        "sample_questions": _val("genie_sample_questions", []),
        "benchmarks": _val("genie_benchmarks", []),
        "sql_filters": _val("genie_sql_filters", []),
        "sql_expressions": _val("genie_sql_expressions", []),
        "sql_measures": _val("genie_sql_measures", []),
        "join_specs": _val("genie_join_specs", []),
    }
    if "genie_acl_groups" in cfg:
        space_config["genie_acl_groups"] = cfg["genie_acl_groups"]
    elif "acl_groups" in cfg:
        space_config["acl_groups"] = cfg["acl_groups"]
    # Strip empty values
    space_config = {
        k: v for k, v in space_config.items()
        if k in {"acl_groups", "genie_acl_groups"} or v not in ("", [], {}, None)
    }

    print(f"  [SPLIT] Converted legacy genie keys to genie_space_configs[\"{title}\"]")
    return {title: space_config}


def build_workspace_config(full_cfg: dict) -> dict:
    cfg: dict = {}
    for key in WORKSPACE_KEYS:
        if key not in full_cfg:
            continue
        value = full_cfg[key]
        if value in ("", [], {}):
            continue
        # Sanitize genie_space_configs: strip ${var.*} refs from each space
        if key == "genie_space_configs" and isinstance(value, dict):
            value = {
                name: _strip_var_refs(sc) if isinstance(sc, dict) else sc
                for name, sc in value.items()
            }
        # Also strip top-level legacy keys with ${var.*} values
        if isinstance(value, str) and "${var." in value:
            continue
        cfg[key] = value

    # Convert legacy single-space keys to genie_space_configs if needed
    is_legacy = full_cfg.get("genie_space_legacy_mode") is True
    if is_legacy:
        converted = _convert_legacy_to_genie_space_configs(full_cfg)
        if converted:
            cfg["genie_space_configs"] = converted
            # Remove legacy keys since they're now in genie_space_configs
            for legacy_key in (
                "genie_space_title", "genie_space_description",
                "genie_sample_questions", "genie_instructions",
                "genie_benchmarks", "genie_sql_filters",
                "genie_sql_expressions", "genie_sql_measures",
                "genie_join_specs",
            ):
                cfg.pop(legacy_key, None)

    derived_acls = full_cfg.get("genie_space_derived_acl_groups") or {}
    genie_configs = cfg.get("genie_space_configs") or {}
    if not is_legacy and not genie_configs and derived_acls:
        genie_configs = {name: {} for name in derived_acls}
        cfg["genie_space_configs"] = genie_configs
    if isinstance(genie_configs, dict):
        resolved_configs = {}
        for name, space in genie_configs.items():
            if not isinstance(space, dict):
                resolved_configs[name] = space
                continue
            resolved = dict(space)
            resolved["acl_groups"] = resolve_space_acl(
                name, space, derived_acls, allow_legacy_explicit=is_legacy
            )
            resolved.pop("genie_acl_groups", None)
            resolved_configs[name] = resolved
        cfg["genie_space_configs"] = resolved_configs

    return cfg


def build_data_access_config(full_cfg: dict) -> dict:
    cfg: dict = {}
    for key in DATA_ACCESS_KEYS:
        if key not in full_cfg:
            continue
        value = full_cfg[key]
        if value in ("", [], {}):
            continue
        cfg[key] = value
    genie_configs = full_cfg.get("genie_space_configs") or {}
    is_legacy = full_cfg.get("genie_space_legacy_mode") is True
    if is_legacy:
        genie_configs = _convert_legacy_to_genie_space_configs(full_cfg) or {}
    elif not genie_configs and (full_cfg.get("genie_space_derived_acl_groups") or {}):
        genie_configs = {
            name: {} for name in full_cfg["genie_space_derived_acl_groups"]
        }
    derived_acls = full_cfg.get("genie_space_derived_acl_groups") or {}
    if isinstance(genie_configs, dict) and genie_configs:
        cfg["genie_space_acl_groups"] = {
            name: resolve_space_acl(
                name, space, derived_acls, allow_legacy_explicit=is_legacy
            )
            for name, space in genie_configs.items()
            if isinstance(space, dict)
        }
    return cfg


def write_tfvars(
    path: Path,
    config: dict,
    header: str,
    ordered_keys: tuple[str, ...],
):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_tfvars(config, header, ordered_keys))


def render_tfvars(config: dict, header: str, ordered_keys: tuple[str, ...]) -> str:
    pieces = [header.strip(), ""]
    handled = set()

    for key in ordered_keys:
        if key not in config:
            continue
        handled.add(key)
        pieces.append(f"{key} = {render_value(config[key])}")
        pieces.append("")

    for key in config:
        if key in handled:
            continue
        pieces.append(f"{key} = {render_value(config[key])}")
        pieces.append("")

    return "\n".join(pieces).rstrip() + "\n"


ACCOUNT_HEADER = """
# ============================================================================
# ACCOUNT-OWNED ABAC CONFIG
# ============================================================================
# Generated from the workspace draft and merged into shared account state.
# Owns groups, optional group membership, and tag policy definitions.
# Tag policies are account-scoped and shared across all workspace environments.
# ============================================================================
"""


class AccountFileError(RuntimeError):
    """The shared account file can't be merged safely; nothing was written."""


def _check_account_path(path: Path) -> None:
    parent = path.parent
    if parent.is_symlink() or not parent.is_dir():
        raise AccountFileError(f"{parent} is a symlink or not a directory; the shared account file is only "
                               "written in a real envs/account directory. Nothing was written.")
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise AccountFileError(f"{path} is a symlink or not a regular file; GenieRails never writes the shared "
                               "account file through a link. Replace it with a regular file (or remove it). "
                               "Nothing was written.")


def _atomic_write(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` by temp file + rename in its directory, never following a link."""
    mode = stat.S_IMODE(path.stat().st_mode) if path.is_file() else 0o644
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
            os.fchmod(handle.fileno(), mode)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def merge_account_file(full_cfg: dict, account_path: Path, lock_nonce: str = "") -> bool:
    """Merge a draft's account part into the shared account file; False when it was already there.

    Every env's split (and rehearse's and release's account step) merges into
    this one file, so the read-merge-write runs under the account env's lock
    mutex (scripts/environment_lock.py mutex), losing no other env's part,
    and the write is atomic and never through a link. A change also respects
    the account lock (the one account-plan and apply-account hold): while a
    live run holds it, only that run may write, by passing the nonce its lock
    printed (``lock_nonce``); anyone else is refused with nothing written. An
    unchanged part writes nothing and needs no lock. A free or stale lock
    (its pid dead, e.g. kill -9) lets the write through: no lock can be taken
    while the mutex is held, so the write is still exclusive.
    """
    from scripts import environment_lock

    account_path.parent.mkdir(parents=True, exist_ok=True)
    _check_account_path(account_path)
    try:
        with environment_lock.mutex(account_path.parent):
            _check_account_path(account_path)
            existing_text = account_path.read_text() if account_path.exists() else None
            existing_account_cfg = load_hcl(account_path) if existing_text is not None else None
            text = render_tfvars(build_account_config(full_cfg, existing_account_cfg), ACCOUNT_HEADER, ACCOUNT_KEYS)
            if text == existing_text:
                return False
            if not (lock_nonce and environment_lock.holds(account_path.parent, lock_nonce)):
                held = environment_lock.live_holder(account_path.parent)
                if held is not None:
                    raise AccountFileError(
                        f"the shared account layer is locked by another run (make {held.get('owner', '?')}, pid "
                        f"{held.get('pid', '?')} on {held.get('host', '?')}, since {held.get('started_at', '?')}), "
                        f"so {account_path} can't change now; nothing was written. Re-run when that run finishes.")
            _atomic_write(account_path, text)
            return True
    except RuntimeError as exc:  # environment_lock.lock_path: an unusable GENIERAILS_STATE_DIR
        if isinstance(exc, AccountFileError):
            raise
        raise AccountFileError(f"{exc}. Nothing was written.") from exc


def read_lock_nonce(path: Path) -> str:
    """The account lock's nonce from the file the lock holder wrote; refuses anything but a private file.

    One open, never through a link (O_NOFOLLOW; O_NONBLOCK so a FIFO can't
    hang it), then fstat of that same descriptor: a regular file of this
    user, mode exactly 0600. The nonce is read from that descriptor, so the
    file checked is the file read.
    """
    refused = f"the account lock nonce file {path} "
    tail = "; nothing was written."
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as exc:
        if exc.errno == errno.ELOOP or path.is_symlink():
            raise AccountFileError(refused + "is a symlink" + tail) from exc
        raise AccountFileError(refused + f"can't be opened ({exc.strerror})" + tail) from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise AccountFileError(refused + "is not a regular file" + tail)
        if info.st_uid != os.getuid():
            raise AccountFileError(refused + f"is owned by uid {info.st_uid}, not this user ({os.getuid()})" + tail)
        if stat.S_IMODE(info.st_mode) != 0o600:
            raise AccountFileError(refused + f"has mode {stat.S_IMODE(info.st_mode):04o}; it must be exactly 0600" + tail)
        data = b""
        while len(data) <= 4096:
            chunk = os.read(fd, 4097 - len(data))
            if not chunk:
                break
            data += chunk
        if len(data) > 4096:
            raise AccountFileError(refused + "is too large to be a lock nonce" + tail)
    finally:
        os.close(fd)
    return data.decode("ascii", "replace").strip()


def main():
    # --account-only <draft> <account file>: write only the shared account file,
    # exactly as the full split would. rehearse's and release's account step
    # plans the account layer from it before the env's own split runs.
    argv = list(sys.argv)
    lock_nonce = ""
    if "--account-lock-nonce-file" in argv:
        # The account step holds the account lock: the nonce its lock printed, in a file it wrote.
        at = argv.index("--account-lock-nonce-file")
        if at + 1 >= len(argv):
            print("ERROR: --account-lock-nonce-file needs a file")
            sys.exit(1)
        try:
            lock_nonce = read_lock_nonce(Path(argv[at + 1]))
        except AccountFileError as exc:
            print(f"ERROR: {exc}")
            sys.exit(1)
        del argv[at:at + 2]
    account_only = argv[1:2] == ["--account-only"]
    if len(argv) != (4 if account_only else 5):
        print(
            "Usage: python scripts/split_abac_config.py "
            "generated/abac.auto.tfvars "
            "envs/account/abac.auto.tfvars "
            "envs/<env>/data_access/abac.auto.tfvars "
            "envs/dev/abac.auto.tfvars\n"
            "       python scripts/split_abac_config.py --account-only "
            "generated/abac.auto.tfvars envs/account/abac.auto.tfvars\n"
            "       (either form: --account-lock-nonce-file <file> when the caller holds the account lock)"
        )
        sys.exit(1)

    args = argv[2:] if account_only else argv[1:]
    source_path = Path(args[0])
    account_path = Path(args[1])

    if not source_path.exists():
        print(f"ERROR: source file not found: {source_path}")
        sys.exit(1)

    full_cfg = load_generated_config(source_path)
    if not account_only:
        data_access_path = Path(args[2])
        workspace_path = Path(args[3])
        data_access_cfg = build_data_access_config(full_cfg)
        workspace_cfg = build_workspace_config(full_cfg)

    data_access_header = """
# ============================================================================
# DATA-ACCESS-OWNED ABAC CONFIG
# ============================================================================
# Generated from the workspace draft for this environment's governance layer.
# Owns group references, tag assignments, and FGAC policies.
# Tag policy definitions live in envs/account — not here.
# ============================================================================
"""

    workspace_header = """
# ============================================================================
# WORKSPACE-OWNED ABAC CONFIG
# ============================================================================
# Generated from the workspace draft for this environment only.
# Owns workspace lookups/ACLs and Genie config only.
# ============================================================================
"""

    try:
        changed = merge_account_file(full_cfg, account_path, lock_nonce)
    except AccountFileError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)
    account_line = (f"  Wrote shared account config: {account_path}" if changed
                    else f"  Shared account config unchanged: {account_path}")
    if account_only:
        print(account_line)
        return
    write_tfvars(
        data_access_path,
        data_access_cfg,
        data_access_header,
        DATA_ACCESS_KEYS,
    )
    write_tfvars(
        workspace_path,
        workspace_cfg,
        workspace_header,
        WORKSPACE_KEYS,
    )

    print(account_line)
    print(f"  Wrote env-scoped data-access config: {data_access_path}")
    print(f"  Wrote workspace config:      {workspace_path}")


if __name__ == "__main__":
    main()
