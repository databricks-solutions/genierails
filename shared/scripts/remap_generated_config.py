#!/usr/bin/env python3
"""Remap a generated draft from one env's catalog namespace to another.

Supports multiple catalog mappings for multi-catalog Genie agents.
Mappings are sorted by source name length (longest first) to prevent
a shorter catalog name from being substituted inside a longer one.

Usage:
  python scripts/remap_generated_config.py \\
    <source_abac> <source_sql> <out_abac> <out_sql> \\
    --map src_catalog=dest_catalog \\
    [--map src_catalog2=dest_catalog2 ...]

  # Single-pair shorthand (positional, backward-compatible):
  python scripts/remap_generated_config.py \\
    <source_abac> <source_sql> <src_catalog> <dest_catalog> <out_abac> <out_sql>
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import threading
from pathlib import Path
from typing import Callable

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))


def _top_level_list_span(text: str, key: str, opener: str = "[") -> tuple[int, int, int] | None:
    """Return (key_start, open_bracket, end_after_close) for a top-level list (or map, opener "{")."""
    closer = {"[": "]", "{": "}"}[opener]
    match = re.search(rf"(?m)^{re.escape(key)}\s*=\s*{re.escape(opener)}", text)
    if not match:
        return None

    opening_bracket = text.find(opener, match.start(), match.end())
    depth = 0
    in_string = False
    escaped = False
    index = opening_bracket

    while index < len(text):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char == opener:
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0:
                return match.start(), opening_bracket, index + 1
        index += 1

    raise ValueError(f"Unterminated top-level {key} {'list' if opener == '[' else 'map'}")


def remove_tag_assignments(text: str) -> str:
    """Empty the generated top-level tag_assignments list.

    Assignments are environment-specific classification facts. Cross-environment
    promotion carries governance rules, while the destination classifier derives
    its own facts from the destination catalog.
    """
    span = _top_level_list_span(text, "tag_assignments")
    if span is None:
        return text
    start, _bracket, end = span
    while end < len(text) and text[end] in " \t":
        end += 1
    if end < len(text) and text[end] == "\n":
        end += 1
    # Keep an explicit empty section. The destination-side
    # derive-assignments command atomically replaces this section.
    return text[:start] + "tag_assignments = []\n" + text[end:]


def remap_policy_name(name: str, src: str, dest: str) -> str:
    """Replace the policy's own catalog where it is a whole ``_``-delimited token.

    Derived masks are named ``gr_mask_<catalog>_<treatment>``; the catalog keeps
    the name unique across an env's catalogs (it is the Terraform for_each key).
    """
    if not src or src == dest:
        return name
    return re.sub(rf"(?<![A-Za-z0-9]){re.escape(src)}(?![A-Za-z0-9])", dest, name)


def remote_policy_name(catalog: str, key: str) -> str:
    """The PolicyInfo name materialized by modules/data_access main.tf."""
    return f"{catalog}_{key}"


def read_state_policy_keys(path: Path | None = None, *, state: dict | None = None) -> set[tuple[str, str]] | None:
    """Return (catalog, key) of the data_access policies in a Terraform state.

    Only ``module.data_access.databricks_policy_info.policies`` counts. The
    result can only add reasons to keep a name: a state without a key doesn't
    prove the remote policy is absent. None when the state is missing or
    unreadable.
    """
    import json

    if state is None:
        if path is None or not path.is_file():
            return None
        try:
            state = json.loads(path.read_text())
        except (OSError, ValueError):
            return None
    if not isinstance(state, dict) or "resources" not in state:
        return None
    keys: set[tuple[str, str]] = set()
    for resource in state.get("resources") or []:
        if (
            resource.get("module") != "module.data_access"
            or resource.get("mode") != "managed"
            or resource.get("type") != "databricks_policy_info"
            or resource.get("name") != "policies"
        ):
            continue
        for instance in resource.get("instances") or []:
            catalog = (instance.get("attributes") or {}).get("on_securable_fullname")
            if isinstance(instance.get("index_key"), str) and catalog:
                keys.add((catalog, instance["index_key"]))
    return keys


def live_policy_lister(
    auth_path: Path | None, timeout: float = 90.0
) -> Callable[[str], set[str] | None]:
    """Return catalog -> remote policy names on it, read-only via the SDK.

    The lister returns None when the listing can't be trusted: no or placeholder
    credentials, SDK missing, any API/auth error, or no answer within timeout.
    """
    client = None
    unavailable_reason = ""
    unavailable = auth_path is None or not auth_path.is_file()
    if auth_path is not None:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from auth_configured import AuthStatus, auth_status
        status = auth_status(auth_path)
        unavailable = status is not AuthStatus.CONFIGURED
        if status is AuthStatus.UNREADABLE:
            unavailable_reason = f"{auth_path} can't be read"
        elif status is AuthStatus.MALFORMED:
            unavailable_reason = f"{auth_path} is not valid HCL"
        elif status is AuthStatus.INCOMPLETE:
            unavailable_reason = (
                f"fill in {auth_path} (workspace host, client ID, and client secret)"
            )

    def fetch(catalog: str) -> set[str]:
        nonlocal client
        if client is None:
            from auth_configured import workspace_client
            client = workspace_client(
                auth_path,
                http_timeout_seconds=30,
                retry_timeout_seconds=60,
                product="genierails",
                product_version="0.1.0",
            )
        return {
            policy.name
            for policy in client.policies.list_policies(
                on_securable_type="CATALOG", on_securable_fullname=catalog
            )
            if policy.name
        }

    def list_names(catalog: str) -> set[str] | None:
        nonlocal unavailable, unavailable_reason
        if unavailable:
            if unavailable_reason:
                print(f"WARNING: {unavailable_reason}; skipping live policy lookup.", file=sys.stderr)
                unavailable_reason = ""
            return None
        # The SDK's own timeouts don't bound auth discovery against an
        # unreachable host, so cap the whole call; promote must not hang.
        result: dict[str, object] = {}

        def run() -> None:
            try:
                result["names"] = fetch(catalog)
            except Exception as exc:
                result["error"] = exc

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        worker.join(timeout)
        if "names" not in result:
            unavailable = True
            return None
        return result["names"]  # type: ignore[return-value]

    return list_names


def remap_policy_names(
    source_text: str,
    remapped_text: str,
    pairs: list[tuple[str, str]],
    state_keys: set[tuple[str, str]] | None = None,
    live_names: Callable[[str], set[str] | None] | None = None,
    *, pure: bool = False,
) -> tuple[str, list[str]]:
    """Carry the destination catalog into fgac policy names.

    The data_access module names the remote policy ``<catalog>_<name>`` and keys
    it by ``name``. Renaming an applied policy is not safe: provider 1.111's
    update sends the new name as the request path and omits ``name`` from the
    update mask, and a new for_each key is a delete plus a create. So a policy
    is renamed only when a successful live listing of its destination catalog
    shows neither the old nor the new remote name, and the destination state
    doesn't hold the old key. A new remote name that already exists is fine only
    when the state holds it under the new key (the same managed policy);
    otherwise this raises, since neither name can be applied safely. Drafts and
    config files are never evidence.

    ``pure`` (a deterministic source, design section 9): every policy takes its
    destination name and neither state nor the live listing is read, so the
    output is a function of the commit alone. make release-plan keeps the names
    prod already deploys in an ignored overlay (scripts/runtime_overlay.py).
    """
    from scripts.hcl2_guard import import_hcl2

    try:
        hcl2 = import_hcl2()
        policies = hcl2.loads(source_text).get("fgac_policies") or []
    except Exception as exc:
        raise ValueError(f"Cannot parse the source fgac_policies: {exc}") from exc
    mapping = dict(pairs)
    live_cache: dict[str, set[str] | None] = {}
    renames: dict[str, str] = {}
    notes: list[str] = []
    final_names: list[str] = []
    for policy in policies:
        name = policy.get("name") or ""
        src = policy.get("catalog") or ""
        dest = mapping.get(src, src)
        new_name = remap_policy_name(name, src, dest)
        if new_name != name and not pure:
            if dest not in live_cache:
                live_cache[dest] = live_names(dest) if live_names else None
            live = live_cache[dest]
            managed = state_keys or set()
            old_remote = remote_policy_name(dest, name)
            new_remote = remote_policy_name(dest, new_name)
            if (dest, name) in managed or (live is not None and old_remote in live):
                notes.append(f"  kept policy name {old_remote} (deployed in {dest})")
                new_name = name
            elif live is None:
                notes.append(
                    f"  kept policy name {old_remote} "
                    f"(couldn't confirm it isn't deployed in {dest})"
                )
                new_name = name
            elif new_remote in live and (dest, new_name) not in managed:
                # The old policy is absent, so keeping its name would create it
                # beside the unmanaged one: two masks on the same columns.
                raise ValueError(
                    f"{dest} already has a policy named {new_remote} that GenieRails "
                    "doesn't manage; remove it or import it into the prod data_access "
                    "state, then re-run make promote"
                )
        if new_name != name:
            renames[name] = new_name
        final_names.append(new_name)

    duplicates = sorted({n for n in final_names if final_names.count(n) > 1})
    if duplicates:
        raise ValueError(
            "Promoted fgac_policies would share a name: " + ", ".join(duplicates)
        )
    if not renames:
        return remapped_text, notes

    span = _top_level_list_span(remapped_text, "fgac_policies")
    if span is None:
        return remapped_text, notes
    _start, bracket, end = span
    section = remapped_text[bracket:end]
    pattern = re.compile(r'(?m)^(\s*name\s*=\s*")([^"]*)(")')
    section = pattern.sub(
        lambda m: m.group(1) + renames.get(m.group(2), m.group(2)) + m.group(3),
        section,
    )
    return remapped_text[:bracket] + section + remapped_text[end:], notes


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments, supporting both new flag-based and legacy positional styles."""
    # Detect legacy positional invocation: 6 positional args with no --map flags.
    # Legacy: source_abac source_sql src_catalog dest_catalog out_abac out_sql
    if len(sys.argv) == 7 and "--map" not in sys.argv:
        ns = argparse.Namespace()
        ns.source_abac = Path(sys.argv[1])
        ns.source_sql = Path(sys.argv[2])
        ns.out_abac = Path(sys.argv[5])
        ns.out_sql = Path(sys.argv[6])
        ns.map = [f"{sys.argv[3]}={sys.argv[4]}"]
        ns.quiet_remaps = False
        ns.state = None
        ns.auth = None
        ns.pure = False
        return ns

    parser = argparse.ArgumentParser(
        description="Remap catalog names in a generated ABAC draft.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("source_abac", type=Path, help="Source generated abac.auto.tfvars")
    parser.add_argument("source_sql", type=Path, help="Source masking_functions.sql")
    parser.add_argument("out_abac", type=Path, help="Output abac.auto.tfvars")
    parser.add_argument("out_sql", type=Path, help="Output masking_functions.sql")
    parser.add_argument(
        "--map",
        metavar="SRC=DEST",
        action="append",
        required=True,
        help="Catalog name mapping (repeatable). E.g. --map dev_cat=prod_cat",
    )
    parser.add_argument("--quiet-remaps", action="store_true",
                        help="Do not repeat successful catalog remap lines.")
    parser.add_argument(
        "--state",
        metavar="PATH",
        type=Path,
        help="Destination data_access terraform.tfstate.",
    )
    parser.add_argument(
        "--state-env-dir",
        metavar="PATH",
        type=Path,
        help="Destination env dir; its data_access state is read through scripts/state_layout.py "
             "(GENIERAILS_STATE_DIR or remote state).",
    )
    parser.add_argument(
        "--auth",
        metavar="PATH",
        type=Path,
        help="Destination auth.auto.tfvars, for a read-only listing of live policies.",
    )
    parser.add_argument(
        "--group-map-env",
        metavar="PATH",
        type=Path,
        help="Destination env.auto.tfvars: in a deterministic destination its group_map renames "
             "the policies' principals (the source's raw_exempt_principals are never mapped).",
    )
    parser.add_argument("--source-env", metavar="PATH", type=Path,
                        help="Source env.auto.tfvars (its raw_exempt_principals).")
    parser.add_argument("--pure", action="store_true",
                        help="Name every promoted policy after its destination catalog without reading "
                             "--state or listing live policies (deterministic promotions).")
    return parser.parse_args()


_PRINCIPALS_LINE = re.compile(r'^(?P<lead>\s*(?:to|except)_principals\s*=\s*)(?P<list>\[[^\]\n]*\])(?P<tail>\s*)$',
                              re.MULTILINE)


class PromotionError(ValueError):
    """The promoted config can't be written safely; nothing is written."""


def map_principals(hcl_text: str, group_map: dict[str, str], exempt: set[str],
                   dest_exempt: list[str] | None = None, deployers: tuple[str, str] | None = None) -> str:
    """Rename policy principals through group_map (decision 8).

    raw_exempt_principals are env-owned and never mapped: where a policy
    names one of the source's, the destination's whole exempt set takes its
    place (in that list only), so a promoted policy never names a dev-only
    exempt principal. A destination exempt that is an everyone-principal
    (account users, users) would exempt everyone from the mask, so that
    refuses the promotion instead (PromotionError) whether or not a policy
    names a source exempt.

    ``deployers`` is (source, destination) deployer SP application IDs: each
    env applies with its own SP, so the source's in an ``except_principals``
    list becomes the destination's. Only that one principal is replaced; a
    list without it (a never-raw policy) gets none.
    """
    dest_exempt = list(dest_exempt or [])
    from table_readers import is_everyone

    everyone = [name for name in dest_exempt if is_everyone(name)]
    if everyone:
        raise PromotionError(
            f"destination raw_exempt_principals names {', '.join(repr(n) for n in everyone)}, which is "
            "everyone: substituting it into the promoted policies' except_principals would let everyone "
            "see raw values")
    source_deployer, dest_deployer = deployers or ("", "")
    if not group_map and not exempt and source_deployer == dest_deployer:
        return hcl_text

    def rename(match: re.Match) -> str:
        names = json.loads(match["list"])
        except_list = match["lead"].lstrip().startswith("except_")
        mapped = []
        for name in names:
            if source_deployer and name == source_deployer and except_list:
                targets = [dest_deployer]
            else:
                targets = dest_exempt if name in exempt else [group_map.get(name, name)]
            for target in targets:
                if target not in mapped:
                    mapped.append(target)
        return f"{match['lead']}[{', '.join(json.dumps(n) for n in mapped)}]{match['tail']}"

    return _PRINCIPALS_LINE.sub(rename, hcl_text)


def map_group_keys(hcl_text: str, group_map: dict[str, str], exempt: set[str]) -> str:
    """Rename the keys of the top-level ``groups`` and ``group_members`` maps through group_map.

    The policies' principals are mapped, so the groups they name must be
    defined under the same names. raw_exempt_principals keep their names, as
    in map_principals. Two keys that would end up with one name (a mapping
    onto a group the source already defines) merge tiers, so that refuses the
    promotion (PromotionError). The result is re-read; a layout this can't
    rename in place refuses rather than leave a dev name behind.
    """
    from scripts.hcl2_guard import import_hcl2

    if not group_map:
        return hcl_text
    hcl2 = import_hcl2()
    cfg = hcl2.loads(hcl_text)
    for key in ("groups", "group_members"):
        value = cfg.get(key)
        if not isinstance(value, dict) or not value:
            continue
        renames = {name: group_map[name] for name in value if name in group_map and name not in exempt}
        targets: dict[str, list[str]] = {}
        for name in value:
            targets.setdefault(renames.get(name, name), []).append(name)
        collisions = {target: names for target, names in targets.items() if len(names) > 1}
        if collisions:
            raise PromotionError("; ".join(
                f"group_map would give {key} {', '.join(repr(n) for n in names)} the one name {target!r}"
                for target, names in sorted(collisions.items())))
        if not renames:
            continue
        span = _top_level_list_span(hcl_text, key, "{")
        if span is None:
            raise PromotionError(f"cannot find the top-level {key} map to apply group_map to")
        _start, brace, end = span
        section = hcl_text[brace:end]
        # Keys sit one level in, at the start of their line.
        out, depth = [], 0
        for line in section.splitlines(keepends=True):
            if depth == 1:
                key_match = re.match(r'^(\s*)(?:"([^"\\]*)"|([A-Za-z_][\w-]*))(\s*=)', line)
                name = key_match and (key_match[2] if key_match[2] is not None else key_match[3])
                if name in renames:
                    line = f"{key_match[1]}{json.dumps(renames[name])}{key_match[4]}{line[key_match.end():]}"
            out.append(line)
            depth += _brace_delta(line)
        hcl_text = hcl_text[:brace] + "".join(out) + hcl_text[end:]
        expected = [renames.get(name, name) for name in value]
        if list((hcl2.loads(hcl_text).get(key) or {})) != expected:
            raise PromotionError(f"could not rename the {key} keys through group_map in place")
    return hcl_text


def _brace_delta(line: str) -> int:
    depth, in_string, escaped = 0, False, False
    for char in line:
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char == "#":
            break
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
    return depth


def _deployer_ids(args, hcl_text: str) -> tuple[str, str] | None:
    """(source, destination) committed deployer IDs for a deterministic source (deployer_identity.py)."""
    from scripts.hcl2_guard import import_hcl2
    from deployer_identity import DeployerIdentityError, promotion_deployers

    if not args.source_env or not args.group_map_env:
        return None
    try:
        hcl2 = import_hcl2()
        return promotion_deployers(args.source_env.parent, args.group_map_env.parent,
                                   hcl2.loads(hcl_text).get("fgac_policies") or [])
    except DeployerIdentityError as exc:
        raise PromotionError(str(exc)) from exc


def _group_map_inputs(args) -> tuple[dict[str, str], set[str], list[str] | None]:
    from scripts.hcl2_guard import import_hcl2

    if not args.group_map_env or not args.group_map_env.is_file():
        return {}, set(), None
    hcl2 = import_hcl2()
    dest = hcl2.loads(args.group_map_env.read_text())
    if dest.get("governance_mode", "legacy") != "deterministic":
        return {}, set(), None
    source = hcl2.loads(args.source_env.read_text()) if args.source_env and args.source_env.is_file() else {}
    return ({str(k): str(v) for k, v in (dest.get("group_map") or {}).items()},
            {str(p) for p in source.get("raw_exempt_principals") or []},
            [str(p) for p in dest.get("raw_exempt_principals") or []])


def parse_catalog_pairs(raw_pairs: list[str]) -> list[tuple[str, str]]:
    """Parse 'src=dest' strings into (src, dest) tuples, sorted longest-src-first.

    Sorting by descending source length prevents a short name (e.g. 'cat') from
    being substituted inside a longer name (e.g. 'cat_v2') before it gets its
    own mapping applied.
    """
    pairs: list[tuple[str, str]] = []
    for entry in raw_pairs:
        # Support comma-separated pairs within a single --map value for shell convenience.
        for item in entry.split(","):
            item = item.strip()
            if not item:
                continue
            if "=" not in item:
                print(f"ERROR: Invalid --map value '{item}'. Expected format: src_catalog=dest_catalog")
                sys.exit(1)
            src, dest = item.split("=", 1)
            src, dest = src.strip(), dest.strip()
            if not src:
                print(f"ERROR: Empty source catalog in mapping '{item}'.")
                sys.exit(1)
            if not dest:
                print(f"ERROR: Empty destination catalog in mapping '{item}'.")
                sys.exit(1)
            pairs.append((src, dest))

    # Longest source name first to avoid prefix collisions.
    pairs.sort(key=lambda p: len(p[0]), reverse=True)
    return pairs


def remap_hcl(text: str, pairs: list[tuple[str, str]]) -> str:
    """Substitute all catalog references in HCL text for every mapping pair.

    Handles:
    - Catalog-prefixed table refs:  "src.schema.table"  -> "dest.schema.table"
    - Standalone catalog fields:    catalog = "src"     -> catalog = "dest"
    - function_catalog fields:      function_catalog = "src" -> function_catalog = "dest"
    - Bare quoted catalog names:    "src"               -> "dest"
      (catches references in genie_space_configs where the LLM may use the
      catalog name without a trailing dot, e.g. in comments or descriptions)
    """
    result = remove_tag_assignments(text)
    for src, dest in pairs:
        # Replace catalog-prefixed table refs (e.g. in entity_name, inline strings).
        result = result.replace(f"{src}.", f"{dest}.")

        # Replace standalone catalog field assignments not already caught above
        # (e.g. catalog = "src_catalog" without a trailing dot).
        for field in ("catalog", "function_catalog"):
            result = re.sub(
                rf'(^\s*{re.escape(field)}\s*=\s*"){re.escape(src)}(")',
                rf'\g<1>{dest}\g<2>',
                result,
                flags=re.MULTILINE,
            )

        # Replace bare quoted catalog name anywhere (e.g. "dev_fin" → "prod_fin").
        # Uses word boundaries to avoid partial matches inside longer names.
        result = re.sub(
            rf'"{re.escape(src)}"',
            f'"{dest}"',
            result,
        )
    return result


def remap_sql(text: str, pairs: list[tuple[str, str]]) -> str:
    """Substitute catalog references in masking SQL for every mapping pair.

    Handles USE CATALOG statements and catalog-prefixed identifiers.
    """
    for src, dest in pairs:
        lines: list[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            match = re.match(
                r"^(USE\s+CATALOG\s+)([^;\s]+)(;?)$",
                stripped,
                re.IGNORECASE,
            )
            if match:
                prefix, catalog, suffix = match.groups()
                catalog = catalog.rstrip(";")
                new_catalog = dest if catalog == src else catalog
                lines.append(f"{prefix}{new_catalog}{suffix}")
            else:
                lines.append(line.replace(f"{src}.", f"{dest}."))
        text = "\n".join(lines) + "\n"
    return text


def _destination_policy_keys(args) -> set[tuple[str, str]] | None:
    if args.state_env_dir is None:
        return read_state_policy_keys(args.state)
    from scripts.state_layout import state_json

    env_dir = args.state_env_dir
    return read_state_policy_keys(state=state_json(env_dir, env_dir.name, "data_access") or {})


def main() -> None:
    args = parse_args()

    if not args.source_abac.exists():
        print(f"ERROR: Required file not found: {args.source_abac}")
        sys.exit(1)

    # masking_functions.sql is optional — genie-mode envs don't generate it.
    has_sql = args.source_sql.exists()

    pairs = parse_catalog_pairs(args.map)
    if not pairs:
        print("ERROR: No catalog mappings provided. Use --map src=dest.")
        sys.exit(1)

    args.out_abac.parent.mkdir(parents=True, exist_ok=True)

    try:
        remapped_hcl, name_notes = remap_policy_names(
            args.source_abac.read_text(),
            remap_hcl(args.source_abac.read_text(), pairs),
            pairs,
            None if args.pure else _destination_policy_keys(args),
            None if args.pure else live_policy_lister(args.auth),
            pure=args.pure,
        )
    except (RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)
    group_map, exempt, dest_exempt = _group_map_inputs(args)
    try:
        deployers = _deployer_ids(args, remapped_hcl)
        remapped_hcl = map_principals(remapped_hcl, group_map, exempt, dest_exempt, deployers)
        remapped_hcl = map_group_keys(remapped_hcl, group_map, exempt)
    except PromotionError as exc:
        print(f"ERROR: {exc}; nothing was written.")
        sys.exit(1)
    for note in name_notes:
        print(note)
    remapped_sql = remap_sql(args.source_sql.read_text(), pairs) if has_sql else None

    # Warn if any source catalog name was not found in either output file.
    source_abac_text = args.source_abac.read_text()
    source_sql_text = args.source_sql.read_text() if has_sql else ""
    for src, dest in pairs:
        if src == dest:
            print(f"  Catalog unchanged: {src} (same-catalog remap is a no-op)")
            continue
        found_in_hcl = f"{src}." in source_abac_text or f'"{src}"' in source_abac_text
        found_in_sql = f"{src}." in source_sql_text or f"CATALOG {src}" in source_sql_text.upper()
        if not found_in_hcl and not found_in_sql:
            print(
                f"  WARNING: Source catalog '{src}' was not found in the generated files "
                f"— the mapping '{src}={dest}' had no effect. Check for typos."
            )
        elif not args.quiet_remaps:
            print(f"  Catalog remap: {src} -> {dest}")

    args.out_abac.write_text(remapped_hcl)
    print(f"  Wrote remapped generated config: {args.out_abac}")

    if remapped_sql is not None:
        args.out_sql.parent.mkdir(parents=True, exist_ok=True)
        args.out_sql.write_text(remapped_sql)
        print(f"  Wrote remapped masking SQL:      {args.out_sql}")
    else:
        print(f"  Skipped masking SQL (not present in source — genie mode)")


if __name__ == "__main__":
    main()
