#!/usr/bin/env python3
"""Remap env.auto.tfvars for cross-env promotion.

Reads source env.auto.tfvars, remaps catalog names, and writes dest env.auto.tfvars.
When genie_space_id is set but uc_tables/name are missing, queries the Genie API
to discover them (using include_serialized_space=true).

Usage:
    python remap_env_config.py <source_env_dir> <dest_env_dir> <catalog_map>

    catalog_map: comma-separated "src=dest" pairs, e.g. "dev_bank=prod_bank"
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from scripts.footprint import (
    FootprintError,
    load_discovered_footprint,
    load_hcl,
    resolve_footprint,
)
from scripts.remap_generated_config import remap_hcl
from verify_effective_access import normalize_key_map
from walkthrough_marker import PROMOTED_HEADER, follows_walkthrough

try:
    import hcl2
except ImportError:
    print("ERROR: python-hcl2 required")
    sys.exit(2)


def _str(v) -> str:
    return (v[0] if isinstance(v, list) else v or "").strip()


def _promotion_setting(value) -> str:
    if isinstance(value, dict):
        return "{ " + ", ".join(f"{json.dumps(str(k))} = {json.dumps(str(v))}" for k, v in value.items()) + " }"
    return json.dumps(_str(value))


def _discover_from_genie_api(space_id: str, auth_cfg: dict) -> tuple[str, list[str]]:
    """Query Genie agent API to get name and tables.

    Returns (space_title, table_identifiers).
    """
    host = _str(auth_cfg.get("databricks_workspace_host", ""))
    client_id = _str(auth_cfg.get("databricks_client_id", ""))
    client_secret = _str(auth_cfg.get("databricks_client_secret", ""))

    if not host or not client_id:
        return "", []

    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(
            host=host, client_id=client_id, client_secret=client_secret,
            product="genierails", product_version="0.1.0",
        )
        resp = w.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}",
            query={"include_serialized_space": "true"},
        )
    except Exception as e:
        print(f"  WARNING: Could not query Genie agent {space_id}: {e}")
        return "", []

    title = resp.get("title", "")
    serialized = resp.get("serialized_space", "")
    tables = []
    if serialized:
        try:
            space_data = json.loads(serialized) if isinstance(serialized, str) else serialized
            for t in space_data.get("data_sources", {}).get("tables", []):
                ident = t.get("identifier", "")
                if ident:
                    tables.append(ident)
        except (json.JSONDecodeError, TypeError):
            pass

    return title, tables


def _space_key(name: str) -> str:
    """Terraform for_each key of a named space (roots/workspace merged_spaces)."""
    import re

    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _deployed_space_keys(dest_env_dir: str) -> dict[str, list[str]]:
    """Map each Genie space key in the destination workspace state to its resource addresses (without the key)."""
    state_path = Path(dest_env_dir) / "terraform.tfstate"
    if not state_path.exists():
        return {}
    try:
        state = json.loads(state_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(f"ERROR: cannot read {state_path} to check deployed Genie spaces: {exc}")
        sys.exit(1)
    keys: dict[str, list[str]] = {}
    for resource in state.get("resources", []):
        rtype = resource.get("type")
        if rtype not in ("null_resource", "terraform_data") or not resource.get("name", "").startswith("genie_space"):
            continue
        prefix = (resource.get("module") + "." if resource.get("module") else "") + f"{rtype}.{resource['name']}"
        for instance in resource.get("instances", []):
            key = instance.get("index_key")
            if isinstance(key, str):
                keys.setdefault(key, []).append(prefix)
    return keys


def _promoted_access_tier_groups(cfg: dict, source_env_dir: str, dest_cfg: dict) -> list[str]:
    """Preserve destination tiers on re-promote; seed them from source initially."""
    shared_root = str(Path(__file__).resolve().parent.parent)
    if shared_root not in sys.path:
        sys.path.insert(0, shared_root)
    from access_tier_groups import promoted_lines

    try:
        if "access_tier_groups" in dest_cfg:
            return ["", "access_tier_groups = " + json.dumps(dest_cfg["access_tier_groups"])]
        return promoted_lines(cfg, Path(source_env_dir) / "env.auto.tfvars")
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)


def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <source_env_dir> <dest_env_dir> <catalog_map>")
        sys.exit(2)

    source_env_dir = sys.argv[1]
    dest_env_dir = sys.argv[2]
    catalog_map_str = sys.argv[3]
    source_env = Path(source_env_dir).name

    # Parse catalog map
    pairs = {}
    for pair in catalog_map_str.split(","):
        if "=" in pair:
            k, v = pair.split("=", 1)
            pairs[k.strip()] = v.strip()

    # Space names key genie_space_configs and the ACL sidecar, which
    # remap_generated_config.py rewrites with remap_hcl; rename spaces the
    # same way (e.g. a title naming the dev catalog) so the keys still match.
    sorted_pairs = sorted(pairs.items(), key=lambda p: len(p[0]), reverse=True)

    def remap_name(name: str) -> str:
        return json.loads(remap_hcl(json.dumps(name), sorted_pairs))

    def remap_table(table: str) -> str:
        parts = table.split(".", 1)
        if len(parts) == 2 and parts[0] in pairs:
            return pairs[parts[0]] + "." + parts[1]
        return table

    # Load source config
    try:
        cfg = load_hcl(Path(source_env_dir) / "env.auto.tfvars")
        spaces = cfg.get("genie_spaces", [])
        effective_tables = resolve_footprint(source_env_dir)
        discovered_tables, discovered_agents = load_discovered_footprint(source_env_dir)
    except FootprintError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)
    generated_path = os.path.join(source_env_dir, "generated", "abac.auto.tfvars")
    id_to_name = {}
    source_space_configs = {}
    if os.path.exists(generated_path):
        generated_cfg = hcl2.load(open(generated_path))
        id_to_name = generated_cfg.get("genie_space_id_to_name") or {}
        source_space_configs = generated_cfg.get("genie_space_configs") or {}

    # Load source auth for API queries
    auth_cfg = {}
    for auth_path in [
        os.path.join(source_env_dir, "auth.auto.tfvars"),
        os.path.join(source_env_dir, "..", "auth.auto.tfvars"),
    ]:
        if os.path.exists(auth_path):
            auth_cfg = hcl2.load(open(auth_path))
            break

    # Resolve canonical names before matching discovered table attribution.
    for space in spaces:
        space_id = _str(space.get("genie_space_id", ""))
        if not _str(space.get("name", "")) and space_id:
            canonical_name = _str(id_to_name.get(space_id, ""))
            if canonical_name:
                space["name"] = canonical_name

    # Enrich spaces from persisted discovery first; query the API only as a
    # last resort when a configured space still has no attributable tables.
    api_resolved_tables: list[str] = []
    for space in spaces:
        space_id = _str(space.get("genie_space_id", ""))
        name = _str(space.get("name", ""))
        uc_tables = space.get("uc_tables") or []

        if not uc_tables and name:
            uc_tables = [
                table for table, agents in discovered_agents.items()
                if name in (agents or [])
            ]
            if uc_tables:
                space["uc_tables"] = uc_tables
                print(f"  Resolved {len(uc_tables)} persisted table(s) for {name}")

        # Legacy single-agent discovery did not record attribution.
        if not uc_tables and len(spaces) == 1 and not discovered_agents and discovered_tables:
            uc_tables = list(discovered_tables)
            space["uc_tables"] = uc_tables

        if space_id and (not name or not uc_tables):
            print(f"  Querying Genie agent {space_id} for name/tables...")
            api_title, api_tables = _discover_from_genie_api(space_id, auth_cfg)
            if not name and api_title:
                space["name"] = api_title
                print(f"  Discovered name: {api_title}")
            if not uc_tables and api_tables:
                space["uc_tables"] = api_tables
                api_resolved_tables.extend(
                    table for table in api_tables if table not in api_resolved_tables
                )
                effective_tables.extend(
                    table for table in api_tables if table not in effective_tables
                )
                print(f"  Discovered {len(api_tables)} table(s)")

        if not (space.get("uc_tables") or []):
            agent = space_id or name or "<unknown>"
            print(
                f"ERROR: no tables found for agent {agent}; run "
                f"`make generate ENV={source_env}` first"
            )
            sys.exit(1)

    if not effective_tables:
        print(
            "ERROR: no tables found for source environment; run "
            f"`make generate ENV={source_env}` first"
        )
        sys.exit(1)

    space_tables = list(dict.fromkeys(
        table
        for space in spaces
        for table in (space.get("uc_tables") or [])
        if table
    ))
    missing_from_top_level = [
        table for table in space_tables if table not in effective_tables
    ]
    if missing_from_top_level:
        print(
            "ERROR: internal footprint invariant failed: space-level table(s) are "
            "missing from the promoted top-level union: "
            + ", ".join(missing_from_top_level)
        )
        sys.exit(1)

    tables_to_write = list(dict.fromkeys(effective_tables + space_tables))
    unmapped_catalogs = sorted({
        table.split(".")[0]
        for table in tables_to_write
        if table.count(".") >= 2 and table.split(".")[0] not in pairs
    })
    if unmapped_catalogs:
        print(
            "ERROR: DEST_CATALOG_MAP is missing mappings for resolved catalog(s): "
            + ", ".join(unmapped_catalogs)
        )
        if any(
            table.count(".") >= 2 and table.split(".")[0] in unmapped_catalogs
            for table in api_resolved_tables
        ):
            print(
                f"       Run `make generate ENV={source_env}` first "
                "to persist the API-discovered catalogs, then update DEST_CATALOG_MAP."
            )
        sys.exit(1)

    canonical_names = [_str(space.get("name", "")) for space in spaces]
    missing = [i for i, name in enumerate(canonical_names) if not name]
    if missing:
        print(
            "ERROR: Cannot promote Genie space(s) without a canonical name. "
            "Run `make generate` in the source environment first."
        )
        sys.exit(1)
    duplicates = sorted({name for name in canonical_names if canonical_names.count(name) > 1})
    if duplicates:
        print(
            "ERROR: Multiple Genie spaces resolve to the same canonical name: "
            + ", ".join(repr(name) for name in duplicates)
            + ". Set distinct names before promoting."
        )
        sys.exit(1)

    # Promotion renames spaces whose name references a source catalog. Two
    # names must not collapse into one (that would merge their configs, ACLs
    # and Terraform addresses), and a space already deployed in the destination
    # must not change key: its genie_space resource would be destroyed, which
    # trashes the live Genie agent, and a new one created in its place.
    renamed: dict[str, list[str]] = {}
    for name in canonical_names:
        renamed.setdefault(remap_name(name), []).append(name)
    collisions = {dest: srcs for dest, srcs in renamed.items() if len(srcs) > 1}
    if collisions:
        for dest, srcs in sorted(collisions.items()):
            print(
                "ERROR: Genie spaces " + ", ".join(repr(src) for src in srcs)
                + f" would all be promoted as {dest!r}."
            )
        print("       Give them names that stay distinct after DEST_CATALOG_MAP, then re-promote.")
        sys.exit(1)

    # The API identity guard uses the title users see, which is an explicit
    # genie_space_configs title when present and the canonical space name
    # otherwise. Validate those effective titles after catalog remapping. Title
    # identity is exact and case-sensitive, matching genie_space.sh.
    effective_titles: dict[str, list[str]] = {}
    for name in canonical_names:
        explicit_title = _str((source_space_configs.get(name) or {}).get("title", ""))
        effective_title = remap_name(explicit_title or name)
        effective_titles.setdefault(effective_title, []).append(name)
    duplicate_titles = {
        title: owners for title, owners in effective_titles.items() if len(owners) > 1
    }
    if duplicate_titles:
        for title, owners in sorted(duplicate_titles.items()):
            print(
                f"ERROR: Genie spaces {owners[0]!r} and {owners[1]!r} "
                f"would have the same effective destination title {title!r}."
            )
        print("       Set distinct config.title values (or names) before promoting. Nothing was written.")
        sys.exit(1)
    # Distinct names can still normalize to one Terraform for_each key
    # (e.g. "x (prod.demo)" and "x prod demo"); refuse keys the rename merges.
    by_key: dict[str, list[str]] = {}
    for name in canonical_names:
        by_key.setdefault(_space_key(remap_name(name)), []).append(name)
    key_collisions = {
        key: srcs for key, srcs in by_key.items()
        if len(srcs) > 1 and len({_space_key(src) for src in srcs}) > 1
    }
    if key_collisions:
        for key, srcs in sorted(key_collisions.items()):
            print(
                "ERROR: Genie spaces " + ", ".join(repr(src) for src in srcs)
                + f" would all be promoted under Terraform key {key!r}."
            )
        print("       Give them names that stay distinct after DEST_CATALOG_MAP, then re-promote.")
        sys.exit(1)
    deployed = _deployed_space_keys(dest_env_dir)
    for name in canonical_names:
        old_key, new_key = _space_key(name), _space_key(remap_name(name))
        if old_key == new_key or old_key not in deployed:
            continue
        print(
            f"ERROR: Genie space {name!r} is already deployed in {Path(dest_env_dir).name} "
            f"under key {old_key!r}; promotion now names it {remap_name(name)!r} "
            f"(key {new_key!r}). Applying that would trash the deployed agent and create a new one."
        )
        print("       Nothing was written. To keep the deployed agent, move its state to the new key, then re-promote:")
        env = Path(dest_env_dir).name
        for address in deployed[old_key]:
            print(
                f"         ENVS_DIR=\"$PWD/envs\" ../shared/scripts/terraform_layer.sh workspace {env} "
                f"state-mv '{address}[{json.dumps(old_key)}]' '{address}[{json.dumps(new_key)}]'"
            )
        id_file = Path(dest_env_dir) / f".genie_space_id_{old_key}"
        if id_file.exists():
            print(f"         mv '{id_file}' '{id_file.with_name(f'.genie_space_id_{new_key}')}'")
        adopted_marker = Path(dest_env_dir) / f".genie_adopted_{old_key}"
        if adopted_marker.exists():
            print(
                f"         mv '{adopted_marker}' "
                f"'{adopted_marker.with_name(f'.genie_adopted_{new_key}')}'"
            )
        sys.exit(1)

    # Preserve destination-owned settings across remediation re-promotions.
    dest_path = os.path.join(dest_env_dir, "env.auto.tfvars")
    try:
        dest_cfg = load_hcl(Path(dest_path))
        dest_discovered, _dest_agents = load_discovered_footprint(dest_env_dir)
    except FootprintError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)
    preserved_warehouse = _str(dest_cfg.get("sql_warehouse_id", ""))
    preserved_auto_tagging = dest_cfg.get("enable_auto_tagging")
    if preserved_auto_tagging not in (None, True, False):
        print("ERROR: destination enable_auto_tagging must be true, false, or omitted")
        sys.exit(1)
    if (
        preserved_auto_tagging is False
        and os.environ.get("ALLOW_DISABLE_AUTO_TAGGING") != "1"
    ):
        print(
            "ERROR: destination "
            f"{dest_path} sets enable_auto_tagging = false; promotion refuses to "
            "preserve a setting that can disable UI-managed auto-tagging. Delete "
            "that line to keep the UI setting, or pass "
            "ALLOW_DISABLE_AUTO_TAGGING=1 to really turn it off"
        )
        sys.exit(1)
    # Acknowledgements are reviewed per destination (they name its catalogs),
    # so keep the destination's own list and never carry the source's.
    preserved_acknowledged = [
        _str(column) for column in dest_cfg.get("coverage_acknowledged_columns") or []
    ]
    # make promote-to's saved source env + catalog map belong to the destination.
    preserved_promote = {
        name: dest_cfg[name]
        for name in ("promote_from", "catalog_map")
        if dest_cfg.get(name)
    }
    promoted_verify_key = (
        _str(cfg.get("verify_key_column", ""))
        or _str(dest_cfg.get("verify_key_column", ""))
    )
    # Per-table keys: the source's (tables remapped to the destination's
    # catalogs, columns unchanged), else the destination's own.
    source_key_map = normalize_key_map(cfg.get("verify_key_columns"))
    promoted_key_map = (
        {remap_table(table): column for table, column in source_key_map.items()}
        if source_key_map else normalize_key_map(dest_cfg.get("verify_key_columns"))
    )

    remapped_effective_tables = [remap_table(table) for table in tables_to_write]
    stale_discovered = [
        table for table in dest_discovered if table not in remapped_effective_tables
    ]
    if stale_discovered:
        print(
            "ERROR: destination discovered footprint contains table(s) outside the "
            "promoted footprint: " + ", ".join(stale_discovered) + ". Remove the "
            "stale tool-owned discovery file before promoting: "
            + str(Path(dest_env_dir) / "data_access" / "discovered_uc_tables.auto.tfvars")
        )
        sys.exit(1)
    if "sql_warehouse_id" in dest_cfg:
        print(f"  Preserved destination sql_warehouse_id={preserved_warehouse!r}")
    if "enable_auto_tagging" in dest_cfg:
        print(
            "  Preserved destination enable_auto_tagging="
            f"{str(preserved_auto_tagging).lower()}"
        )
    if "business_access_enabled" in dest_cfg:
        # Retired: access follows the coverage check, so the rewrite drops the
        # line instead of resetting it (a reset used to close live prod).
        print("  Dropped the retired business_access_enabled setting from the destination")
    if preserved_acknowledged:
        print(
            "  Preserved destination coverage_acknowledged_columns "
            f"({len(preserved_acknowledged)} column(s))"
        )

    dest_spaces = dest_cfg.get("genie_spaces", [])
    source_space_ids = {
        _str(space.get("genie_space_id", ""))
        for space in spaces
        if _str(space.get("genie_space_id", ""))
    }

    def matching_dest_space(name: str) -> dict:
        """Match the promoted name first, then its Terraform normalized key."""
        exact = [space for space in dest_spaces if _str(space.get("name", "")) == name]
        if len(exact) == 1:
            return exact[0]
        keyed = [
            space for space in dest_spaces
            if _str(space.get("name", ""))
            and _space_key(_str(space.get("name", ""))) == _space_key(name)
        ]
        if len(keyed) > 1:
            print(
                f"ERROR: Multiple destination Genie spaces match promoted space {name!r} "
                f"under Terraform key {_space_key(name)!r}; nothing was written."
            )
            sys.exit(1)
        return keyed[0] if keyed else {}

    # Build dest env.auto.tfvars. The complete promoted union is top-level so
    # Terraform, classification, derive-assignments, and release share it.
    lines = []
    if follows_walkthrough(Path(source_env_dir) / "env.auto.tfvars"):
        lines += [PROMOTED_HEADER, ""]
    lines.append("genie_spaces = [")
    for space in spaces:
        name = remap_name(_str(space.get("name", "")))
        uc_tables = space.get("uc_tables") or []
        remapped_tables = [remap_table(t) for t in uc_tables]

        lines.append("  {")
        lines.append(f"    name             = {json.dumps(name)}")
        dest_space = matching_dest_space(name)
        # IDs belong to a workspace. Preserve only an explicitly attached
        # destination ID; never copy the source environment's ID.
        destination_space_id = _str(dest_space.get("genie_space_id", ""))
        if destination_space_id and destination_space_id in source_space_ids:
            print(
                f"ERROR: Destination Genie space {name!r} has genie_space_id "
                f"{destination_space_id!r}, which is also configured in the source "
                "environment. Refusing to preserve a source-workspace ID; nothing was written."
            )
            sys.exit(1)
        lines.append(f"    genie_space_id   = {json.dumps(destination_space_id)}")
        lines.append(f'    uc_tables = [')
        for t in remapped_tables:
            lines.append(f'      "{t}",')
        lines.append(f'    ]')
        if destination_space_id:
            print(
                f"  Preserved destination Genie space {name!r} "
                f"genie_space_id={destination_space_id!r}"
            )
        if "sql_warehouse_id" in dest_space:
            space_warehouse = _str(dest_space.get("sql_warehouse_id", ""))
            lines.append(f"    sql_warehouse_id = {json.dumps(space_warehouse)}")
            print(
                f"  Preserved destination Genie space {name!r} "
                f"sql_warehouse_id={space_warehouse!r}"
            )
        # An existing destination space owns its ACL intent, including deliberate
        # omission (derive from destination policy) and explicit []. First promote
        # seeds from the source; later ACL changes are reviewed directly in prod.
        is_repromote = bool(dest_space)
        acl_source = dest_space if is_repromote else space
        if "acl_groups" in acl_source:
            acl_groups = acl_source["acl_groups"]
            if acl_groups is not None and (
                not isinstance(acl_groups, list) or not all(
                    isinstance(group, str) for group in acl_groups
                )
            ):
                print(
                    f"ERROR: acl_groups for Genie space {name!r} must be a list "
                    "of group names, null, or omitted ([] means nobody)."
                )
                sys.exit(1)
            if acl_groups is not None:
                rendered_acl = ", ".join(json.dumps(group) for group in acl_groups)
                lines.append(f"    acl_groups       = [{rendered_acl}]")
        if is_repromote:
            if "acl_groups" in dest_space:
                print(
                    f"  Preserved destination Genie space {name!r} "
                    f"acl_groups={dest_space['acl_groups']!r}"
                )
            else:
                print(
                    f"  Preserved destination Genie space {name!r} omitted acl_groups "
                    "(destination policy derivation remains authoritative)"
                )
            source_acl = space.get("acl_groups")
            dest_acl = dest_space.get("acl_groups")
            if isinstance(source_acl, list) and isinstance(dest_acl, list):
                added = sorted(set(source_acl) - set(dest_acl))
                revoked = sorted(set(dest_acl) - set(source_acl))
                if added or revoked:
                    print(
                        f"  Genie ACL diff for {name!r} (dev vs prod): "
                        f"added in dev={added!r}; revoked in dev={revoked!r}. "
                        "To change prod, edit envs/prod/env.auto.tfvars in a PR."
                    )
            elif source_acl != dest_acl:
                source_display = repr(source_acl) if isinstance(source_acl, list) else "<derived>"
                dest_display = repr(dest_acl) if isinstance(dest_acl, list) else "<derived>"
                print(
                    f"  Genie ACL diff for {name!r} (dev vs prod): "
                    f"dev={source_display}; prod={dest_display}. "
                    "To change prod, edit envs/prod/env.auto.tfvars in a PR."
                )
        lines.append("  },")
    lines.append("]")
    lines.append("")
    lines.append("uc_tables = [")
    for table in remapped_effective_tables:
        lines.append(f'  "{table}",')
    lines.append("]")
    lines.append("")
    lines.append(
        f"sql_warehouse_id = {json.dumps(preserved_warehouse)}"
        "  # empty means auto-create in dest workspace"
    )
    lines.append(f"verify_key_column = {json.dumps(promoted_verify_key)}")
    if promoted_key_map:
        lines.append("verify_key_columns = {")
        lines.extend(f"  {json.dumps(table)} = {json.dumps(column)}"
                     for table, column in sorted(promoted_key_map.items()))
        lines.append("}")
    lines.append("")
    lines.append("# Safe production defaults. Business access is granted only through the")
    lines.append("# coverage check that make release runs; there is no access flag to set.")
    lines.append("enable_classification = true")
    if preserved_auto_tagging is not None:
        lines.append(f"enable_auto_tagging = {str(preserved_auto_tagging).lower()}")
    if preserved_acknowledged:
        lines.append("")
        lines.append("# Reviewed as not sensitive; the coverage check doesn't block first exposure on them.")
        lines.append("coverage_acknowledged_columns = [")
        lines.extend(f"  {json.dumps(column)}," for column in preserved_acknowledged)
        lines.append("]")
    lines.extend(_promoted_access_tier_groups(cfg, source_env_dir, dest_cfg))
    if preserved_promote:
        lines.append("")
        lines.append("# Saved by make promote-to (source env + catalog map for the next promote).")
        lines.extend(f"{name} = {_promotion_setting(value)}" for name, value in preserved_promote.items())

    # Write
    os.makedirs(dest_env_dir, exist_ok=True)
    with open(dest_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"  Wrote {dest_path}")
    for src, dest in pairs.items():
        print(f"  Catalog remap: {src} -> {dest}")


if __name__ == "__main__":
    main()
