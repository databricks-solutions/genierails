#!/usr/bin/env python3
"""Sync tag policy values from abac.auto.tfvars to Databricks via REST API.

The Databricks Terraform provider has a bug where it reorders tag policy
values after apply, causing "Provider produced inconsistent result" errors.
This script bypasses Terraform by updating tag policy values directly via
the Databricks REST API, so Terraform can use ignore_changes = [values] safely.

The sync is add-only, in every governance mode: it adds missing values and
never removes one. Live values this checkout doesn't desire (another repo's,
another team's, a legacy env's) are reported as left in place and the sync
continues. With ``--remove KEY:VALUE`` (repeatable) it also removes exactly
the named values, after checking they're unused (below); only ``make
apply-account`` passes them, from its approved manifest. ``--mode replace``
(remove every undesired value, no usage check) is refused: a tag policy is
account-wide, so its values may be used outside this checkout.

A live deterministic gr_treatment value is never removed: only the staged
retirement removes a generation.

A named removal is refused, and nothing at all is written, when the value
isn't live, is still desired, is a deterministic generation, or is still in
use: assigned to any catalog, schema, table, column or volume in
``system.information_schema`` (every catalog the deployer can read), or
referenced by an ABAC policy on any catalog, schema or table the deployer can
list (policies attach at every level, and a policy can name a value no
securable carries yet). If that check can't run or can't see everything (no
``--warehouse-id``, a failed or truncated query, a listing or policy read that
fails on any securable), the removal is refused too.

``--dry-run`` reports exactly what the real run would do, refusals and exit
code included, and writes nothing.

The tag-policy API only supports replacing the complete value list; it has no
per-value add or optimistic-concurrency token. This script refreshes and merges
the live list immediately before each PATCH and verifies this checkout's
desired values afterward. One race is therefore unavoidable: a value another
writer adds after that refresh but before the PATCH can still be dropped. It
heals only if that writer also verifies/retries; older clients, the UI, and
other tools may not. The bounded retry prevents this run's desired additions
from being silently lost, but cannot make the full-list API atomic.

Usage:
    python3 scripts/sync_tag_policies.py [--remove KEY:VALUE ...]
        [--warehouse-id ID] [--dry-run [--json]] [path/to/abac.auto.tfvars]
"""
import argparse
import json
import os
import random
import shlex
import sys
import time
from pathlib import Path

WORK_DIR = Path.cwd()
TAG_POLICY_UPDATE_ATTEMPTS = 3


def retained_deterministic_values(key: str, current_values: set[str]) -> set[str]:
    """Live deterministic gr_treatment values (``<treatment>__<type>__<gen>``)."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from governance_policies import TAG_KEY, parse_treatment_value

    if key != TAG_KEY and not key.startswith(TAG_KEY + "_"):
        return set()
    return {value for value in current_values if parse_treatment_value(value)}


def _load_auth():
    """Read auth.auto.tfvars and return config dict + set SDK env vars."""
    auth_path = WORK_DIR / "auth.auto.tfvars"
    from auth_configured import AuthStatus, auth_status
    status = auth_status(auth_path)
    if status is AuthStatus.UNREADABLE:
        raise SystemExit(f"ERROR: {auth_path} can't be read") from None
    if status is AuthStatus.MALFORMED:
        raise SystemExit(f"ERROR: {auth_path} is not valid HCL") from None
    if status is AuthStatus.INCOMPLETE:
        raise SystemExit(
            f"ERROR: fill in {auth_path} (workspace host, client ID, and client secret)"
        ) from None
    try:
        import hcl2
    except ImportError:
        import subprocess
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", "python-hcl2>=7.3.1,<8"],
        )
        import hcl2

    try:
        with open(auth_path) as f:
            cfg = hcl2.load(f)
    except Exception:
        cfg = None
    if cfg is None:
        # Raised outside the handler with no parser text, which can echo a
        # malformed secret (also via __context__).
        raise SystemExit(f"ERROR: {auth_path} is not valid HCL") from None

    mapping = {
        "databricks_workspace_host": "DATABRICKS_HOST",
        "databricks_client_id": "DATABRICKS_CLIENT_ID",
        "databricks_client_secret": "DATABRICKS_CLIENT_SECRET",
    }
    for tfvar_key, env_key in mapping.items():
        val = cfg.get(tfvar_key, "").strip()
        if val:
            os.environ[env_key] = val

    return cfg


def _str(v):
    return (v[0] if isinstance(v, list) else v or "").strip()


REPLACE_REFUSED = (
    "--mode replace is refused: it would delete every live tag-policy value this checkout doesn't desire, "
    "from the account tag policy for every workspace on the metastore, with no usage check. Remove named "
    "values with 'make account-plan ALLOW_TAG_VALUE_REMOVAL=<key>:<value>,... WAREHOUSE_ID=<id>', get the "
    "manifest approved, then 'make apply-account'."
)

REMOVAL_HINT = (
    "This sync never removes values. Removing one deletes it from the account tag policy for every "
    "workspace on the metastore, including tags and ABAC policies outside this repo. If that is intended, "
    "run 'make account-plan ALLOW_TAG_VALUE_REMOVAL=<key>:<value>,... WAREHOUSE_ID=<id>' (it checks the "
    "values are unused), get the manifest approved, then 'make apply-account'."
)


class Refusal(Exception):
    pass


class StaleRemoval(RuntimeError):
    """A named removal a concurrent writer restored after the PATCH."""

    def __init__(self, tag_key, values):
        self.tag_key, self.values = tag_key, sorted(values)
        super().__init__(
            "named removal became stale after the usage check; concurrent writer restored "
            f"{self.values} after the PATCH, so no retry was attempted"
        )


def stale_removal_next_steps(pending, warehouse_id):
    """The commands that re-plan the named removals still pending, for review."""
    removal = ",".join(f"{key}:{value}" for key, value in pending)
    return (
        "Next: re-run the usage check and plan the removal with "
        f"'make account-plan ALLOW_TAG_VALUE_REMOVAL={shlex.quote(removal)} "
        f"WAREHOUSE_ID={shlex.quote(warehouse_id or '<id>')}', review its manifest, get it approved, "
        "then run 'make apply-account'."
    )


def parse_removal(text):
    """``KEY:VALUE`` -> ``(key, value)``; the value may itself contain ':'."""
    key, sep, value = text.partition(":")
    if not sep or not key.strip() or not value.strip():
        raise Refusal(f"--remove {text!r}: expected KEY:VALUE")
    return key.strip(), value.strip()


def _live_key(key, existing):
    # Exact match first, then suffix match (Terraform appends _<hex> suffix
    # to tag keys for account-level isolation in parallel test environments).
    if key in existing:
        return key
    return next((db_key for db_key in existing if db_key.startswith(key + "_")), None)


def plan_updates(desired_policies, existing, removals=()):
    """``(updates, left, refused)``.

    ``updates`` is ``[(actual_key, desired, added, removed)]``: this checkout's
    desired values per key. ``left`` is ``[(actual_key, values)]``: live values
    add-only leaves in place though this checkout doesn't desire them.
    ``refused`` is ``[(key, value, reason)]``: named removals that can't run.
    """
    updates, left, refused = [], [], []
    named = {}
    for key, value in removals:
        named.setdefault(key, set()).add(value)
    seen = set()
    for tp in desired_policies:
        key = tp["key"]
        desired_values = set(tp["values"])
        actual_key = _live_key(key, existing)
        if actual_key is None:
            continue
        current_values = existing[actual_key]

        # A deterministic treatment value stays allowed while a retained policy
        # generation or a not-yet-retagged column may still use it; only the
        # staged retirement removes generations, so this sync never does.
        retained = retained_deterministic_values(key, current_values)
        missing = desired_values - current_values
        wanted = named.get(key, set()) | named.get(actual_key, set())
        seen.update(k for k in (key, actual_key) if k in named)
        remove = set()
        for value in sorted(wanted):
            if value in desired_values:
                refused.append((actual_key, value, "still desired by this checkout's abac.auto.tfvars"))
            elif value in retained:
                refused.append((actual_key, value, "a deterministic generation; only the staged retirement removes it"))
            elif value not in current_values:
                refused.append((actual_key, value, "not a live value of the tag policy"))
            else:
                remove.add(value)
        foreign = current_values - desired_values - retained - remove
        if foreign:
            left.append((actual_key, sorted(foreign)))
        if missing or remove:
            updates.append((actual_key, sorted(desired_values), sorted(missing), sorted(remove)))
    for key in sorted(set(named) - seen):
        for value in sorted(named[key]):
            refused.append((key, value, "no tag policy of this checkout has this key live"))
    return updates, left, refused


def update_diff(updates, left=()):
    """Stable JSON shape: per key, the values a sync would add/remove/leave in place."""
    entries = {}
    for actual_key, _values, added, removed in updates:
        if added or removed:
            entries[actual_key] = {"key": actual_key, "added": list(added), "removed": list(removed)}
    for actual_key, values in left:
        entry = entries.setdefault(actual_key, {"key": actual_key, "added": [], "removed": []})
        entry["left_in_place"] = list(values)
    return [entries[k] for k in sorted(entries)]


def _policy_values(w, tag_key):
    """Return the current values for one policy, failing if it disappears."""
    policy = w.tag_policies.get_tag_policy(tag_key)
    return {
        getattr(value, "name", "") or ""
        for value in (getattr(policy, "values", None) or [])
        if getattr(value, "name", "") or ""
    }


def _update_values_without_lost_additions(w, tag_key, desired, removed, TagPolicy, Value):
    """Reconcile a full-list PATCH without overwriting additions we can observe.

    The API/SDK exposes neither a per-value add nor an optimistic-concurrency
    token.  Therefore every attempt refreshes the live list immediately before
    PATCH, unions it with this checkout's desired values, and verifies after PATCH.
    A concurrent full-list writer that lands after us can remove our additions;
    the verification detects that and retries from its new list.  Values added
    by another writer and visible to an attempt are always carried forward.
    """
    desired, removed = set(desired), set(removed)
    last_live = set()
    for attempt in range(1, TAG_POLICY_UPDATE_ATTEMPTS + 1):
        live = _policy_values(w, tag_key)
        write_values = (live | desired) - removed
        w.tag_policies.update_tag_policy(
            tag_key=tag_key,
            tag_policy=TagPolicy(tag_key=tag_key, values=[Value(name=v) for v in sorted(write_values)]),
            update_mask="values",
        )
        last_live = _policy_values(w, tag_key)
        reappeared = removed & last_live
        if reappeared:
            raise StaleRemoval(tag_key, reappeared)
        if desired <= last_live:
            return
        missing = sorted(desired - last_live)
        print(f"  [WARN] {tag_key}: concurrent tag-policy update detected after attempt {attempt}; "
              f"missing desired values={missing}")
        if attempt < TAG_POLICY_UPDATE_ATTEMPTS:
            time.sleep(random.uniform(0.05, 0.15))
    raise RuntimeError(
        f"concurrent tag-policy updates did not converge after {TAG_POLICY_UPDATE_ATTEMPTS} attempts; "
        f"missing desired values={sorted(desired - last_live)}"
    )


# -- usage check for named removals ----------------------------------------------------

TAG_VIEWS = (
    ("catalog", "catalog_tags", ("catalog_name",)),
    ("schema", "schema_tags", ("catalog_name", "schema_name")),
    ("table", "table_tags", ("catalog_name", "schema_name", "table_name")),
    ("column", "column_tags", ("catalog_name", "schema_name", "table_name", "column_name")),
    ("volume", "volume_tags", ("catalog_name", "schema_name", "volume_name")),
)
SKIPPED_CATALOGS = ("system",)


def _sql_list(values):
    return ", ".join("'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'" for v in sorted(values))


def assignment_sql(keys, values):
    """Every visible assignment of any of ``values`` under any of ``keys``."""
    parts = [
        f"SELECT '{kind}' AS kind, concat_ws('.', {', '.join(cols)}) AS securable, tag_name, tag_value "
        f"FROM system.information_schema.{view} "
        f"WHERE lower(tag_name) IN ({_sql_list(k.lower() for k in keys)}) AND tag_value IN ({_sql_list(values)})"
        for kind, view, cols in TAG_VIEWS
    ]
    return " UNION ALL ".join(parts) + " ORDER BY kind, securable, tag_name, tag_value"


def policy_securables(w):
    """``(catalogs, [(type, full_name)])``: every catalog, schema and table the caller can list.

    ABAC policies attach to catalogs, schemas and tables, and a policy can name
    a value that no securable carries yet, so every one is listed, not only
    those tagged with the key. Unbound and browse-only catalogs are included:
    a catalog bound to another workspace still shares the account tag policy.
    A listing that fails anywhere raises, so the caller refuses.
    """
    try:
        catalogs = sorted(c.name for c in w.catalogs.list(include_browse=True, include_unbound=True)
                          if c.name and c.name not in SKIPPED_CATALOGS)
    except Exception as exc:
        raise Refusal(f"could not list catalogs ({exc})") from exc
    securables = []
    for catalog in catalogs:
        securables.append(("CATALOG", catalog))
        try:
            schemas = list(w.schemas.list(catalog_name=catalog, include_browse=True))
        except Exception as exc:
            raise Refusal(f"could not list the schemas of catalog {catalog} ({exc})") from exc
        securables.extend(("SCHEMA", s.full_name or f"{catalog}.{s.name}")
                          for s in sorted(schemas, key=lambda s: s.name or ""))
        try:
            tables = list(w.tables.list_summaries(catalog_name=catalog))
        except Exception as exc:
            raise Refusal(f"could not list the tables of catalog {catalog} ({exc})") from exc
        names = [t.full_name for t in tables]
        if not all(names):
            raise Refusal(f"a table of catalog {catalog} was listed without a full name")
        securables.extend(("TABLE", name) for name in sorted(names))
    return catalogs, securables


def _conditions(policy):
    texts = [str(getattr(policy, "when_condition", "") or "")]
    for match in getattr(policy, "match_columns", None) or []:
        texts.append(str(getattr(match, "condition", "") or ""))
    return [t for t in texts if t]


def references(policy, key, value):
    """Conservative: a condition naming the key and the quoted value references it."""
    for text in _conditions(policy):
        if key.lower() in text.lower() and (f"'{value}'" in text or f'"{value}"' in text):
            return True
    return False


def check_usage(w, warehouse_id, removals):
    """``(in_use, evidence)`` for ``[(actual_key, value)]``; raise Refusal if it can't tell.

    ``in_use`` maps each used ``(key, value)`` to the places that use it.
    """
    if not warehouse_id:
        raise Refusal("no SQL warehouse to read system.information_schema; set WAREHOUSE_ID=<id>")
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from scripts.audit_schema_drift import _run_sql

    keys = sorted({k for k, _ in removals})
    values = sorted({v for _, v in removals})
    wanted = set(removals)
    in_use = {}
    try:
        rows = _run_sql(w, warehouse_id, assignment_sql(keys, values), complete=True)
    except Exception as exc:
        raise Refusal(f"could not read tag assignments from system.information_schema ({exc})") from exc
    for kind, securable, tag_name, tag_value in (tuple(r) for r in rows):
        for key, value in wanted:
            if str(tag_name).lower() == key.lower() and tag_value == value:
                in_use.setdefault((key, value), []).append(f"{kind} {securable}")
    catalogs, securables = policy_securables(w)
    for kind, name in securables:
        try:
            policies = list(w.policies.list_policies(kind, name))
        except Exception as exc:
            raise Refusal(f"could not list ABAC policies on {kind.lower()} {name} ({exc})") from exc
        for policy in policies:
            for key, value in wanted:
                if references(policy, key, value):
                    in_use.setdefault((key, value), []).append(
                        f"ABAC policy {getattr(policy, 'name', '?')} on {kind.lower()} {name}")
    evidence = {"warehouse_id": warehouse_id, "catalogs": len(catalogs), "policy_securables": len(securables),
                "tables": sum(kind == "TABLE" for kind, _ in securables)}
    return {k: sorted(v) for k, v in in_use.items()}, evidence


def _report(updates, left, refused, dry_run):
    verb = "[PLAN]" if dry_run else "[SYNC]"
    for actual_key, values in left:
        print(f"  [WARN] {actual_key}: {len(values)} live value(s) not desired by this checkout are left "
              f"in place: {values}")
    if left:
        print(f"  {REMOVAL_HINT}")
    if refused:
        # Nothing is written: a partial sync would leave the account half-updated.
        print("  [ERROR] Tag-policy value removal refused; nothing was written:", file=sys.stderr)
        for key, value, reason in refused:
            print(f"    - {key}:{value}: {reason}", file=sys.stderr)
        return
    if dry_run:
        for actual_key, _values, missing, removed in updates:
            changes = [f"would add {missing}"] if missing else []
            changes += [f"would remove {removed}"] if removed else []
            print(f"  {verb} {actual_key}: {', '.join(changes) if changes else 'would reassert desired values'}")
        if not updates:
            print(f"  {verb} tag policies already in sync")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Sync tag policy values to Databricks.")
    parser.add_argument("--mode", choices=("add-only", "replace"), default="add-only",
                        help="add-only, the only mode; replace is refused")
    parser.add_argument("--remove", action="append", default=[], metavar="KEY:VALUE",
                        help="also remove this live value once it's proven unused (repeatable)")
    parser.add_argument("--warehouse-id", default="", help="SQL warehouse for the --remove usage check")
    parser.add_argument("--dry-run", action="store_true", help="report what the run would do; write nothing")
    parser.add_argument("--json", action="store_true", help="with --dry-run, print the add/remove diff as JSON")
    parser.add_argument("tfvars", nargs="?", default=str(WORK_DIR / "abac.auto.tfvars"))
    args = parser.parse_args(argv)
    if args.mode == "replace":
        print(f"  [ERROR] {REPLACE_REFUSED}", file=sys.stderr)
        raise SystemExit(1)
    try:
        removals = sorted({parse_removal(item) for raw in args.remove for item in raw.split(",") if item.strip()})
    except Refusal as exc:
        print(f"  [ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)
    tfvars_path = Path(args.tfvars)
    desired_policies = []
    if tfvars_path.exists():
        import hcl2

        with open(tfvars_path) as f:
            desired_policies = hcl2.load(f).get("tag_policies", [])
    if not desired_policies and not removals:
        if args.dry_run and args.json:
            print("[]")
        else:
            print(f"  [SKIP] {tfvars_path} not found" if not tfvars_path.exists()
                  else "  [SKIP] No tag_policies found in config")
        return
    auth_cfg = _load_auth()

    # Get auth token from the env-pinned OAuth client.
    from auth_configured import workspace_client
    from databricks.sdk.service.tags import TagPolicy, Value
    host = _str(auth_cfg.get("databricks_workspace_host", ""))
    client_id = _str(auth_cfg.get("databricks_client_id", ""))
    client_secret = _str(auth_cfg.get("databricks_client_secret", ""))

    w = workspace_client(
        WORK_DIR / "auth.auto.tfvars",
        product="genierails",
        product_version="0.1.0",
    )
    token = w.config.authenticate()  # {'Authorization': 'Bearer ...'}
    base = host.rstrip("/")
    if not base:
        print("  [WARN] No workspace host found; skipping tag policy sync")
        return

    # List existing tag policies via SDK. The REST list path has returned 404
    # in some workspaces even though the tag policy service is available.
    existing = {}
    try:
        for tp in w.tag_policies.list_tag_policies():
            tag_key = getattr(tp, "tag_key", "") or ""
            values = {
                getattr(v, "name", "") or ""
                for v in (getattr(tp, "values", None) or [])
                if getattr(v, "name", "") or ""
            }
            existing[tag_key] = values
    except Exception as list_err:
        print(f"  [ERROR] Could not list tag policies ({list_err})")
        raise SystemExit(1)

    try:
        updates, left, refused = plan_updates(desired_policies, existing, removals)
    except Refusal as exc:
        print(f"  [ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)
    planned = [(k, v) for k, _values, _added, removed in updates for v in removed] if removals else []
    checks = {}
    if planned and not refused:
        try:
            in_use, checks = check_usage(w, args.warehouse_id or os.environ.get("WAREHOUSE_ID", ""), planned)
        except Refusal as exc:
            refused = [(k, v, f"usage unknown: {exc}") for k, v in planned]
        else:
            refused = [(k, v, "still in use: " + "; ".join(where)) for (k, v), where in sorted(in_use.items())]
    if args.dry_run and args.json:
        if refused:
            _report([], [], refused, True)
            raise SystemExit(1)
        diff = update_diff(updates, left)
        for entry in diff:
            if entry["removed"]:
                entry["removal_check"] = dict(checks)
        print(json.dumps(diff, sort_keys=True))
        return
    _report(updates, left, refused, args.dry_run)
    if refused:
        raise SystemExit(1)
    if args.dry_run:
        return

    updated = 0
    for index, (actual_key, values, missing, removed) in enumerate(updates):
        try:
            _update_values_without_lost_additions(
                w, actual_key, values, removed, TagPolicy, Value,
            )
            changes = []
            if missing:
                changes.append(f"added {missing}")
            if removed:
                changes.append(f"removed {removed}")
            if not changes:
                changes.append("reasserted desired values")
            print(f"  [SYNC] {actual_key}: {', '.join(changes)}")
            updated += 1
        except StaleRemoval as e:
            # Earlier keys' removals landed; this key's restored values and every
            # later key's removals are still pending.
            pending = [(actual_key, v) for v in e.values]
            pending += [(k, v) for k, _values, _added, later in updates[index + 1:] for v in later]
            print(f"  [ERROR] {actual_key}: {e}")
            print(f"  {stale_removal_next_steps(pending, args.warehouse_id or os.environ.get('WAREHOUSE_ID', ''))}")
            raise SystemExit(1)
        except Exception as e:
            print(f"  [ERROR] {actual_key}: {e}")
            raise SystemExit(1)

    if updated:
        print(f"  Synced {updated} tag policy/ies")
    else:
        print("  Tag policies already in sync")


if __name__ == "__main__":
    main()
