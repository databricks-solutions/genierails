"""``group_map``: dev group names to the destination's (design decision 8).

``access_tier_groups`` is governance, promoted from dev, so prod uses exactly
the rehearsed tiers. Where prod's group names differ, the destination's
env-owned ``group_map`` maps them, like ``catalog_map`` maps catalogs::

    group_map = { dev_analysts = "prod_analysts", dev_viewers = "prod_viewers" }

``make promote-to`` maps every group it writes: ``access_tier_groups``,
``row_filters`` ``values_by_group``, the principals of the promoted policies
and the generated ``groups`` (and ``group_members``) keys that define them, and
the first seeding of ``acl_groups`` and ``table_readers``. A dev group
with no mapping is used as is. ``raw_exempt_principals`` are env-owned and
never mapped. Nested membership is expanded only after mapping (the reader
and agent checks read the destination's own, mapped config).

promote-to stays a pure function of committed files. What needs the live
destination runs in rehearse and release (release-plan once it exists):
``check`` refuses any resulting group that doesn't exist there, mapped or
not, including every principal of the committed promoted policies, and ``check-ack`` refuses a changed ``group_map`` (added, removed or
retargeted mapping, against the one the last apply used) unless each change
is acknowledged as ``<env>:weaken:group_map:<dev>=<target>`` in
``envs/<env>/ack.txt`` (for a removed mapping the target is the dev group
itself).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable, Iterable, Mapping

SETTING = "group_map"
APPLIED_RECORD = ".group_map_applied.json"  # runtime, written after a successful apply
EVERYONE = frozenset({"account users", "users"})


class GroupMapError(ValueError):
    """group_map can't be applied safely."""


def _load_hcl(path: Path) -> dict:
    import hcl2

    if not path.is_file():
        return {}
    try:
        value = hcl2.loads(path.read_text())
    except Exception as exc:
        raise GroupMapError(f"cannot parse {path}: {exc}") from exc
    return value if isinstance(value, dict) else {}


def validation_errors(cfg: Mapping) -> list[str]:
    value = cfg.get(SETTING)
    if value is None:
        return []
    if cfg.get("governance_mode", "legacy") != "deterministic":
        return [f'{SETTING} applies only with governance_mode = "deterministic"']
    if not isinstance(value, dict) or not all(
            isinstance(k, str) and k.strip() and isinstance(v, str) and v.strip() for k, v in value.items()):
        return [f"{SETTING} must map dev group names to destination group names"]
    errors = []
    targets: dict[str, list[str]] = {}
    for dev, target in value.items():
        targets.setdefault(target, []).append(dev)
        if dev.lower() in EVERYONE or target.lower() in EVERYONE:
            errors.append(f'{SETTING}["{dev}"] = "{target}": account users is never mapped')
    for target, devs in sorted(targets.items()):
        if len(devs) > 1:
            errors.append(f"{SETTING} maps {', '.join(sorted(devs))} all to {target!r}: many-to-one mappings "
                          "merge tiers or readers and are refused")
    exempt = set(cfg.get("raw_exempt_principals") or [])
    for dev in sorted(set(value) & exempt):
        errors.append(f"{SETTING} maps {dev!r}, which is in raw_exempt_principals; those are env-owned "
                      "and never mapped")
    return errors


def mapping(cfg: Mapping) -> dict[str, str]:
    value = cfg.get(SETTING) or {}
    return {str(k): str(v) for k, v in value.items()} if isinstance(value, dict) else {}


def map_group(group: str, group_map: Mapping[str, str], exempt: Iterable[str] = ()) -> str:
    return group if group in set(exempt) else group_map.get(group, group)


def map_groups(groups: Iterable[str], group_map: Mapping[str, str], exempt: Iterable[str] = ()) -> list[str]:
    exempt = set(exempt)
    result: list[str] = []
    for group in groups:
        mapped = map_group(group, group_map, exempt)
        if mapped not in result:
            result.append(mapped)
    return result


def referenced_groups(cfg: Mapping) -> list[str]:
    """Every group the env's governance and agents name (the promoted, mapped ones)."""
    groups: set[str] = set(cfg.get("access_tier_groups") or [])
    for readers in (cfg.get("table_readers") or {}).values():
        groups.update(readers or [])
    for space in cfg.get("genie_spaces") or []:
        if isinstance(space, dict):
            groups.update(space.get("acl_groups") or [])
    for rule in cfg.get("row_filters") or []:
        if isinstance(rule, dict):
            groups.update((rule.get("values_by_group") or {}).keys())
    return sorted(g for g in groups if isinstance(g, str) and g.lower() not in EVERYONE)


# Committed promotion output whose policies name principals: the split
# data_access config Terraform applies and the generated config it comes from.
PROMOTED_CONFIGS = (Path("data_access") / "abac.auto.tfvars", Path("generated") / "abac.auto.tfvars")


def policy_principals(env_dir: Path) -> list[str]:
    """Every to_principals / except_principals of the committed promoted policies."""
    names: set[str] = set()
    for relative in PROMOTED_CONFIGS:
        for policy in _load_hcl(Path(env_dir) / relative).get("fgac_policies") or []:
            if not isinstance(policy, dict):
                raise GroupMapError(f"{Path(env_dir) / relative}: fgac_policies entries must be objects")
            for field in ("to_principals", "except_principals"):
                names.update(str(p) for p in policy.get(field) or [])
    return sorted(n for n in names if n.lower() not in EVERYONE)


def missing_groups(cfg: Mapping, existing: Iterable[str], policy_names: Iterable[str] = (),
                   deployer: str = "") -> list[str]:
    present = set(existing) | ({deployer} if deployer else set())
    return sorted({g for g in [*referenced_groups(cfg), *policy_names] if g not in present})


def _workspace_groups(env_dir: Path) -> list[str]:
    """Group names and service principal application IDs visible in the workspace."""
    from scripts.audit_schema_drift import _get_sdk_client

    client = _get_sdk_client(env_dir)
    names = []
    for group in client.groups.list(attributes="displayName"):
        if not getattr(group, "display_name", None):
            raise GroupMapError(f"group {getattr(group, 'id', '?')!r} has no visible name")
        names.append(str(group.display_name))
    for sp in client.service_principals.list(attributes="applicationId"):
        if getattr(sp, "application_id", None):
            names.append(str(sp.application_id))
    return names


def applied_record_path(env_dir: Path) -> Path:
    """The record of the last applied group_map (under GENIERAILS_STATE_DIR when set)."""
    from scripts.state_layout import record_path
    return record_path(Path(env_dir), APPLIED_RECORD)


def _applied(env_dir: Path) -> dict[str, str] | None:
    path = applied_record_path(env_dir)
    if not path.is_file():
        return None
    try:
        value = json.loads(path.read_text()).get(SETTING)
    except (OSError, ValueError, AttributeError) as exc:
        raise GroupMapError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GroupMapError(f"{path}: {SETTING} must be a map")
    return {str(k): str(v) for k, v in value.items()}


def _ever_applied(env_dir: Path) -> bool:
    from scripts.state_layout import state_json
    env_dir = Path(env_dir)
    return any(state_json(env_dir, env_dir.name, layer) is not None
               for layer in ("data_access", "workspace"))


def required_acks(env_dir: Path, env: str, cfg: Mapping) -> dict[str, str]:
    """ack key -> consequence for every mapping that differs from the last apply.

    With no record, a fresh env (no state) starts from its first group_map;
    an env that was already applied counts as having had none.
    """
    current = mapping(cfg)
    applied = _applied(env_dir)
    if applied is None:
        if not _ever_applied(env_dir):
            return {}
        applied = {}
    keys = {}
    for dev in sorted(set(current) | set(applied)):
        before, after = applied.get(dev, dev), current.get(dev, dev)
        if before != after:
            keys[f"{env}:weaken:{SETTING}:{dev}={after}"] = (
                f"{dev} now resolves to {after!r} instead of {before!r}: that group gets the tier, "
                f"readers and CAN_RUN {dev!r} has in {env}")
    return keys


def ack_problems(env_dir: Path, env: str, cfg: Mapping) -> list[str]:
    from scripts.ack_file import parse_file

    needed = required_acks(env_dir, env, cfg)
    if not needed:
        return []
    ack_path = Path(env_dir) / "ack.txt"
    entries = parse_file(ack_path, env) if ack_path.is_file() else ()
    have = {entry.key for entry in entries}
    return [f"{key}  ({why})" for key, why in needed.items() if key not in have]


def record_applied(env_dir: Path, cfg: Mapping) -> None:
    path = applied_record_path(env_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({SETTING: mapping(cfg)}, indent=2, sort_keys=True) + "\n")


def main(argv: list[str] | None = None, lister: Callable[[Path], Iterable[str]] = _workspace_groups) -> int:
    shared = Path(__file__).resolve().parent
    if str(shared) not in sys.path:
        sys.path.insert(0, str(shared))
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("check", "record"))
    parser.add_argument("--env-dir", required=True, type=Path)
    parser.add_argument("--env", required=True)
    parser.add_argument("--command-name", default="this command")
    args = parser.parse_args(argv)
    try:
        from agent_files import merged_config

        cfg = merged_config(args.env_dir)
        if cfg.get("governance_mode", "legacy") != "deterministic":
            return 0
        if args.command == "record":
            record_applied(args.env_dir, cfg)
            return 0
        problems = validation_errors(cfg)
        policy_names = policy_principals(args.env_dir) if not problems else []
        deployer = str(cfg.get("deployer_principal_id") or "")
        missing = missing_groups(cfg, lister(args.env_dir), policy_names, deployer) if not problems else []
        unacked = ack_problems(args.env_dir, args.env, cfg) if not problems else []
    except Exception as exc:  # an unreadable account or ack file never passes
        print(f"ERROR: {args.command_name} refused: the group check could not run: {exc}", file=sys.stderr)
        return 2
    for problem in problems:
        print(f"ERROR: {args.command_name} refused: {problem}", file=sys.stderr)
    if missing:
        in_policies = [name for name in missing if name in set(policy_names)]
        where = (f" ({', '.join(in_policies)} named in the promoted policies)" if in_policies else "")
        print(f"ERROR: {args.command_name} refused: these groups don't exist in {args.env} "
              f"(after group_map): {', '.join(missing)}{where}. Create them, or map the dev names with "
              "group_map in env.auto.tfvars and re-run make promote-to.", file=sys.stderr)
    if unacked:
        print(f"ERROR: {args.command_name} refused: group_map changed since the last apply, which is a "
              f"weakening. Review, then add to {args.env_dir / 'ack.txt'}:", file=sys.stderr)
        for line in unacked:
            print(f"  {line}", file=sys.stderr)
    return 1 if problems or missing or unacked else 0


if __name__ == "__main__":
    raise SystemExit(main())
