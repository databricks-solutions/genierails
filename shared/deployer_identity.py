"""``deployer_principal_id``: the env's deployer SP, committed (decision 8).

Deterministic policies exempt the deployer SP that applies them, so its
application ID is part of the governance output. Each env applies with its
own SP and commits that SP's application ID in ``env.auto.tfvars``::

    deployer_principal_id = "5a7e0000-0000-0000-0000-00000000beef"

The ID isn't secret. ``make generate``, validation, ``make check`` and
``make promote-to`` (which swaps dev's for prod's in the promoted EXCEPT
lists) read only this committed value, never ``auth.auto.tfvars``, so they
need no credentials. Before taking the env lock, rehearse, release-plan,
release and maintain refuse unless the ``databricks_client_id`` they will
apply with is this same ID.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Mapping

SETTING = "deployer_principal_id"
_APPLICATION_ID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


class DeployerIdentityError(ValueError):
    """The deployer identity is missing, malformed or doesn't match."""


def _load_hcl(path: Path) -> dict:
    if not path.is_file():
        return {}
    from scripts.hcl2_guard import import_hcl2

    hcl2 = import_hcl2()
    try:
        value = hcl2.loads(path.read_text())
    except Exception as exc:
        if path.name != "auth.auto.tfvars":
            raise DeployerIdentityError(f"cannot parse {path}: {exc}") from exc
        value = None
    if value is None:
        # Raised outside the handler with no parser text, which can echo a
        # malformed secret (also via __context__).
        raise DeployerIdentityError(f"{path} is not valid HCL") from None
    return value if isinstance(value, dict) else {}


def validation_errors(cfg: Mapping) -> list[str]:
    """Required in deterministic mode; when set, an application ID (never everyone)."""
    value = cfg.get(SETTING)
    deterministic = cfg.get("governance_mode", "legacy") == "deterministic"
    if value is None or value == "":
        if deterministic:
            return [f"{SETTING} is required in deterministic mode: set it to the application ID of the "
                    "service principal that applies this env (databricks_client_id in its auth.auto.tfvars)"]
        return []
    if isinstance(value, str) and value.startswith("<") and value.endswith(">"):
        return [f"replace the {SETTING} placeholder {value!r} with the application ID of the "
                "service principal that applies this env"]
    if not isinstance(value, str) or not _APPLICATION_ID.fullmatch(value):
        # A group name (account users, users) or anything else here would be exempted from every mask.
        return [f"{SETTING} must be a service principal application ID "
                f"(xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx), not {value!r}"]
    return []


def committed(env_dir: Path) -> str:
    """The env's committed deployer ID ("" when unset). Raises on a malformed value."""
    cfg = _load_hcl(Path(env_dir) / "env.auto.tfvars")
    value = cfg.get(SETTING)
    if value in (None, ""):
        return ""
    errors = validation_errors({SETTING: value})
    if errors:
        raise DeployerIdentityError(f"{Path(env_dir) / 'env.auto.tfvars'}: {errors[0]}")
    return str(value)


def promotion_deployers(source_env_dir: Path, dest_env_dir: Path, policies: list) -> tuple[str, str] | None:
    """(source, destination) deployer IDs a deterministic promotion swaps, or None.

    None when the source isn't deterministic or no promoted policy exempts its
    deployer. Raises when a policy does and the destination has no valid ID.
    """
    source_cfg = _load_hcl(Path(source_env_dir) / "env.auto.tfvars")
    if source_cfg.get("governance_mode", "legacy") != "deterministic":
        return None
    source = committed(source_env_dir)
    named = source and any(isinstance(policy, dict) and source in (policy.get("except_principals") or [])
                           for policy in policies)
    if not named:
        return None
    dest = committed(dest_env_dir)
    if not dest:
        raise DeployerIdentityError(
            f"{SETTING} is not set, and the promoted policies exempt the source deployer {source!r}: set it "
            f"in {Path(dest_env_dir) / 'env.auto.tfvars'} to the application ID of the service principal "
            "that applies that env, then re-run make promote-to")
    return source, dest


def apply_problems(env_dir: Path) -> list[str]:
    """Why a deterministic env can't be applied with its auth.auto.tfvars SP ([] when it can)."""
    env_dir = Path(env_dir)
    cfg = _load_hcl(env_dir / "env.auto.tfvars")
    if cfg.get("governance_mode", "legacy") != "deterministic":
        return []
    errors = validation_errors(cfg)
    if errors:
        return errors
    expected = str(cfg[SETTING])
    auth_path = env_dir / "auth.auto.tfvars"
    actual = str(_load_hcl(auth_path).get("databricks_client_id") or "")
    if actual.lower() != expected.lower():
        return [f"{auth_path} databricks_client_id is {actual or 'not set'}, but env.auto.tfvars "
                f"{SETTING} is {expected}: the policies exempt {expected}, so apply with that service "
                f"principal, or set {SETTING} to the one you apply with, make generate, and commit"]
    return []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("check",))
    parser.add_argument("--env-dir", required=True, type=Path)
    parser.add_argument("--command-name", default="this command")
    args = parser.parse_args(argv)
    try:
        problems = apply_problems(args.env_dir)
    except DeployerIdentityError as exc:
        problems = [str(exc)]
    for problem in problems:
        print(f"ERROR: {args.command_name} refused before taking the lock: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
