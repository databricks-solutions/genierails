#!/usr/bin/env python3
"""Check that every legacy Genie agent can be adopted before the migration.

Earlier versions created Genie agents with null_resource.genie_space_create.
terraform_data.genie_space replaces it and must adopt each existing agent, never
create a second one. This lists every legacy key in the workspace state with
its ID file, agent ID, workspace host and GET status, and fails unless all of
them are adoptable (ID file present, recorded on this host, GET 200).

  genie_adopt_preflight.py <env_dir>          read-only check
  genie_adopt_preflight.py <env_dir> --arm    also mark each key adoption-
                                              required for genie_space.sh create
  --quiet                                     print only when the check fails

`make apply` runs it with --arm before every workspace apply; it prints
nothing when the state holds no legacy agent.

  genie_adopt_preflight.py <env_dir> --id-files [--warn]
      [--env-name <env> --runner <terraform_layer.sh>]
      no workspace call: every agent terraform_data.genie_space created must
      still have its ID file. State does not record the agent ID, so with the
      file gone Terraform plans no change and an apply skips, while the next
      config or ACL change, the removal of the agent and make destroy can no
      longer find it. Fails (or with --warn, warns) naming each missing file;
      only then, with --runner, terraform console says which of them the
      config removed. make runs it on the workspace layer before any early
      exit of apply (fails) and plan (warns), and in maintain (warns).
"""

import argparse
import json
import sys
from pathlib import Path

LEGACY_TYPE = "null_resource"
LEGACY_NAME = "genie_space_create"


def id_file_for(env_dir: Path, key: str) -> Path:
    return env_dir / f".genie_space_id_{key}"


def marker_for(env_dir: Path, key: str) -> Path:
    # Deliberately outside the .genie_space_id_* pattern other tooling globs.
    return env_dir / f".genie_adopt_required_{key}"


def legacy_agents(env_dir: Path) -> list[tuple[str, str]]:
    """Return (key, recorded host) for each legacy agent in the workspace state."""
    state_path = env_dir / "terraform.tfstate"
    if not state_path.exists():
        return []
    state = json.loads(state_path.read_text())
    found = []
    for resource in state.get("resources", []):
        if resource.get("type") != LEGACY_TYPE or resource.get("name") != LEGACY_NAME:
            continue
        for instance in resource.get("instances", []):
            key = instance.get("index_key")
            host = (instance.get("attributes", {}).get("triggers") or {}).get("host", "")
            found.append((str(key), str(host).rstrip("/")))
    return found


CREATED_TYPE = "terraform_data"
CREATED_NAME = "genie_space"


def _state(env_dir: Path) -> dict:
    state_path = env_dir / "terraform.tfstate"
    return json.loads(state_path.read_text()) if state_path.exists() else {}


def _workspace_instances(state: dict, name: str, rtype: str):
    for resource in state.get("resources", []):
        if (resource.get("mode", "managed") == "managed" and resource.get("type") == rtype
                and resource.get("name") == name
                and resource.get("module", "module.workspace") == "module.workspace"):
            yield from resource.get("instances", [])


def created_agents(env_dir: Path) -> list[str]:
    """Keys of the agents terraform_data.genie_space created, as in the state.

    Tainted and deposed objects are left out: Terraform replaces them, and
    create re-adopts the agent (by its ID file, else its unique exact title).
    """
    return [
        str(instance.get("index_key"))
        for instance in _workspace_instances(_state(env_dir), CREATED_NAME, CREATED_TYPE)
        if instance.get("status") != "tainted" and not instance.get("deposed")
    ]


def _attr(instance: dict, field: str) -> dict:
    value = (instance.get("attributes") or {}).get(field) or {}
    value = value.get("value", value) if isinstance(value, dict) and "type" in value else value
    return value if isinstance(value, dict) else {}


def _identify(state: dict, key: str) -> str:
    """Host and tables the state recorded for an agent, to find it by hand."""
    host = next((_attr(i, "triggers_replace").get("host", "")
                 for i in _workspace_instances(state, CREATED_NAME, CREATED_TYPE)
                 if str(i.get("index_key")) == key), "")
    tables = next((_attr(i, "triggers").get("tables", "")
                   for i in _workspace_instances(state, "genie_space_config", "null_resource")
                   if str(i.get("index_key")) == key), "")
    parts = [f"on {host}" if host else "", f"tables {tables}" if tables else ""]
    return "; ".join(p for p in parts if p)


# The agents terraform_data.genie_space keeps (modules/workspace
# managed_created_spaces) as they would be with the ID files restored:
# Terraform's own new_spaces, plus the root's state-verified create-to-ID
# handoffs before their ID-file check (a missing file is what is being
# diagnosed, and it would drop a kept handoff from managed_created_spaces).
DESIRED_CREATED_EXPRESSION = (
    "base64encode(jsonencode(sort(setunion("
    "module.workspace.new_space_keys, keys(local.created_acl_handoff_candidates)))))"
)


def desired_created_keys(env_dir: Path, env_name: str, runner: str) -> set[str] | None:
    """Ask Terraform (console) which created agents the config keeps; None if it can't answer."""
    import base64
    import os
    import subprocess

    try:
        result = subprocess.run([runner, "workspace", env_name, "console"],
                                input=DESIRED_CREATED_EXPRESSION + "\n", capture_output=True, text=True,
                                env={**os.environ, "LAYER_ENV_DIR": str(env_dir)}, timeout=600)
        line = [l.strip() for l in result.stdout.splitlines() if l.strip()][-1]
        if result.returncode != 0 or not (line.startswith('"') and line.endswith('"')):
            return None
        keys = json.loads(base64.b64decode(line[1:-1], validate=True))
        return {str(k) for k in keys} if isinstance(keys, list) else None
    except (OSError, IndexError, ValueError, subprocess.SubprocessError):
        return None


def check_id_files(env_dir: Path, warn: bool, env_name: str = "", runner: str = "") -> int:
    """Every created agent must still have its ID file; refuse (or warn) if not.

    Refusing is right in both cases, and never creates or orphans anything:
    - kept in config: a config or ACL change and make destroy need the ID;
    - removed from config: Terraform would trash the agent by its ID, and
      genie_space.sh trash refuses without one rather than orphan it.
    Only when a file is missing, Terraform is asked which agents the config
    keeps, to say which case applies; if it can't answer, both are described.
    """
    label = "WARNING" if warn else "ERROR"
    try:
        state = _state(env_dir)
        missing = [key for key in created_agents(env_dir)
                   if not (id_file_for(env_dir, key).is_file() and id_file_for(env_dir, key).read_text().strip())]
    except (OSError, ValueError) as exc:
        print(f"{label}: cannot read {env_dir / 'terraform.tfstate'} to check Genie ID files ({exc})"
              + ("" if warn else "; nothing was applied"), file=sys.stderr)
        return 0 if warn else 1
    if not missing:
        return 0
    desired = desired_created_keys(env_dir, env_name, runner) if env_name and runner else None
    outcome = "make apply would refuse" if warn else "nothing was applied"
    print(f"{label}: {len(missing)} Genie agent(s) created by GenieRails lost their ID file; {outcome}:",
          file=sys.stderr)
    removed = []
    for key in missing:
        where = _identify(state, key)
        if desired is not None and key not in desired:
            removed.append(key)
            status = "removed from config, so this apply would trash it"
        elif desired is None:
            status = "in config, or removed from it (could not ask Terraform)"
        else:
            status = "still in config"
        print(f"  {key}: {id_file_for(env_dir, key)} is missing or empty ({status}"
              + (f"; {where}" if where else "") + ")", file=sys.stderr)
    print("  The state does not record the agent ID; this file is the only record of it. Write the\n"
          "  agent's ID (it is in the agent URL) into each file, then re-run.", file=sys.stderr)
    if removed or desired is None:
        print("  For an agent removed from config: GenieRails trashes a removed agent by its ID and, without\n"
              "  it, would orphan it, so the removal is refused too. With the ID written back the removal\n"
              "  trashes the agent; if you already deleted it by hand, write its old ID (a 404 counts as\n"
              "  already deleted). To keep the agent instead, put it back in config.", file=sys.stderr)
    if len(missing) > len(removed):
        print("  For an agent still in config: a config or ACL change and make destroy can't find it\n"
              "  without the ID.", file=sys.stderr)
    print(f"  make plan ENV={env_name or env_dir.name} shows whether any file is still missing.", file=sys.stderr)
    return 0 if warn else 1


def load_auth(env_dir: Path) -> dict:
    import hcl2

    with open(env_dir / "auth.auto.tfvars") as f:
        return hcl2.load(f)


def get_status(auth: dict, agent_id: str) -> str:
    """GET the agent on the current workspace; return the HTTP status or error."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound, PermissionDenied, Unauthenticated

    w = WorkspaceClient(
        host=auth["databricks_workspace_host"],
        client_id=auth["databricks_client_id"],
        client_secret=auth["databricks_client_secret"],
    )
    try:
        w.api_client.do("GET", f"/api/2.0/genie/spaces/{agent_id}")
        return "200"
    except NotFound:
        return "404"
    except PermissionDenied:
        return "403"
    except Unauthenticated:
        return "401"
    except Exception as exc:  # network, auth setup, unexpected API error
        return type(exc).__name__


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("env_dir")
    parser.add_argument("--arm", action="store_true",
                        help="mark each adoptable key adoption-required")
    parser.add_argument("--quiet", action="store_true",
                        help="print only when the check fails")
    parser.add_argument("--id-files", action="store_true",
                        help="only check that every created agent still has its ID file (no workspace call)")
    parser.add_argument("--warn", action="store_true",
                        help="with --id-files: warn instead of failing")
    parser.add_argument("--env-name", default="", help="with --id-files: env name for terraform console")
    parser.add_argument("--runner", default="",
                        help="with --id-files: terraform_layer.sh, to ask which agents the config keeps")
    args = parser.parse_args(argv)
    if args.id_files:
        return check_id_files(Path(args.env_dir).resolve(), args.warn, args.env_name, args.runner)
    lines: list[str] = []
    out = lines.append if args.quiet else print
    env_dir = Path(args.env_dir).resolve()

    legacy = legacy_agents(env_dir)
    if not legacy:
        if not (args.arm or args.quiet):
            print(f"Genie adoption preflight ({env_dir.name}): no legacy Genie agents; nothing to adopt.")
        return 0

    try:
        auth = load_auth(env_dir)
        host = str(auth.get("databricks_workspace_host", "")).rstrip("/")
    except Exception as exc:
        print(f"ERROR: cannot read {env_dir / 'auth.auto.tfvars'} ({type(exc).__name__}); nothing was applied.")
        return 1

    out(f"=== Genie adoption preflight ({env_dir.name}): {len(legacy)} legacy agent(s) on {host} ===")
    failed = 0
    for key, recorded_host in legacy:
        id_file = id_file_for(env_dir, key)
        agent_id = id_file.read_text().strip() if id_file.is_file() else ""
        if not agent_id:
            status, problem = "-", "ID file missing or empty"
        elif recorded_host and recorded_host != host:
            status, problem = "-", f"created on {recorded_host}, not this host"
        else:
            status = get_status(auth, agent_id)
            problem = "" if status == "200" else f"GET returned {status}"
        ok = not problem
        failed += not ok
        out(f"  {'OK  ' if ok else 'FAIL'}  {key}  id={agent_id or '<none>'}  {id_file.name}  "
              f"GET {status}{'' if ok else '  -> ' + problem}")

    if failed:
        for line in lines:
            print(line)
        print(
            f"ERROR: {failed} legacy Genie agent(s) can't be adopted; nothing was applied.\n"
            f"  Each needs {env_dir}/.genie_space_id_<key> holding the existing agent ID (it is\n"
            f"  in the agent URL), and the agent must answer GET 200 on {host}.\n"
            f"  Fix that, then re-run: make genie-adopt-preflight ENV={env_dir.name}"
        )
        return 1
    if args.arm:
        for key, _ in legacy:
            marker_for(env_dir, key).write_text("adoption required\n")
    out("  All legacy agents will be adopted with their current IDs; none will be created.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
