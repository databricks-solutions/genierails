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
    args = parser.parse_args(argv)
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
