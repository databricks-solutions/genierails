#!/usr/bin/env python3
"""Captured Genie agents and who owns their config (design section 5).

    capture        make capture ENV=dev SPACE=<agent>: one read-only GET; writes
                   only envs/dev/agents/<agent>.space.json. No governance
                   drafting, no model call, no Terraform state.
    import-new     after make generate ENV=dev: captures the agents that have a
                   settings file but no capture yet. Never re-captures one: an
                   agent whose capture was ever committed is refused (make
                   capture replaces it on purpose). --check, run before
                   generate writes anything, refuses the same way.
    restore-ids    release-plan (and legacy release), before planning: an agent
                   whose ID file is gone gets it back from the last release
                   receipt, only if the receipt's key, lifecycle and pushed
                   config still match the live agent at that ID.
    check-deletes  release-plan, before its lock: delete = true needs a committed
                   <env>:delete-agent:<key> line in envs/<env>/ack.txt.
    plan           release-plan: records, per agent, what release will push
                   (agent ID, capture digest, live-config digest) and prints
                   the field-level overwrite report; the manifest carries it.
    preflight      release MANIFEST=, under its lock and before sync-tags or any
                   saved plan: read-only; refuses if the agent set, a capture, an
                   agent ID, a planned-new agent or a live config moved.
    sync           release: pushes each captured config to its agent as one
                   full replacement and prints what it overwrote. In a
                   deterministic env only what the approved manifest recorded,
                   only under release's lock, and only if neither the capture,
                   the agent ID nor the live config moved since release-plan. A
                   rejected field fails the release. Never in dev.
    receipt        legacy release, after it passed: records the commit and each
                   agent's ID. Deterministic envs record them in step 9's
                   release receipt (release_gate.py release-receipt).
    detach         make detach-agent, under its own env lock: refuses unless
                   HEAD is the last released commit and that release had the
                   agent detached; then terraform state rm for its exact
                   addresses.
    guard-removals before each workspace plan/apply: refuses an agent that is
                   gone from config while the workspace state still holds it.

Applying commands write only ignored runtime files (state, ID files, receipts
in the state dir); only capture and import-new write committed files.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

SHARED = Path(__file__).resolve().parents[1]
if str(SHARED) not in sys.path:
    sys.path.insert(0, str(SHARED))

import agent_files  # noqa: E402
from agent_files import AgentFileError  # noqa: E402

LEGACY_RECEIPT = "agents-release-receipt.json"
DEV_ENV = "dev"
PLAN_FILE = "agents.json"
# Space metadata that is never part of a capture. The GET response's own envelope
# (space_id, etag, title, warehouse_id, ACLs) is dropped by taking only its
# serialized_space; these keys are also removed from the top level of the decoded
# serialized_space itself, should they appear there. Nested item IDs are kept.
OUTER_FIELDS = ("space_id", "etag", "access_control_list", "permissions", "acl")
SPACE_API = "/api/2.0/genie/spaces/{}"

Getter = Callable[[str, dict], dict]


class AgentError(RuntimeError):
    pass


# ── Payload ──────────────────────────────────────────────────────────────────

def payload_from_response(response: dict) -> dict:
    """The serialized_space of a GET response, minus outer fields; inner IDs are kept."""
    serialized = response.get("serialized_space") if isinstance(response, dict) else None
    if isinstance(serialized, str):
        if not serialized.strip():
            raise AgentError("the Genie response has an empty serialized_space (is the agent still being created?)")
        try:
            payload = json.loads(serialized)
        except json.JSONDecodeError as exc:
            raise AgentError(f"serialized_space is not valid JSON: {exc}") from exc
    elif isinstance(serialized, dict):
        payload = serialized
    else:
        raise AgentError("the Genie response has no serialized_space")
    if not isinstance(payload, dict):
        raise AgentError("serialized_space must hold a JSON object")
    return {key: value for key, value in payload.items() if key not in OUTER_FIELDS}


def render(payload: dict) -> str:
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def write_payload(payload: dict, destination: Path) -> bool:
    text = render(payload)
    if destination.is_file() and destination.read_text() == text:
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(text)
    return True


def _ids(items: list) -> list[str] | None:
    keys = [item.get("id") for item in items if isinstance(item, dict)]
    if len(keys) == len(items) and all(isinstance(k, str) for k in keys) and len(set(keys)) == len(keys):
        return keys
    return None


def changed_fields(before: Any, after: Any, prefix: str = "") -> list[str]:
    """Field paths that differ. List items with an "id" are matched by ID."""
    if isinstance(before, dict) and isinstance(after, dict):
        fields = []
        for key in sorted(set(before) | set(after)):
            path = f"{prefix}.{key}" if prefix else key
            if key not in after:
                fields.append(f"{path} (removed)")
            elif key not in before:
                fields.append(f"{path} (added)")
            else:
                fields.extend(changed_fields(before[key], after[key], path))
        return fields
    if isinstance(before, list) and isinstance(after, list):
        old, new = _ids(before), _ids(after)
        if old is not None and new is not None:
            by_old = {item["id"]: item for item in before}
            by_new = {item["id"]: item for item in after}
            fields = []
            for key in sorted(set(by_old) | set(by_new)):
                path = f"{prefix}[id={key}]"
                if key not in by_new:
                    fields.append(f"{path} (removed)")
                elif key not in by_old:
                    fields.append(f"{path} (added)")
                else:
                    fields.extend(changed_fields(by_old[key], by_new[key], path))
            if not fields and old != new:
                fields.append(f"{prefix} (order)")
            return fields
    if before != after:
        return [prefix or "<root>"]
    return []


# ── Workspace access ─────────────────────────────────────────────────────────

def _api(env_dir: Path):
    """The workspace API client from the env's auth.auto.tfvars (or the SDK's defaults)."""
    from databricks.sdk import WorkspaceClient

    auth = agent_files._load_hcl(env_dir / "auth.auto.tfvars")
    kwargs = {}
    for source, target in (("databricks_workspace_host", "host"), ("databricks_client_id", "client_id"),
                           ("databricks_client_secret", "client_secret")):
        if auth.get(source):
            kwargs[target] = auth[source]
    return WorkspaceClient(**kwargs).api_client


def _get_space(api, space_id: str) -> dict:
    return api.do("GET", SPACE_API.format(space_id), query={"include_serialized_space": "true"})


# ── IDs ──────────────────────────────────────────────────────────────────────

def id_file(env_dir: Path, name: str) -> Path:
    """.genie_space_id_<key>, the file the workspace root's create resource reads and writes."""
    return env_dir / f".genie_space_id_{agent_files.key_of(env_dir, name)}"


def display_title(agent: dict) -> str:
    """The title the workspace root creates the agent with (its display name)."""
    return agent["display_name"] or agent["name"]


def _legacy_id(env_dir: Path, name: str) -> str:
    cfg = agent_files._load_hcl(env_dir / "env.auto.tfvars")
    ids = [str(s.get("genie_space_id") or "") for s in cfg.get("genie_spaces") or []
           if isinstance(s, dict) and s.get("name") == name]
    return ids[0] if len(ids) == 1 else ""


def agent_id(env_dir: Path, name: str) -> str:
    """The agent's ID: its settings file, else its ID file (create path), else ''."""
    if agent_files.settings_path(env_dir, name).is_file():
        configured = agent_files.load(env_dir, name)["genie_space_id"]
    else:
        configured = _legacy_id(env_dir, name)
    if configured:
        return configured
    path = id_file(env_dir, name)
    return path.read_text().strip() if path.is_file() else ""


# ── Authoring: capture ───────────────────────────────────────────────────────

def capture(env_dir: Path, env: str, name: str, get: Getter | None = None) -> bool:
    if env != DEV_ENV:
        raise AgentError(f"capture runs only with ENV={DEV_ENV}: agents are curated in dev and reach "
                         f"{env} through promote-to")
    agent_files.check_name(name)
    space_id = agent_id(env_dir, name)
    if not space_id:
        raise AgentError(
            f"agent {name!r} has no genie_space_id. Create {agent_files.settings_path(env_dir, name)} with\n"
            f'  genie_space_id = "<the agent ID from its URL>"\n  acl_groups     = ["<group>"]')
    if get is None:
        api = _api(env_dir)
        get = lambda path, query: api.do("GET", path, query=query)  # noqa: E731
    # Read-only by contract: one GET, no PATCH fallback.
    payload = payload_from_response(get(SPACE_API.format(space_id), {"include_serialized_space": "true"}))
    destination = agent_files.capture_path(env_dir, name)
    changed = write_payload(payload, destination)
    print(f"{'Captured' if changed else 'Unchanged'}: {name} ({space_id}) -> agents/{destination.name}")
    if changed:
        print("  Review and commit it; make promote-to carries it to the next env.")
    return changed


def capture_committed(env_dir: Path, name: str) -> bool:
    """Whether agents/<name>.space.json is in the checkout's git history.

    That is what "new" means for import-new (design section 5: generate only
    imports agents not yet in git). Nothing else can tell: agent_tables in
    generated/governed_tables.json also lists an agent whose tables come from
    uc_tables, and release receipts are prod's. Outside a git checkout nothing
    was committed, so there is nothing to overwrite.
    """
    path = agent_files.capture_path(env_dir, name)
    try:
        listed = subprocess.run(["git", "-C", str(path.parent.parent), "log", "-1", "--format=%H", "--",
                                 str(path.relative_to(path.parent.parent))],
                                capture_output=True, text=True)
    except OSError:
        return False
    return listed.returncode == 0 and bool(listed.stdout.strip())


def import_new(env_dir: Path, env: str, get: Getter | None = None, *, check: bool = False) -> list[str]:
    """Capture every agent with a settings file and an ID that was never captured.

    An agent whose committed capture is gone is refused before anything is
    read or written: a plain generate must not replace a reviewed capture with
    whatever the dev UI holds now. ``check`` refuses the same way and captures
    nothing.
    """
    if env != DEV_ENV:
        return []
    missing = [a["name"] for a in agent_files.load_all(env_dir)
               if a["genie_space_id"] and not agent_files.capture_path(env_dir, a["name"]).is_file()]
    lost = [name for name in missing if capture_committed(env_dir, name)]
    if lost:
        raise AgentError(
            "make generate imports only agents never captured before, and these agents' committed captures "
            "are gone:\n" + "".join(
                f"  {agent_files.AGENTS_DIR}/{agent_files.capture_path(env_dir, name).name}: restore it from git, "
                f'or replace it with the live dev agent: make capture ENV={env} SPACE="{name}"\n'
                for name in lost) + "  Nothing was written.")
    if check:
        return missing
    for name in missing:
        capture(env_dir, env, name, get)
    return missing


# ── Release ──────────────────────────────────────────────────────────────────

def _gate():
    from scripts import release_gate

    return release_gate


def deterministic(env_dir: Path) -> bool:
    cfg = agent_files._load_hcl(Path(env_dir) / "env.auto.tfvars")
    return cfg.get("governance_mode", "legacy") == "deterministic"


def legacy_receipt_path(env_dir: Path, env: str) -> Path:
    gate = _gate()
    return gate.env_state(gate.cloud_dir_of(env_dir), env) / LEGACY_RECEIPT


def load_receipt(env_dir: Path, env: str) -> dict:
    """The last release receipt: step 9's in deterministic envs, else the legacy agent receipt."""
    if deterministic(env_dir):
        gate = _gate()
        return gate.last_release(gate.cloud_dir_of(env_dir), env) or {}
    path = legacy_receipt_path(env_dir, env)
    if not path.is_file():
        return {}
    try:
        receipt = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise AgentError(f"{path}: unreadable release receipt ({exc})") from exc
    return receipt if isinstance(receipt, dict) else {}


def promoted(env_dir: Path) -> bool:
    """A deterministic promote-to target (promote_from set, or prod): IDs never come from git."""
    return agent_files.promoted(env_dir)


def lifecycle(agent: dict) -> str:
    return "deleted" if agent["delete"] else "detached" if agent["detached"] else "active"


def receipt_agents(env_dir: Path, manifest: dict | None = None) -> dict[str, dict]:
    """Each agent as released, with the identity a later recovery must match.

    key and lifecycle come from the agent file; config_sha256 is the capture
    release pushed (a recovered ID must still hold exactly that config);
    binding says how its ID was established (BINDINGS), create_resource_id is
    the Terraform ID of its create resource and created_space_id the agent ID
    that resource recorded in state. For a manifest release, an agent the saved
    plan created or adopted is verified again before the receipt is written.
    """
    planned = ((manifest or {}).get("agents") or {}).get("agents") or {}
    previous = (load_receipt(env_dir, env_dir.name).get("agents") or {})
    result = {}
    for agent in agent_files.load_all(env_dir):
        name = agent["name"]
        key = agent["key"]
        entry = planned.get(name) or {}
        capture = agent_files.capture_path(env_dir, name)
        space_id = agent_id(env_dir, name)
        binding = entry.get("binding") or binding_now(env_dir, agent)[0]
        if manifest is not None and entry.get("action") == "push" and binding in ("create", "adopt"):
            space_id = verify_created(env_dir, name, key, entry["genie_space_id"] or None)
        pushed = entry.get("capture_sha256") if manifest is not None else (
            _sha(capture.read_text()) if capture.is_file() and lifecycle(agent) == "active" else None)
        created = create_instance(env_dir, key)
        record = recorded_id(env_dir, key)
        result[name] = {
            "genie_space_id": space_id,
            "key": key,
            "acl_groups": agent["acl_groups"],
            "detached": agent["detached"],
            "delete": agent["delete"],
            "lifecycle": lifecycle(agent),
            "config_sha256": pushed if entry.get("action", "push") == "push" else None,
            "binding": binding,
            "create_resource_id": (created or {}).get("id"),
            "created_space_id": ((record or {}).get("result") or {}).get("id"),
        }
    current_keys = {entry["key"] for entry in result.values()}
    # Detached agents remain tombstoned in every later receipt even after the
    # removal PR drops their committed capture. This keeps recovery/audit state
    # sticky; an explicit active config with the same identity supersedes it.
    for previous_name, entry in previous.items():
        if not isinstance(entry, dict):
            continue
        key = str(entry.get("key") or agent_files.space_key(previous_name))
        detached = entry.get("detached") is True or entry.get("lifecycle") == "detached"
        if detached and key not in current_keys:
            result[previous_name] = dict(entry, key=key, detached=True, lifecycle="detached")
    return result


def _lock_owner(env_dir: Path) -> str | None:
    from scripts.environment_lock import held_by_ancestor

    return held_by_ancestor(env_dir)


def _state_resources(env_dir: Path) -> list[dict]:
    """The workspace state's resources, read through scripts/state_layout.py (fails closed)."""
    from scripts.state_layout import state_json

    env_dir = Path(env_dir)
    try:
        return (state_json(env_dir, env_dir.name, "workspace") or {}).get("resources") or []
    except RuntimeError as exc:
        raise AgentError(str(exc)) from exc


def _instances(env_dir: Path, mode: str, type_: str, name: str) -> dict[str, dict]:
    """index key -> attributes of the current (non-deposed) module.workspace instances."""
    found = {}
    for resource in _state_resources(env_dir):
        if (resource.get("module"), resource.get("mode", "managed"), resource.get("type"), resource.get("name")) != (
                "module.workspace", mode, type_, name):
            continue
        for instance in resource.get("instances") or []:
            if isinstance(instance.get("index_key"), str) and not instance.get("deposed"):
                found[instance["index_key"]] = instance.get("attributes") or {}
    return found


def create_instance(env_dir: Path, key: str) -> dict | None:
    """The workspace state's terraform_data.genie_space[key] (the create resource), or None."""
    return _instances(env_dir, "managed", "terraform_data", "genie_space").get(key)


def recorded_id(env_dir: Path, key: str) -> dict | None:
    """data.external.genie_space_id[key]: the agent ID the create resource left, recorded in state."""
    return _instances(env_dir, "data", "external", "genie_space_id").get(key)


def _same_path(a: object, b: Path) -> bool:
    return bool(a) and Path(str(a)).resolve() == Path(b).resolve()


# How an agent's ID was established. create: release-plan saw no ID and no create
# resource, so the saved plan creates the agent. adopt: an ID file but no create
# resource, so the saved plan's create resource adopts that ID. existing: the ID
# was known at release-plan (its create resource then too, if it has one).
BINDINGS = ("create", "adopt", "existing")


def binding_now(env_dir: Path, agent: dict) -> tuple[str, str | None]:
    """(binding, create resource ID) as the state and ID files are now."""
    created = create_instance(env_dir, agent["key"])
    space_id = agent_id(env_dir, agent["name"])
    if not space_id:
        return "create", None
    if created is None and not agent["genie_space_id"] and promoted(env_dir):
        return "adopt", None
    return "existing", (created or {}).get("id")


def verify_created(env_dir: Path, name: str, key: str, expected: str | None) -> str:
    """After the saved workspace plan: the agent ID came from this agent's create resource.

    The create resource terraform_data.genie_space[key] must be in state and
    name exactly this agent's ID file, create must not have title-adopted
    another agent, and the agent ID the resource recorded in state
    (data.external.genie_space_id[key]) must equal the ID file's contents
    (and the planned ID, for an adoption). Returns that ID.
    """
    path = id_file(env_dir, name)
    created = create_instance(env_dir, key)
    if created is None:
        elsewhere = [k for k, attrs in _instances(env_dir, "managed", "terraform_data", "genie_space").items()
                     if _same_path((attrs.get("input") or {}).get("id_file"), path)]
        if elsewhere:
            raise AgentError(f"the create resource for {name!r}'s ID file is in state under key "
                             f"{elsewhere[0]!r}, not {key!r}; its config was not pushed")
        raise AgentError(f"the saved workspace plan didn't create {name!r}: no create resource "
                         f"terraform_data.genie_space[{key!r}] in state; its config was not pushed")
    named = (created.get("input") or {}).get("id_file")
    if not _same_path(named, path):
        raise AgentError(f"the create resource for {name!r} names the ID file {named}, not {path}; "
                         "its config was not pushed")
    if adopted_marker(env_dir, name).exists():
        raise AgentError(f"creating {name!r} adopted an existing agent titled "
                         f"{display_title(agent_files.load(env_dir, name))!r} instead of creating one "
                         f"({adopted_marker(env_dir, name).name}); its config was not pushed")
    record = recorded_id(env_dir, key)
    if record is None:
        raise AgentError(f"the create resource for {name!r} recorded no agent ID in state "
                         f"(data.external.genie_space_id[{key!r}]); its config was not pushed")
    if not _same_path((record.get("query") or {}).get("id_file"), path):
        raise AgentError(f"the agent ID recorded in state for {name!r} was read from "
                         f"{(record.get('query') or {}).get('id_file')}, not {path}; its config was not pushed")
    state_id = str((record.get("result") or {}).get("id") or "")
    file_id = path.read_text().strip() if path.is_file() else ""
    if not state_id or state_id != file_id:
        raise AgentError(f"{path.name} holds {file_id or 'nothing'}, but the create resource recorded "
                         f"{state_id or 'nothing'} in state; nothing is pushed to an agent it didn't create")
    if expected is not None and state_id != expected:
        raise AgentError(f"the create resource for {name!r} recorded {state_id}, the manifest planned {expected}")
    return state_id


def adopted_marker(env_dir: Path, name: str) -> Path:
    """genie_space.sh create writes this when it title-adopts an agent instead of creating one."""
    return Path(env_dir) / f".genie_adopted_{agent_files.key_of(env_dir, name)}"


def _title_matches(api, title: str) -> list[str]:
    """IDs of live agents titled exactly ``title`` (all pages)."""
    found, token, pages = [], "", 0
    while True:
        pages += 1
        if pages > 1000:
            raise AgentError("the Genie agent listing exceeded 1000 pages")
        query = {"page_token": token} if token else {}
        try:
            page = api.do("GET", "/api/2.0/genie/spaces", query=query) or {}
        except Exception as exc:
            raise AgentError(f"could not list Genie agents to check for {title!r}: {exc}") from exc
        found += [str(s.get("space_id")) for s in page.get("spaces") or [] if s.get("title") == title]
        token = page.get("next_page_token") or ""
        if not token:
            return found


def restore_ids(env_dir: Path, env: str, api=None) -> list[str]:
    """Rewrite a lost ID file from the last release receipt, only when the identity still matches.

    The receipt entry must have the agent's key, a known binding, an active
    lifecycle and the config release pushed. Its create resource must be the
    one in state now (same Terraform ID, naming exactly this agent's ID file,
    and the agent ID it recorded), or, for an agent released with none, state
    must hold none. The live agent at that ID must hold exactly the pushed
    config. Anything else is refused (recover by writing the ID file yourself),
    except an agent released as detached or deleted, which isn't recovered: one
    with that name now is a new agent.
    """
    released = load_receipt(env_dir, env).get("agents") or {}
    restored = []
    for agent in agent_files.load_all(env_dir):
        name = agent["name"]
        key = agent["key"]
        if agent["genie_space_id"] or agent["delete"]:
            continue
        path = id_file(env_dir, name)
        entry = released.get(name)
        if not entry or (path.is_file() and path.read_text().strip()):
            continue
        recorded = str(entry.get("genie_space_id") or "")
        if not recorded:
            continue
        refusal = f"can't prove that agent {recorded} (from the release receipt) is {name!r}"
        manual = f"If it is, write the ID into {path} yourself; release-plan then shows its live config."
        if entry.get("key") != key:
            raise AgentError(f"the release receipt's entry for {name!r} has key {entry.get('key')!r}, not "
                             f"{key!r}; its ID {recorded} is not recovered")
        released_lifecycle = entry.get("lifecycle") or ("detached" if entry.get("detached") else "active")
        if released_lifecycle != "active":
            print(f"Not recovering {name}'s ID: the last release had it {released_lifecycle}, so an agent "
                  f"with this name now is a new one.")
            continue
        binding, resource_id = entry.get("binding"), entry.get("create_resource_id")
        if binding not in BINDINGS:
            raise AgentError(f"{refusal}: the receipt records no known binding ({binding!r}; written by an earlier "
                             f"version?). {manual}")
        if binding in ("create", "adopt") and not resource_id:
            raise AgentError(f"{refusal}: the receipt says the saved plan {binding}d it but records no create "
                             f"resource. {manual}")
        created = create_instance(env_dir, key)
        if resource_id:
            if created is None:
                raise AgentError(f"{refusal}: the receipt names create resource {resource_id}, but the state "
                                 f"holds no terraform_data.genie_space[{key!r}]. {manual}")
            if created.get("id") != resource_id:
                raise AgentError(f"{refusal}: the receipt names create resource {resource_id}, the state holds "
                                 f"{created.get('id')}. {manual}")
            if not _same_path((created.get("input") or {}).get("id_file"), path):
                raise AgentError(f"{refusal}: its create resource names the ID file "
                                 f"{(created.get('input') or {}).get('id_file')}, not {path}. {manual}")
            state_id = ((recorded_id(env_dir, key) or {}).get("result") or {}).get("id")
            if state_id and state_id != recorded:
                raise AgentError(f"{refusal}: its create resource recorded {state_id} in state. {manual}")
        elif created is not None:
            raise AgentError(f"{refusal}: the receipt records no create resource, but the state holds "
                             f"terraform_data.genie_space[{key!r}] ({created.get('id')}). {manual}")
        if not entry.get("config_sha256"):
            raise AgentError(f"{refusal}: the receipt records no config release pushed to it. {manual}")
        api = api or _api(env_dir)
        live = _live(api, name, recorded)
        if live is None or _sha(render(live)) != entry["config_sha256"]:
            raise AgentError(f"{refusal}: its live config isn't the one release last pushed. {manual}")
        path.write_text(recorded + "\n")
        restored.append(name)
        print(f"Recovered {name}'s agent ID {recorded} from the release receipt ({path.name}); "
              "its create resource and live config match what release recorded.")
    return restored


def _sha(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode()).hexdigest()


def _live(api, name: str, space_id: str) -> dict | None:
    """The agent's live payload; None when it doesn't serialize yet (a just-created agent)."""
    try:
        return payload_from_response(_get_space(api, space_id))
    except AgentError:
        return None
    except Exception as exc:
        raise AgentError(f"could not read agent {name!r} ({space_id}): {exc}") from exc


def _push(api, name: str, space_id: str, desired: dict, current: dict | None) -> list[str]:
    """One full-replacement PATCH; no retry that drops a field the API rejects."""
    report = changed_fields(current or {}, desired)
    try:
        response = api.do("PATCH", SPACE_API.format(space_id),
                          body={"serialized_space": json.dumps(desired, separators=(",", ":"))})
    except Exception as exc:
        raise AgentError(f"Genie rejected the captured config for {name!r} ({space_id}); nothing was "
                         f"skipped, so the release fails: {exc}") from exc
    returned = str((response or {}).get("space_id") or space_id)
    if returned != space_id:
        raise AgentError(f"Genie answered for agent {returned}, not {space_id}, while updating {name!r}")
    print(f"Genie agent {name} ({space_id}): config replaced from agents/{name}.space.json")
    for field in report:
        print(f"  overwrote {field}")
    if not report:
        print("  no field changed")
    return report


# ── delete = true needs a committed acknowledgement ──────────────────────────

def delete_ack_key(env: str, key: str) -> str:
    """The ack line for deleting the agent with Terraform key ``key``."""
    return f"{env}:delete-agent:{key}"


def _ack_keys(env_dir: Path, env: str) -> set[str]:
    from ack_file import AckFileError, parse_file

    path = Path(env_dir) / "ack.txt"
    if not path.is_file():
        return set()
    try:
        return {entry.key for entry in parse_file(path, env)}
    except AckFileError as exc:
        raise AgentError(f"{path}: {exc}") from exc


def delete_consequences(env_dir: Path, env: str) -> dict[str, str]:
    """ack key -> consequence, for every agent with delete = true (release_gate reconciles them)."""
    return {delete_ack_key(env, a["key"]): f"deletes Genie agent {a['name']} and its conversations"
            for a in agent_files.load_all(env_dir) if a["delete"]}


def check_delete_acks(env_dir: Path, env: str) -> list[str]:
    """Refuse delete = true without its <env>:delete-agent:<key> ack, and a delete-agent ack nothing needs."""
    needed = delete_consequences(env_dir, env)
    acked = {k for k in _ack_keys(env_dir, env) if k.startswith(f"{env}:delete-agent:")}
    missing = sorted(set(needed) - acked)
    if missing:
        raise AgentError("delete = true deletes the agent and its conversations, so it needs a reviewed "
                         f"acknowledgement: add {', '.join(missing)} to envs/{env}/ack.txt in a PR")
    stale = sorted(acked - set(needed))
    if stale:
        raise AgentError(f"envs/{env}/ack.txt acknowledges deleting {', '.join(stale)}, but no agent there has "
                         "delete = true: remove the stale line in a PR")
    return sorted(needed)


def reactivation_consequences(env_dir: Path, env: str) -> dict[str, str]:
    previous = (load_receipt(env_dir, env).get("agents") or {})
    detached = {str(entry.get("key") or agent_files.space_key(name))
                for name, entry in previous.items() if isinstance(entry, dict)
                and (entry.get("detached") is True or entry.get("lifecycle") == "detached")}
    active = {agent["key"]: agent["name"]
              for agent in agent_files.load_all(env_dir)
              if not agent["detached"] and not agent["delete"]}
    return {f"{env}:revive-agent:{key}": f"reactivates detached Genie agent {active[key]}"
            for key in sorted(detached & set(active))}


def check_detach_acks(env_dir: Path, env: str) -> list[str]:
    keys = _ack_keys(env_dir, env)
    configured = {agent["key"]: agent for agent in agent_files.load_all(env_dir)}
    invalid = sorted(key for key in keys if key.startswith(f"{env}:detached-agent:")
                     and (key.rsplit(":", 1)[-1] not in configured
                          or not configured[key.rsplit(":", 1)[-1]]["detached"]
                          or configured[key.rsplit(":", 1)[-1]]["acl_groups"]))
    if invalid:
        raise AgentError(f"the agent is still retired by {', '.join(invalid)}; its settings must remain "
                         "detached with acl_groups = [], or remove the ack.txt line in a "
                         "governance-reviewed PR to revive it")
    needed = reactivation_consequences(env_dir, env)
    if not needed:
        return []
    needed_keys = {key.rsplit(":", 1)[-1] for key in needed}
    persistent = sorted(key for key in keys if key.startswith(f"{env}:detached-agent:")
                        and key.rsplit(":", 1)[-1] in needed_keys)
    if persistent:
        raise AgentError(f"the agent is still retired by {', '.join(persistent)}; remove that line from "
                         f"envs/{env}/ack.txt in a governance-reviewed PR to revive it")
    missing = sorted(set(needed) - keys)
    if missing:
        raise AgentError("the last release receipt records this agent as detached; reactivation needs a "
                         f"governance acknowledgement: add {', '.join(missing)} to envs/{env}/ack.txt")
    return sorted(needed)


def sync(env_dir: Path, env: str, api=None) -> dict[str, list[str]]:
    """Legacy envs: push every captured config to its agent (full replacement, one PATCH each)."""
    if env == DEV_ENV:
        print("Genie agent config: dev's is never overwritten (curate it in the UI, then make capture).")
        return {}
    if deterministic(env_dir):
        raise AgentError("a deterministic env pushes agent config only from an approved manifest: "
                         f"make release-plan ENV={env}, then make release ENV={env} MANIFEST=<it>")
    reports: dict[str, list[str]] = {}
    for agent in agent_files.load_all(env_dir):
        name = agent["name"]
        if not agent_files.capture_path(env_dir, name).is_file():
            continue
        if agent["delete"] or agent["detached"]:
            print(f"Genie agent {name}: {'deleted' if agent['delete'] else 'detached'}; config not pushed.")
            continue
        space_id = agent_id(env_dir, name)
        if not space_id:
            raise AgentError(f"agent {name!r} has no ID after apply (no genie_space_id and no "
                             f"{id_file(env_dir, name).name}); its config was not pushed")
        desired = json.loads(agent_files.capture_path(env_dir, name).read_text())
        api = api or _api(env_dir)
        reports[name] = _push(api, name, space_id, desired, _live(api, name, space_id))
    return reports


def _planned(agent: dict, env_dir: Path, env: str) -> dict:
    """What release will do to one agent, from committed files and local runtime files only."""
    name = agent["name"]
    key = agent["key"]
    space_id = agent_id(env_dir, name)
    if agent["delete"]:
        # A promoted env's agents are create-path agents: the saved workspace plan trashes them.
        return {"action": "delete", "key": key, "genie_space_id": space_id, "ack": delete_ack_key(env, key),
                "via": "saved workspace plan" if promoted(env_dir) or not agent["genie_space_id"] else "api"}
    capture = agent_files.capture_path(env_dir, name)
    if agent["detached"] or not capture.is_file():
        return {"action": "none", "key": key, "genie_space_id": space_id,
                "reason": "detached" if agent["detached"] else "not captured"}
    binding, resource_id = binding_now(env_dir, agent)
    return {"action": "push", "key": key, "genie_space_id": space_id, "capture_sha256": _sha(capture.read_text()),
            "binding": binding, "create_resource_id": resource_id}


def plan(env_dir: Path, env: str, plan_dir: Path, api=None) -> dict:
    """release-plan: what release will do to each agent, with the overwrite report; writes agents.json."""
    check_delete_acks(env_dir, env)
    check_detach_acks(env_dir, env)
    entries: dict[str, dict] = {}
    for agent in ([] if env == DEV_ENV else agent_files.load_all(env_dir)):
        entry = _planned(agent, env_dir, env)
        name = agent["name"]
        if entry["action"] == "push":
            entry |= {"live_sha256": None, "report": ["<new agent: every captured field>"]}
            api = api or _api(env_dir)
            if entry["binding"] != "create":
                live = _live(api, name, entry["genie_space_id"])
                if live is not None:
                    entry["live_sha256"] = _sha(render(live))
                    entry["report"] = changed_fields(live, json.loads(agent_files.capture_path(env_dir, name)
                                                                      .read_text()))
            else:
                _require_new(env_dir, name, entry["key"], api)
        entries[name] = entry
    result = {"agents": entries}
    (Path(plan_dir) / PLAN_FILE).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    if entries:
        print(f"=== Genie agents ({env}): what release will overwrite ===")
    for name, entry in sorted(entries.items()):
        if entry["action"] == "push":
            target = entry["genie_space_id"] or "a new agent, created by the saved workspace plan"
            print(f"  {name} ({target}): full replacement from agents/{name}.space.json")
            for field in entry["report"]:
                print(f"    overwrites {field}")
            if not entry["report"]:
                print("    no field changes")
        elif entry["action"] == "delete":
            print(f"  {name} ({entry['genie_space_id'] or 'no ID'}): DELETED with its conversations "
                  f"({entry['ack']}; {entry['via']})")
        else:
            print(f"  {name}: not pushed ({entry.get('reason', 'nothing to do')})")
    return result


def _require_new(env_dir: Path, name: str, key: str, api) -> None:
    """A planned-new agent must still be new: no ID file, no create resource, no live agent with its title."""
    if id_file(env_dir, name).is_file():
        raise AgentError(f"{id_file(env_dir, name).name} appeared after release-plan planned {name!r} as a new "
                         "agent; nothing is ever pushed to an agent the manifest didn't name")
    if create_instance(env_dir, key) is not None:
        raise AgentError(f"the workspace state already holds the create resource for {name!r}, which "
                         "release-plan planned as new")
    created_title = display_title(agent_files.load(env_dir, name))
    if adopted_marker(env_dir, name).exists():
        raise AgentError(f"{adopted_marker(env_dir, name).name} exists: an earlier create adopted an agent "
                         f"titled {created_title!r}. Check it, then delete that marker and re-run release-plan")
    matches = _title_matches(api, created_title)
    if matches:
        raise AgentError(f"a live agent titled {created_title!r} already exists ({', '.join(matches)}), so creating "
                         f"{name!r} would adopt it by title. If it is this agent, write its ID into "
                         f"{id_file(env_dir, name)} and re-run release-plan (it then shows what release "
                         "overwrites); otherwise rename one of them")


def _manifest_entries(env_dir: Path, env: str, manifest_path: Path, *, applied: bool) -> dict[str, dict]:
    if env == DEV_ENV:
        raise AgentError("dev's agent config is never overwritten")
    owner = _lock_owner(env_dir)
    if owner != "release":
        raise AgentError("agent config in a deterministic env is pushed only by make release MANIFEST=<it>, "
                         f"under its env lock{f' (this runs under make {owner})' if owner else ''}")
    manifest = _gate().load_manifest(Path(manifest_path), env)
    configured = agent_files.load_all(env_dir)
    if "agents" not in manifest:
        captured = [a["name"] for a in configured if agent_files.capture_path(env_dir, a["name"]).is_file()]
        if captured:
            raise AgentError(f"{manifest_path} records no agent plan, but {', '.join(captured)} "
                             f"{'is' if len(captured) == 1 else 'are'} captured; run make release-plan ENV={env} again")
        return {}
    entries = (manifest.get("agents") or {}).get("agents") or {}
    drift = sorted(set(entries) ^ {a["name"] for a in configured})
    if drift:
        raise AgentError(f"the agents changed since release-plan ({', '.join(drift)}); "
                         f"run make release-plan ENV={env} again")
    # A planned create or adoption changes binding once the saved plan ran: judged in _check_entry.
    fields = ("action", "key", "capture_sha256", "ack", "reason") + (() if applied else ("binding", "create_resource_id"))
    for agent in configured:
        entry = entries[agent["name"]]
        now = _planned(agent, env_dir, env)
        for field in fields:
            if field in ("binding", "create_resource_id") and entry.get("binding") == "create" \
                    and now.get("binding") != "create":
                continue  # an ID file or create resource that appeared: _require_new says which
            if now.get(field) != entry.get(field):
                raise AgentError(f"agent {agent['name']!r} changed since release-plan ({field}: "
                                 f"{entry.get(field)!r} -> {now.get(field)!r}); run make release-plan ENV={env} again")
    if set(check_delete_acks(env_dir, env)) != {e["ack"] for e in entries.values() if e["action"] == "delete"}:
        raise AgentError(f"the agent deletions changed since release-plan; run make release-plan ENV={env} again")
    check_detach_acks(env_dir, env)
    return entries


def _check_entry(env_dir: Path, env: str, name: str, entry: dict, api, *, applied: bool):
    """Raise unless the agent still is what the manifest planned; returns (space_id, live)."""
    stale = f"run make release-plan ENV={env} again"
    binding = entry.get("binding")
    if binding not in BINDINGS:
        raise AgentError(f"the manifest records no known binding for {name!r} ({binding!r}); {stale}")
    if binding == "create":
        if not applied:
            _require_new(env_dir, name, entry["key"], api)
            return "", None
        space_id = verify_created(env_dir, name, entry["key"], None)
        return space_id, _live(api, name, space_id)
    space_id = agent_id(env_dir, name)
    if space_id != entry["genie_space_id"]:
        raise AgentError(f"agent {name!r} is now {space_id or 'unknown'}, the manifest planned "
                         f"{entry['genie_space_id']}; nothing is ever pushed to another agent")
    if binding == "adopt" and applied:
        verify_created(env_dir, name, entry["key"], entry["genie_space_id"])
    elif binding == "existing" and entry.get("create_resource_id"):
        created = create_instance(env_dir, entry["key"])
        if (created or {}).get("id") != entry["create_resource_id"]:
            raise AgentError(f"{name!r}'s create resource is now {(created or {}).get('id')}, the manifest planned "
                             f"{entry['create_resource_id']}; {stale}")
        if not _same_path((created.get("input") or {}).get("id_file"), id_file(env_dir, name)):
            raise AgentError(f"{name!r}'s create resource names the ID file "
                             f"{(created.get('input') or {}).get('id_file')}, not {id_file(env_dir, name)}")
        state_id = ((recorded_id(env_dir, entry["key"]) or {}).get("result") or {}).get("id")
        if state_id and state_id != space_id:
            raise AgentError(f"{id_file(env_dir, name).name} holds {space_id}, but {name!r}'s create resource "
                             f"recorded {state_id} in state; nothing is ever pushed to another agent")
    live = _live(api, name, space_id)
    if entry.get("live_sha256") is not None and (live is None or _sha(render(live)) != entry["live_sha256"]):
        raise AgentError(f"agent {name!r} ({space_id}) was changed in the workspace since release-plan, so the "
                         f"approved overwrite report is stale; {stale}")
    return space_id, live


def preflight(env_dir: Path, env: str, manifest_path: Path, api=None) -> list[str]:
    """release MANIFEST=, before sync-tags or any saved plan: read-only; raise if any agent moved."""
    entries = _manifest_entries(env_dir, env, manifest_path, applied=False)
    checked = []
    for name, entry in sorted(entries.items()):
        if entry["action"] != "push":
            continue
        api = api or _api(env_dir)
        _check_entry(env_dir, env, name, entry, api, applied=False)
        checked.append(name)
    if entries:
        print(f"Genie agents ({env}): {len(entries)} agent(s) match the manifest "
              f"({len(checked)} to push); nothing applied yet.")
    return checked


def sync_manifest(env_dir: Path, env: str, manifest_path: Path, api=None) -> dict[str, list[str]]:
    """release MANIFEST=, after the saved workspace plan: re-check, then push exactly what was planned."""
    entries = _manifest_entries(env_dir, env, manifest_path, applied=True)
    pushes = []
    for name, entry in sorted(entries.items()):
        if entry["action"] != "push":
            continue
        api = api or _api(env_dir)
        pushes.append((name, entry, *_check_entry(env_dir, env, name, entry, api, applied=True)))
    reports: dict[str, list[str]] = {}
    for name, entry, space_id, live in pushes:
        # Immediately before each PATCH, the live config is checked once more.
        space_id, live = _check_entry(env_dir, env, name, entry, api, applied=True)
        desired = json.loads(agent_files.capture_path(env_dir, name).read_text())
        reports[name] = _push(api, name, space_id, desired, live)
    for name, entry in sorted(entries.items()):
        if entry["action"] == "delete" and entry.get("via") == "api":
            api = api or _api(env_dir)
            api.do("DELETE", SPACE_API.format(entry["genie_space_id"]))
            print(f"Genie agent {name} ({entry['genie_space_id']}): deleted ({entry['ack']}).")
    return reports


def delete_agents(env_dir: Path, env: str, api=None) -> list[str]:
    """Legacy envs: delete = true on an agent configured by ID deletes it, once acknowledged."""
    check_delete_acks(env_dir, env)
    deleted = []
    for agent in agent_files.load_all(env_dir):
        if not agent["delete"] or not agent["genie_space_id"]:
            continue
        api = api or _api(env_dir)
        api.do("DELETE", SPACE_API.format(agent["genie_space_id"]))
        deleted.append(agent["name"])
        print(f"Genie agent {agent['name']} ({agent['genie_space_id']}): deleted ({delete_ack_key(env, agent['key'])}).")
    return deleted


def _git(env_dir: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(env_dir), *args], check=True, text=True,
                          capture_output=True).stdout.strip()


def head_commit(env_dir: Path) -> str:
    try:
        return _git(env_dir, "rev-parse", "HEAD")
    except (OSError, subprocess.CalledProcessError) as exc:
        raise AgentError(f"{env_dir} is not in a git checkout ({exc})") from exc


def tree_clean(env_dir: Path) -> bool:
    try:
        return _git(env_dir, "status", "--porcelain", "--untracked-files=no") == ""
    except (OSError, subprocess.CalledProcessError):
        return False


def write_receipt(env_dir: Path, env: str, commit: str | None = None) -> dict:
    """Legacy release: record the release in the state dir, outside the checkout.

    Outside a git checkout the commit is empty, so detach-agent refuses.
    """
    if commit is None:
        try:
            commit = head_commit(env_dir)
        except AgentError:
            commit = ""
            print(f"NOTE: {env_dir} is not in a git checkout; the release receipt records no commit.")
    receipt = {
        "env": env,
        "commit": commit,
        "clean_tree": tree_clean(env_dir),
        "released_at": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "agents": receipt_agents(env_dir),
    }
    path = legacy_receipt_path(env_dir, env)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(f"Release receipt: {path} (commit {receipt['commit'][:12] or 'none'}, "
          f"{len(receipt['agents'])} agent(s))")
    return receipt


# ── Operations: detach ───────────────────────────────────────────────────────

def _run(runner: Path, env_dir: Path, env: str, *args: str) -> str:
    process_env = dict(os.environ, LAYER_ENV_DIR=str(env_dir))
    return subprocess.run([str(runner), "workspace", env, *args], env=process_env, check=True,
                          text=True, capture_output=True).stdout


def agent_addresses(state_lines: list[str], agent: dict) -> list[str]:
    """The agent's exact module.workspace addresses in a state listing."""
    wanted = {f'module.workspace.{kind}.{resource}["{key}"]'
              for kind in ("null_resource", "terraform_data")
              for resource in agent_files.STATE_RESOURCES
              for key in agent_files.agent_keys(agent)}
    return [line.strip() for line in state_lines if line.strip() in wanted]


def detach(env_dir: Path, env: str, name: str, runner: Path, *, commit: str | None = None,
           clean: bool | None = None) -> list[str]:
    path = agent_files.settings_path(env_dir, name)
    if not path.is_file():
        raise AgentError(f"{path} not found: detach-agent works on an agent file")
    agent = agent_files.load(env_dir, name)
    if not agent["detached"]:
        raise AgentError(
            f"{name} is not detached. Step 1 is a PR that sets acl_groups = [] and detached = true in "
            f"agents/{name}.auto.tfvars, released normally (it revokes CAN_RUN); then run detach-agent.")
    owner = _lock_owner(env_dir)
    if owner != "detach-agent":
        raise AgentError("detach runs only as make detach-agent, under its env lock"
                         + (f" (this runs under make {owner})" if owner else ""))
    receipt = load_receipt(env_dir, env)
    if not receipt:
        raise AgentError(f"{env} has no release receipt: release the detached = true change first")
    if not receipt.get("commit"):
        raise AgentError("the last release recorded no commit (it ran outside a git checkout): release the "
                         "detached = true change from a checkout of main first")
    head = commit or head_commit(env_dir)
    if receipt.get("commit") != head:
        raise AgentError(f"HEAD {head[:12]} is not the last released commit "
                         f"{str(receipt.get('commit'))[:12]}: check out the released commit and re-run")
    if not (tree_clean(env_dir) if clean is None else clean):
        raise AgentError("the checkout has uncommitted changes: run detach-agent from a clean checkout of "
                         "the released commit")
    released = (receipt.get("agents") or {}).get(name) or {}
    if released.get("detached") is not True or released.get("acl_groups") != []:
        raise AgentError(f"the last release ({head[:12]}) did not have {name} detached with acl_groups = []; "
                         "release that change first")
    addresses = agent_addresses(_run(runner, env_dir, env, "state-list").splitlines(), agent)
    for address in addresses:
        _run(runner, env_dir, env, "state-rm", address)
        print(f"  forgot {address}")
    print(f"Detached {name}: {len(addresses)} state address(es) removed; the agent and its conversations stay.")
    key = agent["key"]
    print(f"  Next: a governance-reviewed PR that adds {env}:detached-agent:{key} to ack.txt, "
          f"removes only agents/{name}.space.json, and keeps detached agents/{name}.auto.tfvars — "
          f"then runs make promote-to ENV={env} and commits governed_tables.json; its plan must show no "
          "destroy. To revive later, remove that ack.txt line in a governance PR.")
    return addresses


# ── Guard ────────────────────────────────────────────────────────────────────

def configured_keys(env_dir: Path) -> set[str]:
    """Every key the workspace root may use for a configured (or deleting) agent."""
    cfg = agent_files._load_hcl(env_dir / "env.auto.tfvars")
    keys: set[str] = set()
    inline = [space for space in cfg.get("genie_spaces") or [] if isinstance(space, dict)]
    keys |= set(agent_files.terraform_keys(inline))
    for space in inline:
        keys |= agent_files.agent_keys({"name": str(space.get("name") or ""),
                                        "genie_space_id": str(space.get("genie_space_id") or "")})
    title = str(cfg.get("genie_space_title") or "")
    if title or cfg.get("genie_space_id"):
        keys.add(agent_files.space_key(title or "Genie Space"))
    for agent in agent_files.load_all(env_dir):
        keys |= agent_files.agent_keys(agent)
    return keys


def guard_removals(env_dir: Path) -> None:
    cfg = agent_files._load_hcl(env_dir / "env.auto.tfvars")
    if cfg.get("governance_mode", "legacy") != "deterministic" and not agent_files.names(env_dir):
        return  # legacy envs keep their remove-means-destroy behaviour
    held = agent_files.state_keys(env_dir)
    configured = configured_keys(env_dir)
    gone = sorted(key for key in held
                  if key not in configured and key.split("--", 1)[0] not in configured)
    if gone:
        raise AgentError(
            "agent(s) removed from config while the workspace state still holds them: " + ", ".join(gone) + ".\n"
            "  Put the agent file back, then detach it in two steps: a PR with acl_groups = [] and\n"
            "  detached = true, released; then make detach-agent ENV=<env> SPACE=<agent>.\n"
            "  To delete the agent instead, set delete = true.")


# ── CLI ──────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("capture", "import-new", "restore-ids", "check-deletes", "check-detach", "plan",
                                            "preflight", "sync", "receipt", "detach", "guard-removals"))
    parser.add_argument("--env-dir", type=Path, required=True)
    parser.add_argument("--env", required=True)
    parser.add_argument("--space", default="")
    parser.add_argument("--runner", type=Path)
    parser.add_argument("--plan-dir", type=Path, help="plan: release-plan's plan dir")
    parser.add_argument("--manifest", type=Path, help="sync: the approved release manifest")
    parser.add_argument("--check", action="store_true", help="import-new: refuse as it would, capture nothing")
    args = parser.parse_args(argv)
    env_dir = args.env_dir
    try:
        if args.command == "capture":
            if not args.space:
                raise AgentError('SPACE="<agent>" is required')
            capture(env_dir, args.env, args.space)
        elif args.command == "import-new":
            import_new(env_dir, args.env, check=args.check)
        elif args.command == "restore-ids":
            restore_ids(env_dir, args.env)
        elif args.command == "check-deletes":
            for key in check_delete_acks(env_dir, args.env):
                print(f"Acknowledged agent deletion: {key}")
        elif args.command == "check-detach":
            for key in check_detach_acks(env_dir, args.env):
                print(f"Acknowledged agent reactivation: {key}")
        elif args.command == "preflight":
            if not args.manifest:
                raise AgentError("preflight needs --manifest")
            preflight(env_dir, args.env, args.manifest)
        elif args.command == "plan":
            if not args.plan_dir:
                raise AgentError("plan needs --plan-dir")
            plan(env_dir, args.env, args.plan_dir)
        elif args.command == "sync":
            if args.manifest:
                sync_manifest(env_dir, args.env, args.manifest)
            else:
                sync(env_dir, args.env)
                delete_agents(env_dir, args.env)
        elif args.command == "receipt":
            write_receipt(env_dir, args.env)
        elif args.command == "detach":
            if not args.space or not args.runner:
                raise AgentError('detach needs SPACE="<agent>" and --runner')
            detach(env_dir, args.env, args.space, args.runner)
        else:
            guard_removals(env_dir)
    except (AgentError, AgentFileError, OSError, subprocess.CalledProcessError) as exc:
        detail = (getattr(exc, "stderr", "") or "").strip()
        print(f"ERROR: {args.command}: {exc}" + (f"\n{detail}" if detail else ""), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
