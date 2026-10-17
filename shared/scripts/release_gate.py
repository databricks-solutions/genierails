#!/usr/bin/env python3
"""Release gating for deterministic envs (design sections 10 and 17, rollout step 9).

Receipts, manifests and release receipts are ignored runtime files, kept
outside git under the **state dir**: ``$GENIERAILS_STATE_DIR/<cloud>/<env>/``
when GENIERAILS_STATE_DIR is set (the deployment machine), else
``<cloud>/.genierails-state/<env>/`` in the checkout (gitignored). Legacy envs
never reach this script.

rehearse-start / rehearse-finish
    ``make rehearse ENV=dev`` writes an append-only receipt per commit:
    commit, clean-tree flag, the fingerprint of code and dev inputs, result,
    and the digest of what ``promote-to`` produces for each env promoted from
    dev. When the fingerprint matches an earlier passing clean receipt,
    rehearse writes a receipt for the new commit without re-applying (the
    promote-to digests are still recomputed).

provenance
    ``make release-plan``: HEAD has a passing clean rehearse receipt, the tree
    is clean, HEAD is on freshly fetched origin/main, the fingerprint and the
    committed envs/<env>/generated digest match the receipt, and HEAD isn't
    older than the last release unless ``<env>:rollback:<commit>`` names it in
    envs/<env>/ack.txt on origin/main (a commit can't name itself, so the
    reviewed rollback acknowledgement is merged to main after it).

reports
    The reader inventory (scripts/reader_inventory.py) for tables governance
    newly covers, and refuse-weakening (scripts/weakening.py) of the deployed
    protection against the effective config; every ack.txt entry is
    reconciled once across them, group_map and the rollback.

manifest-write / manifest-check
    One immutable manifest per release-plan: commit, state serial per root,
    plan file hashes, the live-snapshot and effective-config digests, the
    kept-name mapping, the reports, and the Genie agent plan (what release
    pushes to each agent, with its overwrite report; scripts/agent_ownership.py). ``make release MANIFEST=<path>`` refuses
    and asks for a fresh release-plan if any of them changed.

release-receipt / maintain-check
    A finished release writes a receipt (commit, digests, manifest, and each
    Genie agent's ID and detach state, which detach-agent and ID recovery read);
    ``make maintain`` refuses unless HEAD is that commit with matching digests.

apply-guard / destroy-guard
    No side door around those gates. In a deterministic env, the targets that
    apply the data_access or workspace root (apply, apply-governance,
    apply-genie, _apply-layer) run only inside a gated command: the env lock
    must be held by a process this one runs under, by an owner allowed for the
    env's role. A **promoted** env (promote_from set, or prod) changes only
    through ``release MANIFEST=`` (which applies saved plans, never these
    targets) and ``maintain`` (apply-governance, from the released commit). The
    **rehearse** env (dev) applies only through ``rehearse`` (and ``ungovern``'s
    governance apply). The lock can't be faked with a variable: only those
    commands take it. Destroying a promoted env needs CONFIRM_DESTROY=<env>.

Exit codes: 0 ok, 1 refused (the message says why and what to run), 2 error.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping

SHARED = Path(__file__).resolve().parents[1]
for _path in (SHARED, SHARED / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from state_layout import root_state_paths, state_json  # noqa: E402

STATE_DIR_ENV = "GENIERAILS_STATE_DIR"
DEFAULT_STATE = ".genierails-state"
VERSION = 1
# Files under envs/<env> that applying commands write: never inputs.
RUNTIME_FILES = {
    "auth.auto.tfvars", "abac.auto.tfvars", "classification.auto.tfvars",
    "discovered_uc_tables.auto.tfvars", "ddl/_fetched.sql",
    "generated/genie_space_derived_acl_groups.auto.tfvars",
}
RUNTIME_DIRS = {"data_access"}
# AI-helper drafts (scripts/ai_helpers.py) are committed for review but are not
# inputs: nothing that applies reads them, so they never change a fingerprint.
DRAFTS_DIR = "drafts"
STATE_SUFFIXES = (".tfstate", ".tfstate.backup", ".tfplan")
# What promote-to writes into the destination's generated/ (design section 9:
# it writes only the files it owns).
PROMOTE_OWNED = ("abac.auto.tfvars", "masking_functions.sql", "expected_classification.json")
CODE_ROOTS = ("shared",)
CODE_SKIP = ("shared/docs/", "shared/tests/")
ROOTS = ("account", "data_access", "workspace")


class Refused(RuntimeError):
    """A gate refused; the message names the fix."""


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(Path(path).read_bytes())


def _json_digest(value) -> str:
    return sha256_bytes(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def _load_hcl(path: Path) -> dict:
    import hcl2

    if not Path(path).is_file():
        return {}
    return hcl2.loads(Path(path).read_text()) or {}


def deterministic(env_dir: Path) -> bool:
    return _load_hcl(Path(env_dir) / "env.auto.tfvars").get("governance_mode", "legacy") == "deterministic"


# -- where things live --------------------------------------------------------

def cloud_dir_of(env_dir: Path) -> Path:
    return Path(env_dir).resolve().parent.parent


def state_root(cloud_dir: Path) -> Path:
    from state_layout import checked_state_dir

    base = checked_state_dir()  # a missing GENIERAILS_STATE_DIR refuses, never an empty store
    cloud_dir = Path(cloud_dir).resolve()
    return base / cloud_dir.name if base else cloud_dir / DEFAULT_STATE


def env_state(cloud_dir: Path, env: str) -> Path:
    return state_root(cloud_dir) / env


def store_id(cloud_dir: Path, env: str) -> str:
    """This receipt store's random id (created once); every receipt it writes records it.

    Only receipts carrying it count, so a receipt copied in from elsewhere
    (a CI artifact, another machine's store) never satisfies release-plan or
    lets rehearse skip. Like the env lock nonce this stops mistakes and
    unauthenticated artifacts, not someone who can write the store itself.
    TODO(rollout step 12, prod in CI): accept a CI receipt only with a verified
    origin (GitHub artifact attestation via gh attestation verify
    --signer-workflow .../dev-rehearse.yml, or the ADO producing run checked
    through the pipeline resource) binding run id, workflow and commit.
    """
    path = env_state(cloud_dir, env) / ".store-id"
    try:
        value = path.read_text().strip()
    except FileNotFoundError:
        value = ""
    if value:
        return value
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return path.read_text().strip()
    value = secrets.token_hex(16)
    with os.fdopen(fd, "w") as handle:
        handle.write(value + "\n")
    return value


def _write_once(path: Path, payload: Mapping) -> Path:
    """Write a receipt or manifest exactly once (append-only: never replaced)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.chmod(tmp, 0o444)
    try:
        os.link(tmp, path)  # fails if it exists: nothing is ever overwritten
    except FileExistsError as exc:
        raise Refused(f"{path} already exists; receipts and manifests are never overwritten") from exc
    finally:
        tmp.unlink(missing_ok=True)
    return path


def _read_json(path: Path) -> dict | None:
    try:
        value = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


# -- git ----------------------------------------------------------------------

def git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(["git", "-C", str(repo), *args], text=True, capture_output=True, check=False)
    if check and result.returncode:
        raise Refused(f"git {' '.join(args)} failed: {(result.stderr or result.stdout).strip()}")
    return result


def repo_root(path: Path) -> Path:
    result = git(Path(path), "rev-parse", "--show-toplevel", check=False)
    if result.returncode:
        raise Refused(f"{path} is not in a git checkout; releases need the committed envs/ layout")
    return Path(result.stdout.strip()).resolve()


def head(repo: Path) -> str:
    return git(repo, "rev-parse", "HEAD").stdout.strip()


def _rel(repo: Path, path: Path) -> str:
    return os.path.relpath(Path(path).resolve(), repo)


def dirty_paths(repo: Path, paths: Iterable[Path]) -> list[str]:
    """Changed or untracked (not ignored) files under ``paths``."""
    rels = [_rel(repo, p) for p in paths if Path(p).exists()]
    if not rels:
        return []
    out = git(repo, "status", "--porcelain", "--untracked-files=all", "--", *rels).stdout
    return [line for line in out.splitlines() if line.strip()]


def tracked(repo: Path, path: Path) -> bool:
    return git(repo, "ls-files", "--error-unmatch", "--", _rel(repo, path), check=False).returncode == 0


def is_ancestor(repo: Path, older: str, newer: str) -> bool:
    return git(repo, "merge-base", "--is-ancestor", older, newer, check=False).returncode == 0


def commit_known(repo: Path, commit: str) -> bool:
    return git(repo, "cat-file", "-e", f"{commit}^{{commit}}", check=False).returncode == 0


# -- fingerprints and digests ---------------------------------------------------

def _files_digest(base: Path, files: Iterable[Path]) -> str:
    entries = sorted((os.path.relpath(f, base), sha256_file(f)) for f in files)
    return _json_digest(entries)


def code_files(repo: Path, cloud_dir: Path) -> list[Path]:
    """GenieRails code a run depends on: tracked shared/ files and the cloud Makefile."""
    cloud_rel = _rel(repo, cloud_dir)
    out = git(repo, "ls-files", "-z", "--", *CODE_ROOTS, f"{cloud_rel}/Makefile").stdout
    files = []
    for rel in sorted(filter(None, out.split("\0"))):
        if rel.startswith(CODE_SKIP) or rel.endswith(".md"):
            continue
        path = repo / rel
        if path.is_file() and not path.is_symlink():
            files.append(path)
    return files


def _is_input(rel: Path) -> bool:
    parts = rel.parts
    if not parts or parts[0] in RUNTIME_DIRS or parts[0] == DRAFTS_DIR or str(rel) in RUNTIME_FILES:
        return False
    return not any(part.startswith(".") or part.endswith(STATE_SUFFIXES) or part == "__pycache__"
                   for part in parts)


def env_input_files(env_dir: Path) -> list[Path]:
    """An env's committed inputs: everything under envs/<env> except runtime files."""
    env_dir = Path(env_dir)
    if not env_dir.is_dir():
        return []
    files = []
    for path in sorted(env_dir.rglob("*")):
        if path.is_file() and not path.is_symlink() and _is_input(path.relative_to(env_dir)):
            files.append(path)
    return files


def code_fingerprint(repo: Path, cloud_dir: Path) -> str:
    return _files_digest(repo, code_files(repo, cloud_dir))


def env_fingerprint(env_dir: Path) -> str:
    return _files_digest(Path(env_dir), env_input_files(env_dir))


def inputs_fingerprint(repo: Path, cloud_dir: Path, env: str) -> dict:
    """The fingerprint of code and an env's inputs (with the shared account layer)."""
    parts = {
        "code": code_fingerprint(repo, cloud_dir),
        "env": env_fingerprint(Path(cloud_dir) / "envs" / env),
        "account": env_fingerprint(Path(cloud_dir) / "envs" / "account"),
    }
    return parts | {"fingerprint": _json_digest(parts)}


def generated_digest(generated_dir: Path) -> str:
    """Digest of the promote-to-owned files in a generated/ dir (missing ones count)."""
    generated_dir = Path(generated_dir)
    entries = {name: sha256_file(generated_dir / name) if (generated_dir / name).is_file() else None
               for name in PROMOTE_OWNED}
    return _json_digest(entries)


def promotion_targets(cloud_dir: Path, source: str) -> list[str]:
    """Workspace envs promoted from ``source`` (promote_from; prod defaults to dev)."""
    targets = []
    envs = Path(cloud_dir) / "envs"
    for env_file in sorted(envs.glob("*/env.auto.tfvars")):
        name = env_file.parent.name
        if name in ("account", "data_access", source):
            continue
        promote_from = str(_load_hcl(env_file).get("promote_from") or ("dev" if name == "prod" else ""))
        if promote_from == source:
            targets.append(name)
    return targets


def promoted_digest(cloud_dir: Path, makefile: Path, make: str, target: str) -> tuple[str | None, str]:
    """What promote-to ENV=<target> writes, from the committed files alone (a scratch copy)."""
    from scripts.check_env import CheckFailed, Scratch

    try:
        scratch = Scratch(Path(cloud_dir), Path(makefile), make)
    except CheckFailed as exc:
        return None, str(exc)
    try:
        print(f"  Recomputing the promote-to ENV={target} digest in a scratch copy:")
        if scratch.make("promote-to", f"ENV={target}") != 0:
            return None, f"promote-to ENV={target} failed in a scratch copy"
        return generated_digest(scratch.envs / target / "generated"), ""
    finally:
        scratch.close()


# -- rehearse receipts --------------------------------------------------------

def _receipts(directory: Path) -> list[dict]:
    found = []
    for path in sorted(directory.rglob("*.json")) if directory.is_dir() else []:
        if path.name.startswith("."):
            continue
        receipt = _read_json(path)
        if receipt and receipt.get("version") == VERSION:
            found.append(receipt | {"_path": str(path)})
    return sorted(found, key=lambda r: str(r.get("created_at", "")))


def rehearse_dir(cloud_dir: Path, env: str) -> Path:
    return env_state(cloud_dir, env) / "receipts" / "rehearse"


def rehearse_receipts(cloud_dir: Path, env: str, *, any_store: bool = False) -> list[dict]:
    """The rehearse receipts this store wrote (``any_store``: also ones copied in, for messages)."""
    store = store_id(cloud_dir, env)
    return [r for r in _receipts(rehearse_dir(cloud_dir, env))
            if r.get("kind") == "rehearse" and (any_store or r.get("store") == store)]


def passing_receipt(cloud_dir: Path, env: str, commit: str) -> dict | None:
    """The latest passing receipt of a clean checkout at ``commit``."""
    found = [r for r in rehearse_receipts(cloud_dir, env)
             if r.get("commit") == commit and r.get("result") == "passed" and r.get("clean_tree") is True]
    return found[-1] if found else None


def reusable_receipt(cloud_dir: Path, env: str, fingerprint: str) -> dict | None:
    found = [r for r in rehearse_receipts(cloud_dir, env)
             if r.get("result") == "passed" and r.get("clean_tree") is True
             and r.get("fingerprint") == fingerprint]
    return found[-1] if found else None


def checkout_status(env_dir: Path) -> dict:
    """Commit, clean flag (with why not) of the checkout an env lives in."""
    env_dir = Path(env_dir)
    repo = repo_root(env_dir)
    cloud_dir = cloud_dir_of(env_dir)
    reasons = [f"uncommitted: {line}" for line in dirty_paths(repo, [cloud_dir, repo / "shared"])[:20]]
    if not tracked(repo, env_dir / "env.auto.tfvars"):
        reasons.append(f"{_rel(repo, env_dir / 'env.auto.tfvars')} is not committed (commit envs/; "
                       "see shared/docs/version-control.md)")
    return {"repo": str(repo), "commit": head(repo), "clean_tree": not reasons, "dirty": reasons}


def _write_rehearse(cloud_dir: Path, env: str, receipt: dict) -> Path:
    stamp = receipt["created_at"].replace(":", "").replace(".", "")
    name = f"{stamp}-{receipt['result']}-{os.getpid()}.json"
    return _write_once(rehearse_dir(cloud_dir, env) / receipt["commit"] / name, receipt)


def _target_digests(cloud_dir: Path, env: str, makefile: Path, make: str) -> dict:
    targets = {}
    for target in promotion_targets(cloud_dir, env):
        digest, error = promoted_digest(cloud_dir, makefile, make, target)
        targets[target] = {"digest": digest} | ({"error": error} if error else {})
    return targets


def rehearse_start(env_dir: Path, env: str, makefile: Path, make: str) -> str:
    """'reused' (a receipt was written without applying) or 'run' (rehearse must apply)."""
    env_dir = Path(env_dir)
    cloud_dir = cloud_dir_of(env_dir)
    status = checkout_status(env_dir)
    fp = inputs_fingerprint(Path(status["repo"]), cloud_dir, env)
    pending = env_state(cloud_dir, env) / "rehearse.pending.json"
    pending.parent.mkdir(parents=True, exist_ok=True)
    pending.write_text(json.dumps({"commit": status["commit"], "fingerprint": fp["fingerprint"],
                                   "parts": fp, "started_at": _now()}, indent=2) + "\n")
    earlier = reusable_receipt(cloud_dir, env, fp["fingerprint"])
    if earlier is None:
        return "run"
    receipt = {
        "kind": "rehearse", "version": VERSION, "env": env, "cloud": cloud_dir.name,
        "commit": status["commit"], "clean_tree": status["clean_tree"], "dirty": status["dirty"],
        "fingerprint": fp["fingerprint"], "fingerprint_parts": fp, "result": "passed",
        "reused_from": earlier["commit"], "applied": False, "store": store_id(cloud_dir, env),
        "targets": _target_digests(cloud_dir, env, makefile, make),
        "host": socket.gethostname(), "created_at": _now(),
    }
    path = _write_rehearse(cloud_dir, env, receipt)
    pending.unlink(missing_ok=True)
    print(f"=== Rehearse ({env}): code and {env} inputs are unchanged since the passing rehearsal of "
          f"{earlier['commit'][:12]}; wrote a receipt for {status['commit'][:12]} without re-applying ===")
    print(f"  Receipt: {path}")
    _print_targets(receipt["targets"])
    return "reused"


def rehearse_finish(env_dir: Path, env: str, result: str, makefile: Path, make: str, reason: str = "") -> dict:
    env_dir = Path(env_dir)
    cloud_dir = cloud_dir_of(env_dir)
    pending_path = env_state(cloud_dir, env) / "rehearse.pending.json"
    pending = _read_json(pending_path) or {}
    status = checkout_status(env_dir)
    fp = inputs_fingerprint(Path(status["repo"]), cloud_dir, env)
    if result == "passed" and (pending.get("commit") != status["commit"]
                               or pending.get("fingerprint") != fp["fingerprint"]):
        result, reason = "failed", ("the commit or the inputs changed during the run (an applying run must "
                                    "never rewrite committed files)")
    receipt = {
        "kind": "rehearse", "version": VERSION, "env": env, "cloud": cloud_dir.name,
        "commit": status["commit"], "clean_tree": status["clean_tree"], "dirty": status["dirty"],
        "fingerprint": fp["fingerprint"], "fingerprint_parts": fp, "result": result,
        "reused_from": None, "applied": True, "store": store_id(cloud_dir, env),
        "targets": _target_digests(cloud_dir, env, makefile, make) if result == "passed" else {},
        "host": socket.gethostname(), "created_at": _now(),
    } | ({"reason": reason} if reason else {})
    path = _write_rehearse(cloud_dir, env, receipt)
    pending_path.unlink(missing_ok=True)
    print(f"Rehearse receipt ({result}{'' if status['clean_tree'] else ', NOT a clean checkout'}): {path}")
    if not status["clean_tree"]:
        for line in status["dirty"][:5]:
            print(f"  {line}")
        print("  A release needs a passing rehearsal of a clean checkout of the commit it releases.")
    if result == "passed":
        _print_targets(receipt["targets"])
    else:
        print(f"  rehearse failed: {reason or 'see above'}", file=sys.stderr)
    return receipt | {"_path": str(path)}


def _print_targets(targets: Mapping) -> None:
    for target, info in sorted(targets.items()):
        if info.get("digest"):
            print(f"  promote-to ENV={target} digest: {info['digest'][:16]}")
        else:
            print(f"  promote-to ENV={target}: no digest ({info.get('error', 'unknown error')}); "
                  f"release-plan ENV={target} will refuse this commit")


# -- release receipts ---------------------------------------------------------

def release_dir(cloud_dir: Path, env: str) -> Path:
    return env_state(cloud_dir, env) / "receipts" / "release"


def release_receipts(cloud_dir: Path, env: str) -> list[dict]:
    return [r for r in _receipts(release_dir(cloud_dir, env)) if r.get("kind") == "release"]


def last_release(cloud_dir: Path, env: str) -> dict | None:
    found = release_receipts(cloud_dir, env)
    return found[-1] if found else None


# -- provenance ---------------------------------------------------------------

def _ack_entries(env_dir: Path, env: str):
    from ack_file import parse_file

    path = Path(env_dir) / "ack.txt"
    return parse_file(path, env) if path.is_file() else ()


def source_env(env_dir: Path) -> str:
    cfg = _load_hcl(Path(env_dir) / "env.auto.tfvars")
    source = str(cfg.get("promote_from") or ("dev" if Path(env_dir).name == "prod" else ""))
    if not source:
        raise Refused(f"{Path(env_dir).name} has no promote_from: release-plan releases an env promoted "
                      "from a rehearsed one (set promote_from in its env.auto.tfvars)")
    return source


def provenance(env_dir: Path, env: str, *, remote: str = "origin", branch: str = "main",
               fetch: bool = True) -> dict:
    """Everything release-plan requires of the checkout and the receipts; raises Refused."""
    env_dir = Path(env_dir)
    cloud_dir = cloud_dir_of(env_dir)
    status = checkout_status(env_dir)
    repo, commit = Path(status["repo"]), status["commit"]
    source = source_env(env_dir)
    problems = []
    if not status["clean_tree"]:
        problems.append("the checkout is not clean:\n      " + "\n      ".join(status["dirty"][:10]))
    if fetch:
        git(repo, "fetch", "--quiet", remote, branch)
    upstream = f"{remote}/{branch}"
    if not commit_known(repo, upstream):
        problems.append(f"{upstream} is unknown; fetch it first")
    elif not is_ancestor(repo, commit, upstream):
        problems.append(f"HEAD {commit[:12]} is not on {upstream} (freshly fetched); release only merged commits")
    receipt = passing_receipt(cloud_dir, source, commit)
    fp = inputs_fingerprint(repo, cloud_dir, source)
    committed = generated_digest(env_dir / "generated")
    if receipt is None:
        foreign = [r for r in rehearse_receipts(cloud_dir, source, any_store=True)
                   if r.get("commit") == commit and r.get("store") != store_id(cloud_dir, source)]
        problems.append(f"no passing rehearsal of {commit[:12]} (a clean checkout): run make rehearse "
                        f"ENV={source} on this commit first (receipts: {rehearse_dir(cloud_dir, source)})"
                        + (f"; {len(foreign)} receipt(s) for it were written by another store (a downloaded "
                           "CI artifact or a copy) and are informational only" if foreign else ""))
    else:
        if receipt.get("fingerprint") != fp["fingerprint"]:
            changed = [k for k in ("code", "env", "account")
                       if (receipt.get("fingerprint_parts") or {}).get(k) != fp[k]]
            problems.append(f"the {source} fingerprint differs from the rehearsal receipt "
                            f"({', '.join(changed) or 'fingerprint'} changed); re-run make rehearse ENV={source}")
        target = (receipt.get("targets") or {}).get(env) or {}
        if not target.get("digest"):
            problems.append(f"the rehearsal receipt has no promote-to ENV={env} digest "
                            f"({target.get('error', f'{env} was not a promotion target then')})")
        elif target["digest"] != committed:
            problems.append(f"envs/{env}/generated doesn't match what promote-to produces from this commit "
                            f"(stale promotion): run make promote-to ENV={env}, open a PR, then rehearse "
                            "its merge commit")
    last = last_release(cloud_dir, env)
    rollback = None
    if last and last.get("commit") and last["commit"] != commit:
        older = not commit_known(repo, last["commit"]) or is_ancestor(repo, commit, last["commit"])
        if older:
            acks = [e for e in _upstream_acks(repo, env_dir, env, upstream) if e.kind == "rollback"
                    and commit.startswith(e.object_name)]
            if acks:
                rollback = {"key": acks[0].key, "source": upstream, "consequence":
                            f"releases {commit[:12]}, older than the last release {last['commit'][:12]}"}
            else:
                problems.append(
                    f"{commit[:12]} is older than the last release {last['commit'][:12]} (an out-of-order or "
                    f"stale approval). To roll back to it on purpose, merge {env}:rollback:{commit} into "
                    f"envs/{env}/ack.txt on {upstream} in a reviewed PR, then re-run release-plan at {commit[:12]}")
    if problems:
        raise Refused("release-plan refused:\n  - " + "\n  - ".join(problems))
    return {
        "commit": commit, "remote": upstream, "source_env": source,
        "receipt": receipt.get("_path"), "fingerprint": fp["fingerprint"], "fingerprint_parts": fp,
        "generated_digest": committed, "last_release": (last or {}).get("commit"), "rollback": rollback,
        "checked_at": _now(),
    }


def _upstream_acks(repo: Path, env_dir: Path, env: str, upstream: str):
    """The acknowledgements in envs/<env>/ack.txt at the tip of upstream (reviewed, code-owned)."""
    from ack_file import parse_text

    shown = git(repo, "show", f"{upstream}:{_rel(repo, Path(env_dir) / 'ack.txt')}", check=False)
    return parse_text(shown.stdout, env) if shown.returncode == 0 else ()


def rollback_consequences(env_dir: Path, env: str, entries) -> dict[str, str]:
    """A rollback entry is accounted for once the commit it names was released."""
    released = [r.get("commit", "") for r in release_receipts(cloud_dir_of(env_dir), env)]
    out = {}
    for entry in entries:
        if entry.kind == "rollback" and any(c.startswith(entry.object_name) for c in released):
            out[entry.key] = f"the rollback to {entry.object_name} was released"
    return out


# -- terraform state ----------------------------------------------------------

def state_serials(env_dir: Path) -> dict[str, dict | None]:
    serials = {}
    env = Path(env_dir).name
    for root in ROOTS:
        try:
            state = state_json(env_dir, env, root)
        except RuntimeError as exc:
            raise Refused(str(exc)) from exc
        serials[root] = None if state is None else {"serial": state.get("serial"), "lineage": state.get("lineage")}
    return serials


# -- weakening and reader reports ------------------------------------------------

def _policy_value(policy: Mapping, known: Iterable[str] = ()) -> str:
    """The treatment value a mask policy matches: its condition, else the known value its name ends with.

    Names are ``gr_mask_<catalog>_<value>_<partial|full>``; the catalog may be
    an older one (kept names), so the value is matched by suffix, never parsed.
    """
    from scripts.policy_generations import policy_value

    value = policy_value(policy)
    if value:
        return value
    name = str(policy.get("name") or "")
    found = [v for v in known if name.endswith((f"_{v}_partial", f"_{v}_full"))]
    return max(found, key=len) if found else ""


def _resolved_policy_level(value: str, policy: Mapping) -> str:
    """The output level pinned by a deterministic value's generation hash."""
    if policy.get("level"):
        return str(policy["level"])
    if policy["name"].endswith("_full"):
        return "redacted"

    from governance_policies import ColumnTreatment, normalize_sql_type, parse_treatment_value
    from mask_library import load_library

    parsed = parse_treatment_value(value)
    item = load_library()["treatments"].get(parsed[0]) if parsed else None
    if not parsed or not item:
        return "partial"  # Unknown values must never be assumed to provide stronger protection.
    token = parsed[1].upper()
    sql_type = (f"DECIMAL({token.split('_')[1]},{token.split('_')[2]})"
                if token.startswith("DECIMAL_") else normalize_sql_type(token))
    redacting = {
        ColumnTreatment(parsed[0], sql_type, version, item["full"], False).generation
        for version in ("redacted", "null")
    }
    return "redacted" if parsed[2] in redacting else "partial"


def _levels(tags: Mapping[str, str], policies: list[Mapping], universe: set[str]) -> dict:
    """column -> principal -> (level, consequence, value) from the mask policies that match it."""
    from governance_policies import CUSTOM_TREATMENT, base_treatment, parse_treatment_value
    from scripts.policy_generations import ALL_USERS

    def covers(policy: Mapping, who: str) -> bool:
        to, excepted = set(policy.get("to") or []), set(policy.get("except") or [])
        return (ALL_USERS in to or who in to) and who not in excepted

    by_value: dict[str, list[Mapping]] = {}
    for policy in policies:
        by_value.setdefault(policy["value"], []).append(policy)
    out: dict[str, dict] = {}
    for column, value in tags.items():
        treatment = base_treatment(value)
        parsed = parse_treatment_value(value)
        # A custom mask's generation is its SQL, so the consequence names it.
        shown = f"{treatment}@{parsed[2]}" if parsed and CUSTOM_TREATMENT.fullmatch(treatment) else treatment
        levels = {}
        for who in universe:
            on = [p for p in by_value.get(value, []) if covers(p, who)]
            if any(_resolved_policy_level(value, p) == "redacted" for p in on):
                levels[who] = ("redacted", f"redacted ({treatment})", value)
            elif on:
                levels[who] = ("partial", f"partial ({shown})", value)
            else:
                levels[who] = ("raw", "raw", value)
        out[column] = levels
    return out


def _deployed(env_dir: Path) -> dict:
    """What prod's data_access state says GenieRails deployed."""
    from scripts.staged_policy_apply import _read_state, live_from_state, load_journal
    from scripts import migration, policy_generations as pg
    from table_readers import applied_grants

    legacy = migration.baseline(Path(env_dir))
    if legacy is not None:  # a migration from legacy: compare with what the legacy masks enforce
        return migration.legacy_view(legacy)
    live = live_from_state(_read_state(Path(env_dir)))
    journal = load_journal(Path(env_dir)) or {}
    snapshot = pg.load_snapshot(Path(env_dir), journal.get("active", "")) if journal.get("active") else None
    values = {}
    for policy in (snapshot or {}).get("fgac_policies") or []:
        values[policy["name"]] = _policy_value(policy)
    known = set(live["tags"].values()) | set(values.values())
    policies = [{"name": name, "value": values.get(name) or _policy_value({"name": name}, known),
                 "to": p["to"], "except": p["except"]} for name, p in live["policies"].items()]
    readers: dict[str, set[str]] = {}
    for key in applied_grants(Path(env_dir)):
        table, principal = key.split("|", 1)
        readers.setdefault(table.lower(), set()).add(principal)
    return {"tags": live["tags"], "policies": policies, "settings": (snapshot or {}).get("settings") or {},
            "table_readers": readers, "has_generation": bool(journal.get("active"))}


def _desired(env_dir: Path, config_path: Path | None = None) -> dict:
    from scripts.runtime_overlay import effective
    from governance_policies import TAG_KEY
    from table_readers import governed_tables, normalize, readers_for

    cfg = _load_hcl(config_path or effective(env_dir, "abac.auto.tfvars"))
    env_cfg = _load_hcl(Path(env_dir) / "env.auto.tfvars")
    tags = {str(i["entity_name"]): str(i["tag_value"]) for i in cfg.get("tag_assignments") or []
            if i.get("entity_type") == "columns" and i.get("tag_key") == TAG_KEY}
    policies = [{"name": str(p["name"]), "value": _policy_value(p), "to": list(p.get("to_principals") or []),
                 "except": list(p.get("except_principals") or [])}
                for p in cfg.get("fgac_policies") or [] if p.get("policy_type") == "POLICY_TYPE_COLUMN_MASK"]
    readers_cfg = normalize(env_cfg.get("table_readers"), str(env_cfg.get("uc_catalog") or ""))
    tables = set(governed_tables(Path(env_dir), env_cfg)) | {c.rsplit(".", 1)[0] for c in tags}
    readers = {t.lower(): set(readers_for(t, readers_cfg)) for t in tables if readers_for(t, readers_cfg)}
    return {"tags": tags, "policies": policies, "settings": env_cfg, "table_readers": readers}


def _partial_versions(settings: Mapping) -> dict[str, str]:
    versions = {}
    for treatment, value in dict(settings.get("treatment_versions") or {}).items():
        if isinstance(value, Mapping) and value.get("partial") is not None:
            versions[str(treatment)] = str(value["partial"])
    for column, value in dict(settings.get("column_overrides") or {}).items():
        if isinstance(value, Mapping) and value.get("partial") is not None:
            versions[str(column)] = str(value["partial"])
    return versions


def governance_states(before: Mapping, after: Mapping):
    """weakening.GovernanceState before and after, over one principal universe."""
    from weakening import GovernanceState, Protection
    from scripts.policy_generations import ALL_USERS

    universe = {ALL_USERS}
    for side in (before, after):
        for policy in side["policies"]:
            universe.update(policy["to"] + policy["except"])
        for readers in side["table_readers"].values():
            universe.update(readers)
        universe.update(side["settings"].get("raw_exempt_principals") or [])
    states = []
    columns = set(before["tags"]) | set(after["tags"])
    for side in (before, after):
        levels = _levels(side["tags"], side["policies"], universe)
        for column in columns - set(side["tags"]):
            levels[column] = {who: ("raw", "raw (no GenieRails mask)") for who in universe}
        states.append(GovernanceState(
            columns={c: {w: Protection(*lv) for w, lv in per.items()} for c, per in levels.items()},
            raw_exempt_principals=set(side["settings"].get("raw_exempt_principals") or []),
            partial_versions=_partial_versions(side["settings"]),
            table_readers={t: set(r) for t, r in side["table_readers"].items()},
        ))
    return states[0], states[1]


def newly_covered(before: Mapping, after: Mapping) -> list[str]:
    """Tables the reader check inventories: all on a first deterministic apply, else new ones."""
    after_tables = {c.rsplit(".", 1)[0].lower() for c in after["tags"]} | set(after["table_readers"])
    if not before.get("has_generation"):
        return sorted(after_tables)
    before_tables = {c.rsplit(".", 1)[0].lower() for c in before["tags"]} | set(before["table_readers"])
    return sorted(after_tables - before_tables)


def reports(env_dir: Path, env: str, *, reader_source: Callable[[], object] | None = None,
            rollback: Mapping | None = None) -> dict:
    """Reader inventory and refuse-weakening, reconciled once against ack.txt."""
    from ack_file import merge_consequences, reconcile
    from reader_inventory import Grant, check_readers, inventory_readers
    from weakening import check_weakenings
    from dataclasses import asdict
    from scripts import migration
    import group_map

    env_dir = Path(env_dir)
    entries = _ack_entries(env_dir, env)
    keys = {e.key for e in entries}
    before, after = _deployed(env_dir), _desired(env_dir)
    weak = check_weakenings(*governance_states(before, after), env, keys)
    # A migration from legacy: its revocations (and, once adopted, every
    # acknowledgement the adoption accepted, so none turns stale).
    migrated, unacked_revocations = migration.accounted(env_dir, env)
    if migration.baseline(env_dir) is not None:  # each lost SELECT is reported once, as its revocation
        weak = migration.without_revoked(weak, env, migrated)
    tables = newly_covered(before, after)
    reader_reports, reader_checks, reader_consequences = [], [], {}
    if tables:
        if reader_source is None:
            raise Refused(f"the reader check needs a SQL warehouse to inventory {len(tables)} newly governed "
                          "table(s); set sql_warehouse_id in env.auto.tfvars or pass WAREHOUSE_ID=<id>")
        source = reader_source()
        managed = [Grant("TABLE", t, p, "SELECT") for t, ps in before["table_readers"].items() for p in ps]
        tiers = list(after["settings"].get("access_tier_groups") or [])
        exempt = list(after["settings"].get("raw_exempt_principals") or [])
        for table in tables:
            inventory = inventory_readers(source, table, managed)
            check = check_readers(inventory, env, tiers, exempt, keys)
            reader_reports.append(inventory.to_dict())
            reader_checks.append(asdict(check))
            reader_consequences.update(check.consequences)
    group_consequences = group_map.required_acks(env_dir, env, after["settings"])
    rollbacks = rollback_consequences(env_dir, env, entries)
    if rollback:
        rollbacks[rollback["key"]] = rollback["consequence"]
    from scripts.agent_ownership import delete_consequences, reactivation_consequences

    deletes = delete_consequences(env_dir, env)
    reactivations = reactivation_consequences(env_dir, env)
    consequences = merge_consequences(weak.consequences, reader_consequences, group_consequences, rollbacks,
                                      migrated, deletes, reactivations)
    # Unclassified acks belong to the classification check (ACK_UNCLASSIFIED today).
    reconciled = [e for e in entries if e.kind not in ("unclassified", "detached-agent")]
    _, unmatched = reconcile(reconciled, consequences)
    unacked_readers = [f for c in reader_checks for f in c["unacknowledged"]]
    return {
        "ok": weak.ok and not unacked_readers and not unmatched and not unacked_revocations,
        "revocations": {"acknowledged": sorted(k for k in migrated if k.split(":")[1] == "revoke"
                                               and k not in unacked_revocations),
                        "unacknowledged": [{"ack_key": k, "consequence": migrated[k]} for k in unacked_revocations]},
        "weakening": {"acknowledged": [asdict(w) for w in weak.acknowledged],
                      "unacknowledged": [asdict(w) for w in weak.unacknowledged]},
        "readers": {"tables": tables, "inventories": reader_reports, "checks": reader_checks},
        "group_map": group_consequences, "rollback": rollbacks, "agent_deletions": deletes,
        "unmatched_acknowledgements": [e.key for e in unmatched],
    }


def print_reports(report: Mapping) -> None:
    for item in report["weakening"]["acknowledged"]:
        print(f"  ACKNOWLEDGED {item['ack_key']}: {item['before']} -> {item['after']}")
    for check in report["readers"]["checks"]:
        for item in check["acknowledged"]:
            print(f"  ACKNOWLEDGED {item['ack_key']}: {item['consequence']}")
    tables = report["readers"]["tables"]
    print(f"  Reader check: {len(tables)} newly governed table(s) inventoried"
          + (f" ({', '.join(tables[:5])}{', ...' if len(tables) > 5 else ''})" if tables else ""))
    for item in report["weakening"]["unacknowledged"]:
        print(f"ERROR: unacknowledged weakening {item['ack_key']}: {item['before']} -> {item['after']}",
              file=sys.stderr)
    for check in report["readers"]["checks"]:
        for item in check["unacknowledged"]:
            print(f"ERROR: unacknowledged reader {item['ack_key']}: {item['consequence']}", file=sys.stderr)
    for item in (report.get("revocations") or {}).get("unacknowledged") or []:
        print(f"ERROR: unacknowledged revocation {item['ack_key']}: {item['consequence']}", file=sys.stderr)
    for key in report["unmatched_acknowledgements"]:
        print(f"ERROR: ack.txt entry matches nothing (stale): {key}", file=sys.stderr)


def _databricks_reader_source(env_dir: Path, warehouse_id: str):
    def build():
        from reader_inventory import DatabricksSource
        from scripts.audit_schema_drift import _get_sdk_client

        return DatabricksSource(warehouse_id, client=_get_sdk_client(Path(env_dir) / "data_access"))
    return build


# -- live snapshot --------------------------------------------------------------

def live_policies(env_dir: Path, catalogs: Iterable[str], lister=None) -> dict[str, list[str]]:
    from remap_generated_config import live_policy_lister

    lister = lister or live_policy_lister(Path(env_dir) / "data_access" / "auth.auto.tfvars")
    out = {}
    for catalog in sorted(set(catalogs)):
        names = lister(catalog)
        if names is None:
            raise Refused(f"the policies on {catalog} couldn't be listed; the live snapshot can't be taken")
        out[catalog] = sorted(names)
    return out


def snapshot_digest(abac: Path, sql: Path, ddl: Path, policies: Mapping) -> str:
    def sha(path: Path) -> str | None:
        return sha256_file(path) if Path(path).is_file() else None
    return _json_digest({"abac": sha(abac), "sql": sha(sql), "ddl": sha(ddl), "policies": policies})


def live_snapshot(env_dir: Path, *, rederive: bool, derive_script: Path | None = None) -> str:
    """Digest of live UC as GenieRails reads it: the derived config, the DDL and the live policies.

    Without ``rederive`` it digests what the last derive-assignments wrote
    (release-plan, right after deriving). With ``rederive`` it derives again
    into a scratch dir, with the same kept names (release, before applying).
    """
    from scripts import runtime_overlay as ro

    env_dir = Path(env_dir)
    if not rederive:
        abac, sql = ro.effective(env_dir, "abac.auto.tfvars"), ro.effective(env_dir, "masking_functions.sql")
        policies = live_policies(env_dir, _catalogs(abac))
        return snapshot_digest(abac, sql, env_dir / "ddl" / "_fetched.sql", policies)
    with tempfile.TemporaryDirectory(prefix="genierails-live-") as tmp:
        out = Path(tmp)
        script = derive_script or SHARED / "scripts" / "derive_assignments.py"
        result = subprocess.run(
            [sys.executable, str(script), "--auth-file", "auth.auto.tfvars", "--env-file", "env.auto.tfvars",
             "--config", "generated/abac.auto.tfvars", "--out-dir", str(out), "--write-ddl",
             str(out / "_fetched.sql")], cwd=env_dir, check=False)
        if result.returncode:
            raise Refused("re-reading live UC for the live snapshot failed (above)")
        _rename_kept(out / "abac.auto.tfvars", ro.load_kept_names(env_dir))
        policies = live_policies(env_dir, _catalogs(out / "abac.auto.tfvars"))
        return snapshot_digest(out / "abac.auto.tfvars", out / "masking_functions.sql", out / "_fetched.sql",
                               policies)


def _catalogs(abac: Path) -> set[str]:
    return {str(p.get("catalog")) for p in _load_hcl(abac).get("fgac_policies") or [] if p.get("catalog")}


def _rename_kept(path: Path, kept: Mapping[str, str]) -> None:
    import re
    from remap_generated_config import _top_level_list_span

    if not kept or not path.is_file():
        return
    text = path.read_text()
    span = _top_level_list_span(text, "fgac_policies")
    if span is None:
        return
    _start, bracket, end = span
    section = re.sub(r'(?m)^(\s*name\s*=\s*")([^"]*)(")',
                     lambda m: m.group(1) + kept.get(m.group(2), m.group(2)) + m.group(3), text[bracket:end])
    path.write_text(text[:bracket] + section + text[end:])


# -- manifests ----------------------------------------------------------------

def manifests_dir(cloud_dir: Path, env: str) -> Path:
    return env_state(cloud_dir, env) / "manifests"


def new_plan_dir(env_dir: Path, env: str) -> Path:
    """A private dir for release-plan's saved plans.

    A saved Terraform plan carries the variable values it was planned with,
    including the deployer's auth.auto.tfvars, and the prior state; like
    Terraform state, manifests are readable by their owner only.
    """
    directory = manifests_dir(cloud_dir_of(env_dir), env)
    directory.mkdir(parents=True, exist_ok=True)
    os.chmod(directory, 0o700)
    return Path(tempfile.mkdtemp(prefix=".pending-", dir=directory))  # mkdtemp: 0700


def _plan_entry(plan_dir: Path, root: str) -> dict:
    """A saved plan of the account or workspace root (see make release-plan)."""
    info = _read_json(plan_dir / f"{root}.json")
    if info is None:
        raise Refused(f"release-plan didn't record the {root} plan")
    if info.get("skipped"):
        return {"skipped": info["skipped"]}
    plan = plan_dir / info["file"]
    if not plan.is_file():
        raise Refused(f"the saved {root} plan {plan} is missing")
    return {"file": info["file"], "sha256": sha256_file(plan), "changes": bool(info.get("changes"))}


def _data_access_entry(plan_dir: Path) -> dict:
    steps_file = plan_dir / "data_access" / "phases.json"
    info = _read_json(steps_file)
    if info is None:
        raise Refused(f"the staged policy controller recorded no plan ({steps_file})")
    for step in info.get("steps") or []:
        if step.get("file"):
            path = plan_dir / "data_access" / step["file"]
            if not path.is_file() or sha256_file(path) != step.get("sha256"):
                raise Refused(f"the saved data_access plan {path} doesn't match what the controller recorded")
    return info


def _imports_entry(env_dir: Path, plan_dir: Path) -> dict:
    """The imports and stale-tag cleanups release-plan recorded instead of running (import_existing.sh --record).

    release MANIFEST= runs exactly these, under the env lock, and nothing else;
    each must be an entry the recorder writes (scripts/import_record.py).
    """
    from scripts import import_record

    path = plan_dir / "imports.jsonl"
    if not path.is_file():
        return {"count": 0}
    try:
        entries = import_record.load(path, env_dir)
    except import_record.Invalid as exc:
        raise Refused(f"the recorded imports {path} hold an entry release-plan never records ({exc}); "
                      "nothing was recorded") from exc
    if not entries:
        return {"count": 0}
    return {"count": len(entries), "file": path.name, "sha256": sha256_file(path), "entries": entries}


def manifest_write(env_dir: Path, env: str, plan_dir: Path, prov: Mapping, live: str,
                   report: Mapping) -> Path:
    from scripts import runtime_overlay as ro

    env_dir, plan_dir = Path(env_dir), Path(plan_dir)
    status = checkout_status(env_dir)
    if status["commit"] != prov.get("commit") or not status["clean_tree"]:
        raise Refused("the checkout changed while release-plan ran; nothing was recorded. Re-run release-plan")
    if not report.get("ok"):
        raise Refused("the reader or weakening check refused (above); add the reviewed acknowledgements to "
                      f"envs/{env}/ack.txt in a PR, or fix the config, then re-run make release-plan ENV={env}")
    data_access = _data_access_entry(plan_dir)
    plans = {"account": _plan_entry(plan_dir, "account"), "data_access": data_access,
             "workspace": _plan_entry(plan_dir, "workspace")}
    # The Genie agent plan (scripts/agent_ownership.py plan); release pushes only it. None
    # recorded means no agents: release refuses if the env's agents differ from it.
    agents = _read_json(plan_dir / "agents.json") or {"agents": {}}
    if plans["account"].get("changes"):
        raise Refused(f"the account plan is not empty; rehearse and release may only add treatment-tag values. "
                      f"Run make apply-account ENV={env} in the operations stage first")
    created = _now()
    manifest_id = f"{created[:19].replace(':', '').replace('-', '')}-{prov['commit'][:12]}"
    final_dir = manifests_dir(cloud_dir_of(env_dir), env) / manifest_id
    manifest = {
        "kind": "release-manifest", "version": VERSION, "id": manifest_id, "env": env,
        "cloud": cloud_dir_of(env_dir).name, "commit": prov["commit"], "provenance": dict(prov),
        "state_serials": state_serials(env_dir), "plans": plans, "imports": _imports_entry(env_dir, plan_dir),
        "final": bool(data_access.get("final")),
        "effective_digest": ro.digest(env_dir), "live_snapshot": live,
        "kept_names": ro.load_kept_names(env_dir), "reports": dict(report), "agents": agents,
        "host": socket.gethostname(), "created_at": created,
    }
    _write_once(plan_dir / "manifest.json", manifest)
    for path in plan_dir.rglob("*"):
        os.chmod(path, 0o400 if path.is_file() else 0o700)
    os.rename(plan_dir, final_dir)  # the manifest dir appears complete or not at all
    return final_dir / "manifest.json"


def load_manifest(path: Path, env: str) -> dict:
    manifest = _read_json(Path(path))
    if manifest is None or manifest.get("kind") != "release-manifest" or manifest.get("version") != VERSION:
        raise Refused(f"{path} is not a release manifest")
    if manifest.get("env") != env:
        raise Refused(f"{path} is a manifest for {manifest.get('env')}, not {env}")
    return manifest


def manifest_check(env_dir: Path, env: str, manifest_path: Path, *, live: str | None = None) -> dict:
    """Refuse a manifest anything has drifted from; returns it."""
    from scripts import runtime_overlay as ro

    env_dir, manifest_path = Path(env_dir), Path(manifest_path)
    manifest = load_manifest(manifest_path, env)
    plan_dir = manifest_path.parent
    status = checkout_status(env_dir)
    drift = []
    if manifest.get("cloud") != cloud_dir_of(env_dir).name:
        drift.append(f"the manifest is for {manifest.get('cloud')}, not {cloud_dir_of(env_dir).name}")
    if status["commit"] != manifest["commit"]:
        drift.append(f"HEAD is {status['commit'][:12]}, the manifest planned {manifest['commit'][:12]}")
    if not status["clean_tree"]:
        drift.append("the checkout is not clean:\n      " + "\n      ".join(status["dirty"][:10]))
    serials = state_serials(env_dir)
    for root in ROOTS:
        if serials.get(root) != manifest["state_serials"].get(root):
            drift.append(f"the {root} state changed since release-plan (serial "
                         f"{(manifest['state_serials'].get(root) or {}).get('serial')} -> "
                         f"{(serials.get(root) or {}).get('serial')})")
    for root in ("account", "workspace"):
        entry = manifest["plans"][root]
        if entry.get("file"):
            path = plan_dir / entry["file"]
            if not path.is_file() or sha256_file(path) != entry["sha256"]:
                drift.append(f"the saved {root} plan {path} changed or is missing")
    for step in manifest["plans"]["data_access"].get("steps") or []:
        if step.get("file"):
            path = plan_dir / "data_access" / step["file"]
            if not path.is_file() or sha256_file(path) != step.get("sha256"):
                drift.append(f"the saved data_access plan {path} changed or is missing")
    imports = manifest.get("imports") or {}
    if imports.get("file") not in (None, "imports.jsonl"):  # release runs <manifest dir>/imports.jsonl
        drift.append(f"the manifest's recorded imports are {imports.get('file')!r}, not imports.jsonl")
    elif imports.get("file"):
        path = plan_dir / imports["file"]
        if not path.is_file() or sha256_file(path) != imports.get("sha256"):
            drift.append(f"the recorded imports {path} changed or are missing")
        else:
            from scripts import import_record
            try:
                if import_record.load(path, env_dir) != imports.get("entries"):
                    drift.append(f"the recorded imports {path} differ from the manifest's entries")
            except import_record.Invalid as exc:
                drift.append(f"the recorded imports {path} hold an entry release-plan never records ({exc})")
    elif imports.get("count") not in (0, None):
        drift.append("the manifest counts recorded imports but names no file")
    if ro.digest(env_dir) != manifest["effective_digest"]:
        drift.append("the effective config (generated/.runtime) changed since release-plan")
    if ro.load_kept_names(env_dir) != manifest.get("kept_names", {}):
        drift.append("the kept policy names changed since release-plan")
    if live is not None and live != manifest["live_snapshot"]:
        drift.append("live Unity Catalog changed since release-plan (tags, column types or policies)")
    if drift:
        raise Refused(f"make release refused: {manifest_path} no longer matches:\n  - " + "\n  - ".join(drift)
                      + f"\nNothing was applied. Run make release-plan ENV={env} again and approve the new manifest.")
    return manifest


def release_receipt(env_dir: Path, env: str, manifest_path: Path) -> Path:
    from scripts.agent_ownership import receipt_agents

    env_dir, manifest_path = Path(env_dir), Path(manifest_path)
    manifest = load_manifest(manifest_path, env)
    cloud_dir = cloud_dir_of(env_dir)
    status = checkout_status(env_dir)
    repo = Path(status["repo"])
    receipt = {
        "kind": "release", "version": VERSION, "env": env, "cloud": cloud_dir.name,
        "commit": manifest["commit"], "clean_tree": status["clean_tree"],
        "digests": {
            "code": code_fingerprint(repo, cloud_dir), "env": env_fingerprint(env_dir),
            "generated": generated_digest(env_dir / "generated"),
            "effective": manifest["effective_digest"], "live_snapshot": manifest["live_snapshot"],
        },
        "kept_names": manifest.get("kept_names", {}),
        "manifest": {"id": manifest["id"], "path": str(manifest_path), "sha256": sha256_file(manifest_path)},
        "phase_manifests": [m["id"] for m in phase_records(cloud_dir, env, manifest["commit"])],
        # Prod agent IDs live here and in state, never in git (design section 5).
        "agents": receipt_agents(env_dir, manifest),
        "state_serials": state_serials(env_dir), "host": socket.gethostname(), "created_at": _now(),
    }
    stamp = receipt["created_at"].replace(":", "").replace(".", "")
    return _write_once(release_dir(cloud_dir, env) / f"{stamp}-{manifest['commit'][:12]}.json", receipt)


def phase_record(env_dir: Path, env: str, manifest_path: Path) -> Path:
    """A non-final manifest (one staged policy phase) was applied."""
    manifest = load_manifest(Path(manifest_path), env)
    cloud_dir = cloud_dir_of(env_dir)
    record = {"kind": "release-phase", "version": VERSION, "env": env, "commit": manifest["commit"],
              "id": manifest["id"], "manifest": str(manifest_path), "created_at": _now()}
    return _write_once(env_state(cloud_dir, env) / "receipts" / "phases" / f"{manifest['id']}.json", record)


def phase_records(cloud_dir: Path, env: str, commit: str) -> list[dict]:
    last = last_release(cloud_dir, env)
    since = str((last or {}).get("created_at", ""))
    return [r for r in _receipts(env_state(cloud_dir, env) / "receipts" / "phases")
            if r.get("commit") == commit and str(r.get("created_at", "")) > since]


def maintain_check(env_dir: Path, env: str) -> dict:
    """maintain runs only from the last released commit, with matching digests."""
    env_dir = Path(env_dir)
    cloud_dir = cloud_dir_of(env_dir)
    last = last_release(cloud_dir, env)
    if last is None:
        raise Refused(f"make maintain refused: {env} has no release receipt yet "
                      f"({release_dir(cloud_dir, env)}). Release it first: make release-plan ENV={env}, "
                      f"then make release ENV={env} MANIFEST=<manifest>")
    status = checkout_status(env_dir)
    repo = Path(status["repo"])
    problems = []
    if status["commit"] != last["commit"]:
        problems.append(f"HEAD is {status['commit'][:12]}, but the last release is {last['commit'][:12]}: "
                        f"check out that commit (git checkout --detach {last['commit']})")
    if not status["clean_tree"]:
        problems.append("the checkout is not clean:\n      " + "\n      ".join(status["dirty"][:10]))
    digests = last.get("digests") or {}
    now = {"code": code_fingerprint(repo, cloud_dir), "env": env_fingerprint(env_dir),
           "generated": generated_digest(env_dir / "generated")}
    for key, value in now.items():
        if digests.get(key) != value:
            problems.append(f"the {key} digest differs from the release receipt")
    if problems:
        raise Refused("make maintain refused:\n  - " + "\n  - ".join(problems))
    return last


# -- no side door: applying targets in deterministic envs -----------------------

PROMOTED, REHEARSE, LEGACY = "promoted", "rehearse", "legacy"
# target -> lock owners (make targets) it may run under, per env role.
APPLY_OWNERS = {
    PROMOTED: {"apply": (), "apply-genie": (), "apply-governance": ("maintain",),
               "_apply-layer:data_access": ("maintain",), "_apply-layer:workspace": ()},
    REHEARSE: {"apply": ("rehearse",), "apply-genie": ("rehearse",), "apply-governance": ("rehearse", "ungovern"),
               "_apply-layer:data_access": ("rehearse", "ungovern"), "_apply-layer:workspace": ("rehearse",)},
}


def env_role(env_dir: Path) -> str:
    """legacy; promoted (a promote-to target: promote_from set, or prod); else the rehearse env."""
    env_dir = Path(env_dir)
    if not deterministic(env_dir):
        return LEGACY
    cfg = _load_hcl(env_dir / "env.auto.tfvars")
    return PROMOTED if cfg.get("promote_from") or env_dir.name == "prod" else REHEARSE


def apply_guard(env_dir: Path, env: str, target: str) -> None:
    from scripts.environment_lock import held_by_ancestor

    role = env_role(env_dir)
    if role == LEGACY:
        legacy_guard(env_dir, env, target.split(":")[0])
        return
    allowed = APPLY_OWNERS[role].get(target)
    if allowed is None:
        raise Refused(f"unknown applying target {target!r}")
    owner = held_by_ancestor(env_dir)
    if owner in allowed:
        return
    name = target.split(":")[0]
    layer = f" (the {target.split(':')[1]} layer)" if ":" in target else ""
    if role == PROMOTED:
        how = (f"changes reach it only through make release-plan ENV={env}, then make release ENV={env} "
               f"MANIFEST=<the manifest it printed>" + ("; make maintain ENV=" + env + " re-applies governance "
                                                         "from the released commit" if "maintain" in allowed else ""))
        why = f"ENV={env} is a deterministic env promoted from {source_env(env_dir)}"
    else:
        how = f"run make rehearse ENV={env} (it applies, verifies and writes the receipt release-plan needs)"
        why = f"ENV={env} is a deterministic env, applied only by rehearse"
    ran = f" (it runs under make {owner})" if owner else ""
    raise Refused(f"make {name}{layer} refused{ran}: {why}; {how}.")


def legacy_guard(env_dir: Path, env: str, command: str) -> None:
    """A legacy-mode applying command refuses once a migration to deterministic began.

    Read from the data_access state as well as the migration record, so an
    adoption interrupted after its first state move is caught too.
    """
    from scripts import migration

    if not deterministic(env_dir):
        why = migration.migrated(Path(env_dir))
        if why:
            raise Refused(f"make {command} refused: ENV={env} was migrated to deterministic governance ({why}), "
                          "and a legacy apply would destroy and recreate its moved treatment tags. Restore "
                          "governance_mode = \"deterministic\"; to undo a change, roll back within deterministic "
                          "governance (a revert PR and <env>:rollback:<commit>)")


def destroy_guard(env_dir: Path, env: str, confirm: str) -> None:
    if env_role(env_dir) != PROMOTED or confirm == env:
        return
    raise Refused(f"make destroy refused: ENV={env} is a deterministic promoted env; destroying it removes its "
                  f"masks and grants outside any manifest. If you mean it (a throwaway env, an operations "
                  f"teardown), re-run with CONFIRM_DESTROY={env} on the command line.")


# -- CLI ------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    def command(name: str, **kwargs):
        cmd = sub.add_parser(name, **kwargs)
        cmd.add_argument("--env-dir", type=Path, required=True)
        cmd.add_argument("--env", required=True)
        return cmd

    command("state-dir")
    for name in ("rehearse-start", "rehearse-finish"):
        cmd = command(name)
        cmd.add_argument("--makefile", type=Path, required=True)
        cmd.add_argument("--make", default="make")
        if name == "rehearse-finish":
            cmd.add_argument("--result", choices=("passed", "failed"), required=True)
            cmd.add_argument("--reason", default="")
    cmd = command("provenance")
    cmd.add_argument("--remote", default="origin")
    cmd.add_argument("--branch", default="main")
    cmd.add_argument("--out", type=Path)
    command("plan-dir")
    cmd = command("reports")
    cmd.add_argument("--plan-dir", type=Path, required=True)
    cmd.add_argument("--provenance", type=Path, required=True)
    cmd.add_argument("--warehouse-id", default="")
    cmd = command("live-snapshot")
    cmd.add_argument("--rederive", action="store_true")
    cmd = command("manifest-write")
    cmd.add_argument("--plan-dir", type=Path, required=True)
    cmd.add_argument("--provenance", type=Path, required=True)
    cmd.add_argument("--live-snapshot", required=True)
    cmd = command("manifest-check")
    cmd.add_argument("--manifest", type=Path, required=True)
    cmd.add_argument("--live-snapshot", default=None)
    cmd.add_argument("--field", default="", help="print one manifest field (json path a.b) instead")
    for name in ("release-receipt", "phase-record"):
        command(name).add_argument("--manifest", type=Path, required=True)
    command("maintain-check")
    command("apply-guard").add_argument("--target", required=True)
    command("destroy-guard").add_argument("--confirm", default="")
    command("env-role")
    command("legacy-guard").add_argument("--command-name", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "state-dir":
            print(env_state(cloud_dir_of(args.env_dir), args.env))
        elif args.command == "rehearse-start":
            # 10: a receipt was written without re-applying (inputs unchanged).
            return 10 if rehearse_start(args.env_dir, args.env, args.makefile, args.make) == "reused" else 0
        elif args.command == "rehearse-finish":
            receipt = rehearse_finish(args.env_dir, args.env, args.result, args.makefile, args.make, args.reason)
            return 0 if receipt["result"] == "passed" else 1
        elif args.command == "provenance":
            prov = provenance(args.env_dir, args.env, remote=args.remote, branch=args.branch)
            if args.out:
                args.out.write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
            print(f"Provenance: {prov['commit'][:12]} is on {prov['remote']}, clean, rehearsed in "
                  f"{prov['source_env']} (receipt matches; envs/{args.env}/generated matches its promotion)"
                  + (f"; ROLLBACK acknowledged ({prov['rollback']['key']})" if prov["rollback"] else ""))
        elif args.command == "plan-dir":
            print(new_plan_dir(args.env_dir, args.env))
        elif args.command == "reports":
            prov = _read_json(args.provenance) or {}
            source = _databricks_reader_source(args.env_dir, args.warehouse_id) if args.warehouse_id else None
            report = reports(args.env_dir, args.env, reader_source=source, rollback=prov.get("rollback"))
            (args.plan_dir / "reports.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            print_reports(report)
            if not report["ok"]:
                return 1
        elif args.command == "live-snapshot":
            print(live_snapshot(args.env_dir, rederive=args.rederive))
        elif args.command == "manifest-write":
            report = _read_json(args.plan_dir / "reports.json")
            if report is None:
                raise Refused("release-plan recorded no reader/weakening report")
            path = manifest_write(args.env_dir, args.env, args.plan_dir, _read_json(args.provenance) or {},
                                  args.live_snapshot, report)
            print(path)
        elif args.command == "manifest-check":
            if args.field:
                value = load_manifest(args.manifest, args.env)
                for part in args.field.split("."):
                    value = value.get(part) if isinstance(value, dict) else None
                print(json.dumps(value) if not isinstance(value, str) else value)
                return 0
            manifest_check(args.env_dir, args.env, args.manifest, live=args.live_snapshot)
            print(f"Manifest {args.manifest}: commit, state serials, saved plans"
                  + (", effective config and live snapshot" if args.live_snapshot else " and effective config")
                  + " match.")
        elif args.command == "release-receipt":
            print(f"Release receipt: {release_receipt(args.env_dir, args.env, args.manifest)}")
        elif args.command == "phase-record":
            print(f"Phase record: {phase_record(args.env_dir, args.env, args.manifest)}")
        elif args.command == "apply-guard":
            apply_guard(args.env_dir, args.env, args.target)
        elif args.command == "destroy-guard":
            destroy_guard(args.env_dir, args.env, args.confirm)
        elif args.command == "legacy-guard":
            legacy_guard(args.env_dir, args.env, args.command_name)
        elif args.command == "env-role":
            print(env_role(args.env_dir))
        elif args.command == "maintain-check":
            last = maintain_check(args.env_dir, args.env)
            print(f"maintain: HEAD is the last released commit {last['commit'][:12]} (digests match).")
    except Refused as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # an unreadable store or a git failure never passes
        print(f"ERROR: {args.command} failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
