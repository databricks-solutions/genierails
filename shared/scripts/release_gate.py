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
    and asks for a fresh release-plan if any of them changed, if the manifest
    was already released, or if a newer release-plan superseded it.

current-manifest / manifest-summary
    manifest-write records the manifest it wrote as the env's current one
    (manifests/CURRENT, the manifest id; a newer release-plan replaces it).
    ``make release ENV=<env>`` with no MANIFEST= releases the current manifest
    after printing its path, commit and change summary; MANIFEST=<path> is the
    explicit override pipelines use.

release-receipt / maintain-check
    A finished release writes a receipt (commit, digests, manifest, and each
    Genie agent's ID and detach state, which detach-agent and ID recovery read);
    ``make maintain`` refuses unless HEAD is that commit with matching digests.
    Once the receipt (or an applied phase's record) is written, the manifest's
    saved plans are deleted: they carry the deployer's credentials, and nothing
    reads them after (the manifest keeps their sha256). A failed or unreleased
    manifest keeps them, read-only.

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

from state_layout import data_access_subdir, root_state_paths, state_json  # noqa: E402

STATE_DIR_ENV = "GENIERAILS_STATE_DIR"
DEFAULT_STATE = ".genierails-state"
# manifests/CURRENT: the id of the manifest a plain make release ENV=<env> releases.
CURRENT_MANIFEST = "CURRENT"
VERSION = 1
# Files under envs/<env> that applying commands write: never inputs.
RUNTIME_FILES = {
    "auth.auto.tfvars", "abac.auto.tfvars", "classification.auto.tfvars",
    "discovered_uc_tables.auto.tfvars", "ddl/_fetched.sql",
    "generated/genie_space_derived_acl_groups.auto.tfvars",
}
# AI-helper drafts (scripts/ai_helpers.py) are committed for review but are not
# inputs: nothing that applies reads them, so they never change a fingerprint.
DRAFTS_DIR = "drafts"
STATE_SUFFIXES = (".tfstate", ".tfstate.backup", ".tfplan")
# What promote-to writes into the destination's generated/ (design section 9:
# it writes only the files it owns).
PROMOTE_OWNED = ("abac.auto.tfvars", "masking_functions.sql", "expected_classification.json")
CODE_ROOTS = ("shared",)
CODE_SKIP = ("shared/docs/", "shared/tests/")
# Terraform layer names (state_json layers, manifest keys), not directories:
# the data_access layer's directory is data_access_subdir().
ROOTS = ("account", "data_access", "workspace")
ACCOUNT_ENV = "account"


def runtime_dirs() -> set[str]:
    """Env subdirectories that are runtime, not inputs: the data_access layer's."""
    return {data_access_subdir()}


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


def any_deterministic_env(account_env_dir: Path) -> bool:
    """Fail-closed scan of workspace envs sharing an account layer."""
    envs_dir = Path(account_env_dir).resolve().parent
    reserved = {ACCOUNT_ENV, data_access_subdir()}
    for path in sorted(envs_dir.glob("*/env.auto.tfvars")):
        if path.parent.name in reserved:
            continue
        try:
            config = _load_hcl(path)
        except Exception as exc:
            raise Refused(f"cannot determine whether deterministic governance is in use: {path} is invalid: {exc}") from exc
        if config.get("governance_mode", "legacy") == "deterministic":
            return True
    return False


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
    if not parts or parts[0] in runtime_dirs() or parts[0] == DRAFTS_DIR or str(rel) in RUNTIME_FILES:
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
        if name in (ACCOUNT_ENV, data_access_subdir(), source):
            continue
        promote_from = str(_load_hcl(env_file).get("promote_from") or ("dev" if name == "prod" else ""))
        if promote_from == source:
            targets.append(name)
    return targets


def identity_check(env_dir: Path, env: str) -> None:
    """Refuse when ENV or an env promoted from it maps a catalog to itself (S2).

    A rehearsal receipt vouches for the commit for every promotion target, so a
    hand-edited target catalog_map is refused before the rehearsal locks or applies.
    """
    from scripts.saved_settings import env_identity_catalog_map_error

    env_dir = Path(env_dir)
    cloud_dir = cloud_dir_of(env_dir)
    files = [env_dir / "env.auto.tfvars"] + [
        cloud_dir / "envs" / target / "env.auto.tfvars" for target in promotion_targets(cloud_dir, env)]
    for env_file in files:
        error = env_identity_catalog_map_error(env_file)
        if error:
            raise Refused(error)


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
    _print_unclean(status)
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
    _print_unclean(status)
    if result == "passed":
        _print_targets(receipt["targets"])
    else:
        print(f"  rehearse failed: {reason or 'see above'}", file=sys.stderr)
    return receipt | {"_path": str(path)}


def _print_unclean(status: Mapping) -> None:
    if not status["clean_tree"]:
        print("  NOT a clean checkout; release-plan ignores this receipt:")
        for line in status["dirty"][:5]:
            print(f"  {line}")
        print("  A release needs a passing rehearsal of a clean checkout of the commit it releases: "
              "commit or remove these, then rehearse again.")


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
    source_dir = cloud_dir / "envs" / source
    if not (source_dir / "env.auto.tfvars").is_file():
        problems.append(f"envs/{source}/env.auto.tfvars was not found; fix promote_from in envs/{env}/env.auto.tfvars")
    elif deterministic(env_dir) and not deterministic(source_dir):
        problems.append(
            f"envs/{source} uses legacy governance but envs/{env} is deterministic; migrate the source first "
            f'(shared/docs/legacy-migration.md), or set governance_mode = "legacy" in envs/{env}/env.auto.tfvars'
        )
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
        unclean = [r for r in rehearse_receipts(cloud_dir, source)
                   if r.get("commit") == commit and r.get("result") == "passed" and r.get("clean_tree") is not True]
        problems.append(f"no passing rehearsal of {commit[:12]} (a clean checkout): run make rehearse "
                        f"ENV={source} on this commit first (receipts: {rehearse_dir(cloud_dir, source)})"
                        + (f"; {len(foreign)} receipt(s) for it were written by another store (a downloaded "
                           "CI artifact or a copy) and are informational only" if foreign else ""))
        if unclean:
            problems.append("a receipt exists but its checkout wasn't clean: "
                            + "; ".join(unclean[-1].get("dirty") or ["(no details recorded)"])
                            + f". Commit or remove these, then re-run make rehearse ENV={source}")
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
                            f"(stale promotion): run make promote-to ENV={env}, open a PR, then make rehearse "
                            f"ENV={source} on its merge commit and re-run make release-plan ENV={env}")
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


def account_provenance(env_dir: Path, env: str = ACCOUNT_ENV, *, remote: str = "origin", branch: str = "main",
                       fetch: bool = True) -> dict:
    """Everything account-plan/apply-account require of the checkout; raises Refused."""
    env_dir = Path(env_dir)
    status = checkout_status(env_dir)
    repo, commit = Path(status["repo"]), status["commit"]
    problems = []
    if env != ACCOUNT_ENV:
        problems.append(f"account operations use ENV={ACCOUNT_ENV}, not {env}")
    if not status["clean_tree"]:
        problems.append("the checkout is not clean:\n      " + "\n      ".join(status["dirty"][:10]))
    if fetch:
        git(repo, "fetch", "--quiet", remote, branch)
    upstream = f"{remote}/{branch}"
    upstream_commit = ""
    if not commit_known(repo, upstream):
        problems.append(f"{upstream} is unknown; fetch it first")
    else:
        upstream_commit = git(repo, "rev-parse", upstream).stdout.strip()
        if commit != upstream_commit:
            problems.append(f"HEAD {commit[:12]} is not exactly {upstream} ({upstream_commit[:12]}); "
                            "account operations run only from the current main commit")
    if problems:
        raise Refused("account operation refused:\n  - " + "\n  - ".join(problems))
    return {"commit": commit, "remote": upstream, "remote_commit": upstream_commit, "checked_at": _now()}


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

        return DatabricksSource(
            warehouse_id, client=_get_sdk_client(Path(env_dir) / data_access_subdir()))
    return build


# -- live snapshot --------------------------------------------------------------

def live_policies(env_dir: Path, catalogs: Iterable[str], lister=None) -> dict[str, list[str]]:
    from remap_generated_config import live_policy_lister

    lister = lister or live_policy_lister(
        Path(env_dir) / data_access_subdir() / "auth.auto.tfvars")
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


def current_pointer(cloud_dir: Path, env: str) -> Path:
    return manifests_dir(cloud_dir, env) / CURRENT_MANIFEST


def current_manifest_id(cloud_dir: Path, env: str) -> str | None:
    """The id of the manifest the last release-plan wrote; None before any did."""
    pointer = _read_json(current_pointer(cloud_dir, env))
    if pointer is None:
        return None
    value = pointer.get("id")
    if not isinstance(value, str) or not value or "/" in value or value.startswith("."):
        raise Refused(f"{current_pointer(cloud_dir, env)} names no manifest; run make release-plan ENV={env} again")
    return value


def _set_current(cloud_dir: Path, env: str, manifest_id: str, created: str) -> None:
    """Point the env at its newest manifest (replaced, never appended: it supersedes the previous one)."""
    pointer = current_pointer(cloud_dir, env)
    tmp = pointer.with_name(f".{pointer.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps({"id": manifest_id, "created_at": created}, indent=2, sort_keys=True) + "\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, pointer)


def released_by(cloud_dir: Path, env: str, manifest_id: str) -> str | None:
    """The receipt (release or applied phase) that already released this manifest, if any."""
    for receipt in release_receipts(cloud_dir, env):
        if (receipt.get("manifest") or {}).get("id") == manifest_id:
            return receipt["_path"]
    for record in _receipts(env_state(cloud_dir, env) / "receipts" / "phases"):
        if record.get("kind") == "release-phase" and record.get("id") == manifest_id:
            return record["_path"]
    return None


def check_releasable(env_dir: Path, env: str, manifest_path: Path, manifest: Mapping) -> None:
    """Refuse a manifest that was already released or that a newer release-plan superseded."""
    cloud_dir = cloud_dir_of(env_dir)
    receipt = released_by(cloud_dir, env, manifest["id"])
    if receipt:
        raise Refused(f"make release refused: {manifest_path} was already released ({receipt}). Nothing was "
                      f"applied. Run make release-plan ENV={env} for a new manifest, then make release ENV={env}.")
    current = current_manifest_id(cloud_dir, env)
    if current is not None and current != manifest["id"]:
        newer = manifests_dir(cloud_dir, env) / current / "manifest.json"
        raise Refused(f"make release refused: {manifest_path} was superseded by a newer make release-plan "
                      f"({newer}). Nothing was applied. Review and release the current one with make release "
                      f"ENV={env}, or run make release-plan ENV={env} again.")


def current_manifest(env_dir: Path, env: str) -> Path:
    """The env's current manifest for a plain make release; refuses when there is none to release."""
    cloud_dir = cloud_dir_of(env_dir)
    current = current_manifest_id(cloud_dir, env)
    if current is None:
        raise Refused(f"make release refused: ENV={env} has no current release manifest. Run make release-plan "
                      f"ENV={env}, get the manifest it writes approved, then make release ENV={env}.")
    path = manifests_dir(cloud_dir, env) / current / "manifest.json"
    if not path.is_file():
        raise Refused(f"make release refused: the current manifest {path} is missing. Run make release-plan "
                      f"ENV={env} again.")
    check_releasable(env_dir, env, path, load_manifest(path, env))
    return path


def manifest_summary(manifest_path: Path, manifest: Mapping) -> list[str]:
    """What a manifest applies, from its own records (printed before a plain make release applies it)."""
    plans = manifest.get("plans") or {}
    lines = [f"  Manifest: {manifest_path}",
             f"  Commit:   {manifest.get('commit')} (planned {manifest.get('created_at')} on {manifest.get('host')})",
             "  Changes:"]
    imports = (manifest.get("imports") or {}).get("count") or 0
    if imports:
        lines.append(f"    imports:      {imports} recorded import(s)/stale-tag cleanup(s), and nothing else")
    steps = (plans.get("data_access") or {}).get("steps") or []
    changing = [str(s.get("phase")) for s in steps if s.get("kind") == "apply" and s.get("changes")]
    if steps and [s.get("kind") for s in steps] == ["adopt"]:
        lines.append("    data_access:  adopt the legacy policies (one staged phase)")
    elif not imports:
        what = f"saved plan(s) with changes, phase(s) {', '.join(changing)}" if changing else "no changes"
        lines.append(f"    data_access:  {what}"
                     + ("" if manifest.get("final") else " (one phase of a staged policy change)"))
    workspace = plans.get("workspace") or {}
    lines.append("    workspace:    " + (f"not planned ({workspace['skipped']})" if workspace.get("skipped")
                                       else "saved plan with changes" if workspace.get("changes") else "no changes"))
    agents = ((manifest.get("agents") or {}).get("agents") or {}).values()
    actions = [a.get("action") for a in agents]
    if actions:
        lines.append(f"    Genie agents: {actions.count('push')} pushed, {actions.count('delete')} deleted, "
                     f"{len(actions) - actions.count('push') - actions.count('delete')} not pushed")
    report = manifest.get("reports") or {}
    acked = len((report.get("weakening") or {}).get("acknowledged") or []) \
        + len((report.get("revocations") or {}).get("acknowledged") or [])
    newly = len((report.get("readers") or {}).get("tables") or [])
    if acked or newly:
        lines.append(f"    reviewed:     {acked} acknowledged weakening(s)/revocation(s), "
                     f"{newly} newly governed table(s)")
    return lines


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


def account_state_serial(env_dir: Path) -> dict | None:
    try:
        state = state_json(Path(env_dir), ACCOUNT_ENV, "account")
    except RuntimeError as exc:
        raise Refused(str(exc)) from exc
    return None if state is None else {"serial": state.get("serial"), "lineage": state.get("lineage")}


ACCOUNT_RUNTIME_INPUTS = ("abac.auto.tfvars", "classification.auto.tfvars", "discovered_uc_tables.auto.tfvars")
ACCOUNT_AUTH_ENDPOINT_KEYS = (
    "databricks_account_host", "databricks_account_id", "databricks_workspace_host",
)
DEFAULT_DATABRICKS_ACCOUNT_HOST = "https://accounts.cloud.databricks.com"


def _account_auth_endpoint_hash(env_dir: Path) -> dict | None:
    """Hash only the non-secret auth fields that select the account REST endpoint."""
    auth = Path(env_dir) / "auth.auto.tfvars"
    if not auth.exists() and not auth.is_symlink():
        return None
    try:
        resolved = auth.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise Refused(f"account auth endpoint is not pinned: {auth} is not a readable file: {exc}") from exc
    envs_dir = Path(env_dir).resolve().parent
    try:
        resolved.relative_to(envs_dir)
    except ValueError as exc:
        raise Refused(f"account auth endpoint is not pinned: {auth} resolves outside {envs_dir}") from exc
    if not resolved.is_file():
        raise Refused(f"account auth endpoint is not pinned: {auth} does not resolve to a file")
    try:
        values = _load_hcl(resolved)
    except Exception:
        values = None
    if values is None:
        # Raised outside the handler with no parser text, which can echo a
        # malformed secret (also via __context__).
        raise Refused(f"account auth endpoint is not pinned: {auth} is not valid HCL") from None
    endpoint = {name: values.get(name) for name in ACCOUNT_AUTH_ENDPOINT_KEYS}
    endpoint["databricks_account_host"] = (
        endpoint["databricks_account_host"] or DEFAULT_DATABRICKS_ACCOUNT_HOST
    )
    missing = [name for name in ("databricks_account_id", "databricks_workspace_host")
               if not endpoint.get(name)]
    if missing:
        raise Refused("account auth endpoint is not pinned: auth.auto.tfvars is missing non-empty "
                      + ", ".join(missing))
    return {"path": "auth.auto.tfvars#databricks_account_host,databricks_account_id,databricks_workspace_host",
            "sha256": _json_digest(endpoint)}


def account_input_hashes(env_dir: Path) -> list[dict]:
    """Account-layer inputs that are not protected by git cleanliness.

    Only the effective ``databricks_account_host``, ``databricks_account_id``
    and ``databricks_workspace_host`` are represented from
    ``auth.auto.tfvars``; their canonical digest pins both Terraform and REST
    destinations without hashing or persisting any credential field.
    """
    env_dir = Path(env_dir)
    files = {p.resolve() for p in env_input_files(env_dir)}
    files.update((env_dir / name).resolve() for name in ACCOUNT_RUNTIME_INPUTS if (env_dir / name).is_file())
    result = [
        {"path": os.path.relpath(path, env_dir), "sha256": sha256_file(path)}
        for path in sorted(files)
        if path.is_file() and not path.is_symlink() and path.name != "auth.auto.tfvars"
    ]
    endpoint = _account_auth_endpoint_hash(env_dir)
    if endpoint:
        result.append(endpoint)
    return sorted(result, key=lambda item: item["path"])


def canonical_removals(text: str | list | tuple) -> list[str]:
    """``ALLOW_TAG_VALUE_REMOVAL`` as sorted unique ``key:value`` items (deterministic)."""
    items = text.split(",") if isinstance(text, str) else list(text)
    result = set()
    for item in (str(i).strip() for i in items):
        if not item:
            continue
        key, sep, value = item.partition(":")
        if not sep or not key.strip() or not value.strip():
            raise Refused(f"ALLOW_TAG_VALUE_REMOVAL item {item!r} is not <key>:<value>")
        result.add(f"{key.strip()}:{value.strip()}")
    return sorted(result)


def account_tag_sync_diff(env_dir: Path, allowed: list[str] = (), warehouse_id: str = "") -> list[dict]:
    """What make apply-account's tag sync will do: add missing values, remove only ``allowed``.

    The sync script refuses (and so does this) when an allowed removal is not
    live, still desired, a deterministic generation, in use, or of unknown use.
    """
    # Validate and pin the auth endpoint before invoking the REST-oriented tag
    # diff, so a missing routing key is reported by name rather than as JSON.
    account_input_hashes(env_dir)
    script = SHARED / "scripts" / "sync_tag_policies.py"
    extra = [arg for item in allowed for arg in ("--remove", item)]
    if allowed:
        extra += ["--warehouse-id", warehouse_id]
    result = subprocess.run(
        [sys.executable, str(script), "--mode", "add-only", "--dry-run", "--json", *extra,
         str(Path(env_dir) / "abac.auto.tfvars")],
        cwd=env_dir, text=True, capture_output=True, check=False,
    )
    if result.returncode:
        message = (result.stderr or result.stdout).strip()
        raise Refused(f"the account tag-policy diff could not be computed"
                      + (f":\n{message}" if message else ""))
    try:
        value = json.loads(result.stdout or "[]")
    except ValueError as exc:
        raise Refused("the account tag-policy diff was not valid JSON") from exc
    if not isinstance(value, list):
        raise Refused("the account tag-policy diff was not a JSON list")
    return value


def tag_sync_changes(diff) -> list[dict]:
    """The part of a tag diff a manifest approves: per key, values added and removed.

    Values left in place (live, not desired by this checkout) are reported but
    never written, so another repo adding or retagging its own values doesn't
    invalidate an approved manifest.
    """
    return [{"key": e.get("key"), "added": list(e.get("added") or []), "removed": list(e.get("removed") or [])}
            for e in diff or [] if e.get("added") or e.get("removed")]


def tag_removals(diff) -> list[str]:
    return sorted(f"{e.get('key')}:{v}" for e in diff or [] for v in e.get("removed") or [])


def require_no_tag_policy_deletes(data: Mapping | None) -> None:
    """Refuse an account plan that deletes (or replaces) a tag policy.

    Terraform ignores a tag policy's values, but deleting the policy drops every
    value of its key for every workspace on the metastore, with none of the
    usage checks a named removal gets. Unreadable plan JSON refuses too.
    """
    if data is None:
        raise Refused("the account plan could not be read as terraform show -json output; nothing was applied")
    keys = sorted(
        str(((item.get("change") or {}).get("before") or {}).get("tag_key") or item.get("address"))
        for item in data.get("resource_changes") or []
        if item.get("type") == "databricks_tag_policy" and "delete" in ((item.get("change") or {}).get("actions") or [])
    )
    if keys:
        raise Refused(
            f"the account plan deletes tag polic{'y' if len(keys) == 1 else 'ies'} {keys}: that removes every value "
            "of the key from every workspace on the metastore, with no usage check, so it is refused and nothing "
            "was applied. Keep the key in tag_policies; remove values you no longer use with make account-plan "
            "ALLOW_TAG_VALUE_REMOVAL=<key>:<value>,... WAREHOUSE_ID=<id> (each is checked unused) and make "
            "apply-account.")


def _plan_summary_data(data: Mapping | None, *, changes: bool) -> dict:
    summary = {"changes": bool(changes)}
    if data is None:
        return summary | {"resource_changes": {"unknown": int(bool(changes))}}
    counts: dict[str, int] = {}
    for item in data.get("resource_changes") or []:
        actions = (item.get("change") or {}).get("actions") or []
        key = ",".join(str(a) for a in actions) if actions else "no-op"
        counts[key] = counts.get(key, 0) + 1
    summary["resource_changes"] = counts
    return summary


def _plan_summary(show_json: Path, *, changes: bool) -> dict:
    return _plan_summary_data(_read_json(show_json), changes=changes)


def account_manifest_write(env_dir: Path, env: str, plan_dir: Path, prov: Mapping) -> Path:
    env_dir, plan_dir = Path(env_dir), Path(plan_dir)
    status = checkout_status(env_dir)
    if status["commit"] != prov.get("commit") or not status["clean_tree"]:
        raise Refused("the checkout changed while account-plan ran; nothing was recorded. Re-run account-plan")
    info = _read_json(plan_dir / "account.json")
    if info is None:
        raise Refused("account-plan didn't record the account plan")
    plan = plan_dir / str(info.get("file", ""))
    if not plan.is_file():
        raise Refused(f"the saved account plan {plan} is missing")
    summary = _read_json(plan_dir / "account.summary.json")
    if summary is None:
        raise Refused("account-plan didn't record the account plan summary")
    tag_sync = _read_json(plan_dir / "tag_sync.json")
    if tag_sync is None or not isinstance(tag_sync.get("diff"), list):
        raise Refused("account-plan didn't record the account tag-policy diff")
    allowed = canonical_removals(tag_sync.get("allowed") or [])
    # The live key may carry a Terraform _<hex> suffix, so compare the values.
    if sorted(r.partition(":")[2] for r in tag_removals(tag_sync["diff"])) != \
            sorted(a.partition(":")[2] for a in allowed):
        raise Refused("account-plan's tag diff doesn't remove exactly the values ALLOW_TAG_VALUE_REMOVAL names")
    created = _now()
    manifest_id = f"{created.replace(':', '').replace('.', '').replace('-', '')}-{prov['commit'][:12]}-account"
    final_dir = manifests_dir(cloud_dir_of(env_dir), env) / manifest_id
    plan_entry = {"changes": bool(info.get("changes")), "summary": summary}
    if account_plan_noop(plan_entry, tag_sync["diff"]):
        # No later operation applies or otherwise reads a no-op account plan.
        # Keep the immutable result, but not the credential-bearing plan file.
        plan.unlink()
    else:
        plan_entry |= {"file": Path(info["file"]).name, "sha256": sha256_file(plan)}
    manifest = {
        "kind": "account-manifest", "version": VERSION, "id": manifest_id, "env": env,
        "cloud": cloud_dir_of(env_dir).name, "commit": prov["commit"], "provenance": dict(prov),
        # account-plan does not lock state. Terraform's saved-plan stale-state
        # check is the final backstop if state changes after this serial read.
        "state_serial": account_state_serial(env_dir),
        "plan": plan_entry,
        "inputs": account_input_hashes(env_dir),
        "tag_sync": tag_sync["diff"],
        # Approving the manifest approves exactly these removals; apply-account
        # passes them (and only them) to the sync, which re-checks their usage.
        "tag_value_removal": {"allowed": allowed, "allowed_arg": ",".join(allowed),
                              "warehouse_id": str(tag_sync.get("warehouse_id") or "") if allowed else "",
                              "removed": tag_removals(tag_sync["diff"])},
        "host": socket.gethostname(), "created_at": created,
    }
    _write_once(plan_dir / "manifest.json", manifest)
    for path in plan_dir.rglob("*"):
        os.chmod(path, 0o400 if path.is_file() else 0o700)
    os.rename(plan_dir, final_dir)
    drop_superseded_saved_plans(env_dir, env, final_dir / "manifest.json")
    return final_dir / "manifest.json"


def account_plan_noop(plan: Mapping, tag_diff) -> bool:
    """True when applying the account manifest would change nothing.

    The tag policies ignore their values in Terraform, so a plan whose only
    effect is adding or (approved) removing tag values has no Terraform changes;
    apply-account makes those changes in its tag sync after the plan. Only a
    manifest with neither is a no-op.
    """
    return not plan.get("changes") and not tag_sync_changes(tag_diff)


def account_manifest_noop(manifest: Mapping) -> bool:
    return account_plan_noop(manifest.get("plan") or {}, manifest.get("tag_sync"))


def load_account_manifest(path: Path, env: str) -> dict:
    manifest = _read_json(Path(path))
    if manifest is None or manifest.get("kind") != "account-manifest" or manifest.get("version") != VERSION:
        raise Refused(f"{path} is not an account manifest")
    if manifest.get("env") != env:
        raise Refused(f"{path} is a manifest for {manifest.get('env')}, not {env}")
    return manifest


def account_manifest_used(cloud_dir: Path, env: str, manifest_id: str) -> bool:
    return any(((r.get("manifest") or {}).get("id") == manifest_id) for r in account_receipts(cloud_dir, env))


def account_manifest_paths(cloud_dir: Path, env: str = ACCOUNT_ENV) -> list[Path]:
    return sorted(manifests_dir(cloud_dir, env).glob("*/manifest.json"))


def newer_account_manifest(cloud_dir: Path, env: str, manifest: Mapping) -> Path | None:
    """Newest unreceipted account manifest written after ``manifest``, if any."""
    older_key = (str(manifest.get("created_at") or ""), str(manifest.get("id") or ""))
    newer = []
    for path in account_manifest_paths(cloud_dir, env):
        candidate = _read_json(path)
        if (candidate and candidate.get("kind") == "account-manifest" and candidate.get("version") == VERSION
                and candidate.get("env") == env
                and (str(candidate.get("created_at") or ""), str(candidate.get("id") or "")) > older_key
                and not account_manifest_used(cloud_dir, env, str(candidate.get("id") or ""))):
            newer.append(path)
    return newer[-1] if newer else None


def select_account_manifest(env_dir: Path, env: str) -> Path:
    status = checkout_status(env_dir)
    cloud_dir = cloud_dir_of(env_dir)
    candidates = []
    for path in account_manifest_paths(cloud_dir, env):
        manifest = _read_json(path)
        entry = (manifest or {}).get("plan") or {}
        plan = path.parent / str(entry.get("file") or "")
        if (manifest and manifest.get("kind") == "account-manifest" and manifest.get("version") == VERSION
                and manifest.get("env") == env and manifest.get("commit") == status["commit"]
                and not account_manifest_noop(manifest)
                and entry.get("file") == "account.tfplan" and plan.is_file()
                and not account_manifest_used(cloud_dir, env, str(manifest.get("id") or ""))):
            candidates.append((path, manifest))
    if len(candidates) != 1:
        ids = [f"{m.get('id')} ({p})" for p, m in candidates]
        if not candidates:
            raise Refused("make apply-account found no unreceipted account manifest for HEAD "
                          "(a manifest that changes nothing is never selected). Run make account-plan, get its manifest approved, then run make apply-account.")
        raise Refused("make apply-account found multiple unreceipted account manifests for HEAD; "
                      "rerun with MANIFEST=<path> for one of:\n  - " + "\n  - ".join(ids))
    return candidates[0][0]


def account_manifest_check(env_dir: Path, env: str, manifest_path: Path, *, remote: str = "origin",
                           branch: str = "main", allow_removal: str = "") -> dict:
    env_dir, manifest_path = Path(env_dir), Path(manifest_path)
    manifest = load_account_manifest(manifest_path, env)
    plan_dir = manifest_path.parent
    drift = []
    status = checkout_status(env_dir)
    if status["commit"] != manifest.get("commit"):
        drift.append(f"HEAD is {status['commit'][:12]}, the manifest planned {manifest.get('commit', '')[:12]}")
    try:
        prov = account_provenance(env_dir, env, remote=remote, branch=branch)
    except Refused as exc:
        drift.append(str(exc))
        prov = {}
    if manifest.get("cloud") != cloud_dir_of(env_dir).name:
        drift.append(f"the manifest is for {manifest.get('cloud')}, not {cloud_dir_of(env_dir).name}")
    if account_manifest_used(cloud_dir_of(env_dir), env, str(manifest.get("id") or "")):
        drift.append("the manifest already has an account receipt; account manifests are single-use")
    serial = account_state_serial(env_dir)
    if serial != manifest.get("state_serial"):
        drift.append(f"the account state changed since account-plan (serial "
                     f"{(manifest.get('state_serial') or {}).get('serial')} -> {(serial or {}).get('serial')})")
    entry = manifest.get("plan") or {}
    # A newer no-op manifest supersedes too: account-plan is the latest word on the account.
    newer = newer_account_manifest(cloud_dir_of(env_dir), env, manifest)
    if newer:
        drift.append(f"the manifest was superseded by a newer account-plan ({newer})")
    if account_manifest_noop(manifest):
        drift.append("the manifest records no account changes (no Terraform changes and no tag values to "
                     "add or remove), so it kept no saved plan and has nothing to apply")
    elif entry.get("file") != "account.tfplan":
        drift.append(f"the manifest's account plan is {entry.get('file')!r}, not account.tfplan")
    else:
        path = plan_dir / entry["file"]
        if path.is_file() and sha256_file(path) != entry.get("sha256"):
            drift.append(f"the saved account plan {path} changed or is missing")
        elif not path.is_file() and not newer:
            drift.append(f"the saved account plan {path} changed or is missing")
    current_inputs = account_input_hashes(env_dir)
    if current_inputs != manifest.get("inputs"):
        drift.append("the account input file hashes changed since account-plan")
    removal = manifest.get("tag_value_removal")
    if not isinstance(removal, dict):
        drift.append("the manifest records no approved tag-value removals (planned before removals were gated)")
        removal = {}
    allowed = list(removal.get("allowed") or [])
    if allow_removal.strip() and canonical_removals(allow_removal) != allowed:
        drift.append(f"ALLOW_TAG_VALUE_REMOVAL is set to {canonical_removals(allow_removal)} but the manifest "
                     f"approved {allowed}; removals are chosen at account-plan, not at apply-account")
    try:
        current_tag_sync = account_tag_sync_diff(env_dir, allowed, str(removal.get("warehouse_id") or ""))
    except Refused as exc:
        drift.append(str(exc))
    else:
        planned, live = tag_sync_changes(manifest.get("tag_sync")), tag_sync_changes(current_tag_sync)
        if planned != live:
            drift.append("the account tag-policy diff changed since account-plan: planned removals "
                         f"{tag_removals(planned)}, additions {sorted(v for e in planned for v in e['added'])}; "
                         f"live removals now {tag_removals(live)}, additions "
                         f"{sorted(v for e in live for v in e['added'])}")
    if drift:
        raise Refused(f"make apply-account refused: {manifest_path} no longer matches:\n  - "
                      + "\n  - ".join(drift)
                      + "\nNothing was applied. Run make account-plan again and approve the new manifest.")
    return manifest


def require_account_lock(env_dir: Path) -> None:
    from scripts.environment_lock import held_by_ancestor

    owner = held_by_ancestor(Path(env_dir))
    if owner == "apply-account":
        return
    raise Refused("make sync-tags TAG_SYNC_REMOVE=... refused: tag-value removals may run only under the "
                  "live account lock held by make apply-account, from the removals its approved manifest names.")


def account_manifest_summary(manifest_path: Path, manifest: Mapping) -> list[str]:
    """What an account manifest applies, tag-value removals first and loudest."""
    plan = manifest.get("plan") or {}
    counts = (plan.get("summary") or {}).get("resource_changes") or {}
    diff = manifest.get("tag_sync") or []
    lines = [f"  Manifest: {manifest_path}",
             f"  Commit:   {manifest.get('commit')} (planned {manifest.get('created_at')} on {manifest.get('host')})",
             "  Account plan: " + (", ".join(f"{n} {a}" for a, n in sorted(counts.items()))
                                   if plan.get("changes") else "no changes")]
    removed = [(e.get("key"), e.get("removed")) for e in diff if e.get("removed")]
    if removed:
        lines.append("  !! TAG VALUES THIS MANIFEST REMOVES (from every workspace on the metastore):")
        for key, values in removed:
            lines.append(f"  !!   {key}: {values}")
        check = next((e.get("removal_check") for e in diff if e.get("removal_check")), None) or {}
        if check:
            lines.append(f"  !!   checked unused: no tag assignment in system.information_schema (warehouse "
                         f"{check.get('warehouse_id')}), no ABAC policy reference on {check.get('policy_securables')} "
                         f"catalog/schema/table securable(s) in {check.get('catalogs')} catalog(s); re-checked "
                         "under the account lock before removal")
    else:
        lines.append("  Tag values removed: none")
    for e in diff:
        if e.get("added"):
            lines.append(f"  Tag values added: {e.get('key')}: {e.get('added')}")
    for e in diff:
        if e.get("left_in_place"):
            lines.append(f"  Tag values left in place (live, not desired here; never removed): {e.get('key')}: "
                         f"{len(e['left_in_place'])} value(s) {e['left_in_place']}")
    return lines


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
                      "Run make account-plan, get its manifest approved, then run make apply-account "
                      "in the operations stage first")
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
    _set_current(cloud_dir_of(env_dir), env, manifest_id, created)  # supersedes the previous manifest
    drop_superseded_saved_plans(env_dir, env, final_dir / "manifest.json")
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
    # A released manifest's saved plans were deleted after its receipt (drop_saved_plans):
    # check_releasable below reports it as released, not as missing its plans.
    released = released_by(cloud_dir_of(env_dir), env, manifest["id"])
    current = current_manifest_id(cloud_dir_of(env_dir), env)
    superseded = current is not None and current != manifest["id"]
    for root, path, digest in _saved_plans(manifest_path, manifest):
        # Successful releases and newer manifests deliberately remove these.
        # Preserve the stronger released/superseded refusal below rather than
        # misreporting the expected cleanup as drift.
        if (released or superseded) and not path.exists():
            continue
        if not path.is_file() or sha256_file(path) != digest:
            drift.append(f"the saved {root} plan {path} changed or is missing")
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
    # After the drift checks, so a changed manifest is always reported as changed.
    check_releasable(env_dir, env, manifest_path, manifest)
    return manifest


def _saved_plans(manifest_path: Path, manifest: Mapping) -> list[tuple[str, Path, str | None]]:
    """(root, path, sha256) of each saved plan a release manifest names."""
    plan_dir = Path(manifest_path).parent
    plans = [(root, plan_dir / manifest["plans"][root]["file"], manifest["plans"][root].get("sha256"))
             for root in ("account", "workspace") if manifest["plans"][root].get("file")]
    return plans + [("data_access", plan_dir / "data_access" / step["file"], step.get("sha256"))
                    for step in manifest["plans"]["data_access"].get("steps") or [] if step.get("file")]


def drop_superseded_saved_plans(env_dir: Path, env: str, current_path: Path) -> list[Path]:
    """Delete only the credential-bearing plans superseded by ``current_path``.

    The caller has completed the new immutable manifest and holds the env lock,
    so an older unreceipted manifest cannot be entering apply concurrently.
    Manifests and all non-plan audit records remain append-only.
    """
    env_dir, current_path = Path(env_dir), Path(current_path)
    left = []
    try:
        current = _read_json(current_path) or {}
        kind = current.get("kind")
        current_key = (str(current.get("created_at") or ""), str(current.get("id") or ""))
        cloud_dir = cloud_dir_of(env_dir)
        for manifest_path in sorted(manifests_dir(cloud_dir, env).glob("*/manifest.json")):
            try:
                if manifest_path == current_path:
                    continue
                manifest = _read_json(manifest_path)
                if not manifest or manifest.get("kind") != kind or manifest.get("env") != env:
                    continue
                # A cleanup may only move backward in manifest time. The env
                # lock serializes writers, but this guard also protects a
                # restored/copied future manifest from being treated as old.
                manifest_key = (str(manifest.get("created_at") or ""), str(manifest.get("id") or ""))
                if manifest_key >= current_key:
                    continue
                manifest_id = str(manifest.get("id") or "")
                if kind == "release-manifest":
                    if released_by(cloud_dir, env, manifest_id):
                        continue
                    paths = [path for _, path, _ in _saved_plans(manifest_path, manifest)]
                elif kind == "account-manifest":
                    if account_manifest_used(cloud_dir, env, manifest_id):
                        continue
                    entry = manifest.get("plan") or {}
                    paths = [manifest_path.parent / entry["file"]] if entry.get("file") else []
                else:
                    continue
                for path in paths:
                    try:
                        path.unlink(missing_ok=True)
                    except Exception:
                        print(f"WARNING: superseded saved plan {path} contains credentials and could not be "
                              "deleted; delete it by hand.", file=sys.stderr)
                        left.append(path)
            except Exception:
                print(f"WARNING: superseded saved-plan cleanup failed for {manifest_path}", file=sys.stderr)
                continue
    except Exception:
        print(f"WARNING: superseded saved-plan cleanup failed for {current_path}", file=sys.stderr)
    return left


def drop_saved_plans(env_dir: Path, env: str, manifest_path: Path) -> list[Path]:
    """Delete a released manifest's saved plans; returns those that couldn't be deleted.

    A saved plan carries the deployer's auth.auto.tfvars (client secret).
    Only a manifest with a release receipt or an applied phase's record
    (released_by) loses them, so a failed or interrupted release keeps the
    plans it was approved with. Nothing reads them afterwards: a released
    manifest is never released again, and the manifest keeps each plan's
    sha256. A plan that can't be deleted is reported, not fatal: the apply
    and its receipt already happened.
    """
    manifest = load_manifest(Path(manifest_path), env)
    if not released_by(cloud_dir_of(Path(env_dir)), env, manifest["id"]):
        raise Refused(f"{manifest_path} has no release receipt or phase record; its saved plans are kept")
    left = []
    for _, path, _ in _saved_plans(manifest_path, manifest):
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            print(f"WARNING: the release was applied and its receipt was written, but the saved plan {path} "
                  f"contains credentials and could not be deleted ({exc.strerror}); delete it.", file=sys.stderr)
            left.append(path)
    return left


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


def account_receipt(env_dir: Path, env: str, manifest_path: Path) -> Path:
    env_dir, manifest_path = Path(env_dir), Path(manifest_path)
    manifest = load_account_manifest(manifest_path, env)
    cloud_dir = cloud_dir_of(env_dir)
    status = checkout_status(env_dir)
    receipt = {
        "kind": "account", "version": VERSION, "env": env, "cloud": cloud_dir.name,
        "commit": manifest["commit"], "clean_tree": status["clean_tree"],
        "manifest": {"id": manifest["id"], "path": str(manifest_path), "sha256": sha256_file(manifest_path)},
        "plan": manifest["plan"], "state_serial": account_state_serial(env_dir),
        "host": socket.gethostname(), "created_at": _now(),
    }
    stamp = receipt["created_at"].replace(":", "").replace(".", "")
    return _write_once(env_state(cloud_dir, env) / "receipts" / "account" /
                       f"{stamp}-{manifest['commit'][:12]}.json", receipt)


def account_receipts(cloud_dir: Path, env: str = ACCOUNT_ENV) -> list[dict]:
    return [r for r in _receipts(env_state(cloud_dir, env) / "receipts" / "account")
            if r.get("kind") == "account"]


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
                      f"then make release ENV={env}")
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
    """Promoted (promote_from set, or prod); else deterministic rehearsal or legacy."""
    env_dir = Path(env_dir)
    cfg = _load_hcl(env_dir / "env.auto.tfvars")
    if cfg.get("promote_from") or env_dir.name == "prod":
        return PROMOTED
    return REHEARSE if deterministic(env_dir) else LEGACY


def generate_guard(env_dir: Path, env: str) -> None:
    """make generate refuses a deterministic promoted env: its generated/ comes only
    from promote-to, and release-plan refuses a regenerated one as a stale promotion.
    Legacy promoted envs have no release-plan and still generate (e.g. genie-only prod).
    A config that doesn't parse is left to generate's own checks."""
    try:
        if not deterministic(env_dir) or env_role(env_dir) != PROMOTED:
            return
        source = source_env(env_dir)
    except Exception:
        return
    from scripts.coverage_fix import promoted_release_steps

    # A source that is itself promoted can't generate either: start where the chain does.
    chain = [env, source]
    try:
        while (len(chain) <= 10 and (Path(env_dir).parent / chain[-1] / "env.auto.tfvars").is_file()
               and deterministic(Path(env_dir).parent / chain[-1])
               and env_role(Path(env_dir).parent / chain[-1]) == PROMOTED):
            upstream = source_env(Path(env_dir).parent / chain[-1])
            if upstream in chain:
                break
            chain.append(upstream)
    except Exception:
        pass
    root, hops = chain[-1], chain[-2::-1]
    raise Refused(f"make generate ENV={env} refused: ENV={env} is a deterministic env promoted from {source}, "
                  f"so envs/{env}/generated/ comes only from promote-to (release-plan refuses a regenerated "
                  f"one as a stale promotion). Change the rules in {root}: make generate ENV={root}, "
                  f"commit, make rehearse ENV={root}, then "
                  + ", then ".join(f"make promote-to ENV={hop}" for hop in hops)
                  + f", then {promoted_release_steps(env, source)}.")


def apply_guard(env_dir: Path, env: str, target: str) -> None:
    from scripts.environment_lock import held_by_ancestor

    if not deterministic(env_dir):
        legacy_guard(env_dir, env, target.split(":")[0])
        return
    role = env_role(env_dir)
    allowed = APPLY_OWNERS[role].get(target)
    if allowed is None:
        raise Refused(f"unknown applying target {target!r}")
    owner = held_by_ancestor(env_dir)
    if owner in allowed:
        return
    name = target.split(":")[0]
    layer = f" (the {target.split(':')[1]} layer)" if ":" in target else ""
    if role == PROMOTED:
        how = (f"changes reach it only through make release-plan ENV={env}, then make release ENV={env}"
               + ("; make maintain ENV=" + env + " re-applies governance "
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
    if not deterministic(env_dir) or env_role(env_dir) != PROMOTED or confirm == env:
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
    cmd = command("account-provenance")
    cmd.add_argument("--remote", default="origin")
    cmd.add_argument("--branch", default="main")
    cmd.add_argument("--out", type=Path)
    command("plan-dir")
    command("account-plan-dir")
    cmd = command("account-plan-summary")
    cmd.add_argument("--show-json", type=Path, required=True)
    cmd.add_argument("--changes", choices=("true", "false"), required=True)
    cmd.add_argument("--out", type=Path, required=True)
    command("tag-policy-delete-guard").add_argument("--show-json", type=Path, required=True)
    cmd = command("account-tag-sync-diff")
    cmd.add_argument("--out", type=Path, required=True)
    cmd.add_argument("--allow-removal", default="", help="ALLOW_TAG_VALUE_REMOVAL: <key>:<value>,...")
    cmd.add_argument("--warehouse-id", default="")
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
    cmd = command("account-manifest-write")
    cmd.add_argument("--plan-dir", type=Path, required=True)
    cmd.add_argument("--provenance", type=Path, required=True)
    cmd = command("manifest-check")
    cmd.add_argument("--manifest", type=Path, required=True)
    cmd.add_argument("--live-snapshot", default=None)
    cmd.add_argument("--field", default="", help="print one manifest field (json path a.b) instead")
    cmd = command("account-manifest-check")
    cmd.add_argument("--manifest", type=Path, required=True)
    cmd.add_argument("--remote", default="origin")
    cmd.add_argument("--branch", default="main")
    cmd.add_argument("--field", default="", help="print one manifest field (json path a.b) instead")
    cmd.add_argument("--allow-removal", default="", help="ALLOW_TAG_VALUE_REMOVAL at apply time (must match)")
    command("account-manifest-select")
    command("account-manifest-summary").add_argument("--manifest", type=Path, required=True)
    cmd = command("account-manifest-info")
    cmd.add_argument("--manifest", type=Path, required=True)
    for name in ("release-receipt", "phase-record"):
        command(name).add_argument("--manifest", type=Path, required=True)
    command("current-manifest")
    command("manifest-summary").add_argument("--manifest", type=Path, required=True)
    command("account-receipt").add_argument("--manifest", type=Path, required=True)
    command("account-lock-check")
    command("any-deterministic-env")
    command("maintain-check")
    command("apply-guard").add_argument("--target", required=True)
    command("destroy-guard").add_argument("--confirm", default="")
    command("env-role")
    command("generate-guard")
    command("identity-check")
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
        elif args.command == "account-provenance":
            prov = account_provenance(args.env_dir, args.env, remote=args.remote, branch=args.branch)
            if args.out:
                args.out.write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
            print(f"Account provenance: {prov['commit'][:12]} is the clean HEAD of {prov['remote']}.")
        elif args.command == "plan-dir":
            print(new_plan_dir(args.env_dir, args.env))
        elif args.command == "account-plan-dir":
            print(new_plan_dir(args.env_dir, args.env))
        elif args.command == "account-plan-summary":
            data = _read_json(args.show_json)
            if data is None:
                raise Refused(f"{args.show_json} is not valid terraform show -json output")
            require_no_tag_policy_deletes(data)
            args.out.write_text(json.dumps(_plan_summary_data(data, changes=args.changes == "true"),
                                           indent=2, sort_keys=True) + "\n")
        elif args.command == "tag-policy-delete-guard":
            require_no_tag_policy_deletes(_read_json(args.show_json))
        elif args.command == "account-tag-sync-diff":
            allowed = canonical_removals(args.allow_removal)
            if allowed and not args.warehouse_id.strip():
                raise Refused("ALLOW_TAG_VALUE_REMOVAL needs WAREHOUSE_ID=<id>: the usage check reads "
                              "system.information_schema through a SQL warehouse")
            diff = account_tag_sync_diff(args.env_dir, allowed, args.warehouse_id.strip())
            args.out.write_text(json.dumps({"diff": diff, "allowed": allowed,
                                            "warehouse_id": args.warehouse_id.strip()},
                                           indent=2, sort_keys=True) + "\n")
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
        elif args.command == "account-manifest-write":
            path = account_manifest_write(args.env_dir, args.env, args.plan_dir, _read_json(args.provenance) or {})
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
        elif args.command == "current-manifest":
            print(current_manifest(args.env_dir, args.env))
        elif args.command == "manifest-summary":
            print("\n".join(manifest_summary(args.manifest, load_manifest(args.manifest, args.env))))
        elif args.command == "account-manifest-check":
            if args.field:
                value = load_account_manifest(args.manifest, args.env)
                for part in args.field.split("."):
                    value = value.get(part) if isinstance(value, dict) else None
                print(json.dumps(value) if not isinstance(value, str) else value)
                return 0
            account_manifest_check(args.env_dir, args.env, args.manifest, remote=args.remote, branch=args.branch,
                                   allow_removal=args.allow_removal)
            print(f"Account manifest {args.manifest}: commit, account state serial, saved plan, "
                  "account inputs and tag-policy diff (approved removals) match.")
        elif args.command == "account-manifest-summary":
            print("\n".join(account_manifest_summary(args.manifest, load_account_manifest(args.manifest, args.env))))
        elif args.command == "account-manifest-select":
            path = select_account_manifest(args.env_dir, args.env)
            manifest = load_account_manifest(path, args.env)
            print(path)
            print(f"Selected account manifest {manifest['id']} (sha256 {sha256_file(path)})", file=sys.stderr)
        elif args.command == "account-manifest-info":
            manifest = load_account_manifest(args.manifest, args.env)
            print(f"Selected account manifest {manifest['id']} (sha256 {sha256_file(args.manifest)})")
        elif args.command == "release-receipt":
            print(f"Release receipt: {release_receipt(args.env_dir, args.env, args.manifest)}")
            drop_saved_plans(args.env_dir, args.env, args.manifest)  # after the receipt, never before
        elif args.command == "account-receipt":
            print(f"Account receipt: {account_receipt(args.env_dir, args.env, args.manifest)}")
        elif args.command == "phase-record":
            print(f"Phase record: {phase_record(args.env_dir, args.env, args.manifest)}")
            drop_saved_plans(args.env_dir, args.env, args.manifest)  # after the record, never before
        elif args.command == "apply-guard":
            apply_guard(args.env_dir, args.env, args.target)
        elif args.command == "destroy-guard":
            destroy_guard(args.env_dir, args.env, args.confirm)
        elif args.command == "account-lock-check":
            require_account_lock(args.env_dir)
        elif args.command == "any-deterministic-env":
            print(int(any_deterministic_env(args.env_dir)))
        elif args.command == "legacy-guard":
            legacy_guard(args.env_dir, args.env, args.command_name)
        elif args.command == "env-role":
            print(env_role(args.env_dir))
        elif args.command == "generate-guard":
            generate_guard(args.env_dir, args.env)
        elif args.command == "identity-check":
            identity_check(args.env_dir, args.env)
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
