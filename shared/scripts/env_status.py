#!/usr/bin/env python3
"""make status ENV=<env> (JSON=1) and make next ENV=<env>: where an env is, and the one command after it.

Strictly read-only: it reads the env's files, the receipt store and git, and
nothing else. No Databricks client, no Terraform (an env with remote_state.hcl
skips what only its state could tell), no git fetch, no lock, no write: a
receipt store with no .store-id yet counts no receipt instead of creating one
(release_gate.store_id would). It never prints a credential: auth.auto.tfvars
is reported only as configured or not (auth_configured.auth_status), and the
lock's nonce is never read out.

In CI it never suggests a target make's CI guard would refuse: CI detection
and the opt-out are scripts/ci_guard.py's (what _guard-not-ci-apply runs),
and which targets carry the guard is read from Makefile.shared's
prerequisites. Such a step prints as "run <command> from the deployment
machine, not CI".

``next`` walks the gates the commands themselves apply (release_gate
provenance, current-manifest and maintain-check, the staged policy journal,
the env lock, the placeholder and deployer checks, coverage_fix's
promotion_chain) in the order the walkthrough runs them
(shared/examples/dev_to_prod/README.md), so the command it prints is one make
would not refuse for a reason it can see. A step that is not a make command
(edit a file, commit, push) is printed as that step.

Exit codes: 0 ok, 2 error (ENV is not a workspace env, or a usage error).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
from pathlib import Path

SHARED = Path(__file__).resolve().parents[1]
for _path in (SHARED, SHARED / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from scripts import release_gate as rg  # noqa: E402
from scripts.auth_configured import AuthStatus, auth_status  # noqa: E402
from scripts.coverage_fix import promotion_chain  # noqa: E402
from scripts.state_layout import data_access_subdir, remote_configured  # noqa: E402

EVIDENCE = Path("generated") / "evidence"
MAKEFILE = SHARED / "Makefile.shared"
_TARGET_LINE = re.compile(r"^([a-z_][a-z0-9_-]*):(?!=)([^#]*)")
ALLOW_CI_APPLY = "GENIERAILS_ALLOW_CI_APPLY"
_MAKE_COMMAND = re.compile(r"^make ([a-z][a-z0-9_-]*)(?: ENV=(\S+))?")


def ci_guarded_targets(makefile: Path = MAKEFILE) -> dict[str, str]:
    """target -> the CI guard it runs first ("apply" or "rebind"), read from Makefile.shared's prerequisites."""
    guarded = {}
    for line in makefile.read_text().splitlines():
        match = _TARGET_LINE.match(line)
        if match:
            prereqs = match.group(2).split()
            if "_guard-not-ci-rebind" in prereqs:
                guarded[match.group(1)] = "rebind"
            elif "_guard-not-ci-apply" in prereqs:
                guarded[match.group(1)] = "apply"
    return guarded


def _ci(cloud_dir: Path, makefile_cloud_dir: Path) -> dict:
    """What _guard-not-ci-apply would see (scripts/ci_guard.py), and its dev rehearse exemption.

    A missing or failing ci_guard counts as CI with no opt-out, as make's guards then refuse.
    """
    try:
        from scripts import ci_guard

        # Only ci_guard's exact answers count; anything else is CI with no opt-out.
        detected = ci_guard.status_line() != ci_guard.STATUS_NOT_CI
        allowed = detected and ci_guard.status_line(allow_opt_out=True) == ci_guard.STATUS_NOT_CI
        markers = list(ci_guard.ci_markers()) if detected else []
        if detected and not markers:
            raise ValueError("ci-status gave no valid answer")
        out = {"detected": detected, "markers": markers, "allow_ci_apply": allowed}
    except Exception as exc:
        return {"detected": True, "markers": ["ci_guard.py failed"], "allow_ci_apply": False,
                "dev_rehearse_allowed": False, "error": f"{type(exc).__name__}: {exc}"}
    if markers:
        saved = os.environ.get("GENIERAILS_MAKE_GOALS")
        os.environ["GENIERAILS_MAKE_GOALS"] = "rehearse"  # the goal make next would suggest
        try:
            out["dev_rehearse_allowed"] = ci_guard.dev_rehearse_allowed(
                cloud_dir / "envs" / "dev", "dev", makefile_cloud_dir, cloud_dir)
        finally:
            if saved is None:
                os.environ.pop("GENIERAILS_MAKE_GOALS", None)
            else:
                os.environ["GENIERAILS_MAKE_GOALS"] = saved
    return out


def ci_refusal(command: str, ci: dict, cloud_dir: Path) -> str:
    """Why make would refuse ``command`` in this CI ("" when it wouldn't, or it isn't a guarded target)."""
    match = _MAKE_COMMAND.match(command)
    if not match or not ci["detected"]:
        return ""
    target, env = match.group(1), match.group(2) or "dev"
    guard = ci_guarded_targets().get(target)
    where = ", ".join(ci["markers"])
    if guard == "rebind":
        return f"make {target} refuses in CI ({where}), even with {ALLOW_CI_APPLY}=1"
    if guard != "apply" or ci["allow_ci_apply"]:
        return ""
    if target == "rehearse" and env == "dev" and ci.get("dev_rehearse_allowed") \
            and rg.deterministic(cloud_dir / "envs" / "dev"):
        return ""
    return (f"make {target} refuses applying targets in CI ({where}) unless {ALLOW_CI_APPLY}=1 "
            "(isolated integration-test workflows only)"
            + ("; make rehearse ENV=dev runs in CI only for a deterministic dev with configured, locked "
               "remote state" if target == "rehearse" else ""))


# -- facts ----------------------------------------------------------------------

def _store(cloud_dir: Path, env: str) -> str:
    """The receipt store's id, or "" when it has none yet (never created here)."""
    try:
        return (rg.env_state(cloud_dir, env) / ".store-id").read_text().strip()
    except OSError:
        return ""


def _rehearsals(cloud_dir: Path, env: str) -> list[dict]:
    return [r for r in rg._receipts(rg.rehearse_dir(cloud_dir, env)) if r.get("kind") == "rehearse"]


def _config(env_dir: Path) -> dict:
    path = env_dir / "env.auto.tfvars"
    if not path.is_file():
        return {"exists": False}
    try:
        cfg = rg._load_hcl(path)
    except Exception:  # never the parser's text
        return {"exists": True, "parses": False}
    det = cfg.get("governance_mode", "legacy") == "deterministic"
    role = rg.env_role(env_dir)
    source = str(cfg.get("promote_from") or ("dev" if env_dir.name == "prod" else ""))
    return {"exists": True, "parses": True, "cfg": cfg, "governance_mode": "deterministic" if det else "legacy",
            "role": role, "promote_from": source, "chain": promotion_chain(env_dir, env_dir.name) if det else []}


def _placeholders(env_dir: Path, cfg: dict, role: str) -> list[str]:
    from genie_space_placeholder import agent_files_placeholder_errors, placeholder_errors

    path = env_dir / "env.auto.tfvars"
    errors = placeholder_errors(cfg, path, allow_access_tier_placeholders=role == rg.PROMOTED)
    errors += agent_files_placeholder_errors(env_dir)
    catalog_map = cfg.get("catalog_map")
    if role == rg.PROMOTED and isinstance(catalog_map, dict) and any(
            "<" in str(k) or "<" in str(v) for k, v in catalog_map.items()):
        errors.append(f"replace the catalog_map placeholders in {_rel_env(env_dir)}/env.auto.tfvars "
                      "with each source catalog and its catalog in this env")
    if cfg.get("governance_mode") == "deterministic":
        from deployer_identity import validation_errors

        errors += [e for e in validation_errors(cfg) if "placeholder" not in e]
    return errors


def _rel_env(env_dir: Path) -> str:
    return f"envs/{env_dir.name}"


def _git(env_dir: Path, cloud_dir: Path, remote: str, branch: str) -> dict:
    try:
        repo = rg.repo_root(env_dir if env_dir.is_dir() else cloud_dir)
    except rg.Refused:
        return {"checkout": False}
    commit = rg.git(repo, "rev-parse", "--verify", "-q", "HEAD", check=False).stdout.strip()
    try:
        dirty = rg.dirty_paths(repo, [cloud_dir, repo / "shared"])
    except rg.Refused:  # shared/ outside this checkout (a symlinked copy): only the cloud dir counts
        dirty = rg.dirty_paths(repo, [cloud_dir])
    env_rel = rg._rel(repo, env_dir)
    upstream = f"{remote}/{branch}"
    known = bool(commit) and rg.commit_known(repo, upstream)
    remote_url = rg.git(repo, "remote", "get-url", remote, check=False)
    return {
        "checkout": True, "repo": repo, "head": commit, "cloud_rel": rg._rel(repo, cloud_dir),
        "env_dirty": [line for line in dirty if line[3:].strip('"').startswith(env_rel + "/")],
        "dirty": dirty,
        "env_file_committed": bool(commit) and rg.tracked(repo, env_dir / "env.auto.tfvars"),
        "remote": upstream, "remote_configured": remote_url.returncode == 0, "remote_known": known,
        "head_on_remote": rg.is_ancestor(repo, commit, upstream) if known else None,
        "head_is_remote": known and rg.git(repo, "rev-parse", upstream).stdout.strip() == commit,
    }


def _rehearsal(cloud_dir: Path, source: str, commit: str, env: str = "") -> dict:
    store = _store(cloud_dir, source)
    mine = [r for r in _rehearsals(cloud_dir, source) if store and r.get("store") == store]
    at_head = [r for r in mine if r.get("commit") == commit]
    passing = [r for r in at_head if r.get("result") == "passed" and r.get("clean_tree") is True]
    out = {"source": source, "commit": commit, "receipts": len(mine), "passing": bool(passing),
           "foreign": len([r for r in _rehearsals(cloud_dir, source)
                           if r.get("commit") == commit and r.get("store") != store]),
           "unclean": len([r for r in at_head if r.get("result") == "passed" and r.get("clean_tree") is not True]),
           "failed": len([r for r in at_head if r.get("result") != "passed"])}
    if passing:
        receipt = passing[-1]
        out |= {"receipt": receipt["_path"], "created_at": receipt.get("created_at"),
                "reused_from": receipt.get("reused_from")}
        if env:
            target = (receipt.get("targets") or {}).get(env) or {}
            committed = rg.generated_digest(cloud_dir / "envs" / env / "generated")
            out["target_digest"] = ("missing" if not target.get("digest")
                                    else "matches" if target["digest"] == committed else "stale")
    return out


def _release(env_dir: Path, cloud_dir: Path, env: str, commit: str) -> dict:
    last = rg.last_release(cloud_dir, env)
    phases = [r for r in rg._receipts(rg.env_state(cloud_dir, env) / "receipts" / "phases")
              if r.get("kind") == "release-phase"
              and str(r.get("created_at", "")) > str((last or {}).get("created_at", ""))]
    out = {"last_release": {"commit": last.get("commit"), "created_at": last.get("created_at")} if last else None,
           "phases_since_release": len(phases),
           "phases_at_head": len([r for r in phases if r.get("commit") == commit])}
    if last is None:
        out["first_release"] = {"in_progress": bool(phases), "rounds_applied": len(phases)}
    try:
        current = rg.current_manifest_id(cloud_dir, env)
    except rg.Refused as exc:
        return out | {"current_manifest": {"error": str(exc)}}
    if current:
        path = rg.manifests_dir(cloud_dir, env) / current / "manifest.json"
        manifest = rg._read_json(path) or {}
        released = rg.released_by(cloud_dir, env, current)
        out["current_manifest"] = {
            "id": current, "path": str(path), "exists": path.is_file(), "commit": manifest.get("commit"),
            "created_at": manifest.get("created_at"), "final": manifest.get("final"),
            "released": bool(released), "at_head": manifest.get("commit") == commit}
    return out


def _staged(env_dir: Path, env: str) -> dict:
    from scripts import staged_policy_apply as spa

    try:
        journal = spa.load_journal(env_dir)
    except (spa.Refused, OSError, ValueError, RuntimeError) as exc:
        return {"error": str(exc)}
    if journal is None:
        return {"journal": False}
    t = journal.get("transition")
    out = {"journal": True, "active": journal.get("active") or None,
           "waiting_to_retire": [e.get("fingerprint") for e in journal.get("pending") or []]}
    if not t:
        return out
    out["transition"] = {k: t.get(k) for k in ("run_id", "from", "to", "status", "failed_phase", "commit")} | {
        "completed": list(t.get("completed") or []), "phases": list(t.get("phases") or [])}
    try:
        from scripts import policy_generations as pg

        desired = pg.desired_generation(env_dir)
        out["config_moved_on"] = t.get("to") != desired["fingerprint"]
    except Exception:
        desired, out["config_moved_on"] = None, None
    # abandon's own pre-plan checks read the data_access state; a remote one needs Terraform.
    if desired is None or remote_configured(env_dir) or remote_configured(env_dir / data_access_subdir()):
        out["abandonable"] = None
    else:
        out["abandonable"] = spa.Controller(env_dir, env)._may_abandon(t, desired)
    return out


def _lock(env_dir: Path) -> dict:
    from scripts.environment_lock import _pid_alive, _read_lock, lock_path

    try:
        path = lock_path(env_dir)
    except RuntimeError as exc:
        return {"error": str(exc)}
    if not (path.exists() or path.is_symlink()):
        return {"path": str(path), "state": "free"}
    held = _read_lock(path)
    if held is None:
        return {"path": str(path), "state": "unknown"}
    here = held["host"] == socket.gethostname()
    state = "stale" if here and not _pid_alive(held["pid"]) else "held"
    return {"path": str(path), "state": state, "owner": str(held.get("owner") or "?"), "pid": held["pid"],
            "host": held["host"], "started_at": held.get("started_at"), "this_host": here}


def _account(cloud_dir: Path, commit: str) -> dict:
    paths = rg.account_manifest_paths(cloud_dir)
    pending = []
    for path in paths:
        manifest = rg._read_json(path)
        if (not manifest or manifest.get("kind") != "account-manifest" or manifest.get("env") != rg.ACCOUNT_ENV
                or rg.account_manifest_noop(manifest)
                or rg.account_manifest_used(cloud_dir, rg.ACCOUNT_ENV, str(manifest.get("id") or ""))):
            continue
        pending.append({"id": manifest.get("id"), "path": str(path), "commit": manifest.get("commit"),
                        "at_head": manifest.get("commit") == commit,
                        "plan_saved": (path.parent / "account.tfplan").is_file()})
    return {"manifests": len(paths), "receipts": len(rg.account_receipts(cloud_dir)), "pending": pending}


def _evidence(env_dir: Path, git: dict) -> dict:
    path = env_dir / EVIDENCE
    files = sorted(p for p in path.rglob("*") if p.is_file()) if path.is_dir() else []
    rel = f"{_rel_env(env_dir)}/{EVIDENCE}/"
    untracked = any(line.startswith("??") and rel in line for line in git.get("dirty") or [])
    return {"path": f"{_rel_env(env_dir)}/{EVIDENCE}", "exists": path.is_dir(), "files": len(files),
            "untracked": untracked}


def collect(cloud_dir: Path, env: str, *, remote: str = "origin", branch: str = "main",
            with_next: bool = True, makefile_cloud_dir: Path | None = None) -> dict:
    cloud_dir = Path(cloud_dir).resolve()
    env_dir = cloud_dir / "envs" / env
    report: dict = {"env": env, "cloud": cloud_dir.name, "env_dir": _rel_env(env_dir), "errors": []}
    config = _config(env_dir)
    report["config"] = {k: v for k, v in config.items() if k != "cfg"}
    git = _git(env_dir, cloud_dir, remote, branch)
    report["git"] = {k: v for k, v in git.items() if k != "repo"}
    report["auth"] = auth_status(env_dir / "auth.auto.tfvars").value if env_dir.is_dir() else "missing"
    report["lock"] = _lock(env_dir)
    report["evidence"] = _evidence(env_dir, git)
    report["ci"] = _ci(cloud_dir, Path(makefile_cloud_dir or cloud_dir))
    report["generated"] = (env_dir / "generated" / "abac.auto.tfvars").is_file()
    if config.get("parses"):
        report["placeholders"] = _placeholders(env_dir, config["cfg"], config["role"])
        report["promotion_targets"] = rg.promotion_targets(cloud_dir, env)
    commit = git.get("head") or ""
    det = config.get("governance_mode") == "deterministic"
    try:
        if det:
            report["staged"] = _staged(env_dir, env)
        elif config.get("parses") and not remote_configured(env_dir):
            # make's legacy-guard: a legacy env whose migration began refuses release and rehearse.
            from scripts import migration

            report["migrated"] = migration.migrated(env_dir) or None
        if det and commit:
            source = env if config["role"] == rg.REHEARSE else config["promote_from"]
            report["rehearsal"] = _rehearsal(cloud_dir, source, commit,
                                             env if config["role"] == rg.PROMOTED else "")
            if config["role"] == rg.PROMOTED:
                report["release"] = _release(env_dir, cloud_dir, env, commit)
            report["account"] = _account(cloud_dir, commit)
    except Exception as exc:  # a state dir that can't be read: report it, never guess
        report["errors"].append(str(exc))
    if with_next:
        command, why = next_step(cloud_dir, env, report, remote=remote, branch=branch)
        refusal = ci_refusal(command, report["ci"], cloud_dir)
        if refusal:
            command, why = f"run {command} from the deployment machine, not CI", f"{why}; but {refusal}"
        report["next"] = {"command": command, "why": why}
    return report


# -- next -------------------------------------------------------------------------

def _commit_step(dirty: list[str], cloud_rel: str) -> str:
    """git add for porcelain ``dirty`` lines (repo-relative), as run from the cloud dir like make."""
    paths = set()
    for line in dirty:
        path = line[3:].strip('"').split(" -> ")[-1]
        if cloud_rel not in ("", ".") and path.startswith(cloud_rel + "/"):
            path = path[len(cloud_rel) + 1:]
            paths.add("envs/" if path.startswith("envs/") else path)
        else:
            paths.add(os.path.relpath(path, cloud_rel or "."))
    return f"git add {' '.join(sorted(paths))} && git commit"


def next_step(cloud_dir: Path, env: str, report: dict, *, remote: str = "origin", branch: str = "main",
              _seen: tuple = ()) -> tuple[str, str]:
    """(command, why) for ``report`` (collect's); follows the promotion chain for the source's steps."""
    cloud_dir = Path(cloud_dir)
    env_dir = cloud_dir / "envs" / env
    rel = _rel_env(env_dir)
    config, git, lock = report["config"], report["git"], report["lock"]
    if report["errors"]:
        return (f"make status ENV={env}", f"the receipt store can't be read ({report['errors'][0]}); "
                "fix GENIERAILS_STATE_DIR or the store first")
    if not config["exists"]:
        return (f"make setup ENV={env}", f"{rel}/env.auto.tfvars doesn't exist yet; setup writes it and "
                "prints this env's steps")
    if not config["parses"]:
        return (f"edit {rel}/env.auto.tfvars", "it is not valid HCL; once it parses, make setup "
                f"ENV={env} prints this env's steps")
    if lock.get("state") in ("held", "unknown"):
        who = (f"make {lock['owner']} (pid {lock['pid']} on {lock['host']}, since {lock.get('started_at') or '?'})"
               if lock["state"] == "held" else f"an unreadable lock file ({lock['path']})")
        return (f"wait for {who} to finish, then make next ENV={env}",
                f"{who} holds the {env} env lock; every applying command refuses until it is released")
    role, det = config["role"], config["governance_mode"] == "deterministic"
    if role != rg.PROMOTED and report["auth"] != AuthStatus.CONFIGURED.value:
        return (f"edit {rel}/auth.auto.tfvars", f"fill in the {env} workspace host, client ID and client secret "
                f"(it is {report['auth']}; no value is shown here); generate and rehearse need them")
    if report.get("placeholders"):
        return (f"edit {rel}/env.auto.tfvars" if "agents/" not in report["placeholders"][0]
                else f"edit {rel}/agents/", report["placeholders"][0])
    live_note = ""

    def live(command: str, why: str) -> tuple[str, str]:
        """A step that reaches the workspace: it needs this env's credentials first."""
        if report["auth"] != AuthStatus.CONFIGURED.value:
            return (f"edit {rel}/auth.auto.tfvars",
                    f"{command} needs the {env} workspace host, client ID and client secret there "
                    f"(it is {report['auth']}); no value is shown here")
        return command, why + live_note

    def commit_first() -> tuple[str, str] | None:
        if report["evidence"]["untracked"]:
            return (f"move {report['evidence']['path']}/ out of the checkout",
                    "make evidence left it untracked under envs/, so the checkout isn't clean and "
                    "release-plan, rehearse receipts and account-plan refuse it; keep it outside the repo")
        dirty = git["dirty"] if git.get("checkout") else []
        if dirty or (git.get("checkout") and not git["env_file_committed"]):
            files = ", ".join(line[3:] for line in dirty[:3]) or f"{rel}/env.auto.tfvars"
            return (_commit_step(dirty, git["cloud_rel"]) if dirty else f"git add {rel}/ && git commit",
                    f"uncommitted changes ({files}{', ...' if len(dirty) > 3 else ''}): rehearse receipts, "
                    "release-plan and account-plan read only committed files")
        return None

    def push_first(why: str) -> tuple[str, str] | None:
        if not git["remote_configured"]:
            if remote != "origin":
                return (f"git remote add {remote} <your repo URL> && git push {remote} HEAD:{branch}",
                        f"there is no {remote} remote; {why}")
            return (f"make setup ENV={env} REPO=<your repo URL> && git push -u origin HEAD:{branch}",
                    f"this is a local-only repo (no origin remote; setup's REPO= adds it, and setup may then offer to push); {why}")
        if not git["remote_known"] or not git["head_on_remote"]:
            return (f"git push {remote} HEAD:{branch} (or merge its PR, then pull)",
                    f"HEAD {git['head'][:12]} isn't on {git['remote']} (as last fetched; status doesn't fetch); "
                    f"{why}")
        return None

    # -- legacy -------------------------------------------------------------------
    if not det and report.get("migrated"):
        return (f'edit {rel}/env.auto.tfvars: governance_mode = "deterministic"',
                f"{env} was migrated to deterministic governance ({report['migrated']}), so make's legacy "
                "commands refuse it (a legacy apply would recreate its moved treatment tags)")
    if not det:
        if role == rg.PROMOTED:
            if not report["generated"]:
                return (f"make promote-to ENV={env}", f"{rel}/generated/ is empty; promote-to copies the rules "
                        f"from {config['promote_from']}")
            return live(f"make release ENV={env}", f"{env} uses legacy governance: release derives, checks, "
                        "applies and verifies in one run (legacy envs have no release-plan)")
        if not report["generated"]:
            return live(f"make generate ENV={env}", f"{rel}/generated/ has no abac.auto.tfvars yet")
        targets = report.get("promotion_targets") or []
        then = f"; once it passes, make promote-to ENV={targets[0]}" if targets else ""
        return live(f"make rehearse ENV={env}", "legacy envs keep no rehearse receipt, so status can't tell "
                    f"whether this commit was rehearsed{then}")

    # -- deterministic ----------------------------------------------------------------
    if not git.get("checkout") or not git.get("head"):
        return ("git init && git add .gitignore envs/ && git commit" if not git.get("checkout")
                else _commit_step(git["dirty"], git["cloud_rel"]),
                "no commit yet: deterministic envs release only committed files (rehearse receipts and "
                "release-plan read git)")
    staged = report.get("staged") or {}
    t = staged.get("transition")
    if t and staged.get("config_moved_on"):
        if staged.get("abandonable"):
            return live(f"make policy-abandon ENV={env}",
                        f"unfinished policy change {t['run_id']} ({t['status']}) is of generation {t['to']}, and "
                        "the generated config has moved on; drop it, then re-run what started it")
        return (f"git checkout --detach {t['commit'] or '<its commit>'}",
                f"unfinished policy change {t['run_id']} resumes only from its own commit and generation "
                f"{t['to']} (the generated config has moved on, and it can't be abandoned"
                f"{'' if staged.get('abandonable') is False else ' as far as local files show'})")
    if t:
        live_note = (f" (it resumes unfinished policy change {t['run_id']}, {t['status']}, done: "
                     f"{', '.join(t['completed']) or 'none'})")

    if role == rg.REHEARSE:
        if not report["generated"]:
            return live(f"make generate ENV={env}", f"{rel}/generated/ has no abac.auto.tfvars yet")
        step = commit_first()
        if step:
            return step
        rehearsal = report["rehearsal"]
        if not rehearsal["passing"]:
            account = report["account"]
            due = [m for m in account["pending"] if m["at_head"] and m["plan_saved"]]
            first = not rehearsal["receipts"] and not account["receipts"] and not account["manifests"]
            if due or first:
                command = "make apply-account" if due else "make account-plan"
                if not git["head_is_remote"]:
                    return push_first(f"{command} runs only from exactly {git['remote']}") or (
                        f"git pull --ff-only {remote} {branch}",
                        f"{command} runs only from exactly {git['remote']}, and HEAD is behind it")
                # On a terminal rehearse itself shows the account plan and asks y/N (Makefile
                # _ACCOUNT_APPROVE_APPLY); next can't see the caller's terminal, so the primitive leads.
                on_a_terminal = f"; or, on a terminal, make rehearse ENV={env} shows it and asks y/N"
                return live(command, f"the approved account manifest {due[0]['id']} for HEAD is not applied yet"
                            + on_a_terminal if due else "first deploy: rehearse refuses a non-empty account plan "
                            "(the new gr_treatment tag policy), so plan the account layer and get its manifest "
                            "approved" + on_a_terminal)
            why = f"no passing rehearsal of HEAD {git['head'][:12]} from this receipt store"
            if rehearsal["failed"]:
                why += " (one at HEAD failed: fix what stopped it)"
            elif rehearsal["foreign"]:
                why += f" ({rehearsal['foreign']} receipt(s) for it came from another store and don't count)"
            return live(f"make rehearse ENV={env}", why + "; release-plan needs one")
        targets = report.get("promotion_targets") or []
        if not targets:
            if not (cloud_dir / "envs" / "prod").exists():
                return ("make setup ENV=prod", f"{env} is rehearsed at HEAD and no env is promoted from it yet")
            return ("nothing to do", f"{env} is rehearsed at HEAD and no env is promoted from it")
        steady = None
        for target in targets:
            if target in _seen:
                continue
            sub = collect_for_next(cloud_dir, target, remote, branch)
            command, why = next_step(cloud_dir, target, sub, remote=remote, branch=branch, _seen=(*_seen, env))
            if not command.startswith("make maintain"):
                return command, why
            steady = steady or (command, why)
        return steady or ("nothing to do", f"{env} is rehearsed at HEAD")

    # promoted
    source = config["promote_from"]
    chain = config["chain"]
    source_dir = cloud_dir / "envs" / source
    if not (source_dir / "env.auto.tfvars").is_file():
        return (f"edit {rel}/env.auto.tfvars", f'promote_from = "{source}" names no env: envs/{source}/'
                "env.auto.tfvars doesn't exist")
    if not rg.deterministic(source_dir):
        return (f"migrate envs/{source} to deterministic governance (shared/docs/legacy-migration.md)",
                f"envs/{source} uses legacy governance but {env} is deterministic, so release-plan ENV={env} "
                f'refuses; or set governance_mode = "legacy" in {rel}/env.auto.tfvars')
    if len(chain) > 2:
        return (f'edit {rel}/env.auto.tfvars: promote_from = "{chain[0]}"',
                f"release-plan ENV={env} needs a passing rehearsal of {source}, and make rehearse refuses "
                f"envs/{source} while it has promote_from (chain {' -> '.join(chain)}); a deterministic env is "
                "released only from an env with no promote_from")
    if not report["generated"]:
        if not (source_dir / "generated" / "abac.auto.tfvars").is_file():
            return next_step(cloud_dir, source, collect_for_next(cloud_dir, source, remote, branch),
                             remote=remote, branch=branch, _seen=(*_seen, env))
        return (f"make promote-to ENV={env}", f"{rel}/generated/ is empty; promote-to writes it from {source}")
    step = commit_first()
    if step:
        return step
    release = report["release"]
    current = release.get("current_manifest") or {}
    rounds = (release.get("first_release") or {}).get("rounds_applied")
    first = f" (first release, round {rounds + 1})" if rounds is not None else ""
    if current.get("id") and current.get("exists") and not current["released"] and current["at_head"]:
        return live(f"make release ENV={env}", f"the current manifest {current['id']}{first} plans HEAD and is "
                    "not released yet: get it approved, then release it")
    last = release.get("last_release") or {}
    if last.get("commit") == git["head"] and not release["phases_at_head"] and not t:
        return live(f"make maintain ENV={env}", f"{env} is released at HEAD; maintain keeps it reconciled "
                    "(scheduled)")
    rehearsal = report["rehearsal"]
    if not rehearsal["passing"]:
        # The source's own next step gets it there (generate, commit, the account layer, rehearse).
        need = f"release-plan ENV={env} needs a passing rehearsal of {source} at HEAD {git['head'][:12]}"
        if source not in _seen:
            command, why = next_step(cloud_dir, source, collect_for_next(cloud_dir, source, remote, branch),
                                     remote=remote, branch=branch, _seen=(*_seen, env))
            if command == f"make rehearse ENV={source}":
                return command, f"{need}; {why}"
            if not command.startswith("make maintain") and command != "nothing to do":
                return command, why
        return live(f"make rehearse ENV={source}", need)
    if rehearsal.get("target_digest") == "stale":
        return (f"make promote-to ENV={env}", f"{rel}/generated doesn't match what promote-to produces from "
                f"{source} at this commit (stale promotion); then commit, push, make rehearse ENV={source}")
    if rehearsal.get("target_digest") == "missing":
        return live(f"make rehearse ENV={source}", f"the rehearsal of {source} at HEAD recorded no promote-to "
                    f"ENV={env} digest ({env} was not a promotion target then)")
    step = push_first(f"release-plan plans only commits on {git['remote']}")
    if step:
        return step
    if last.get("commit") and last["commit"] != git["head"] and rg.is_ancestor(
            rg.repo_root(env_dir), git["head"], last["commit"]):
        return (f"merge {env}:rollback:{git['head']} into {rel}/ack.txt on {git['remote']} in a reviewed PR",
                f"HEAD {git['head'][:12]} is older than the last release {last['commit'][:12]}; release-plan "
                "refuses an out-of-order release without that acknowledgement")
    why = f"{source} is rehearsed at HEAD {git['head'][:12]} and {rel}/generated matches it"
    if rounds:
        why = f"round {rounds} of the first release is applied; plan round {rounds + 1}"
    elif release["phases_at_head"]:
        why = "the last manifest applied one phase of a staged policy change; plan the next"
    elif current.get("id") and not current.get("at_head") and not current.get("released"):
        why += f" (the current manifest {current['id']} plans another commit, so release would refuse it)"
    return live(f"make release-plan ENV={env}", why)


def collect_for_next(cloud_dir: Path, env: str, remote: str, branch: str) -> dict:
    """collect() without its own next step (next_step walks to it itself)."""
    return collect(cloud_dir, env, remote=remote, branch=branch, with_next=False)


# -- output -----------------------------------------------------------------------

def _text(report: dict) -> list[str]:
    env, config, git = report["env"], report["config"], report["git"]
    lines = [f"=== GenieRails status ({env}) ==="]
    if not config["exists"]:
        lines.append(f"  Config:      {report['env_dir']}/env.auto.tfvars doesn't exist")
    elif not config["parses"]:
        lines.append(f"  Config:      {report['env_dir']}/env.auto.tfvars is not valid HCL")
    else:
        role = {"promoted": f"promoted from {config['promote_from']}", "rehearse": "rehearsal (no promote_from)",
                "legacy": "legacy (no promote_from)"}[config["role"]]
        lines.append(f"  Governance:  {config['governance_mode']}")
        lines.append(f"  Role:        {role}")
        if config.get("chain") and len(config["chain"]) > 1:
            lines.append(f"  Chain:       {' -> '.join(config['chain'])}")
        if report.get("promotion_targets"):
            lines.append(f"  Promotes to: {', '.join(report['promotion_targets'])}")
    lines.append(f"  Credentials: auth.auto.tfvars {report['auth']}")
    if not git.get("checkout"):
        lines.append("  Git:         not a git checkout")
    elif not git.get("head"):
        lines.append("  Git:         no commit yet")
    else:
        where = ("no remote " + git["remote"].split("/")[0] + " (local-only repo)" if not git["remote_configured"]
                 else f"{git['remote']} unknown (never fetched)" if not git["remote_known"]
                 else f"on {git['remote']}" if git["head_on_remote"] else f"not on {git['remote']}")
        lines.append(f"  Git:         HEAD {git['head'][:12]}, {where} (as last fetched)")
        lines.append(f"  {report['env_dir']}: " + (f"{len(git['env_dirty'])} uncommitted file(s)" if git["env_dirty"]
                                                   else "committed" if git["env_file_committed"]
                                                   else "env.auto.tfvars not committed"))
        other = [line for line in git["dirty"] if line not in git["env_dirty"]]
        if other:
            lines.append(f"  Elsewhere:   {len(other)} uncommitted file(s) ({other[0][3:]}"
                         f"{', ...' if len(other) > 1 else ''})")
    lines.append(f"  Generated:   {'yes' if report['generated'] else 'no'} (generated/abac.auto.tfvars)")
    rehearsal = report.get("rehearsal")
    if rehearsal:
        if rehearsal["passing"]:
            text = f"passing rehearsal of {rehearsal['source']} covers HEAD ({rehearsal['receipt']})"
            if rehearsal.get("target_digest"):
                text += f"; promote-to digest {rehearsal['target_digest']}"
        else:
            text = f"no passing rehearsal of {rehearsal['source']} covers HEAD"
            if rehearsal["failed"]:
                text += f" ({rehearsal['failed']} failed at HEAD)"
            if rehearsal["foreign"]:
                text += f" ({rehearsal['foreign']} from another store, informational)"
        lines.append(f"  Rehearsal:   {text}")
    release = report.get("release")
    if release:
        current = release.get("current_manifest")
        if not current:
            lines.append("  Manifest:    none (no release-plan yet)")
        elif current.get("error"):
            lines.append(f"  Manifest:    {current['error']}")
        else:
            state = ("released" if current["released"] else "awaiting release" if current["at_head"]
                     else f"plans {str(current.get('commit'))[:12]}, not HEAD")
            lines.append(f"  Manifest:    {current['id']} ({state}): {current['path']}")
        last = release.get("last_release")
        lines.append("  Released:    " + (f"{str(last['commit'])[:12]} at {last['created_at']}" if last else "never"))
        first = release.get("first_release")
        if first:
            lines.append("  First release: " + (f"in progress, {first['rounds_applied']} round(s) applied"
                                                if first["in_progress"] else "not started"))
    staged = report.get("staged")
    if staged:
        if staged.get("error"):
            lines.append(f"  Policy:      {staged['error']}")
        elif not staged["journal"]:
            lines.append("  Policy:      no policy journal yet")
        else:
            lines.append(f"  Policy:      active generation {staged['active'] or 'none'}")
            t = staged.get("transition")
            if t:
                abandon = {True: "abandonable (make policy-abandon)", False: "not abandonable",
                           None: "abandonable unknown (make policy-status)"}[staged.get("abandonable")]
                lines.append(f"  Open change: run {t['run_id']} {t['from'] or 'nothing'} -> {t['to']}, {t['status']}"
                             f", done: {', '.join(t['completed']) or 'none'}; "
                             + ("config moved on; " if staged.get("config_moved_on") else "resumable; ") + abandon)
    ci = report["ci"]
    lines.append("  CI:          " + ("not detected" if not ci["detected"] else
                                     f"detected ({', '.join(ci['markers'])}); "
                                     + (f"{ALLOW_CI_APPLY}=1: applying targets may run"
                                        if ci["allow_ci_apply"] else "applying targets refuse")))
    lock = report["lock"]
    if lock.get("error"):
        lines.append(f"  Lock:        {lock['error']}")
    else:
        lines.append("  Lock:        " + {"free": "free", "unknown": f"unreadable lock file {lock['path']}"}.get(
            lock["state"], f"{'held' if lock['state'] == 'held' else 'stale (dead pid; the next lock removes it)'} "
                           f"by make {lock.get('owner')} (pid {lock.get('pid')} on {lock.get('host')}, "
                           f"since {lock.get('started_at')})"))
    account = report.get("account")
    if account:
        pending = account["pending"]
        lines.append("  Account:     " + ("no pending account manifest" if not pending else "; ".join(
            f"pending manifest {m['id']} ({'HEAD' if m['at_head'] else 'commit ' + str(m['commit'])[:12]})"
            for m in pending)))
    evidence = report["evidence"]
    if evidence["exists"]:
        lines.append(f"  Evidence:    {evidence['path']}/ ({evidence['files']} file(s)"
                     f"{', untracked' if evidence['untracked'] else ''})")
    for error in report["errors"]:
        lines.append(f"  ERROR:       {error}")
    lines += ["", f"Next: {report['next']['command']}", f"  Why: {report['next']['why']}"]
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("status", "next"))
    parser.add_argument("--cloud-dir", type=Path, required=True)
    parser.add_argument("--env", required=True)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--makefile-cloud-dir", type=Path, default=None,
                        help="the cloud dir the Makefile records (GENIERAILS_CLOUD_DIR), as the CI guard gets it")
    args = parser.parse_args(argv)
    os.environ["GIT_OPTIONAL_LOCKS"] = "0"  # git status must not refresh the index (a write)
    if args.env in (rg.ACCOUNT_ENV, data_access_subdir()):
        print(f"make {args.command} is workspace-driven. Use ENV=<workspace> instead of ENV={args.env}.",
              file=sys.stderr)
        return 2
    report = collect(args.cloud_dir, args.env, remote=args.remote, branch=args.branch,
                     makefile_cloud_dir=args.makefile_cloud_dir)
    if args.command == "next":
        print(report["next"]["command"])
        print(f"  Why: {report['next']['why']}")
    elif args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        print("\n".join(_text(report)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
