#!/usr/bin/env python3
"""Read-only checks behind make check and make promote-to CHECK=1.

check          make check ENV=<env>: validate; generated files are current
               (deterministic envs; with promote_from or ENV=prod, also the
               promote-to output); validate-generated; the offline coverage
               check; the mask-library unit tests; a structural plan.
promote-check  make promote-to ENV=<env> CHECK=1: rerun promote-to from the
               committed files and fail if its output differs.

Every step runs make in a scratch copy of the cloud dir. The copy holds the
env files git tracks, never git-ignored or untracked files, credentials,
Terraform state or runtime records, so a local-only file can't change the
result, steps can't read live prod or prod state, and whatever they write
lands in the scratch dir, which is deleted afterwards. Nothing under envs/ is
written, no Databricks write API is called and no applying target runs, so
both commands are safe under CI=true and never trip the CI apply guard.

promote-to is a pure function of the commit (design section 9): for a
deterministic source it never reads prod state or lists live policies, so
the comparison is exact. The deployed policy names prod keeps are an ignored
overlay make release-plan builds (scripts/runtime_overlay.py).
TODO(rollout step 12): check the dev receipt store in the PR check.
TODO(rollout step 12, remote state): a state-aware plan once an env has remote
state; until then the plan is always structural (empty state).
"""

from __future__ import annotations

import argparse
import difflib
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from scripts.sticky_governance import GovernedTablesError, _load_hcl, deterministic  # noqa: E402
from scripts.release_gate import PROMOTED, env_role  # noqa: E402

RESERVED_ENVS = ("account", "data_access")
# Runtime files never enter the scratch copy: credentials, state, Terraform
# data dirs and the dot-records commands leave next to them (coverage and
# CAN_RUN results, live refresh, locks, certifications). Agent ID files are
# inputs, so they stay. remote_state.hcl stays out too: the scratch plan is
# structural and never reads (or locks) an env's remote state.
RUNTIME_NAMES = {"auth.auto.tfvars", "remote_state.hcl"}
RUNTIME_SUFFIXES = (".tfstate", ".tfstate.backup")
KEPT_DOT_PREFIX = ".genie_space_id"
# Layer files promote splits out of generated/; applying commands re-split,
# so a missing one is a note, not a failure. Paths relative to envs/<env>.
SPLIT_FILES = {
    "abac.auto.tfvars",
    "data_access/abac.auto.tfvars",
    "data_access/masking_functions.sql",
    "generated/genie_space_derived_acl_groups.auto.tfvars",
}
DIFF_LINES = 40
# Make state from the caller's make must not leak into the scratch make.
LEAKED_VARS = (
    "MAKEFLAGS", "MAKELEVEL", "MFLAGS", "MAKEOVERRIDES", "ENV", "ENV_DIR", "ENVS_DIR",
    "CLOUD_ROOT", "SHARED_ROOT", "GENIERAILS_CLOUD_DIR", "SOURCE_ENV", "SOURCE_ENV_DIR",
    "DEST_ENV", "DEST_ENV_DIR", "DEST_CATALOG_MAP", "ACCOUNT_ENV_DIR", "FROM", "CATALOG_MAP",
    "CHECK", "APPLY_FLAGS", "GENIERAILS_APPLY_FLAGS", "TARGET_ENV", "SPACE", "MODE",
    "GENIERAILS_STATE_DIR",
)


class CheckFailed(RuntimeError):
    pass


def _is_runtime(rel: Path) -> bool:
    for part in rel.parts:
        if part in RUNTIME_NAMES or part.endswith(RUNTIME_SUFFIXES):
            return True
        if part.startswith(".") and not part.startswith(KEPT_DOT_PREFIX):
            return True
    return False


def _not_copied(rel: Path) -> bool:
    """Runtime files, and envs/<env>/drafts/ (AI-helper drafts, which no check reads)."""
    return _is_runtime(rel) or rel.parts[1:2] == ("drafts",)


# The one link make itself scaffolds in a copied env (_prepare-env); it is not
# copied, and the scratch make recreates it inside the copy.
SCAFFOLD_LINKS = {("data_access", "env.auto.tfvars"): "../env.auto.tfvars"}


def _is_scaffold_link(envs: Path, rel: Path) -> bool:
    return (len(rel.parts) == 3 and (envs / rel).is_symlink()
            and SCAFFOLD_LINKS.get(rel.parts[1:]) == os.readlink(envs / rel))


def _refuse_symlinks(cloud_dir: Path, envs: Path) -> None:
    """Refuse an envs/ tree a scratch copy could write out of through a symlink.

    Same rule as promote-to (#88): envs/ and every env dir must be real
    directories whose physical path is exactly <cloud>/envs/<name>, and no
    copied path inside an env may be a symlink, except the scaffold link
    above. Runtime files are never copied, so they are not checked.
    """
    if not envs.exists() and not envs.is_symlink():
        return
    if envs.is_symlink() or os.path.realpath(envs) != os.path.join(os.path.realpath(cloud_dir), "envs"):
        raise CheckFailed(f"{envs} is a symlink or not this cloud's envs/; the check copies only a real envs/")
    for directory, dirnames, filenames in os.walk(envs):
        for name in dirnames + filenames:
            path = Path(directory) / name
            rel = path.relative_to(envs)
            if _not_copied(rel) or not path.is_symlink():
                continue
            if _is_scaffold_link(envs, rel):
                continue
            raise CheckFailed(
                f"envs/{rel} is a symlink (to {os.readlink(path)}); the check copies only real files and "
                "directories under envs/, so it can't write outside its scratch copy. Replace the symlink "
                "with the file or directory it points to.")


def _assert_contained(root: Path) -> None:
    """Before any make step: no path in the copy is a link or leaves it."""
    real_root = root.resolve()
    for path in root.rglob("*"):
        if path.is_symlink() or not path.resolve().is_relative_to(real_root):
            raise CheckFailed(f"scratch copy path {path} is a symlink or resolves outside {real_root}")


def _tracked_env_files(envs: Path) -> list[Path]:
    """Every file git tracks under envs/, relative to it (working-tree content).

    Git-ignored and untracked files never reach the copy, so a local-only file
    (such as the ignored discovery file make generate writes) can't make the
    check pass on a laptop and fail in CI, or the other way round.
    """
    try:
        listed = subprocess.run(["git", "-C", str(envs), "ls-files", "-z", "--", "."],
                                capture_output=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = (getattr(exc, "stderr", None) or b"").decode(errors="replace").strip()
        raise CheckFailed(f"cannot list the files git tracks under {envs}: {detail or exc}; "
                          "make check runs in a git checkout") from exc
    files = [Path(os.fsdecode(raw)) for raw in listed.split(b"\0") if raw]
    if not files and any(envs.iterdir()):
        raise CheckFailed(f"git tracks no file under {envs} (is envs/ git-ignored?); make check reads only "
                          "committed env files. Put the checkout in your own repo and commit envs/ first: "
                          "see shared/examples/dev_to_prod/README.md (Dev step 2 / Commit it) — "
                          "rename origin to genierails, add your remote, make setup ENV=<env> writes a "
                          ".gitignore that keeps runtime files out, then git add .gitignore envs/.")
    return files


def _copy_tracked_envs(source: Path, dest: Path) -> None:
    """Copy the tracked env files, minus runtime files and the scaffold link."""
    dest.mkdir()
    for rel in _tracked_env_files(source):
        src = source / rel
        if _not_copied(rel) or _is_scaffold_link(source, rel) or not src.is_file():
            continue  # a tracked file deleted in the working tree is not copied either
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, target)


def _uncommitted_env_files(cloud_dir: Path, env: str) -> tuple[list[str], list[str]]:
    """(tracked files changed since the commit, untracked files) under envs/<env>.

    Informational only, from one read-only git call (no optional index
    refresh). Any git failure, including not being in a repo, returns nothing.
    """
    try:
        out = subprocess.run(["git", "--no-optional-locks", "-C", str(cloud_dir / "envs" / env), "status",
                              "--porcelain", "-z", "--untracked-files=all", "--", "."],
                             capture_output=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return [], []
    changed, untracked = [], []
    fields = iter(os.fsdecode(out).split("\0"))
    for entry in fields:
        if len(entry) < 4:
            continue
        status, path = entry[:2], entry[3:]
        (untracked if status == "??" else changed).append(path)
        if "R" in status or "C" in status:
            next(fields, None)  # the rename/copy source follows its destination
    return sorted(changed), sorted(untracked)


def _note_uncommitted_env_files(cloud_dir: Path, env: str) -> None:
    changed, untracked = _uncommitted_env_files(cloud_dir, env)
    if changed:
        print(f"  NOTE: make check passed against uncommitted working-tree content in envs/{env}:")
        for path in changed:
            print(f"    {path}")
    if untracked:
        print(f"  NOTE: untracked files in envs/{env} were not checked; commit them if they belong to the env:")
        for path in untracked:
            print(f"    {path}")
    if changed or untracked:
        print("  rehearse and release-plan use the commit, not these uncommitted changes.")


def _snapshot(envs: Path) -> dict[str, tuple[str, bytes]]:
    """Relative path -> (kind, content) for every non-runtime file in the copy.

    The copy starts with no links (_assert_contained), so any link in it is
    one the scratch make scaffolded, not a regenerated file.
    """
    found = {}
    for path in sorted(envs.rglob("*")):
        rel = path.relative_to(envs)
        if not _not_copied(rel) and not path.is_symlink() and path.is_file():
            found[str(rel)] = ("file", path.read_bytes())
    return found


class Scratch:
    """A throwaway cloud dir whose envs/ copies the tracked env files minus runtime files."""

    def __init__(self, cloud_dir: Path, makefile: Path, make_bin: str):
        self.real_envs = cloud_dir / "envs"
        self.make_bin = make_bin
        _refuse_symlinks(cloud_dir, self.real_envs)
        self.root = Path(tempfile.mkdtemp(prefix="genierails-check-"))
        try:
            self.cloud = self.root / cloud_dir.name
            self.cloud.mkdir()
            shutil.copyfile(makefile, self.cloud / "Makefile")
            self.envs = self.cloud / "envs"
            if self.real_envs.is_dir():
                _copy_tracked_envs(self.real_envs, self.envs)
            else:
                self.envs.mkdir()
            _assert_contained(self.root)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def make(self, target: str, *args: str, extra_env: dict[str, str] | None = None) -> int:
        env = {k: v for k, v in os.environ.items() if k not in LEAKED_VARS} | (extra_env or {})
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        # The copy has no runtime overlay: regenerations write generated/ in
        # place, which is what the comparison checks (scripts/runtime_overlay.py).
        env["GENIERAILS_NO_OVERLAY"] = "1"
        command = [self.make_bin, "-C", str(self.cloud), "--no-print-directory", target,
                   f"CLOUD_ROOT={self.cloud}", f"SHARED_ROOT={SHARED_ROOT}", *args]
        sys.stdout.flush()
        return subprocess.run(command, env=env).returncode

    def add_credentials(self, env_name: str) -> None:
        """Copy the env's and account's auth files into the throwaway copy."""
        for rel in (Path(env_name) / "auth.auto.tfvars",
                    Path(env_name) / "data_access" / "auth.auto.tfvars",
                    Path("account") / "auth.auto.tfvars"):
            source, dest = self.real_envs / rel, self.envs / rel
            if not (source.is_file() or source.is_symlink()):
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            if dest.is_symlink() or dest.exists():
                dest.unlink()
            # A real file in the copy (contents read through any link), so
            # nothing the plan step touches points outside it.
            shutil.copyfile(source, dest)
            os.chmod(dest, 0o600)


def _display(rel: str) -> str:
    return f"envs/{rel}"


def _compare(before: dict, after: dict, *, missing_ok: set[str]) -> tuple[list[str], list[str]]:
    """Return (failures, notes) for what a regeneration changed."""
    failures, notes = [], []
    for rel in sorted(set(before) | set(after)):
        old, new = before.get(rel), after.get(rel)
        if old == new:
            continue
        if old is None:
            if str(Path(*Path(rel).parts[1:])) in missing_ok:
                notes.append(f"  NOTE: {_display(rel)} is not committed; the applying commands write it.")
            else:
                failures.append(f"  {_display(rel)}: missing from the commit")
            continue
        if new is None:
            failures.append(f"  {_display(rel)}: committed, but regeneration does not produce it")
            continue
        failures.append(f"  {_display(rel)}: differs from the regenerated file")
        if old[0] == "file" and new[0] == "file":
            diff = list(difflib.unified_diff(
                old[1].decode(errors="replace").splitlines(),
                new[1].decode(errors="replace").splitlines(),
                f"committed/{_display(rel)}", f"regenerated/{_display(rel)}", lineterm=""))
            shown = diff[:DIFF_LINES]
            failures.extend(f"    {line}" for line in shown)
            if len(diff) > len(shown):
                failures.append(f"    ... {len(diff) - len(shown)} more diff line(s)")
    return failures, notes


def _report(label: str, before: dict, after: dict, fix: str) -> None:
    failures, notes = _compare(before, after, missing_ok=SPLIT_FILES)
    for note in notes:
        print(note)
    if failures:
        print(f"ERROR: {label}: committed files differ from what the commit regenerates:", file=sys.stderr)
        for line in failures:
            print(line, file=sys.stderr)
        print(f"  Generated files are never hand-edited. {fix}", file=sys.stderr)
        raise CheckFailed(label)
    print(f"  OK: {label}: committed files match.")


def _promote_check(args) -> None:
    scratch = Scratch(args.cloud_dir, args.makefile, args.make)
    try:
        scratch.add_credentials(args.env)
        before = _snapshot(scratch.envs)
        extra = [f"FROM={args.from_env}"] if args.from_env else []
        extra += [f"CATALOG_MAP={args.catalog_map}"] if args.catalog_map else []
        print("  Running promote-to in a scratch copy; paths and next steps it prints refer to that copy.")
        if scratch.make("promote-to", f"ENV={args.env}", *extra) != 0:
            raise CheckFailed(f"promote-to ENV={args.env} failed in the scratch copy (see above)")
        flags = " ".join(extra)
        _report(f"promote-to ENV={args.env} CHECK=1", before, _snapshot(scratch.envs),
                f"Run make promote-to ENV={args.env}{' ' + flags if flags else ''} and commit the result.")
    finally:
        scratch.close()


def _check(args) -> None:
    env = args.env
    real_env_dir = args.cloud_dir / "envs" / env
    is_deterministic = deterministic(real_env_dir)
    promotes = env_role(real_env_dir) == PROMOTED
    has_generated = (real_env_dir / "generated" / "abac.auto.tfvars").is_file()
    scratch = Scratch(args.cloud_dir, args.makefile, args.make)
    steps = 6

    def step(number: int, title: str) -> None:
        print(f"\n--- check {number}/{steps}: {title} ---")

    try:
        # Validators run throughout the check, not only in the final plan. Give
        # every scratch step the real runtime auth values so an empty-secret
        # warning describes the caller's config. The files remain inside the
        # private throwaway copy and are never included in comparisons. Copied
        # inside the try: a copy that fails partway still leaves no secret in
        # the scratch directory.
        scratch.add_credentials(env)
        step(1, "validate")
        if not has_generated and not (scratch.envs / env / "abac.auto.tfvars").is_file():
            print(f"  NOTE: skipped: envs/{env} has not been generated; run make "
                  f"{'promote-to' if promotes and is_deterministic else 'generate'} ENV={env}.")
        elif not (scratch.envs / env / "abac.auto.tfvars").is_file():
            then = ("the regeneration below splits generated/ and validates every layer" if is_deterministic
                    else "validate-generated below checks generated/")
            print(f"  NOTE: skipped: envs/{env}/abac.auto.tfvars (the split config) is not committed; {then}.")
        elif scratch.make("validate", f"ENV={env}") != 0:
            raise CheckFailed("validate")

        step(2, "generated files are current")
        if not is_deterministic:
            print(f'  NOTE: skipped: envs/{env} is in legacy mode (generated files come from an LLM draft '
                  'and are not reproducible). Set governance_mode = "deterministic" to check them.')
        else:
            if promotes:
                _promote_check(args)
            if has_generated:
                env_dir = scratch.envs / env
                before = _snapshot(scratch.envs)
                if scratch.make("promote", f"ENV={env}") != 0:
                    raise CheckFailed(f"promote ENV={env} (the same-env split) failed")
                # A promoted env records only what promote-to records (agents in agents/
                # files, or a record already committed), so an inline-agent promotion is
                # current without a record and a committed one must still be current.
                if subprocess.run([sys.executable, str(SHARED_ROOT / "scripts" / "sticky_governance.py"),
                                   "record", *(["--if-agent-files"] if promotes else []), "--env-dir", str(env_dir),
                                   "--config", str(env_dir / "generated" / "abac.auto.tfvars")]).returncode != 0:
                    raise CheckFailed("recording the governed tables failed")
                _report(f"generated files (envs/{env})", before, _snapshot(scratch.envs),
                        f"Run make {'promote-to' if promotes else 'generate'} ENV={env} on the latest main "
                        "and commit the result.")
            else:
                print(f"  NOTE: envs/{env}/generated/abac.auto.tfvars is not committed; nothing to regenerate.")

        generated = scratch.envs / env / "generated"
        step(3, "validate-generated")
        if (generated / "abac.auto.tfvars").is_file():
            if scratch.make("validate-generated", f"ENV={env}") != 0:
                raise CheckFailed("validate-generated")
        else:
            print(f"  NOTE: skipped: no envs/{env}/generated/abac.auto.tfvars.")

        step(4, "coverage check (offline)")
        native_tags_pending = (
            _load_hcl(scratch.envs / env / "env.auto.tfvars").get("enable_classification") is True
            and not _load_hcl(generated / "abac.auto.tfvars").get("tag_assignments")
        )
        if not ((generated / "abac.auto.tfvars").is_file() and (generated / "masking_functions.sql").is_file()):
            print(f"  NOTE: skipped: envs/{env}/generated/ has no abac.auto.tfvars and masking_functions.sql.")
        elif native_tags_pending:
            print(f"  NOTE: skipped: envs/{env} takes its tags from native classification and its generated "
                  "config has none yet; release and maintain run the coverage check on live tags.")
        elif scratch.make("coverage-gate", f"ENV={env}") != 0:
            raise CheckFailed("coverage check")

        step(5, "mask-library unit tests")
        tests = SHARED_ROOT / "tests" / "test_mask_library.py"
        has_pytest = subprocess.run([sys.executable, "-c", "import pytest"], capture_output=True).returncode == 0
        if not tests.is_file() or not has_pytest:
            print(f"  NOTE: skipped: {'pytest is not installed' if tests.is_file() else f'{tests} is missing'}.")
        else:
            sys.stdout.flush()
            if subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", str(tests)],
                              cwd=scratch.root, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}).returncode != 0:
                raise CheckFailed("mask-library unit tests")

        step(6, "plan")
        # TODO(rollout step 12, remote state): run a state-aware plan when the
        # env has remote state; no env has it yet.
        print(f"  STRUCTURAL plan: envs/{env} as committed, against EMPTY state (no remote state is "
              "configured). It shows every resource as new and proves the config plans; it is not the "
              "change a release would make. Live tags are not re-read, so new SELECT grants show as withheld.")
        print("  INFO: Genie CAN_RUN withheld in this empty-state pre-release plan is expected; ordinary "
              "plans and applies still fail when CAN_RUN is unexpectedly withheld.")
        # The structural plan doesn't re-read live tags (a plan applies nothing;
        # the gate withholds new grants), so it hands the plan the one-time
        # token that skips the derive (scripts/exposure_token.py).
        from exposure_token import ENV_VAR, mint

        token = mint(scratch.envs / env)
        from scripts.state_layout import STRUCTURAL_CHECK_TOKEN_ENV, mint_structural_check_token
        structural_token = mint_structural_check_token(scratch.envs / env)
        try:
            if scratch.make("plan", f"ENV={env}", extra_env={ENV_VAR: str(token),
                                                             "CLASSIFICATION_READ_ONLY": "1",
                                                             STRUCTURAL_CHECK_TOKEN_ENV: str(structural_token)}) != 0:
                raise CheckFailed("plan")
        finally:
            token.unlink(missing_ok=True)
            structural_token.unlink(missing_ok=True)
    finally:
        scratch.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("check", "promote-check"))
    parser.add_argument("--env", required=True)
    parser.add_argument("--cloud-dir", type=Path, required=True, help="aws/ or azure/ (holds envs/)")
    parser.add_argument("--makefile", type=Path, required=True, help="the cloud Makefile to run in the copy")
    parser.add_argument("--make", default="make", help="make binary")
    parser.add_argument("--from", dest="from_env", default="", help="promote-check: FROM override")
    parser.add_argument("--catalog-map", default="", help="promote-check: CATALOG_MAP override")
    parser.add_argument("--env-dir-override", type=Path, help="a command-line ENV_DIR, refused unless it is envs/<env>")
    args = parser.parse_args(argv)
    args.cloud_dir = args.cloud_dir.resolve()
    if args.env_dir_override and args.env_dir_override.resolve() != args.cloud_dir / "envs" / args.env:
        print(f"ERROR: ENV_DIR={args.env_dir_override} is not envs/{args.env} for ENV={args.env}; drop ENV_DIR "
              "(the check reads this cloud's envs/ only).", file=sys.stderr)
        return 1
    if args.env in RESERVED_ENVS:
        print(f"ERROR: ENV={args.env} is not a workspace env.", file=sys.stderr)
        return 1
    label = f"make check ENV={args.env}" if args.command == "check" else f"make promote-to ENV={args.env} CHECK=1"
    print(f"=== {label} (read-only: runs in a scratch copy; writes nothing under envs/) ===")
    try:
        (_check if args.command == "check" else _promote_check)(args)
    except (CheckFailed, GovernedTablesError) as exc:
        print(f"\n=== {label}: FAILED ({exc}) ===", file=sys.stderr)
        return 1
    if args.command == "check":
        _note_uncommitted_env_files(args.cloud_dir, args.env)
    print(f"\n=== {label}: passed ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
