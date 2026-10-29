#!/usr/bin/env python3
"""make release ENV=<deterministic env> at a terminal: plan, y/N, release, round by round.

A first (or staged) release takes several manifests: imports, each staged
policy phase, the SELECT round, then the workspace. At a terminal, plain
``make release`` runs them for you. Each round:

1. offers the env's current manifest if it can still be released at HEAD
   (round 1 only: what an earlier release-plan wrote), otherwise runs
   ``make release-plan``;
2. shows the manifest and asks ``[y/N]`` (default no);
3. on y, writes the approval record (scripts/approval.py) and releases that
   manifest with ``make _release-manifest MANIFEST=<it> APPROVAL=<record>``;

until the release receipt is written (``=== Release complete (<env>) ===``),
at most MAX_ROUNDS rounds. The loop only runs the primitives in order: every
refusal (provenance, the rehearsal, reader acks, the coverage check, the live
snapshot, locks) still happens inside release-plan and _release-manifest, and
a refusal or a failed apply stops the loop; it never re-plans after one. It
never runs make rehearse or applies the source env: with no rehearsal to
release from, it stops and says so. Acks are never asked: they stay a
committed envs/<env>/ack.txt change. It holds no lock while it waits for an
answer.

The Makefile runs this only when approval.py says a person is at the
terminal; MANIFEST=, CI, no terminal, GENIERAILS_INTERACTIVE=0 and nested
makes release once, exactly as before. There is no flag that answers for you.
"""

from __future__ import annotations

import argparse
import os
import shlex
import signal
import subprocess
import sys
from pathlib import Path

SHARED = Path(__file__).resolve().parents[1]
for _path in (SHARED, SHARED / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from scripts import approval  # noqa: E402
from scripts import release_gate as rg  # noqa: E402

MAX_ROUNDS = 8  # a constant, not a setting: no knob changes how a release is approved

COMPLETE, PHASE = "complete", "phase"

# release-plan refusals that need the person to do something the loop never does for them.
_HINTS = (
    (("reader or weakening check",),
     "Acknowledgements are a reviewed change, never a prompt: add the keys you accept to envs/{env}/ack.txt, "
     "commit and merge it, then re-run make release ENV={env}."),
    (("no passing rehearsal of", "run make rehearse ENV="),
     "make release never rehearses or applies {source}: run make rehearse ENV={source} on this commit, then "
     "make release ENV={env} again."),
    (("the account plan is not empty",),
     "The account layer changes only through its own approved manifest: make account-plan, then make "
     "apply-account; then re-run make release ENV={env}."),
)


def _say(line: str = "") -> None:
    print(line, flush=True)


class Child:
    """Run one make; Ctrl-C reaches it from the terminal, TERM is forwarded; wait for it either way."""

    def __init__(self) -> None:
        self.proc: subprocess.Popen | None = None
        self.interrupted = 0

    def _signal(self, signum, _frame) -> None:
        self.interrupted = signum
        if signum == signal.SIGTERM and self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)

    def run(self, cmd: list[str], *, scan: bool = False) -> tuple[int, str]:
        """(exit code, its stderr when ``scan``); stderr is still shown as it comes."""
        previous = {sig: signal.signal(sig, self._signal) for sig in (signal.SIGINT, signal.SIGTERM)}
        lines: list[str] = []
        try:
            # close_fds=False: make's jobserver descriptors (MAKEFLAGS) must reach the child.
            self.proc = subprocess.Popen(cmd, close_fds=False, stderr=subprocess.PIPE if scan else None,
                                         text=True, errors="replace")
            if scan:
                for line in self.proc.stderr:
                    sys.stderr.write(line)
                    sys.stderr.flush()
                    lines.append(line)
            rc = self.proc.wait()
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
            self.proc = None
        if self.interrupted and rc == 0:
            rc = 128 + self.interrupted
        return rc, "".join(lines)


def _hint(stderr: str, env: str, source: str) -> str | None:
    for needles, text in _HINTS:
        if any(n in stderr for n in needles):
            return text.format(env=env, source=source)
    return None


def _source(env_dir: Path) -> str:
    try:
        return rg.source_env(env_dir)
    except rg.Refused:
        return "<source env>"


def _still_releasable(env_dir: Path, env: str) -> Path | None:
    """The current manifest if release can apply it now (round 1), else None (say why when there is one)."""
    try:
        if rg.current_manifest_id(rg.cloud_dir_of(env_dir), env) is None:
            return None
        path = rg.current_manifest(env_dir, env)
        rg.manifest_check(env_dir, env, path)
    except rg.Refused as exc:
        why = str(exc).splitlines()
        reasons = [line.strip()[2:] for line in why if line.strip().startswith("- ")]
        _say(f"The current manifest can't be released any more ({'; '.join(reasons) or why[0]}): planning a new one.")
        return None
    return path


def _outcome(env_dir: Path, env: str, manifest_id: str) -> str | None:
    """What releasing the manifest wrote: its release receipt (complete) or its phase record (phase)."""
    cloud_dir = rg.cloud_dir_of(env_dir)
    if any((r.get("manifest") or {}).get("id") == manifest_id for r in rg.release_receipts(cloud_dir, env)):
        return COMPLETE
    if (rg.env_state(cloud_dir, env) / "receipts" / "phases" / f"{manifest_id}.json").is_file():
        return PHASE
    return None


def release(env: str, env_dir: Path, make: list[str], verify_key_column: str = "") -> int:
    env_dir = Path(env_dir).resolve()
    if not approval.interactive():
        print(f"ERROR: {approval.NOT_A_TERMINAL}", file=sys.stderr)
        return 1
    # This run's nonce: only the records its own prompt writes count (scripts/approval.py).
    run = os.environ[approval.RUN_ENV] = approval.new_run()
    child, source = Child(), _source(env_dir)
    base = [*make, "--no-print-directory"]
    env_args = [f"ENV={env}", f"ENV_DIR={env_dir}"]
    current = _still_releasable(env_dir, env)
    for round_no in range(1, MAX_ROUNDS + 1):
        if current is None:
            _say(f"=== Release round {round_no} ({env}): make release-plan ENV={env} ===")
            before = rg.current_manifest_id(rg.cloud_dir_of(env_dir), env)
            rc, stderr = child.run([*base, "release-plan", *env_args], scan=True)
            if rc != 0:
                hint = _hint(stderr, env, source)
                if hint:
                    print(f"make release ENV={env}: stopped: {hint}", file=sys.stderr)
                print(f"make release ENV={env}: release-plan refused or failed (above); round {round_no} "
                      "released nothing.", file=sys.stderr)
                return rc
            current_id = rg.current_manifest_id(rg.cloud_dir_of(env_dir), env)
            if current_id is None or current_id == before:
                print(f"ERROR: release-plan ENV={env} exited 0 but wrote no new current manifest; nothing was "
                      "released.", file=sys.stderr)
                return 1
            current = rg.manifests_dir(rg.cloud_dir_of(env_dir), env) / current_id / "manifest.json"
        else:
            _say(f"=== Release round {round_no} ({env}): the current manifest, planned earlier, can still be "
                 "released ===")
        try:
            manifest = rg.load_manifest(current, env)
        except rg.Refused as exc:
            print(f"ERROR: {exc}; nothing was released.", file=sys.stderr)
            return 1
        _say()
        for line in rg.manifest_summary(current, manifest):
            _say(line)
        question = (f"Approve and release manifest {manifest['id']} (commit {manifest['commit'][:12]}, round "
                    f"{round_no} of at most {MAX_ROUNDS})?")
        if not approval.ask(question):
            _say(f"Not released. Manifest {manifest['id']} stays current: re-run make release ENV={env} to be "
                 f"asked again; a pipeline releases it with make release ENV={env} MANIFEST={current}")
            return 1
        record = approval.record(env_dir, env, current, question, run=run, round_no=round_no)
        _say(f"Approved at this terminal: {record}")
        cmd = [*base, "_release-manifest", *env_args, f"MANIFEST={current}", f"APPROVAL={record}"]
        if verify_key_column:
            cmd.append(f"VERIFY_KEY_COLUMN={verify_key_column}")
        rc, _ = child.run(cmd)
        if rc != 0:
            # A refused or failed apply needs a person: never re-plan past it.
            print(f"make release ENV={env}: round {round_no} stopped (above); nothing after it was planned or "
                  f"applied. make status ENV={env} shows where the release is.", file=sys.stderr)
            return rc
        outcome = _outcome(env_dir, env, manifest["id"])
        if outcome == COMPLETE:
            _say()
            _say(f"=== Release complete ({env}) after {round_no} round(s), each approved at this terminal ===")
            return 0
        if outcome is None:
            print(f"ERROR: make release ENV={env}: round {round_no} exited 0 but wrote neither a phase record nor "
                  f"the release receipt; run make status ENV={env}.", file=sys.stderr)
            return 1
        _say()
        _say(f"=== Round {round_no} applied (one phase of the release); planning the next round ===")
        current = None
    print(f"make release ENV={env}: stopped after {MAX_ROUNDS} rounds without the release completing; run make "
          f"status ENV={env}.", file=sys.stderr)
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0], allow_abbrev=False)
    parser.add_argument("--env", required=True)
    parser.add_argument("--env-dir", type=Path, required=True)
    parser.add_argument("--make", required=True, help="$(MAKE), as the recipe would run it")
    parser.add_argument("--verify-key-column", default="")
    args = parser.parse_args(argv)
    try:
        return release(args.env, args.env_dir, shlex.split(args.make), args.verify_key_column)
    except (approval.Refused, rg.Refused) as exc:
        print(f"ERROR: {exc}; nothing more was released.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
