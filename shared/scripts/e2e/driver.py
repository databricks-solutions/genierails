#!/usr/bin/env python3
"""Run the deterministic-governance acceptance plan through public Make targets.

The runner is deliberately data driven.  Live discovery is read-only; every
mutation is delegated to a documented public Make target.  ``--dry-run`` does
not load authentication, run git, invoke make, or contact Databricks.
"""
from __future__ import annotations

import argparse
import contextlib
import dataclasses
import datetime as dt
import fnmatch
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import signal
import time
from typing import Any, Callable, ClassVar, Iterable, Iterator, Mapping, Protocol, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from governance_policies import FAILSAFE_TREATMENT, is_never_raw, parse_treatment_value


DESIGN_SOURCE = "shared/docs/design/deterministic-governance.md"
SECRET_KEYS = re.compile(r"(?i)(secret|token|password|private[_-]?key)")
TOKEN_VALUE = re.compile(r"(?i)\b(?:dapi|dkea|dose|ghp_|gho_|github_pat_)[A-Za-z0-9_-]{8,}\b")
PLACEHOLDER = re.compile(r"\{([a-z_][a-z0-9_]*)\}")
# The driver owns this remote and its main branch: local merges are pushed here
# and every make call uses it as RELEASE_REMOTE, so release-plan's "merged on
# main" provenance rule holds without touching origin.
E2E_REMOTE = "genierails-e2e"
E2E_BRANCH_PREFIX = "genierails-e2e/"
MAX_RELEASE_PHASES = 8
MANUAL_RESTORE_COMMANDS = (
    "make rehearse ENV=dev",
    "make release-plan ENV=prod",
    "make release ENV=prod MANIFEST=<path-from-release-plan>",
    "make maintain ENV=prod",
)
ANOTHER_RELEASE_PHASE = re.compile(r"(?:Release phase applied|needs? another release-plan)", re.I)
IMPORT_UNSET_LOG = re.compile(
    r"(?im)(?:Cleared tag gr_treatment|gr_treatment.*(?:clearing tag|UNSET TAGS)|UNSET TAGS.*gr_treatment)"
)
IMPORT_UNSET_SOURCE_ANCHORS = ("Cleared tag {tkey} on", "; clearing tag")
TAG_VALUE_MATCH = re.compile(r"hasTagValue\(\s*'gr_treatment'\s*,\s*'([^']*)'\s*\)", re.I)
# PolicyInfo fields in the pinned databricks-sdk. The condition inside each
# MatchColumn is covered by match_columns; PolicyInfo has no top-level condition.
CONDITION_FIELDS = ("when_condition", "match_columns")
ABSENT_CONDITION = "absent"
LEGACY_REDACTED_TREATMENTS = frozenset({"redact", "compensation_redacted"})
GIT_IDENTITY = {"GIT_AUTHOR_NAME": "genierails-e2e", "GIT_AUTHOR_EMAIL": "genierails-e2e@localhost",
                "GIT_COMMITTER_NAME": "genierails-e2e", "GIT_COMMITTER_EMAIL": "genierails-e2e@localhost"}
# Targets that apply to a workspace/account: the checkout must be clean and
# pinned before them, and running one means the scenario must be restored.
APPLYING = frozenset({"rehearse", "release-plan", "release", "maintain", "detach-agent",
                      "apply-account"})
# Targets that author files under envs/ (committed later by a local merge) and,
# for ungovern in a rehearse env, also apply.
AUTHORING = frozenset({"setup", "generate", "promote-to", "capture", "ungovern", "scaffold-treatments"})
# Scenarios whose purpose is to remove or loosen protection mid-scenario. The
# post-restore check still applies to them.
WEAKENING_EXPECTED = frozenset({"s8", "s9"})
# The upstream .gitignore ignores envs/, so a local merge can only commit
# authored changes in a checkout whose <cloud>/.gitignore has the block
# `make setup GITIGNORE=customer` writes (scripts/customer_gitignore.py BEGIN).
CUSTOMER_GITIGNORE_MARKER = "# >>> GenieRails customer repo (make setup GITIGNORE=customer"
CUSTOMER_ENVS = ("dev", "prod")


@dataclasses.dataclass(frozen=True)
class Step:
    name: str
    command: tuple[str, ...] = ()
    env: str | None = None
    target: str | None = None
    human_prompt: str | None = None
    expect_failure: bool = False
    refusal: str | None = None
    extra_env: tuple[tuple[str, str | None], ...] = ()
    action: str | None = None
    value: str | None = None
    # Typed checks (GrantDelta, AccessLevel, ...) evaluated over this step.
    assertions: tuple[Any, ...] = ()
    refusal_in_ci: bool = True


@dataclasses.dataclass(frozen=True)
class Scenario:
    key: str
    title: str
    preconditions: tuple[str, ...]
    steps: tuple[Step, ...]
    pass_criteria: tuple[str, ...]
    restore: tuple[str, ...]
    requires: tuple[str, ...] = ()
    known_failure: str | None = None
    read_only: bool = False
    # Typed checks evaluated over the whole scenario (baseline to final snapshot).
    checks: tuple[Any, ...] = ()


@dataclasses.dataclass(frozen=True)
class Adoption:
    old: str
    new: str
    legacy_function: str
    # Exact live changes printed by the reviewed migrate-plan.  The table is
    # implied by the adopted column; no blanket policy/function exception is
    # possible.
    policy_conditions: tuple[tuple[str, str, str], ...] = ()
    mask_definitions: tuple[tuple[str, str], ...] = ()


# Typed checks. Each has an evaluator in EVALUATORS that runs during live runs;
# --dry-run fails on any assertion without one. String fields may hold
# {placeholders} from --value, and the fields of a grant may hold
# comma-separated lists (one grant per combination).
ENVS = ("dev", "prod")
SNAPSHOT_COMPONENTS = ("uc", "agents", "state_serials", "receipts")
ACCESS_LEVELS = ("raw", "partial", "full", "redacted", "masked")
# An access target meaning every column the scenario's migrate-plan adopted.
ADOPTED_COLUMNS = "@migrate-plan"
Grant = tuple[str, str, str]  # (principal, privilege, securable)


@dataclasses.dataclass(frozen=True)
class GrantDelta:
    """The exact grant change in ``env``: every ``added`` grant is new, nothing
    else is granted except ``may_add`` (named grants a fixture may already hold),
    and nothing is revoked except ``revoked``. ``added`` and ``may_add`` are also
    the only widening the per-step protection checks permit. The securable
    ``agent:<key>`` is that Genie agent (CAN_RUN); USE_CATALOG and USE_SCHEMA
    entries name a table and mean its catalog or schema."""
    env: str
    added: tuple[Grant, ...] = ()
    may_add: tuple[Grant, ...] = ()
    revoked: tuple[Grant, ...] = ()
    kind: ClassVar[str] = "grant-delta"
    snapshots: ClassVar[bool] = True


@dataclasses.dataclass(frozen=True)
class AccessLevel:
    """verify-access proved ``principal`` ("*": every principal it checked) sees
    ``level`` in every check of ``target`` (a table or a column). ``redacted`` is
    ``full`` through a never-raw treatment; ``masked`` is anything but raw."""
    env: str
    target: str
    principal: str
    level: str
    kind: ClassVar[str] = "access-level"
    snapshots: ClassVar[bool] = False


@dataclasses.dataclass(frozen=True)
class AccessVerified:
    """verify-access checked ``target`` (a table, a column or ADOPTED_COLUMNS) and
    every principal saw exactly the tier the spec expects for it."""
    env: str
    target: str
    kind: ClassVar[str] = "access-verified"
    snapshots: ClassVar[bool] = False


@dataclasses.dataclass(frozen=True)
class DiffLimitedTo:
    """Only the selected resources of a snapshot component changed in ``env``.
    Selectors: agent keys (agents), securables or function names, each covering
    what is under it (uc), or paths (state_serials, receipts); globs allowed."""
    env: str
    component: str
    selectors: tuple[str, ...]
    kind: ClassVar[str] = "diff-limited-to"
    snapshots: ClassVar[bool] = True


@dataclasses.dataclass(frozen=True)
class Unchanged:
    """No selected resource of a snapshot component changed in ``env``."""
    env: str
    component: str
    selectors: tuple[str, ...] = ("*",)
    kind: ClassVar[str] = "unchanged"
    snapshots: ClassVar[bool] = True


@dataclasses.dataclass(frozen=True)
class AgentIdUnchanged:
    """Agent ``agent`` keeps exactly its space ID(s): it was not recreated, so
    the conversations that live in that space are retained."""
    env: str
    agent: str
    kind: ClassVar[str] = "agent-id-unchanged"
    snapshots: ClassVar[bool] = True


def with_checks(steps: Sequence[Step], name: str, *checks: Any) -> tuple[Step, ...]:
    """``steps`` with ``checks`` added to the step called ``name``."""
    if name not in {step.name for step in steps}: raise ValueError(f"no step named {name!r}")
    return tuple(dataclasses.replace(step, assertions=step.assertions + checks) if step.name == name else step
                 for step in steps)


def mk(name: str, target: str, env: str | None = None, *,
       args: Sequence[str] = (), fail: bool = False, refusal: str | None = None,
       extra_env: Mapping[str, str | None] | None = None,
       assertions: Sequence[Any] = (), refusal_in_ci: bool = True) -> Step:
    command = ("make", target) + ((f"ENV={env}",) if env else ()) + tuple(args)
    return Step(name, command, env, target, expect_failure=fail, refusal=refusal,
                extra_env=tuple((extra_env or {}).items()),
                assertions=tuple(assertions), refusal_in_ci=refusal_in_ci)


def action(name: str, kind: str, value: str | None = None) -> Step:
    return Step(name=name, action=kind, value=value)


# Each refusal pattern is the diagnostic the implementing target prints, so an
# unrelated non-zero exit (a typo, a crash, a timeout) is never a passing refusal.
REFUSALS = {
    "covered": r"refusing to ungovern \S+: a configured agent still uses it",
    "plan_provenance": r"release-plan refused:",
    "release_provenance": r"(make release refused|is not a release manifest|is a manifest for \S+, not)",
    "maintain_provenance": r"make maintain refused",
    "ci": r"applying targets cannot run in CI",
    "weakening": r"(refused by the reader or weakening check|unacknowledged (weakening|reader) )",
    "detach": r"ERROR: detach: .*?(?:not found: detach-agent works on an agent file|is not detached)",
    "tamper": r"(make release refused: \S+ no longer matches|is not a release manifest|is a manifest for \S+, not)",
    "stale": r"committed files differ from what the commit regenerates",
}
REFUSAL_SOURCE_ANCHORS: Mapping[str, tuple[str, str]] = {
    REFUSALS["covered"]: ("shared/scripts/sticky_governance.py", "refusing to ungovern"),
    REFUSALS["plan_provenance"]: ("shared/scripts/release_gate.py", "release-plan refused:"),
    REFUSALS["release_provenance"]: ("shared/scripts/release_gate.py", "make release refused:"),
    REFUSALS["maintain_provenance"]: ("shared/scripts/release_gate.py", "make maintain refused"),
    REFUSALS["ci"]: ("shared/Makefile.shared", "applying targets cannot run in CI"),
    REFUSALS["weakening"]: ("shared/Makefile.shared", "refused by the reader or weakening check"),
    REFUSALS["detach"]: ("shared/scripts/agent_ownership.py", "not found: detach-agent works on an agent file"),
    REFUSALS["tamper"]: ("shared/scripts/release_gate.py", "no longer matches:"),
    REFUSALS["stale"]: ("shared/scripts/check_env.py", "committed files differ from what the commit regenerates"),
}
CI_MARKERS = (
    "CI", "TF_BUILD", "GITHUB_ACTIONS", "JENKINS_URL", "GITLAB_CI", "BUILDKITE", "CIRCLECI",
    "BUILD_BUILDID", "SYSTEM_TEAMFOUNDATIONCOLLECTIONURI", "TEAMCITY_VERSION", "TRAVIS",
    "BITBUCKET_BUILD_NUMBER", "CODEBUILD_BUILD_ID", "DRONE", "SEMAPHORE", "APPVEYOR",
)
CI_ENV = {"CI": "true", "GENIERAILS_ALLOW_CI_APPLY": None}
NON_CI_ENV = {key: None for key in (*CI_MARKERS, "GENIERAILS_ALLOW_CI_APPLY")}


AUTHOR_MERGE = action("Commit and merge authored changes on the driver's local main", "local_merge")
DAY2 = (
    mk("PR check (dev)", "check", "dev"),
    mk("Dev rehearsal", "rehearse", "dev"),
    mk("Promote deterministic output", "promote-to", "prod"),
    action("Commit and merge promotion on the driver's local main", "local_merge"),
    mk("PR check (prod)", "check", "prod"),
    mk("Promotion determinism", "promote-to", "prod", args=("CHECK=1",)),
    mk("Rehearse promotion merge", "rehearse", "dev"),
    mk("Create immutable release manifest", "release-plan", "prod"),
    mk("Apply approved manifest", "release", "prod", args=("MANIFEST={manifest}",)),
    mk("Governance-only maintenance", "maintain", "prod"),
)


def _scenario(key: str, title: str, pre: Sequence[str], lead: Sequence[Step], passed: Sequence[str],
              restore: Sequence[str], requires: Sequence[str] = (), known_failure: str | None = None,
              read_only: bool = False, checks: Sequence[Any] = ()) -> Scenario:
    return Scenario(key, title, tuple(pre), tuple(lead), tuple(passed), tuple(restore), tuple(requires),
                    known_failure, read_only, tuple(checks))


def _business_grants(principals: str, tables: str) -> tuple[Grant, ...]:
    """The usage grants a table's SELECT needs, which a fixture may already hold."""
    return ((principals, "USE_CATALOG", tables), (principals, "USE_SCHEMA", tables))


APPLY_RELEASE = "Apply approved manifest"
# The champion writes dev agent A's file (envs/dev/agents/<agent_a>.auto.tfvars)
# from these required values: who may run an agent is its acl_groups (design
# decision 4; a missing value stops make generate). agent_a must be its own
# Terraform key, so ``agent:{agent_a}`` names the same agent in dev and prod.
AGENT_FILE_VALUES = ("agent_a", "dev_agent_id", "acl_groups")
AGENT_KEY = re.compile(r"[a-z0-9]+(?:_[a-z0-9]+)*")
AGENT_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")
GROUP_NAME = re.compile(r"[^\s\"'<>{}\[\],#]+(?: [^\s\"'<>{}\[\],#]+)*")


SCENARIOS: tuple[Scenario, ...] = (
    _scenario("champion", "First champion deployment", (
        "Fresh prefixed dev agent A (ID dev_agent_id) and matching test-only dev/prod tables exist.",
        "Catalog mapping, tier groups, acl_groups (who may run A) and external state directory are reviewed.",
    ), (
        mk("Scaffold dev environment", "setup", "dev"),
        action("Write dev agent A's file: genie_space_id and acl_groups", "write_agent_file"),
        Step("Review dev classification", human_prompt="In dev Catalog Explorer review classification, exclude false positives and enable auto-tagging."),
        mk("Generate reviewed dev configuration", "generate", "dev"),
        AUTHOR_MERGE,
    ) + DAY2[:3] + (
        Step("Review prod classification", human_prompt="In prod Catalog Explorer review classification and confirm expected_classification.json."),
    ) + DAY2[3:], (
        "Prod grants, masks, tags, agent export/ACL and receipts match committed governance.",
        "Second release-plan and maintain are idempotent; agent ID and conversations are stable.",
    ), ("Release delete=true for disposable agents; remove governed fixtures through reviewed lifecycle changes.",),
        checks=(
            GrantDelta("prod", added=(("{acl_groups}", "CAN_RUN", "agent:{agent_a}"),
                                      ("{acl_groups}", "SELECT", "{prod_tables}")),
                       may_add=_business_grants("{acl_groups}", "{prod_tables}")),
            # Dev A exists before the run, so its access may already be in place.
            GrantDelta("dev", may_add=(("{acl_groups}", "CAN_RUN", "agent:{agent_a}"),
                                       ("{acl_groups}", "SELECT", "{dev_tables}"),
                                       *_business_grants("{acl_groups}", "{dev_tables}"))),
        )),
    _scenario("s1", "Promote A while B has unfinished dev edits", (
        "A and B have released captured baselines; make an unmistakable UI-only edit to dev B.",
    ), (Step("Curate only A", human_prompt="Curate dev A and leave B unfinished."),
        mk("Capture A only", "capture", "dev", args=("SPACE={agent_a}",)), AUTHOR_MERGE) + DAY2, (
        "Only A changes; dev B remains UI-edited and prod B stays byte-identical with stable conversations.",
    ), ("Revert/capture A through lifecycle; manually discard or separately capture B edit.",),
        checks=(DiffLimitedTo("dev", "agents", ("{agent_a}",)), DiffLimitedTo("prod", "agents", ("{agent_a}",)),
                AgentIdUnchanged("prod", "{agent_a}"))),
    _scenario("s2", "Mask-only change while B is mid-curation", ("Begin an uncaptured edit to dev B; review a mask version/override change.",),
        (Step("Leave B mid-curation", human_prompt="Leave B's UI edit uncaptured and apply the reviewed config edit."),
         mk("Regenerate governance", "generate", "dev"), AUTHOR_MERGE)
        + with_checks(DAY2, APPLY_RELEASE, AccessVerified("prod", "{mask_column}")),
        ("Only governance changes; B config, ID and conversations do not change; staged replacement never exposes raw data.",),
        ("Revert governance change through the same lifecycle.",), ("Staged policy operation may require step 6 operation targets.",),
        checks=(Unchanged("dev", "agents"), Unchanged("prod", "agents"))),
    _scenario("s3", "Agent adds a new table", ("Create uniquely prefixed, classified dev/prod test table pair.",),
        (Step("Add table to A", human_prompt="Add the dev table to A in the UI and review classification."),
         mk("Capture A", "capture", "dev", args=("SPACE={agent_a}",)), mk("Generate governance", "generate", "dev"),
         AUTHOR_MERGE) + with_checks(DAY2, APPLY_RELEASE, AccessVerified("prod", "{prod_table}")),
        ("Unreadable table is refused; once readers are reviewed, prod is masked before access and identifiers are remapped.",),
        ("Remove table from A, capture/release, ungovern, then drop disposable fixtures.",),
        checks=(
            # new_table_principals: A's acl_groups plus the table's reviewed readers.
            GrantDelta("prod", added=(("{new_table_principals}", "SELECT", "{prod_table}"),),
                       may_add=_business_grants("{new_table_principals}", "{prod_table}")),
            GrantDelta("dev", may_add=(("{new_table_principals}", "SELECT", "{dev_table}"),
                                       *_business_grants("{new_table_principals}", "{dev_table}"))),
            DiffLimitedTo("prod", "agents", ("{agent_a}",)), AgentIdUnchanged("prod", "{agent_a}"),
        )),
    _scenario("s4", "New sensitive column appears in prod", ("Add a known-class prefixed column via an approved external fixture migration.",),
        (mk("Maintain released commit", "maintain", "prod"),
         mk("Verify every tier of the new column", "verify-access", "prod",
            assertions=(AccessVerified("prod", "{new_column}"),)),
         mk("Prove idempotence", "maintain", "prod")),
        ("Known class is masked in one run for every tier; no Genie update occurs.",),
        ("Remove disposable column through its owning migration.",), checks=(Unchanged("prod", "agents"),)),
    _scenario("s5", "Unmapped class fail-safe", ("Add prefixed unknown_secret with an unmapped class tag; no acknowledgement.",),
        (mk("Plan protected release", "release-plan", "prod"),
         mk("Apply protected release", "release", "prod", args=("MANIFEST={manifest}",),
            assertions=(AccessLevel("prod", "{secret_column}", "*", "redacted"),)),
         mk("Maintain fail-safe", "maintain", "prod"),
         mk("Scaffold reviewed treatment", "scaffold-treatments", "dev", args=("TREATMENT={treatment}",)),
         AUTHOR_MERGE),
        ("Gap is reported and every principal sees never-raw redaction; new access is blocked.",),
        ("Promote a reviewed mapping or remove the fixture tag/column; prove no stale fail-safe policy.",),
        checks=(GrantDelta("dev"), GrantDelta("prod"))),
    _scenario("s6", "table_readers widening requires acknowledgement", ("Prepare a prod-owned reader widening with stale coverage and no matching ack.",),
        (mk("Refuse stale/unacknowledged widening", "release-plan", "prod", fail=True, refusal=REFUSALS["weakening"], refusal_in_ci=False),
         action("Write exact reviewed acknowledgement", "add_ack", "ack_entry"),
         action("Commit and merge acknowledgement on the driver's local main", "local_merge")) + DAY2,
        ("Negative plan creates no applicable manifest; exact reviewed ack permits only the named grant.",),
        ("Revert reader and acknowledgement through lifecycle; verify only GenieRails-owned SELECT is revoked.",),
        # reader_principal and reader_table come from ack_entry (prod:reader:<table>:<principal>).
        checks=(GrantDelta("dev"),
                GrantDelta("prod", added=(("{reader_principal}", "SELECT", "{reader_table}"),),
                           may_add=_business_grants("{reader_principal}", "{reader_table}")))),
    _scenario("s7", "Detach agent", ("A is released, detached=true and acl_groups=[] have been released.",),
        (mk("Refuse wrong commit/agent", "detach-agent", "prod", args=("SPACE={wrong_agent}",), fail=True, refusal=REFUSALS["detach"], refusal_in_ci=False),
         mk("Detach A state only", "detach-agent", "prod", args=("SPACE={agent_a}",))) + DAY2[:2],
        ("Only A resource addresses leave state; live space, conversations, grants and masks remain.",),
        ("Use documented adoption/reattachment; otherwise retain disposable agent for explicit cleanup.",),
        checks=(GrantDelta("prod"), Unchanged("prod", "uc"), Unchanged("prod", "agents"),
                AgentIdUnchanged("prod", "{agent_a}"))),
    _scenario("s8", "Stop governing a table", ("Disposable table is in governed list; prepare each covering-source negative case.",),
        (mk("Refuse while covered", "ungovern", "dev", args=("TABLE={table}", "YES=1"), fail=True, refusal=REFUSALS["covered"], refusal_in_ci=False),
         mk("Edit governed list", "ungovern", "dev", args=("TABLE={uncovered_table}", "YES=1")), AUTHOR_MERGE) + DAY2,
        ("Every coverage source is named/refused; only reviewed lifecycle removes policies/tags.",),
        ("Re-add via uc_tables/agent/readers and lifecycle, or drop disposable table.",)),
    _scenario("s9", "Rollback", ("R1 and visible-change R2 are released; exact prod rollback ack is reviewed.",), DAY2,
        ("Bad/missing ack refuses before mutation; exact ack restores R1 behavior without agent recreation/raw exposure.",),
        ("Forward-fix through lifecycle and remove obsolete acknowledgement.",)),
    _scenario("s10", "Stale promotion PR refusal", ("Create promotion at M1, advance main to M2 and leave promotion stale.",),
        (mk("Refuse stale generated output", "promote-to", "prod", args=("CHECK=1",), fail=True, refusal=REFUSALS["stale"]),
         mk("Refuse stale prod check", "check", "prod", fail=True, refusal=REFUSALS["stale"])),
        ("Scratch regeneration differs and checks fail without workspace mutation.",), ("Delete simulator branches/worktrees.",),
        read_only=True),
    _scenario("s11", "Release refuses unrehearsed or older commit", ("Fixture receipts model U, N and ancestor O around last release R.",),
        (action("Checkout unrehearsed U", "checkout_value", "u"),
         mk("Refuse candidate provenance", "release-plan", "prod", fail=True, refusal=REFUSALS["plan_provenance"], refusal_in_ci=False),
         action("Checkout older O", "checkout_value", "o"),
         mk("Refuse foreign manifest", "release", "prod", args=("MANIFEST={manifest}",), fail=True, refusal=REFUSALS["release_provenance"], refusal_in_ci=False)),
        ("Unrehearsed, dirty, off-main, older and mismatched-manifest cases refuse with no mutation.",), ("Remove negative-case branches only.",),
        read_only=True),
    _scenario("s12", "Maintain refuses off released commit", ("Release receipt points to R while main is R+1.",),
        (action("Checkout R+1", "checkout_value", "r_plus_1"),
         mk("Refuse R+1", "maintain", "prod", fail=True, refusal=REFUSALS["maintain_provenance"], refusal_in_ci=False),
         action("Checkout exact release R", "checkout_value", "r"), mk("Maintain exact R", "maintain", "prod")),
        ("Off-receipt/dirty/digest mismatch refuse before mutation; exact R is governance-only.",), ("Return persistent checkout to requested commit.",)),
    _scenario("s13", "CI apply guard", ("No authorized pipeline provenance; CI=true is set by the scenario fixture.",),
        tuple(mk(f"Refuse CI {t}", t, "prod",
                 args=(("SPACE=ci-guard-fixture",) if t == "detach-agent" else ()), fail=True, refusal=REFUSALS["ci"],
                 extra_env=CI_ENV)
              for t in ("release-plan", "maintain", "detach-agent")) +
        # The guard refuses before MANIFEST is read; the path is a fixed, real
        # value so no placeholder ever reaches make.
        (mk("Refuse CI release", "release", "prod", args=("MANIFEST=/tmp/genierails-ci-guard-fixture.json",),
            fail=True, refusal=REFUSALS["ci"], extra_env=CI_ENV),) +
        (mk("Refuse CI account apply", "apply-account", fail=True, refusal=REFUSALS["ci"], extra_env=CI_ENV),),
        ("Every unauthorized apply fails with zero API calls and unchanged state/locks; authoring/check remain usable.",), ("Unset CI markers.",),
        read_only=True),
    _scenario("s14", "Governance-only environment", ("Test env has prefixed uc_tables, no agent files/IDs, initially no table_readers.",),
        (mk("Setup", "setup", "dev"), mk("Generate governance only", "generate", "dev"), AUTHOR_MERGE) + DAY2,
        ("No Genie resource/CAN_RUN is created; first run grants no SELECT, reviewed second run grants only table_readers.",),
        ("Remove readers, ungovern, then remove disposable tables.",),
        # The run models the first, reader-less release: no grant changes at all.
        checks=(GrantDelta("dev"), GrantDelta("prod"), Unchanged("dev", "agents"), Unchanged("prod", "agents"))),
    _scenario("s15", "Two PRs merged then one promotion", ("X changes A; Y changes governance/B; ordered merge commits are available.",),
        (action("Checkout merge X", "checkout_value", "x"), mk("Rehearse X", "rehearse", "dev"),
         action("Checkout merge Y", "checkout_value", "y"), mk("Rehearse Y", "rehearse", "dev")) + DAY2[2:],
        ("Append-only receipts exist for X/Y/promotion; one cumulative prod release has no missing/duplicate change.",),
        ("Cumulative revert or ordered reverts followed by one promotion.",)),
    _scenario("s16", "Prod UI drift overwritten; conversations retained", ("Released prod A has a conversation and a recorded unsupported UI edit.",),
        (Step("Make prod drift", human_prompt="Record A export/conversation IDs and make a recognizable prod UI edit."),) + DAY2,
        ("Overwrite report names drift; config equals Git after remap; ID/conversations remain; no recreation.",),
        ("Restore desired content via dev UI, capture and lifecycle.",),
        checks=(DiffLimitedTo("prod", "agents", ("{agent_a}",)), AgentIdUnchanged("prod", "{agent_a}"))),
    _scenario("manifest", "Manifest tamper and concurrency refusals", ("Disposable fixtures and an approved manifest exist.",),
        (mk("Refuse tampered manifest", "release", "prod", args=("MANIFEST={tampered_manifest}",), fail=True, refusal=REFUSALS["tamper"], refusal_in_ci=False),),
        ("Commit, plan bytes, serial, live snapshot and kept-name changes each invalidate the old manifest; locks serialize applies.",),
        ("Remove each external grant/mask through its owning fixture immediately.",), read_only=True),
    _scenario("migration", "Legacy migration operations", ("Reviewed report-only legacy fixture and revocation report exist.",),
        (mk("Migration report", "migrate-plan", "prod"),),
        ("Every column/reader consequence and proposed table_readers entry is reported without mutation.",),
        ("Nothing to restore; migrate-plan is report-only.",), read_only=True),
    _scenario("migration-cutover", "Legacy migration cutover", (
        "Reviewed pending legacy migration fixture exists in dev and prod.",
    ), (
        mk("Review dev migration mapping", "migrate-plan", "dev"),
        mk("Review prod migration mapping", "migrate-plan", "prod"),
        mk("Rehearse dev adoption", "rehearse", "dev"),
        mk("Plan prod adopt-only release", "release-plan", "prod"),
        mk("Apply prod adopt-only release", "release", "prod", args=("MANIFEST={manifest}",),
           assertions=(AccessVerified("prod", ADOPTED_COLUMNS),)),
    ), ("Legacy values are adopted into deterministic values without weaker protection or raw exposure.",),
       ("Forward-release the pinned baseline through the same bounded staged lifecycle.",)),
)

# Explicit top-level snapshot components each scenario is allowed to change.
# Any component not named here is an assertion failure, not just report prose.
_ALL_COMPONENTS = frozenset({"uc", "agents", "state_serials", "receipts"})
ALLOWED_CHANGES: Mapping[str, frozenset[str]] = {
    "champion": _ALL_COMPONENTS, "s1": _ALL_COMPONENTS, "s2": frozenset({"uc", "state_serials", "receipts"}),
    "s3": _ALL_COMPONENTS, "s4": frozenset({"uc", "state_serials", "receipts"}),
    "s5": frozenset({"uc", "state_serials", "receipts"}), "s6": _ALL_COMPONENTS,
    "s7": _ALL_COMPONENTS, "s8": frozenset({"uc", "state_serials", "receipts"}), "s9": _ALL_COMPONENTS,
    "s10": frozenset(), "s11": frozenset(), "s12": frozenset({"uc", "state_serials", "receipts"}),
    "s13": frozenset(), "s14": frozenset({"uc", "state_serials", "receipts"}), "s15": _ALL_COMPONENTS,
    "s16": _ALL_COMPONENTS, "manifest": frozenset(),
    "migration": frozenset(), "migration-cutover": frozenset({"uc", "state_serials", "receipts"}),
}


class SnapshotProvider(Protocol):
    def capture(self, env: str) -> Mapping[str, Any]: ...


class HumanAbort(Exception):
    """stdin closed at a human gate: the scenario is NOT RUN, never skipped past."""


def canonical(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): canonical(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        normalized = [canonical(v) for v in value]
        return sorted(normalized, key=lambda v: json.dumps(v, sort_keys=True, default=str))
    return value


def snapshot_diff(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    """Return stable top-level changes; ordering-only differences disappear."""
    left, right = canonical(before), canonical(after)
    keys = sorted(set(left) | set(right))
    return {key: {"before": left.get(key), "after": right.get(key)}
            for key in keys if left.get(key) != right.get(key)}


def redact(value: Any, secrets: Iterable[str] = ()) -> Any:
    secrets = tuple(secrets)
    if isinstance(value, Mapping):
        return {str(k): "<redacted>" if SECRET_KEYS.search(str(k)) else redact(v, secrets) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(v, secrets) for v in value]
    if isinstance(value, str):
        return redact_text(value, secrets)
    return value


def redact_text(text: str, secrets: Iterable[str] = ()) -> str:
    for secret in sorted((s for s in secrets if s), key=len, reverse=True):
        text = text.replace(secret, "<redacted>")
    text = re.sub(r'(?i)("[^"]*(?:token|secret|password|authorization)[^"]*"\s*:\s*)"[^"]*"', r'\1"<redacted>"', text)
    text = re.sub(r"(?i)(Authorization\s*[:=]\s*)\S+(?:\s+\S+)?", r"\1<redacted>", text)
    text = re.sub(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]{8,}", r"\1 <redacted>", text)
    text = re.sub(r"(?i)\b([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD)[A-Z0-9_]*\s*[=:]\s*)\"?[^\s\"]+\"?", r"\1<redacted>", text)
    return TOKEN_VALUE.sub("<redacted>", text)


class CommandRunner:
    def __init__(self, cwd: Path, log_dir: Path, extra_env: Mapping[str, str] | None = None,
                 secrets: Iterable[str] = (), timeout: int = 1800):
        self.cwd, self.log_dir = cwd, log_dir
        self.env = {**os.environ, **(extra_env or {})}
        self.secrets, self.timeout = tuple(secrets), timeout
        self.count = 0

    def run(self, argv: Sequence[str], label: str,
            extra_env: Mapping[str, str | None] | None = None) -> subprocess.CompletedProcess[str]:
        env = dict(self.env)
        for key, value in (extra_env or {}).items():
            if value is None: env.pop(key, None)
            else: env[key] = value
        self.count += 1
        print(f"  -> {redact_text(label, self.secrets)}", flush=True)
        try:
            # stdin is never the operator's terminal: a target that prompts
            # reads EOF and refuses instead of silently waiting or proceeding.
            result = subprocess.run(list(argv), cwd=self.cwd, env=env, text=True, stdin=subprocess.DEVNULL,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False,
                                    timeout=self.timeout)
        except subprocess.TimeoutExpired as exc:
            partial = exc.stdout.decode(errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            result = subprocess.CompletedProcess(list(argv), 124, partial + f"\nERROR: timed out after {self.timeout}s\n")
        safe = redact_text(result.stdout or "", self.secrets)
        header = redact_text("$ " + " ".join(argv), self.secrets)
        (self.log_dir / f"{self.count:04d}-{slug(label)[:80]}.log").write_text(header + "\n" + safe, encoding="utf-8")
        return subprocess.CompletedProcess(result.args, result.returncode, safe, result.stderr)


def read_tfvars(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise RuntimeError(f"missing authentication file: {path}")
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        m = re.match(r'^\s*([A-Za-z_][\w]*)\s*=\s*"((?:[^"\\]|\\.)*)"\s*(?:#.*)?$', line)
        if m:
            values[m.group(1)] = bytes(m.group(2), "utf-8").decode("unicode_escape")
    return values


def collect_secrets(cloud_root: Path) -> set[str]:
    """Every secret value in this cloud's auth files, so logs and reports can scrub it."""
    found: set[str] = set()
    paths = [cloud_root / "auth.auto.tfvars", *cloud_root.glob("envs/*/auth.auto.tfvars"),
             *cloud_root.glob("envs/*/*/auth.auto.tfvars")]
    for path in paths:
        if path.is_file():
            found.update(v for k, v in read_tfvars(path).items() if SECRET_KEYS.search(k) and v)
    return found


@contextlib.contextmanager
def _without_ambient_auth() -> Iterator[None]:
    """Hide shell auth (DATABRICKS_*, ARM_*, AZURE_*) while a client is configured."""
    hidden = {k: os.environ.pop(k) for k in list(os.environ)
              if k.startswith(("DATABRICKS_", "ARM_", "AZURE_"))}
    try:
        yield
    finally:
        os.environ.update(hidden)


class LiveSnapshotProvider:
    """Read-only SDK/SQL snapshot pinned to <cloud>/envs/<env>/auth.auto.tfvars (values never logged)."""
    def __init__(self, cloud_root: Path, state_dir: Path):
        self.cloud_root, self.state_dir = cloud_root, state_dir
        self.secret_values: set[str] = set()

    _tfvars = staticmethod(read_tfvars)

    def client(self, env: str) -> Any:
        try:
            from databricks.sdk import WorkspaceClient  # type: ignore
        except ImportError as exc:
            raise RuntimeError("live snapshots require databricks-sdk") from exc
        auth = read_tfvars(self.cloud_root / "envs" / env / "auth.auto.tfvars")
        self.secret_values.update(v for k, v in auth.items() if SECRET_KEYS.search(k) and v)
        host = (auth.get("databricks_workspace_host") or "").strip()
        if not host:
            raise RuntimeError(f"{env}: auth.auto.tfvars has no databricks_workspace_host")
        if not auth.get("databricks_client_id") or not auth.get("databricks_client_secret"):
            raise RuntimeError(f"{env}: auth.auto.tfvars needs databricks_client_id and databricks_client_secret")
        # Explicit OAuth M2M with ambient auth hidden: never a profile, token or
        # host from the shell, so the snapshot reads exactly this env's workspace.
        with _without_ambient_auth():
            client = WorkspaceClient(host=host, client_id=auth["databricks_client_id"],
                                     client_secret=auth["databricks_client_secret"], auth_type="oauth-m2m",
                                     product="genierails-e2e", product_version="1")
        connected = str(getattr(getattr(client, "config", None), "host", "") or "")
        if _norm_host(connected) != _norm_host(host):
            raise RuntimeError(f"{env}: client resolved host {connected!r}, not the env's {host!r}")
        return client

    def capture(self, env: str) -> Mapping[str, Any]:
        env_dir = self.cloud_root / "envs" / env
        client = self.client(env)
        config = _load_hcl(env_dir / "env.auto.tfvars")
        catalogs = _read_catalogs(env_dir, config)
        warehouse = config.get("sql_warehouse_id") or next((space.get("sql_warehouse_id")
            for space in config.get("genie_spaces") or []
            if isinstance(space, Mapping) and space.get("sql_warehouse_id")), None)
        if not warehouse: raise RuntimeError(f"{env}: env.auto.tfvars has no sql_warehouse_id")
        if not catalogs: raise RuntimeError(f"{env}: no governed catalogs could be resolved")
        sql = {catalog: _sql_snapshots(client, str(warehouse), catalog, self.secret_values)
               for catalog in catalogs}
        spaces = _agent_ids(env_dir, config, self.state_dir / env, self.cloud_root)
        agents: list[dict[str, Any]] = []
        for name, space_id in spaces:
            exported = client.genie.get_space(space_id).as_dict()
            acl = client.permissions.get("genie", space_id).as_dict()
            agents.append({"name": name, "space_id": space_id, "export": exported, "acl": acl})
        return redact({
            "uc": sql,
            "agents": agents,
            "state_serials": _state_serials(self.state_dir / env, env_dir),
            "receipts": _receipt_inventory(self.state_dir / env),
        }, self.secret_values)


def _norm_host(host: str) -> str:
    host = host.strip().rstrip("/").lower()
    return host if host.startswith("https://") else "https://" + host.removeprefix("http://")


def _sql_snapshots(client: Any, warehouse: str, catalog: str,
                   secrets: Iterable[str] = ()) -> Mapping[str, Any]:
    queries = {
        "catalog_grants": f"SELECT * FROM system.information_schema.catalog_privileges WHERE catalog_name = '{_sql_lit(catalog)}'",
        "schema_grants": f"SELECT * FROM system.information_schema.schema_privileges WHERE catalog_name = '{_sql_lit(catalog)}'",
        "table_grants": f"SELECT * FROM system.information_schema.table_privileges WHERE table_catalog = '{_sql_lit(catalog)}'",
        "governed_tags": f"SELECT * FROM system.information_schema.column_tags WHERE catalog_name = '{_sql_lit(catalog)}'",
        "mask_functions": f"SELECT routine_catalog,routine_schema,routine_name,routine_type,routine_definition FROM system.information_schema.routines WHERE routine_catalog = '{_sql_lit(catalog)}'",
        "tables": f"SELECT table_catalog,table_schema,table_name FROM system.information_schema.tables WHERE table_catalog = '{_sql_lit(catalog)}' AND table_schema <> 'information_schema'",
    }
    out: dict[str, Any] = {}
    for name, statement in queries.items():
        response = client.statement_execution.execute_statement(warehouse_id=warehouse, statement=statement, wait_timeout="50s")
        _require_succeeded(response, name, secrets)
        columns = [c.name for c in (response.manifest.schema.columns or [])]
        rows = list(response.result.data_array or [])
        next_index = getattr(response.result, "next_chunk_index", None)
        while next_index is not None:
            chunk = client.statement_execution.get_statement_result_chunk_n(response.statement_id, next_index)
            rows.extend(chunk.data_array or [])
            next_index = getattr(chunk, "next_chunk_index", None)
        out[name] = [dict(zip(columns, row)) for row in rows]
    # information_schema shows only what the principal can see: a governed
    # catalog with no visible tables is an unreadable catalog, not "no data".
    if not out["tables"]:
        raise RuntimeError(f"{catalog}: no visible tables; the snapshot principal cannot read this governed catalog")
    # Effective-policy evidence is per table. Inability to read it is a failed
    # snapshot, never evidence that no policy exists.
    policies: list[Mapping[str, Any]] = []
    policy_functions: set[str] = set()
    for table in out.pop("tables"):
        qualified = ".".join(str(table[k]) for k in ("table_catalog", "table_schema", "table_name"))
        try:
            listed = client.policies.list_policies("TABLE", qualified, include_inherited=True)
            captured = [_snapshot_live_policy(p, qualified) for p in listed]
            policies.append({"table": qualified, "policies": captured})
            policy_functions.update(str(mask.get("function_name")) for policy in captured
                                    if (mask := policy.get("column_mask") or policy.get("row_filter"))
                                    and mask.get("function_name"))
        except Exception as exc:
            raise RuntimeError(f"policy inventory failed for {qualified}: {type(exc).__name__}") from exc
    out["abac_policies"] = policies
    routines: list[Mapping[str, Any]] = []
    captured_functions: set[str] = set()
    for routine in out["mask_functions"]:
        qualified = ".".join(str(routine.get(k) or "")
                             for k in ("routine_catalog", "routine_schema", "routine_name"))
        name = str(routine.get("routine_name") or "")
        if qualified not in policy_functions and not name.startswith(("mask_", "gr_mask_", "filter_", "gr_filter_")):
            continue
        definition = routine.pop("routine_definition", None)
        if not isinstance(definition, str) or not definition.strip():
            raise RuntimeError(f"{qualified}: policy function definition is unreadable")
        routine["definition_hash"] = hashlib.sha256(definition.encode()).hexdigest()
        routines.append(routine)
        captured_functions.add(qualified)
    missing_functions = sorted(policy_functions - captured_functions)
    if missing_functions:
        raise RuntimeError(f"{missing_functions[0]}: policy function definition is unreadable")
    out["mask_functions"] = routines
    return out


def _condition_hash(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{label}: policy condition is unreadable")
    return hashlib.sha256(value.encode()).hexdigest()


def _snapshot_live_policy(policy: Any, table: str) -> dict[str, Any]:
    """Capture SDK fields explicitly: ``as_dict`` deliberately drops None."""
    raw = policy.as_dict()
    for field in CONDITION_FIELDS:
        if not hasattr(policy, field):
            raise RuntimeError(f"{table} {raw.get('name') or '<unnamed>'} {field}: policy condition is unreadable")
        raw[field] = getattr(policy, field)
    return _snapshot_policy(raw, table)


def _snapshot_policy(policy: Mapping[str, Any], table: str) -> dict[str, Any]:
    """Keep condition integrity evidence without persisting live SQL text."""
    result = dict(policy)
    name = str(result.get("name") or "<unnamed>")
    for field in ("when_condition",):
        value = result.pop(field, None)
        result[f"{field}_hash"] = (ABSENT_CONDITION if value is None else
                                    _condition_hash(value, f"{table} {name} {field}"))
    columns = result.pop("match_columns", None)
    if columns is not None:
        if not isinstance(columns, list):
            raise RuntimeError(f"{table} {name} match_columns: policy condition is unreadable")
        hashed = []
        match_values: set[str] = set()
        for index, column in enumerate(columns):
            if hasattr(column, "as_dict"):
                column = column.as_dict()
            if not isinstance(column, Mapping):
                raise RuntimeError(f"{table} {name} match_columns[{index}]: policy condition is unreadable")
            item = {k: v for k, v in column.items() if k != "condition"}
            match_values.update(TAG_VALUE_MATCH.findall(str(column.get("condition") or "")))
            item["condition_hash"] = _condition_hash(
                column.get("condition"), f"{table} {name} match_columns[{index}]")
            hashed.append(item)
        result["match_columns_hash"] = hashlib.sha256(
            json.dumps(sorted(hashed, key=lambda item: json.dumps(item, sort_keys=True)),
                       sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        result["match_values"] = sorted(match_values)
    else:
        result["match_columns_hash"] = ABSENT_CONDITION
    return result


def _sql_lit(value: str) -> str:
    return value.replace("'", "''")


def _require_succeeded(response: Any, label: str, secrets: Iterable[str] = ()) -> None:
    state = getattr(getattr(response, "status", None), "state", None)
    if str(getattr(state, "value", state)).upper() != "SUCCEEDED":
        error = getattr(getattr(response, "status", None), "error", None)
        details = error.as_dict() if hasattr(error, "as_dict") else error
        if isinstance(details, Mapping):
            code = details.get("error_code") or details.get("code") or "unknown"
            message = details.get("message") or "unavailable"
        else:
            code = getattr(error, "error_code", None) or getattr(error, "code", None) or "unknown"
            message = getattr(error, "message", None) or (str(error) if error else "unavailable")
        safe = redact_text(f"code={code}, message={message}", secrets).replace("\n", " ").strip()
        raise RuntimeError(f"{label} snapshot query did not succeed (state={state}; {safe})")
    if getattr(response, "result", None) is None or getattr(response, "manifest", None) is None:
        raise RuntimeError(f"{label} snapshot query returned no result")


def _load_hcl(path: Path) -> dict[str, Any]:
    if not path.is_file(): raise RuntimeError(f"missing config: {path}")
    try:
        import hcl2  # type: ignore
        with path.open(encoding="utf-8") as handle: return hcl2.load(handle)
    except Exception as exc:
        raise RuntimeError(f"could not parse {path.name}: {type(exc).__name__}") from exc


def _read_catalogs(env_dir: Path, config: Mapping[str, Any]) -> list[str]:
    catalogs: set[str] = set()
    for table in config.get("uc_tables") or []:
        if isinstance(table, str) and "." in table: catalogs.add(table.split(".", 1)[0])
    for space in config.get("genie_spaces") or []:
        if isinstance(space, Mapping):
            for table in space.get("uc_tables") or []:
                if isinstance(table, str) and "." in table: catalogs.add(table.split(".", 1)[0])
    mapping = config.get("catalog_map") or {}
    if isinstance(mapping, Mapping): catalogs.update(str(v) for v in mapping.values())
    governed = env_dir / "generated" / "governed_tables.json"
    if governed.is_file():
        try:
            data = json.loads(governed.read_text())
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"could not read {governed}: {type(exc).__name__}") from exc
        values = data if isinstance(data, list) else data.get("tables", [])
        catalogs.update(str(t).split(".", 1)[0] for t in values if isinstance(t, str) and "." in t)
    return sorted(catalogs)


def _agent_ids(env_dir: Path, config: Mapping[str, Any], state_root: Path, cloud_root: Path) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    for index, space in enumerate(config.get("genie_spaces") or []):
        if isinstance(space, Mapping) and space.get("genie_space_id"):
            found.append((str(space.get("name") or f"space-{index}"), str(space["genie_space_id"])))
    for path in sorted(env_dir.glob("agents/*")):
        if not path.is_file(): continue
        text = path.read_text(encoding="utf-8")
        match = re.search(r'^\s*(?:genie_space_id|space_id)\s*=\s*"([^"]+)"', text, re.M)
        if match: found.append((path.stem, match.group(1)))
    for legacy in (env_dir / ".genie_space_id", cloud_root / ".genie_space_id"):
        if legacy.is_file() and legacy.read_text().strip(): found.append(("legacy", legacy.read_text().strip()))
    for path in sorted(env_dir.glob(".genie_space_id_*")):
        if path.is_file() and path.read_text().strip():
            found.append((path.name.removeprefix(".genie_space_id_"), path.read_text().strip()))
    for path in sorted(state_root.rglob("*.tfstate")) if state_root.exists() else []:
        try:
            for resource in json.loads(path.read_text()).get("resources", []):
                if "genie" not in str(resource.get("type", "")).lower(): continue
                for inst in resource.get("instances", []):
                    attrs = inst.get("attributes") or {}
                    sid = attrs.get("space_id") or attrs.get("id")
                    if sid: found.append((str(resource.get("name", "state")), str(sid)))
        except (OSError, ValueError, TypeError, AttributeError) as exc:
            raise RuntimeError(f"could not read agent IDs from {path}") from exc
    return sorted(set(found))


def _state_serials(root: Path, env_dir: Path | None = None) -> Mapping[str, int | str]:
    result: dict[str, int | str] = {}
    for path in sorted(root.rglob("*.tfstate")) if root.exists() else []:
        try: result[str(path.relative_to(root))] = int(json.loads(path.read_text())["serial"])
        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise RuntimeError(f"could not read the Terraform serial of {path}") from exc
    remote = bool(env_dir and any(env_dir.rglob("remote_state.hcl")))
    if remote:
        result["remote_state"] = "remote, not snapshotted"
    return result


def _receipt_inventory(root: Path) -> list[Mapping[str, Any]]:
    result = []
    if not root.exists(): return result
    for path in sorted(p for p in root.rglob("*") if p.is_file() and ("receipt" in p.name or "manifest" in p.name)):
        raw = path.read_bytes()
        result.append({"path": str(path.relative_to(root)), "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)})
    return result


def make_targets(cloud_root: Path, timeout: int = 60) -> set[str]:
    print("  -> inventory public Make targets", flush=True)
    result = subprocess.run(["make", "-qp"], cwd=cloud_root, text=True, stdout=subprocess.PIPE, stdin=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, check=False, timeout=timeout)
    found = {m.group(1) for line in result.stdout.splitlines()
             if (m := re.match(r"^([A-Za-z0-9][A-Za-z0-9_.-]*):(?:\s|$)", line)) and "%" not in m.group(1)}
    # An unreadable Makefile is an error, not "every target is missing".
    if "help" not in found:
        raise RuntimeError(f"could not inventory Make targets in {cloud_root}")
    return found


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def render(value: str, values: Mapping[str, str]) -> str:
    missing = [name for name in PLACEHOLDER.findall(value) if not values.get(name)]
    if missing: raise ValueError(f"unresolved placeholder(s): {', '.join(sorted(set(missing)))}")
    rendered = PLACEHOLDER.sub(lambda m: values[m.group(1)], value)
    check_argument(rendered)
    return rendered


def check_argument(arg: str) -> None:
    """No placeholder text, and no empty KEY=, ever reaches make."""
    if re.fullmatch(r"[A-Z_][A-Z0-9_]*=", arg) or re.search(r"[<>{}]", arg):
        raise ValueError(f"unresolved placeholder in argument {arg!r}")


def unresolved_values(scenario: Scenario, values: Mapping[str, str]) -> list[str]:
    """Every value a scenario needs that is neither supplied nor produced by an earlier step."""
    produced: set[str] = set()
    missing: list[str] = []
    for step in scenario.steps:
        if step.action in ("checkout_value", "add_ack") and not values.get(step.value or ""):
            missing.append(step.value or "")
        if step.action == "write_agent_file":
            missing += [name for name in AGENT_FILE_VALUES if not values.get(name)]
            missing += agent_file_value_problems(values)
        for name in (n for part in step.command for n in PLACEHOLDER.findall(part)):
            if not values.get(name) and name not in produced: missing.append(name)
        for name in (n for check in step.assertions for n in check_placeholders(check)):
            if not values.get(name): missing.append(name)
        if step.target == "release-plan" and not step.expect_failure: produced.add("manifest")
    missing += [n for check in scenario.checks for n in check_placeholders(check) if not values.get(n)]
    for step in scenario.steps:
        if step.action == "checkout_value" and values.get(step.value or "") and not re.fullmatch(r"[0-9a-fA-F]{40}", values[step.value or ""]):
            missing.append(f"{step.value} (must be a 40-hex commit)")
    return sorted(set(missing))


def _acl_groups(value: str) -> list[str]:
    return [group.strip() for group in value.split(",") if group.strip()]


def agent_file_value_problems(values: Mapping[str, str]) -> list[str]:
    """Each supplied agent-file value that can't be written as given (missing ones are reported apart)."""
    problems = []
    if values.get("agent_a") and not AGENT_KEY.fullmatch(values["agent_a"]):
        problems.append("agent_a (must be the agent's Terraform key: lowercase letters, digits and '_')")
    if values.get("dev_agent_id") and not AGENT_ID.fullmatch(values["dev_agent_id"]):
        problems.append("dev_agent_id (must be a Genie agent ID: letters, digits, '_' or '-')")
    if values.get("acl_groups") and not (
            _acl_groups(values["acl_groups"])
            and all(GROUP_NAME.fullmatch(group) for group in _acl_groups(values["acl_groups"]))):
        problems.append("acl_groups (must be comma-separated group names)")
    return problems


def write_agent_file(env_dir: Path, values: Mapping[str, str]) -> None:
    """Write dev agent A's settings file; an existing file must already say the same."""
    problems = [f"missing value: {name}" for name in AGENT_FILE_VALUES if not values.get(name)]
    problems += agent_file_value_problems(values)
    if problems: raise RuntimeError("cannot write the agent file: " + "; ".join(problems))
    groups = _acl_groups(values["acl_groups"])
    path = env_dir / "agents" / f"{values['agent_a']}.auto.tfvars"
    if path.exists():
        config = _load_hcl(path)
        if config.get("genie_space_id") != values["dev_agent_id"] or config.get("acl_groups") != groups:
            raise RuntimeError(f"{path} already sets a different genie_space_id or acl_groups; nothing was written")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"genie_space_id = {json.dumps(values['dev_agent_id'])}\n"
                    f"acl_groups     = [{', '.join(json.dumps(group) for group in groups)}]\n", encoding="utf-8")


def governance_only_env(cloud_root: Path, env: str) -> bool:
    path = cloud_root / "envs" / env / "env.auto.tfvars"
    if not path.is_file(): return False
    config = _load_hcl(path)
    return (config.get("governance_mode") == "deterministic"
            and not config.get("genie_spaces") and not config.get("genie_space_id")
            and not config.get("genie_space_title")
            and not list((path.parent / "agents").glob("*.auto.tfvars")))


def deterministic_env(cloud_root: Path, env: str | None) -> bool:
    if not env: return False
    path = cloud_root / "envs" / env / "env.auto.tfvars"
    return path.is_file() and _load_hcl(path).get("governance_mode") == "deterministic"


def import_unset_treatment(output: str) -> bool:
    return bool(IMPORT_UNSET_LOG.search(output))


def import_unset_source_anchored(repo_root: Path) -> bool:
    source = repo_root / "shared/scripts/import_existing.sh"
    if not source.is_file(): return False
    text = source.read_text(encoding="utf-8", errors="replace")
    return all(anchor in text for anchor in IMPORT_UNSET_SOURCE_ANCHORS)


def parse_manifest(output: str) -> str:
    matches = re.findall(r"(?m)Manifest \(immutable\):\s*(\S+)", output) or re.findall(r"MANIFEST=(\S+)", output)
    if not matches: raise RuntimeError("release-plan succeeded but printed no manifest path")
    manifest = matches[-1].strip("'\"")
    check_argument(manifest)
    if not os.path.isabs(manifest): raise RuntimeError(f"release-plan printed a non-absolute manifest path {manifest!r}")
    return manifest


def release_needs_another_phase(output: str) -> bool:
    return bool(ANOTHER_RELEASE_PHASE.search(output))


def refusal_found_in_source(repo_root: Path, pattern: str) -> bool:
    anchor = REFUSAL_SOURCE_ANCHORS.get(pattern)
    if not anchor: return False
    path, tail = anchor
    source = repo_root / path
    return source.is_file() and tail in source.read_text(encoding="utf-8", errors="replace")


def parse_migration_mappings(output: str) -> dict[str, Adoption]:
    mappings: dict[str, Adoption] = {}
    current: tuple[str, str, str] | None = None
    declaration_location: str | None = None
    for line in output.splitlines():
        match = re.fullmatch(r"\s+(\S+\.\S+\.\S+\.\S+):\s+(\S+)\s+->\s+(\S+)\s*", line)
        if match:
            location, old, new = match.groups()
            current = (location, old, new) if old != "untagged" and parse_treatment_value(new) else None
            declaration_location = None
            continue
        mask = re.match(r"\s+mask today:\s+([^:]+):", line)
        if current and mask:
            location, old, new = current
            mappings[location] = Adoption(old, new, mask.group(1).strip())
            declaration_location = location
            current = None
            continue
        policy_change = re.fullmatch(
            r"\s+reviewed policy condition:\s+(\S+)\s+(when_condition|match_columns)\s+(absent|[0-9a-f]{64})\s*", line)
        if declaration_location and policy_change:
            adoption = mappings[declaration_location]
            mappings[declaration_location] = dataclasses.replace(
                adoption, policy_conditions=(*adoption.policy_conditions, policy_change.groups()))
            continue
        mask_change = re.fullmatch(r"\s+reviewed mask definition:\s+(\S+)\s+([0-9a-f]{64})\s*", line)
        if declaration_location and mask_change:
            adoption = mappings[declaration_location]
            mappings[declaration_location] = dataclasses.replace(
                adoption, mask_definitions=(*adoption.mask_definitions, mask_change.groups()))
            continue
        declaration_location = None
    if not mappings:
        raise RuntimeError("migrate-plan printed no reviewed mapping with its exact legacy mask function")
    return mappings


def scenario_plan(scenario: Scenario, targets: set[str]) -> list[dict[str, Any]]:
    planned = []
    for step in scenario.steps:
        missing = bool(step.target and step.target not in targets)
        marker = "target not implemented"
        planned.append({"name": step.name, "command": " ".join(step.command), "human_prompt": step.human_prompt,
                        "expect_failure": step.expect_failure, "refusal": step.refusal,
                        "action": step.action, "status": "skip" if missing else "planned",
                        "skip_reason": marker if missing else None,
                        "assertions": [describe_check(check) for check in step.assertions]})
    return planned


def write_report(out: Path, scenario: Scenario, payload: Mapping[str, Any], secrets: Iterable[str] = ()) -> None:
    out.mkdir(parents=True, exist_ok=True)
    clean = redact(payload, secrets)
    (out / f"{scenario.key}.json").write_text(json.dumps(clean, indent=2, sort_keys=True, default=str) + "\n")
    lines = [f"# {scenario.key}: {scenario.title}", "", f"Status: **{clean['status']}**", ""]
    if clean.get("error"): lines += [f"Error: {clean['error']}", ""]
    if clean.get("blocked_by"): lines += ["Not run because: " + "; ".join(map(str, clean["blocked_by"])), ""]
    lines += ["## Preconditions", ""] + [f"- {x}" for x in scenario.preconditions]
    lines += ["", "## Steps", ""]
    for item in clean["steps"]:
        detail = item.get("skip_reason") or (f"exit {item['exit_code']}" if "exit_code" in item else item["status"])
        lines.append(f"- `{item['status']}` {item['name']}: {item.get('command') or item.get('action') or 'human gate'} ({detail})")
    lines += ["", "## Pass criteria", ""] + [f"- {x}" for x in scenario.pass_criteria]
    lines += ["", "## Restore", ""] + [f"- {x}" for x in scenario.restore]
    if clean.get("manual_restore_commands"):
        lines += ["", "## Manual restore required", ""]
        lines += [f"- `{command}`" for command in clean["manual_restore_commands"]]
    diff = clean.get("snapshot_diff", {})
    lines += ["", "## Snapshot comparison", "", f"Changed top-level components: {', '.join(diff) if diff else 'none'}", ""]
    if diff:
        lines += ["```json", json.dumps(diff, indent=2, sort_keys=True, default=str), "```", ""]
    post_reset_diff = clean.get("post_reset_snapshot_diff", {})
    if post_reset_diff:
        lines += ["## Post-reset snapshot comparison", "", "```json",
                  json.dumps(post_reset_diff, indent=2, sort_keys=True, default=str), "```", ""]
    (out / f"{scenario.key}.md").write_text("\n".join(lines), encoding="utf-8")


class Checkout:
    """The persistent checkout: pinned HEAD, clean tree, and the driver's own local main."""

    def __init__(self, runner: CommandRunner, baseline: str, remote_dir: Path):
        self.runner, self.baseline, self.remote_dir = runner, baseline, remote_dir
        self.owns_remote = False
        self.head = baseline
        self.main_tip = baseline

    def git(self, *argv: str, label: str) -> str:
        result = self.runner.run(("git", *argv), label)
        if result.returncode: raise RuntimeError(f"git {argv[0]} failed ({label}); see its log")
        return result.stdout.strip()

    def switch(self, commit: str, label: str = "checkout") -> str:
        self.git("checkout", "--force", "--detach", commit, label=label)
        resolved = self.git("rev-parse", "--verify", f"{commit}^{{commit}}", label=f"{label}-resolved")
        self.head = resolved
        self.verify(label)
        self.require_clean(label)
        return resolved

    def verify(self, label: str) -> None:
        head = self.git("rev-parse", "HEAD", label=f"{label}-head")
        if head != self.head:
            raise RuntimeError(f"checkout HEAD {head[:12] or '?'} is not the pinned {self.head[:12]} ({label})")

    def require_clean(self, label: str) -> None:
        status = self.runner.run(("git", "status", "--porcelain", "--untracked-files=all"), f"clean-{label}")
        if status.returncode or status.stdout.strip(): raise RuntimeError(f"checkout is not clean at {label}")

    def init_remote(self) -> None:
        """Create (or reuse) the driver-owned bare remote and point its main at the baseline."""
        if not (self.remote_dir / "HEAD").is_file():
            self.git("init", "--bare", "--quiet", str(self.remote_dir), label="e2e-remote-init")
        url = self.remote_url("e2e-remote-url")
        if url is None:
            self.git("remote", "add", E2E_REMOTE, str(self.remote_dir), label="e2e-remote-add")
        elif Path(url).resolve() != self.remote_dir.resolve():
            raise RuntimeError(
                f"git remote {E2E_REMOTE} exists and points elsewhere ({url}); refusing to reuse it. If that URL "
                f"is an old e2e reports dir (<reports>/e2e-remote.git) left by an earlier run, clear it with "
                f"`git remote remove {E2E_REMOTE}` and re-run")
        self.owns_remote = True
        self.publish(self.baseline)

    def remote_url(self, label: str) -> str | None:
        url = self.runner.run(("git", "remote", "get-url", E2E_REMOTE), label)
        return None if url.returncode else url.stdout.strip()

    def remove_remote(self) -> None:
        """Drop the driver's remote at the end of the run, only if it still points at this run's bare repo."""
        if not self.owns_remote: return
        url = self.remote_url("e2e-remote-cleanup-url")
        if url is not None and Path(url).resolve() == self.remote_dir.resolve():
            self.git("remote", "remove", E2E_REMOTE, label="e2e-remote-remove")
        self.owns_remote = False

    def publish(self, commit: str) -> None:
        self.git("branch", "--force", f"{E2E_BRANCH_PREFIX}main", commit, label="e2e-main-branch")
        self.git("push", "--quiet", "--force", E2E_REMOTE, f"{commit}:refs/heads/main", label="e2e-main-push")
        self.main_tip = commit

    def local_merge(self, key: str) -> str:
        """Commit authored envs/ changes on a driver branch and merge it into the driver's main."""
        stamp = time.time_ns()
        self.git("switch", "--quiet", "-c", f"{E2E_BRANCH_PREFIX}{key}-{stamp}", label=f"{key}-author-branch")
        self.git("add", "-A", "--", "envs", label=f"{key}-author-add")
        self.git("commit", "--quiet", "--allow-empty", "-m", f"e2e {key}: authored changes", label=f"{key}-author-commit")
        author = self.git("rev-parse", "HEAD", label=f"{key}-author-head")
        self.git("checkout", "--quiet", "--detach", self.main_tip, label=f"{key}-merge-base")
        self.git("merge", "--quiet", "--no-ff", "--no-edit", "-m", f"e2e {key}: merge", author, label=f"{key}-merge")
        merged = self.git("rev-parse", "HEAD", label=f"{key}-merge-head")
        self.publish(merged)
        return self.switch(merged, f"{key}-merged")

    def reset(self) -> None:
        """Back to the requested commit; drop anything the scenario authored under envs/."""
        self.git("checkout", "--force", "--detach", self.baseline, label="reset-checkout")
        self.git("clean", "-fdq", "--", "envs", label="reset-clean")
        self.head = self.baseline

    def forward_fix(self) -> str:
        """A commit on the driver's main whose tree is the baseline's: restore is a forward release."""
        if self.main_tip == self.baseline: return self.switch(self.baseline, "restore-checkout")
        tree = self.git("rev-parse", f"{self.baseline}^{{tree}}", label="restore-tree")
        if self.git("rev-parse", f"{self.main_tip}^{{tree}}", label="restore-tip-tree") == tree:
            return self.switch(self.main_tip, "restore-checkout")
        fixed = self.git("commit-tree", tree, "-p", self.main_tip, "-m", "e2e: restore the requested commit's tree",
                         label="restore-commit")
        self.publish(fixed)
        return self.switch(fixed, "restore-checkout")


def customer_checkout_problems(checkout_dir: Path, cloud: str, commit: str) -> list[str]:
    """Why ``commit`` can't carry authored envs/ changes; read-only git only.

    The checkout must already be clean at ``commit``, so git's own ignore
    rules (every .gitignore, .git/info/exclude, core.excludesFile) are
    evaluated against exactly that tree without switching anything.
    """
    def git(*argv: str) -> subprocess.CompletedProcess:
        return subprocess.run(("git", "--no-optional-locks", *argv), cwd=checkout_dir,
                              capture_output=True, text=True)

    if git("cat-file", "-e", f"{commit}^{{commit}}").returncode:
        return [f"commit {commit[:12]} is not in {checkout_dir}"]
    head = git("rev-parse", "HEAD")
    if head.returncode or head.stdout.strip() != commit:
        return [f"{checkout_dir} is at {head.stdout.strip()[:12] or '?'}, not {commit[:12]}; "
                f"check out the requested commit first (git checkout --detach {commit[:12]})"]
    status = git("status", "--porcelain", "--untracked-files=all")
    if status.returncode or status.stdout.strip():
        return [f"{checkout_dir} is not clean at {commit[:12]}"]
    problems = []
    gitignore = git("show", f"{commit}:{cloud}/.gitignore")
    if gitignore.returncode or CUSTOMER_GITIGNORE_MARKER not in gitignore.stdout:
        problems.append(f"{cloud}/.gitignore at {commit[:12]} has no GenieRails customer block, so the "
                        "upstream .gitignore ignores envs/ and `git add -A -- envs` fails")
    for env in CUSTOMER_ENVS:
        path = f"{cloud}/envs/{env}/env.auto.tfvars"
        parts = path.split("/")
        # Each component must be a real directory and the file a regular blob, not a symlink.
        modes = {}
        for i in range(1, len(parts) + 1):
            name = "/".join(parts[:i])
            listed = git("ls-tree", "-z", commit, "--", name)
            entry = listed.stdout.split("\0")[0] if listed.returncode == 0 else ""
            if "\t" in entry and entry.split("\t", 1)[1] == name:
                modes[name] = entry.split()[0]
            else:
                break
        bad = [f"{name} ({mode})" for name, mode in modes.items()
               if mode not in (("100644", "100755") if name == path else ("040000",))]
        if not bad and path not in modes:
            problems.append(f"{path} is not committed at {commit[:12]}")
        elif bad:
            problems.append(f"{path} must be a regular committed file in real directories; not: {', '.join(bad)}")
        elif git("check-ignore", "-q", "--no-index", path).returncode == 0:
            problems.append(f"{path} is git-ignored, so authored env changes can't be committed")
    return problems


def _protection(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Per-column and per-principal protection facts, independent of IDs and timestamps."""
    result: dict[str, Any] = {"columns": {}, "masks": set(), "mask_names": set(),
                              "mask_definitions": {}, "policies": {}, "grants": set()}
    uc = snapshot.get("uc", {})
    for catalog, evidence in uc.items() if isinstance(uc, Mapping) else []:
        if not isinstance(evidence, Mapping): continue
        for row in evidence.get("governed_tags") or []:
            location = ".".join(str(row.get(k)) for k in ("catalog_name", "schema_name", "table_name", "column_name"))
            tag, value = str(row.get("tag_name")), str(row.get("tag_value"))
            parsed = parse_treatment_value(value) if tag == "gr_treatment" else None
            result["columns"][(location, tag)] = (parsed[0], parsed[1], value) if parsed else value
        for row in evidence.get("mask_functions") or []:
            name = str(row.get("routine_name"))
            normalized = _normalized_mask(name)
            prefix = f"{row.get('routine_catalog')}.{row.get('routine_schema')}."
            result["masks"].add(prefix + ("__".join(normalized) if normalized else name))
            qualified = prefix + name
            result["mask_names"].add(qualified)
            if row.get("definition_hash"):
                result["mask_definitions"][qualified] = str(row["definition_hash"])
        for entry in evidence.get("abac_policies") or []:
            for policy in entry.get("policies") or []:
                mask = policy.get("column_mask") or policy.get("row_filter") or {}
                function = str(mask.get("function_name") or "")
                normalized = _normalized_mask(function.rsplit(".", 1)[-1])
                prefix = function.rsplit(".", 1)[0] + "." if "." in function else ""
                normalized_function = prefix + "__".join(normalized) if normalized else function
                key = ((str(entry.get("table")), str(catalog), *normalized) if normalized
                       else (str(entry.get("table")), str(catalog), str(policy.get("name"))))
                fact = {"function": {normalized or (function,)},
                        "masks": {normalized_function},
                        "raw_functions": {function},
                        "values": set(policy.get("match_values") or []) or
                                  {value for column in policy.get("match_columns") or []
                                   for value in TAG_VALUE_MATCH.findall(str(column.get("condition") or ""))},
                        "to": set(policy.get("to_principals") or []),
                        "except": set(policy.get("except_principals") or []),
                        "conditions": {(str(policy.get("name")), field): {str(policy[f"{field}_hash"])}
                                       for field in CONDITION_FIELDS if f"{field}_hash" in policy}}
                prior = result["policies"].get(key)
                if prior:
                    prior["function"].update(fact["function"])
                    prior["masks"].update(fact["masks"])
                    prior["raw_functions"].update(fact["raw_functions"])
                    prior["values"].update(fact["values"])
                    prior["to"].intersection_update(fact["to"])
                    prior["except"].update(fact["except"])
                    for field, hashes in fact["conditions"].items():
                        prior["conditions"].setdefault(field, set()).update(hashes)
                else:
                    result["policies"][key] = fact
    result["grants"] = _grant_facts(snapshot)
    for fact in result["policies"].values():
        fact["function"] = tuple(sorted(fact["function"]))
        fact["masks"] = tuple(sorted(fact["masks"]))
        fact["conditions"] = {field: tuple(sorted(hashes)) for field, hashes in fact["conditions"].items()}
    return result


def _grant_facts(snapshot: Mapping[str, Any]) -> set[Grant]:
    """UC grants and direct Genie CAN_RUN as (principal, privilege, securable)."""
    facts: set[Grant] = set()
    uc = snapshot.get("uc", {})
    for evidence in uc.values() if isinstance(uc, Mapping) else []:
        if not isinstance(evidence, Mapping): continue
        for kind in ("catalog_grants", "schema_grants", "table_grants"):
            for row in evidence.get(kind) or []:
                securable = ".".join(str(row[k]) for k in ("table_catalog", "table_schema", "table_name",
                                                           "catalog_name", "schema_name") if row.get(k))
                facts.add((str(row.get("grantee")), str(row.get("privilege_type")), securable))
    for agent in snapshot.get("agents") or []:
        acl = agent.get("acl") if isinstance(agent, Mapping) else None
        for entry in (acl or {}).get("access_control_list") or []:
            principal = entry.get("group_name") or entry.get("user_name") or entry.get("service_principal_name")
            if any(p.get("permission_level") == "CAN_RUN" and not p.get("inherited")
                   for p in entry.get("all_permissions") or []):
                facts.add((str(principal), "CAN_RUN", f"genie:{agent.get('space_id')}"))
    return facts


def _grant_key(grant: Sequence[str]) -> Grant:
    """information_schema spells USE_SCHEMA as ``USE SCHEMA``; compare either."""
    principal, privilege, securable = grant
    return (principal.strip(), re.sub(r"[\s_]+", " ", privilege.strip().upper()), securable.strip())


def _normalized_mask(name: str) -> tuple[str, str, str] | None:
    match = re.fullmatch(r"gr_mask_(.+)_(partial|full)", name or "")
    parsed = parse_treatment_value(match.group(1)) if match else None
    return (parsed[0], parsed[1], match.group(2)) if parsed and match else None


def _covers(fact: Mapping[str, Any], principal: str) -> bool:
    return (principal in fact["to"] or "account users" in fact["to"]) and principal not in fact["except"]


def _verified_adoptions(before: Mapping[str, Any], after: Mapping[str, Any],
                        requested: Mapping[str, Adoption]) -> dict[str, dict[str, set[Any]]]:
    """Reviewed adoptions whose deterministic replacement exists and covers at
    least every principal the exact legacy policy on that table covered."""
    verified: dict[str, dict[str, set[Any]]] = {}
    for location, adoption in requested.items():
        parsed = parse_treatment_value(adoption.new)
        if not parsed: continue
        table, catalog = location.rsplit(".", 1)[0], location.split(".", 1)[0]
        legacy_function = adoption.legacy_function.lower()
        # Only the policies on this table that masked with migrate-plan's
        # "mask today" function and (when listed) matched the old value.
        legacy = {key: fact for key, fact in before["policies"].items()
                  if len(key) == 3 and key[0] == table
                  and any(fn.lower() == legacy_function or ("." not in legacy_function
                          and fn.rsplit(".", 1)[-1].lower() == legacy_function) for fn in fact["masks"])
                  and (not fact["values"] or adoption.old in fact["values"])}
        if not legacy: continue
        levels = ("full",) if is_never_raw(adoption.new) else ("partial", "full")
        replacements = []
        for level in levels:
            policy = after["policies"].get((table, catalog, parsed[0], parsed[1], level))
            wanted = f"gr_mask_{adoption.new}_{level}"
            if not policy or not any(fn.startswith(f"{catalog}.") and fn.rsplit(".", 1)[-1] == wanted
                                     and fn in after["mask_names"] for fn in policy["raw_functions"]):
                break
            replacements.append(policy)
        else:
            principals = {"account users"}.union(*(f["to"] | f["except"] for f in (*legacy.values(), *replacements)))
            if all(any(_covers(r, who) for r in replacements)
                   for fact in legacy.values() for who in principals if _covers(fact, who)):
                verified[location] = {"legacy_keys": set(legacy),
                                      "legacy_masks": {fn for f in legacy.values() for fn in f["masks"]}}
    return verified


def _excused_legacy(before: Mapping[str, Any], after: Mapping[str, Any],
                    requested: Mapping[str, Adoption]) -> tuple[set[Any], set[str]]:
    """Legacy policies and functions a verified cutover may remove: on a table,
    only once every column there that carried the old value was adopted."""
    verified = _verified_adoptions(before, after, requested)
    tagged: dict[tuple[str, str], set[str]] = {}
    for (location, tag), value in before["columns"].items():
        raw = value[2] if isinstance(value, tuple) else value
        if tag == "gr_treatment": tagged.setdefault((location.rsplit(".", 1)[0], raw), set()).add(location)
    keys: set[Any] = set(); masks: set[str] = set()
    for location, item in verified.items():
        table, old = location.rsplit(".", 1)[0], requested[location].old
        if tagged.get((table, old), set()) <= set(verified): keys |= item["legacy_keys"]; masks |= item["legacy_masks"]
    # A catalog-level legacy policy is listed on every table; where no column
    # carried the old value it masked nothing, so its removal there is excused.
    names = {(k[1], k[2], before["policies"][k]["masks"]): requested[loc].old
             for loc, item in verified.items() for k in item["legacy_keys"] if k in keys}
    for key, fact in before["policies"].items():
        old = names.get((key[1], key[2], fact["masks"])) if len(key) == 3 else None
        if old is not None and not tagged.get((key[0], old)): keys.add(key)
    # Never excuse a function that a surviving policy still masks with.
    masks -= {fn for fact in after["policies"].values() for fn in fact["masks"]}
    return keys, masks


def protection_diff(baseline: Mapping[str, Any], current: Mapping[str, Any], *, include_grants: bool = False,
                    allowed_adoptions: Mapping[str, Adoption] | None = None,
                    allowed_grants: Iterable[Grant] = ()) -> dict[str, list[str]]:
    """What is weaker in ``current``: a column untagged, a mask or policy gone,
    a principal newly exempted or no longer covered, and (optionally) a grant
    or CAN_RUN added that is not one of the exact ``allowed_grants``."""
    before, after = _protection(baseline), _protection(current)
    allowed_adoptions = allowed_adoptions or {}
    verified_adoptions = _verified_adoptions(before, after, allowed_adoptions)
    excused_policy_keys, excused_masks = _excused_legacy(before, after, allowed_adoptions)
    allowed_conditions = {(location.rsplit(".", 1)[0], name, field): digest
                          for location, adoption in allowed_adoptions.items()
                          for name, field, digest in adoption.policy_conditions}
    allowed_definitions = {name: digest for adoption in allowed_adoptions.values()
                           for name, digest in adoption.mask_definitions}
    weaker: dict[str, list[str]] = {}
    lost_columns = []
    for key, was in sorted(before["columns"].items()):
        now = after["columns"].get(key)
        if now is None:
            display = "__".join(was) if isinstance(was, tuple) else was
            lost_columns.append(f"{key[0]} {key[1]}={display}")
        elif key[1] == "gr_treatment":
            was_raw = was[2] if isinstance(was, tuple) else was
            now_raw = now[2] if isinstance(now, tuple) else now
            # The same observed treatment cannot be a transition.  In
            # particular, a legacy ``redact`` value is inferred as never-raw
            # below while the deterministic parser deliberately does not
            # infer that property from the bare legacy label.  Comparing those
            # two interpretations made an unchanged mid-migration environment
            # look like ``never-raw redact -> redact``.
            if was_raw == now_raw:
                continue
            requested = allowed_adoptions.get(key[0])
            adoption = (key[0] in verified_adoptions and requested is not None
                        and (requested.old, requested.new) == (was_raw, now_raw))
            # A reviewed cutover may replace only the exact pair migrate-plan
            # printed. Never-raw -> weaker remains forbidden regardless.
            legacy_never_raw = was_raw in LEGACY_REDACTED_TREATMENTS
            if (is_never_raw(was_raw) or legacy_never_raw) and not is_never_raw(now_raw):
                lost_columns.append(f"{key[0]} gr_treatment never-raw {was_raw} -> {now_raw}")
            elif adoption:
                pass
            elif isinstance(was, tuple) and not isinstance(now, tuple):
                lost_columns.append(f"{key[0]} gr_treatment became legacy ({now})")
            elif isinstance(now, tuple) and now[0] == FAILSAFE_TREATMENT and was[:2] != now[:2]:
                lost_columns.append(f"{key[0]} gr_treatment became fail-safe ({now[0]})")
            elif isinstance(was, tuple) and isinstance(now, tuple) and was[:2] != now[:2]:
                lost_columns.append(f"{key[0]} gr_treatment {was[:2]} -> {now[:2]}")
            elif not isinstance(was, tuple) and isinstance(now, tuple):
                lost_columns.append(f"{key[0]} gr_treatment {was} -> {now[:2]}")
            elif not isinstance(was, tuple) and not isinstance(now, tuple) and was != now:
                lost_columns.append(f"{key[0]} gr_treatment {was} -> {now}")
        elif was != now:
            lost_columns.append(f"{key[0]} {key[1]}={was}: changed to {now}")
    if lost_columns: weaker["columns"] = lost_columns
    lost_masks = sorted((before["masks"] - after["masks"]) - excused_masks)
    live_before = {fn for fact in before["policies"].values() for fn in fact["raw_functions"]}
    live_after = {fn for fact in after["policies"].values() for fn in fact["raw_functions"]}
    changed_masks = sorted(fn for fn in live_before & live_after
                           if fn in before["mask_definitions"] and fn in after["mask_definitions"]
                           and before["mask_definitions"][fn] != after["mask_definitions"][fn]
                           and allowed_definitions.get(fn) != after["mask_definitions"][fn])
    lost_masks.extend(f"{fn}: definition changed" for fn in changed_masks)
    unreadable_masks = sorted(fn for fn in live_before & live_after
                              if ((before["mask_definitions"] and fn not in before["mask_definitions"])
                                  or (after["mask_definitions"] and fn not in after["mask_definitions"])))
    lost_masks.extend(f"{fn}: definition unreadable" for fn in unreadable_masks)
    if lost_masks: weaker["masks"] = lost_masks
    policies = []
    for key, was in sorted(before["policies"].items()):
        label = f"{key[0]} {key[2]}" if len(key) == 3 else " ".join((key[0], *key[2:]))
        now = after["policies"].get(key)
        legacy_replaced = key in excused_policy_keys
        if now is None and not legacy_replaced: policies.append(f"{label}: removed")
        elif now is not None and now["function"] != was["function"]:
            policies.append(f"{label}: function {sorted(was['function'])} -> {sorted(now['function'])}")
        elif now is not None:
            condition_keys = set(was["conditions"]) | set(now["conditions"])
            for (policy_name, field) in sorted(condition_keys):
                digest = was["conditions"].get((policy_name, field))
                current_digest = now["conditions"].get((policy_name, field))
                allowed = allowed_conditions.get((key[0], policy_name, field))
                declared = allowed is not None and current_digest == (allowed,)
                if current_digest != digest and not declared:
                    policies.append(f"{label}: {policy_name} {field} changed or became unreadable")
            table_values = {value[2] if isinstance(value, tuple) else value
                            for (location, tag), value in after["columns"].items()
                            if tag == "gr_treatment" and location.rsplit(".", 1)[0] == key[0]}
            policies += [f"{label}: no longer matches {value}" for value in
                         sorted((was["values"] - now["values"]) & table_values)]
            policies += [f"{label}: no longer applies to {p}" for p in sorted(was["to"] - now["to"])]
            policies += [f"{label}: newly exempts {p}" for p in sorted(now["except"] - was["except"])]
    if policies: weaker["policies"] = policies
    allowed = {_grant_key(grant) for grant in allowed_grants}
    widened = sorted(" ".join(grant) for grant in after["grants"] - before["grants"]
                     if _grant_key(grant) not in allowed)
    if include_grants and widened: weaker["widened_grants"] = widened
    return weaker


def protection_diff_envs(baseline: Mapping[str, Any], current: Mapping[str, Any], *,
                         allowed_adoptions: Mapping[str, Mapping[str, Adoption]] | None = None,
                         allowed_grants: Mapping[str, Iterable[Grant]] | None = None,
                         **kwargs: Any) -> dict[str, Any]:
    adoptions, grants = allowed_adoptions or {}, allowed_grants or {}
    return {env: weak for env in sorted(baseline)
            if (weak := protection_diff(baseline[env], current.get(env, {}),
                                        allowed_adoptions=adoptions.get(env),
                                        allowed_grants=grants.get(env, ()), **kwargs))}


def live_protection_diff(scenario: Scenario, before: Mapping[str, Any], after: Mapping[str, Any], *,
                         allowed_adoptions: Mapping[str, Mapping[str, Adoption]],
                         allowed_grants: Mapping[str, Iterable[Grant]]) -> dict[str, Any]:
    """The step/phase/scenario-end check: weaker protection or any grant or
    CAN_RUN widening beyond the scenario's declared GrantDelta. A scenario that
    is expected to weaken protection is still held to its grant allowance."""
    weak = protection_diff_envs(before, after, include_grants=True, allowed_adoptions=allowed_adoptions,
                                allowed_grants=allowed_grants)
    if scenario.key in WEAKENING_EXPECTED:
        weak = {env: {"widened_grants": found["widened_grants"]}
                for env, found in weak.items() if "widened_grants" in found}
    return weak


def unexpected_changes(scenario: str, before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, list[str]]:
    allowed = ALLOWED_CHANGES[scenario]
    result: dict[str, list[str]] = {}
    for env in ("dev", "prod"):
        changed = set(snapshot_diff(before.get(env, {}), after.get(env, {}))) - allowed
        if changed: result[env] = sorted(changed)
    return result


def release_plan_workspace_diff(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    """Release planning may write immutable manifests, but never live/state facts."""
    return {env: {key: value for key, value in snapshot_diff(before.get(env, {}), after.get(env, {})).items()
                  if key != "receipts"}
            for env in ("dev", "prod")
            if any(key != "receipts" for key in snapshot_diff(before.get(env, {}), after.get(env, {})))}


@dataclasses.dataclass(frozen=True)
class CheckContext:
    """What checks are evaluated over: one step, or a whole scenario."""
    before: Mapping[str, Any]
    after: Mapping[str, Any]
    cloud_root: Path
    # Per env, the verify-access result file's signature when the span began.
    verify_before: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    adoptions: Mapping[str, Mapping[str, Adoption]] = dataclasses.field(default_factory=dict)


VERIFY_RESULT = Path("generated") / ".verify_access.json"
READER_ACK = re.compile(r"[^:\s]+:reader:(?P<table>[^:\s]+):(?P<principal>[^:#,\n]+)")


def derived_values(values: Mapping[str, str]) -> dict[str, str]:
    """Values that checks read from other values: S6's reader grant is its ack."""
    match = READER_ACK.fullmatch(values.get("ack_entry", ""))
    derived = {"reader_table": match["table"], "reader_principal": match["principal"]} if match else {}
    return {**values, **derived}


def describe_check(check: Any) -> str:
    if not (dataclasses.is_dataclass(check) and hasattr(check, "kind")): return repr(check)
    return f"{check.kind}(" + ", ".join(f"{f.name}={getattr(check, f.name)!r}" for f in dataclasses.fields(check)) + ")"


def check_placeholders(check: Any) -> set[str]:
    found: set[str] = set()
    def walk(value: Any) -> None:
        if isinstance(value, str): found.update(PLACEHOLDER.findall(value))
        elif isinstance(value, tuple):
            for item in value: walk(item)
    if dataclasses.is_dataclass(check):
        for f in dataclasses.fields(check): walk(getattr(check, f.name))
    return found


def render_check(check: Any, values: Mapping[str, str]) -> Any:
    def one(value: Any) -> Any:
        if isinstance(value, tuple): return tuple(one(item) for item in value)
        if not isinstance(value, str): return value
        missing = sorted({name for name in PLACEHOLDER.findall(value) if not values.get(name)})
        if missing: raise ValueError(f"unresolved placeholder(s): {', '.join(missing)}")
        return PLACEHOLDER.sub(lambda m: values[m.group(1)], value)
    return dataclasses.replace(check, **{f.name: one(getattr(check, f.name)) for f in dataclasses.fields(check)})


def _shape_problems(check: Any) -> list[str]:
    problems = [] if check.env in ENVS else [f"env must be one of {', '.join(ENVS)}"]
    if isinstance(check, (DiffLimitedTo, Unchanged)):
        if check.component not in SNAPSHOT_COMPONENTS:
            problems.append(f"component must be one of {', '.join(SNAPSHOT_COMPONENTS)}")
        if not check.selectors or not all(isinstance(s, str) and s for s in check.selectors):
            problems.append("needs at least one non-empty selector")
        if isinstance(check, DiffLimitedTo) and "*" in check.selectors:
            problems.append("a '*' selector limits nothing")
    if isinstance(check, (AccessLevel, AccessVerified)) and not check.target:
        problems.append("needs a table or column target")
    if isinstance(check, AccessLevel):
        if check.level not in ACCESS_LEVELS: problems.append(f"level must be one of {', '.join(ACCESS_LEVELS)}")
        if not check.principal: problems.append("needs a principal (or '*')")
    if isinstance(check, AgentIdUnchanged) and not check.agent:
        problems.append("needs an agent key")
    if isinstance(check, GrantDelta):
        for grant in (*check.added, *check.may_add, *check.revoked):
            if not (isinstance(grant, tuple) and len(grant) == 3
                    and all(isinstance(part, str) and part.strip() for part in grant)):
                problems.append(f"grant {grant!r} must be (principal, privilege, securable)")
            elif any(ch in part for part in grant for ch in "*?[") or grant[2] == "agent:":
                problems.append(f"grant {grant!r} must name one principal, privilege and securable exactly")
    return problems


def check_problems(scenario: Scenario) -> list[str]:
    """Every check a scenario carries that has no evaluator or is malformed."""
    located = [(f"step {step.name!r}", check) for step in scenario.steps for check in step.assertions]
    located += [("scenario", check) for check in scenario.checks]
    problems = []
    for where, check in located:
        if type(check) not in EVALUATORS:
            problems.append(f"{where}: {describe_check(check)} has no evaluator; use a typed check "
                            f"({', '.join(sorted(kind.kind for kind in EVALUATORS))})")
        else:
            problems += [f"{where}: {describe_check(check)}: {p}" for p in _shape_problems(check)]
    return problems


def _agent_space_ids(snapshot: Mapping[str, Any], key: str) -> set[str]:
    """Space IDs recorded for an agent key (``a``, ``a.auto``, ``a.space``) or a literal space ID."""
    return {str(agent.get("space_id")) for agent in snapshot.get("agents") or []
            if isinstance(agent, Mapping)
            and (str(agent.get("space_id")) == key or str(agent.get("name") or "").split(".", 1)[0] == key)}


def _expand_grants(grants: Iterable[Grant], *snapshots: Mapping[str, Any]) -> tuple[set[Grant], list[str]]:
    expanded: set[Grant] = set(); problems: list[str] = []
    for grant in grants:
        for principal, privilege, securable in itertools.product(*(
                [item.strip() for item in part.split(",") if item.strip()] for part in grant)):
            key = _grant_key((principal, privilege, securable))
            if securable.startswith("agent:"):
                ids = set().union(*(_agent_space_ids(snap, securable[len("agent:"):]) for snap in snapshots))
                if not ids: problems.append(f"{securable} has no space ID in the snapshots")
                expanded |= {(key[0], key[1], f"genie:{space_id}") for space_id in ids}
            elif key[1] in ("USE CATALOG", "USE SCHEMA"):
                parts = securable.split(".")
                expanded.add((key[0], key[1], ".".join(parts[:1 if key[1] == "USE CATALOG" else 2])))
            else:
                expanded.add(key)
    return expanded, problems


def grant_allowances(checks: Iterable[Any], before: Mapping[str, Any],
                     after: Mapping[str, Any]) -> dict[str, set[Grant]]:
    """Per env, the exact grants the scenario's GrantDelta checks expect or permit."""
    allowed: dict[str, set[Grant]] = {}
    for check in checks:
        if isinstance(check, GrantDelta):
            grants, _ = _expand_grants((*check.added, *check.may_add),
                                       before.get(check.env, {}), after.get(check.env, {}))
            allowed.setdefault(check.env, set()).update(grants)
    return allowed


def _eval_grant_delta(check: GrantDelta, ctx: CheckContext) -> list[str]:
    snaps = (ctx.before.get(check.env, {}), ctx.after.get(check.env, {}))
    before, after = ({_grant_key(g) for g in _grant_facts(snap)} for snap in snaps)
    required, problems = _expand_grants(check.added, *snaps)
    optional, _ = _expand_grants(check.may_add, *snaps)
    revoked, revoke_problems = _expand_grants(check.revoked, *snaps)
    added, removed = after - before, before - after
    show = lambda grants: ", ".join(" ".join(g) for g in sorted(grants))
    problems += revoke_problems
    if required - added: problems.append(f"{check.env}: expected grant(s) not newly added: {show(required - added)}")
    if added - required - optional:
        problems.append(f"{check.env}: unexpected grant(s) added: {show(added - required - optional)}")
    if revoked - removed: problems.append(f"{check.env}: expected revoke(s) missing: {show(revoked - removed)}")
    if removed - revoked: problems.append(f"{check.env}: unexpected grant(s) revoked: {show(removed - revoked)}")
    return problems


def _uc_identity(kind: str, row: Mapping[str, Any], catalog: str) -> str:
    if kind.endswith("_grants"):
        return ".".join(str(row[k]) for k in ("table_catalog", "table_schema", "table_name",
                                              "catalog_name", "schema_name") if row.get(k))
    if kind == "governed_tags":
        return ".".join(str(row.get(k)) for k in ("catalog_name", "schema_name", "table_name", "column_name"))
    if kind == "mask_functions":
        return ".".join(str(row.get(k)) for k in ("routine_catalog", "routine_schema", "routine_name"))
    if kind == "abac_policies":
        return str(row.get("table"))
    return catalog


def _component_items(snapshot: Mapping[str, Any], component: str) -> dict[str, list[str]]:
    """A snapshot component as resource identity -> its canonical facts."""
    items: dict[str, set[str]] = {}
    def add(identity: str, value: Any) -> None:
        items.setdefault(identity, set()).add(json.dumps(canonical(value), sort_keys=True, default=str))
    data = snapshot.get(component)
    if component == "agents":
        # One live space, however many local files name it.
        for agent in data or []:
            if isinstance(agent, Mapping):
                add(str(agent.get("space_id")), {"export": agent.get("export"), "acl": agent.get("acl")})
    elif component == "uc":
        for catalog, evidence in (data or {}).items():
            for kind, rows in (evidence or {}).items() if isinstance(evidence, Mapping) else []:
                for row in rows or []:
                    add(_uc_identity(kind, row, str(catalog)) if isinstance(row, Mapping) else str(catalog), {kind: row})
    elif isinstance(data, Mapping):
        for key, value in data.items(): add(str(key), value)
    else:
        for row in data or []: add(str(row.get("path")) if isinstance(row, Mapping) else str(row), row)
    return {identity: sorted(values) for identity, values in items.items()}


def _changed_resources(check: DiffLimitedTo | Unchanged, ctx: CheckContext) -> tuple[list[str], list[str]]:
    """(changed resources the selectors cover, changed resources they don't)."""
    before = ctx.before.get(check.env, {}); after = ctx.after.get(check.env, {})
    selectors = list(check.selectors)
    if check.component == "agents":
        selectors = [space_id for key in check.selectors
                     for space_id in (_agent_space_ids(before, key) | _agent_space_ids(after, key) or {key})]
    left, right = _component_items(before, check.component), _component_items(after, check.component)
    changed = sorted(k for k in set(left) | set(right) if left.get(k) != right.get(k))
    covered = lambda identity: any(identity == s or identity.startswith(s + ".") or fnmatch.fnmatchcase(identity, s)
                                   for s in selectors)
    return [k for k in changed if covered(k)], [k for k in changed if not covered(k)]


def _eval_diff_limited_to(check: DiffLimitedTo, ctx: CheckContext) -> list[str]:
    outside = _changed_resources(check, ctx)[1]
    return [f"{check.env} {check.component} changed outside {list(check.selectors)}: {', '.join(outside)}"] if outside else []


def _eval_unchanged(check: Unchanged, ctx: CheckContext) -> list[str]:
    inside = _changed_resources(check, ctx)[0]
    return [f"{check.env} {check.component} changed: {', '.join(inside)}"] if inside else []


def _eval_agent_id_unchanged(check: AgentIdUnchanged, ctx: CheckContext) -> list[str]:
    before = _agent_space_ids(ctx.before.get(check.env, {}), check.agent)
    after = _agent_space_ids(ctx.after.get(check.env, {}), check.agent)
    if not before:
        return [f"{check.env} agent {check.agent!r} has no space ID in the baseline snapshot; its stability is unproven"]
    if after != before:
        return [f"{check.env} agent {check.agent!r} space ID {sorted(before)} -> {sorted(after)}: recreated or lost"]
    return []


def file_signature(path: Path) -> tuple[int, str] | None:
    try:
        return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()
    except FileNotFoundError:
        return None


def verify_signatures(cloud_root: Path) -> dict[str, tuple[int, str] | None]:
    return {env: file_signature(cloud_root / "envs" / env / VERIFY_RESULT) for env in ENVS}


def _verify_entries(check: AccessLevel | AccessVerified, ctx: CheckContext,
                    kinds: Sequence[str]) -> tuple[list[Mapping[str, Any]], list[str]]:
    """The verify-access result entries for a check's target, written during the span."""
    path = ctx.cloud_root / "envs" / check.env / VERIFY_RESULT
    signature = file_signature(path)
    if signature is None:
        return [], [f"{check.env}: no verify-access result at {path}"]
    if signature == ctx.verify_before.get(check.env):
        return [], [f"{check.env}: verify-access result was not rewritten during this span (stale)"]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return [], [f"{check.env}: verify-access result is not JSON"]
    entries = data.get("checks") if isinstance(data, Mapping) else None
    if not isinstance(entries, list):
        return [], [f"{check.env}: verify-access result has no per-check access results"]
    targets = sorted(ctx.adoptions.get(check.env, {})) if check.target == ADOPTED_COLUMNS else [check.target]
    if not targets:
        return [], [f"{check.env}: no migrate-plan adoption was recorded to verify"]
    matched: list[Mapping[str, Any]] = []; problems: list[str] = []
    for target in targets:
        found = [entry for entry in entries if isinstance(entry, Mapping) and entry.get("kind") in kinds
                 and target.lower() in (str(entry.get("table")).lower(),
                                        f"{entry.get('table')}.{entry.get('column')}".lower())]
        if not found: problems.append(f"{check.env}: verify-access did not check {target}")
        matched += found
    return matched, problems


def _entry_label(entry: Mapping[str, Any]) -> str:
    return f"{entry.get('kind')} {entry.get('table')}" + (f".{entry['column']}" if entry.get("column") else "")


def _never_raw_function(function: Any) -> bool:
    normalized = _normalized_mask(str(function or "").rsplit(".", 1)[-1])
    return bool(normalized) and normalized[2] == "full" and is_never_raw(normalized[0])


def _has_level(entry: Mapping[str, Any], access: Mapping[str, Any], level: str) -> bool:
    observed = access.get("observed")
    if level == "masked": return observed in ("partial", "full", "masked")
    if level == "redacted": return observed == "full" and _never_raw_function(entry.get("full_function"))
    return observed == level


def _eval_access_level(check: AccessLevel, ctx: CheckContext) -> list[str]:
    entries, problems = _verify_entries(check, ctx, ("column-mask",))
    for entry in entries:
        principals = entry.get("principals") or {}
        chosen = principals if check.principal == "*" else {
            p: v for p, v in principals.items() if p == check.principal}
        if not chosen:
            problems.append(f"{_entry_label(entry)}: verify-access checked no principal {check.principal!r}")
        wrong = {p: v.get("observed") for p, v in chosen.items() if not _has_level(entry, v, check.level)}
        if wrong:
            problems.append(f"{_entry_label(entry)}: not {check.level} for {wrong}")
    return problems


def _eval_access_verified(check: AccessVerified, ctx: CheckContext) -> list[str]:
    entries, problems = _verify_entries(check, ctx, ("column-mask", "row-filter"))
    for entry in entries:
        principals = entry.get("principals") or {}
        if entry.get("status") != "PASS":
            problems.append(f"{_entry_label(entry)}: verify-access status {entry.get('status')}")
        if not principals:
            problems.append(f"{_entry_label(entry)}: no principal was checked")
        wrong = {p: f"{v.get('observed')} (expected {v.get('expected')})" for p, v in principals.items()
                 if v.get("observed") != v.get("expected")}
        if wrong:
            problems.append(f"{_entry_label(entry)}: {wrong}")
    return problems


EVALUATORS: Mapping[type, Callable[[Any, CheckContext], list[str]]] = {
    GrantDelta: _eval_grant_delta,
    AccessLevel: _eval_access_level,
    AccessVerified: _eval_access_verified,
    DiffLimitedTo: _eval_diff_limited_to,
    Unchanged: _eval_unchanged,
    AgentIdUnchanged: _eval_agent_id_unchanged,
}


def evaluate_checks(checks: Iterable[Any], ctx: CheckContext) -> list[dict[str, Any]]:
    return [{"check": describe_check(check), "problems": EVALUATORS[type(check)](check, ctx)} for check in checks]


def failed_checks(results: Iterable[Mapping[str, Any]]) -> list[str]:
    return [f"{r['check']}: {'; '.join(r['problems'])}" for r in results if r["problems"]]


def _run_action(step: Step, scenario: Scenario, repo: Checkout, values: Mapping[str, str]) -> None:
    if step.action == "checkout_value":
        repo.require_clean(f"{scenario.key}-before-checkout")
        repo.switch(values[step.value or ""], f"{scenario.key}-checkout-{step.value}")
        return
    if step.action == "add_ack":
        entry = values[step.value or ""]
        if not re.fullmatch(r"prod:(?:weaken|reader|unclassified|revoke|rollback):[^\s:#][^\n#]*", entry):
            raise RuntimeError("ack_entry is not an exact typed prod acknowledgement")
        path = repo.runner.cwd / "envs" / "prod" / "ack.txt"
        prior = path.read_text(encoding="utf-8") if path.exists() else ""
        if entry not in prior.splitlines(): path.write_text(prior.rstrip("\n") + ("\n" if prior.strip() else "") + entry + "\n", encoding="utf-8")
        return
    if step.action == "local_merge":
        repo.local_merge(scenario.key)
        return
    if step.action == "write_agent_file":
        write_agent_file(repo.runner.cwd / "envs" / "dev", values)
        return
    raise RuntimeError(f"unknown scenario action: {step.action}")


def _restore_baseline(runner: CommandRunner, repo: Checkout, targets: set[str], label: str) -> None:
    """Restore via a forward release of the requested commit's tree; never Terraform or direct APIs."""
    required = {"rehearse", "release-plan", "release", "maintain"}
    missing = required - targets
    if missing: raise RuntimeError(f"restore targets unavailable: {', '.join(sorted(missing))}")
    repo.reset()
    repo.forward_fix()
    result = runner.run(("make", "rehearse", "ENV=dev"), f"{label}-restore-rehearse")
    if result.returncode: raise RuntimeError("baseline restore rehearse failed")
    if deterministic_env(runner.cwd, "dev") and import_unset_treatment(result.stdout):
        raise RuntimeError("baseline restore rehearse attempted to clear gr_treatment")
    for phase in range(1, MAX_RELEASE_PHASES + 1):
        repo.verify(f"{label}-restore-phase-{phase}")
        plan = runner.run(("make", "release-plan", "ENV=prod"), f"{label}-restore-release-plan-{phase}")
        if plan.returncode: raise RuntimeError("baseline restore release-plan failed")
        if deterministic_env(runner.cwd, "prod") and import_unset_treatment(plan.stdout):
            raise RuntimeError("baseline restore release-plan attempted to clear gr_treatment")
        manifest = parse_manifest(plan.stdout)
        result = runner.run(("make", "release", "ENV=prod", f"MANIFEST={manifest}"),
                            f"{label}-restore-release-{phase}")
        if result.returncode: raise RuntimeError("baseline restore release failed")
        if deterministic_env(runner.cwd, "prod") and import_unset_treatment(result.stdout):
            raise RuntimeError("baseline restore release attempted to clear gr_treatment")
        if not release_needs_another_phase(plan.stdout + "\n" + result.stdout):
            break
    else:
        raise RuntimeError(f"baseline restore exceeded {MAX_RELEASE_PHASES} release phases")
    result = runner.run(("make", "maintain", "ENV=prod"), f"{label}-restore-maintain")
    if result.returncode: raise RuntimeError("baseline restore maintain failed")
    if deterministic_env(runner.cwd, "prod") and import_unset_treatment(result.stdout):
        raise RuntimeError("baseline restore maintain attempted to clear gr_treatment")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cloud", choices=("aws", "azure"), required=True)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--checkout", type=Path, help="persistent v1 deployment checkout")
    parser.add_argument("--commit", help="required 40-hex commit for live runs")
    parser.add_argument("--scenarios", default="all", help="comma-separated keys; use --list-scenarios to inspect")
    parser.add_argument("--reports", type=Path, default=Path("/tmp/genierails-e2e-reports"))
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--non-interactive", action="store_true", help="report scenarios with human gates as NOT RUN")
    parser.add_argument("--list-scenarios", action="store_true")
    parser.add_argument("--confirm-live", action="store_true", help="required for non-dry-run execution")
    parser.add_argument("--value", action="append", default=[], metavar="KEY=VALUE", help="safe scenario placeholder")
    parser.add_argument("--timeout", type=int, default=1800, help="per-command timeout in seconds")
    return parser.parse_args(argv)


def _human_gate(scenario: Scenario, step: Step) -> dict[str, str]:
    if not sys.stdin.isatty(): raise HumanAbort("stdin is not a TTY at a human gate")
    try:
        response = input(f"[{scenario.key}] {step.human_prompt} Type {scenario.key} to confirm: ").strip()
    except EOFError as exc:
        raise HumanAbort("stdin closed at a human gate") from exc
    if response != scenario.key: raise RuntimeError(f"human gate requires exact token {scenario.key!r}")
    return {"step": step.name, "token": response, "at": dt.datetime.now(dt.timezone.utc).isoformat()}


def _raise_interrupt(signum: int, _frame: Any) -> None:
    raise KeyboardInterrupt(signal.Signals(signum).name)


def main(argv: Sequence[str] | None = None, *, snapshot_factory: Callable[[Path, Path], SnapshotProvider] = LiveSnapshotProvider) -> int:
    args = parse_args(argv)
    if args.list_scenarios:
        for scenario in SCENARIOS: print(f"{scenario.key:10} {scenario.title}")
        return 0
    wanted = {x.strip() for x in args.scenarios.split(",") if x.strip()}
    selected = list(SCENARIOS) if wanted == {"all"} else [s for s in SCENARIOS if s.key in wanted]
    unknown = wanted - {s.key for s in SCENARIOS} - {"all"}
    if unknown: raise SystemExit(f"unknown scenarios: {', '.join(sorted(unknown))}")
    repo_root = args.repo.resolve(); checkout_dir = (args.checkout or repo_root).resolve(); cloud_root = checkout_dir / args.cloud
    reports = args.reports.resolve()
    values: dict[str, str] = {}
    for item in args.value:
        if "=" not in item or not item.split("=", 1)[0]: raise SystemExit(f"--value must be KEY=VALUE, got {item!r}")
        key, value = item.split("=", 1); values[key] = value
    values = derived_values(values)
    if args.dry_run:
        # Dry-run is hermetic: source parsing only, but it resolves the selected
        # cloud entry point and describes every reason a live scenario cannot run.
        cloud_makefile = repo_root / args.cloud / "Makefile"
        target_text = cloud_makefile.read_text(encoding="utf-8") + "\n" + (
            repo_root / "shared" / "Makefile.shared").read_text(encoding="utf-8")
        targets = set(re.findall(r"^([a-zA-Z0-9][\w.-]*):(?:\s|$)", target_text, re.M))
        unevaluable = False
        for scenario in selected:
            plan = scenario_plan(scenario, targets)
            problems = check_problems(scenario)
            unevaluable |= bool(problems)
            gates = [step.human_prompt for step in scenario.steps if step.human_prompt]
            missing_values = unresolved_values(scenario, values)
            refusals = [{"step": step.name, "pattern": step.refusal,
                         "found_in_source": bool(step.refusal and refusal_found_in_source(repo_root, step.refusal))}
                        for step in scenario.steps if step.expect_failure]
            would_not_run = [x["skip_reason"] for x in plan if x["status"] == "skip"]
            would_not_run += ["human UI gate requires typed confirmation"] if gates else []
            would_not_run += [f"missing value: {name}" for name in missing_values]
            would_not_run += [f"refusal pattern not found in source: {x['pattern']}"
                              for x in refusals if not x["found_in_source"]]
            if not import_unset_source_anchored(repo_root):
                would_not_run.append("import_existing.sh no longer prints the gr_treatment cleanup lines "
                                     "the UNSET log guard matches")
            payload = {"scenario": scenario.key, "status": "dry-run", "cloud": args.cloud,
                       "known_failure": scenario.known_failure,
                       "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                       "target_source": str(cloud_makefile), "steps": plan, "snapshot_diff": {},
                       "human_gates": gates, "missing_values": missing_values,
                       "refusal_patterns": refusals, "would_not_run": would_not_run,
                       "checks": [describe_check(check) for check in scenario.checks],
                       "check_problems": problems}
            if problems: payload["status"] = "ERROR"
            write_report(reports, scenario, payload)
            known = f" — KNOWN FAILURE until {scenario.known_failure}" if scenario.known_failure else ""
            print(f"[{scenario.key}] {scenario.title}{known}" + (" — NOT RUN live (see report)" if payload["would_not_run"] else ""))
            for item in payload["steps"]:
                suffix = f" — SKIP ({item['skip_reason']})" if item["status"] == "skip" else ""
                print(f"  {item['command'] or ('[' + item['action'] + ']' if item['action'] else '[human pause]')}{suffix}")
            for problem in problems:
                print(f"  ERROR: {problem}")
        return 1 if unevaluable else 0
    if not args.confirm_live: raise SystemExit("live execution requires --confirm-live")
    if not args.checkout: raise SystemExit("live execution requires explicit --checkout")
    if not args.commit or not re.fullmatch(r"[0-9a-fA-F]{40}", args.commit):
        raise SystemExit("live execution requires explicit 40-hex --commit")
    if not args.state_dir: raise SystemExit("live execution requires --state-dir outside the checkout")
    state_dir = args.state_dir.resolve()
    if state_dir == checkout_dir or checkout_dir in state_dir.parents: raise SystemExit("--state-dir must be outside the checkout")
    if reports == checkout_dir or checkout_dir in reports.parents: raise SystemExit("--reports must be outside the checkout")
    # Checked before any log, switch, snapshot or make: nothing runs on a refusal.
    problems = customer_checkout_problems(checkout_dir, args.cloud, args.commit.lower())
    if problems:
        raise SystemExit(
            f"live execution requires a customer-shaped checkout that commits envs/; {checkout_dir} at "
            f"{args.commit[:12]} does not:\n" + "".join(f"  - {p}\n" for p in problems)
            + f"  Fix: cd {args.cloud} && make setup ENV=dev GITIGNORE=customer && "
            f"make setup ENV=prod GITIGNORE=customer, commit {args.cloud}/.gitignore "
            f"and {args.cloud}/envs/, check that commit out and pass it as --commit. Nothing was run.")
    log_root = reports / "logs"; log_root.mkdir(parents=True, exist_ok=True)
    secrets = collect_secrets(cloud_root)
    runner = CommandRunner(cloud_root, log_root, {"GENIERAILS_STATE_DIR": str(state_dir), "RELEASE_REMOTE": E2E_REMOTE,
                                                  "RELEASE_BRANCH": "main", **GIT_IDENTITY},
                           secrets=secrets, timeout=args.timeout)
    repo = Checkout(runner, args.commit.lower(), reports / "e2e-remote.git")
    repo.switch(args.commit.lower(), "initial-checkout")
    targets = make_targets(cloud_root, min(args.timeout, 60))
    provider = snapshot_factory(cloud_root, state_dir / args.cloud)

    def capture() -> dict[str, Any]:
        snaps = {env: provider.capture(env) for env in ("dev", "prod")}
        runner.secrets = tuple(set(runner.secrets) | set(getattr(provider, "secret_values", ())))
        return snaps

    failed = False
    remote_ready = False
    run_baseline: dict[str, Any] = {}
    old_handlers = {sig: signal.signal(sig, _raise_interrupt) for sig in (signal.SIGTERM, signal.SIGHUP)}
    try:
        for scenario in selected:
            plan = scenario_plan(scenario, targets)
            # Everything a scenario needs is checked before it touches anything:
            # one missing target, gate or value and nothing of it runs.
            blocked = [item["skip_reason"] for item in plan if item["status"] == "skip"]
            if scenario.key == "s14" and not governance_only_env(cloud_root, "dev"):
                blocked.append("s14 requires dev to be a deterministic governance-only environment")
            if any(s.human_prompt for s in scenario.steps) and (args.non_interactive or not sys.stdin.isatty()):
                blocked.append("human UI gate needs an interactive TTY and a typed confirmation")
            missing_values = unresolved_values(scenario, values)
            unevaluable = check_problems(scenario)
            if blocked or missing_values or unevaluable:
                for item in plan:
                    if item["status"] == "planned": item["status"] = "NOT RUN"
                status = "NOT RUN" if blocked and not unevaluable else "ERROR"
                failed |= status == "ERROR"
                error = ("; ".join(unevaluable) if unevaluable else
                         f"unresolved placeholder(s): {', '.join(missing_values)}" if missing_values and not blocked else None)
                write_report(reports, scenario, {"scenario": scenario.key, "status": status, "cloud": args.cloud,
                    "commit": args.commit, "known_failure": scenario.known_failure, "blocked_by": blocked,
                    "error": error, "steps": plan, "snapshot_diff": {}}, runner.secrets)
                continue
            before: dict[str, Any] = {}
            after: dict[str, Any] = {}
            restored: dict[str, Any] = {}
            scenario_status = "passed"
            error_message: str | None = None
            refusal_diffs: list[dict[str, Any]] = []
            successful_step_diffs: list[dict[str, Any]] = []
            manifest = values.get("manifest", "")
            release_plan_output = ""
            mutated = False
            restored_lifecycle = False
            manual_restore_commands: list[str] = []
            post_reset_diff: dict[str, Any] = {}
            stop = False
            confirmations: list[dict[str, str]] = []
            allowed_adoptions: dict[str, dict[str, Adoption]] = {}
            scenario_checks = [render_check(check, values) for check in scenario.checks]
            declared_checks: list[Any] = list(scenario_checks)
            scenario_check_results: list[dict[str, Any]] = []
            try:
                # Pin: every scenario starts at exactly the requested commit.
                repo.switch(repo.baseline, f"{scenario.key}-pin")
                if not remote_ready:
                    repo.init_remote(); remote_ready = True
                scenario_verify = verify_signatures(cloud_root)
                before = capture()
                if not run_baseline: run_baseline = before
                for index, (step, item) in enumerate(zip(scenario.steps, plan), 1):
                    label = f"{scenario.key}-{index}"
                    repo.verify(label)
                    item["head"] = repo.head
                    if step.human_prompt:
                        confirmations.append(_human_gate(scenario, step))
                        item["status"] = "passed"; continue
                    if step.action:
                        _run_action(step, scenario, repo, values)
                        item["status"] = "passed"; continue
                    argv2 = [render(part, {**values, "manifest": manifest}) for part in step.command]
                    step_checks = [render_check(check, {**values, "manifest": manifest}) for check in step.assertions]
                    declared_checks.extend(step_checks)
                    step_snapshots = any(check.snapshots for check in step_checks)
                    applying = step.target in APPLYING
                    if applying: repo.require_clean(label)
                    step_verify = verify_signatures(cloud_root)
                    step_before = capture() if (step.expect_failure or applying or step_snapshots) else None
                    # Assume a mutation from the moment an applying command
                    # starts (it may be interrupted half-way); a refusal that
                    # held with an unchanged snapshot takes the assumption back.
                    prior_mutated = mutated
                    mutated |= applying or step.target == "ungovern"
                    step_env = dict(step.extra_env)
                    if step.expect_failure and step.refusal and step.refusal_in_ci:
                        step_env = {**CI_ENV, **step_env}
                    elif step.expect_failure and step.refusal:
                        step_env = {**step_env, **NON_CI_ENV}
                    result = runner.run(argv2, f"{label}-{step.name}", step_env)
                    item["exit_code"] = result.returncode
                    if step.target == "migrate-plan" and not step.expect_failure:
                        ok = result.returncode in (0, 1) and "=== Migration dry run" in result.stdout and "Result:" in result.stdout
                        if not ok: item["assertion"] = "migrate-plan must exit 0/1 with its report header and Result line"
                    elif step.expect_failure:
                        ok = result.returncode != 0 and bool(step.refusal and re.search(step.refusal, result.stdout))
                        if not ok: item["assertion"] = "nonzero exit and the expected refusal message are both required"
                    else: ok = result.returncode == 0
                    if applying and deterministic_env(cloud_root, step.env) and import_unset_treatment(result.stdout):
                        ok = False
                        item["assertion"] = "deterministic apply attempted to clear gr_treatment"
                    item["status"] = "passed" if ok else "ERROR"
                    step_after = capture() if step_before is not None else None
                    if step.expect_failure and step_before is not None and step_after is not None:
                        mutation = snapshot_diff(step_before, step_after)
                        refusal_diffs.append({"step": step.name, "env": step.env or "account", "diff": mutation})
                        if mutation: item["status"] = "ERROR"; item["assertion"] = "refusal changed the snapshot"; ok = False
                    elif applying and step_before is not None and step_after is not None:
                        mutation = snapshot_diff(step_before, step_after)
                        successful_step_diffs.append({"step": step.name, "env": step.env or "account", "diff": mutation})
                        if step.target == "release-plan" and release_plan_workspace_diff(step_before, step_after):
                            item["status"] = "ERROR"; item["assertion"] = "successful release-plan changed the snapshot"; ok = False
                        weak_step = live_protection_diff(
                            scenario, step_before, step_after, allowed_adoptions=allowed_adoptions,
                            allowed_grants=grant_allowances(declared_checks, step_before, step_after))
                        if weak_step:
                            item["status"] = "ERROR"
                            if "assertion" not in item:
                                item["assertion"] = f"successful apply weakened protection: {weak_step}"
                            ok = False
                    if not ok: raise RuntimeError(f"step did not satisfy its contract: {step.name}")
                    if step.expect_failure: mutated = prior_mutated
                    if scenario.key == "migration-cutover" and step.target == "migrate-plan":
                        allowed_adoptions[step.env or "prod"] = parse_migration_mappings(result.stdout)
                    if step.target == "release-plan" and not step.expect_failure:
                        manifest = parse_manifest(result.stdout)
                        release_plan_output = result.stdout
                    if step.target == "release" and not step.expect_failure:
                        phase_output = release_plan_output + "\n" + result.stdout
                        for phase in range(2, MAX_RELEASE_PHASES + 1):
                            if not release_needs_another_phase(phase_output): break
                            repo.verify(f"{label}-phase-{phase}")
                            repo.require_clean(f"{label}-phase-{phase}")
                            phase_before = capture()
                            plan_result = runner.run(("make", "release-plan", "ENV=prod"),
                                                     f"{label}-release-plan-phase-{phase}")
                            if plan_result.returncode: raise RuntimeError(f"release phase {phase} plan failed")
                            phase_plan_after = capture()
                            plan_diff = snapshot_diff(phase_before, phase_plan_after)
                            successful_step_diffs.append({"step": f"{step.name} phase {phase} plan", "env": "prod", "diff": plan_diff})
                            if release_plan_workspace_diff(phase_before, phase_plan_after):
                                raise RuntimeError(f"release phase {phase} plan changed the snapshot")
                            manifest = parse_manifest(plan_result.stdout)
                            phase_release = runner.run(("make", "release", "ENV=prod", f"MANIFEST={manifest}"),
                                                       f"{label}-release-phase-{phase}")
                            if phase_release.returncode: raise RuntimeError(f"release phase {phase} failed")
                            phase_after = capture()
                            successful_step_diffs.append({"step": f"{step.name} phase {phase}", "env": "prod",
                                                          "diff": snapshot_diff(phase_plan_after, phase_after)})
                            if deterministic_env(cloud_root, "prod") and import_unset_treatment(
                                    plan_result.stdout + "\n" + phase_release.stdout):
                                raise RuntimeError(f"release phase {phase} attempted to clear gr_treatment")
                            weak_phase = live_protection_diff(
                                scenario, phase_plan_after, phase_after, allowed_adoptions=allowed_adoptions,
                                allowed_grants=grant_allowances(declared_checks, phase_plan_after, phase_after))
                            if weak_phase:
                                raise RuntimeError(f"release phase {phase} weakened protection: {weak_phase}")
                            phase_output = plan_result.stdout + "\n" + phase_release.stdout
                        else:
                            if release_needs_another_phase(phase_output):
                                raise RuntimeError(f"release exceeded {MAX_RELEASE_PHASES} phases")
                    if step_checks:
                        item["checks"] = evaluate_checks(step_checks, CheckContext(
                            step_before or {}, capture() if step_snapshots else {}, cloud_root,
                            step_verify, allowed_adoptions))
                        if failures := failed_checks(item["checks"]):
                            item["status"] = "ERROR"; item["assertion"] = "; ".join(failures)
                            raise RuntimeError(f"step did not satisfy its checks: {step.name}: {'; '.join(failures)}")
                    if applying: repo.require_clean(f"after-{label}")
                after = capture()
                weak = live_protection_diff(scenario, before, after, allowed_adoptions=allowed_adoptions,
                                            allowed_grants=grant_allowances(declared_checks, before, after))
                if weak: raise RuntimeError(f"protection became weaker or access widened: {weak}")
                scenario_check_results = evaluate_checks(scenario_checks, CheckContext(
                    before, after, cloud_root, scenario_verify, allowed_adoptions))
                if failures := failed_checks(scenario_check_results):
                    raise RuntimeError(f"scenario checks failed: {'; '.join(failures)}")
                unexpected = unexpected_changes(scenario.key, before, after)
                if unexpected: raise RuntimeError(f"unexpected snapshot components changed: {unexpected}")
            except KeyboardInterrupt as exc:
                scenario_status = "interrupted"; error_message = f"interrupted ({exc or 'SIGINT'})"; failed = True; stop = True
            except HumanAbort as exc:
                scenario_status = "NOT RUN"; error_message = str(exc); stop = True
            except Exception as exc:
                scenario_status = "ERROR"; error_message = f"{type(exc).__name__}: {exc}"; failed = True
            finally:
                try:
                    if before:
                        if not after:
                            after = capture()
                        read_only_diff = snapshot_diff(before, after) if scenario.read_only else {}
                        if scenario.read_only and read_only_diff:
                            scenario_status = "RESTORE NEEDED"
                            failed = True
                            stop = True
                            manual_restore_commands = list(MANUAL_RESTORE_COMMANDS)
                            error_message = "; ".join(filter(None, (
                                error_message,
                                "read-only/refusal-only scenario changed the snapshot; no automatic apply was attempted",
                            )))
                        elif mutated and not scenario.read_only:
                            _restore_baseline(runner, repo, targets, scenario.key)
                            restored_lifecycle = True
                        elif repo.main_tip != repo.baseline:
                            # An unreleased local merge must not leak into the
                            # next scenario's main.
                            repo.reset(); repo.forward_fix()
                        repo.reset()
                        repo.switch(repo.baseline, f"{scenario.key}-restored")
                        restored = capture()
                        post_reset_diff = snapshot_diff(before, restored) if scenario.read_only else {}
                        if scenario.read_only and post_reset_diff:
                            scenario_status = "RESTORE NEEDED"
                            failed = True
                            stop = True
                            manual_restore_commands = list(MANUAL_RESTORE_COMMANDS)
                            notice = "read-only/refusal-only post-reset snapshot differs; no automatic apply was attempted"
                            if notice not in (error_message or ""):
                                error_message = "; ".join(filter(None, (error_message, notice)))
                        weak_restore = protection_diff_envs(before, restored, include_grants=True,
                                                            allowed_adoptions=allowed_adoptions)
                        if weak_restore and not scenario.read_only:
                            scenario_status = "RESTORE FAILED"; failed = True
                            error_message = "; ".join(filter(None, (error_message, f"protection not restored: {weak_restore}")))
                except BaseException as exc:
                    scenario_status = "RESTORE FAILED"; failed = True; stop = True
                    error_message = "; ".join(filter(None, (error_message, f"restore: {type(exc).__name__}: {exc}")))
                if scenario.known_failure and scenario_status == "passed":
                    scenario_status = f"UNEXPECTED PASS — remove known_failure {scenario.known_failure}"
                    error_message = f"known failure unexpectedly passed; remove {scenario.known_failure}"
                    failed = True
                elif scenario.known_failure and scenario_status == "ERROR":
                    scenario_status = f"ERROR (expected until {scenario.known_failure})"
                    error_message = "; ".join(filter(None, (error_message,
                        f"known failure: expected until {scenario.known_failure}")))
                payload = {"scenario": scenario.key, "status": scenario_status, "error": error_message,
                           "known_failure": scenario.known_failure,
                           "cloud": args.cloud, "commit": args.commit, "steps": plan, "confirmations": confirmations,
                           "checks": scenario_check_results,
                           "before": before, "after": after, "restored": restored,
                           "restored_lifecycle": restored_lifecycle,
                           "manual_restore_commands": manual_restore_commands,
                           "snapshot_diff": snapshot_diff(before, after) if before and after else {},
                           "post_reset_snapshot_diff": post_reset_diff,
                           "refusal_snapshot_diffs": refusal_diffs,
                           "successful_step_snapshot_diffs": successful_step_diffs}
                write_report(reports, scenario, payload, runner.secrets)
            if stop: break
    finally:
        try:
            try:
                if run_baseline:
                    final = capture()
                    weak_final = protection_diff_envs(run_baseline, final, include_grants=True)
                    (reports / "final-protection.json").write_text(json.dumps(redact(
                        {"status": "ERROR" if weak_final else "passed", "weaker_than_baseline": weak_final},
                        runner.secrets), indent=2, sort_keys=True) + "\n")
                    failed |= bool(weak_final)
            except Exception as exc:
                (reports / "final-protection.json").write_text(json.dumps(
                    {"status": "ERROR", "error": redact_text(f"{type(exc).__name__}: {exc}", runner.secrets)}, indent=2) + "\n")
                failed = True
        finally:
            # Even an interrupt during the final capture must not leave the run's remote behind.
            try:
                repo.remove_remote()
            except Exception as exc:
                print(f"warning: could not remove git remote {E2E_REMOTE}: {exc}; "
                      f"run `git remote remove {E2E_REMOTE}` in {checkout_dir}", file=sys.stderr)
            finally:
                for sig, handler in old_handlers.items(): signal.signal(sig, handler)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
