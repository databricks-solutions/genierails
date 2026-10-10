#!/usr/bin/env python3
"""Effective-access verification for GenieRails governance.

Roadmap item #5 ("Stronger Integration Test Assertions"): the existing
integration checks only prove that masks / tags / policies *exist* in the
metastore (see ``information_schema.column_masks`` queries in
``scripts/setup_test_data.py``). They never prove the governance actually
*takes effect* when a real principal runs a query.

This module closes that gap. It verifies masking and row-filtering by
**effect** — it runs the same ``SELECT`` as principals in different access
tiers and compares the *values they get back*:

  * a lower-tier principal must see the **masked** value while a higher-tier
    principal sees the **raw** value for the same row, and
  * a row-filtered table must return **fewer rows** to a restricted principal
    than to an unrestricted one.

Why per-tier test principals?
-----------------------------
Databricks does not offer general per-user query impersonation — you cannot
"run this SELECT as user alice" from an admin service principal. The supported
mechanism is therefore a set of **dedicated test principals**: one service
principal per access tier, each added as a member of that tier's group. Each
principal authenticates with its own OAuth (client_id / client_secret) and runs
the query itself, so Unity Catalog evaluates the FGAC policies against *its*
group membership. See ``docs/effective-access-verification.md``.

Layering
--------
The module is split so the comparison logic is testable without a workspace:

  * **Pure logic** (no Databricks): spec types, ``derive_spec_from_config``,
    and the ``evaluate_*`` comparison functions. Covered by unit tests in
    ``tests/test_verify_effective_access.py`` with mocked query results.
  * **Live layer** (needs a workspace): principal provisioning, per-principal
    query execution, and the ``verify_effective_access_live`` orchestrator.
    Guarded behind the ``--live`` flag / ``GENIERAILS_LIVE_VERIFY=1`` env so a
    plain unit run never touches a cluster.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import re
import secrets
import sys
import threading
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

# ---------------------------------------------------------------------------
# Environment / flag guard
# ---------------------------------------------------------------------------
# The live workspace path is gated on BOTH an explicit CLI flag and this env
# var, so importing this module or running the unit suite never provisions
# principals or hits a cluster.
LIVE_ENV_FLAG = "GENIERAILS_LIVE_VERIFY"

# Mask checks compare a bounded sample of rows per tier, spread across the
# table by a salted hash of the key; set the salt to repeat a run's sample.
SAMPLE_ROWS = 25
SAMPLE_SALT_ENV = "GENIERAILS_VERIFY_SAMPLE_SALT"

# A per-mask "unmasked" comparison principal that is a workspace/metastore
# admin sees raw values for everything; use it as the ground-truth tier when a
# policy masks a column for a *specific* business group (the admin is not a
# member of that group, so it sees the raw value).
DEFAULT_ADMIN_TIER = "__admin__"

# The built-in Databricks pseudo-group that contains every workspace user /
# service principal — including the admin baseline. It is not a provisionable
# access tier, so it is never treated as a test principal. A mask that targets
# it applies to *everyone except* its ``except_principals``, so those exceptions
# are the only reliable raw-value baseline.
ALL_USERS_GROUP = "account users"
OUT_OF_TIER_PRINCIPAL = "__out_of_tier__"
DUAL_TIER_PRINCIPAL = "__dual_tier__"


# ---------------------------------------------------------------------------
# Spec types (pure)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ColumnMaskCheck:
    """One column that should be masked for some tiers and raw for others.

    ``key_column`` is a non-sensitive primary key used to pair the same row
    across principals when comparing values.
    """
    table: str
    column: str
    key_column: str
    masked_principals: tuple[str, ...]
    unmasked_principals: tuple[str, ...]
    policy_name: str = ""
    # catalog.schema.function the policy applies to the column alone (column
    # masks bind no other arguments). With it, the admin baseline can tell a
    # raw value the mask leaves unchanged (a fixed point) from a leak.
    mask_function: str = ""
    # Deterministic governance only.  Each principal maps to raw/partial/full;
    # expected masked values are computed by calling these caller-independent
    # functions as the admin over the paired raw rows.
    expected_tiers: tuple[tuple[str, str], ...] = ()
    partial_function: str = ""
    full_function: str = ""
    # During a tighten-before-loosen move, only these principals may
    # temporarily receive a fail-closed "more than one mask" query error.
    moving_principals: tuple[str, ...] = ()

    def describe(self) -> str:
        return f"column-mask {self.table}.{self.column} (policy={self.policy_name or 'n/a'})"


@dataclass(frozen=True)
class RowFilterCheck:
    """One table whose rows should be restricted for some tiers."""
    table: str
    restricted_principals: tuple[str, ...]
    unrestricted_principals: tuple[str, ...]
    policy_name: str = ""

    def describe(self) -> str:
        return f"row-filter {self.table} (policy={self.policy_name or 'n/a'})"


@dataclass
class VerificationSpec:
    """The set of effective-access checks to run."""
    column_masks: list[ColumnMaskCheck] = field(default_factory=list)
    row_filters: list[RowFilterCheck] = field(default_factory=list)
    # The ABAC config the checks came from ({"fgac_policies", "tag_assignments"}),
    # when known: tells whether a tag on a pairing key is one a mask matches.
    mask_config: Optional[dict[str, Any]] = None
    # Optional test-principal label -> account groups. This permits a single
    # verification SP to exercise overlapping tiers; expected_tiers must use
    # the most privileged configured membership.
    principal_memberships: dict[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def principals(self) -> set[str]:
        """Every principal referenced by any check (the tiers we must provision)."""
        out: set[str] = set()
        for c in self.column_masks:
            out.update(c.masked_principals)
            out.update(c.unmasked_principals)
            out.update(principal for principal, _tier in c.expected_tiers)
        for r in self.row_filters:
            out.update(r.restricted_principals)
            out.update(r.unrestricted_principals)
        out.discard(DEFAULT_ADMIN_TIER)
        out.discard(ALL_USERS_GROUP)
        return out

    def is_empty(self) -> bool:
        return not self.column_masks and not self.row_filters


# ---------------------------------------------------------------------------
# Result types (pure)
# ---------------------------------------------------------------------------
# A verification gate must never report success for something it did not
# actually prove. There are therefore only two passing outcomes — PASS — and
# every non-conclusive outcome (INCONCLUSIVE) is treated as a failure that makes
# the CLI exit non-zero, exactly like a proven violation (FAIL). INCONCLUSIVE is
# kept distinct from FAIL only so the operator can tell "we couldn't verify"
# (usually a test-data / permissions problem) apart from "we proved a leak"
# (a real governance bug); both block the gate.
PASS = "PASS"
FAIL = "FAIL"            # verification proved the policy did NOT take effect
INCONCLUSIVE = "INCONCLUSIVE"  # could not be conclusively verified — NOT a pass

# Every status that is not PASS blocks the gate.
NON_PASSING = (FAIL, INCONCLUSIVE)


@dataclass
class CheckResult:
    kind: str            # "column-mask" | "row-filter"
    target: str          # human description of what was checked
    status: str          # PASS | FAIL | INCONCLUSIVE
    detail: str          # human-readable explanation
    evidence: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        # ONLY a proven PASS counts as success. Inconclusive never passes.
        return self.status == PASS


@dataclass
class EffectiveAccessReport:
    results: list[CheckResult] = field(default_factory=list)
    not_verified: list[CheckResult] = field(default_factory=list)
    # table -> the key that paired every one of its passing mask checks
    pairing_keys: dict[str, str] = field(default_factory=dict)
    sample_note: str = ""   # how many rows the mask checks looked at

    def add(self, result: CheckResult) -> None:
        self.results.append(result)

    @property
    def passed(self) -> bool:
        # An empty report proves nothing, so it does not pass either.
        return bool(self.results) and all(r.ok for r in self.results)

    @property
    def failures(self) -> list[CheckResult]:
        """Every non-passing result (proven failures AND inconclusive checks)."""
        return [r for r in self.results if r.status in NON_PASSING]

    def counts(self) -> dict[str, int]:
        c = {PASS: 0, FAIL: 0, INCONCLUSIVE: 0}
        for r in self.results:
            c[r.status] = c.get(r.status, 0) + 1
        return c

    def summary(self) -> str:
        c = self.counts()
        lines = [
            "=" * 60,
            "  Effective-Access Verification",
            "=" * 60,
        ]
        if not self.results:
            lines.append("  ✗ [INCONCLUSIVE] no checks were run — nothing was verified")
        for r in self.results:
            marker = {PASS: "✓", FAIL: "✗", INCONCLUSIVE: "✗"}.get(r.status, "?")
            lines.append(f"  {marker} [{r.status}] {r.target}")
            if r.status != PASS:
                lines.append(f"        {r.detail}")
        for r in self.not_verified:
            lines.append(f"  ! [NOT VERIFIED] {r.target}")
            lines.append(f"        {r.detail}")
        if self.sample_note:
            lines.append(f"  Sample: {self.sample_note}")
        lines.append("-" * 60)
        total = sum(c.values())
        blocking = c[FAIL] + c[INCONCLUSIVE]
        if self.passed and self.not_verified:
            lines.append(
                f"  RESULT: ROW FILTERS EFFECTIVE — {len(self.not_verified)} mask "
                f"check(s) NOT VERIFIED (no row-pairing key; {c[PASS]} passed / {total})"
            )
        elif self.passed:
            lines.append(f"  RESULT: ALL EFFECTIVE ({c[PASS]} passed / {total})")
        elif not self.results and self.not_verified:
            lines.append(
                f"  RESULT: MASKING NOT VERIFIED — {len(self.not_verified)} mask "
                "check(s) skipped (no row-pairing key)"
            )
        else:
            lines.append(
                f"  RESULT: NOT VERIFIED — {blocking} blocking "
                f"({c[FAIL]} failed, {c[INCONCLUSIVE]} inconclusive, {c[PASS]} passed / {total})"
            )
        lines.append("=" * 60)
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Config parsing / spec derivation (pure)
# ---------------------------------------------------------------------------
_TAG_CONDITION_RE = re.compile(
    r"hasTagValue\(\s*['\"](?P<key>[^'\"]+)['\"]\s*,\s*['\"](?P<value>[^'\"]+)['\"]\s*\)"
)


def _as_str(value: Any) -> str:
    """Normalize an HCL-parsed value (hcl2 wraps scalars in single-item lists)."""
    if isinstance(value, list):
        return str(value[0]).strip() if value else ""
    return str(value or "").strip()


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value]
    return [str(value).strip()]


def parse_tag_conditions(condition: str) -> list[tuple[str, str]]:
    """Return the (tag_key, tag_value) pairs referenced by a match/when condition."""
    return [(m.group("key"), m.group("value")) for m in _TAG_CONDITION_RE.finditer(condition or "")]


def resolve_columns_for_condition(
    condition: str,
    tag_assignments: Sequence[Mapping[str, Any]],
    *,
    entity_type: str = "columns",
) -> list[dict[str, str]]:
    """Resolve a tag condition to the concrete tagged entities.

    Returns a list of ``{"table": ..., "column": ...}`` (column is "" for table
    entities) for every tag assignment whose (key, value) matches the condition.
    Pure — the tag_assignments are the parsed ``tag_assignments = [...]`` blocks.
    """
    wanted = set(parse_tag_conditions(condition))
    if not wanted:
        return []
    out: list[dict[str, str]] = []
    for ta in tag_assignments:
        if _as_str(ta.get("entity_type")) != entity_type:
            continue
        key = _as_str(ta.get("tag_key"))
        val = _as_str(ta.get("tag_value"))
        if (key, val) not in wanted:
            continue
        entity = _as_str(ta.get("entity_name"))
        parts = entity.split(".")
        if entity_type == "columns":
            if len(parts) < 4:
                continue
            table = ".".join(parts[:3])
            column = ".".join(parts[3:])
            out.append({"table": table, "column": column})
        else:  # tables
            table = ".".join(parts[:3]) if len(parts) >= 3 else entity
            out.append({"table": table, "column": ""})
    return out


def effective_mask_policies(fgac_policies: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """The column-mask policies that mask someone once exceptions are removed."""
    return [
        pol for pol in fgac_policies
        if isinstance(pol, Mapping)
        and _as_str(pol.get("policy_type")) == "POLICY_TYPE_COLUMN_MASK"
        and set(_as_list(pol.get("to_principals"))) - set(_as_list(pol.get("except_principals")))
    ]


def required_mask_columns(
    fgac_policies: Sequence[Mapping[str, Any]],
    tag_assignments: Sequence[Mapping[str, Any]],
) -> set[tuple[str, str]]:
    """Every (table, column) a column mask actually applies to (pure).

    The coverage a verification must prove, whatever principals a check could
    use (derive_spec_from_config drops a mask whose masked tier set comes out
    empty, e.g. "account users" with no concrete groups, so it can't be the
    measure). Resolved as Terraform/Unity Catalog applies the policies, with
    validate_abac's evaluator: policies that target someone once the
    exceptions are removed, scoped to the policy's catalog, the full
    match_condition against each column's tags (hasTagValue, hasTag, AND, OR,
    parentheses) and when_condition against its table's tags.
    """
    from validate_abac import column_mask_matches, condition_is_supported

    effective = effective_mask_policies(fgac_policies)
    # A condition the evaluator can't read (e.g. snake_case has_tag_value())
    # would match nothing and silently drop its mask from the coverage.
    unreadable = sorted(
        f"{_as_str(pol.get('name')) or '<unnamed>'}: {cond!r}"
        for pol in effective
        for cond in (_as_str(pol.get("match_condition")), _as_str(pol.get("when_condition")))
        if not condition_is_supported(cond)
    )
    if unreadable:
        raise ValueError("cannot tell which columns these column masks apply to (only hasTagValue, "
                         "hasTag, AND, OR and parentheses are understood): " + "; ".join(unreadable))
    columns = column_mask_matches({"fgac_policies": list(effective), "tag_assignments": list(tag_assignments)})
    return {
        (table.lower(), column.lower())
        for table, _, column in (name.rpartition(".") for name in columns)
    }


def required_mask_columns_from_tfvars(tfvars_file: Path) -> set[tuple[str, str]]:
    """required_mask_columns of a data_access abac.auto.tfvars."""
    import hcl2

    with open(tfvars_file) as f:
        data = hcl2.load(f)
    return required_mask_columns(data.get("fgac_policies", []) or [], data.get("tag_assignments", []) or [])


def unchecked_mask_columns(
    required: set[tuple[str, str]], checks: Sequence["ColumnMaskCheck"], *, keyed_only: bool = True,
) -> list[str]:
    """Required masked columns no (keyed) check covers, as "table.column"."""
    covered = {(c.table.lower(), c.column.lower()) for c in checks
               if not keyed_only or c.key_column.strip()}
    return sorted(f"{t}.{c}" for t, c in required - covered)


# Fixed-point exclusion (a raw value the mask maps to itself is not compared)
# applies only to a mask proven caller-independent and only within bounds, so a
# near-identity mask can't pass on a handful of changed rows. Any doubt keeps
# the strict rule: a masked value equal to the raw value is a leak.
FIXED_POINT_MIN_COMPARED = 5
FIXED_POINT_MAX_SHARE = 0.10

# Deterministic, caller-independent scalar builtins a fixed-point-eligible mask
# may call. Anything else (a UDF, an unknown function, identity/session/time
# or random functions) keeps the strict rule.
FIXED_POINT_SAFE_CALLS = frozenset({
    "abs", "array_join", "bround", "cast", "ceil", "ceiling", "char_length", "character_length",
    "coalesce", "concat", "concat_ws", "date_format", "date_trunc", "day", "dayofmonth",
    "dayofweek", "dayofyear", "element_at", "floor", "greatest", "hash", "hex", "hour", "if",
    "ifnull", "initcap", "instr", "isnotnull", "isnull", "last_day", "lcase", "least", "left",
    "length", "locate", "lower", "lpad", "ltrim", "make_date", "mask", "md5", "minute", "mod",
    "month", "nullif", "nvl", "nvl2", "overlay", "pmod", "position", "quarter", "regexp_extract",
    "regexp_replace", "repeat", "replace", "reverse", "right", "round", "rpad", "rtrim", "second",
    "sha1", "sha2", "sign", "size", "split", "split_part", "substr", "substring",
    "substring_index", "to_date", "transform", "translate", "trim", "trunc", "try_cast", "ucase",
    "upper", "weekofyear", "xxhash64", "year",
})
# Every token of an eligible mask body must be one of these (or the function's
# single parameter, or an allowlisted builtin in call position). Anything else
# (a backtick-quoted or double-quoted identifier, a dot, an unknown word) makes
# the function ineligible: the strict rule applies.
_FIXED_POINT_KEYWORDS = frozenset({
    "case", "when", "then", "else", "end", "is", "not", "null", "and", "or", "in", "between",
    "like", "rlike", "ilike", "true", "false", "as",
    "string", "int", "integer", "bigint", "smallint", "tinyint", "double", "float", "decimal",
    "date", "timestamp", "timestamp_ntz", "boolean", "binary",
})
_FIXED_POINT_OPERATORS = frozenset("(),+-*/%=<>!|&^~:")
# Only the routine metadata a pure SQL scalar function reports.
_FIXED_POINT_DATA_ACCESS = frozenset({"NO_SQL", "CONTAINS_SQL"})
_FIXED_POINT_DETERMINISTIC = frozenset({"YES", "TRUE"})


def fixed_point_function_problem(
    *, routine_body: Any, external_language: Any, is_deterministic: Any, sql_data_access: Any,
    definition: Any, parameters: Sequence[Any],
) -> str:
    """Why a mask function's fixed points can't be trusted ("" when they can).

    Pure and allowlist-only. The function must be a SQL function declared
    deterministic that reads no data (NO_SQL / CONTAINS_SQL), with exactly
    one parameter, and every token of its body must be a single-quoted string
    or a number literal, an operator, a keyword or type name, that parameter,
    or an allowlisted builtin called by its plain name. A body that could
    depend on the caller, the session or time, or call a UDF, could be the
    identity for the admin but not for a tier, so its "fixed points" could
    hide a leak; anything not positively classified counts as that.
    """
    from sql_tokenizer import SqlTokenizeError, sql_tokens

    if str(routine_body or "").upper() != "SQL" or str(external_language or "").upper() not in ("", "SQL"):
        return f"the mask function is not a SQL function (body {routine_body!r}, language {external_language!r})"
    if str(is_deterministic or "").upper() not in _FIXED_POINT_DETERMINISTIC:
        return f"the mask function is not declared deterministic ({is_deterministic!r})"
    if str(sql_data_access or "").upper() not in _FIXED_POINT_DATA_ACCESS:
        return f"the mask function's data access is {sql_data_access!r}, not NO_SQL or CONTAINS_SQL"
    if len(parameters) != 1:
        return f"the mask function takes {len(parameters)} arguments, not exactly one"
    parameter = str(parameters[0] or "").strip().lower()
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", parameter):
        return f"the mask function's parameter name {parameters[0]!r} is not a plain identifier"
    if not isinstance(definition, str) or not definition.strip():
        return "the mask function's definition could not be read"
    try:
        tokens = sql_tokens(definition)
    except SqlTokenizeError as exc:
        return f"the mask function's definition could not be parsed ({exc})"
    # Anywhere in the body, not only in call position.
    quoted = next((t for t in tokens if t.startswith("`")), None)
    if quoted:
        return f"the mask function uses a quoted identifier ({quoted})"
    if "." in tokens:
        return "the mask function uses a qualified name or a decimal literal"
    for i, token in enumerate(tokens):
        calls = i + 1 < len(tokens) and tokens[i + 1] == "("
        if token.startswith("'") or re.fullmatch(r"[0-9][0-9a-z]*", token) or token in _FIXED_POINT_OPERATORS:
            continue
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", token):
            return f"the mask function's definition has a token the verifier can't classify ({token[:20]})"
        if calls:
            if token in FIXED_POINT_SAFE_CALLS or token in ("and", "or", "not", "in", "when", "then", "else",
                                                            "case", "is", "between"):
                continue
            return f"the mask function calls {token}(), which is not a known caller-independent builtin"
        if token == parameter or token in _FIXED_POINT_KEYWORDS:
            continue
        return f"the mask function refers to {token}, which is not its parameter, a keyword or a literal"
    return ""


def _live_policy_targets(policy: Mapping[str, Any], principals: Iterable[str]) -> bool:
    """Whether a live policy applies to every one of ``principals``."""
    targets = {str(p) for p in policy.get("to_principals") or []}
    excepted = {str(p) for p in policy.get("except_principals") or []}
    return all((ALL_USERS_GROUP in targets or p in targets) and p not in excepted for p in principals)


def live_mask_problem(check: "ColumnMaskCheck", policies: Sequence[Mapping[str, Any]],
                      column_tags: Sequence[tuple[str, str]], table_tags: Sequence[tuple[str, str]],
                      direct_masks: int) -> str:
    """Why the live mask on the column isn't proven to be ``check.mask_function`` ("" if it is).

    Pure. ABAC masks don't show in information_schema.column_masks, so the
    live mask is the one live column-mask policy whose match condition the
    column's live tags satisfy (and when_condition its table's tags). It must
    be the only mask on the column (no directly attached one), call exactly
    the configured function with no extra USING arguments, and apply to every
    masked tier.
    """
    from validate_abac import _condition_matches_tags, condition_is_supported

    if direct_masks:
        return f"{direct_masks} column mask(s) are attached to the column directly"

    def tag_map(rows):
        out: dict[str, set[str]] = {}
        for name, value in rows:
            out.setdefault(name, set()).add(value)
        return out

    col_tags, tbl_tags = tag_map(column_tags), tag_map(table_tags)
    matching = []
    for policy in policies:
        if policy.get("policy_type") != "POLICY_TYPE_COLUMN_MASK":
            continue
        mask = policy.get("column_mask") or {}
        conditions = [policy.get("when_condition") or ""] + [
            m.get("condition") or "" for m in policy.get("match_columns") or []]
        if not all(condition_is_supported(c) for c in conditions):
            return f"live policy {policy.get('name')!r} has a condition the verifier can't evaluate"
        if policy.get("when_condition") and not _condition_matches_tags(policy["when_condition"], tbl_tags):
            continue
        if any(m.get("alias") == mask.get("on_column") and _condition_matches_tags(m.get("condition") or "", col_tags)
               for m in policy.get("match_columns") or []):
            matching.append(policy)
    if len(matching) != 1:
        return f"{len(matching)} live column-mask policies resolve for the column, not exactly one"
    policy = matching[0]
    mask = policy.get("column_mask") or {}
    if str(mask.get("function_name") or "").lower() != check.mask_function.lower():
        return (f"the live policy {policy.get('name')!r} applies {mask.get('function_name')!r}, "
                f"not the configured {check.mask_function!r}")
    if mask.get("using"):
        return f"the live policy {policy.get('name')!r} passes extra USING arguments to the mask"
    if not _live_policy_targets(policy, check.masked_principals):
        return f"the live policy {policy.get('name')!r} does not apply to every masked tier"
    return ""


def _mask_function(policy: Mapping[str, Any]) -> str:
    """The policy's catalog.schema.function, or "" if any part is missing."""
    parts = [_as_str(policy.get(k)) for k in ("function_catalog", "function_schema", "function_name")]
    return ".".join(parts) if all(parts) else ""


def most_privileged_tier(memberships: Iterable[str], access_tier_groups: Sequence[str]) -> str:
    """raw/partial/full for memberships; the earliest configured group wins."""
    member_set = set(memberships)
    matched = next((index for index, group in enumerate(access_tier_groups) if group in member_set), None)
    if matched == 0:
        return "raw"
    if matched is not None and matched < len(access_tier_groups) - 1:
        return "partial"
    return "full"


def derive_spec_from_config(
    fgac_policies: Sequence[Mapping[str, Any]],
    tag_assignments: Sequence[Mapping[str, Any]],
    all_groups: Iterable[str],
    *,
    key_column: str = "",
    key_column_by_table: Optional[Mapping[str, str]] = None,
    default_admin_tier: str = DEFAULT_ADMIN_TIER,
) -> VerificationSpec:
    """Build a :class:`VerificationSpec` from parsed ABAC config (pure).

    * ``POLICY_TYPE_COLUMN_MASK`` → for each column matched by the policy's
      ``match_condition``, the ``to_principals`` are the *masked* tiers; the
      *unmasked* tiers are the remaining groups (plus any ``except_principals``,
      and ``default_admin_tier`` as a guaranteed raw-value baseline).
    * ``POLICY_TYPE_ROW_FILTER`` → for each table matched by ``when_condition``,
      the ``to_principals`` are the *restricted* tiers; the rest are unrestricted.

    ``key_column`` (or per-table ``key_column_by_table``) names the primary-key
    column used to pair rows across principals.
    """
    # The all-users pseudo-group is never a provisionable tier; drop it from the
    # concrete group set (it is handled specially per-policy below).
    all_groups = [g for g in dict.fromkeys(all_groups) if g != ALL_USERS_GROUP]
    key_by_table = {t.lower(): k for t, k in (key_column_by_table or {}).items() if k}
    spec = VerificationSpec()
    seen_masks: set[tuple[str, str]] = set()
    seen_filters: set[str] = set()

    for pol in fgac_policies:
        ptype = _as_str(pol.get("policy_type"))
        name = _as_str(pol.get("name"))
        to_principals = tuple(_as_list(pol.get("to_principals")))
        except_principals = tuple(_as_list(pol.get("except_principals")))

        if ptype == "POLICY_TYPE_COLUMN_MASK":
            cols = resolve_columns_for_condition(
                _as_str(pol.get("match_condition")), tag_assignments, entity_type="columns"
            )
            if ALL_USERS_GROUP in to_principals:
                # Mask applies to everyone except the exceptions. The admin
                # baseline is also "everyone", so it is NOT a raw baseline here;
                # only the excepted principals see the raw value.
                masked = tuple(g for g in all_groups if g not in except_principals)
                unmasked = list(except_principals)
            else:
                masked = to_principals
                # Unmasked = the other concrete groups, the explicit exceptions,
                # and the admin baseline (admin is not a member of the masked
                # group, so it sees the raw value).
                unmasked = [g for g in all_groups if g not in masked]
                for e in except_principals:
                    if e not in unmasked and e not in masked:
                        unmasked.append(e)
                if default_admin_tier and default_admin_tier not in unmasked:
                    unmasked.append(default_admin_tier)
            for c in cols:
                sig = (c["table"], c["column"])
                if sig in seen_masks or not masked:
                    continue
                seen_masks.add(sig)
                kc = key_by_table.get(c["table"].lower(), key_column)
                spec.column_masks.append(
                    ColumnMaskCheck(
                        table=c["table"],
                        column=c["column"],
                        key_column=kc,
                        masked_principals=masked,
                        unmasked_principals=tuple(unmasked),
                        policy_name=name,
                        mask_function=_mask_function(pol),
                    )
                )

        elif ptype == "POLICY_TYPE_ROW_FILTER":
            tables = resolve_columns_for_condition(
                _as_str(pol.get("when_condition")), tag_assignments, entity_type="tables"
            )
            if ALL_USERS_GROUP in to_principals:
                restricted = tuple(g for g in all_groups if g not in except_principals)
                unrestricted = list(except_principals)
            else:
                restricted = to_principals
                unrestricted = [g for g in all_groups if g not in restricted]
                for e in except_principals:
                    if e not in unrestricted and e not in restricted:
                        unrestricted.append(e)
                if default_admin_tier and default_admin_tier not in unrestricted:
                    unrestricted.append(default_admin_tier)
            for t in tables:
                if t["table"] in seen_filters or not restricted:
                    continue
                seen_filters.add(t["table"])
                spec.row_filters.append(
                    RowFilterCheck(
                        table=t["table"],
                        restricted_principals=restricted,
                        unrestricted_principals=tuple(unrestricted),
                        policy_name=name,
                    )
                )

    return spec


# ---------------------------------------------------------------------------
# Comparison logic (pure)
# ---------------------------------------------------------------------------
# Observation shapes produced by the live layer (or mocked in tests):
#
#   column_values: {(table, column): {principal: [(row_key, value), ...]}}
#                  (a {row_key: value} mapping is accepted too)
#   row_counts:    {table: {principal: int}}


def _normalize_value(v: Any) -> Any:
    """Values come back from SQL as strings; normalize for equality comparison."""
    if v is None:
        return None
    return str(v).strip()


def _normalize_exact_value(v: Any) -> Any:
    """Preserve string bytes for deterministic exact-output comparisons."""
    if v is None:
        return None
    return str(v)


def _errors_for(
    principals: Iterable[str], errors_by_principal: Optional[Mapping[str, str]],
) -> dict[str, str]:
    """Return the query errors recorded for any of ``principals``."""
    if not errors_by_principal:
        return {}
    return {p: errors_by_principal[p] for p in principals if p in errors_by_principal}


def _pairing_key_problem(key_column: str, table: str, problem: str) -> str:
    return (f"row-pairing key {key_column} {problem} on {table}; "
            "choose a unique, non-null, unmasked key")


def _key_problem_message(check: ColumnMaskCheck, problem: str) -> str:
    return _pairing_key_problem(check.key_column, check.table, problem)


def sampled_keys_problem(key_column: str, table: str, principal: str, keys: Sequence[Any]) -> str:
    """Why sampled keys cannot pair rows ("" if they can): a NULL or a repeat."""
    if any(k is None for k in keys):
        return _pairing_key_problem(
            key_column, table, f"has NULLs ({sum(k is None for k in keys)} of {len(keys)} sampled rows for {principal})")
    if len(set(keys)) != len(keys):
        return _pairing_key_problem(
            key_column, table, f"is not unique ({len(set(keys))} distinct keys in {len(keys)} sampled rows for {principal})")
    return ""


def sample_key_problem(check: ColumnMaskCheck, principal: str, rows: Any) -> str:
    """Why one principal's fetched rows cannot be paired by key ("" if they can).

    Rows are paired across tiers by ``check.key_column``; a NULL or repeated key
    in the sample means two tiers can pair *different* rows under the same key,
    so a mask that is not applied can look applied. Values are never included.
    """
    return sampled_keys_problem(check.key_column, check.table, principal, [k for k, _ in _row_pairs(rows)])


def key_masked_message(check: ColumnMaskCheck, principal: str) -> str:
    return (f"row-pairing key {check.key_column} is masked for {principal} on {check.table}; "
            "choose a unique, non-null, unmasked key")


def _row_pairs(rows: Any) -> list[tuple[Any, Any]]:
    """Fetched rows as (key, value) pairs; a mapping is {key: value}."""
    if not rows:
        return []
    if isinstance(rows, Mapping):
        return list(rows.items())
    return [(r[0], r[1]) for r in rows]


def evaluate_column_mask_check(
    check: ColumnMaskCheck,
    values_by_principal: Mapping[str, Any],
    errors_by_principal: Optional[Mapping[str, str]] = None,
    *,
    pairing_problem: str = "",
    unpaired: Optional[Mapping[str, str]] = None,
    fixed_point_keys: Optional[Iterable[Any]] = None,
) -> CheckResult:
    """Prove a mask takes effect: masked tiers get a masked value, unmasked tiers
    get the *raw* value.

    ``fixed_point_keys`` are rows whose raw value the mask function maps to
    itself (e.g. a date already on 1 January under a year mask), as the admin
    baseline evaluated it: such a row shows the same value masked or not, so
    it can't demonstrate masking and is left out like a NULL raw value. With no
    other row to compare, a tier is INCONCLUSIVE, never PASS.

    ``values_by_principal`` maps ``principal -> rows`` for this (table, column),
    where rows are the fetched ``(row_key, value)`` pairs (or a
    ``{row_key: value}`` mapping); ``errors_by_principal`` maps
    ``principal -> error string`` for principals whose query failed.
    ``pairing_problem`` is a reason the live layer could not prove the key pairs
    rows (e.g. the whole-table uniqueness proof failed), and ``unpaired`` maps
    a principal whose rows cannot be paired with the baseline (it sees the key
    column masked, or none of the sampled rows) to the reason.

    No value or key is ever written into the detail or evidence — FAIL details
    are printed and land in CI logs — only principals and counts.

    A PASS is only returned when ALL of the following hold — otherwise the check
    FAILs (proven violation) or is INCONCLUSIVE (could not be verified). Neither
    passes the gate:

    * No involved principal's query failed.
    * The key pairs rows: no pairing problem was found, and no principal's
      sample has a NULL or repeated key (else two tiers could compare different
      rows under one key) → INCONCLUSIVE.
    * No involved principal is unpaired (sees the key masked, or none of the
      sampled rows) → INCONCLUSIVE; the other tiers are still compared, so a
      proven leak still FAILs.
    * At least one unmasked principal returned rows, and all unmasked principals
      that returned rows **agree** on each shared row's value — that agreed value
      is the raw ground truth. Disagreement means one of them is not actually
      unmasked → FAIL.
    * At least one shared row has a **non-null / non-empty** raw value — otherwise
      the dataset cannot demonstrate masking → INCONCLUSIVE.
    * Every row a masked principal returned is also seen by an unmasked
      principal (else it has no raw value to compare) → INCONCLUSIVE.
    * Every masked principal that returned rows shares at least one maskable row
      with the raw baseline, and its value there **differs** from the raw value.
      Equality is a leak → FAIL. No overlap → INCONCLUSIVE.
    """
    target = check.describe()
    involved = set(check.masked_principals) | set(check.unmasked_principals)

    # (issue 3) Any query failure on an involved principal is a hard failure.
    errs = _errors_for(involved, errors_by_principal)
    if errs:
        return CheckResult(
            "column-mask", target, FAIL,
            f"query failed for principal(s) — cannot verify masking: {errs}",
            {"errors": errs},
        )

    if pairing_problem:
        return CheckResult("column-mask", target, INCONCLUSIVE, pairing_problem,
                           {"key_column": check.key_column})
    # An unpaired tier is left out; compare the rest (so a leak there still
    # FAILs) and report its reason below instead of a vague no-overlap.
    unpaired = {p: why for p, why in sorted((unpaired or {}).items()) if p in involved}
    rows_by_principal = {
        p: _row_pairs(values_by_principal.get(p))
        for p in involved if p not in unpaired
    }
    for p in sorted(rows_by_principal):
        problem = sample_key_problem(check, p, rows_by_principal[p])
        if problem:
            return CheckResult("column-mask", target, INCONCLUSIVE, problem,
                               {"key_column": check.key_column})
    values_by_principal = {p: dict(rows) for p, rows in rows_by_principal.items()}

    unmasked_present = [p for p in check.unmasked_principals if values_by_principal.get(p)]
    masked_present = [p for p in check.masked_principals if values_by_principal.get(p)]
    if unpaired and (not unmasked_present or not masked_present):
        return CheckResult("column-mask", target, INCONCLUSIVE,
                           "; ".join(unpaired.values()), {"unpaired": sorted(unpaired)})

    if not unmasked_present:
        return CheckResult(
            "column-mask", target, INCONCLUSIVE,
            "no unmasked/baseline principal returned rows — cannot establish the "
            "raw value to compare against",
            {"masked_principals": list(check.masked_principals)},
        )
    if not masked_present:
        return CheckResult(
            "column-mask", target, INCONCLUSIVE,
            "no masked principal returned rows — nothing to check for masking",
            {"unmasked_principals": unmasked_present},
        )

    # Raw ground truth per row = the value the unmasked principals agree on.
    # (issue 2) Disagreement means a supposedly-unmasked tier is actually masked
    # differently, so we cannot trust any of them as raw → FAIL.
    raw_by_row: dict[Any, Any] = {}
    conflicts: dict[str, int] = {}
    for up in unmasked_present:
        for row_key, val in values_by_principal[up].items():
            nval = _normalize_value(val)
            if row_key not in raw_by_row:
                raw_by_row[row_key] = nval
            elif raw_by_row[row_key] != nval:
                conflicts[up] = conflicts.get(up, 0) + 1
    if conflicts:
        return CheckResult(
            "column-mask", target, FAIL,
            ("unmasked principals disagree on the raw value — at least one is not "
             f"actually unmasked, so masking cannot be trusted. Disagreeing rows "
             f"(by key {check.key_column}) per principal: {conflicts}"),
            {"conflicts_by_principal": conflicts},
        )

    # (issue 2) The raw baseline must contain at least one maskable value: not
    # NULL/empty, and not a value the mask leaves unchanged.
    fixed_points = set(fixed_point_keys or ()) & set(raw_by_row)
    maskable_rows = {k for k, v in raw_by_row.items() if v not in (None, "") and k not in fixed_points}
    if not maskable_rows:
        return CheckResult(
            "column-mask", target, INCONCLUSIVE,
            ("every raw value in the sample is NULL/empty or one the mask leaves "
             "unchanged — the dataset cannot demonstrate that masking changes anything"),
            {"raw_rows": len(raw_by_row), "fixed_point_rows": len(fixed_points)},
        )

    leaks: dict[str, int] = {}
    masked_ok = 0
    per_principal_compared: dict[str, int] = {}
    per_principal_fixed: dict[str, int] = {}
    for mp in masked_present:
        compared_here = 0
        per_principal_fixed[mp] = sum(1 for k in values_by_principal[mp] if k in fixed_points)
        for row_key, val in values_by_principal[mp].items():
            if row_key not in maskable_rows:
                continue
            compared_here += 1
            if _normalize_value(val) == raw_by_row[row_key]:
                leaks[mp] = leaks.get(mp, 0) + 1
            else:
                masked_ok += 1
        per_principal_compared[mp] = compared_here

    if leaks:
        leaked = sum(leaks.values())
        return CheckResult(
            "column-mask", target, FAIL,
            (f"{leaked} row(s) leaked the raw value to a masked principal "
             f"(mask not effective). Leaked rows (by key {check.key_column}) per "
             f"principal: {leaks}"),
            {"leaked_rows": leaked, "leaks_by_principal": leaks, "masked_ok": masked_ok},
        )

    if unpaired:
        return CheckResult("column-mask", target, INCONCLUSIVE,
                           "; ".join(unpaired.values()),
                           {"unpaired": sorted(unpaired),
                            "per_principal_compared": per_principal_compared})

    # A masked tier's row no unmasked principal sees has no raw value to
    # compare with, so it proves nothing — and could be an unmasked leak.
    unbaselined = {mp: n for mp in masked_present
                   if (n := sum(1 for k in values_by_principal[mp] if k not in raw_by_row))}
    if unbaselined:
        return CheckResult(
            "column-mask", target, INCONCLUSIVE,
            (f"masked principal(s) see row(s) no unmasked principal sees, so masking "
             f"could not be verified for them (rows by key {check.key_column}): {unbaselined}"),
            {"unbaselined_by_principal": unbaselined,
             "per_principal_compared": per_principal_compared},
        )

    # Excluding fixed points must not let a near-identity mask pass: a tier
    # that had any needs FIXED_POINT_MIN_COMPARED compared rows, and fixed
    # points may be at most FIXED_POINT_MAX_SHARE of its sampled rows.
    unbounded = {
        mp: f"{per_principal_compared[mp]} compared, {fixed} unchanged of {len(values_by_principal[mp])} sampled"
        for mp, fixed in per_principal_fixed.items()
        if fixed and (per_principal_compared[mp] < FIXED_POINT_MIN_COMPARED
                      or fixed > FIXED_POINT_MAX_SHARE * len(values_by_principal[mp]))
    }
    if unbounded:
        return CheckResult(
            "column-mask", target, INCONCLUSIVE,
            (f"too many sampled rows have a value the mask leaves unchanged to prove masking "
             f"(needs at least {FIXED_POINT_MIN_COMPARED} compared rows and at most "
             f"{FIXED_POINT_MAX_SHARE:.0%} unchanged per tier): {unbounded}"),
            {"per_principal_compared": per_principal_compared,
             "fixed_point_rows_by_principal": per_principal_fixed},
        )

    # (issue 2) Every masked principal must have actually been compared on a
    # maskable row — otherwise we proved nothing for it.
    uncompared = [p for p, n in per_principal_compared.items() if n == 0]
    if uncompared:
        return CheckResult(
            "column-mask", target, INCONCLUSIVE,
            (f"masked principal(s) {uncompared} shared no maskable row with the raw "
             "baseline — masking could not be verified for them"),
            {"per_principal_compared": per_principal_compared},
        )

    skipped = (f"; {len(fixed_points)} row(s) whose raw value the mask leaves unchanged "
               "were not compared") if fixed_points else ""
    return CheckResult(
        "column-mask", target, PASS,
        (f"masked principal(s) {masked_present} see a masked value that differs "
         f"from the raw value seen by {unmasked_present} across {masked_ok} row(s){skipped}"),
        {"masked_ok": masked_ok, "per_principal_compared": per_principal_compared,
         "fixed_point_rows": len(fixed_points)},
    )


def evaluate_tiered_column_mask_check(
    check: ColumnMaskCheck,
    values_by_principal: Mapping[str, Any],
    expected_by_tier: Mapping[str, Any],
    errors_by_principal: Optional[Mapping[str, str]] = None,
    *,
    pairing_problem: str = "",
    principal_memberships: Optional[Mapping[str, Sequence[str]]] = None,
) -> CheckResult:
    """Compare every deterministic tier with its exact admin-computed output.

    ``expected_by_tier`` contains paired ``(key, value)`` rows for raw,
    partial and full.  No values are included in evidence or diagnostics.
    """
    target = check.describe()
    tiers = dict(check.expected_tiers)
    rank = {"raw": 0, "partial": 1, "full": 2}
    for principal, memberships in (principal_memberships or {}).items():
        if principal not in tiers:
            continue
        membership_tiers = [tiers.get(group, "full") for group in memberships]
        resolved = min(membership_tiers, key=rank.__getitem__) if membership_tiers else "full"
        if tiers[principal] != resolved:
            return CheckResult(
                "column-mask", target, INCONCLUSIVE,
                f"principal {principal!r} is declared {tiers[principal]} but its memberships resolve to {resolved}",
                {"principal": principal, "declared_tier": tiers[principal], "resolved_tier": resolved},
            )
    involved = set(tiers)
    errors = _errors_for(involved, errors_by_principal)
    moving = set(check.moving_principals)
    accepted = {
        p for p, detail in errors.items()
        if p in moving
        and "more than one mask" in detail.lower()
        and not any(marker in detail.lower() for marker in (
            "permission_denied", "permission denied", "insufficient_permissions"))
    }
    refused = {p: detail for p, detail in errors.items() if p not in accepted}
    if refused:
        return CheckResult("column-mask", target, FAIL,
                           f"query failed for principal(s) — cannot verify masking: {refused}",
                           {"errors": refused})
    if pairing_problem:
        return CheckResult("column-mask", target, INCONCLUSIVE, pairing_problem,
                           {"key_column": check.key_column})
    expected_rows = {tier: _row_pairs(expected_by_tier.get(tier))
                     for tier in ("raw", "partial", "full")}
    for tier, rows in expected_rows.items():
        problem = sample_key_problem(check, f"expected-{tier}", rows)
        if problem:
            return CheckResult("column-mask", target, INCONCLUSIVE, problem,
                               {"key_column": check.key_column})
    expected = {tier: dict(rows) for tier, rows in expected_rows.items()}
    used_tiers = set(tiers.values())
    common = set.intersection(*(set(expected[tier]) for tier in used_tiers)) if used_tiers else set()
    # Only tier pairs whose expected results differ need separate proof.  Some
    # reviewed treatments intentionally use the same function for partial and
    # full access; requiring three distinct values would make those treatments
    # impossible to verify.  Raw versus every used masked tier must still be
    # distinguishable on at least one paired row.
    required_pairs: list[tuple[str, str]] = []
    for left in sorted(used_tiers):
        for right in sorted(used_tiers):
            if left >= right:
                continue
            differs = any(
                _normalize_exact_value(expected[left][key])
                != _normalize_exact_value(expected[right][key])
                for key in common
            )
            if differs:
                required_pairs.append((left, right))
            elif ({left, right} == {"partial", "full"}
                  and check.partial_function.lower() != check.full_function.lower()):
                return CheckResult(
                    "column-mask", target, INCONCLUSIVE,
                    ("partial and full use different expected functions but their outputs "
                     "cannot be distinguished on the sample"),
                    {"sampled_rows": len(common)},
                )
    masked_tiers = used_tiers - {"raw"}
    raw_pairs_proven = all(
        any(_normalize_exact_value(expected["raw"][key])
            != _normalize_exact_value(expected[tier][key]) for key in common)
        for tier in masked_tiers
    ) if "raw" in used_tiers else True
    if not common or not raw_pairs_proven:
        return CheckResult(
            "column-mask", target, INCONCLUSIVE,
            f"{', '.join(sorted(used_tiers))} outputs cannot be distinguished on the sample",
            {"sampled_rows": len(common)},
        )
    mismatches: dict[str, int] = {}
    raw_leaks: dict[str, int] = {}
    compared: dict[str, int] = {}
    missing: list[str] = []
    fixed_by_principal: dict[str, int] = {}
    raw_keys = set(expected["raw"])
    for principal, tier in tiers.items():
        if principal in accepted:
            continue
        actual_rows = _row_pairs(values_by_principal.get(principal))
        problem = sample_key_problem(check, principal, actual_rows)
        if problem:
            return CheckResult("column-mask", target, INCONCLUSIVE, problem,
                               {"key_column": check.key_column})
        actual = dict(actual_rows)
        wanted = expected.get(tier, {})
        actual_keys = set(actual)
        own_distinguishing_key = all(
            any(key in actual_keys
                and _normalize_exact_value(expected[left][key])
                != _normalize_exact_value(expected[right][key]) for key in common)
            for left, right in required_pairs
        )
        if (not actual or any(key not in wanted for key in actual)
                or (actual_keys != raw_keys and not own_distinguishing_key)):
            missing.append(principal)
            continue
        compared[principal] = 0
        fixed_by_principal[principal] = 0
        for key, value in actual.items():
            normalized = _normalize_exact_value(value)
            wanted_value = _normalize_exact_value(wanted[key])
            raw_value = _normalize_exact_value(expected["raw"].get(key))
            if normalized != wanted_value:
                mismatches[principal] = mismatches.get(principal, 0) + 1
            # NULL and caller-independent fixed points cannot demonstrate that
            # a mask ran. They are neither leaks nor proof rows.
            proof_row = tier != "raw" and raw_value not in (None, "") and wanted_value != raw_value
            if proof_row:
                compared[principal] += 1
            elif tier != "raw" and raw_value not in (None, ""):
                fixed_by_principal[principal] += 1
            if proof_row and normalized == raw_value:
                raw_leaks[principal] = raw_leaks.get(principal, 0) + 1
    if raw_leaks:
        return CheckResult("column-mask", target, FAIL,
                           f"raw value observed for masked principal(s): {raw_leaks}",
                           {"raw_leaks_by_principal": raw_leaks})
    if mismatches:
        return CheckResult("column-mask", target, FAIL,
                           f"principal tier output did not exactly match the expected function: {mismatches}",
                           {"mismatches_by_principal": mismatches})
    if missing:
        return CheckResult("column-mask", target, INCONCLUSIVE,
                           f"principal(s) had no fully paired sample: {missing}", {"missing": missing})
    masked_success = [principal for principal, tier in tiers.items()
                      if tier != "raw" and principal not in accepted and compared.get(principal, 0)]
    if any(tier != "raw" for tier in tiers.values()) and not masked_success:
        return CheckResult(
            "column-mask", target, INCONCLUSIVE,
            "no masked principal returned a distinguishing row; masking was not proven",
            {"accepted_fail_closed": sorted(accepted), "per_principal_compared": compared},
        )
    under_proven = {
        principal: compared.get(principal, 0)
        for principal, tier in tiers.items()
        if tier != "raw" and principal not in accepted
        and compared.get(principal, 0) < 1
    }
    if under_proven:
        return CheckResult(
            "column-mask", target, INCONCLUSIVE,
            f"no distinguishing proof row for masked principal(s): {under_proven}",
            {"per_principal_compared": compared},
        )
    unbounded = {
        principal: f"{compared.get(principal, 0)} compared, {fixed} unchanged of {len(_row_pairs(values_by_principal.get(principal)))} sampled"
        for principal, fixed in fixed_by_principal.items()
        if fixed and (compared.get(principal, 0) < FIXED_POINT_MIN_COMPARED
                      or fixed > FIXED_POINT_MAX_SHARE * len(_row_pairs(values_by_principal.get(principal))))
    }
    if unbounded:
        return CheckResult(
            "column-mask", target, INCONCLUSIVE,
            (f"too many sampled rows have a value the expected mask leaves unchanged "
             f"(needs at least {FIXED_POINT_MIN_COMPARED} compared rows and at most "
             f"{FIXED_POINT_MAX_SHARE:.0%} unchanged per tier): {unbounded}"),
            {"per_principal_compared": compared, "fixed_point_rows_by_principal": fixed_by_principal},
        )
    return CheckResult("column-mask", target, PASS,
                       f"all deterministic tiers matched their exact expected output across {sum(compared.values())} row(s)",
                       {"per_principal_compared": compared, "accepted_fail_closed": sorted(accepted)})


def evaluate_row_filter_check(
    check: RowFilterCheck,
    counts_by_principal: Mapping[str, Optional[int]],
    errors_by_principal: Optional[Mapping[str, str]] = None,
) -> CheckResult:
    """Prove a row filter takes effect: restricted tiers see *fewer* rows.

    A count of ``None`` means "not collected" and is inconclusive; a recorded
    query error is a hard failure. A PASS requires a positive unrestricted
    baseline, a collected count for every restricted principal, and every
    restricted count strictly below the baseline.
    """
    target = check.describe()
    involved = set(check.restricted_principals) | set(check.unrestricted_principals)

    # (issue 3) Query failures are hard failures.
    errs = _errors_for(involved, errors_by_principal)
    if errs:
        return CheckResult(
            "row-filter", target, FAIL,
            f"query failed for principal(s) — cannot verify row filter: {errs}",
            {"errors": errs},
        )

    unrestricted = {
        p: counts_by_principal[p]
        for p in check.unrestricted_principals
        if counts_by_principal.get(p) is not None
    }
    restricted = {
        p: counts_by_principal[p]
        for p in check.restricted_principals
        if counts_by_principal.get(p) is not None
    }

    if not unrestricted:
        return CheckResult(
            "row-filter", target, INCONCLUSIVE,
            "no unrestricted/baseline principal row count available",
            {},
        )
    if not restricted:
        return CheckResult(
            "row-filter", target, INCONCLUSIVE,
            "no restricted principal row count available",
            {},
        )

    # A restricted principal we were asked to check but got no count for leaves
    # a gap we cannot pass over.
    missing_restricted = [
        p for p in check.restricted_principals if counts_by_principal.get(p) is None
    ]
    if missing_restricted:
        return CheckResult(
            "row-filter", target, INCONCLUSIVE,
            f"no row count for restricted principal(s) {missing_restricted} — "
            "cannot verify the filter for them",
            {"restricted": restricted},
        )

    baseline = max(unrestricted.values())
    if baseline <= 0:
        return CheckResult(
            "row-filter", target, INCONCLUSIVE,
            f"unrestricted baseline saw {baseline} rows — cannot demonstrate restriction",
            {"unrestricted": unrestricted},
        )

    violations = {p: n for p, n in restricted.items() if n >= baseline}
    if violations:
        return CheckResult(
            "row-filter", target, FAIL,
            (f"restricted principal(s) saw >= the unrestricted baseline "
             f"({baseline} rows); filter not effective: {violations}"),
            {"restricted": restricted, "unrestricted": unrestricted},
        )
    return CheckResult(
        "row-filter", target, PASS,
        (f"restricted principal(s) {restricted} see fewer rows than the "
         f"unrestricted baseline ({baseline})"),
        {"restricted": restricted, "unrestricted": unrestricted},
    )


def evaluate_effective_access(
    spec: VerificationSpec,
    column_values: Mapping[tuple, Mapping[str, Mapping[Any, Any]]],
    row_counts: Mapping[str, Mapping[str, Optional[int]]],
    column_errors: Optional[Mapping[tuple, Mapping[str, str]]] = None,
    row_errors: Optional[Mapping[str, Mapping[str, str]]] = None,
    *,
    pairing_problems: Optional[Mapping[tuple, str]] = None,
    unpaired: Optional[Mapping[tuple, Mapping[str, str]]] = None,
    fixed_points: Optional[Mapping[tuple, Iterable[Any]]] = None,
    expected_values: Optional[Mapping[tuple, Mapping[str, Any]]] = None,
) -> EffectiveAccessReport:
    """Evaluate every check in the spec against collected observations (pure)."""
    report = EffectiveAccessReport()
    for check in spec.column_masks:
        sig = (check.table, check.column)
        vals = column_values.get(sig, {})
        errs = (column_errors or {}).get(sig, {})
        if check.expected_tiers:
            report.add(evaluate_tiered_column_mask_check(
                check, vals, (expected_values or {}).get(sig, {}), errs,
                pairing_problem=(pairing_problems or {}).get(sig, ""),
                principal_memberships=spec.principal_memberships))
        else:
            report.add(evaluate_column_mask_check(
                check, vals, errs,
                pairing_problem=(pairing_problems or {}).get(sig, ""),
                unpaired=(unpaired or {}).get(sig),
                fixed_point_keys=(fixed_points or {}).get(sig),
            ))
    for check in spec.row_filters:
        counts = row_counts.get(check.table, {})
        errs = (row_errors or {}).get(check.table, {})
        report.add(evaluate_row_filter_check(check, counts, errs))
    return report


# ---------------------------------------------------------------------------
# Row-pairing key selection (pure ranking; the live layer proves each pick)
# ---------------------------------------------------------------------------
# Each masked table gets its own key, in this order: (0) an explicit per-table
# verify_key_columns entry (or a VERIFY_SPEC key_column), else the global
# VERIFY_KEY_COLUMN / verify_key_column when the table has that column; (1) a
# single-column PRIMARY KEY; (2) an id-like column. Auto-picked candidates must
# be untagged, unmasked and of a type that pairs exactly; the first one the
# admin proves unique and non-null is used. An explicit key that fails its
# proof is reported, never silently replaced.
_PREFERRED_KEY_TYPES = {"STRING", "VARCHAR", "CHAR", "INT", "INTEGER", "LONG", "BIGINT"}
_OTHER_KEY_TYPES = {"SHORT", "SMALLINT", "BYTE", "TINYINT"}
SOURCE_TABLE_KEY = "verify_key_columns"
SOURCE_GLOBAL_KEY = "VERIFY_KEY_COLUMN"
SOURCE_PRIMARY_KEY = "primary key"
SOURCE_ID_LIKE = "id-like column"
EXPLICIT_SOURCES = (SOURCE_TABLE_KEY, SOURCE_GLOBAL_KEY)


@dataclass(frozen=True)
class TableKeyFacts:
    """Column metadata the admin reads for a table (never row values)."""
    columns: tuple[tuple[str, str], ...]   # (name, data type), table order
    primary_key: tuple[str, ...] = ()
    column_tags: tuple[tuple[str, str, str], ...] = ()   # live (column, tag name, tag value)


@dataclass
class KeyPick:
    """The row-pairing key chosen for one table, or why there is none."""
    table: str
    key: str = ""
    source: str = ""
    problem: str = ""
    warning: str = ""   # an override that looks sensitive (still used: the user's call)

    @property
    def explicit(self) -> bool:
        return self.source in EXPLICIT_SOURCES


def no_key_message(table: str) -> str:
    return (f'no provable row-pairing key for {table}; set verify_key_columns["{table}"] '
            "or VERIFY_KEY_COLUMN")


def _type_rank(data_type: str) -> Optional[int]:
    """0 = preferred key type, 1 = acceptable, None = cannot pair exactly."""
    base = re.split(r"[(<\s]", (data_type or "").strip().upper(), maxsplit=1)[0]
    if base in _PREFERRED_KEY_TYPES:
        return 0
    if base in _OTHER_KEY_TYPES:
        return 1
    return None


def _singular(name: str) -> str:
    n = name.lower()
    if n.endswith("ies") and len(n) > 3:
        return n[:-3] + "y"
    if n.endswith(("sses", "xes", "ches", "shes")):
        return n[:-2]
    if n.endswith("s") and not n.endswith("ss"):
        return n[:-1]
    return n


def sensitive_key_reason(column: str, tags: Iterable[tuple[str, str]] = ()) -> str:
    """Why ``column`` looks sensitive ("" if it doesn't): never auto-picked as a key.

    The coverage check's sensitive-looking-name rule (validate_abac's
    categories, minus the ones first exposure doesn't block on), or a
    ``class.*`` tag of a sensitive category (sensitivity_source); a class tag
    that only marks an identifier type is fine.
    """
    from sensitivity_source import sensitive_class_semantic
    from validate_abac import FIRST_EXPOSURE_NONBLOCKING_CATEGORIES, _infer_column_categories

    categories = sorted(_infer_column_categories(column) - FIRST_EXPOSURE_NONBLOCKING_CATEGORIES)
    if categories:
        return f"its name looks sensitive ({', '.join(categories)})"
    semantics = sorted({sem for name, value in tags if (sem := sensitive_class_semantic(name, value))})
    if semantics:
        return f"it is classified sensitive (class.{', class.'.join(semantics)})"
    return ""


def key_candidates(table: str, facts: TableKeyFacts, unsafe: Iterable[str] = (),
                   tags_by_column: Optional[Mapping[str, Sequence[tuple[str, str]]]] = None,
                   ) -> list[tuple[str, str]]:
    """Auto-pick candidates for ``table``, best first, as (column, source) (pure).

    ``unsafe`` names the columns a column mask applies to; a column that looks
    sensitive (sensitive_key_reason, over its live and configured tags in
    ``tags_by_column``) is never a candidate. Other tags are judged by the key
    checks every candidate then goes through (key_mask_metadata).
    """
    excluded = {c.lower() for c in unsafe}
    tags_by_column = {c.lower(): t for c, t in (tags_by_column or {}).items()}
    types = {name.lower(): data_type for name, data_type in facts.columns}
    names = {name.lower(): name for name, _ in facts.columns}

    def usable(column: str) -> bool:
        # Names come from live metadata and go into admin SQL: plain identifiers only.
        return (bool(_IDENT_RE.fullmatch(column)) and column.lower() not in excluded
                and _type_rank(types.get(column.lower(), "")) is not None
                and not sensitive_key_reason(column, tags_by_column.get(column.lower(), ())))

    out: list[tuple[str, str]] = []
    if (len(facts.primary_key) == 1 and facts.primary_key[0].lower() in names
            and usable(facts.primary_key[0])):
        out.append((names[facts.primary_key[0].lower()], SOURCE_PRIMARY_KEY))
    short = table.rsplit(".", 1)[-1].strip("`").lower()
    preferred = {f"{_singular(short)}_id", f"{short}_id"}

    def rank(column: str) -> tuple:
        lower = column.lower()
        tier = 0 if lower in preferred else 1 if lower == "id" else 2
        return (tier, _type_rank(types[lower]), lower)

    id_like = sorted((name for lower, name in names.items()
                      if (lower == "id" or lower.endswith("_id")) and usable(name)), key=rank)
    out.extend((name, SOURCE_ID_LIKE) for name in id_like if all(name != c for c, _ in out))
    return out


def unsafe_key_columns(checks: Sequence[ColumnMaskCheck],
                       mask_config: Optional[Mapping[str, Any]] = None) -> dict[str, set[str]]:
    """Per lower-case table: the columns a column mask applies to, which no key may use.

    The checked columns, plus every column the config's masks match
    (required_mask_columns, the coverage check's matcher). An unreadable
    condition adds nothing here; key_mask_metadata then refuses any tagged key.
    """
    out: dict[str, set[str]] = {}
    for c in checks:
        out.setdefault(c.table.lower(), set()).add(c.column.lower())
    if mask_config is not None:
        try:
            masked = required_mask_columns(mask_config.get("fgac_policies") or [],
                                           mask_config.get("tag_assignments") or [])
        except ValueError:
            masked = set()
        for table, column in masked:
            if table in out:
                out[table].add(column)
    return out


def pick_table_key(
    table: str,
    *,
    explicit: str = "",
    global_key: str = "",
    facts: Optional[TableKeyFacts] = None,
    facts_error: str = "",
    unsafe: Iterable[str] = (),
    prove: Any,
    prove_explicit: bool = True,
    tags_by_column: Optional[Mapping[str, Sequence[tuple[str, str]]]] = None,
) -> KeyPick:
    """Choose and prove ``table``'s row-pairing key.

    ``prove(column)`` runs the admin proof (in-sample unique and non-null, then
    the whole-table count) and returns "" or the reason it failed. Without
    column metadata (``facts_error``) only an explicit key can be tried; the
    global key then applies as it always did. With ``prove_explicit=False`` an
    explicit key is taken unproven: the live run's per-check proof covers it
    and, failing, reports it (an explicit key never falls through).
    """
    unsafe = {c.lower() for c in unsafe}
    tags_by_column = {c.lower(): t for c, t in (tags_by_column or {}).items()}

    def explicit_pick(column: str, source: str) -> KeyPick:
        if column.lower() in unsafe:
            problem = f"row-pairing key {column} ({source}) is a masked column on {table}"
        elif facts is not None and column.lower() not in {n.lower() for n, _ in facts.columns}:
            problem = f"row-pairing key {column} ({source}) does not exist on {table}"
        else:
            problem = prove(column) if prove_explicit else ""
        why = sensitive_key_reason(column, tags_by_column.get(column.lower(), ()))
        warning = (f"WARNING: row-pairing key {column} ({source}) on {table} is used as you set it, "
                   f"but {why}; prefer a non-sensitive id") if why else ""
        return KeyPick(table, "" if problem else column, source, problem, warning)

    if explicit.strip():
        return explicit_pick(explicit.strip(), SOURCE_TABLE_KEY)
    global_key = global_key.strip()
    if facts is None:
        if global_key:
            return explicit_pick(global_key, SOURCE_GLOBAL_KEY)
        return KeyPick(table, problem=f"{no_key_message(table)} (could not read its columns: {facts_error})")
    if global_key and global_key.lower() in {n.lower() for n, _ in facts.columns}:
        return explicit_pick(global_key, SOURCE_GLOBAL_KEY)
    tried = []
    for column, source in key_candidates(table, facts, unsafe, tags_by_column):
        problem = prove(column)
        if not problem:
            return KeyPick(table, column, source)
        tried.append(f"{column}: {problem.split('; choose a unique')[0]}")
    detail = f" (tried {'; '.join(tried)})" if tried else ""
    return KeyPick(table, problem=no_key_message(table) + detail)


# ---------------------------------------------------------------------------
# Live workspace layer (guarded — requires databricks-sdk + a workspace)
# ---------------------------------------------------------------------------
def _require_live_enabled() -> None:
    if os.environ.get(LIVE_ENV_FLAG) != "1":
        raise RuntimeError(
            f"Live verification is disabled. Set {LIVE_ENV_FLAG}=1 and pass --live "
            "to run against a real workspace (needs a SQL warehouse and account admin)."
        )


def load_auth(auth_file: Path) -> dict[str, str]:
    """Parse an auth.auto.tfvars file into host/client_id/client_secret."""
    import hcl2  # local import: only needed for the live path

    with open(auth_file) as f:
        auth = hcl2.load(f)
    return {
        "host": _as_str(auth.get("databricks_workspace_host")),
        "client_id": _as_str(auth.get("databricks_client_id")),
        "client_secret": _as_str(auth.get("databricks_client_secret")),
        "account_host": _as_str(auth.get("databricks_account_host"))
        or "https://accounts.cloud.databricks.com",
        "account_id": _as_str(auth.get("databricks_account_id"))
        or os.environ.get("DATABRICKS_ACCOUNT_ID", ""),
        "workspace_id": _as_str(auth.get("databricks_workspace_id")),
    }


# Table, key and value column names are written into SQL, so every part must
# be a plain identifier; anything else is refused rather than escaped through.
_IDENT_RE = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_-]{0,254}")


def quote_identifier(name: str) -> str:
    """Backtick-quote one identifier part, refusing anything but [A-Za-z0-9_-]."""
    if not isinstance(name, str) or not _IDENT_RE.fullmatch(name):
        raise ValueError(f"unsafe SQL identifier {name!r}: use letters, digits, '_' or '-'")
    return "`" + name.replace("`", "``") + "`"


def table_parts(table: str) -> tuple[str, str, str]:
    """A catalog.schema.table name as its three validated parts."""
    parts = table.split(".") if isinstance(table, str) else []
    if len(parts) != 3:
        raise ValueError(f"unsafe SQL table name {table!r}: expected catalog.schema.table")
    for part in parts:
        quote_identifier(part)
    return parts[0], parts[1], parts[2]


def quote_table(table: str) -> str:
    return ".".join(quote_identifier(p) for p in table_parts(table))


def validate_spec_identifiers(spec: VerificationSpec) -> None:
    """Refuse a spec whose table/column names aren't plain identifiers."""
    for c in spec.column_masks:
        quote_table(c.table)
        quote_identifier(c.column)
        if c.key_column:
            quote_identifier(c.key_column)
        if c.expected_tiers:
            invalid = sorted({tier for _principal, tier in c.expected_tiers
                              if tier not in {"raw", "partial", "full"}})
            if invalid:
                raise ValueError(f"invalid deterministic tier(s) for {c.table}.{c.column}: {invalid}")
            if not c.partial_function or not c.full_function:
                raise ValueError(f"deterministic mask check {c.table}.{c.column} needs partial_function and full_function")
            table_parts(c.partial_function)
            table_parts(c.full_function)
    for r in spec.row_filters:
        quote_table(r.table)


# At most this many key values are bound into one statement; longer key lists
# are read in batches, so a large tier count can't overflow a statement.
KEY_PARAM_BATCH = 100


def _key_batches(keys: Sequence[Any]) -> list[list[Any]]:
    keys = list(keys)
    return [keys[i:i + KEY_PARAM_BATCH] for i in range(0, len(keys), KEY_PARAM_BATCH)]


def check_tiers(check: ColumnMaskCheck, admin_tier: str = DEFAULT_ADMIN_TIER) -> list[str]:
    """Every principal a mask check reads as: its tiers and the admin baseline."""
    return sorted(set(check.masked_principals) | set(check.unmasked_principals)
                  | set(dict(check.expected_tiers)) | {admin_tier})


def key_may_be_masked_message(check: ColumnMaskCheck, why: str,
                              admin_tier: str = DEFAULT_ADMIN_TIER) -> str:
    return (f"row-pairing key {check.key_column} may be masked for "
            f"{', '.join(check_tiers(check, admin_tier))} on {check.table} ({why}); "
            "choose a unique, non-null, unmasked key")


def _tag_assignments(entity_type: str, entity: str, tags: Sequence[tuple[str, str]]) -> list[dict]:
    # The matcher skips a valueless tag, and live tags (e.g. class.*) often have
    # none; a value no config can name keeps hasTag() matching it.
    return [{"entity_type": entity_type, "entity_name": entity, "tag_key": name,
             "tag_value": value or "\x00"} for name, value in tags]


def mask_policies_use_table_tags(mask_config: Optional[Mapping[str, Any]], table: str = "") -> bool:
    """Whether a column-mask policy that can apply to ``table`` reads table tags.

    Only policies the shared matcher would consider count: effective ones (not
    every target excepted) scoped to ``table``'s catalog, with a when_condition.
    """
    from validate_abac import _catalog_matches

    return any(
        _as_str(p.get("when_condition")) and _catalog_matches(dict(p), table)
        for p in effective_mask_policies((mask_config or {}).get("fgac_policies") or [])
    )


def key_tags_mask_problem(
    mask_config: Optional[Mapping[str, Any]], table: str, key_column: str,
    tags: Sequence[tuple[str, str]], table_tags: Sequence[tuple[str, str]] = (),
) -> str:
    """Why the key column's live ``tags`` could get it masked ("" if they can't).

    Decided as Terraform/Unity Catalog applies the masks, with the shared
    matcher (required_mask_columns: catalog-scoped, full tag conditions,
    fully-excepted policies skipped, names case-insensitive) over the config's
    tag assignments plus these live tags (and the table's live ``table_tags``,
    which when_conditions read). Fails closed when there is no config to check
    against or its conditions can't be read.
    """
    if not tags:
        return ""
    if mask_config is None:
        return f"{len(tags)} column tag(s), and no mask policies were given to check them against"
    assignments = (list(mask_config.get("tag_assignments") or [])
                   + _tag_assignments("columns", f"{table}.{key_column}", tags)
                   + _tag_assignments("tables", table, table_tags))
    try:
        masked = required_mask_columns(mask_config.get("fgac_policies") or [], assignments)
    except ValueError as exc:
        return f"{len(tags)} column tag(s), and the mask policies can't be checked: {exc}"
    if (table.lower(), key_column.lower()) in masked:
        return f"{len(tags)} column tag(s) a column-mask policy matches"
    return ""


def _key_filter(key_column: str, keys: Optional[Sequence[Any]]) -> tuple[str, dict[str, Any]]:
    """`` WHERE key IN (:k0, ...)`` and its parameters ("" when no keys)."""
    if keys is None:
        return "", {}
    if len(keys) > KEY_PARAM_BATCH:
        raise ValueError(f"{len(keys)} key values in one statement (max {KEY_PARAM_BATCH}); "
                         "read them in batches")
    params = {f"k{i}": k for i, k in enumerate(keys)}
    if not params:
        return " WHERE FALSE", {}
    return f" WHERE {quote_identifier(key_column)} IN ({', '.join(':' + n for n in params)})", params


@dataclass
class TestPrincipal:
    """A provisioned per-tier service principal and its query credentials."""
    tier: str                    # the group / access tier it belongs to
    display_name: str
    application_id: str
    client_secret: str
    sp_id: str = ""


class EffectiveAccessVerifier:
    """Provisions per-tier test principals and runs queries as each of them.

    Everything in this class touches a live workspace. The live guard
    (``GENIERAILS_LIVE_VERIFY=1``) is enforced at construction AND re-checked
    before every method that reaches the network, so no instance can perform a
    live call without the flag — even if it is constructed or driven directly
    rather than through :func:`verify_effective_access_live`.
    """

    def __init__(self, auth: Mapping[str, str], warehouse_id: str = "",
                 name_prefix: str = "genierails-verify", *, admin_only: bool = False):
        _require_live_enabled()
        self.auth = dict(auth)
        self.warehouse_id = warehouse_id
        # Admin-only runs (the pre-apply key check) grant nothing, so they may
        # use a workspace warehouse before Terraform has created the env's own.
        self.admin_only = admin_only
        self.name_prefix = name_prefix
        # The parsed ABAC config ({"fgac_policies", "tag_assignments"}) used to
        # tell whether a tag on a key column is one a column mask matches.
        self.mask_config: Optional[Mapping[str, Any]] = None
        self._admin_ws = None
        self._account = None

    @staticmethod
    def _guard() -> None:
        # Re-check on every network-facing call so the flag cannot be unset (or
        # never set) between construction and use.
        _require_live_enabled()

    # -- clients -----------------------------------------------------------
    @property
    def admin_ws(self):
        self._guard()
        if self._admin_ws is None:
            from databricks.sdk import WorkspaceClient
            self._admin_ws = WorkspaceClient(
                host=self.auth["host"],
                client_id=self.auth["client_id"],
                client_secret=self.auth["client_secret"],
            )
        return self._admin_ws

    @property
    def account(self):
        self._guard()
        if self._account is None:
            from databricks.sdk import AccountClient
            self._account = AccountClient(
                host=self.auth["account_host"],
                account_id=self.auth["account_id"],
                client_id=self.auth["client_id"],
                client_secret=self.auth["client_secret"],
            )
        return self._account

    def resolve_warehouse(self) -> str:
        self._guard()
        if self.warehouse_id:
            return self.warehouse_id
        if self.admin_only:
            sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))
            from warehouse_utils import select_warehouse

            chosen = select_warehouse([w for w in self.admin_ws.warehouses.list() if w.id])
            if chosen is not None:
                self.warehouse_id = chosen.id
                return self.warehouse_id
        raise RuntimeError(
            "No SQL warehouse was resolved; pass --warehouse-id or configure one "
            "in env.auto.tfvars. Arbitrary workspace warehouse selection is disabled."
        )

    # -- provisioning ------------------------------------------------------
    def resolve_principal_groups(self, memberships: Sequence[str]) -> list[Any]:
        """Resolve every configured membership to one exact account group."""
        self._guard()
        a = self.account
        resolved_groups = []
        for membership in memberships:
            candidates = list(a.groups.list(
                filter=f"displayName eq {json.dumps(membership, ensure_ascii=False)}"))
            exact = [group for group in candidates
                     if getattr(group, "display_name", None) == membership]
            if len(exact) != 1:
                raise RuntimeError(
                    f"Tier group {membership!r} resolved to {len(exact)} exact account-group "
                    f"matches ({len(candidates)} SCIM result(s)); expected exactly one. "
                    "Apply or correct the account layer before verification."
                )
            resolved_groups.append(exact[0])
        return resolved_groups

    def provision_principal(
        self, tier: str, memberships: Optional[Sequence[str]] = None,
        *, resolved_groups: Optional[Sequence[Any]] = None,
    ) -> TestPrincipal:
        """Create (or reuse) a service principal and add it to the tier group."""
        self._guard()
        from databricks.sdk.service import iam

        display_name = f"{self.name_prefix}-{tier}"
        a = self.account
        if resolved_groups is None:
            resolved_groups = self.resolve_principal_groups(
                (tier,) if memberships is None else memberships)

        candidates = list(a.service_principals.list(
            filter=f"displayName eq {json.dumps(display_name, ensure_ascii=False)}"))
        exact = [sp for sp in candidates
                 if getattr(sp, "display_name", None) == display_name]
        if len(exact) > 1:
            raise RuntimeError(
                f"Verification service principal {display_name!r} resolved to "
                f"{len(exact)} exact matches; expected at most one.")
        if not exact:
            sp = a.service_principals.create(display_name=display_name, active=True)
        else:
            sp = exact[0]

        # Mint an OAuth secret so the principal can authenticate on its own.
        secret = a.service_principal_secrets.create(service_principal_id=int(sp.id))

        # Add the SP to every requested account group. A dual-tier test
        # principal proves policy precedence, not merely each tier in isolation.
        for group in resolved_groups:
            if not any((m.value == sp.id) for m in (group.members or [])):
                a.groups.patch(
                    group.id,
                    operations=[iam.Patch(op=iam.PatchOp.ADD, path="members", value=[{"value": sp.id}])],
                    schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP])

        workspace_id = self.auth.get("workspace_id", "")
        if not workspace_id:
            raise RuntimeError(
                "databricks_workspace_id is required to assign verification "
                "principals to the workspace"
            )
        a.workspace_assignment.update(
            workspace_id=int(workspace_id),
            principal_id=int(sp.id),
            permissions=[iam.WorkspacePermission.USER],
        )
        deadline = time.time() + int(
            os.environ.get("GENIERAILS_VERIFY_WORKSPACE_SYNC_TIMEOUT", "90")
        )
        while True:
            visible = next(
                (
                    item
                    for item in self.admin_ws.service_principals.list(
                        filter=f'applicationId eq "{sp.application_id}"'
                    )
                ),
                None,
            )
            if visible is not None:
                break
            if time.time() >= deadline:
                raise TimeoutError(
                    f"Verification principal {sp.application_id} was assigned to "
                    "the workspace but did not become visible before timeout"
                )
            time.sleep(2)

        return TestPrincipal(
            tier=tier,
            display_name=display_name,
            application_id=sp.application_id or "",
            client_secret=secret.secret or "",
            sp_id=sp.id or "",
        )

    def deprovision_principal(self, principal: TestPrincipal) -> None:
        self._guard()
        try:
            if principal.sp_id:
                self.account.service_principals.delete(principal.sp_id)
        except Exception as exc:  # best-effort cleanup
            print(f"  WARN: could not delete {principal.display_name}: {exc}")

    def grant_warehouse_use(self, principal: TestPrincipal) -> None:
        """Grant a temporary test principal CAN_USE on the query warehouse."""
        self._guard()
        from databricks.sdk.service import iam

        warehouse_id = self.resolve_warehouse()
        self.admin_ws.permissions.update(
            request_object_type="warehouses",
            request_object_id=warehouse_id,
            access_control_list=[
                iam.AccessControlRequest(
                    service_principal_name=principal.application_id,
                    permission_level=iam.PermissionLevel.CAN_USE,
                )
            ],
        )

    def grant_outsider_table_access(
        self, principal: TestPrincipal, tables: Sequence[str], *, revoke: bool = False,
    ) -> None:
        """Temporarily grant/revoke the outsider access to only checked tables.

        These direct SQL grants deliberately never enter Terraform state.  The
        caller always revokes them in its outermost ``finally`` block.
        """
        self._guard()
        verb = "REVOKE" if revoke else "GRANT"
        joiner = " FROM " if revoke else " TO "
        grantee = quote_identifier(principal.application_id)
        ws = self.admin_ws
        catalogs: set[str] = set()
        schemas: set[tuple[str, str]] = set()
        normalized_tables: set[tuple[str, str, str]] = set()
        for table in tables:
            parts = table.split(".")
            if len(parts) != 3:
                raise ValueError(f"checked table {table!r} is not catalog.schema.table")
            catalog, schema, name = parts
            catalogs.add(catalog)
            schemas.add((catalog, schema))
            normalized_tables.add((catalog, schema, name))
        statements = [
            f"{verb} SELECT ON TABLE {quote_table('.'.join(table))}{joiner}{grantee}"
            for table in sorted(normalized_tables)
        ]
        statements.extend(
            f"{verb} USE SCHEMA ON SCHEMA {quote_identifier(catalog)}."
            f"{quote_identifier(schema)}{joiner}{grantee}"
            for catalog, schema in sorted(schemas)
        )
        statements.extend(
            f"{verb} USE CATALOG ON CATALOG {quote_identifier(catalog)}{joiner}{grantee}"
            for catalog in sorted(catalogs)
        )
        # Revoke narrow privileges before their parents; grant parents first.
        if not revoke:
            statements.reverse()
        failures = []
        for statement in statements:
            try:
                self.run_query(ws, statement)
            except BaseException as exc:
                if not revoke:
                    raise
                failures.append(str(exc))
        if failures:
            raise RuntimeError(
                f"failed to revoke {len(failures)} temporary outsider privilege(s): "
                + "; ".join(failures)
            )

    def _ws_for(self, principal: TestPrincipal):
        self._guard()
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient(
            host=self.auth["host"],
            client_id=principal.application_id,
            client_secret=principal.client_secret,
        )

    # -- querying ----------------------------------------------------------
    def run_query(self, ws, sql: str,
                  parameters: Optional[Mapping[str, Any]] = None) -> list[list[Any]]:
        """Run ``sql`` as ``ws``; ``parameters`` bind its ``:name`` markers.

        Key values are always bound as parameters, never written into the SQL,
        so a query error (which may echo the statement) cannot print them.
        """
        self._guard()
        from databricks.sdk.service.sql import StatementParameterListItem, StatementState

        stmt = ws.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id, statement=sql.strip(), wait_timeout="50s",
            parameters=[
                StatementParameterListItem(name=name, value=None if value is None else str(value))
                for name, value in (parameters or {}).items()
            ] or None,
        )
        deadline = time.time() + 300
        while True:
            state = stmt.status.state
            if state == StatementState.SUCCEEDED:
                return (stmt.result.data_array or []) if stmt.result else []
            if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
                raise RuntimeError(f"Query failed ({state}): {stmt.status.error}")
            if time.time() > deadline:
                raise TimeoutError(f"Query timed out: {sql[:80]}")
            time.sleep(2)
            stmt = ws.statement_execution.get_statement(stmt.statement_id)

    def collect_column_values(
        self, principal: TestPrincipal, check: ColumnMaskCheck, limit: int = 25,
        keys: Optional[Sequence[Any]] = None, salt: Optional[str] = None,
    ) -> list[tuple[Any, Any]]:
        """Return the (row_key, column_value) rows a principal sees.

        Every fetched row is returned (not a {key: value} dict) so a NULL or
        repeated key stays visible to the evaluator. With ``keys``, only rows
        whose key is one of them are fetched (in batches), so every tier reads
        the same rows. Without, ``limit`` rows are sampled: the lowest keys, or
        with ``salt`` rows spread across the table by a salted hash of the key —
        NULL keys still first, and repeats of a key still adjacent.

        Raises on any failure — a failed/denied query is a verification failure,
        not an empty (and falsely-passing) result. The caller records the error.
        """
        self._guard()
        if not check.key_column:
            raise ValueError(
                f"no key_column configured for {check.table}.{check.column}; "
                "cannot pair rows across principals"
            )
        if keys is not None and len(keys) > KEY_PARAM_BATCH:
            return [row for batch in _key_batches(keys)
                    for row in self.collect_column_values(principal, check, len(batch), batch)]
        ws = self._ws_for(principal)
        where, params = _key_filter(check.key_column, keys)
        key = quote_identifier(check.key_column)
        order = key
        if keys is None and salt is not None:
            order = f"{key} IS NULL DESC, xxhash64(:salt, {key})"
            params = {**params, "salt": salt}
        sql = (
            f"SELECT {key}, {quote_identifier(check.column)} "
            f"FROM {quote_table(check.table)}{where} ORDER BY {order} LIMIT {int(limit)}"
        )
        try:
            rows = self.run_query(ws, sql, params)
        except Exception as exc:
            detail = str(exc)
            detail_lower = detail.lower()
            key_is_named = check.key_column.lower() in detail_lower
            missing_column_error = any(marker in detail_lower for marker in (
                "unresolved_column", "unresolved column", "column not found",
                "cannot be resolved", "cannot resolve column",
            ))
            if key_is_named and missing_column_error:
                raise RuntimeError(
                    f"verification key column {check.key_column!r} is missing or "
                    f"inaccessible on {check.table}: {exc}"
                ) from exc
            raise
        return [(r[0], r[1]) for r in rows if r]

    def prove_key_unique(
        self, principal: TestPrincipal, check: ColumnMaskCheck, keys: Sequence[Any],
    ) -> str:
        """Prove, across the whole table, that each sampled key names one row.

        Run as the admin baseline. The sample alone cannot show this: a repeat
        of the last sampled key can sit past the LIMIT. Returns "" when proven,
        else the reason (counts only, never key values).
        """
        self._guard()
        key = quote_identifier(check.key_column)
        total = distinct = non_null = 0
        # Batches hold disjoint keys, so their distinct counts add up.
        for batch in _key_batches(list(dict.fromkeys(keys))):
            where, params = _key_filter(check.key_column, batch)
            rows = self.run_query(
                self._ws_for(principal),
                f"SELECT COUNT(*), COUNT(DISTINCT {key}), COUNT({key}) "
                f"FROM {quote_table(check.table)}{where}",
                params,
            )
            counts = [int(v or 0) for v in (rows[0] if rows else (0, 0, 0))]
            total, distinct, non_null = total + counts[0], distinct + counts[1], non_null + counts[2]
        if total == distinct == non_null == len(keys):
            return ""
        return _key_problem_message(
            check,
            f"is not unique / has NULLs ({len(keys)} sampled keys match {total} rows, "
            f"{distinct} distinct, {non_null} non-null)",
        )

    def prove_pairing_key(self, principal: TestPrincipal, table: str, key_column: str,
                          salt: Optional[str] = None) -> str:
        """Admin proof that ``key_column`` can pair ``table``'s rows ("" when proven).

        Exactly the admin-side checks the live run applies to any key, with the
        same functions: key_mask_metadata (no live column mask, no live tag a
        column-mask policy matches), a spread sample (collect_column_values)
        free of NULL and repeated keys (sample_key_problem), and the
        whole-table count (prove_key_unique). Reasons carry counts only.
        """
        self._guard()
        check = ColumnMaskCheck(table, key_column, key_column, (), ())
        try:
            found = self.key_mask_metadata(principal, check)
            if found:
                return _pairing_key_problem(key_column, table, f"may be masked (it has {', '.join(found)})")
            rows = self.collect_column_values(principal, check, SAMPLE_ROWS, salt=salt)
            if not rows:
                return f"the admin baseline sees no rows of {table}, so rows cannot be paired by {key_column}"
            return (sample_key_problem(check, principal.tier, rows)
                    or self.prove_key_unique(principal, check, [k for k, _ in rows]))
        except Exception as exc:
            return f"could not prove row-pairing key {key_column} on {table}: {exc}"

    def table_key_facts(self, principal: TestPrincipal, table: str) -> TableKeyFacts:
        """Columns, PRIMARY KEY and live column tags of ``table`` (read as the
        admin). The tags only rule out sensitive-looking candidates
        (sensitive_key_reason); mask-relevance is key_mask_metadata's call."""
        self._guard()
        params = dict(zip(("c", "s", "t"), (p.lower() for p in table_parts(table))))
        ws = self._ws_for(principal)
        columns = tuple(
            (str(r[0]), str(r[1] or "")) for r in self.run_query(ws, (
                "SELECT column_name, data_type FROM system.information_schema.columns "
                "WHERE table_catalog = :c AND table_schema = :s AND table_name = :t "
                "ORDER BY ordinal_position"), params) if r)
        if not columns:
            raise RuntimeError(f"no columns of {table} are visible to the admin")
        primary_key = tuple(str(r[0]) for r in self.run_query(ws, (
            "SELECT k.column_name FROM system.information_schema.table_constraints c "
            "JOIN system.information_schema.key_column_usage k "
            "ON k.constraint_catalog = c.constraint_catalog AND k.constraint_schema = c.constraint_schema "
            "AND k.constraint_name = c.constraint_name "
            "WHERE c.constraint_type = 'PRIMARY KEY' AND c.table_catalog = :c "
            "AND c.table_schema = :s AND c.table_name = :t ORDER BY k.ordinal_position"), params) if r)
        column_tags = tuple(
            (str(r[0]), str(r[1]), "" if r[2] is None else str(r[2])) for r in self.run_query(ws, (
                "SELECT column_name, tag_name, tag_value FROM system.information_schema.column_tags "
                "WHERE lower(catalog_name) = :c AND lower(schema_name) = :s AND lower(table_name) = :t"),
                params) if r)
        return TableKeyFacts(columns, primary_key, column_tags)

    def count_rows_with_keys(
        self, principal: TestPrincipal, check: ColumnMaskCheck, keys: Sequence[Any],
    ) -> int:
        """How many rows ``principal`` sees whose key is one of ``keys``."""
        self._guard()
        total = 0
        for batch in _key_batches(list(dict.fromkeys(keys))):
            where, params = _key_filter(check.key_column, batch)
            rows = self.run_query(self._ws_for(principal),
                                  f"SELECT COUNT(*) FROM {quote_table(check.table)}{where}", params)
            total += int(rows[0][0] or 0) if rows else 0
        return total

    def run_api(self, ws, path: str, query: Optional[Mapping[str, Any]] = None) -> Mapping[str, Any]:
        """GET a Databricks REST path as ``ws`` (the admin baseline)."""
        return ws.api_client.do("GET", path, query=dict(query or {}))

    def table_policies(self, principal: TestPrincipal, table: str) -> list[Mapping[str, Any]]:
        """Every live ABAC policy on ``table``, including inherited ones (all pages)."""
        self._guard()
        catalog, schema, name = table_parts(table)
        policies: list[Mapping[str, Any]] = []
        token = ""
        for _ in range(100):
            query = {"include_inherited": "true", **({"page_token": token} if token else {})}
            page = self.run_api(self._ws_for(principal),
                                f"/api/2.1/unity-catalog/policies/TABLE/{catalog}.{schema}.{name}", query)
            policies += list(page.get("policies") or [])
            token = page.get("next_page_token") or ""
            if not token:
                return policies
        raise RuntimeError(f"too many pages of policies on {table}")

    def fixed_point_problem(self, principal: TestPrincipal, check: ColumnMaskCheck,
                            policies: Sequence[Mapping[str, Any]]) -> str:
        """Why fixed points can't be excluded for ``check`` ("" when they can).

        Run as the admin. The live mask on the column must be exactly the
        configured function with no extra arguments (live_mask_problem), and
        that function caller-independent (fixed_point_function_problem).
        """
        self._guard()
        fn_catalog, fn_schema, fn_name = table_parts(check.mask_function)
        catalog, schema, table = table_parts(check.table)
        quote_identifier(check.column)
        ws = self._ws_for(principal)
        col = {"c": catalog.lower(), "s": schema.lower(), "t": table.lower(), "k": check.column.lower()}
        where = ("WHERE lower({0}) = :c AND lower({1}) = :s "
                 "AND lower(table_name) = :t AND lower(column_name) = :k")
        rows = self.run_query(ws, "SELECT COUNT(*) FROM system.information_schema.column_masks "
                              + where.format("table_catalog", "table_schema"), col)
        direct = int(rows[0][0] or 0) if rows else 0
        column_tags = [(str(r[0]), "" if r[1] is None else str(r[1])) for r in self.run_query(
            ws, "SELECT tag_name, tag_value FROM system.information_schema.column_tags "
            + where.format("catalog_name", "schema_name"), col) if r]
        table_tags = [(str(r[0]), "" if r[1] is None else str(r[1])) for r in self.run_query(
            ws, "SELECT tag_name, tag_value FROM system.information_schema.table_tags "
            "WHERE lower(catalog_name) = :c AND lower(schema_name) = :s AND lower(table_name) = :t",
            {n: col[n] for n in ("c", "s", "t")}) if r] if any(p.get("when_condition") for p in policies) else []
        problem = live_mask_problem(check, policies, column_tags, table_tags, direct)
        if problem:
            return problem
        fn = {"c": fn_catalog.lower(), "s": fn_schema.lower(), "n": fn_name.lower()}
        routines = self.run_query(
            ws, "SELECT routine_body, external_language, is_deterministic, sql_data_access, "
            "routine_definition FROM system.information_schema.routines WHERE lower(routine_catalog) = :c "
            "AND lower(routine_schema) = :s AND lower(routine_name) = :n", fn)
        if len(routines) != 1:
            return f"{len(routines)} routines named {check.mask_function}, not exactly one"
        params = self.run_query(
            ws, "SELECT parameter_name FROM system.information_schema.parameters WHERE lower(specific_catalog) = :c "
            "AND lower(specific_schema) = :s AND lower(specific_name) = :n", fn)
        body, language, deterministic, data_access, definition = routines[0]
        return fixed_point_function_problem(
            routine_body=body, external_language=language, is_deterministic=deterministic,
            sql_data_access=data_access, definition=definition,
            parameters=[row[0] for row in params])

    def fixed_point_keys(
        self, principal: TestPrincipal, check: ColumnMaskCheck, keys: Sequence[Any],
    ) -> set[Any]:
        """Keys among ``keys`` whose raw value ``check.mask_function`` maps to itself.

        Run as the admin baseline, which sees raw values. NULL-safe equality, so
        a NULL raw value counts too (it is never compared anyway).
        """
        self._guard()
        catalog, schema, name = table_parts(check.mask_function)
        function = ".".join(quote_identifier(part) for part in (catalog, schema, name))
        column, key = quote_identifier(check.column), quote_identifier(check.key_column)
        found: set[Any] = set()
        for batch in _key_batches(list(dict.fromkeys(keys))):
            where, params = _key_filter(check.key_column, batch)
            rows = self.run_query(
                self._ws_for(principal),
                f"SELECT {key} FROM {quote_table(check.table)}{where} AND ({function}({column}) <=> {column})",
                params)
            found.update(row[0] for row in rows)
        return found

    def expected_tier_values(
        self, principal: TestPrincipal, check: ColumnMaskCheck, keys: Sequence[Any],
    ) -> dict[str, list[tuple[Any, Any]]]:
        """Apply reviewed caller-independent functions to raw rows as admin."""
        self._guard()
        key, column = quote_identifier(check.key_column), quote_identifier(check.column)
        functions = {"partial": check.partial_function, "full": check.full_function}
        output: dict[str, list[tuple[Any, Any]]] = {"raw": []}
        for batch in _key_batches(list(dict.fromkeys(keys))):
            where, params = _key_filter(check.key_column, batch)
            expressions = [column]
            for tier in ("partial", "full"):
                catalog, schema, name = table_parts(functions[tier])
                function = ".".join(quote_identifier(part) for part in (catalog, schema, name))
                expressions.append(f"{function}({column})")
            rows = self.run_query(self._ws_for(principal),
                                  f"SELECT {key}, {', '.join(expressions)} FROM {quote_table(check.table)}{where}",
                                  params)
            for row in rows:
                if row:
                    output.setdefault("raw", []).append((row[0], row[1]))
                    output.setdefault("partial", []).append((row[0], row[2]))
                    output.setdefault("full", []).append((row[0], row[3]))
        return output

    def key_mask_metadata(self, principal: TestPrincipal, check: ColumnMaskCheck) -> list[str]:
        """What could mask the key column for some tier ([] when nothing can).

        Run as the admin baseline. A key mask can permute keys or map them onto
        other sampled keys, which no row comparison can see, so a key column is
        refused if it has a live column mask, or a live tag some column-mask
        policy in ``self.mask_config`` matches given the table's live tags
        (key_tags_mask_problem). Other tags, e.g. native class.* tags on an ID,
        don't disqualify it.
        """
        self._guard()
        catalog, schema, table = table_parts(check.table)
        quote_identifier(check.key_column)
        params = {"c": catalog.lower(), "s": schema.lower(), "t": table.lower(),
                  "k": check.key_column.lower()}
        where = ("WHERE lower({0}) = :c AND lower({1}) = :s "
                 "AND lower(table_name) = :t AND lower(column_name) = :k")
        ws = self._ws_for(principal)
        found = []
        rows = self.run_query(
            ws, "SELECT COUNT(*) FROM system.information_schema.column_masks "
            + where.format("table_catalog", "table_schema"), params)
        masks = int(rows[0][0] or 0) if rows else 0
        if masks:
            found.append(f"{masks} column mask(s)")
        def tag_rows(sql: str, query_params: Mapping[str, Any]) -> list[tuple[str, str]]:
            return [(str(r[0]), "" if r[1] is None else str(r[1]))
                    for r in self.run_query(ws, sql, query_params) if r]

        tags = tag_rows("SELECT tag_name, tag_value FROM system.information_schema.column_tags "
                        + where.format("catalog_name", "schema_name"), params)
        # A when_condition is judged on the table's tags; read them live too
        # (a failure to read them propagates, so the key fails closed).
        table_tags = tag_rows(
            "SELECT tag_name, tag_value FROM system.information_schema.table_tags "
            "WHERE lower(catalog_name) = :c AND lower(schema_name) = :s AND lower(table_name) = :t",
            {n: params[n] for n in ("c", "s", "t")},
        ) if tags and mask_policies_use_table_tags(self.mask_config, check.table) else []
        problem = key_tags_mask_problem(self.mask_config, check.table, check.key_column, tags,
                                        table_tags)
        if problem:
            found.append(problem)
        return found

    def collect_row_count(self, principal: TestPrincipal, table: str) -> int:
        """Return the row count a principal sees. Raises on query failure."""
        self._guard()
        ws = self._ws_for(principal)
        rows = self.run_query(ws, f"SELECT COUNT(*) FROM {quote_table(table)}")
        return int(rows[0][0]) if rows else 0


def verify_effective_access_live(
    spec: VerificationSpec,
    auth_file: Path,
    *,
    warehouse_id: str = "",
    keep_principals: bool = False,
    admin_tier: str = DEFAULT_ADMIN_TIER,
    global_key: str = "",
    unsafe_by_table: Optional[Mapping[str, set[str]]] = None,
) -> EffectiveAccessReport:
    """Provision per-tier principals, run queries as each, and evaluate effects.

    First, as the admin only, every masked table's row-pairing key is picked
    and proven (see pick_table_key). A table with no provable key is always
    blocking (INCONCLUSIVE), so the run can't pass; the rest are verified with
    their keys.

    Guarded: raises unless ``GENIERAILS_LIVE_VERIFY=1``.
    """
    _require_live_enabled()
    validate_spec_identifiers(spec)
    auth = load_auth(auth_file)
    verifier = EffectiveAccessVerifier(auth, warehouse_id=warehouse_id)
    verifier.mask_config = spec.mask_config
    verifier.resolve_warehouse()
    # Mask checks sample rows spread across each table by a salted hash of the
    # key, a fresh salt per run; logging it (never a key) makes a run repeatable.
    salt = os.environ.get(SAMPLE_SALT_ENV) or secrets.token_hex(8)

    principals: dict[str, TestPrincipal] = {}
    # The admin baseline uses the admin credentials directly (raw values).
    admin_principal = TestPrincipal(
        tier=admin_tier,
        display_name="admin-baseline",
        application_id=auth["client_id"],
        client_secret=auth["client_secret"],
    )
    principals[admin_tier] = admin_principal

    # Explicit keys are proven per check below (#90's checks), like auto picks.
    picks = pick_pairing_keys(verifier, admin_principal, spec.column_masks, global_key=global_key,
                              unsafe_by_table=unsafe_by_table, prove_explicit=False, salt=salt)
    print_key_picks(picks)
    keyed, blocking = [], []
    for check in spec.column_masks:
        pick = picks[check.table.lower()]
        if pick.key:
            keyed.append(replace(check, key_column=check.key_column or pick.key))
            continue
        blocking.append(CheckResult("column-mask", check.describe(), INCONCLUSIVE, pick.problem,
                                    {"key_source": pick.source}))
    spec = VerificationSpec(column_masks=keyed, row_filters=list(spec.row_filters),
                            mask_config=spec.mask_config,
                            principal_memberships=dict(spec.principal_memberships))
    if spec.is_empty():
        return EffectiveAccessReport(results=blocking)

    old_signal_handlers: dict[int, Any] = {}
    if threading.current_thread() is threading.main_thread():
        def interrupt_for_cleanup(signum, _frame):
            raise KeyboardInterrupt(f"received {signal.Signals(signum).name}; cleaning up verification access")

        for signum in (signal.SIGTERM, signal.SIGHUP):
            old_signal_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, interrupt_for_cleanup)

    report: Optional[EffectiveAccessReport] = None
    cleanup_error: Optional[BaseException] = None
    try:
        resolved_groups = {}
        for tier in sorted(spec.principals):
            memberships = spec.principal_memberships.get(tier, (tier,))
            resolved_groups[tier] = verifier.resolve_principal_groups(memberships)

        for tier in sorted(spec.principals):
            print(f"  Provisioning test principal for tier: {tier}")
            memberships = spec.principal_memberships.get(tier)
            principal = verifier.provision_principal(
                tier,
                memberships if tier in spec.principal_memberships else None,
                resolved_groups=resolved_groups[tier],
            )
            principals[tier] = principal
            print(f"  Granting warehouse CAN_USE to test principal: {tier}")
            verifier.grant_warehouse_use(principal)

        outsider = principals.get(OUT_OF_TIER_PRINCIPAL)
        outsider_tables = sorted({check.table for check in spec.column_masks})
        if outsider and outsider_tables:
            print("  Granting temporary checked-table access to out-of-tier principal")
            verifier.grant_outsider_table_access(outsider, outsider_tables)

        # Newly-added group membership can take a short while to propagate.
        time.sleep(int(os.environ.get("GENIERAILS_VERIFY_PROPAGATION_SLEEP", "10")))
        if spec.column_masks:
            print(f"  Mask-check row sample salt: {salt} ({SAMPLE_SALT_ENV}={salt} repeats it)")

        column_values: dict[tuple, dict[str, list[tuple[Any, Any]]]] = {}
        column_errors: dict[tuple, dict[str, str]] = {}
        expected_values: dict[tuple, dict[str, list[tuple[Any, Any]]]] = {}
        pairing_problems: dict[tuple, str] = {}
        fixed_points: dict[tuple, set[Any]] = {}
        table_policies: dict[str, list[Mapping[str, Any]]] = {}
        key_proofs: dict[tuple, str] = {}   # once per (table, key, read keys)
        key_metadata: dict[tuple, str] = {}  # once per (table, key)
        for check in spec.column_masks:
            sig = (check.table, check.column)
            per_principal: dict[str, list[tuple[Any, Any]]] = {}
            per_errors: dict[str, str] = {}
            involved = (set(dict(check.expected_tiers)) if check.expected_tiers else
                        set(check.masked_principals) | set(check.unmasked_principals))
            tiers = sorted(involved | {admin_tier})
            # (1) A key that some tier could see masked pairs rows wrongly in
            # ways no row comparison can detect (a permuting mask stays unique
            # and overlapping), so its masks/tags must show nothing.
            meta_sig = (check.table, check.key_column)
            if meta_sig not in key_metadata:
                try:
                    found = verifier.key_mask_metadata(admin_principal, check)
                    why = f"it has {', '.join(found)}" if found else ""
                except Exception as exc:
                    why = f"could not read its column masks/tags as the admin baseline: {exc}"
                key_metadata[meta_sig] = key_may_be_masked_message(check, why, admin_tier) if why else ""
            if key_metadata[meta_sig]:
                pairing_problems[sig] = key_metadata[meta_sig]
                continue
            # (2) Samples: the admin's, and each tier's OWN first rows, so rows
            # only a tier sees (e.g. outside the admin's sample) are compared
            # too, not just the rows the admin happened to sample.
            samples: dict[str, list[tuple[Any, Any]]] = {}
            for tier in tiers:
                p = admin_principal if tier == admin_tier else principals.get(tier)
                if p is None:
                    # A tier in the check we could not provision leaves a gap the
                    # evaluator must treat as a failure, not silently ignore.
                    per_errors[tier] = "principal was not provisioned"
                    continue
                try:
                    samples[tier] = verifier.collect_column_values(p, check, SAMPLE_ROWS, salt=salt)
                except Exception as exc:
                    print(f"    ({tier}) query FAILED for {check.table}.{check.column}: {exc}")
                    if tier in involved:
                        per_errors[tier] = str(exc)
                    else:
                        pairing_problems[sig] = (
                            f"could not sample row-pairing key {check.key_column} on "
                            f"{check.table} as the admin baseline: {exc}")
            column_errors[sig] = per_errors
            hard_sample_errors = {
                principal: detail for principal, detail in per_errors.items()
                if principal not in set(check.moving_principals)
                or "more than one mask" not in detail.lower()
            }
            if hard_sample_errors or sig in pairing_problems:
                continue
            problem = next((why for why in (sample_key_problem(check, t, samples[t])
                                             for t in tiers if t in samples)
                            if why), "")
            keys = list(dict.fromkeys(k for t in tiers if t in samples for k, _ in samples[t]))
            if not problem and not keys:
                problem = (f"no principal sees rows of {check.table}, so rows cannot be "
                           f"paired by {check.key_column}")
            # (3) As the admin: every sampled key, whoever sampled it, names
            # exactly one row of the whole table; a tier key the admin can't
            # find means that tier sees the key masked.
            if not problem:
                proof_sig = (check.table, check.key_column, tuple(keys))
                if proof_sig not in key_proofs:
                    try:
                        key_proofs[proof_sig] = verifier.prove_key_unique(admin_principal, check, keys)
                        if key_proofs[proof_sig]:
                            missing = [
                                t for t in tiers if t != admin_tier and samples.get(t)
                                and verifier.count_rows_with_keys(
                                    admin_principal, check, [k for k, _ in samples.get(t, [])],
                                ) < len(samples.get(t, []))
                            ]
                            if missing:
                                key_proofs[proof_sig] = (
                                    f"row-pairing key {check.key_column} may be masked for "
                                    f"{', '.join(missing)} on {check.table} (the admin baseline "
                                    "can't find keys they see); choose a unique, non-null, "
                                    "unmasked key")
                    except Exception as exc:
                        key_proofs[proof_sig] = (
                            f"could not prove row-pairing key {check.key_column} is unique "
                            f"on {check.table}: {exc}")
                problem = key_proofs[proof_sig]
            if problem:
                pairing_problems[sig] = problem
                continue
            # (4) Every tier reads exactly those rows, so each comparison is
            # between the same rows and every tier's own rows are covered.
            for tier in sorted(involved):
                p = admin_principal if tier == admin_tier else principals[tier]
                try:
                    per_principal[tier] = verifier.collect_column_values(
                        p, check, limit=len(keys), keys=keys)
                except Exception as exc:
                    per_errors[tier] = str(exc)
                    print(f"    ({tier}) query FAILED for {check.table}.{check.column}: {exc}")
            column_values[sig] = per_principal
            if check.expected_tiers:
                try:
                    expected_values[sig] = verifier.expected_tier_values(admin_principal, check, keys)
                except Exception as exc:
                    pairing_problems[sig] = f"could not compute exact expected tier outputs as admin: {exc}"
                # Account group membership is eventually consistent. Retry an
                # exact-output mismatch (but never a raw leak) with bounded
                # exponential backoff; query failures retain their normal
                # fail-closed semantics, including moving-principal errors.
                deadline = time.time() + int(os.environ.get(
                    "GENIERAILS_VERIFY_PROPAGATION_TIMEOUT", "300"))
                backoff = max(1, int(os.environ.get(
                    "GENIERAILS_VERIFY_PROPAGATION_BACKOFF", "5")))
                while sig in expected_values and not pairing_problems.get(sig):
                    result = evaluate_tiered_column_mask_check(
                        check, per_principal, expected_values[sig], per_errors,
                        principal_memberships=spec.principal_memberships)
                    expired = time.time() >= deadline
                    if result.status != FAIL or "did not exactly match" not in result.detail or expired:
                        if expired and result.status == FAIL and "did not exactly match" in result.detail:
                            print(f"    Tier membership propagation deadline expired for {check.table}.{check.column}")
                        break
                    print(f"    Tier membership may still be propagating; retrying {check.table}.{check.column}")
                    time.sleep(min(backoff, max(0, deadline - time.time())))
                    backoff = min(backoff * 2, 60)
                    per_errors.clear()
                    for tier in sorted(involved):
                        try:
                            per_principal[tier] = verifier.collect_column_values(
                                admin_principal if tier == admin_tier else principals[tier],
                                check, limit=len(keys), keys=keys)
                        except Exception as exc:
                            per_errors[tier] = str(exc)
            # (5) Rows the mask leaves unchanged can't show masking, but only
            # for the live mask proven to be the configured, caller-independent
            # function. Any doubt or error: every equal value stays a leak.
            if check.mask_function and not per_errors:
                try:
                    if check.table not in table_policies:
                        table_policies[check.table] = verifier.table_policies(admin_principal, check.table)
                    why = verifier.fixed_point_problem(admin_principal, check, table_policies[check.table])
                    if why:
                        print(f"    {check.table}.{check.column}: an unchanged value counts as a leak ({why})")
                    else:
                        fixed_points[sig] = verifier.fixed_point_keys(admin_principal, check, keys)
                except Exception as exc:
                    print(f"    (admin) could not check {check.mask_function} on "
                          f"{check.table}.{check.column}; an unchanged value counts as a leak: {exc}")

        row_counts: dict[str, dict[str, Optional[int]]] = {}
        row_errors: dict[str, dict[str, str]] = {}
        for check in spec.row_filters:
            per_principal_counts: dict[str, Optional[int]] = {}
            per_row_errors: dict[str, str] = {}
            for tier in set(check.restricted_principals) | set(check.unrestricted_principals):
                p = principals.get(tier)
                if p is None:
                    per_row_errors[tier] = "principal was not provisioned"
                    continue
                try:
                    per_principal_counts[tier] = verifier.collect_row_count(p, check.table)
                except Exception as exc:
                    per_row_errors[tier] = str(exc)
                    print(f"    ({tier}) COUNT FAILED for {check.table}: {exc}")
            row_counts[check.table] = per_principal_counts
            row_errors[check.table] = per_row_errors

        report = evaluate_effective_access(
            spec, column_values, row_counts, column_errors, row_errors,
            pairing_problems=pairing_problems, fixed_points=fixed_points,
            expected_values=expected_values,
        )
        report.results.extend(blocking)
        report.pairing_keys = proven_keys_by_table(report, spec)
        if spec.column_masks:
            report.sample_note = (
                f"checked {SAMPLE_ROWS} sampled rows per tier per masked column (spread by salt "
                f"{salt}; rerun with {SAMPLE_SALT_ENV}={salt} to repeat) — a bounded sample, "
                "not every row")
        return report
    finally:
        outsider = principals.get(OUT_OF_TIER_PRINCIPAL)
        outsider_tables = sorted({check.table for check in spec.column_masks})
        if outsider and outsider_tables:
            try:
                verifier.grant_outsider_table_access(outsider, outsider_tables, revoke=True)
            except BaseException as exc:
                cleanup_error = exc
                privileges = ", ".join(f"USE CATALOG/USE SCHEMA/SELECT on {table}"
                                       for table in outsider_tables)
                grantee = quote_identifier(outsider.application_id)
                catalogs = sorted({table_parts(table)[0] for table in outsider_tables})
                schemas = sorted({table_parts(table)[:2] for table in outsider_tables})
                manual_revokes = [
                    f"REVOKE SELECT ON TABLE {quote_table(table)} FROM {grantee}"
                    for table in outsider_tables
                ]
                manual_revokes.extend(
                    f"REVOKE USE SCHEMA ON SCHEMA {quote_identifier(catalog)}."
                    f"{quote_identifier(schema)} FROM {grantee}"
                    for catalog, schema in schemas
                )
                manual_revokes.extend(
                    f"REVOKE USE CATALOG ON CATALOG {quote_identifier(catalog)} FROM {grantee}"
                    for catalog in catalogs
                )
                detail = (
                    f"temporary outsider access was not fully revoked for service principal "
                    f"{outsider.application_id}: {privileges}. Remove it manually with REVOKE "
                    f"statements before retrying: {'; '.join(manual_revokes)}. Cause: {exc}"
                )
                print(f"  ERROR: {detail}", file=sys.stderr)
                if report is not None:
                    report.results.append(CheckResult(
                        "cleanup", outsider.application_id, FAIL, detail,
                        {"principal": outsider.application_id,
                         "tables_with_possible_access": outsider_tables},
                    ))
        if not keep_principals:
            for tier, p in principals.items():
                if tier == admin_tier:
                    continue
                verifier.deprovision_principal(p)
        for signum, handler in old_signal_handlers.items():
            signal.signal(signum, handler)
        if cleanup_error is not None and report is None:
            raise RuntimeError(
                f"verification cleanup failed after another error: {cleanup_error}"
            ) from cleanup_error


def pick_pairing_keys(
    verifier: "EffectiveAccessVerifier", principal: "TestPrincipal",
    checks: Sequence[ColumnMaskCheck], *,
    global_key: str = "", unsafe_by_table: Optional[Mapping[str, set[str]]] = None,
    prove_explicit: bool = True, salt: Optional[str] = None,
) -> dict[str, KeyPick]:
    """Pick and prove a row-pairing key for every table with a mask check.

    A check's own key_column (verify_key_columns or a VERIFY_SPEC) is the
    explicit choice for its table. Admin-only: no test principal is needed.
    Column metadata is read for every table; for an explicit key it only
    feeds the existence check and the sensitive-key warning, so a failed read
    there is ignored. Column tags come live and from the config.
    """
    config_tags: dict[str, list[tuple[str, str]]] = {}
    for item in (getattr(verifier, "mask_config", None) or {}).get("tag_assignments") or []:
        if _as_str(item.get("entity_type")) == "columns":
            config_tags.setdefault(_as_str(item.get("entity_name")).lower(), []).append(
                (_as_str(item.get("tag_key")), _as_str(item.get("tag_value"))))
    tables: dict[str, str] = {}
    explicit: dict[str, str] = {}
    for c in checks:
        tables.setdefault(c.table.lower(), c.table)
        if c.key_column and c.table.lower() not in explicit:
            explicit[c.table.lower()] = c.key_column
    picks: dict[str, KeyPick] = {}
    for lower, table in tables.items():
        facts, facts_error = None, ""
        try:
            facts = verifier.table_key_facts(principal, table)
        except Exception as exc:
            facts_error = str(exc)
        tags_by_column: dict[str, list[tuple[str, str]]] = {}
        for column, name, value in (facts.column_tags if facts else ()):
            tags_by_column.setdefault(column.lower(), []).append((name, value))
        for entity, tags in config_tags.items():
            if entity.rpartition(".")[0] == lower:
                tags_by_column.setdefault(entity.rpartition(".")[2], []).extend(tags)
        picks[lower] = pick_table_key(
            table, explicit=explicit.get(lower, ""), global_key=global_key,
            facts=facts, facts_error=facts_error,
            unsafe=(unsafe_by_table or {}).get(lower, ()),
            prove=lambda column, t=table: verifier.prove_pairing_key(principal, t, column, salt),
            prove_explicit=prove_explicit, tags_by_column=tags_by_column,
        )
    return picks


def print_key_picks(picks: Mapping[str, KeyPick]) -> None:
    """One line per table: the key column and why it was chosen (no values)."""
    for pick in picks.values():
        if pick.key:
            print(f"  Row-pairing key for {pick.table}: {pick.key} ({pick.source})")
        else:
            print(f"  Row-pairing key for {pick.table}: NONE — {pick.problem}")
        if pick.warning:
            print(f"  {pick.warning}")


def proven_keys_by_table(report: EffectiveAccessReport, spec: VerificationSpec) -> dict[str, str]:
    """Tables whose every mask check passed, with the key that paired them."""
    status = {r.target: r.status for r in report.results}
    by_table: dict[str, list[ColumnMaskCheck]] = {}
    for check in spec.column_masks:
        by_table.setdefault(check.table, []).append(check)
    return {
        table: checks[0].key_column
        for table, checks in by_table.items()
        if checks[0].key_column
        and all(c.key_column == checks[0].key_column and status.get(c.describe()) == PASS for c in checks)
    }


def check_pairing_keys(
    spec: VerificationSpec,
    auth_file: Path,
    *,
    warehouse_id: str = "",
    global_key: str = "",
    unsafe_by_table: Optional[Mapping[str, set[str]]] = None,
    admin_tier: str = DEFAULT_ADMIN_TIER,
) -> dict[str, KeyPick]:
    """Pick and prove every masked table's key as the admin only (no test
    principals, no grants); make release runs this before it applies access."""
    _require_live_enabled()
    auth = load_auth(auth_file)
    verifier = EffectiveAccessVerifier(auth, warehouse_id=warehouse_id, admin_only=True)
    verifier.mask_config = spec.mask_config
    verifier.resolve_warehouse()
    salt = os.environ.get(SAMPLE_SALT_ENV) or secrets.token_hex(8)
    print(f"  Key-check row sample salt: {salt} ({SAMPLE_SALT_ENV}={salt} repeats it)")
    admin = TestPrincipal(tier=admin_tier, display_name="admin-baseline",
                          application_id=auth["client_id"], client_secret=auth["client_secret"])
    return pick_pairing_keys(verifier, admin, spec.column_masks, global_key=global_key,
                             unsafe_by_table=unsafe_by_table, salt=salt)


def normalize_key_map(value: Any) -> dict[str, str]:
    """verify_key_columns as parsed from HCL: {"cat.sch.tbl": "col"}, blanks dropped."""
    if isinstance(value, list):
        value = value[0] if value else {}
    if not isinstance(value, Mapping):
        return {}
    out = {}
    for table, column in value.items():
        table, column = str(table).strip().strip('"').strip(), _as_str(column)
        if table and column:
            out[table] = column
    return out


def load_key_map(env_file: Optional[Path]) -> dict[str, str]:
    """The verify_key_columns setting of an env.auto.tfvars ({} when absent)."""
    if env_file is None or not Path(env_file).is_file():
        return {}
    import hcl2

    try:
        with open(env_file) as f:
            return normalize_key_map(hcl2.load(f).get("verify_key_columns"))
    except Exception as exc:
        raise ValueError(f"ERROR: cannot read verify_key_columns from {env_file}: {exc}") from exc


# ---------------------------------------------------------------------------
# Spec loading (pure)
# ---------------------------------------------------------------------------
def load_spec_from_file(path: Path) -> VerificationSpec:
    """Load a spec from a JSON file (schema mirrors the dataclasses)."""
    data = json.loads(Path(path).read_text())
    spec = VerificationSpec(principal_memberships={
        str(principal): tuple(map(str, memberships))
        for principal, memberships in (data.get("principal_memberships", {}) or {}).items()})
    for c in data.get("column_masks", []):
        spec.column_masks.append(ColumnMaskCheck(
            table=c["table"], column=c["column"], key_column=c.get("key_column", ""),
            masked_principals=tuple(c.get("masked_principals", [])),
            unmasked_principals=tuple(c.get("unmasked_principals", [])),
            policy_name=c.get("policy_name", ""),
            mask_function=c.get("mask_function", ""),
            expected_tiers=tuple(
                (str(principal), str(tier))
                for principal, tier in (c.get("expected_tiers", {}) or {}).items()),
            partial_function=c.get("partial_function", ""),
            full_function=c.get("full_function", ""),
            moving_principals=tuple(c.get("moving_principals", [])),
        ))
    for r in data.get("row_filters", []):
        spec.row_filters.append(RowFilterCheck(
            table=r["table"],
            restricted_principals=tuple(r.get("restricted_principals", [])),
            unrestricted_principals=tuple(r.get("unrestricted_principals", [])),
            policy_name=r.get("policy_name", ""),
        ))
    return spec


def load_spec_from_tfvars(
    tfvars_file: Path,
    account_tfvars_file: Optional[Path] = None,
    *,
    env_file: Optional[Path] = None,
    key_column: str = "",
    key_column_by_table: Optional[Mapping[str, str]] = None,
) -> VerificationSpec:
    """Derive a spec from a data_access abac.auto.tfvars (+ optional account tfvars)."""
    import hcl2

    with open(tfvars_file) as f:
        data = hcl2.load(f)
    env_data: dict[str, Any] = {}
    if env_file and Path(env_file).is_file():
        with open(env_file) as f:
            env_data = hcl2.load(f)
    fgac_policies = data.get("fgac_policies", []) or []
    tag_assignments = data.get("tag_assignments", []) or []

    groups: list[str] = []
    for src in (account_tfvars_file, tfvars_file):
        if not src:
            continue
        with open(src) as f:
            d = hcl2.load(f)
        g = d.get("groups")
        if isinstance(g, list) and g and isinstance(g[0], dict):
            for gd in g:
                groups.extend(gd.keys())
        elif isinstance(g, dict):
            groups.extend(g.keys())
    groups.extend(_as_list(env_data.get("access_tier_groups")))
    groups.extend(_as_list(env_data.get("raw_exempt_principals")))
    # Also treat any principal referenced by a policy as a known group.
    for pol in fgac_policies:
        groups.extend(_as_list(pol.get("to_principals")))
        groups.extend(_as_list(pol.get("except_principals")))

    spec = derive_spec_from_config(
        fgac_policies, tag_assignments, groups,
        key_column=key_column, key_column_by_table=key_column_by_table,
    )
    spec.mask_config = {"fgac_policies": fgac_policies, "tag_assignments": tag_assignments}
    governance = {**data, **env_data}
    if _as_str(governance.get("governance_mode")) == "deterministic":
        from deterministic_governance import resolve_precedence

        tiers = _as_list(governance.get("access_tier_groups"))
        raw_exempt = _as_list(governance.get("raw_exempt_principals"))
        invalid_names = [principal for principal in (*tiers, *raw_exempt)
                         if not isinstance(principal, str) or not principal.strip()]
        if invalid_names:
            raise ValueError(
                "ERROR: verify-access requires access_tier_groups and "
                "raw_exempt_principals to contain non-empty group names")
        invalid_exempt = [principal for principal in raw_exempt if "@" in principal]
        if invalid_exempt:
            raise ValueError(
                "ERROR: verify-access requires raw_exempt_principals to name account groups; "
                "user emails cannot be authenticated as "
                f"dedicated test identities: {invalid_exempt}")
        policies_by_column: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
        for policy in fgac_policies:
            if _as_str(policy.get("policy_type")) != "POLICY_TYPE_COLUMN_MASK":
                continue
            for column in resolve_columns_for_condition(
                    _as_str(policy.get("match_condition")), tag_assignments,
                    entity_type="columns"):
                policies_by_column.setdefault((column["table"], column["column"]), []).append(policy)
        deterministic_checks = []
        config_tags = {
            _as_str(item.get("entity_name")).lower(): _as_str(item.get("tag_value"))
            for item in tag_assignments
            if _as_str(item.get("entity_type")) == "columns"
            and _as_str(item.get("tag_key")) == "gr_treatment"
        }
        memberships: dict[str, tuple[str, ...]] = {group: (group,) for group in tiers}
        memberships.update({principal: (principal,) for principal in raw_exempt})
        memberships[OUT_OF_TIER_PRINCIPAL] = ()
        if len(tiers) >= 2:
            memberships[DUAL_TIER_PRINCIPAL] = (tiers[0], tiers[-1])

        def access_for(column: str, treatment: str, principal: str,
                       groups_for_principal: Sequence[str], *, deployer: bool = False) -> str:
            resolved = resolve_precedence(
                column=column, treatment=treatment, group=groups_for_principal,
                library_default="partial", access_tier_groups=tiers,
                column_overrides=governance.get("column_overrides") or {},
                treatment_versions=governance.get("treatment_versions") or {},
                tier_access_overrides=governance.get("tier_access_overrides") or {},
                principal=principal, deployer_principal=principal if deployer else None,
                raw_exempt_principals=raw_exempt)
            return str(getattr(resolved, "access", resolved))

        for check in spec.column_masks:
            policies = policies_by_column.get((check.table, check.column), [])
            full = next((_mask_function(p) for p in policies
                         if ALL_USERS_GROUP in _as_list(p.get("to_principals"))), "")
            partial = next((_mask_function(p) for p in policies
                            if ALL_USERS_GROUP not in _as_list(p.get("to_principals"))), "")
            treatment = config_tags.get(f"{check.table}.{check.column}".lower(), "")
            expectations = [
                (principal, access_for(f"{check.table}.{check.column}", treatment,
                                       principal, principal_memberships))
                for principal, principal_memberships in memberships.items()
            ]
            expectations.append((DEFAULT_ADMIN_TIER, access_for(
                f"{check.table}.{check.column}", treatment, DEFAULT_ADMIN_TIER, (), deployer=True)))
            # Never-raw treatments (secrets, CVV, and equivalent overrides)
            # may intentionally have only the all-users/full policy.  If the
            # resolved spec has no partial audience, the full function is the
            # correct executable stand-in regardless of tier count.
            if not partial and not any(access == "partial" for _, access in expectations):
                partial = full
            deterministic_checks.append(replace(
                check, expected_tiers=tuple(dict(expectations).items()),
                partial_function=partial, full_function=full))
        spec.column_masks = deterministic_checks
        spec.principal_memberships = memberships
    # The rule the live run applies: a key is refused when a column-mask policy
    # can match its configured tags (so it is itself a masked column), not for
    # carrying a tag no mask policy matches (e.g. class.* on an ID).
    problems: dict[tuple[str, str], str] = {}
    for check in spec.column_masks:
        sig = (check.table.lower(), check.key_column.lower())
        if not check.key_column or sig in problems:
            continue
        entity = f"{check.table}.{check.key_column}".lower()
        tags = [(_as_str(t.get("tag_key")), _as_str(t.get("tag_value"))) for t in tag_assignments
                if _as_str(t.get("entity_type")) == "columns"
                and _as_str(t.get("entity_name")).lower() == entity]
        why = key_tags_mask_problem(spec.mask_config, check.table, check.key_column, tags)
        problems[sig] = key_may_be_masked_message(check, f"it has {why}") if why else ""
    refused = [why for why in problems.values() if why]
    if refused:
        raise ValueError("ERROR: " + "; ".join(refused))
    return spec


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="verify_effective_access.py",
        description="Verify masking / row-filtering by effect using per-tier test principals.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--spec", type=Path, help="JSON spec file describing checks to run.")
    p.add_argument("--from-tfvars", type=Path,
                   help="Derive the spec from a data_access abac.auto.tfvars.")
    p.add_argument("--account-tfvars", type=Path,
                   help="Optional account abac.auto.tfvars (source of group names).")
    p.add_argument("--key-column", default="",
                   help="Row-pairing key for every masked table that has this column "
                        "(VERIFY_KEY_COLUMN / verify_key_column; optional).")
    p.add_argument("--env-file", type=Path,
                   help="env.auto.tfvars whose verify_key_columns map gives per-table keys.")
    p.add_argument("--auth-file", type=Path,
                   help="auth.auto.tfvars for the live workspace (required with --live).")
    p.add_argument("--warehouse-id", default="", help="SQL warehouse ID for queries.")
    p.add_argument("--live", action="store_true",
                   help=f"Run against a real workspace (also needs {LIVE_ENV_FLAG}=1).")
    p.add_argument("--check-keys-only", action="store_true",
                   help="As the admin only, pick and prove every masked table's row-pairing "
                        "key and exit (no test principals, no grants; make release runs it "
                        f"before applying). Needs --auth-file and {LIVE_ENV_FLAG}=1.")
    p.add_argument("--keep-principals", action="store_true",
                   help="Do not delete the provisioned test principals (debugging).")
    p.add_argument("--result-file", type=Path,
                   help="After a --live run, write a JSON summary (passed, the proven "
                        "row-pairing key per table) for tooling such as make rehearse/release.")
    p.add_argument("--print-spec", action="store_true",
                   help="Print the resolved spec and exit (no workspace needed).")
    p.add_argument("--require-mask-checks", action="store_true",
                   help="Fail instead of skipping a mask check whose table has no provable "
                        "row-pairing key (make release: production masking must be proven).")
    return p


def _load_spec_from_args(args, key_map: Mapping[str, str]) -> VerificationSpec:
    """The spec, each mask check keyed by its table's explicit key ("" = pick one live)."""
    by_table = {t.lower(): k for t, k in key_map.items()}
    if args.spec:
        if not args.spec.is_file():
            raise SystemExit(f"ERROR: verification spec not found: {args.spec}")
        spec = load_spec_from_file(args.spec)
        spec.column_masks = [replace(c, key_column=c.key_column or by_table.get(c.table.lower(), ""))
                             for c in spec.column_masks]
        return spec
    if args.from_tfvars:
        if not args.from_tfvars.is_file():
            raise SystemExit(
                f"ERROR: promoted data-access config not found: {args.from_tfvars}\n"
                "Run 'make promote' (or 'make apply', which promotes first) before "
                "verify-access-spec."
            )
        if args.account_tfvars and not args.account_tfvars.is_file():
            raise SystemExit(
                f"ERROR: promoted account config not found: {args.account_tfvars}\n"
                "Run 'make promote' (or 'make apply', which promotes first) before "
                "verify-access-spec."
            )
        # Derived with the global key too, so a key tagged sensitive is refused
        # up front; it then applies only to tables that have it (picked live).
        spec = load_spec_from_tfvars(
            args.from_tfvars, args.account_tfvars, key_column=args.key_column,
            key_column_by_table=key_map, env_file=args.env_file,
        )
        spec.column_masks = [replace(c, key_column=by_table.get(c.table.lower(), ""))
                             for c in spec.column_masks]
        return spec
    raise SystemExit("Provide either --spec or --from-tfvars.")


def write_result_file(path: Optional[Path], report: EffectiveAccessReport, spec: Any = None) -> None:
    """Machine-readable proof of a live run: the key proven for each table.

    ``mask_keys_proven_by_table`` lists only tables whose every mask check
    passed. ``mask_keys_complete`` is true only when the run passed and every
    masked table (``masked_tables``) has a proven key; only then does make save
    the map as verify_key_columns.
    """
    if path is None:
        return
    checks = {check.describe(): check for check in getattr(spec, "column_masks", [])}
    by_key: dict[str, int] = {}
    for r in report.results:
        check = checks.get(r.target)
        key = check and (report.pairing_keys.get(check.table) or check.key_column)
        if r.kind == "column-mask" and r.status == PASS and key:
            by_key[key] = by_key.get(key, 0) + 1
    masked_tables = sorted({c.table for c in getattr(spec, "column_masks", [])})
    payload = {
        "passed": report.passed,
        "masked_tables": masked_tables,
        "mask_keys_complete": report.passed and all(t in report.pairing_keys for t in masked_tables),
        "mask_checks_passed": sum(by_key.values()),
        "mask_checks_passed_by_key": by_key,
        "mask_keys_proven_by_table": dict(sorted(report.pairing_keys.items())),
        "row_filter_checks_passed": sum(
            1 for r in report.results if r.kind == "row-filter" and r.status == PASS),
    }
    cleanup_failures = [
        {"target": r.target, "detail": r.detail, "evidence": r.evidence}
        for r in report.results if r.kind == "cleanup" and r.status == FAIL
    ]
    if cleanup_failures:
        payload["cleanup_failures"] = cleanup_failures
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n")
    os.replace(tmp, path)


def _check_keys_only(args, spec: VerificationSpec, unsafe: Mapping[str, set[str]]) -> int:
    if not spec.column_masks:
        print("Row-pairing keys: no masked tables, so no key is needed.")
        return 0
    if not args.auth_file:
        raise SystemExit("--auth-file is required with --check-keys-only.")
    print("=== Row-pairing keys (admin check before any access is granted) ===")
    picks = check_pairing_keys(spec, args.auth_file, warehouse_id=args.warehouse_id,
                               global_key=args.key_column, unsafe_by_table=unsafe)
    print_key_picks(picks)
    missing = [p for p in picks.values() if not p.key]
    if missing:
        print(
            f"ERROR: {len(missing)} masked table(s) have no provable row-pairing key, so their "
            "masking could not be verified:\n" + "\n".join(f"  - {p.problem}" for p in missing),
            file=sys.stderr,
        )
        return 2
    print(f"All {len(picks)} masked table(s) have a proven row-pairing key.")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.result_file is not None:
        args.result_file.unlink(missing_ok=True)
    try:
        spec = _load_spec_from_args(args, load_key_map(args.env_file))
        validate_spec_identifiers(spec)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    sensitive_keys = sorted(
        f"{check.table}.{check.key_column}"
        for check in spec.column_masks
        if check.key_column and check.key_column.lower() == check.column.lower()
    )
    if sensitive_keys:
        raise SystemExit(
            "ERROR: verify key column is itself classified sensitive/masked: "
            + ", ".join(sensitive_keys)
            + ". Configure a non-sensitive stable row identifier."
        )

    if args.require_mask_checks and args.from_tfvars:
        # Every tagged masked column must have a check; a mask that yields
        # none (no concrete masked tier) must not leave the run "passing".
        try:
            required = required_mask_columns_from_tfvars(args.from_tfvars)
        except ValueError as exc:
            print(f"ERROR: {exc}. Refusing to report success.", file=sys.stderr)
            return 2
        unchecked = unchecked_mask_columns(required, spec.column_masks, keyed_only=False)
        if unchecked:
            print(
                f"ERROR: {len(unchecked)} masked column(s) produce no effective-access check, so their "
                f"masking would NOT be verified: {', '.join(unchecked)}. Their policies have no "
                "concrete masked group to test (e.g. 'account users' with no groups in the account "
                "config). Refusing to report success.",
                file=sys.stderr,
            )
            return 2

    unsafe = unsafe_key_columns(spec.column_masks, spec.mask_config)
    if args.check_keys_only:
        return _check_keys_only(args, spec, unsafe)

    if spec.is_empty():
        # (issue 4) Deriving zero checks means we would verify nothing. That is
        # never a success — a passing gate here would be a false success.
        print(
            "ERROR: no effective-access checks were derived from the given "
            "spec/config — nothing would be verified. This usually means the "
            "config has no column-mask / row-filter FGAC policies, or the tag "
            "conditions matched no tag assignments. Refusing to report success.",
            file=sys.stderr,
        )
        return 2

    if args.print_spec or not args.live:
        print("Resolved effective-access spec:")
        for c in spec.column_masks:
            key = repr(c.key_column) if c.key_column else (
                f"auto ({args.key_column!r} if the table has it, else its primary key "
                "or an id-like column; picked and proven live)" if args.key_column.strip()
                else "auto (primary key or an id-like column; picked and proven live)")
            print(f"  [column-mask] {c.table}.{c.column} key={key} "
                  f"masked={list(c.masked_principals)} "
                  f"unmasked={list(c.unmasked_principals)}")
        for r in spec.row_filters:
            print(f"  [row-filter]  {r.table} restricted={list(r.restricted_principals)} "
                  f"unrestricted={list(r.unrestricted_principals)}")
        if not args.live:
            print(f"\n(dry run — pass --live and set {LIVE_ENV_FLAG}=1 to execute against a workspace.)")
            return 0

    if not args.auth_file:
        raise SystemExit("--auth-file is required with --live.")

    report = verify_effective_access_live(
        spec, args.auth_file,
        warehouse_id=args.warehouse_id, keep_principals=args.keep_principals,
        global_key=args.key_column, unsafe_by_table=unsafe,
    )
    print(report.summary())
    write_result_file(args.result_file, report, spec)
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
