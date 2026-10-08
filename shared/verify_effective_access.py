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
import re
import secrets
import sys
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

    @property
    def principals(self) -> set[str]:
        """Every principal referenced by any check (the tiers we must provision)."""
        out: set[str] = set()
        for c in self.column_masks:
            out.update(c.masked_principals)
            out.update(c.unmasked_principals)
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
) -> CheckResult:
    """Prove a mask takes effect: masked tiers get a masked value, unmasked tiers
    get the *raw* value.

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

    # (issue 2) The raw baseline must contain at least one maskable value.
    maskable_rows = {k for k, v in raw_by_row.items() if v not in (None, "")}
    if not maskable_rows:
        return CheckResult(
            "column-mask", target, INCONCLUSIVE,
            "every raw value in the sample is NULL/empty — the dataset cannot "
            "demonstrate that masking changes anything",
            {"raw_rows": len(raw_by_row)},
        )

    leaks: dict[str, int] = {}
    masked_ok = 0
    per_principal_compared: dict[str, int] = {}
    for mp in masked_present:
        compared_here = 0
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

    return CheckResult(
        "column-mask", target, PASS,
        (f"masked principal(s) {masked_present} see a masked value that differs "
         f"from the raw value seen by {unmasked_present} across {masked_ok} row(s)"),
        {"masked_ok": masked_ok, "per_principal_compared": per_principal_compared},
    )


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
) -> EffectiveAccessReport:
    """Evaluate every check in the spec against collected observations (pure)."""
    report = EffectiveAccessReport()
    for check in spec.column_masks:
        sig = (check.table, check.column)
        vals = column_values.get(sig, {})
        errs = (column_errors or {}).get(sig, {})
        report.add(evaluate_column_mask_check(
            check, vals, errs,
            pairing_problem=(pairing_problems or {}).get(sig, ""),
            unpaired=(unpaired or {}).get(sig),
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


@dataclass
class KeyPick:
    """The row-pairing key chosen for one table, or why there is none."""
    table: str
    key: str = ""
    source: str = ""
    problem: str = ""

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


def key_candidates(table: str, facts: TableKeyFacts, unsafe: Iterable[str] = ()) -> list[tuple[str, str]]:
    """Auto-pick candidates for ``table``, best first, as (column, source) (pure).

    ``unsafe`` names the columns a column mask applies to. Tags are not judged
    here: each candidate then goes through the same key checks as any key
    (key_mask_metadata), so a tag only disqualifies it if a mask matches it.
    """
    excluded = {c.lower() for c in unsafe}
    types = {name.lower(): data_type for name, data_type in facts.columns}
    names = {name.lower(): name for name, _ in facts.columns}

    def usable(column: str) -> bool:
        # Names come from live metadata and go into admin SQL: plain identifiers only.
        return (bool(_IDENT_RE.fullmatch(column)) and column.lower() not in excluded
                and _type_rank(types.get(column.lower(), "")) is not None)

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

    def explicit_pick(column: str, source: str) -> KeyPick:
        if column.lower() in unsafe:
            problem = f"row-pairing key {column} ({source}) is a masked column on {table}"
        elif facts is not None and column.lower() not in {n.lower() for n, _ in facts.columns}:
            problem = f"row-pairing key {column} ({source}) does not exist on {table}"
        else:
            problem = prove(column) if prove_explicit else ""
        return KeyPick(table, "" if problem else column, source, problem)

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
    for column, source in key_candidates(table, facts, unsafe):
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
    return sorted(set(check.masked_principals) | set(check.unmasked_principals) | {admin_tier})


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
    def provision_principal(self, tier: str) -> TestPrincipal:
        """Create (or reuse) a service principal and add it to the tier group."""
        self._guard()
        from databricks.sdk.service import iam

        display_name = f"{self.name_prefix}-{tier}"
        a = self.account

        existing = next(
            (sp for sp in a.service_principals.list(filter=f'displayName eq "{display_name}"')),
            None,
        )
        if existing is None:
            sp = a.service_principals.create(display_name=display_name, active=True)
        else:
            sp = existing

        # Mint an OAuth secret so the principal can authenticate on its own.
        secret = a.service_principal_secrets.create(service_principal_id=int(sp.id))

        # Add the SP to the tier's account group so UC evaluates its policies.
        group = next(
            (g for g in a.groups.list(filter=f'displayName eq "{tier}"')),
            None,
        )
        if group is None:
            raise RuntimeError(f"Tier group not found: {tier!r} (apply the account layer first)")
        if not any((m.value == sp.id) for m in (group.members or [])):
            a.groups.patch(
                group.id,
                operations=[
                    iam.Patch(
                        op=iam.PatchOp.ADD,
                        path="members",
                        value=[{"value": sp.id}],
                    )
                ],
                schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
            )

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
        """Columns and PRIMARY KEY of ``table`` (read as the admin). Tags are
        judged per candidate by key_mask_metadata, not here."""
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
        return TableKeyFacts(columns, primary_key)

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
    require_keys: bool = False,
) -> EffectiveAccessReport:
    """Provision per-tier principals, run queries as each, and evaluate effects.

    First, as the admin only, every masked table's row-pairing key is picked
    and proven (see pick_table_key). A table with no provable key is NOT
    VERIFIED, or blocking with ``require_keys`` (make release) or when an
    explicit key failed; the rest are verified with their keys.

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
    keyed, blocking, not_verified = [], [], []
    for check in spec.column_masks:
        pick = picks[check.table.lower()]
        if pick.key:
            keyed.append(replace(check, key_column=check.key_column or pick.key))
            continue
        result = CheckResult("column-mask", check.describe(), INCONCLUSIVE, pick.problem,
                             {"key_source": pick.source})
        (blocking if require_keys or pick.explicit else not_verified).append(result)
    spec = VerificationSpec(column_masks=keyed, row_filters=list(spec.row_filters),
                            mask_config=spec.mask_config)
    if spec.is_empty():
        return EffectiveAccessReport(results=blocking, not_verified=not_verified)

    try:
        for tier in sorted(spec.principals):
            print(f"  Provisioning test principal for tier: {tier}")
            principal = verifier.provision_principal(tier)
            principals[tier] = principal
            print(f"  Granting warehouse CAN_USE to test principal: {tier}")
            verifier.grant_warehouse_use(principal)

        # Newly-added group membership can take a short while to propagate.
        time.sleep(int(os.environ.get("GENIERAILS_VERIFY_PROPAGATION_SLEEP", "10")))
        if spec.column_masks:
            print(f"  Mask-check row sample salt: {salt} ({SAMPLE_SALT_ENV}={salt} repeats it)")

        column_values: dict[tuple, dict[str, list[tuple[Any, Any]]]] = {}
        column_errors: dict[tuple, dict[str, str]] = {}
        pairing_problems: dict[tuple, str] = {}
        key_proofs: dict[tuple, str] = {}   # once per (table, key, read keys)
        key_metadata: dict[tuple, str] = {}  # once per (table, key)
        for check in spec.column_masks:
            sig = (check.table, check.column)
            per_principal: dict[str, list[tuple[Any, Any]]] = {}
            per_errors: dict[str, str] = {}
            involved = set(check.masked_principals) | set(check.unmasked_principals)
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
            if per_errors or sig in pairing_problems:
                continue
            problem = next((why for why in (sample_key_problem(check, t, samples[t]) for t in tiers)
                            if why), "")
            keys = list(dict.fromkeys(k for t in tiers for k, _ in samples[t]))
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
                                t for t in tiers if t != admin_tier and samples[t]
                                and verifier.count_rows_with_keys(
                                    admin_principal, check, [k for k, _ in samples[t]],
                                ) < len(samples[t])
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
            pairing_problems=pairing_problems,
        )
        report.results.extend(blocking)
        report.not_verified.extend(not_verified)
        report.pairing_keys = proven_keys_by_table(report, spec)
        if spec.column_masks:
            report.sample_note = (
                f"checked {SAMPLE_ROWS} sampled rows per tier per masked column (spread by salt "
                f"{salt}; rerun with {SAMPLE_SALT_ENV}={salt} to repeat) — a bounded sample, "
                "not every row")
        return report
    finally:
        if not keep_principals:
            for tier, p in principals.items():
                if tier == admin_tier:
                    continue
                verifier.deprovision_principal(p)


def pick_pairing_keys(
    verifier: "EffectiveAccessVerifier", principal: "TestPrincipal",
    checks: Sequence[ColumnMaskCheck], *,
    global_key: str = "", unsafe_by_table: Optional[Mapping[str, set[str]]] = None,
    prove_explicit: bool = True, salt: Optional[str] = None,
) -> dict[str, KeyPick]:
    """Pick and prove a row-pairing key for every table with a mask check.

    A check's own key_column (verify_key_columns or a VERIFY_SPEC) is the
    explicit choice for its table. Admin-only: no test principal is needed,
    and the verifier is only queried for a table that has no explicit key.
    """
    tables: dict[str, str] = {}
    explicit: dict[str, str] = {}
    for c in checks:
        tables.setdefault(c.table.lower(), c.table)
        if c.key_column and c.table.lower() not in explicit:
            explicit[c.table.lower()] = c.key_column
    picks: dict[str, KeyPick] = {}
    for lower, table in tables.items():
        facts, facts_error = None, ""
        if lower not in explicit:
            try:
                facts = verifier.table_key_facts(principal, table)
            except Exception as exc:
                facts_error = str(exc)
        picks[lower] = pick_table_key(
            table, explicit=explicit.get(lower, ""), global_key=global_key,
            facts=facts, facts_error=facts_error,
            unsafe=(unsafe_by_table or {}).get(lower, ()),
            prove=lambda column, t=table: verifier.prove_pairing_key(principal, t, column, salt),
            prove_explicit=prove_explicit,
        )
    return picks


def print_key_picks(picks: Mapping[str, KeyPick]) -> None:
    """One line per table: the key column and why it was chosen (no values)."""
    for pick in picks.values():
        if pick.key:
            print(f"  Row-pairing key for {pick.table}: {pick.key} ({pick.source})")
        else:
            print(f"  Row-pairing key for {pick.table}: NONE — {pick.problem}")


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
    spec = VerificationSpec()
    for c in data.get("column_masks", []):
        spec.column_masks.append(ColumnMaskCheck(
            table=c["table"], column=c["column"], key_column=c.get("key_column", ""),
            masked_principals=tuple(c.get("masked_principals", [])),
            unmasked_principals=tuple(c.get("unmasked_principals", [])),
            policy_name=c.get("policy_name", ""),
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
    key_column: str = "",
    key_column_by_table: Optional[Mapping[str, str]] = None,
) -> VerificationSpec:
    """Derive a spec from a data_access abac.auto.tfvars (+ optional account tfvars)."""
    import hcl2

    with open(tfvars_file) as f:
        data = hcl2.load(f)
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
    # Also treat any principal referenced by a policy as a known group.
    for pol in fgac_policies:
        groups.extend(_as_list(pol.get("to_principals")))
        groups.extend(_as_list(pol.get("except_principals")))

    spec = derive_spec_from_config(
        fgac_policies, tag_assignments, groups,
        key_column=key_column, key_column_by_table=key_column_by_table,
    )
    spec.mask_config = {"fgac_policies": fgac_policies, "tag_assignments": tag_assignments}
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
            key_column_by_table=key_map,
        )
        spec.column_masks = [replace(c, key_column=by_table.get(c.table.lower(), ""))
                             for c in spec.column_masks]
        return spec
    raise SystemExit("Provide either --spec or --from-tfvars.")


def write_result_file(path: Optional[Path], report: EffectiveAccessReport, spec: Any = None) -> None:
    """Machine-readable proof of a live run: the key proven for each table.

    ``mask_keys_proven_by_table`` lists only tables whose every mask check
    passed; make saves it as verify_key_columns after a passing run.
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
    payload = {
        "passed": report.passed,
        "mask_checks_passed": sum(by_key.values()),
        "mask_checks_passed_by_key": by_key,
        "mask_keys_proven_by_table": dict(sorted(report.pairing_keys.items())),
        "row_filter_checks_passed": sum(
            1 for r in report.results if r.kind == "row-filter" and r.status == PASS),
    }
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
        require_keys=args.require_mask_checks,
    )
    print(report.summary())
    write_result_file(args.result_file, report, spec)
    if report.passed:
        return 0
    # Only unkeyable masks and nothing else to check: NOT VERIFIED, as before
    # (make release passes --require-mask-checks, which makes these blocking).
    return 0 if not report.results and report.not_verified else 1


if __name__ == "__main__":
    sys.exit(main())
