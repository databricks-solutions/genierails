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
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

# ---------------------------------------------------------------------------
# Environment / flag guard
# ---------------------------------------------------------------------------
# The live workspace path is gated on BOTH an explicit CLI flag and this env
# var, so importing this module or running the unit suite never provisions
# principals or hits a cluster.
LIVE_ENV_FLAG = "GENIERAILS_LIVE_VERIFY"

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
        lines.append("-" * 60)
        total = sum(c.values())
        blocking = c[FAIL] + c[INCONCLUSIVE]
        if self.passed and self.not_verified:
            lines.append(
                f"  RESULT: ROW FILTERS EFFECTIVE — {len(self.not_verified)} mask "
                f"check(s) NOT VERIFIED (no key column; {c[PASS]} passed / {total})"
            )
        elif self.passed:
            lines.append(f"  RESULT: ALL EFFECTIVE ({c[PASS]} passed / {total})")
        elif not self.results and self.not_verified:
            lines.append(
                f"  RESULT: MASKING NOT VERIFIED — {len(self.not_verified)} mask "
                "check(s) skipped (no key column)"
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
    key_by_table = dict(key_column_by_table or {})
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
                kc = key_by_table.get(c["table"], key_column)
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
#   column_values: {(table, column): {principal: {row_key: value}}}
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


def evaluate_column_mask_check(
    check: ColumnMaskCheck,
    values_by_principal: Mapping[str, Mapping[Any, Any]],
    errors_by_principal: Optional[Mapping[str, str]] = None,
) -> CheckResult:
    """Prove a mask takes effect: masked tiers get a masked value, unmasked tiers
    get the *raw* value.

    ``values_by_principal`` maps ``principal -> {row_key: value}`` for this
    (table, column); ``errors_by_principal`` maps ``principal -> error string``
    for principals whose query failed.

    A PASS is only returned when ALL of the following hold — otherwise the check
    FAILs (proven violation) or is INCONCLUSIVE (could not be verified). Neither
    passes the gate:

    * No involved principal's query failed.
    * At least one unmasked principal returned rows, and all unmasked principals
      that returned rows **agree** on each shared row's value — that agreed value
      is the raw ground truth. Disagreement means one of them is not actually
      unmasked → FAIL.
    * At least one shared row has a **non-null / non-empty** raw value — otherwise
      the dataset cannot demonstrate masking → INCONCLUSIVE.
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

    unmasked_present = [p for p in check.unmasked_principals if values_by_principal.get(p)]
    masked_present = [p for p in check.masked_principals if values_by_principal.get(p)]

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
    conflicts: list[dict[str, Any]] = []
    for up in unmasked_present:
        for row_key, val in values_by_principal[up].items():
            nval = _normalize_value(val)
            if row_key not in raw_by_row:
                raw_by_row[row_key] = nval
            elif raw_by_row[row_key] != nval:
                conflicts.append({
                    "row_key": row_key, "principal": up,
                    "value": nval, "other": raw_by_row[row_key],
                })
    if conflicts:
        return CheckResult(
            "column-mask", target, FAIL,
            ("unmasked principals disagree on the raw value — at least one is not "
             f"actually unmasked, so masking cannot be trusted. Sample: {conflicts[:5]}"),
            {"conflicts": conflicts},
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

    leaks: list[dict[str, Any]] = []
    masked_ok = 0
    per_principal_compared: dict[str, int] = {}
    for mp in masked_present:
        compared_here = 0
        for row_key, val in values_by_principal[mp].items():
            if row_key not in maskable_rows:
                continue
            compared_here += 1
            masked_val = _normalize_value(val)
            raw_val = raw_by_row[row_key]
            if masked_val == raw_val:
                leaks.append({
                    "principal": mp, "row_key": row_key,
                    "value": masked_val, "raw": raw_val,
                })
            else:
                masked_ok += 1
        per_principal_compared[mp] = compared_here

    if leaks:
        return CheckResult(
            "column-mask", target, FAIL,
            (f"{len(leaks)} row(s) leaked the raw value to a masked principal "
             f"(mask not effective). Sample: {leaks[:5]}"),
            {"leaks": leaks, "masked_ok": masked_ok},
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
) -> EffectiveAccessReport:
    """Evaluate every check in the spec against collected observations (pure)."""
    report = EffectiveAccessReport()
    for check in spec.column_masks:
        vals = column_values.get((check.table, check.column), {})
        errs = (column_errors or {}).get((check.table, check.column), {})
        report.add(evaluate_column_mask_check(check, vals, errs))
    for check in spec.row_filters:
        counts = row_counts.get(check.table, {})
        errs = (row_errors or {}).get(check.table, {})
        report.add(evaluate_row_filter_check(check, counts, errs))
    return report


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
                 name_prefix: str = "genierails-verify"):
        _require_live_enabled()
        self.auth = dict(auth)
        self.warehouse_id = warehouse_id
        self.name_prefix = name_prefix
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
    def run_query(self, ws, sql: str) -> list[list[Any]]:
        self._guard()
        from databricks.sdk.service.sql import StatementState

        stmt = ws.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id, statement=sql.strip(), wait_timeout="50s",
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
    ) -> dict[Any, Any]:
        """Return {row_key: column_value} for a principal.

        Raises on any failure — a failed/denied query is a verification failure,
        not an empty (and falsely-passing) result. The caller records the error.
        """
        self._guard()
        if not check.key_column:
            raise ValueError(
                f"no key_column configured for {check.table}.{check.column}; "
                "cannot pair rows across principals"
            )
        ws = self._ws_for(principal)
        sql = (
            f"SELECT `{check.key_column}`, `{check.column}` "
            f"FROM {check.table} ORDER BY `{check.key_column}` LIMIT {int(limit)}"
        )
        try:
            rows = self.run_query(ws, sql)
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
        return {r[0]: r[1] for r in rows if r}

    def collect_row_count(self, principal: TestPrincipal, table: str) -> int:
        """Return the row count a principal sees. Raises on query failure."""
        self._guard()
        ws = self._ws_for(principal)
        rows = self.run_query(ws, f"SELECT COUNT(*) FROM {table}")
        return int(rows[0][0]) if rows else 0


def verify_effective_access_live(
    spec: VerificationSpec,
    auth_file: Path,
    *,
    warehouse_id: str = "",
    keep_principals: bool = False,
    admin_tier: str = DEFAULT_ADMIN_TIER,
) -> EffectiveAccessReport:
    """Provision per-tier principals, run queries as each, and evaluate effects.

    Guarded: raises unless ``GENIERAILS_LIVE_VERIFY=1``.
    """
    _require_live_enabled()
    auth = load_auth(auth_file)
    verifier = EffectiveAccessVerifier(auth, warehouse_id=warehouse_id)
    verifier.resolve_warehouse()

    principals: dict[str, TestPrincipal] = {}
    # The admin baseline uses the admin credentials directly (raw values).
    admin_principal = TestPrincipal(
        tier=admin_tier,
        display_name="admin-baseline",
        application_id=auth["client_id"],
        client_secret=auth["client_secret"],
    )
    principals[admin_tier] = admin_principal

    try:
        for tier in sorted(spec.principals):
            print(f"  Provisioning test principal for tier: {tier}")
            principal = verifier.provision_principal(tier)
            principals[tier] = principal
            print(f"  Granting warehouse CAN_USE to test principal: {tier}")
            verifier.grant_warehouse_use(principal)

        # Newly-added group membership can take a short while to propagate.
        time.sleep(int(os.environ.get("GENIERAILS_VERIFY_PROPAGATION_SLEEP", "10")))

        column_values: dict[tuple, dict[str, dict[Any, Any]]] = {}
        column_errors: dict[tuple, dict[str, str]] = {}
        for check in spec.column_masks:
            per_principal: dict[str, dict[Any, Any]] = {}
            per_errors: dict[str, str] = {}
            for tier in set(check.masked_principals) | set(check.unmasked_principals):
                p = principals.get(tier)
                if p is None:
                    # A tier in the check we could not provision leaves a gap the
                    # evaluator must treat as a failure, not silently ignore.
                    per_errors[tier] = "principal was not provisioned"
                    continue
                try:
                    per_principal[tier] = verifier.collect_column_values(p, check)
                except Exception as exc:
                    per_errors[tier] = str(exc)
                    print(f"    ({tier}) query FAILED for {check.table}.{check.column}: {exc}")
            column_values[(check.table, check.column)] = per_principal
            column_errors[(check.table, check.column)] = per_errors

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

        return evaluate_effective_access(
            spec, column_values, row_counts, column_errors, row_errors,
        )
    finally:
        if not keep_principals:
            for tier, p in principals.items():
                if tier == admin_tier:
                    continue
                verifier.deprovision_principal(p)


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
    tagged_columns = {
        _as_str(item.get("entity_name")).lower()
        for item in tag_assignments
        if _as_str(item.get("entity_type")) == "columns"
    }
    sensitive_keys = sorted({
        f"{check.table}.{check.key_column}"
        for check in spec.column_masks
        if check.key_column
        and f"{check.table}.{check.key_column}".lower() in tagged_columns
    })
    if sensitive_keys:
        raise ValueError(
            "ERROR: verify key column is itself classified sensitive/masked: "
            + ", ".join(sensitive_keys)
            + ". Configure a non-sensitive stable row identifier."
        )
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
                   help="Primary-key column used to pair rows across principals.")
    p.add_argument("--auth-file", type=Path,
                   help="auth.auto.tfvars for the live workspace (required with --live).")
    p.add_argument("--warehouse-id", default="", help="SQL warehouse ID for queries.")
    p.add_argument("--live", action="store_true",
                   help=f"Run against a real workspace (also needs {LIVE_ENV_FLAG}=1).")
    p.add_argument("--keep-principals", action="store_true",
                   help="Do not delete the provisioned test principals (debugging).")
    p.add_argument("--result-file", type=Path,
                   help="After a --live run, write a JSON summary (passed, mask checks "
                        "passed per key column) for tooling such as make rehearse/release.")
    p.add_argument("--print-spec", action="store_true",
                   help="Print the resolved spec and exit (no workspace needed).")
    return p


def _load_spec_from_args(args) -> VerificationSpec:
    if args.spec:
        if not args.spec.is_file():
            raise SystemExit(f"ERROR: verification spec not found: {args.spec}")
        return load_spec_from_file(args.spec)
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
        return load_spec_from_tfvars(
            args.from_tfvars, args.account_tfvars, key_column=args.key_column,
        )
    raise SystemExit("Provide either --spec or --from-tfvars.")


def write_result_file(path: Optional[Path], report: EffectiveAccessReport, spec: Any) -> None:
    """Machine-readable proof of a live run: which key columns paired a passing mask check."""
    if path is None:
        return
    key_by_target = {check.describe(): check.key_column for check in spec.column_masks}
    by_key: dict[str, int] = {}
    for r in report.results:
        if r.kind == "column-mask" and r.status == PASS and key_by_target.get(r.target):
            key = key_by_target[r.target]
            by_key[key] = by_key.get(key, 0) + 1
    payload = {
        "passed": report.passed,
        "mask_checks_passed": sum(by_key.values()),
        "mask_checks_passed_by_key": by_key,
        "row_filter_checks_passed": sum(
            1 for r in report.results if r.kind == "row-filter" and r.status == PASS),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n")
    os.replace(tmp, path)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.result_file is not None:
        args.result_file.unlink(missing_ok=True)
    try:
        spec = _load_spec_from_args(args)
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

    missing_key_checks = [check for check in spec.column_masks if not check.key_column]
    if missing_key_checks:
        print(
            "SKIP: no verification key column is configured; skipping "
            f"{len(missing_key_checks)} mask comparison(s) that require row pairing. "
            "Row-filter checks will still run. Configure verify_key_column in "
            "env.auto.tfvars or pass VERIFY_KEY_COLUMN=<col> to enable them."
        )

    if args.print_spec or not args.live:
        print("Resolved effective-access spec:")
        for c in spec.column_masks:
            suffix = " — skipped (no key column)" if not c.key_column else ""
            print(f"  [column-mask] {c.table}.{c.column} key={c.key_column!r} "
                  f"masked={list(c.masked_principals)} "
                  f"unmasked={list(c.unmasked_principals)}{suffix}")
        for r in spec.row_filters:
            print(f"  [row-filter]  {r.table} restricted={list(r.restricted_principals)} "
                  f"unrestricted={list(r.unrestricted_principals)}")
        if not args.live:
            if missing_key_checks:
                print(
                    "\nWARNING: masking NOT verified for skipped checks; "
                    "configure a key column to enable row pairing."
                )
            print(f"\n(dry run — pass --live and set {LIVE_ENV_FLAG}=1 to execute against a workspace.)")
            return 0

    skipped_results = [
        CheckResult(
            "column-mask", check.describe(), INCONCLUSIVE,
            "masking NOT verified because no key column was configured for row pairing",
        )
        for check in missing_key_checks
    ]
    spec.column_masks = [check for check in spec.column_masks if check.key_column]
    if spec.is_empty():
        report = EffectiveAccessReport(not_verified=skipped_results)
        print(report.summary())
        write_result_file(args.result_file, report, spec)
        return 0

    if not args.auth_file:
        raise SystemExit("--auth-file is required with --live.")

    report = verify_effective_access_live(
        spec, args.auth_file,
        warehouse_id=args.warehouse_id, keep_principals=args.keep_principals,
    )
    report.not_verified.extend(skipped_results)
    print(report.summary())
    write_result_file(args.result_file, report, spec)
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
