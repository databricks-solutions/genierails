#!/usr/bin/env python3
"""Build versioned JSON and Markdown compliance evidence for a GenieRails footprint.

Run directly, offline mode is the default: it uses local configuration only and
never imports the Databricks SDK. GENIERAILS_EVIDENCE_INTEGRATION=1 collects
live Unity Catalog state through a Databricks SQL warehouse; ``make evidence``
sets it unless OFFLINE=1.

The report is a runtime file, never committed: it is written (0600, in a 0700
directory) to the env's state dir, ``$GENIERAILS_STATE_DIR/<cloud>/<env>/evidence/``
or ``<cloud>/.genierails-state/<env>/evidence/`` (git-ignored), where the
release-gate receipts live. A folder that resolves (symlinks followed) inside any
git work tree that doesn't ignore it is refused (exit 2, nothing written). Nothing goes under envs/<env>/, so the clean
checkout and the env/generated digests that maintain and release-plan check
never see it.

Live mode reads two kinds of mask: table-attached masks and row filters from
information_schema (legacy envs), and the ABAC policies the policies API lists
as effective on each governed table (catalog, schema and table level), which is
how deterministic governance masks columns. A column carries an ABAC policy
only when the mapping is fully evaluated: the table's tags satisfy its
when_condition and, for a column mask, the column's tags satisfy the match
condition bound to its on_column. A policy it can't evaluate exactly is listed
under abac_policies_not_evaluated (listing status "incomplete"), never shown as
protection. A listing that fails (policies or table tags) stops the run
(exit 2) rather than writing a report that claims no protection.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from scripts.footprint import resolve_footprint

SCHEMA_VERSION = "1.0"
INTEGRATION_ENV = "GENIERAILS_EVIDENCE_INTEGRATION"
COLUMN_MASK_TYPES = {"COLUMN_MASK", "POLICY_TYPE_COLUMN_MASK"}
ROW_FILTER_TYPES = {"ROW_FILTER", "POLICY_TYPE_ROW_FILTER"}
# What may remain of a match condition once its tag references are removed for
# "any reference matches" to be its exact meaning: OR, parentheses, whitespace.
_TAG_REF = re.compile(r"hasTagValue\(\s*'[^']+'\s*,\s*'[^']+'\s*\)|hasTag\(\s*'[^']+'\s*\)")
_ONLY_OR = re.compile(r"(?i)[\s()]*(?:\bOR\b[\s()]*)*")


_SECRETISH = re.compile(
    r"(?i)(authorization|bearer|token|secret|password|cookie|api[_-]?key)(\S*\s*[:=]?\s*)\S+"
    r"|\b(?:dapi|dose|dkea)[0-9a-f-]+\b|\b[A-Za-z0-9+/_\-]{32,}={0,2}")


class EvidenceUnavailable(RuntimeError):
    """Live protection state couldn't be read completely, so no report is written."""


def _safe_error(exc: BaseException, limit: int = 200) -> str:
    """The exception type and a redacted, length-capped message (never headers or tokens verbatim)."""
    message = " ".join(str(exc).split())
    message = _SECRETISH.sub(lambda m: f"{m.group(1)}{m.group(2)}[redacted]" if m.group(1) else "[redacted]", message)
    if len(message) > limit:
        message = message[:limit] + "…"
    return f"{type(exc).__name__}: {message}" if message else type(exc).__name__


def _utc(value: datetime | None = None) -> str:
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _fqn(row: dict[str, Any]) -> str:
    return ".".join(str(row[k]) for k in ("catalog", "schema", "table", "column"))


def assemble_report(
    columns: Iterable[dict[str, Any]],
    *,
    classifications: Iterable[dict[str, Any]] = (),
    tags: Iterable[dict[str, Any]] = (),
    masks: Iterable[dict[str, Any]] = (),
    policies: Iterable[dict[str, Any]] = (),
    abac_policies: Iterable[dict[str, Any]] = (),
    grants: Iterable[dict[str, Any]] = (),
    generated_at: datetime | None = None,
    approved_by: str = "unapproved",
    approved_at: str | None = None,
    source: str = "offline-config",
) -> dict[str, Any]:
    """Join normalized state rows into a stable, auditable report."""
    tag_map: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in tags:
        tag_map[_fqn(row)].append({"name": str(row["name"]), "value": str(row.get("value", ""))})
    mask_map = {_fqn(row): str(row["name"]) for row in masks}
    policy_map: dict[str, list[str]] = defaultdict(list)
    for row in policies:
        policy_map[_fqn(row)].append(str(row["name"]))
    abac_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in abac_policies:
        abac_map[_fqn(row)].append({
            "name": str(row["name"]),
            "policy_type": str(row["policy_type"]),
            "on_securable": str(row.get("on_securable", "")),
            "function": str(row.get("function", "")),
            "match_conditions": sorted(str(item) for item in row.get("match_conditions", [])),
            "when_condition": str(row.get("when_condition", "")),
            "to_principals": sorted(str(item) for item in row.get("to_principals", [])),
            "except_principals": sorted(str(item) for item in row.get("except_principals", [])),
        })
        policy_map[_fqn(row)].append(str(row["name"]))
    class_map = {_fqn(row): row for row in classifications}
    grant_map: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in grants:
        grant_map[_fqn(row)].append({
            "principal": str(row["principal"]),
            "privilege": str(row["privilege"]),
            "scope": str(row.get("scope", "COLUMN")),
        })

    evidence = []
    for column in sorted(columns, key=_fqn):
        key = _fqn(column)
        scan = class_map.get(key, {})
        evidence.append({
            "catalog": str(column["catalog"]),
            "schema": str(column["schema"]),
            "table": str(column["table"]),
            "column": str(column["column"]),
            "classification": {
                "status": str(scan.get("status", "not_run")),
                "scanned_at": scan.get("scanned_at"),
            },
            "detected_tags": sorted(tag_map.get(key, []), key=lambda item: (item["name"], item["value"])),
            "applied_mask": mask_map.get(key),
            "applied_policies": sorted(set(policy_map.get(key, []))),
            "applied_abac_policies": sorted(abac_map.get(key, []), key=lambda item: (item["name"], item["on_securable"])),
            "grants": sorted(grant_map.get(key, []), key=lambda item: (item["principal"], item["privilege"], item["scope"])),
        })

    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "genierails.compliance_evidence",
        "header": {
            "generated_at": _utc(generated_at),
            "approved_by": approved_by,
            "approved_at": approved_at,
            "source": source,
        },
        "evidence": evidence,
    }


UNKNOWN_APPROVAL = "unknown"


def receipt_approval(env_dir: Path) -> tuple[str, str | None, dict[str, Any]]:
    """(approved_by, approved_at, approval_record) from the env's newest release receipt.

    Only a receipt that passes the check release-plan uses for the env's
    receipts (release_gate.newest_release_receipt: kind, env, cloud, its
    manifest's sha256) and an ``approval`` field that is still the record it
    embedded (approval.embedded_problem) name anyone: a y typed at a terminal
    shows its self-reported git identity, OS user, terminal (device name),
    the command names above the make that asked (or "not recorded" for a
    record older than process_chain) and time, and says GenieRails can't prove
    a person typed it; ``{"via":
    "external"}`` (the plain ``make release MANIFEST=`` path) says external.
    A newest receipt that doesn't verify is ``unverified: <why>``, never an
    older receipt or the name it claims; no receipt is ``unknown``. Only
    approval.IDENTITY_FIELDS are read, never the question, host, run digest,
    asker pids or the tty's path.
    """
    from scripts import approval as approvals
    from scripts import release_gate as rg

    env_dir = Path(env_dir).resolve()
    env = env_dir.name
    try:
        receipt, problem = rg.newest_release_receipt(rg.cloud_dir_of(env_dir), env)
    except Exception as exc:  # an unreadable store names no one: audit only, never a failure
        receipt, problem = None, f"the release receipts couldn't be read ({type(exc).__name__})"
    if receipt is None and not problem:
        return UNKNOWN_APPROVAL, None, {"source": "none", "detail": "no release receipt for this env"}
    if receipt is None:
        return f"unverified: {_short(problem)}", None, {"source": "unverified", "detail": _short(problem)}
    record: dict[str, Any] = {"source": "release-receipt", "receipt": Path(str(receipt["_path"])).name,
                              "commit": str(receipt.get("commit", ""))[:12]}
    phases = receipt.get("phase_approvals")
    linked_phases = {str(value) for value in receipt.get("phase_manifests") or []}
    round_counts: dict[str, int] = defaultdict(int)
    if isinstance(phases, dict) and phases:
        counts: dict[str, int] = defaultdict(int)
        not_counted: dict[str, int] = defaultdict(int)
        for phase, value in phases.items():
            if str(phase) not in linked_phases:
                not_counted["unlinked"] += 1
                continue
            via = str(value.get("via")) if isinstance(value, dict) and value.get("via") else UNKNOWN_APPROVAL
            if via == "terminal":
                manifest_path = rg.manifests_dir(rg.cloud_dir_of(env_dir), env) / str(phase) / "manifest.json"
                try:
                    manifest = rg._read_json(manifest_path) or {}
                    phase_receipt = {
                        "approval": value,
                        "manifest": {"id": str(phase), "sha256": rg.sha256_file(manifest_path)},
                        "commit": manifest.get("commit"),
                    }
                    phase_problem = approvals.embedded_problem(env_dir, env, phase_receipt)
                except Exception as exc:
                    phase_problem = f"the phase approval couldn't be checked ({type(exc).__name__})"
                if phase_problem:
                    via = "unverified"
            counts[via] += 1
        round_counts.update(counts)
        earlier = dict(sorted(counts.items()))
        record["phase_approvals"] = earlier  # schema 1.0 compatibility
        record["earlier_phase_approvals"] = earlier
        if not_counted:
            record["phase_approvals_not_counted"] = dict(sorted(not_counted.items()))
    try:
        problem = approvals.embedded_problem(env_dir, env, receipt)
    except Exception as exc:
        problem = f"its approval couldn't be checked ({type(exc).__name__})"
    approval = receipt.get("approval") if isinstance(receipt.get("approval"), dict) else {}
    if problem:
        record["via"] = UNKNOWN_APPROVAL
        if "approval" not in receipt:
            return UNKNOWN_APPROVAL, None, record | {"detail": problem}
        return f"unverified: {_short(problem)}", None, record | {"source": "unverified", "detail": _short(problem)}
    record["via"] = str(approval["via"])
    round_counts[record["via"]] += 1
    record["round_approvals"] = dict(sorted(round_counts.items()))
    if approval["via"] == "external":
        return "external (approved outside GenieRails; the release receipt records no terminal approval)", None, record
    identity = {k: approval.get(k) for k in approvals.IDENTITY_FIELDS}
    git = identity["git_user"] if isinstance(identity["git_user"], dict) else {}
    name, email = str(git.get("name") or "").strip(), str(git.get("email") or "").strip()
    user = str(identity["user"] or "").strip()
    who = " ".join(part for part in (name, f"<{email}>" if email else "") if part) or "unnamed git user"
    at = identity["approved_at"]
    terminal = approvals.terminal_name(identity["tty"])
    chain, launched = _launched(identity["process_chain"])
    record |= {"terminal": terminal or UNKNOWN_APPROVAL, "process_chain": chain or "not recorded"}
    return (f"{who} (y typed on terminal {terminal or 'unknown'} by OS user {user or 'unknown'}; {launched}; "
            "self-reported: GenieRails cannot prove a human typed it)",
            str(at) if isinstance(at, str) and at else None, record)


def _launched(chain: Any) -> tuple[list[str], str]:
    """(names, wording) for a record's process_chain: what started the make that asked, names only."""
    from scripts import approval as approvals

    if not isinstance(chain, list) or not chain:
        return [], "process chain not recorded"
    names = [approvals.command_name(name) for name in chain[:1 + approvals.CHAIN_ABOVE_MAKE]]
    via = " <- ".join(names)
    if len(names) < 2:
        return names, f"via {via} (what started make not recorded)"
    if names[1] in approvals.SHELLS:
        return names, f"via {via} (make started from a shell)"
    return names, f"via {via} (make started by {names[1]}, not a shell: a script or driver)"


def _short(text: str, limit: int = 200) -> str:
    """A one-line reason; absolute paths keep only their last two parts (no local paths in the report)."""
    text = re.sub(r"(?:/[^\s/]+)+/([^\s/]+/[^\s/]+)", r"…/\1", " ".join(str(text).split()))
    return text if len(text) <= limit else text[:limit] + "…"


def render_markdown(report: dict[str, Any]) -> str:
    """Render a compact human-readable view of the canonical JSON artifact."""
    header = report["header"]
    lines = [
        "# GenieRails Compliance Evidence",
        "",
        f"- Schema version: `{report['schema_version']}`",
        f"- Generated at: `{header['generated_at']}`",
        f"- Source: `{header['source']}`",
        f"- Approved by: `{header['approved_by']}`",
        f"- Approved at: `{header['approved_at'] or 'unknown'}`",
    ]
    record = header.get("approval_record")
    if isinstance(record, dict) and record.get("source") == "release-receipt":
        phases = record.get("earlier_phase_approvals") or {}
        earlier = (", earlier phases: " + ", ".join(f"{n} {via}" for via, n in phases.items())) if phases else ""
        not_counted = record.get("phase_approvals_not_counted") or {}
        omitted = (", not counted: " + ", ".join(f"{n} {why}" for why, n in not_counted.items())) \
            if not_counted else ""
        detail = f"; {record['detail']}" if record.get("detail") else ""
        lines.append(f"- Approval record: release receipt `{record.get('receipt')}` (commit `{record.get('commit')}`; "
                     f"final round: {record.get('via')}{earlier}{omitted}){detail}")
    elif isinstance(record, dict) and record.get("detail"):
        lines.append(f"- Approval record: {record['detail']}")
    if "column_privileges_availability" in report:
        availability = report["column_privileges_availability"]
        detail = ", ".join(f"{catalog}={status}" for catalog, status in sorted(availability.items())) or "not queried"
        lines.append(f"- Column privileges: `{detail}`")
    if "abac_policy_listing" in report:
        listing = report["abac_policy_listing"]
        lines.append(f"- ABAC policies: `{listing['status']}` ({listing['policies']} effective on {listing['tables']} tables)")
    not_evaluated = report.get("abac_policies_not_evaluated") or []
    if not_evaluated:
        lines.extend([
            f"- **WARNING: {len(not_evaluated)} effective ABAC policy mapping(s) could not be evaluated. "
            "They are listed below and are NOT shown as protection on any column.**",
            "", "| Table | Policy | On | Reason |", "|---|---|---|---|",
        ])
        for item in not_evaluated:
            values = [item["table"], item["policy"], item["on_securable"], item["reason"]]
            lines.append("| " + " | ".join(str(value).replace("|", "\\|") for value in values) + " |")
    lines.extend([
        "", "| Table | Column | Scan status | Scanned at | Tags | Applied mask / policies | Grants |",
        "|---|---|---|---|---|---|---|",
    ])
    for item in report["evidence"]:
        table = ".".join((item["catalog"], item["schema"], item["table"]))
        tags = ", ".join(f"{tag['name']}={tag['value']}" for tag in item["detected_tags"]) or "—"
        grants = ", ".join(
            f"{grant['principal']}:{grant['privilege']} ({grant['scope']})" for grant in item["grants"]
        ) or "—"
        abac = item.get("applied_abac_policies", [])
        abac_names = {policy["name"] for policy in abac}
        protections = ", ".join(
            ([item["applied_mask"]] if item["applied_mask"] else [])
            + [name for name in item["applied_policies"] if name not in abac_names]
            + [f"{policy['name']} (to: {', '.join(policy['to_principals']) or 'none'}; "
               f"except: {', '.join(policy['except_principals']) or 'none'})" for policy in abac]
        ) or "—"
        values = [table, item["column"], item["classification"]["status"],
                  item["classification"]["scanned_at"] or "—", tags,
                  protections, grants]
        lines.append("| " + " | ".join(str(value).replace("|", "\\|") for value in values) + " |")
    return "\n".join(lines) + "\n"


def _load_hcl(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        import hcl2
    except ImportError as exc:
        raise RuntimeError("python-hcl2 is required; install it with: pip install python-hcl2") from exc
    with path.open(encoding="utf-8") as handle:
        return hcl2.load(handle)


def configured_columns(env_dir: Path) -> list[dict[str, str]]:
    """Return column footprint declared by environment tag assignments."""
    configs = [env_dir / "data_access" / "abac.auto.tfvars", env_dir / "abac.auto.tfvars"]
    found: dict[str, dict[str, str]] = {}
    for config in configs:
        for assignment in _load_hcl(config).get("tag_assignments", []):
            if assignment.get("entity_type") != "columns":
                continue
            parts = str(assignment.get("entity_name", "")).split(".")
            if len(parts) == 4:
                row = dict(zip(("catalog", "schema", "table", "column"), parts))
                found[_fqn(row)] = row
    return list(found.values())


def configured_tags(env_dir: Path) -> list[dict[str, str]]:
    rows = []
    for config in (env_dir / "data_access" / "abac.auto.tfvars", env_dir / "abac.auto.tfvars"):
        for item in _load_hcl(config).get("tag_assignments", []):
            parts = str(item.get("entity_name", "")).split(".")
            if item.get("entity_type") == "columns" and len(parts) == 4:
                row = dict(zip(("catalog", "schema", "table", "column"), parts))
                row.update(name=str(item.get("tag_key", "")), value=str(item.get("tag_value", "")))
                rows.append(row)
    return rows


def configured_tables(env_dir: Path) -> list[str]:
    """Return the tables managed by configured Genie agents."""
    tables = set(resolve_footprint(env_dir))
    # Tag assignments remain a useful fallback for governance-only environments.
    tables.update(".".join(_fqn(row).split(".")[:3]) for row in configured_columns(env_dir))
    return sorted(table for table in tables if len(table.split(".")) == 3)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _evaluate(condition: str, tags: set[tuple[str, str]]) -> bool | None:
    """Whether ``tags`` satisfy an exact OR of hasTagValue/hasTag checks; None if it's anything else."""
    from scripts.audit_schema_drift import _parse_condition_tag_refs

    if not condition.strip() or _ONLY_OR.fullmatch(_TAG_REF.sub(" ", condition)) is None:
        return None
    value_refs, key_refs = _parse_condition_tag_refs(condition)
    return bool(tags & value_refs or {key for key, _ in tags} & key_refs)


def abac_policy_rows(table: str, policies: Iterable[Any], columns: list[dict[str, Any]],
                     column_tags: dict[str, set[tuple[str, str]]],
                     table_tags: set[tuple[str, str]] = frozenset()) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """(rows, not_evaluated) for the effective ABAC policies on ``table``.

    rows: one per (column, policy) whose mapping is fully evaluated and
    matches: the table's tags satisfy the policy's when_condition (none means
    always), and for a column mask the column's tags satisfy the condition of
    the match column whose alias is the mask's on_column; a row filter covers
    every column. Conditions are exact ORs of hasTagValue/hasTag checks, parsed
    as audit-rulebook and agent coverage do. Any policy this can't evaluate
    exactly is never attached to a column; it is returned in not_evaluated
    with the reason.
    """
    from scripts.staged_policy_verify import _get, normalize_policy

    rows, not_evaluated = [], []
    for policy in policies:
        live = normalize_policy(policy)
        kind = live["policy_type"].upper()
        on = live["on"] or table

        def skip(reason: str) -> None:
            not_evaluated.append({"table": table, "policy": live["name"] or "(unnamed)",
                                  "on_securable": on, "reason": reason})

        if not live["name"] or kind not in COLUMN_MASK_TYPES | ROW_FILTER_TYPES:
            skip(f"policy type {live['policy_type'] or '(none)'} is not a column mask or row filter")
            continue
        matches = list(_get(policy, "match_columns") or [])
        when = str(_get(policy, "when_condition") or "")
        record = {
            "name": live["name"], "policy_type": "COLUMN_MASK" if kind in COLUMN_MASK_TYPES else "ROW_FILTER",
            "on_securable": on, "function": live["function"],
            "match_conditions": [str(_get(m, "condition") or "") for m in matches],
            "when_condition": when, "to_principals": live["to"], "except_principals": live["except"],
        }
        applies = _evaluate(when, table_tags) if when.strip() else True
        if applies is None:
            skip("when_condition is not an exact OR of hasTagValue/hasTag checks")
            continue
        if not applies:
            continue
        if kind in ROW_FILTER_TYPES:
            rows.extend({**column, **record} for column in columns)
            continue
        mask = _get(policy, "column_mask")
        on_column = _get(mask, "on_column") if mask else None
        if not live["function"] or not on_column:
            skip("column mask has no function or no on_column")
            continue
        bound = [m for m in matches if _get(m, "alias") == on_column]
        if len(bound) != 1:
            skip(f"{len(bound)} match columns have alias {on_column!r} (the mask's on_column); expected exactly 1")
            continue
        if len(matches) != 1:
            skip("the mask binds more match columns than its on_column, which this report can't evaluate")
            continue
        condition = str(_get(bound[0], "condition") or "")
        if _evaluate(condition, set()) is None:
            skip("match condition is not an exact OR of hasTagValue/hasTag checks")
            continue
        for column in columns:
            if _evaluate(condition, column_tags.get(_fqn(column), set())):
                rows.append({**column, **record})
    return rows, not_evaluated


def collect_live_state(columns: list[dict[str, str]], tables: list[str], warehouse_id: str,
                       auth_file: Path = Path("auth.auto.tfvars")) -> dict[str, Any]:
    """Read live state through the SDK; called only in integration mode (make evidence's default)."""
    from scripts.auth_configured import workspace_client
    try:
        client = workspace_client(auth_file, product="genierails-evidence", product_version=SCHEMA_VERSION)
    except RuntimeError as exc:  # missing or incomplete auth; the messages name the file, never a value
        raise EvidenceUnavailable(f"can't read live state: {exc} (make evidence OFFLINE=1 writes the "
                                  "config-only report)") from None
    if not tables:
        return {name: [] for name in ("columns", "classifications", "tags", "masks", "policies", "grants")}
    def query(sql: str) -> list[list[Any]]:
        response = client.statement_execution.execute_statement(
            statement=sql, warehouse_id=warehouse_id, wait_timeout="50s"
        )
        state = str(getattr(getattr(response, "status", None), "state", ""))
        while state.endswith(("PENDING", "RUNNING")):
            time.sleep(1)
            response = client.statement_execution.get_statement(response.statement_id)
            state = str(getattr(getattr(response, "status", None), "state", ""))
        if state.endswith("FAILED"):
            error = getattr(getattr(response, "status", None), "error", None)
            raise RuntimeError(getattr(error, "message", "Databricks SQL statement failed"))
        return list(getattr(getattr(response, "result", None), "data_array", None) or [])

    table_key_sql = ", ".join(_sql_literal(key) for key in tables)
    live_columns = query("SELECT table_catalog, table_schema, table_name, column_name FROM system.information_schema.columns WHERE concat(table_catalog, '.', table_schema, '.', table_name) IN (" + table_key_sql + ")")
    live_column_dicts = [dict(zip(("catalog", "schema", "table", "column"), row[:4])) for row in live_columns]
    keys = ", ".join(_sql_literal(_fqn(row)) for row in live_column_dicts)
    if not keys:
        return {name: [] for name in ("columns", "classifications", "tags", "masks", "policies", "grants")}
    predicate = "concat(table_catalog, '.', table_schema, '.', table_name, '.', column_name) IN (" + keys + ")"
    tag_rows = query("SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value FROM system.information_schema.column_tags WHERE concat(catalog_name, '.', schema_name, '.', table_name, '.', column_name) IN (" + keys + ")")
    # Azure's live view has no mask catalog/schema columns, so mask_name must
    # remain bare; table_catalog/table_schema identify the protected table.
    mask_rows = query("SELECT table_catalog, table_schema, table_name, column_name, mask_name FROM system.information_schema.column_masks WHERE concat(table_catalog, '.', table_schema, '.', table_name, '.', column_name) IN (" + keys + ")")
    # COLUMN_PRIVILEGES is optional in Databricks information_schema. Discover it
    # per catalog rather than assuming the system-wide relation exists (Azure
    # workspaces commonly expose no column-level privilege relation at all).
    grant_rows = []
    column_privileges_availability = {}
    for catalog in sorted({key.split(".")[0] for key in tables}):
        information_schema = _sql_identifier(catalog) + ".information_schema"
        relations = query("SHOW TABLES IN " + information_schema)
        available = any(len(row) > 1 and str(row[1]).lower() == "column_privileges" for row in relations)
        column_privileges_availability[catalog] = "available" if available else "not_available"
        if available:
            grant_rows.extend(query("SELECT table_catalog, table_schema, table_name, column_name, grantee, privilege_type FROM " + information_schema + ".column_privileges WHERE " + predicate))
    table_grant_rows = query("SELECT table_catalog, table_schema, table_name, grantee, privilege_type FROM system.information_schema.table_privileges WHERE concat(table_catalog, '.', table_schema, '.', table_name) IN (" + table_key_sql + ")")
    schema_keys = sorted({".".join(key.split(".")[:2]) for key in tables})
    catalog_keys = sorted({key.split(".")[0] for key in tables})
    schema_grant_rows = query("SELECT catalog_name, schema_name, grantee, privilege_type FROM system.information_schema.schema_privileges WHERE concat(catalog_name, '.', schema_name) IN (" + ", ".join(_sql_literal(key) for key in schema_keys) + ")")
    catalog_grant_rows = query("SELECT catalog_name, grantee, privilege_type FROM system.information_schema.catalog_privileges WHERE catalog_name IN (" + ", ".join(_sql_literal(key) for key in catalog_keys) + ")")
    # Azure's live view likewise has no filter catalog/schema columns.
    policy_rows = query("SELECT table_catalog, table_schema, table_name, filter_name FROM system.information_schema.row_filters WHERE concat(table_catalog, '.', table_schema, '.', table_name) IN (" + table_key_sql + ")")

    base = lambda row: dict(zip(("catalog", "schema", "table", "column"), row[:4]))
    result: dict[str, Any] = {
        "columns": live_column_dicts, "classifications": [],
        "tags": [], "masks": [], "policies": [], "grants": [],
    }
    result["column_privileges_availability"] = column_privileges_availability
    for row in tag_rows:
        result["tags"].append({**base(row), "name": row[4], "value": row[5]})
    for row in mask_rows:
        result["masks"].append({**base(row), "name": row[4]})
    for row in grant_rows:
        result["grants"].append({**base(row), "principal": row[4], "privilege": row[5], "scope": "COLUMN"})
    by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for column in result["columns"]:
        by_table[".".join(_fqn(column).split(".")[:3])].append(column)
    for row in table_grant_rows:
        for column in by_table[".".join(str(value) for value in row[:3])]:
            result["grants"].append({**column, "principal": row[3], "privilege": row[4], "scope": "TABLE"})
    for row in schema_grant_rows:
        prefix = ".".join(str(value) for value in row[:2]) + "."
        for table, table_columns in by_table.items():
            if table.startswith(prefix):
                for column in table_columns:
                    result["grants"].append({**column, "principal": row[2], "privilege": row[3], "scope": "SCHEMA"})
    for row in catalog_grant_rows:
        prefix = str(row[0]) + "."
        for table, table_columns in by_table.items():
            if table.startswith(prefix):
                for column in table_columns:
                    result["grants"].append({**column, "principal": row[1], "privilege": row[2], "scope": "CATALOG"})
    for row in policy_rows:
        for column in by_table[".".join(str(value) for value in row[:3])]:
            result["policies"].append({**column, "name": row[3]})
    from scripts.staged_policy_verify import normalize_policy

    # Deterministic governance masks through catalog-level ABAC policies, which
    # neither information_schema view above lists. Read the effective set per
    # table (include_inherited: catalog, schema and table level) through the
    # same env-only client; the SDK follows every page of the listing.
    column_tags: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for row in tag_rows:
        column_tags[_fqn(base(row))].add((str(row[4]), str(row[5])))
    # A policy's when_condition is evaluated against its table's tags.
    try:
        table_tag_rows = query("SELECT catalog_name, schema_name, table_name, tag_name, tag_value FROM system.information_schema.table_tags WHERE concat(catalog_name, '.', schema_name, '.', table_name) IN (" + table_key_sql + ")")
    except Exception as exc:
        raise EvidenceUnavailable(
            f"could not read the table tags ABAC when_conditions are evaluated against ({_safe_error(exc)}); "
            "the report would wrongly show policies as applied or not") from exc
    table_tags: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for row in table_tag_rows:
        table_tags[".".join(str(value) for value in row[:3])].add((str(row[3]), str(row[4])))
    result["abac_policies"] = []
    result["abac_policies_not_evaluated"] = []
    listed: set[tuple[str, str]] = set()
    for table, table_columns in sorted(by_table.items()):
        try:
            policies = list(client.policies.list_policies("TABLE", table, include_inherited=True))
        except Exception as exc:
            raise EvidenceUnavailable(
                f"could not list the ABAC policies effective on {table} ({_safe_error(exc)}); "
                "the report would wrongly show its columns unprotected") from exc
        rows, not_evaluated = abac_policy_rows(table, policies, table_columns, column_tags, table_tags[table])
        result["abac_policies"].extend(rows)
        result["abac_policies_not_evaluated"].extend(not_evaluated)
        listed.update((live["on"] or table, live["name"]) for live in map(normalize_policy, policies))
    result["abac_policy_listing"] = {
        "status": "incomplete" if result["abac_policies_not_evaluated"] else "complete",
        "tables": len(by_table), "policies": len(listed),
    }
    try:
        class_rows = query("SELECT catalog_name, schema_name, table_name, column_name, class_tag, latest_detected_time FROM system.data_classification.results WHERE concat(catalog_name, '.', schema_name, '.', table_name, '.', column_name) IN (" + keys + ")")
        classification_available = True
    except Exception:
        class_rows = []
        classification_available = False
    detected = set()
    for row in class_rows:
        column = base(row)
        detected.add(_fqn(column))
        result["classifications"].append({**column, "status": "detected", "scanned_at": row[5]})
        result["tags"].append({**column, "name": "data_classification", "value": row[4]})
    for column in result["columns"]:
        if _fqn(column) not in detected:
            status = "no_detection_record" if classification_available else "unavailable"
            result["classifications"].append({**column, "status": status, "scanned_at": None})
    return result


def evidence_dir(env_dir: Path) -> Path:
    """The env's evidence folder in its state dir (release_gate.env_state(...) / "evidence")."""
    from scripts.state_layout import IN_CHECKOUT_STATE, checked_state_dir, cloud_dir

    cloud, base = cloud_dir(env_dir), checked_state_dir()  # a missing GENIERAILS_STATE_DIR refuses
    root = base / cloud.name if base else cloud / IN_CHECKOUT_STATE
    return root / Path(env_dir).resolve().name / "evidence"


def report_names(schema_version: str = SCHEMA_VERSION) -> tuple[str, str]:
    stem = "compliance-evidence-v" + schema_version
    return f"{stem}.json", f"{stem}.md"


def _git_env() -> dict[str, str]:
    """The environment for the ignore check: no GIT_* variable (GIT_DIR, GIT_WORK_TREE,
    GIT_INDEX_FILE, GIT_COMMON_DIR, GIT_CEILING_DIRECTORIES, GIT_CONFIG_*, ...) can point git
    at another repo, index or config."""
    return {key: value for key, value in os.environ.items() if not key.upper().startswith("GIT_")}


def enclosing_work_trees(path: Path) -> list[Path]:
    """Every git work tree containing ``path`` (resolved), innermost first.

    Found on the filesystem, not by asking git: ``path`` and each of its ancestors that
    holds a ``.git`` entry (a directory, or the file of a linked worktree or submodule).
    A nested repo never hides the one around it, and the walk follows the filesystem's
    own lookup, so a case alias of a repo's path (macOS) still finds the repo.
    """
    target = Path(path).resolve()
    return [folder for folder in (target, *target.parents) if os.path.lexists(folder / ".git")]


def refuse_committable(output_dir: Path) -> None:
    """Refuse an evidence folder that any enclosing git work tree could commit.

    The default <cloud>/.genierails-state is ignored; a GENIERAILS_STATE_DIR (or a
    symlink) can still point into a work tree. The folder is resolved (symlinks
    followed); then, in every work tree around it, both report paths must be
    ignored by that repo's own rules (.gitignore files and .git/info/exclude, not a
    per-user core.excludesFile) and untracked (git check-ignore reports a tracked
    path as not ignored). Any failure, git missing included, refuses. Read-only git
    only, with a scrubbed environment (_git_env) and an explicit -C.
    """
    target = Path(output_dir).resolve()
    for tree in enclosing_work_trees(target):
        probes = [(target / name).relative_to(tree).as_posix() for name in report_names()]
        try:
            result = subprocess.run(["git", "-C", str(tree), "-c", f"core.excludesFile={os.devnull}",
                                     "check-ignore", "-z", "--stdin"], input="\0".join(probes) + "\0",
                                    text=True, capture_output=True, env=_git_env())
        except OSError as exc:
            raise EvidenceUnavailable(
                f"the evidence folder {target} is inside the git work tree {tree}, and git can't be run "
                f"({exc.strerror or type(exc).__name__}) to confirm the record is git-ignored. Install git, or "
                "point GENIERAILS_STATE_DIR outside every git checkout") from None
        ignored = set(filter(None, result.stdout.split("\0")))
        if result.returncode not in (0, 1) or ignored != set(probes):
            reason = "git couldn't check it" if result.returncode not in (0, 1) else "not git-ignored"
            raise EvidenceUnavailable(
                f"the evidence folder {target} is inside the git work tree {tree} and {reason}, so the record "
                "could be committed. Point GENIERAILS_STATE_DIR outside every git checkout (or git-ignore that "
                "folder in each one) and run make evidence again")


def _write_private(path: Path, text: str) -> None:
    """Write ``path`` 0600, replacing it whole (a failed write leaves the previous file)."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")  # mkstemp creates 0600
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    path.chmod(0o600)


def write_report(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    output_dir.chmod(0o700)
    json_name, md_name = report_names(report["schema_version"])
    json_path, md_path = output_dir / json_name, output_dir / md_name
    _write_private(json_path, json.dumps(report, indent=2, sort_keys=True) + "\n")
    _write_private(md_path, render_markdown(report))
    return json_path, md_path


def main(argv: list[str] | None = None, *, collector: Callable[..., dict[str, Any]] = collect_live_state) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-dir", type=Path, default=Path.cwd())
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--warehouse-id", default=os.getenv("DATABRICKS_WAREHOUSE_ID"))
    parser.add_argument("--approved-by", default=os.getenv("GENIERAILS_EVIDENCE_APPROVED_BY"),
                        help="sign the report (default: the approval the env's last release receipt records)")
    parser.add_argument("--approved-at", default=os.getenv("GENIERAILS_EVIDENCE_APPROVED_AT"))
    args = parser.parse_args(argv)
    approval_record: dict[str, Any] = {"source": "signed"}
    if not args.approved_by:
        args.approved_by, receipt_at, approval_record = receipt_approval(args.env_dir)
        args.approved_at = args.approved_at or receipt_at
    columns, tags = configured_columns(args.env_dir), configured_tags(args.env_dir)
    tables = configured_tables(args.env_dir)
    try:
        output_dir = args.output_dir or evidence_dir(args.env_dir)
        refuse_committable(output_dir)
    except RuntimeError as exc:  # StateError (GENIERAILS_STATE_DIR set but missing) or EvidenceUnavailable
        print(f"ERROR: {exc}. No evidence was written.", file=sys.stderr)
        return 2
    live = os.getenv(INTEGRATION_ENV, "").lower() in {"1", "true", "yes"}
    if live:
        if not args.warehouse_id:
            parser.error(f"--warehouse-id or DATABRICKS_WAREHOUSE_ID is required when {INTEGRATION_ENV}=1")
        try:
            state = collector(columns, tables, args.warehouse_id, args.env_dir / "auth.auto.tfvars")
        except EvidenceUnavailable as exc:
            print(f"ERROR: {exc}. No evidence was written.", file=sys.stderr)
            return 2
        report = assemble_report(state["columns"], classifications=state["classifications"], tags=state["tags"], masks=state["masks"], policies=state["policies"], abac_policies=state.get("abac_policies", []), grants=state["grants"], approved_by=args.approved_by, approved_at=args.approved_at, source="databricks-live")
        report["header"]["approval_record"] = approval_record
        report["column_privileges_availability"] = state.get("column_privileges_availability", {})
        if "abac_policy_listing" in state:
            report["abac_policy_listing"] = state["abac_policy_listing"]
            report["abac_policies_not_evaluated"] = state.get("abac_policies_not_evaluated", [])
    else:
        report = assemble_report(columns, tags=tags, approved_by=args.approved_by, approved_at=args.approved_at)
        report["header"]["approval_record"] = approval_record
    paths = write_report(report, output_dir)
    print("Wrote " + " and ".join(str(path) for path in paths))
    old = Path(args.env_dir) / "generated" / "evidence"
    if old.is_dir():
        print(f"NOTE: {old} is from an earlier GenieRails, which wrote evidence there. Archive it and remove "
              "it if it isn't committed: an uncommitted file under envs/ stops make maintain and release-plan.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
