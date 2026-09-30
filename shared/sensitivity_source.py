#!/usr/bin/env python3
"""Pluggable per-column sensitivity sources for ABAC generation.

GenieRails decides, per column, which governed classification tag it should
carry (``pii_level``, ``pci_level``, ...).  Historically that decision came
solely from inferring sensitivity out of the table DDL — the LLM prompt path
and its deterministic name-pattern backstop in ``generate_abac.py``.

This module introduces a :class:`SensitivitySource` interface with two
implementations so the *input* to generation is explicit and swappable:

* :class:`ClassificationSource` reads **native Unity Catalog Data
  Classification** as the authoritative sensitivity input — the ``class.*``
  namespace in ``system.information_schema.column_tags`` (and, when present,
  ``system.data_classification.results``).
* :class:`LLMSource` wraps the existing DDL-inference path.  It is *demoted*
  behind this interface, never deleted: it stays the fallback for any column
  the native classifier has nothing to say about.

Default selection (see :func:`select_findings`): for any column that carries
native ``class.*`` tags, use :class:`ClassificationSource`; otherwise fall
back to :class:`LLMSource`.  Every :class:`Finding` is labelled with the
:data:`Finding.source` that produced it.

This module is the generation *input* layer only.  It decides what a column's
sensitivity is and where that came from; it does not derive masking treatments,
promote configs, or touch the grant chain.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from typing import Callable, Iterable, Sequence

# --- source labels ---------------------------------------------------------
CLASSIFICATION = "classification"
LLM = "llm"

# Native UC Data Classification uses the reserved ``class`` tag namespace.
CLASS_NAMESPACE = "class."


@dataclass(frozen=True)
class Finding:
    """A single per-column sensitivity determination.

    ``entity_name`` is the fully-qualified column ``catalog.schema.table.column``.
    ``tag_key`` / ``tag_value`` are the governed classification tag the column
    should carry (the same vocabulary the rest of the generator uses, e.g.
    ``pii_level`` = ``masked_email``).  ``source`` records which
    :class:`SensitivitySource` produced this finding.  ``detail`` is a
    human-readable provenance note (the native ``class.*`` tag, or the matched
    DDL pattern) — useful for logging and audit.
    """

    entity_name: str
    tag_key: str
    tag_value: str
    source: str
    detail: str = ""

    @property
    def entity_type(self) -> str:
        # All findings describe columns; kept as a property so a Finding can be
        # dropped straight into a tag_assignment dict shape.
        return "columns"

    def as_assignment(self) -> dict:
        """Return the finding in generate_abac's tag_assignment dict shape."""
        return {
            "entity_type": self.entity_type,
            "entity_name": self.entity_name,
            "tag_key": self.tag_key,
            "tag_value": self.tag_value,
        }


class SensitivitySource(ABC):
    """A source of per-column sensitivity :class:`Finding` objects."""

    #: stable label attached to every finding this source produces
    name: str = ""

    @abstractmethod
    def findings_for(self, columns: Sequence[str]) -> list[Finding]:
        """Return findings for the given fully-qualified column names.

        ``columns`` is the universe of candidate columns.  A source returns a
        finding only for columns it actually has an opinion about; the rest are
        left for another source (see :func:`select_findings`).
        """
        raise NotImplementedError

    def claimed_columns(self, columns: Sequence[str]) -> set[str]:
        """Columns this source is *authoritative* over, even without a finding.

        A claimed column is never overridden by a lower-priority source in
        :func:`select_findings`, whether or not this source emitted a mapped
        finding for it.  The default is "only columns I produced findings for";
        :class:`ClassificationSource` widens this to every natively-classified
        column so an unmapped ``class.*`` tag can never fall through to the LLM.
        """
        return {f.entity_name for f in self.findings_for(columns)}


# ---------------------------------------------------------------------------
# Native classification → governed vocabulary
# ---------------------------------------------------------------------------
# Maps a native Data-Classification *semantic* (the suffix of a ``class.*`` tag,
# or a ``system.data_classification.results`` semantic type) to the governed
# ``(tag_key, tag_value)`` the rest of the generator understands.  Only
# semantics whose tag_value is coverable by the existing masking-function
# library are mapped, so classification findings never introduce an uncovered
# tag that would fail validation downstream.  Keys are matched
# case-insensitively; both the tag suffix and the tag *value* are consulted.
_CLASS_TO_GOVERNED: dict[str, tuple[str, str]] = {
    "email": ("pii_level", "masked_email"),
    "email_address": ("pii_level", "masked_email"),
    "phone": ("pii_level", "masked_phone"),
    "phone_number": ("pii_level", "masked_phone"),
    "telephone": ("pii_level", "masked_phone"),
    "name": ("pii_level", "masked_name"),
    "address": ("pii_level", "redacted_address"),
    "postal_address": ("pii_level", "redacted_address"),
    "street_address": ("pii_level", "redacted_address"),
    "location": ("pii_level", "redacted_address"),
    "date_of_birth": ("pii_level", "masked_dob"),
    "dob": ("pii_level", "masked_dob"),
    "birth_date": ("pii_level", "masked_dob"),
    "ssn": ("pii_level", "masked_ssn"),
    "social_security_number": ("pii_level", "masked_ssn"),
    # Databricks native data-classification semantic tags (documented `class.*`)
    "us_ssn": ("pii_level", "masked_ssn"),
    "us_social_security_number": ("pii_level", "masked_ssn"),
    "us_itin": ("pii_level", "masked_ssn"),
    "tfn": ("pii_level", "masked_tfn"),
    "tax_file_number": ("pii_level", "masked_tfn"),
    "medicare": ("pii_level", "masked_medicare"),
    "medicare_number": ("pii_level", "masked_medicare"),
    "bsb": ("pii_level", "masked_bsb"),
    "aadhaar": ("pii_level", "masked_aadhaar"),
    "aadhar": ("pii_level", "masked_aadhaar"),
    "nric": ("pii_level", "masked_nric"),
    "mykad": ("pii_level", "masked_mykad"),
    "bank_account": ("pii_level", "masked_account"),
    "bank_account_number": ("pii_level", "masked_account"),
    "us_bank_account_number": ("pii_level", "masked_account"),
    "account_number": ("pii_level", "masked_account"),
    "iban": ("pii_level", "masked_account"),
    "iban_code": ("pii_level", "masked_account"),
    "credit_card": ("pci_level", "masked_card_last4"),
    "credit_card_number": ("pci_level", "masked_card_last4"),
    "card_number": ("pci_level", "masked_card_last4"),
    "pan": ("pci_level", "masked_card_last4"),
    "card_security_code": ("pci_level", "redacted_cvv"),
    "cvv": ("pci_level", "redacted_cvv"),
    "cvc": ("pci_level", "redacted_cvv"),
}

# Alias set kept in sync with the coverable governed values above.  Any native
# class semantic NOT present here is still treated as authoritative (a
# classified column is never handed to the LLM), just "unmapped": no governed
# tag is emitted for it and it is surfaced in the log so the coverage/overlap
# gate can pick it up.  See ClassificationSource.claimed_columns / findings_for.


def _normalize_semantic(text: str) -> str:
    """Normalize a raw class semantic for lookup (lowercase, strip, unify sep)."""
    return (text or "").strip().lower().replace("-", "_").replace(" ", "_")


def _semantic_from_tag(tag_name: str, tag_value: str) -> str | None:
    """Extract the classification semantic from a column-tag (name, value) pair.

    Native classification tags live in the ``class`` namespace.  The semantic
    may be encoded as the tag-name suffix (``class.email``) or carried in the
    tag value (``class`` = ``email``).  Returns ``None`` for non-class tags.
    """
    name = (tag_name or "").strip()
    lowered = name.lower()
    if lowered.startswith(CLASS_NAMESPACE):
        return _normalize_semantic(name[len(CLASS_NAMESPACE):])
    if lowered == "class" or lowered.rstrip("s") == "classification":
        return _normalize_semantic(tag_value)
    return None


class ClassificationSource(SensitivitySource):
    """Sensitivity findings sourced from native UC Data Classification.

    Construct directly with pre-read rows (the shape used by unit tests and any
    caller that already has the data), or via :meth:`from_reader` /
    :meth:`from_run_sql` to query the system tables live.

    ``tag_rows``: iterable of ``(catalog, schema, table, column, tag_name,
    tag_value)`` from ``system.information_schema.column_tags`` — only rows in
    the ``class.*`` namespace contribute.

    ``classification_rows``: iterable of ``(catalog, schema, table, column,
    class_name)`` from ``system.data_classification.results`` (optional).
    """

    name = CLASSIFICATION

    def __init__(
        self,
        tag_rows: Iterable[Sequence] | None = None,
        classification_rows: Iterable[Sequence] | None = None,
        mapping: dict[str, tuple[str, str]] | None = None,
    ):
        self._tag_rows = [tuple(r) for r in (tag_rows or [])]
        self._classification_rows = [tuple(r) for r in (classification_rows or [])]
        if mapping is None:
            mapping = dict(_CLASS_TO_GOVERNED)
            # Auto-scaffolded class labels resolve straight to the owned
            # treatment tag. Native class.* tags themselves remain read-only.
            from treatment_derivation import load_treatment_config
            for treatment in load_treatment_config().treatments:
                for label in treatment.class_labels:
                    semantic = _normalize_semantic(
                        label[len(CLASS_NAMESPACE):]
                        if label.lower().startswith(CLASS_NAMESPACE) else label
                    )
                    mapping[semantic] = ("gr_treatment", treatment.value)
        self._mapping = mapping

    # -- construction helpers ------------------------------------------------
    @classmethod
    def from_run_sql(
        cls,
        run_sql: Callable[[str], list],
        table_refs: Sequence[str],
        mapping: dict[str, tuple[str, str]] | None = None,
    ) -> "ClassificationSource":
        """Build by querying the system tables via an injected ``run_sql``.

        ``run_sql(sql) -> rows`` mirrors ``audit_schema_drift._run_sql`` (returns
        a list of row lists).  Reads are best-effort: the column_tags read is
        required, but a missing ``system.data_classification.results`` table is
        tolerated so this works on workspaces without that surface.
        """
        tag_rows = _read_class_column_tags(run_sql, table_refs)
        try:
            classification_rows = _read_data_classification_results(run_sql, table_refs)
        except Exception:
            classification_rows = []
        return cls(tag_rows=tag_rows, classification_rows=classification_rows, mapping=mapping)

    # -- source API ----------------------------------------------------------
    def has_native_data(self) -> bool:
        """True if any native classification rows were supplied at all."""
        return bool(self._tag_rows or self._classification_rows)

    def classified_columns(self) -> set[str]:
        """Columns that carry *any* native ``class.*`` signal (mapped or not)."""
        cols: set[str] = set()
        for entity_name, semantic, _raw in self._iter_semantics():
            if semantic:
                cols.add(entity_name)
        return cols

    def claimed_columns(self, columns: Sequence[str]) -> set[str]:
        """Every natively-classified column in ``columns`` is authoritative.

        This is deliberately wider than ``findings_for``: a column carrying a
        ``class.*`` tag whose semantic we don't (yet) map is still claimed, so
        the LLM never gets to override a natively-classified column.
        """
        wanted = set(columns) if columns else None
        classified = self.classified_columns()
        if wanted is None:
            return classified
        return {c for c in classified if c in wanted}

    def unmapped_columns(self, columns: Sequence[str] | None = None) -> list[tuple[str, str]]:
        """(entity_name, semantic) for classified columns we produced no tag for.

        These are authoritative-but-unmapped: no governed tag is applied and the
        LLM is still blocked, so a caller can surface them (e.g. so a coverage /
        overlap gate flags that a natively-classified column went un-tagged).
        """
        wanted = set(columns) if columns else None
        out: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for entity_name, semantic, _raw in self._iter_semantics():
            if wanted is not None and entity_name not in wanted:
                continue
            if semantic in self._mapping:
                continue
            key = (entity_name, semantic)
            if key in seen:
                continue
            seen.add(key)
            out.append(key)
        return out

    def findings_for(self, columns: Sequence[str]) -> list[Finding]:
        wanted = set(columns) if columns else None
        findings: list[Finding] = []
        seen: set[tuple[str, str, str]] = set()
        for entity_name, semantic, raw in self._iter_semantics():
            if wanted is not None and entity_name not in wanted:
                continue
            mapped = self._mapping.get(semantic)
            if not mapped:
                continue
            tag_key, tag_value = mapped
            key = (entity_name, tag_key, tag_value)
            if key in seen:
                continue
            seen.add(key)
            findings.append(
                Finding(
                    entity_name=entity_name,
                    tag_key=tag_key,
                    tag_value=tag_value,
                    source=CLASSIFICATION,
                    detail=f"{CLASS_NAMESPACE}{raw}" if raw else CLASS_NAMESPACE.rstrip("."),
                )
            )
        return findings

    # -- internals -----------------------------------------------------------
    def _iter_semantics(self):
        """Yield ``(entity_name, normalized_semantic, raw_semantic)`` triples."""
        for row in self._tag_rows:
            if len(row) < 6:
                continue
            catalog, schema, table, column, tag_name, tag_value = row[:6]
            semantic = _semantic_from_tag(str(tag_name), str(tag_value) if tag_value is not None else "")
            if semantic is None:
                continue
            entity_name = f"{catalog}.{schema}.{table}.{column}"
            yield entity_name, semantic, semantic
        for row in self._classification_rows:
            if len(row) < 5:
                continue
            catalog, schema, table, column, class_tag = row[:5]
            # class_tag may arrive namespaced (``class.us_ssn``) or bare
            # (``us_ssn``); normalise both to the semantic suffix.
            raw = str(class_tag or "")
            if raw.lower().startswith(CLASS_NAMESPACE):
                raw = raw[len(CLASS_NAMESPACE):]
            semantic = _normalize_semantic(raw)
            if not semantic:
                continue
            entity_name = f"{catalog}.{schema}.{table}.{column}"
            yield entity_name, semantic, semantic


class LLMSource(SensitivitySource):
    """Wraps the existing DDL-inference path (demoted, not deleted).

    ``infer`` is a callable ``columns -> list[Finding]`` supplied by the caller,
    which reproduces ``generate_abac``'s DDL name-pattern inference (the
    deterministic backstop for the LLM prompt).  Findings are re-labelled
    :data:`LLM` so provenance is always correct regardless of how ``infer``
    tags them.
    """

    name = LLM

    def __init__(self, infer: Callable[[Sequence[str]], Iterable[Finding]]):
        self._infer = infer

    def findings_for(self, columns: Sequence[str]) -> list[Finding]:
        out: list[Finding] = []
        for f in self._infer(columns) or []:
            out.append(f if f.source == LLM else replace(f, source=LLM))
        return out


def select_findings(
    columns: Sequence[str],
    classification_source: SensitivitySource | None,
    llm_source: SensitivitySource,
) -> list[Finding]:
    """Combine sources with the default classification-else-LLM rule.

    Any column the classification source *claims* (see
    :meth:`SensitivitySource.claimed_columns`) is authoritative and is never
    handed to the LLM — this includes natively-classified columns whose semantic
    is unmapped, which produce no finding but must still block the LLM.  The
    classification source's mapped findings are returned first (in source order),
    then the surviving LLM findings — so when the classification source is empty
    (or ``None``), the result equals ``llm_source.findings_for(columns)`` in the
    same order, keeping legacy behaviour byte-identical.
    """
    class_findings: list[Finding] = (
        classification_source.findings_for(columns) if classification_source is not None else []
    )
    claimed_cols = (
        classification_source.claimed_columns(columns) if classification_source is not None else set()
    )
    result = list(class_findings)
    for f in llm_source.findings_for(columns):
        if f.entity_name not in claimed_cols:
            result.append(f)
    return result


# ---------------------------------------------------------------------------
# System-table reads (used by ClassificationSource.from_run_sql)
# ---------------------------------------------------------------------------
def _quote_list(values: Sequence[str]) -> str:
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)


def _table_fqns_from_refs(table_refs: Sequence[str]) -> list[str]:
    """Keep only concrete ``catalog.schema.table`` refs (drop wildcards)."""
    fqns: list[str] = []
    for ref in table_refs or []:
        parts = str(ref).split(".")
        if len(parts) == 3 and "*" not in parts:
            fqns.append(ref)
    return fqns


def _schema_wildcards_from_refs(table_refs: Sequence[str]) -> list[str]:
    """Return ``catalog.schema`` for each ``catalog.schema.*`` footprint ref."""
    schemas: list[str] = []
    for ref in table_refs or []:
        parts = str(ref).split(".")
        if len(parts) == 3 and parts[2] == "*" and "*" not in parts[:2]:
            schemas.append(f"{parts[0]}.{parts[1]}")
    return schemas


def _table_scope_predicate(table_refs: Sequence[str]) -> str | None:
    """SQL predicate matching concrete tables and ``catalog.schema.*`` scopes."""
    clauses: list[str] = []
    fqns = _table_fqns_from_refs(table_refs)
    if fqns:
        clauses.append(
            f"concat(catalog_name, '.', schema_name, '.', table_name) IN ({_quote_list(fqns)})"
        )
    schemas = _schema_wildcards_from_refs(table_refs)
    if schemas:
        clauses.append(f"concat(catalog_name, '.', schema_name) IN ({_quote_list(schemas)})")
    if not clauses:
        return None
    return "(" + " OR ".join(clauses) + ")"


def _read_class_column_tags(run_sql: Callable[[str], list], table_refs: Sequence[str]) -> list[tuple]:
    """Read ``class.*`` column tags from system.information_schema.column_tags."""
    scope = _table_scope_predicate(table_refs)
    if not scope:
        return []
    sql = f"""\
SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value
FROM system.information_schema.column_tags
WHERE lower(tag_name) LIKE 'class.%'
  AND {scope}
ORDER BY catalog_name, schema_name, table_name, column_name, tag_name"""
    return [tuple(row) for row in (run_sql(sql) or [])]


def _read_data_classification_results(run_sql: Callable[[str], list], table_refs: Sequence[str]) -> list[tuple]:
    """Read semantic types from system.data_classification.results, if present.

    The result's semantic type is the ``class_tag`` column (e.g. ``class.us_ssn``
    or ``us_ssn``) — not ``class_name``.  Callers wrap this in try/except — a
    missing table is not fatal.
    """
    scope = _table_scope_predicate(table_refs)
    if not scope:
        return []
    sql = f"""\
SELECT catalog_name, schema_name, table_name, column_name, class_tag
FROM system.data_classification.results
WHERE {scope}"""
    return [tuple(row) for row in (run_sql(sql) or [])]
