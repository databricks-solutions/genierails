"""Pure construction of deterministic governance: treatments, functions, policies.

Everything here is computed from ``class.*`` tags, column types and reviewed
config; nothing reads Databricks or calls a model.

Naming (the interface the staged-apply controller of step 6 relies on):

* A **treatment value** is ``<treatment>__<type>__<generation>``, e.g.
  ``email_partial__string__3f9a1c2b``. ``<type>`` is the column's exact SQL
  type (``decimal_10_2`` for ``DECIMAL(10,2)``), because Unity Catalog has no
  function overloading and a mask must return the column's type.
  ``<generation>`` is a content hash of the treatment's SQL and protection, so
  changing a mask creates new names instead of replacing live ones.
* Functions are ``gr_mask_<value>_partial`` and ``gr_mask_<value>_full``.
* Policies are ``gr_mask_<catalog>_<value>_partial`` (``TO`` tier-2 groups
  ``EXCEPT`` tier 1, ``raw_exempt_principals`` and the deployer) and
  ``gr_mask_<catalog>_<value>_full`` (``TO account users EXCEPT`` tiers 1 and
  2, ``raw_exempt_principals`` and the deployer). Never-raw treatments get only
  the full policy, with no exceptions at all.

Principals are not part of the generation: a tier move updates a policy's
principals in place and never needs a new function.
"""

from __future__ import annotations

import copy
import functools
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from deterministic_governance import NEVER_RAW_TREATMENTS
from mask_library import load_library, partial_treatments, resolve_class, sql_body, type_family, typed_body

TAG_KEY = "gr_treatment"
FAILSAFE_TREATMENT = "unmapped_redact"
POLICY_COMMENT = "GenieRails deterministic governance policy"
TAG_POLICY_DESCRIPTION = (
    "GenieRails deterministic treatment: <treatment>__<column type>__<generation>."
)
SQL_MARKER = "-- === GenieRails deterministic mask library ==="
UNMAPPED_MARKER = "# gr.classification_unmapped:"
# A string column whose classes keep several different partial treatments;
# the coverage check blocks it unless its gr_treatment has no partial version.
MIXED_PARTIAL_MARKER = "# gr.classification_mixed_partial:"
MIXED_PARTIAL_TREATMENT = "redact"

# Warning-only heuristic.  Names and comments are deliberately not policy
# inputs: they can suggest a review, but never choose a treatment.
FREE_TEXT_NAME_HINTS = frozenset({
    "notes", "comment", "description", "free_text", "memo", "remarks",
    "message", "body", "text",
})
FREE_TEXT_COMMENT_RE = re.compile(
    r"\b(?:free[\s-]?(?:text|form)|unstructured\s+(?:text|notes?)|notes?|comments?|remarks?)\b",
    re.IGNORECASE,
)

_VALUE_RE = re.compile(
    r"(?P<treatment>[a-z0-9_]+?)__(?P<type>[a-z0-9_]+)__(?P<generation>[0-9a-f]{8})"
)
_SCALAR_TYPES = {
    "STRING": "STRING", "VARCHAR": "STRING", "CHAR": "STRING",
    "TINYINT": "TINYINT", "BYTE": "TINYINT", "SMALLINT": "SMALLINT", "SHORT": "SMALLINT",
    "INT": "INT", "INTEGER": "INT", "BIGINT": "BIGINT", "LONG": "BIGINT",
    "FLOAT": "FLOAT", "REAL": "FLOAT", "DOUBLE": "DOUBLE",
    "DATE": "DATE", "TIMESTAMP": "TIMESTAMP", "TIMESTAMP_LTZ": "TIMESTAMP",
    "TIMESTAMP_NTZ": "TIMESTAMP_NTZ", "BOOLEAN": "BOOLEAN", "BINARY": "BINARY",
}


class DeterministicGovernanceError(ValueError):
    """Config or input that deterministic governance refuses to guess about."""


# A customer class (Data Classification's custom classes) as class.* tags carry
# it: lowercase, words joined by "_" (sensitivity_source._normalize_semantic).
_CLASS_KEY = re.compile(r"class\.([a-z0-9][a-z0-9_]*)")
# An adopted custom mask (design 18.4.1, envs/<env>/custom_masks/): the
# reserved prefix, and no "__", which separates the parts of a treatment value.
CUSTOM_TREATMENT = re.compile(r"custom_[a-z0-9]+(?:_[a-z0-9]+)*")


def tag_treatment_errors(tag_treatments, library: dict | None = None) -> list[str]:
    """Schema errors for ``tag_treatments``: the env's own class mappings.

    Customer class mappings are governance config in the env's
    env.auto.tfvars, never in shared/ (product code, replaced on upgrade). A
    key is ``class.<name>``; its value is a mask-library treatment. A class the
    library already maps can't be remapped here (one column changes through
    column_overrides). Tags other than class.* are not read in this version,
    so a mapping for one would mask nothing; it is refused rather than ignored.
    A custom_<name> value names a mask adopted into the env's custom_masks/,
    which ``library`` then holds (scripts/custom_masks.effective_library).
    """
    if tag_treatments is None:
        return []
    if not isinstance(tag_treatments, Mapping):
        return ['tag_treatments must be a map of "class.<name>" to a mask-library treatment']
    data = library or load_library()
    mapped = {name: treatment for treatment, item in data["treatments"].items() for name in item["classes"]}
    errors = []
    for key, treatment in sorted(tag_treatments.items(), key=lambda item: str(item[0])):
        label = f'tag_treatments["{key}"]'
        match = _CLASS_KEY.fullmatch(str(key))
        if not match:
            errors.append(
                f"{label}: keys must be class.<name> (lowercase, as the class.* tag names it); "
                "tags other than class.* are not read in this version, so this mapping would mask nothing")
            continue
        if match.group(1) in mapped:
            errors.append(
                f"{label}: the mask library already maps {key} (to {mapped[match.group(1)]}); "
                "change one column with column_overrides instead")
        if isinstance(treatment, str) and CUSTOM_TREATMENT.fullmatch(treatment) \
                and treatment not in data["treatments"]:
            errors.append(
                f"{label} names {treatment!r}, which is not adopted in this env's custom_masks/. "
                "A drafted custom mask can't be named here until it is adopted: make test-custom-mask, "
                f"then make adopt-custom-mask ENV=dev TREATMENT={treatment} (governance never reads drafts/)")
        elif not isinstance(treatment, str) or treatment not in data["treatments"]:
            errors.append(
                f"{label} names {treatment!r}, which is not a mask-library treatment "
                f"(one of: {', '.join(sorted(data['treatments']))}) or an adopted custom_<name> mask. "
                "A custom mask from make draft-custom-mask can't be named here: it stays a draft for "
                "review until make adopt-custom-mask, and governance never reads drafts/")
    return errors


def with_tag_treatments(library: dict, tag_treatments) -> dict:
    """The mask library with the env's ``tag_treatments`` classes added to their treatments."""
    errors = tag_treatment_errors(tag_treatments, library)
    if errors:
        raise DeterministicGovernanceError("; ".join(errors))
    if not tag_treatments:
        return library
    data = copy.deepcopy(library)
    for key, treatment in sorted(tag_treatments.items()):
        item = data["treatments"][treatment]
        item["classes"] = list(item["classes"]) + [_CLASS_KEY.fullmatch(key).group(1)]
    return data


@dataclass(frozen=True)
class ColumnTreatment:
    """The resolved protection of one governed column."""

    treatment: str
    sql_type: str
    partial: str
    full: str
    never_raw: bool
    # The adopted custom mask's partial expression for this type family
    # (envs/<env>/custom_masks/); None for every library version.
    custom_partial_sql: str | None = None

    @property
    def type_token(self) -> str:
        return re.sub(r"[^a-z0-9]+", "_", self.sql_type.lower()).strip("_")

    @property
    def partial_sql(self) -> str:
        if self.custom_partial_sql and not self.never_raw:
            return typed_body(self.custom_partial_sql, self.sql_type)
        return sql_body(self.full if self.never_raw else self.partial, self.sql_type)

    @property
    def full_sql(self) -> str:
        return sql_body(self.full, self.sql_type)

    @property
    def generation(self) -> str:
        payload = json.dumps(
            [self.treatment, self.sql_type, self.never_raw, self.partial_sql, self.full_sql],
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:8]

    @property
    def value(self) -> str:
        return f"{self.treatment}__{self.type_token}__{self.generation}"


def parse_treatment_value(value: str) -> tuple[str, str, str] | None:
    """``(treatment, type token, generation)`` of a deterministic value, else None."""
    match = _VALUE_RE.fullmatch(value or "")
    return (match["treatment"], match["type"], match["generation"]) if match else None


def is_library_treatment_value(tag_key: str, value: str) -> bool:
    """Whether a ``gr_treatment`` value is deterministic and names a treatment this checkout ships.

    Mode-blind: the account tag policy is shared, so it can hold deterministic
    values next to legacy ones. Callers that know the env's mode also check it.
    A custom_<name> value is accepted by its shape: the account policy is one
    union across envs, and an env that hasn't adopted the mask must still
    re-split it (each env's generate requires its own definition).
    """
    parsed = parse_treatment_value(value)
    return bool(tag_key == TAG_KEY and parsed and (
        parsed[0] in _library_treatments() or CUSTOM_TREATMENT.fullmatch(parsed[0])))


@functools.cache
def _library_treatments() -> frozenset[str]:
    return frozenset(load_library()["treatments"]) | {FAILSAFE_TREATMENT}


def base_treatment(value: str) -> str:
    """The library treatment behind a tag value (legacy values pass through)."""
    parsed = parse_treatment_value(value)
    return parsed[0] if parsed else value


def mixed_override_warning(column: str) -> str:
    return (f'column_overrides["{column}"] is not applied: the column\'s class.* tags keep several partial '
            "treatments, so it is redacted for every masked tier. Remove the extra class.* tag, or override "
            "it to redact or a never-raw treatment")


def keeps_string_partial(value: str) -> bool:
    """Whether a deterministic string value may have a partial version (not redact, not never raw)."""
    parsed = parse_treatment_value(value)
    return bool(parsed and parsed[1] == "string" and parsed[0] != MIXED_PARTIAL_TREATMENT
                and not is_never_raw(value))


def suggests_free_text(column: str, comment: str = "") -> bool:
    """Whether a column name/comment suggests prose (warning-only heuristic)."""
    name = column.rsplit(".", 1)[-1].lower()
    name_words = set(re.findall(r"[a-z0-9]+", name))
    return ("free_text" in name or bool(FREE_TEXT_NAME_HINTS & name_words)
            or bool(FREE_TEXT_COMMENT_RE.search(comment or "")))


def free_text_partial_columns(
    treatments: Mapping[str, ColumnTreatment], comments: Mapping[str, str] | None = None,
) -> list[str]:
    """String partial-mask columns whose name/comment suggests free text.

    This is advisory only.  In particular, it must never participate in
    treatment resolution or generated output.
    """
    notes = {str(k).lower(): str(v) for k, v in (comments or {}).items()}
    return [
        column for column, item in sorted(treatments.items())
        if partial_tier_exposes_fragment(item)
        and suggests_free_text(column, notes.get(column.lower(), ""))
    ]


def mixed_partial_problem(value: str, classes: Sequence[str], library: dict | None = None) -> list[str]:
    """The partial treatments a string column's ``classes`` mix, if its value keeps one.

    Empty when the value is fine: not a deterministic string value, the
    ``redact`` treatment, never raw, or classes that keep at most one partial
    treatment (resolve_column gives every mixed column redact or never-raw).
    """
    if not keeps_string_partial(value):
        return []
    names = sorted({c.removeprefix("class.") for c in classes if c})
    mixed = partial_treatments(names, "STRING", library or load_library())
    return sorted(mixed) if len(mixed) > 1 else []


def is_never_raw(value: str) -> bool:
    treatment = base_treatment(value)
    return treatment in NEVER_RAW_TREATMENTS or treatment == FAILSAFE_TREATMENT


def normalize_sql_type(sql_type: str) -> str:
    """The exact scalar SQL type a mask function is declared with."""
    text = re.sub(r"\s+", "", (sql_type or "").upper())
    base, _, rest = text.partition("(")
    if base in {"DECIMAL", "DEC", "NUMERIC"}:
        digits = re.fullmatch(r"(\d+)(?:,(\d+))?\)", rest) if rest else None
        precision, scale = (digits.group(1), digits.group(2) or "0") if digits else ("10", "0")
        return f"DECIMAL({precision},{scale})"
    if base in _SCALAR_TYPES:
        return _SCALAR_TYPES[base]
    raise DeterministicGovernanceError(
        f"column type {sql_type!r} can't be masked deterministically; only scalar types are supported"
    )


def partial_tier_exposes_fragment(item: ColumnTreatment) -> bool:
    """Whether a middle tier sees something weaker than the full mask."""
    return (item.sql_type == "STRING" and not item.never_raw
            and item.partial_sql != item.full_sql)


def value_sql_type(value: str) -> str | None:
    """The exact SQL type a deterministic treatment value was generated for, else None."""
    parsed = parse_treatment_value(value)
    if not parsed:
        return None
    type_token = parsed[1]
    token_types = {re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_"): value
                   for value in set(_SCALAR_TYPES.values())}
    decimal = re.fullmatch(r"decimal_(\d+)_(\d+)", type_token)
    return f"DECIMAL({decimal.group(1)},{decimal.group(2)})" if decimal else token_types.get(type_token)


def treatment_value_column(
    column: str, value: str, settings: Mapping, library: dict,
) -> ColumnTreatment | None:
    """Reconstruct a generated column treatment for coverage advisories."""
    parsed = parse_treatment_value(value)
    if not parsed:
        return None
    treatment = parsed[0]
    exact_type = value_sql_type(value)
    if not exact_type or treatment not in library["treatments"]:
        return None
    configured = dict(normalize_column_overrides(settings.get("column_overrides")).get(column.lower()) or {})
    configured["treatment"] = treatment
    classes = list(library["treatments"][treatment].get("classes") or [])
    if not classes:
        classes = [next(c for item in library["treatments"].values() for c in item.get("classes", []))]
    candidates = [configured]
    # Mixed-class resolution deliberately forces redact/redacted after applying
    # versions.  Try that unchanged resolver outcome as well; the value hash
    # identifies which effective behavior the generated column actually uses.
    if treatment == MIXED_PARTIAL_TREATMENT:
        candidates.append({"treatment": treatment, "partial": "redacted"})
    for override in candidates:
        item = resolve_column(
            column, classes, exact_type, library=library,
            treatment_versions=settings.get("treatment_versions") or {},
            column_overrides={column.lower(): override},
        )
        if item.value == value:
            return item
    return None


def resolve_column(
    column: str, classes: Sequence[str], sql_type: str, *,
    library: dict | None = None,
    treatment_versions: Mapping[str, Mapping[str, str]] | None = None,
    column_overrides: Mapping[str, Mapping[str, str]] | None = None,
) -> ColumnTreatment:
    """Resolve one column's ``class.*`` tags to its treatment.

    The strictest class wins (mask_library.resolve_class). Any never-raw class
    makes the whole column never raw, and a class the library doesn't map
    makes it the fail-safe never-raw redaction (every tier, including tier 1,
    since an unknown class may be one that must never be raw). A string
    column whose classes keep several partial treatments is ``redact`` (unless
    never raw) whatever ``column_overrides`` or ``treatment_versions`` say:
    no one partial format is safe for all of its classes.
    """
    data = library or load_library()
    exact_type = normalize_sql_type(sql_type)
    family = type_family(exact_type)
    names = sorted({c.removeprefix("class.") for c in classes if c})
    known = {name for item in data["treatments"].values() for name in item["classes"]}
    override = dict((column_overrides or {}).get(column.lower()) or {})
    if "keep_current" in override:
        raise DeterministicGovernanceError(
            f'column_overrides["{column}"].keep_current is not supported by deterministic governance'
        )
    if not names or set(names) - known:
        return ColumnTreatment(FAILSAFE_TREATMENT, exact_type, "redacted", "redacted", True)

    treatment, partial, full, never_raw = resolve_class(names, exact_type, data)
    if "treatment" in override:
        treatment = override["treatment"]
        item = data["treatments"].get(treatment)
        if item is None:
            raise DeterministicGovernanceError(
                f'column_overrides["{column}"].treatment names {treatment!r}, '
                "which the mask library does not ship"
            )
        partial = (item.get("partial_by_type", {}).get(family, item.get("partial"))
                   if family in item["types"] else item["full"])
        full = item["full"]
        never_raw = never_raw or treatment in NEVER_RAW_TREATMENTS
    if never_raw and treatment not in NEVER_RAW_TREATMENTS:
        # Name the column after its never-raw class, so every reader of the
        # value (verification, coverage, reports) sees that it is never raw.
        treatment = max(
            name for name, item in data["treatments"].items()
            if name in NEVER_RAW_TREATMENTS and set(names) & set(item["classes"])
        )
    if not never_raw:
        version = (treatment_versions or {}).get(treatment, {}).get("partial")
        partial = override.get("partial", version or partial)
        if len(partial_treatments(names, exact_type, data)) > 1:
            treatment, partial, full = MIXED_PARTIAL_TREATMENT, "redacted", "redacted"
    # An adopted custom mask's partial version is named after it.
    custom = None if never_raw else (data.get("custom_bodies") or {}).get(partial, {}).get(family)
    return ColumnTreatment(treatment, exact_type, partial or full, full, never_raw, custom)


def build_policies(
    *, values_by_catalog: Mapping[str, Sequence[str]],
    access_tier_groups: Sequence[str], raw_exempt_principals: Sequence[str] = (),
    deployer_principal: str = "", function_schemas: Mapping[str, str] | None = None,
) -> list[dict]:
    """The caller-independent policies for every treatment value in use.

    First tier raw, last tier full, any between partial; a user in several
    tiers gets the most privileged one and anyone outside the tiers gets the
    full version. The two policies of one value never both apply to the same
    principal (``principal_sets_are_disjoint``).
    """
    tiers = list(access_tier_groups)
    tier1, tier2 = tiers[:1], tiers[1:-1]
    deployer = [deployer_principal] if deployer_principal else []
    partial_except = list(dict.fromkeys(tier1 + list(raw_exempt_principals) + deployer))
    full_except = list(dict.fromkeys(tier1 + tier2 + list(raw_exempt_principals) + deployer))
    policies: list[dict] = []
    for catalog in sorted(values_by_catalog):
        for value in sorted(set(values_by_catalog[catalog])):
            never_raw = is_never_raw(value)
            common = {
                "policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": catalog,
                "comment": POLICY_COMMENT,
                "match_condition": f"hasTagValue('{TAG_KEY}', '{value}')",
                "match_alias": "masked_col",
                "function_catalog": catalog,
                "function_schema": (function_schemas or {}).get(catalog, "default"),
            }
            # One policy covers everybody for never-raw data; a second matching
            # policy would itself cause MULTIPLE_MASKS.
            if tier2 and not never_raw:
                policies.append(common | {
                    "name": f"gr_mask_{catalog}_{value}_partial",
                    "to_principals": list(tier2),
                    "except_principals": partial_except,
                    "function_name": f"gr_mask_{value}_partial",
                })
            policies.append(common | {
                "name": f"gr_mask_{catalog}_{value}_full",
                "to_principals": ["account users"],
                "except_principals": [] if never_raw else full_except,
                "function_name": f"gr_mask_{value}_full",
            })
    return policies


def function_schemas_for(columns: Sequence[str]) -> dict[str, str]:
    """Each catalog's functions live in its first governed schema, which exists."""
    schemas: dict[str, str] = {}
    for column in sorted(columns):
        catalog, schema = column.split(".")[:2]
        schemas.setdefault(catalog, schema)
    return schemas


def render_functions(columns: Mapping[str, ColumnTreatment]) -> str:
    """CREATE OR REPLACE statements for exactly the functions the policies use."""
    function_schemas = function_schemas_for(list(columns))
    targets = {(column.split(".", 1)[0], item.value): item for column, item in columns.items()}
    blocks = []
    current = None
    for (catalog, value), item in sorted(targets.items()):
        if catalog != current:
            # The deployer's established context form, as make generate writes.
            blocks.append(f"USE CATALOG {catalog};\nUSE SCHEMA {function_schemas[catalog]};")
            current = catalog
        versions = [("full", item.full_sql)] if item.never_raw else [
            ("partial", item.partial_sql), ("full", item.full_sql)]
        for suffix, body in versions:
            blocks.append(
                f"CREATE OR REPLACE FUNCTION gr_mask_{value}_{suffix}"
                f"(value {item.sql_type}) RETURNS {item.sql_type} RETURN {body};"
            )
    return "\n\n".join(blocks) + ("\n" if blocks else "")


def expand_principals(principals: Sequence[str], members: Mapping[str, Sequence[str]]) -> set[str]:
    """Expand nested account groups transitively (cycles are harmless)."""
    expanded: set[str] = set()
    pending = list(principals)
    while pending:
        principal = pending.pop()
        if principal in expanded:
            continue
        expanded.add(principal)
        pending.extend(members.get(principal, ()))
    return expanded


def principal_sets_are_disjoint(
    partial_to: Sequence[str], partial_except: Sequence[str],
    full_to: Sequence[str], full_except: Sequence[str],
    members: Mapping[str, Sequence[str]] | None = None,
) -> bool:
    """Return whether the effective partial and full populations are disjoint."""
    graph = members or {}
    partial = expand_principals(partial_to, graph) - expand_principals(partial_except, graph)
    full = expand_principals(full_to, graph) - expand_principals(full_except, graph)
    return partial.isdisjoint(full)


_DDL_TABLE_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.`]+)\s*\(",
    re.IGNORECASE,
)


def _ddl_column_definitions(ddl_text: str):
    """Yield ``(table, definition)`` using a quote-aware fetched-DDL lexer."""
    text = ddl_text or ""
    for match in _DDL_TABLE_RE.finditer(text):
        table = match.group(1).replace("`", "")
        parens, angles, quote, line_comment = 1, 0, "", False
        current, parts, index = "", [], match.end()
        while index < len(text):
            char = text[index]
            following = text[index + 1] if index + 1 < len(text) else ""
            if line_comment:
                current += char
                line_comment = char != "\n"
                index += 1
                continue
            if quote:
                current += char
                if char == quote:
                    if following == quote:  # SQL escapes quotes by doubling them.
                        current += following
                        index += 2
                        continue
                    quote = ""
                index += 1
                continue
            if char == "-" and following == "-":
                current += "--"
                line_comment = True
                index += 2
                continue
            if char in "'\"`":
                quote = char
                current += char
            elif char == "<":
                angles += 1
                current += char
            elif char == ">" and angles:
                angles -= 1
                current += char
            elif char == "(":
                parens += 1
                current += char
            elif char == ")":
                parens -= 1
                if parens == 0:
                    if current.strip():
                        parts.append(current)
                    break
                current += char
            elif char == "," and parens == 1 and angles == 0:
                parts.append(current)
                current = ""
            else:
                current += char
            index += 1
        for part in parts:
            clean = " ".join(line.strip() for line in part.splitlines()
                             if not line.strip().startswith("--")).strip()
            tokens = clean.split(None, 1)
            if len(tokens) < 2 or tokens[0].upper() in {"CONSTRAINT", "PRIMARY", "FOREIGN", "UNIQUE", "CHECK"}:
                continue
            yield table, tokens[0].strip('`"'), tokens[1]


def column_types_from_ddl(ddl_text: str) -> dict[str, str]:
    """``{catalog.schema.table.column (lowercase): SQL type}`` from fetched DDL."""
    types: dict[str, str] = {}
    for table, name, remainder in _ddl_column_definitions(ddl_text):
        sql_type = re.split(r"\s+(?:NOT\s+NULL|COMMENT|DEFAULT|GENERATED|MASK)\b", remainder,
                            maxsplit=1, flags=re.IGNORECASE)[0].strip()
        types[f"{table}.{name}".lower()] = sql_type
    return types


_DDL_COMMENT_RE = re.compile(
    r"\bCOMMENT\s+(?P<literal>'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\")",
    re.IGNORECASE,
)


def column_comments_from_ddl(ddl_text: str) -> dict[str, str]:
    """Column comments from the same quote-aware parser used for fetched types."""
    comments: dict[str, str] = {}
    for table, name, remainder in _ddl_column_definitions(ddl_text):
        match = _DDL_COMMENT_RE.search(remainder)
        if match:
            literal = match.group("literal")
            quote = literal[0]
            comments[f"{table}.{name}".lower()] = literal[1:-1].replace(quote * 2, quote)
    return comments


@dataclass(frozen=True)
class DeterministicModel:
    columns: dict[str, ColumnTreatment]
    unmapped: list[tuple[str, str]]
    tag_assignments: list[dict]
    fgac_policies: list[dict]
    tag_values: list[str]
    functions_sql: str
    # (column, its partial treatments) for every column whose classes keep
    # more than one; such a column is redacted (resolve_column).
    mixed_partial: list[tuple[str, list[str]]] = field(default_factory=list)
    # The mixed columns whose column_overrides entry asked for something else.
    overridden_mixed: list[str] = field(default_factory=list)


def normalize_column_overrides(
    configured: Mapping[str, Mapping[str, str]] | None,
) -> dict[str, Mapping[str, str]]:
    """``column_overrides`` keyed by lowercase column (UC identifiers ignore case).

    Two keys that differ only by case are refused, naming every spelling, so
    neither rule can silently win.
    """
    spellings: dict[str, list[str]] = {}
    overrides: dict[str, Mapping[str, str]] = {}
    for column, rule in (configured or {}).items():
        normalized = str(column).lower()
        spellings.setdefault(normalized, []).append(str(column))
        overrides[normalized] = rule
    duplicates = sorted(" and ".join(sorted(names)) for names in spellings.values() if len(names) > 1)
    if duplicates:
        raise DeterministicGovernanceError(
            "column_overrides names the same case-insensitive column more than once: "
            + "; ".join(duplicates)
        )
    return overrides


def build_model(
    classes_by_column: Mapping[str, Sequence[str]],
    types_by_column: Mapping[str, str],
    settings: Mapping,
    *, deployer_principal: str = "", library: dict | None = None,
) -> DeterministicModel:
    """The whole governance model for the classified columns of the governed set.

    ``settings`` is the env config (``access_tier_groups``,
    ``raw_exempt_principals``, ``treatment_versions``, ``column_overrides``,
    ``tag_treatments``). ``library`` is the env's effective library (the mask
    library plus its adopted custom masks); the mask library by default.
    """
    if settings.get("tier_access_overrides"):
        raise DeterministicGovernanceError(
            "tier_access_overrides are deferred (group-level overrides of the tier rule); "
            "remove them for deterministic governance"
        )
    library = with_tag_treatments(library or load_library(), settings.get("tag_treatments"))
    overrides = normalize_column_overrides(settings.get("column_overrides"))
    ddl_columns = {column.lower() for column in types_by_column}
    unknown_overrides = sorted(
        column for column in overrides
        if column.lower() not in ddl_columns
    )
    if unknown_overrides:
        raise DeterministicGovernanceError(
            "column_overrides names column(s) absent from the governed DDL: "
            + ", ".join(unknown_overrides)
        )
    classified_columns = {column.lower() for column in classes_by_column}
    unclassified_overrides = sorted(set(overrides) - classified_columns)
    if unclassified_overrides:
        raise DeterministicGovernanceError(
            "column_overrides names column(s) with no class.* tag in this env: "
            + ", ".join(unclassified_overrides)
        )
    known = {name for item in library["treatments"].values() for name in item["classes"]}
    columns: dict[str, ColumnTreatment] = {}
    unmapped: list[tuple[str, str]] = []
    mixed_partial: list[tuple[str, list[str]]] = []
    overridden_mixed: list[str] = []
    missing_types = []
    for column in sorted(classes_by_column):
        classes = sorted({c.removeprefix("class.") for c in classes_by_column[column] if c})
        unmapped.extend((column, c) for c in classes if c not in known)
        sql_type = types_by_column.get(column.lower())
        if not sql_type:
            missing_types.append(column)
            continue
        columns[column] = resolve_column(
            column, classes, sql_type, library=library,
            treatment_versions=settings.get("treatment_versions") or {},
            column_overrides=overrides,
        )
        mixed = partial_treatments(classes, sql_type, library)
        if len(mixed) > 1:
            mixed_partial.append((column, sorted(mixed)))
            rule = overrides.get(column.lower()) or {}
            item = columns[column]
            if rule.get("treatment", item.treatment) != item.treatment or rule.get("partial", item.partial) != item.partial:
                overridden_mixed.append(column)
    if missing_types:
        raise DeterministicGovernanceError(
            "no column type in the fetched DDL for classified column(s): " + ", ".join(missing_types)
            + "; refresh the DDL before deriving governance"
        )
    values_by_catalog: dict[str, set[str]] = {}
    for column, item in columns.items():
        values_by_catalog.setdefault(column.split(".", 1)[0], set()).add(item.value)
    policies = build_policies(
        values_by_catalog=values_by_catalog,
        access_tier_groups=settings.get("access_tier_groups") or [],
        raw_exempt_principals=settings.get("raw_exempt_principals") or [],
        deployer_principal=deployer_principal, function_schemas=function_schemas_for(list(columns)),
    )
    return DeterministicModel(
        columns=columns,
        unmapped=unmapped,
        tag_assignments=[
            {"entity_type": "columns", "entity_name": column, "tag_key": TAG_KEY, "tag_value": item.value}
            for column, item in sorted(columns.items())
        ],
        fgac_policies=policies,
        tag_values=sorted({item.value for item in columns.values()}),
        functions_sql=render_functions(columns),
        mixed_partial=mixed_partial,
        overridden_mixed=overridden_mixed,
    )


def apply_model(cfg: Mapping, model: DeterministicModel) -> dict:
    """Replace every column mask and treatment fact in ``cfg`` with ``model``.

    Row filters and non-column assignments are kept. Column sensitivity tags
    from the legacy flow (``pii_level`` and the like) are dropped:
    deterministic governance reads only ``class.*`` tags.
    """
    from treatment_derivation import load_treatment_config

    sensitivity_keys = {
        key for treatment in load_treatment_config().treatments for key, _ in treatment.sources
    } | {TAG_KEY}
    result = dict(cfg)
    result["tag_assignments"] = [
        dict(item) for item in cfg.get("tag_assignments") or []
        if not (item.get("entity_type") == "columns" and item.get("tag_key") in sensitivity_keys)
    ] + model.tag_assignments
    result["fgac_policies"] = [
        dict(policy) for policy in cfg.get("fgac_policies") or []
        if policy.get("policy_type") != "POLICY_TYPE_COLUMN_MASK"
    ] + model.fgac_policies
    used_keys = {item.get("tag_key") for item in result["tag_assignments"]}
    result["tag_policies"] = [
        dict(policy) for policy in cfg.get("tag_policies") or []
        if policy.get("key") != TAG_KEY
        and (policy.get("key") not in sensitivity_keys or policy.get("key") in used_keys)
    ]
    if model.tag_values:
        result["tag_policies"].append({
            "key": TAG_KEY, "description": TAG_POLICY_DESCRIPTION, "values": model.tag_values,
        })
    return result


def replace_sql_library(sql_text: str, functions_sql: str) -> str:
    """Swap the deterministic block of masking_functions.sql for ``functions_sql``."""
    base = sql_text.split(SQL_MARKER, 1)[0].rstrip()
    block = f"{SQL_MARKER}\n{functions_sql.rstrip()}" if functions_sql.strip() else ""
    return "\n\n".join(part for part in (base, block) if part) + "\n"


def sql_library_block(sql_text: str) -> str:
    """The deterministic block of masking_functions.sql (without its marker)."""
    _, marker, block = (sql_text or "").partition(SQL_MARKER)
    return block.strip() + "\n" if marker and block.strip() else ""


def model_fingerprint(cfg: Mapping, functions_sql: str) -> str:
    """Identity of a deterministic policy generation: masks, principals and tags."""
    masks = sorted(
        json.dumps(policy, sort_keys=True) for policy in cfg.get("fgac_policies") or []
        if policy.get("policy_type") == "POLICY_TYPE_COLUMN_MASK"
    )
    tags = sorted(
        f"{item.get('entity_name')}={item.get('tag_value')}" for item in cfg.get("tag_assignments") or []
        if item.get("entity_type") == "columns" and item.get("tag_key") == TAG_KEY
    )
    functions = sorted(block.strip() for block in functions_sql.split(";\n") if block.strip())
    payload = json.dumps({"masks": masks, "tags": tags, "functions": functions}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]
