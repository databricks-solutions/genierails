#!/usr/bin/env python3
"""
Validate AI-generated ABAC configuration before terraform apply.

Checks:
  1. abac.auto.tfvars structure and required fields
  2. masking_functions.sql function definitions
  3. Cross-references between both files

Usage:
  pip install python-hcl2          # one-time
  python validate_abac.py abac.auto.tfvars masking_functions.sql
  python validate_abac.py abac.auto.tfvars   # skip SQL check
"""

import sys
import re
import json
import argparse
from pathlib import Path

from tag_vocabulary import REGISTRY
from treatment_derivation import load_treatment_config

try:
    import hcl2
except ImportError:
    print("ERROR: python-hcl2 is required.  Install with:")
    print("  pip install python-hcl2")
    sys.exit(2)

VALID_ENTITY_TYPES = {"tables", "columns"}
VALID_POLICY_TYPES = {"POLICY_TYPE_COLUMN_MASK", "POLICY_TYPE_ROW_FILTER"}
BUILTIN_PRINCIPALS = {"account users"}

COUNTRIES_DIR = Path(__file__).resolve().parent / "countries"
INDUSTRIES_DIR = Path(__file__).resolve().parent / "industries"

COLUMN_MASK_REQUIRED = {"name", "policy_type", "catalog", "to_principals", "match_condition", "match_alias", "function_name", "function_catalog", "function_schema"}
ROW_FILTER_REQUIRED = {"name", "policy_type", "catalog", "to_principals", "function_name", "function_catalog", "function_schema"}


class ValidationResult:
    def __init__(self):
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.info: list[str] = []

    def error(self, msg: str):
        self.errors.append(msg)

    def warn(self, msg: str):
        self.warnings.append(msg)

    def ok(self, msg: str):
        self.info.append(msg)

    @property
    def passed(self) -> bool:
        return len(self.errors) == 0

    def print_report(self, summary_label: str = "", verbose: bool = False):
        if summary_label and self.passed and not self.warnings and not verbose:
            print(f"{summary_label}: PASS ({len(self.info)} checks)")
            return
        width = 60
        print("=" * width)
        print("  ABAC Configuration Validation Report")
        print("=" * width)

        if self.info:
            for line in self.info:
                print(f"  [PASS] {line}")

        if self.warnings:
            print()
            for line in self.warnings:
                print(f"  [WARN] {line}")

        if self.errors:
            print()
            for line in self.errors:
                print(f"  [FAIL] {line}")

        print("-" * width)
        counts = (
            f"{len(self.info)} passed, "
            f"{len(self.warnings)} warnings, "
            f"{len(self.errors)} errors"
        )
        if self.passed:
            print(f"  RESULT: PASS  ({counts})")
        else:
            print(f"  RESULT: FAIL  ({counts})")
        print("=" * width)


def parse_tfvars(path: Path) -> dict:
    with open(path) as f:
        return hcl2.load(f)


def parse_sql_functions(path: Path) -> set[str]:
    """Extract function names from CREATE [OR REPLACE] FUNCTION statements."""
    text = path.read_text()
    pattern = re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+"
        r"(?:[\w]+\.[\w]+\.)?"   # optional catalog.schema. prefix
        r"([\w]+)\s*\(",
        re.IGNORECASE,
    )
    return {m.group(1) for m in pattern.finditer(text)}


def parse_sql_function_arg_counts(path: Path) -> dict[str, int]:
    """Extract function names and their argument counts from SQL file.

    Returns a dict mapping function name to argument count (0 for no args).
    Handles nested parentheses in type declarations like DECIMAL(18,2).
    """
    text = path.read_text()
    # Match function name and the start of the argument list
    pattern = re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+"
        r"(?:[\w]+\.[\w]+\.)?"   # optional catalog.schema. prefix
        r"([\w]+)\s*\(",
        re.IGNORECASE,
    )
    result = {}
    for m in pattern.finditer(text):
        name = m.group(1)
        # Find the matching closing paren, handling nested parens (e.g. DECIMAL(18,2))
        start = m.end()  # position after the opening (
        depth = 1
        pos = start
        while pos < len(text) and depth > 0:
            if text[pos] == '(':
                depth += 1
            elif text[pos] == ')':
                depth -= 1
            pos += 1
        args_str = text[start:pos - 1].strip()
        if not args_str:
            result[name] = 0
        else:
            # Count top-level commas only (skip commas inside nested parens)
            count = 1
            d = 0
            for ch in args_str:
                if ch == '(':
                    d += 1
                elif ch == ')':
                    d -= 1
                elif ch == ',' and d == 0:
                    count += 1
            result[name] = count
    return result


def _extract_tag_refs(condition: str) -> tuple[list[tuple[str, str]], list[str]]:
    """Return (hasTagValue refs, hasTag refs) from a condition string."""
    value_refs = re.findall(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", condition or "")
    key_refs = re.findall(r"hasTag\(\s*'([^']+)'\s*\)", condition or "")
    return value_refs, key_refs


def _condition_expr(condition: str, tags: dict[str, set[str]]) -> str | None:
    """The condition as a Python boolean expression over tags, or None if it
    uses anything outside hasTagValue('k','v'), hasTag('k'), AND, OR and
    parentheses. Tag keys and values match exactly (UC tags are case-sensitive)."""
    def repl_value(match: re.Match) -> str:
        key, value = match.group(1), match.group(2)
        return str(value in tags.get(key, set()))

    def repl_key(match: re.Match) -> str:
        key = match.group(1)
        return str(key in tags and bool(tags[key]))

    expr = re.sub(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", repl_value, condition)
    expr = re.sub(r"hasTag\(\s*'([^']+)'\s*\)", repl_key, expr)
    expr = re.sub(r"\bAND\b", " and ", expr)
    expr = re.sub(r"\bOR\b", " or ", expr)

    # Refuse anything outside the expected boolean grammar.
    if re.search(r"[^()\sA-Za-z]", expr):
        return None
    return expr


def condition_is_supported(condition: str) -> bool:
    """Whether _condition_matches_tags can evaluate the condition at all."""
    if not condition:
        return True
    expr = _condition_expr(condition, {})
    if expr is None:
        return False
    try:
        eval(expr, {"__builtins__": {}}, {})
    except Exception:
        return False
    return True


def _condition_matches_tags(condition: str, tags: dict[str, set[str]]) -> bool:
    """Evaluate a limited ABAC condition against a tag context.

    Supported syntax is intentionally narrow and matches the prompt / validator:
    hasTagValue('k','v'), hasTag('k'), AND, OR, parentheses.
    """
    if not condition:
        return True
    expr = _condition_expr(condition, tags)
    if expr is None:
        return False
    try:
        return bool(eval(expr, {"__builtins__": {}}, {}))
    except Exception:
        return False


def _entity_table_name(entity_type: str, entity_name: str) -> str:
    if entity_type == "tables":
        return entity_name
    if entity_type == "columns":
        return ".".join(entity_name.split(".")[:3])
    return ""


def _entity_tags(assignments) -> tuple[dict[tuple[str, str], dict[str, set[str]]], dict[tuple[str, str], str]]:
    """Tags per entity from tag_assignments, and each entity's first spelling.

    Entities are keyed by (entity_type, casefolded name): Unity Catalog
    identifiers are case-insensitive, so differently cased spellings of one
    column are one entity. Tag keys and values stay exact (UC tags are
    case-sensitive).
    """
    entity_tags: dict[tuple[str, str], dict[str, set[str]]] = {}
    spelling: dict[tuple[str, str], str] = {}
    for assignment in assignments if isinstance(assignments, list) else []:
        if not isinstance(assignment, dict):
            continue
        entity_type = assignment.get("entity_type", "")
        entity_name = assignment.get("entity_name", "")
        tag_key = assignment.get("tag_key", "")
        tag_value = assignment.get("tag_value", "")
        if entity_type and entity_name and tag_key and tag_value:
            entity = (entity_type, entity_name.casefold())
            spelling.setdefault(entity, entity_name)
            entity_tags.setdefault(entity, {}).setdefault(tag_key, set()).add(tag_value)
    return entity_tags, spelling


def _catalog_matches(policy: dict, entity_name: str) -> bool:
    policy_catalog = policy.get("catalog", "") or policy.get("function_catalog", "")
    entity_catalog = entity_name.split(".")[0] if entity_name else ""
    return not policy_catalog or not entity_catalog or policy_catalog.casefold() == entity_catalog.casefold()


def column_mask_matches(cfg: dict) -> dict[str, list[str]]:
    """Column -> names of the column-mask policies that resolve for it.

    As Unity Catalog resolves them: the policy's catalog, the column's own
    tags against the full match_condition (hasTagValue, hasTag, AND, OR,
    parentheses), and its table's tags against when_condition.
    """
    policies = cfg.get("fgac_policies", [])
    if not isinstance(policies, list):
        return {}
    entity_tags, spelling = _entity_tags(cfg.get("tag_assignments", []))
    matches: dict[str, list[str]] = {}
    for (entity_type, entity_key), tags in sorted(entity_tags.items()):
        if entity_type != "columns":
            continue
        entity_name = spelling[(entity_type, entity_key)]
        table_tags = entity_tags.get(("tables", _entity_table_name(entity_type, entity_key)), {})
        names = [
            policy.get("name", "<unnamed>")
            for policy in policies
            if isinstance(policy, dict)
            and policy.get("policy_type") == "POLICY_TYPE_COLUMN_MASK"
            and _catalog_matches(policy, entity_name)
            and _condition_matches_tags(policy.get("match_condition", ""), tags)
            and _condition_matches_tags(policy.get("when_condition", ""), table_tags)
        ]
        if names:
            matches[entity_name] = names
    return matches


def validate_policy_overlaps(cfg: dict, result: ValidationResult):
    """Fail when multiple FGAC policies resolve for one securable object."""
    assignments = cfg.get("tag_assignments", [])
    policies = cfg.get("fgac_policies", [])
    if not isinstance(assignments, list) or not isinstance(policies, list):
        return

    entity_tags, spelling = _entity_tags(assignments)
    mask_matches = column_mask_matches(cfg)

    for (entity_type, entity_key), tags in sorted(entity_tags.items()):
        entity_name = spelling[(entity_type, entity_key)]
        if entity_type == "columns":
            matches = mask_matches.get(entity_name, [])
            if len(set(matches)) > 1:
                result.error(
                    f"Column '{entity_name}' matches multiple column-mask policies: "
                    f"{sorted(set(matches))}. Unity Catalog allows only one mask to "
                    f"resolve per column; otherwise apply fails with MULTIPLE_MASKS. "
                    f"Make these policies' tag conditions mutually exclusive."
                )

        elif entity_type == "tables":
            matches = [
                policy.get("name", "<unnamed>")
                for policy in policies
                if isinstance(policy, dict)
                and policy.get("policy_type") == "POLICY_TYPE_ROW_FILTER"
                and _catalog_matches(policy, entity_name)
                and _condition_matches_tags(policy.get("when_condition", ""), tags)
            ]
            if len(set(matches)) > 1:
                result.error(
                    f"Table '{entity_name}' matches multiple row-filter policies: "
                    f"{sorted(set(matches))}. Unity Catalog allows only one row filter "
                    f"to resolve per table. Make these policies' tag conditions mutually exclusive."
                )


def _value_requires_coverage(tag_value: str) -> bool:
    return tag_value.strip().lower() not in {"public", "general", "exact"}


_DDL_TABLE_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([\w.`]+)\s*\((.*?)\)\s*(?:USING|;|\Z)",
    re.IGNORECASE | re.DOTALL,
)


def parse_ddl_columns(ddl_text: str) -> list[str]:
    """Return ``catalog.schema.table.column`` for every column in fetched DDL."""
    columns: list[str] = []
    for match in _DDL_TABLE_RE.finditer(ddl_text or ""):
        table = match.group(1).replace("`", "")
        for line in match.group(2).split("\n"):
            parts = line.strip().rstrip(",").split()
            if len(parts) < 2 or parts[0].startswith("--"):
                continue
            name = parts[0].strip("`\"")
            if name.upper() in {"CONSTRAINT", "PRIMARY", "FOREIGN", "UNIQUE", "CHECK"}:
                continue
            columns.append(f"{table}.{name}")
    return columns


def find_fetched_ddl(tfvars_path: Path) -> Path | None:
    """Locate ``ddl/_fetched.sql`` next to or one level above the tfvars dir."""
    for base in (tfvars_path.parent, tfvars_path.parent.parent):
        candidate = base / "ddl" / "_fetched.sql"
        if candidate.exists():
            return candidate
    return None


# Name-inferred categories that never block a first exposure. Monetary amounts
# are business measures: the native classifier doesn't tag them and rulebooks
# often leave them unmasked on purpose, so they stay a warning.
FIRST_EXPOSURE_NONBLOCKING_CATEGORIES = {"generic", "amount"}


def load_exposure_context(path: Path) -> dict:
    """Read the --exposure-context JSON scripts/coverage_gate.py writes.

    ``first_exposure_tables`` are tables about to be granted SELECT that the
    data_access state doesn't grant yet; ``acknowledged_columns`` come from
    ``coverage_acknowledged_columns``. Anything malformed raises ValueError.
    """
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise ValueError("exposure context must be a JSON object")
    context = {}
    for key in ("first_exposure_tables", "acknowledged_columns"):
        value = data.get(key, [])
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            raise ValueError(f"exposure context {key} must be a list of strings")
        context[key] = value
    context["acknowledge_file"] = str(data.get("acknowledge_file") or "env.auto.tfvars")
    return context


def _check_unclassified_sensitive_columns(
    ddl_columns: list[str] | None,
    covered_columns: set[str],
    result: ValidationResult,
    exposure: dict | None = None,
) -> None:
    """Surface the fail-open gap for untagged sensitive-looking columns.

    Masks bind only to columns that carry a sensitivity tag / ``gr_treatment``.
    A column whose name looks sensitive (any non-``generic`` category from
    ``_infer_column_categories``) but that has neither is unmasked. The name
    can't prove it is sensitive, so this only warns, except on first exposure:
    with ``exposure``, a table that is about to be granted SELECT for the first
    time blocks until such columns are tagged or acknowledged, because an
    unfinished classification scan looks exactly like this. Missing DDL for
    such a table also blocks.
    """
    exposure = exposure or {}
    first = {t.lower() for t in exposure.get("first_exposure_tables", [])}
    acknowledged = {c.lower() for c in exposure.get("acknowledged_columns", [])}
    if first:
        ddl_tables = {c.rsplit(".", 1)[0].lower() for c in ddl_columns or []}
        unreadable = sorted(first - ddl_tables)
        if unreadable:
            result.error(
                "COVERAGE CHECK — first exposure blocked: no fetched DDL "
                "(ddl/_fetched.sql) for these tables, so their columns can't be "
                "checked before SELECT is granted:\n    - " + "\n    - ".join(unreadable)
                + "\n  derive-assignments refreshes ddl/_fetched.sql from Unity Catalog; "
                "re-run the same make command (or make derive-assignments) and check "
                "that these tables exist and the deployment SP can read them."
            )
    if not ddl_columns:
        return
    covered = {c.lower() for c in covered_columns}
    gaps, blocked, reviewed = [], [], []
    for column in ddl_columns:
        if column.lower() in covered:
            continue
        categories = _infer_column_categories(column) - {"generic"}
        if not categories:
            continue
        if column.lower() in acknowledged:
            reviewed.append(column)
            continue
        label = f"{column} (looks like: {', '.join(sorted(categories))})"
        table = column.rsplit(".", 1)[0].lower()
        if table in first and categories - FIRST_EXPOSURE_NONBLOCKING_CATEGORIES:
            blocked.append((column, label))
        else:
            gaps.append(label)
    if reviewed:
        result.ok(
            f"Coverage check: {len(reviewed)} acknowledged column(s) not treated as "
            f"sensitive (coverage_acknowledged_columns): {', '.join(reviewed)}"
        )
    if blocked:
        names = ", ".join(json.dumps(column) for column, _ in blocked)
        result.error(
            "COVERAGE CHECK — first exposure blocked: these tables are about to be "
            "granted SELECT for the first time, and these sensitive-looking columns "
            "have NO class.*/sensitivity tag and NO gr_treatment, so they would be "
            "readable unmasked:\n    - " + "\n    - ".join(label for _, label in blocked)
            + "\n  Do one of the following, then re-run the same make command (it "
            "re-derives tags from Unity Catalog):"
            "\n    1. Wait for the UC Data Classification scan to finish and tag these "
            "columns (Catalog Explorer > catalog > Data classification; auto-tagging "
            "writes the class.* tags)."
            "\n    2. Tag the columns in Unity Catalog yourself with a class.* tag your "
            "treatment rules map."
            "\n    3. If a column is not sensitive, acknowledge it in "
            f"{exposure.get('acknowledge_file', 'env.auto.tfvars')}:"
            f"\n         coverage_acknowledged_columns = [{names}]"
        )
    if gaps:
        result.warn(
            "COVERAGE CHECK (non-blocking) — sensitive-looking columns with NO "
            "sensitivity/class.* tag and NO gr_treatment; these are NOT masked "
            "(fail-open):\n    - " + "\n    - ".join(gaps)
        )


def validate_coverage_gate(
    cfg: dict,
    sql_functions: set[str] | None,
    raw_tfvars: str,
    result: ValidationResult,
    ddl_columns: list[str] | None = None,
    exposure: dict | None = None,
    env_name: str = "",
    env_dir: Path | None = None,
) -> None:
    """Block every classification/treatment coverage gap in generated config.

    With ``env_name`` and ``env_dir``, a gap error also prints how to fix it
    (scripts/coverage_fix.py) with exact commands for that env.

    When ``ddl_columns`` is supplied, additionally warn about sensitive-looking
    columns that carry no sensitivity tag and no treatment. With ``exposure``
    (see load_exposure_context), such columns block instead on tables that are
    about to be granted SELECT for the first time.
    """
    treatment_cfg = load_treatment_config()
    mapped_sources = {source for item in treatment_cfg.treatments for source in item.sources}
    treatment_functions = {item.value: item.masking_function for item in treatment_cfg.treatments}
    assignments = cfg.get("tag_assignments", []) or []
    policies = cfg.get("fgac_policies", []) or []

    source_columns: dict[str, list[tuple[str, str]]] = {}
    treatments: dict[str, str] = {}
    for item in assignments:
        if item.get("entity_type") != "columns":
            continue
        column = item.get("entity_name", "")
        source = (item.get("tag_key", ""), item.get("tag_value", ""))
        if source in mapped_sources:
            source_columns.setdefault(column, []).append(source)
        if item.get("tag_key") == treatment_cfg.tag_key:
            treatments[column] = item.get("tag_value", "")

    unmapped = [
        (column.strip(), detected.strip())
        for column, detected in re.findall(
            r"^\s*#\s*gr\.classification_unmapped:\s*([^|\n]+)\|([^\n]+)$",
            raw_tfvars,
            re.MULTILINE,
        )
    ]
    unprotected: list[str] = []
    missing_policies: set[str] = set()
    missing_functions: set[str] = set()
    policy_quota: list[str] = []

    by_catalog: dict[str, list[str]] = {}
    for policy in policies:
        catalog = policy.get("catalog", "") or policy.get("function_catalog", "")
        if catalog:
            by_catalog.setdefault(catalog, []).append(policy.get("name", "<unnamed>"))
    for catalog, names in sorted(by_catalog.items()):
        if len(names) > 100:
            policy_quota.append(
                f"catalog {catalog}: {len(names)} policies (limit 100): {', '.join(names)}"
            )

    protected_columns = set(source_columns) | set(treatments)
    for column in sorted(protected_columns):
        sources = source_columns.get(column, [])
        treatment = treatments.get(column)
        if not treatment:
            unprotected.append(f"{column} (detected: {', '.join(f'{k}={v}' for k, v in sources)}; no gr_treatment)")
            continue
        catalog = column.split(".", 1)[0]
        matching = [
            p for p in policies
            if p.get("policy_type") == "POLICY_TYPE_COLUMN_MASK"
            and p.get("catalog") == catalog
            and (treatment_cfg.tag_key, treatment) in _extract_tag_refs(p.get("match_condition", ""))[0]
        ]
        if not matching:
            missing_policies.add(f"{treatment} (catalog {catalog}; used by {column})")
            unprotected.append(f"{column} (treatment {treatment}; no covering column-mask policy)")
            continue
        expected_fn = treatment_functions.get(treatment)
        if not expected_fn:
            missing_policies.add(f"{treatment} (no treatment mapping/rule)")
            unprotected.append(f"{column} (unknown gr_treatment {treatment}; no treatment mapping/rule)")
        elif not any(p.get("function_name") == expected_fn for p in matching):
            missing_policies.add(
                f"{treatment} (catalog {catalog}; expected masking function {expected_fn})"
            )
            unprotected.append(
                f"{column} (treatment {treatment}; covering policy does not resolve to {expected_fn})"
            )
        elif sql_functions is None or expected_fn not in sql_functions:
            missing_functions.add(f"{expected_fn} (treatment {treatment}; used by {column})")
            unprotected.append(f"{column} (treatment {treatment}; masking function {expected_fn} missing)")

    _check_unclassified_sensitive_columns(
        ddl_columns, set(source_columns) | set(treatments), result, exposure,
    )

    groups = [
        ("detected tags with no mapping/rule", [f"{c} ({d})" for c, d in unmapped]),
        ("classified but unprotected columns", unprotected),
        ("treatments missing a column-mask policy", sorted(missing_policies)),
        ("treatments missing a masking function", sorted(missing_functions)),
        ("Unity Catalog policy quota exceeded", policy_quota),
    ]
    fixes: dict[str, list[str]] = {}
    if env_name and env_dir is not None and (unmapped or missing_policies):
        from scripts.coverage_fix import fix_lines, materialize_lines

        if unmapped:
            fixes["detected tags with no mapping/rule"] = fix_lines(
                env_name, env_dir, unmapped, config=treatment_cfg,
            )
        missing_treatments = sorted({
            item.split(" ", 1)[0] for item in missing_policies if "; used by " in item
        })
        if missing_treatments:
            fixes["treatments missing a column-mask policy"] = materialize_lines(
                env_name, env_dir, missing_treatments,
            )
    for title, items in groups:
        if items:
            result.error(
                f"COVERAGE CHECK — {title}:\n    - " + "\n    - ".join(items)
                + "".join("\n" + line for line in fixes.get(title, []))
            )
    has_column_mask_policy = any(
        policy.get("policy_type") == "POLICY_TYPE_COLUMN_MASK"
        for policy in policies
    )
    if has_column_mask_policy and not protected_columns:
        result.error(
            "COVERAGE CHECK — zero classified/treatment columns were supplied; "
            "refusing a vacuous pass (populate assignments from classification before retrying)"
        )
    elif not any(items for _, items in groups):
        result.ok(f"Coverage check: {len(protected_columns)} classified/treatment column(s) fully protected")


def _load_country_categories(
    country_codes: list[str],
) -> tuple[dict[str, str], dict[str, set[str]]]:
    """Load country overlays and return (hint→category, function→categories) mappings.

    hint_to_category: maps column name substrings (e.g. "tfn") to category strings
                      (e.g. "government_id") for extending _infer_column_categories.
    func_to_categories: maps masking function names (e.g. "mask_tfn") to the set of
                        categories they are expected to be applied to.
    """
    try:
        import yaml
    except ImportError:
        print("  WARNING: pyyaml not installed — skipping country-aware validation")
        return {}, {}

    hint_to_category: dict[str, str] = {}
    func_to_categories: dict[str, set[str]] = {}

    for code in country_codes:
        code_upper = code.strip().upper()
        yaml_path = COUNTRIES_DIR / f"{code_upper}.yaml"
        if not yaml_path.exists():
            continue

        with open(yaml_path) as f:
            data = yaml.safe_load(f)

        for ident in data.get("identifiers", []):
            category = ident.get("category", "")
            if not category:
                continue
            for hint in ident.get("column_hints", []):
                hint_to_category[hint.lower()] = category
            fn = ident.get("masking_function")
            if fn:
                func_to_categories.setdefault(fn, set()).add(category)

    return hint_to_category, func_to_categories


def _load_industry_categories(
    industry_codes: list[str],
) -> tuple[dict[str, str], dict[str, set[str]]]:
    """Load industry overlays and return (hint→category, function→categories) mappings.

    Same structure as _load_country_categories but reads from shared/industries/
    with lowercase codes.
    """
    try:
        import yaml
    except ImportError:
        print("  WARNING: pyyaml not installed — skipping industry-aware validation")
        return {}, {}

    hint_to_category: dict[str, str] = {}
    func_to_categories: dict[str, set[str]] = {}

    for code in industry_codes:
        code_lower = code.strip().lower()
        yaml_path = INDUSTRIES_DIR / f"{code_lower}.yaml"
        if not yaml_path.exists():
            continue

        with open(yaml_path) as f:
            data = yaml.safe_load(f)

        for ident in data.get("identifiers", []):
            category = ident.get("category", "")
            if not category:
                continue
            for hint in ident.get("column_hints", []):
                hint_to_category[hint.lower()] = category
            fn = ident.get("masking_function")
            if fn:
                func_to_categories.setdefault(fn, set()).add(category)

    return hint_to_category, func_to_categories


# Country/industry-specific hint→category mapping, populated when --country/--industry is used.
_country_hint_to_category: dict[str, str] = {}


def _infer_column_categories(entity_name: str) -> set[str]:
    col = entity_name.split(".")[-1].lower()
    categories: set[str] = set()
    if "email" in col:
        categories.add("email")
    if "phone" in col or "mobile" in col:
        categories.add("phone")
    if "ssn" in col or "social_security" in col:
        categories.add("ssn")
    if "name" in col:
        categories.add("name")
    if "address" in col:
        categories.add("address")
    if "birth" in col or col in {"dob", "date_of_birth"}:
        categories.add("date")
    if (("card" in col and "cardholder" not in col and "card_holder" not in col)
            or "cvv" in col or "pan" in col):
        categories.add("card")
    if any(hint in col for hint in ("amount", "balance", "limit", "salary", "wage", "compensation")):
        categories.add("amount")
    # Country-specific patterns (populated by --country flag)
    for hint, category in _country_hint_to_category.items():
        if hint in col:
            categories.add(category)
    return categories or {"generic"}


GENERIC_SAFE_FUNCTIONS = {"mask_pii_partial", "mask_redact", "mask_nullify", "mask_hash"}
FUNCTION_EXPECTED_CATEGORIES = {
    "mask_email": {"email", "customer_pii", "customer_profile"},
    "mask_phone": {"phone", "customer_pii", "customer_profile"},
    "mask_ssn": {"ssn", "customer_pii", "customer_profile"},
    "mask_ssn_last4": {"ssn", "customer_pii", "customer_profile"},
    "mask_full_name": {"name", "customer_pii", "customer_profile"},
    "mask_name": {"name", "customer_pii", "customer_profile"},
    "mask_credit_card_full": {"card", "payment_card"},
    "mask_credit_card_last4": {"card", "payment_card"},
    "mask_card_last4": {"card", "payment_card"},
    "mask_amount_rounded": {"amount", "transaction"},
    "mask_date_to_year": {"date", "customer_pii", "patient_pii", "generic"},
    "mask_dob_year": {"date", "customer_pii", "patient_pii"},
    "mask_timestamp_to_day": {"date"},
    # India PAN (Permanent Account Number) is a government ID but the LLM
    # often groups it with credit-card columns under the same tag value in
    # banking schemas.  Accept card categories to avoid false-positive mismatches.
    "mask_pan_india": {"government_id", "card", "payment_card"},
}


def validate_groups(cfg: dict, result: ValidationResult):
    groups = cfg.get("groups")
    if not groups:
        referenced = {
            principal
            for policy in (cfg.get("fgac_policies") or [])
            for field in ("to_principals", "except_principals")
            for principal in (policy.get(field) or [])
        }
        if referenced and {principal.lower() for principal in referenced} <= BUILTIN_PRINCIPALS:
            result.ok("groups: built-in principals only (no managed group definitions required)")
            return set()
        result.error("'groups' is missing or empty — at least one group is required")
        return set()
    if not isinstance(groups, dict):
        result.error("'groups' must be a map of group_name -> { description = \"...\" }")
        return set()
    for name, val in groups.items():
        if not isinstance(val, dict):
            result.error(f"groups[\"{name}\"] must be an object with a 'description' key")
    result.ok(f"groups: {len(groups)} group(s) defined")
    return set(groups.keys())


def validate_tag_policies(cfg: dict, result: ValidationResult) -> dict[str, set[str]]:
    """Returns a map of tag_key -> set of allowed values."""
    policies = cfg.get("tag_policies", [])
    if not isinstance(policies, list):
        result.error("'tag_policies' must be a list")
        return {}
    tag_map: dict[str, set[str]] = {}
    seen_keys: set[str] = set()
    seen_canonical_keys: dict[str, str] = {}
    for i, tp in enumerate(policies):
        key = tp.get("key", "")
        if not key:
            result.error(f"tag_policies[{i}]: 'key' is missing")
            continue
        if key in seen_keys:
            result.error(f"tag_policies[{i}]: duplicate key '{key}'")
        seen_keys.add(key)
        canonical_key = REGISTRY.canonical_key(key)
        if canonical_key != key:
            result.error(
                f"tag_policies[{i}]: key '{key}' is non-canonical — use '{canonical_key}'"
            )
        elif canonical_key in seen_canonical_keys:
            result.error(
                f"tag_policies[{i}]: canonical key '{canonical_key}' already defined by "
                f"'{seen_canonical_keys[canonical_key]}'"
            )
        else:
            seen_canonical_keys[canonical_key] = key
        values = tp.get("values", [])
        if not values:
            result.error(f"tag_policies[{i}] (key='{key}'): 'values' is empty")
        for value in values or []:
            canonical_value = REGISTRY.canonical_value(canonical_key, value)
            if canonical_value != value:
                result.error(
                    f"tag_policies[{i}] (key='{key}'): value '{value}' is non-canonical — "
                    f"use '{canonical_value}'"
                )
            elif REGISTRY.is_allowed_value(canonical_key, value) is False:
                result.error(
                    f"tag_policies[{i}] (key='{key}'): value '{value}' is not in the "
                    f"canonical registry {sorted(REGISTRY.canonical_values_for_key(canonical_key) or [])}"
                )
        tag_map[key] = set(values)
    result.ok(f"tag_policies: {len(policies)} policy/ies, {sum(len(v) for v in tag_map.values())} total values")
    return tag_map


def validate_tag_assignments(cfg: dict, tag_map: dict[str, set[str]], result: ValidationResult):
    assignments = cfg.get("tag_assignments", [])
    if not isinstance(assignments, list):
        result.error("'tag_assignments' must be a list")
        return
    seen_keys: set[str] = set()
    entity_tag_values: dict[tuple[str, str, str], set[str]] = {}
    for i, ta in enumerate(assignments):
        prefix = f"tag_assignments[{i}]"
        etype = ta.get("entity_type", "")
        ename = ta.get("entity_name", "")
        tkey = ta.get("tag_key", "")
        tval = ta.get("tag_value", "")
        canonical_key = REGISTRY.canonical_key(tkey)
        canonical_value = REGISTRY.canonical_value(canonical_key, tval)

        if etype not in VALID_ENTITY_TYPES:
            result.error(f"{prefix}: entity_type '{etype}' invalid — must be 'tables' or 'columns'")

        dot_count = ename.count(".")
        if etype == "tables" and dot_count != 2:
            result.error(
                f"{prefix}: entity_name '{ename}' must be fully qualified "
                f"as 'catalog.schema.table' (expected 2 dots, got {dot_count})"
            )
        if etype == "columns" and dot_count != 3:
            result.error(
                f"{prefix}: entity_name '{ename}' must be fully qualified "
                f"as 'catalog.schema.table.column' (expected 3 dots, got {dot_count})"
            )

        if canonical_key != tkey:
            result.error(f"{prefix}: tag_key '{tkey}' is non-canonical — use '{canonical_key}'")
        if canonical_value != tval:
            result.error(
                f"{prefix}: tag_value '{tval}' is non-canonical for tag_key '{tkey}' — "
                f"use '{canonical_value}'"
            )
        elif REGISTRY.is_allowed_value(canonical_key, tval) is False:
            result.error(
                f"{prefix}: tag_value '{tval}' is not in the canonical registry for "
                f"tag_key '{canonical_key}' — allowed: "
                f"{sorted(REGISTRY.canonical_values_for_key(canonical_key) or [])}"
            )

        if tkey and tkey not in tag_map:
            result.error(f"{prefix}: tag_key '{tkey}' not defined in tag_policies")
        elif tkey and tval and tval not in tag_map.get(tkey, set()):
            result.error(
                f"{prefix}: tag_value '{tval}' is not an allowed value for "
                f"tag_key '{tkey}' — allowed: {sorted(tag_map[tkey])}"
            )

        composite = f"{etype}|{ename}|{tkey}|{tval}"
        if composite in seen_keys:
            result.warn(f"{prefix}: duplicate assignment ({etype}, {ename}, {tkey}={tval})")
        seen_keys.add(composite)

        if etype and ename and tkey and tval:
            bucket = entity_tag_values.setdefault((etype, ename, tkey), set())
            bucket.add(tval)
            if len(bucket) > 1:
                result.error(
                    f"{prefix}: entity '{ename}' has multiple values for tag_key '{tkey}' "
                    f"({sorted(bucket)}). Choose exactly one value per tag_key per entity."
                )

    result.ok(f"tag_assignments: {len(assignments)} assignment(s)")


def validate_fgac_policies(
    cfg: dict,
    group_names: set[str],
    tag_map: dict[str, set[str]],
    sql_functions: set[str] | None,
    result: ValidationResult,
    sql_function_arg_counts: dict[str, int] | None = None,
):
    policies = cfg.get("fgac_policies", [])
    if not isinstance(policies, list):
        result.error("'fgac_policies' must be a list")
        return
    seen_names: set[str] = set()
    referenced_functions: set[str] = set()

    for i, p in enumerate(policies):
        name = p.get("name", "")
        prefix = f"fgac_policies[{i}] (name='{name}')"
        ptype = p.get("policy_type", "")

        if not name:
            result.error(f"fgac_policies[{i}]: 'name' is missing")
        if name in seen_names:
            result.error(f"{prefix}: duplicate policy name")
        seen_names.add(name)

        if ptype not in VALID_POLICY_TYPES:
            result.error(f"{prefix}: policy_type '{ptype}' invalid — must be one of {sorted(VALID_POLICY_TYPES)}")
            continue

        provided = {k for k, v in p.items() if v is not None and v != "" and v != []}

        if ptype == "POLICY_TYPE_COLUMN_MASK":
            missing = COLUMN_MASK_REQUIRED - provided
            if missing:
                result.error(f"{prefix}: COLUMN_MASK requires {sorted(missing)}")
        elif ptype == "POLICY_TYPE_ROW_FILTER":
            missing = ROW_FILTER_REQUIRED - provided
            if missing:
                result.error(f"{prefix}: ROW_FILTER requires {sorted(missing)}")

        # Validate principals reference existing groups
        for principal in p.get("to_principals", []):
            if principal.lower() not in BUILTIN_PRINCIPALS and principal not in group_names:
                result.error(
                    f"{prefix}: to_principals group '{principal}' not defined in 'groups'"
                )
        for principal in p.get("except_principals", []) or []:
            if principal.lower() not in BUILTIN_PRINCIPALS and principal not in group_names:
                result.error(
                    f"{prefix}: except_principals group '{principal}' not defined in 'groups'"
                )

        # Validate condition syntax — only hasTagValue() and hasTag() are allowed
        condition = p.get("match_condition") or p.get("when_condition") or ""
        for forbidden in ["columnName()", "tableName()", " IN (", " IN("]:
            if forbidden in condition:
                result.error(
                    f"{prefix}: condition contains '{forbidden}' which is NOT supported "
                    f"by Databricks ABAC. Only hasTagValue() and hasTag() are allowed."
                )
        for tag_ref in re.findall(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", condition):
            ref_key, ref_val = tag_ref
            canonical_key = REGISTRY.canonical_key(ref_key)
            canonical_value = REGISTRY.canonical_value(canonical_key, ref_val)
            if canonical_key != ref_key:
                result.error(
                    f"{prefix}: condition uses non-canonical tag_key '{ref_key}' — "
                    f"use '{canonical_key}'"
                )
            if canonical_value != ref_val:
                result.error(
                    f"{prefix}: condition uses non-canonical tag_value '{ref_val}' for "
                    f"tag_key '{ref_key}' — use '{canonical_value}'"
                )
            elif REGISTRY.is_allowed_value(canonical_key, ref_val) is False:
                result.error(
                    f"{prefix}: condition references canonical tag_value '{ref_val}' "
                    f"outside the registry for '{canonical_key}' — allowed: "
                    f"{sorted(REGISTRY.canonical_values_for_key(canonical_key) or [])}"
                )
            if ref_key not in tag_map:
                result.error(f"{prefix}: condition references undefined tag_key '{ref_key}'")
            elif ref_val not in tag_map.get(ref_key, set()):
                result.error(
                    f"{prefix}: condition references tag_value '{ref_val}' "
                    f"not in tag_policy '{ref_key}' — allowed: {sorted(tag_map[ref_key])}"
                )
        for tag_ref in re.findall(r"hasTag\(\s*'([^']+)'\s*\)", condition):
            canonical_key = REGISTRY.canonical_key(tag_ref)
            if canonical_key != tag_ref:
                result.error(
                    f"{prefix}: condition uses non-canonical tag_key '{tag_ref}' — "
                    f"use '{canonical_key}'"
                )
            if tag_ref not in tag_map:
                result.error(f"{prefix}: condition references undefined tag_key '{tag_ref}'")

        fn = p.get("function_name", "")
        if fn:
            referenced_functions.add(fn)
            if "." in fn:
                result.error(
                    f"{prefix}: function_name '{fn}' should be relative (no dots) — "
                    f"Terraform prepends catalog.schema automatically"
                )
            # Validate function argument count matches policy type.
            # A row filter with match_alias binds that matched column through
            # `using`; a row filter without an alias remains zero-argument.
            # Column masks get the column passed implicitly via on_column,
            # so the function must take exactly 1 arg.
            if sql_function_arg_counts and fn in sql_function_arg_counts:
                argc = sql_function_arg_counts[fn]
                expected_row_filter_args = 1 if p.get("match_alias") else 0
                if ptype == "POLICY_TYPE_ROW_FILTER" and argc != expected_row_filter_args:
                    result.warn(
                        f"{prefix}: ROW_FILTER function '{fn}' takes {argc} argument(s) "
                        f"but this policy binds {expected_row_filter_args} argument(s)."
                    )
                elif ptype == "POLICY_TYPE_COLUMN_MASK" and argc != 1:
                    result.warn(
                        f"{prefix}: COLUMN_MASK function '{fn}' takes {argc} argument(s) "
                        f"but column masks require exactly 1-argument functions."
                    )

    # Cross-reference with SQL file
    if sql_functions is not None:
        for fn in referenced_functions:
            if fn not in sql_functions:
                result.error(
                    f"function '{fn}' referenced in fgac_policies but not found "
                    f"in SQL file — define it with CREATE OR REPLACE FUNCTION {fn}(...)"
                )
        unused = sql_functions - referenced_functions
        if unused:
            result.warn(
                f"SQL file defines functions not used by any policy: {sorted(unused)}. "
                f"These will be created but won't mask anything."
            )

    # Coverage gap analysis and function/category safety checks.
    assignments = cfg.get("tag_assignments", [])
    entity_tags: dict[tuple[str, str], dict[str, set[str]]] = {}
    for ta in assignments:
        etype = ta.get("entity_type", "")
        ename = ta.get("entity_name", "")
        tkey = ta.get("tag_key", "")
        tval = ta.get("tag_value", "")
        if not (etype and ename and tkey and tval):
            continue
        per_entity = entity_tags.setdefault((etype, ename), {})
        per_entity.setdefault(tkey, set()).add(tval)

    def policy_matches_assignment(policy: dict, assignment: dict) -> bool:
        policy_catalog = policy.get("catalog", "") or policy.get("function_catalog", "")
        entity_name = assignment.get("entity_name", "")
        entity_type = assignment.get("entity_type", "")
        entity_catalog = entity_name.split(".")[0] if entity_name else ""
        if policy_catalog and entity_catalog and policy_catalog != entity_catalog:
            return False

        table_name = _entity_table_name(entity_type, entity_name)
        table_tags = entity_tags.get(("tables", table_name), {})
        if entity_type == "columns":
            if policy.get("policy_type") != "POLICY_TYPE_COLUMN_MASK":
                return False
            column_tags = entity_tags.get(("columns", entity_name), {})
            if not _condition_matches_tags(policy.get("match_condition", ""), column_tags):
                return False
            return _condition_matches_tags(policy.get("when_condition", ""), table_tags)

        if entity_type == "tables":
            when_condition = policy.get("when_condition", "")
            if not when_condition:
                return False
            return _condition_matches_tags(when_condition, table_tags)

        return False

    for i, ta in enumerate(assignments):
        tval = ta.get("tag_value", "")
        if not _value_requires_coverage(tval):
            continue
        if not any(policy_matches_assignment(p, ta) for p in policies):
            result.error(
                f"tag_assignments[{i}]: non-public tag '{ta.get('tag_key')}={tval}' on "
                f"'{ta.get('entity_name')}' is not covered by any active fgac_policy"
            )

    validate_policy_overlaps(cfg, result)

    # Detect unsafe tag/function mismatches, especially heterogeneous contact collapse.
    assignments_by_tag: dict[tuple[str, str], list[dict]] = {}
    for ta in assignments:
        if ta.get("entity_type") != "columns":
            continue
        assignments_by_tag.setdefault((ta.get("tag_key", ""), ta.get("tag_value", "")), []).append(ta)

    for p in policies:
        if p.get("policy_type") != "POLICY_TYPE_COLUMN_MASK":
            continue
        fn = p.get("function_name", "")
        if fn in GENERIC_SAFE_FUNCTIONS:
            continue
        value_refs, key_refs = _extract_tag_refs(p.get("match_condition", ""))
        matched_assignments: list[dict] = []
        for key, value in value_refs:
            matched_assignments.extend(assignments_by_tag.get((key, value), []))
        for key in key_refs:
            for (tag_key, _tag_value), items in assignments_by_tag.items():
                if tag_key == key:
                    matched_assignments.extend(items)
        if not matched_assignments:
            continue
        categories = set()
        for ta in matched_assignments:
            categories.update(_infer_column_categories(ta.get("entity_name", "")))
        expected = FUNCTION_EXPECTED_CATEGORIES.get(fn)
        if expected and not categories.issubset(expected):
            result.error(
                f"fgac policy '{p.get('name', '')}' uses function '{fn}' for columns with "
                f"categories {sorted(categories)}; expected only {sorted(expected)}"
            )

    # Per-catalog FGAC policy count check (Databricks enforces max 10 per catalog)
    catalog_counts: dict[str, int] = {}
    for p in policies:
        if not isinstance(p, dict):
            continue
        cat = p.get("catalog", "") or "_unknown"
        if isinstance(cat, list):
            cat = cat[0] if cat else "_unknown"
        catalog_counts[cat] = catalog_counts.get(cat, 0) + 1
    # Platform limits: 100 per catalog, 50 per table (soft limits).
    # Ref: https://docs.databricks.com/en/data-governance/unity-catalog/abac/policies#policy-quotas
    for cat, count in sorted(catalog_counts.items()):
        if count > 100:
            result.error(
                f"Catalog '{cat}': {count} fgac_policies exceeds Databricks platform limit of 100. "
                f"Consolidate policies or split governance across multiple catalogs."
            )
        elif count > 80:
            result.warn(
                f"Catalog '{cat}': {count} fgac_policies is close to the platform limit of 100. "
                f"Consider consolidating to leave headroom."
            )

    result.ok(f"fgac_policies: {len(policies)} policy/ies, {len(referenced_functions)} unique function(s)")


def validate_group_members(cfg: dict, group_names: set[str], result: ValidationResult):
    members = cfg.get("group_members", {})
    if not isinstance(members, dict):
        result.error("'group_members' must be a map of group_name -> list of user IDs")
        return
    for grp, ids in members.items():
        if grp not in group_names:
            result.error(f"group_members: group '{grp}' not defined in 'groups'")
        if not isinstance(ids, list) or not all(isinstance(x, str) for x in ids):
            result.error(f"group_members[\"{grp}\"]: must be a list of user ID strings")
    if members:
        result.ok(f"group_members: {len(members)} group(s) with member assignments")


def validate_acl_groups(cfg: dict, group_names: set[str], result: ValidationResult):
    """Validate acl_groups in genie_space_configs reference defined groups."""
    gsc = cfg.get("genie_space_configs") or {}
    if isinstance(gsc, list):
        gsc = gsc[0] if gsc else {}
    if not isinstance(gsc, dict) or not gsc:
        return
    for space_name, space_cfg in gsc.items():
        if isinstance(space_cfg, list):
            space_cfg = space_cfg[0] if space_cfg else {}
        acl = space_cfg.get("acl_groups") or []
        if isinstance(acl, list) and acl:
            if isinstance(acl[0], list):
                acl = acl[0]
            for g in acl:
                if g not in group_names and g.lower() not in {"account users"}:
                    result.error(
                        f"genie_space_configs[\"{space_name}\"].acl_groups: "
                        f"group '{g}' not defined in 'groups'"
                    )
            if acl:
                result.ok(f"genie_space_configs[\"{space_name}\"].acl_groups: "
                         f"{len(acl)} group(s) configured")


def _find_tfvars_file(tfvars_path: Path, name: str) -> Path | None:
    """Locate a sibling tfvars file relative to the given tfvars file."""
    candidates = [
        tfvars_path.parent / name,
        tfvars_path.parent.parent / name,
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def _find_account_abac_file(tfvars_path: Path) -> Path | None:
    """Locate envs/account/abac.auto.tfvars relative to the given tfvars file."""
    # Walk up until we find an 'envs' directory, then look for envs/account/
    p = tfvars_path.parent
    for _ in range(5):
        candidate = p / "account" / "abac.auto.tfvars"
        if candidate.exists() and candidate != tfvars_path:
            return candidate
        if (p / "account").is_dir():
            return None
        p = p.parent
    return None


def load_validation_context(cfg: dict, result: ValidationResult, tfvars_path: Path) -> dict:
    """Merge supplemental validation context for split-state workspace configs."""
    merged = dict(cfg)
    parent_name = tfvars_path.parent.name
    is_generated = parent_name == "generated"
    if is_generated or parent_name == "data_access":
        env_name = tfvars_path.parent.parent.name
    else:
        env_name = parent_name

    env_cfg = {}
    env_path = _find_tfvars_file(tfvars_path, "env.auto.tfvars")
    if env_path:
        try:
            env_cfg = parse_tfvars(env_path)
        except Exception as e:
            result.warn(f"Could not parse {env_path}: {e}")

    if (
        env_path is not None  # Only enforce in real workspace envs that have an env.auto.tfvars
        and not is_generated
        and parent_name != "data_access"
        and env_name != "account"
        and env_cfg.get("manage_groups", False) is False
    ):
        if cfg.get("tag_policies"):
            result.error(
                "workspace split-state config should not define 'tag_policies' — "
                "tag policies are account-scoped and belong in envs/account/abac.auto.tfvars"
            )
        if cfg.get("tag_assignments"):
            result.error(
                "workspace split-state config should not define 'tag_assignments' — "
                "shared governance belongs in envs/<env>/data_access/abac.auto.tfvars"
            )
        if cfg.get("fgac_policies"):
            result.error(
                "workspace split-state config should not define 'fgac_policies' — "
                "shared governance belongs in envs/<env>/data_access/abac.auto.tfvars"
            )
        if cfg.get("group_members"):
            result.error(
                "workspace lookup-only config should not define 'group_members' — "
                "membership belongs in the shared account config"
            )

    # genie_only constraint: groups must be empty when genie_only = true.
    genie_only = env_cfg.get("genie_only", False)
    if genie_only and merged.get("groups"):
        result.error(
            "genie_only = true but 'groups' is non-empty. "
            "In genie-only mode, groups are managed by the governance team — "
            "set groups = {} or remove the groups block."
        )

    # Supplement tag_policies from envs/account/ when not present in current file.
    # Tag policies are account-scoped and managed in the account layer, so data_access
    # and workspace configs won't have them directly.
    if not merged.get("tag_policies"):
        account_abac = _find_account_abac_file(tfvars_path)
        if account_abac:
            try:
                account_cfg = parse_tfvars(account_abac)
                if account_cfg.get("tag_policies"):
                    merged["tag_policies"] = account_cfg["tag_policies"]
                    result.ok(f"tag_policies loaded from {account_abac}")
            except Exception as e:
                result.warn(f"Could not parse {account_abac}: {e}")

    return merged


def validate_auth(cfg: dict, result: ValidationResult, tfvars_path: Path):
    required = [
        "databricks_account_id",
        "databricks_client_id",
        "databricks_client_secret",
        "databricks_workspace_id",
        "databricks_workspace_host",
    ]

    auth_cfg = dict(cfg)
    for fname in ["auth.auto.tfvars", "env.auto.tfvars"]:
        found = _find_tfvars_file(tfvars_path, fname)
        if found:
            try:
                file_cfg = parse_tfvars(found)
                for k, v in file_cfg.items():
                    if v and not auth_cfg.get(k):
                        auth_cfg[k] = v
                result.ok(f"Vars loaded from {found.name}")
            except Exception as e:
                result.warn(f"Could not parse {found}: {e}")

    for key in required:
        val = auth_cfg.get(key, "")
        if not val:
            result.warn(f"'{key}' is empty — fill in before terraform apply")
        else:
            result.ok(f"{key}: set")


def main():
    parser = argparse.ArgumentParser(
        description="Validate AI-generated ABAC configuration files",
        epilog="Example: python validate_abac.py abac.auto.tfvars masking_functions.sql",
    )
    parser.add_argument("tfvars", help="Path to abac.auto.tfvars file")
    parser.add_argument("sql", nargs="?", help="Path to masking_functions.sql (optional)")
    parser.add_argument(
        "--coverage-gate",
        action="store_true",
        help="Block if any classification-derived treatment lacks a mask policy/function",
    )
    parser.add_argument(
        "--ddl",
        metavar="PATH",
        help="Fetched DDL used by --coverage-gate to warn about untagged "
             "sensitive-looking columns (default: auto-detect ddl/_fetched.sql)",
    )
    parser.add_argument(
        "--exposure-context",
        metavar="PATH",
        help="JSON from scripts/coverage_gate.py naming the tables about to be "
             "granted SELECT for the first time and the acknowledged columns; "
             "untagged sensitive-looking columns on those tables block",
    )
    parser.add_argument(
        "--country",
        metavar="CODE",
        help="Comma-separated region codes for country-specific column inference "
             "(e.g. ANZ, IN, SEA). Extends column category detection with "
             "region-specific identifier patterns. See shared/countries/.",
    )
    parser.add_argument(
        "--industry",
        metavar="CODE",
        help="Comma-separated industry codes for industry-specific column inference "
             "(e.g. financial_services, healthcare, retail). Extends column category "
             "detection with industry-specific identifier patterns. "
             "See shared/industries/.",
    )
    parser.add_argument("--env-name", default="",
                        help="With --coverage-gate: name the env so a gap prints its fix commands.")
    parser.add_argument("--summary-label", default="",
                        help="On a clean pass, print one labelled summary line instead of the full report.")
    parser.add_argument("--verbose", action="store_true",
                        help="Always print the full validation report.")
    args = parser.parse_args()

    # ── Overlay: extend column inference with country/industry hints ────────
    global _country_hint_to_category

    if args.country:
        country_codes = [c.strip().upper() for c in args.country.split(",") if c.strip()]
        if country_codes:
            hints, func_cats = _load_country_categories(country_codes)
            _country_hint_to_category.update(hints)
            # Merge overlay categories with base categories (don't replace).
            # The overlay adds specificity (e.g. mask_email → customer_pii) but
            # the base category (e.g. email) is still valid for column inference.
            for fn, cats in func_cats.items():
                FUNCTION_EXPECTED_CATEGORIES[fn] = FUNCTION_EXPECTED_CATEGORIES.get(fn, set()) | cats
            if hints:
                print(f"  Country overlays loaded: {', '.join(country_codes)} "
                      f"({len(hints)} column hints, {len(func_cats)} function mappings)")

    if args.industry:
        industry_codes = [i.strip().lower() for i in args.industry.split(",") if i.strip()]
        if industry_codes:
            hints, func_cats = _load_industry_categories(industry_codes)
            _country_hint_to_category.update(hints)
            # Merge overlay categories with base categories (don't replace).
            for fn, cats in func_cats.items():
                FUNCTION_EXPECTED_CATEGORIES[fn] = FUNCTION_EXPECTED_CATEGORIES.get(fn, set()) | cats
            if hints:
                print(f"  Industry overlays loaded: {', '.join(industry_codes)} "
                      f"({len(hints)} column hints, {len(func_cats)} function mappings)")

    tfvars_path = Path(args.tfvars).resolve()
    sql_path = Path(args.sql).resolve() if args.sql else None

    if not tfvars_path.exists():
        print(f"ERROR: {tfvars_path} not found")
        sys.exit(1)

    result = ValidationResult()

    # --- Parse tfvars ---
    try:
        cfg = parse_tfvars(tfvars_path)
    except Exception as e:
        result.error(f"Failed to parse {tfvars_path}: {e}")
        result.print_report(args.summary_label, args.verbose)
        sys.exit(1)

    # --- Parse SQL (optional) ---
    sql_functions: set[str] | None = None
    sql_function_arg_counts: dict[str, int] | None = None
    if sql_path:
        if not sql_path.exists():
            result.error(f"SQL file {sql_path} not found")
        else:
            sql_functions = parse_sql_functions(sql_path)
            sql_function_arg_counts = parse_sql_function_arg_counts(sql_path)
            if not sql_functions:
                result.warn(
                    f"No CREATE FUNCTION statements found in {sql_path} — "
                    f"is it the right file?"
                )
            else:
                result.ok(f"SQL file: {len(sql_functions)} function(s) found — {sorted(sql_functions)}")

    merged_cfg = load_validation_context(cfg, result, tfvars_path)

    # --- Run validations ---
    validate_auth(cfg, result, tfvars_path)
    group_names = validate_groups(merged_cfg, result)
    tag_map = validate_tag_policies(merged_cfg, result)
    validate_tag_assignments(merged_cfg, tag_map, result)
    validate_fgac_policies(merged_cfg, group_names, tag_map, sql_functions, result,
                           sql_function_arg_counts=sql_function_arg_counts)
    validate_group_members(merged_cfg, group_names, result)
    validate_acl_groups(merged_cfg, group_names, result)
    if args.coverage_gate:
        if sql_path is None:
            result.error("COVERAGE CHECK — masking_functions.sql is required")
        ddl_path = Path(args.ddl).resolve() if args.ddl else find_fetched_ddl(tfvars_path)
        ddl_columns = None
        if ddl_path and ddl_path.exists():
            ddl_columns = parse_ddl_columns(ddl_path.read_text())
        elif args.ddl:
            result.warn(f"COVERAGE CHECK — DDL file {ddl_path} not found; untagged-column check skipped")
        exposure = None
        if args.exposure_context:
            try:
                exposure = load_exposure_context(Path(args.exposure_context))
            except (OSError, ValueError) as exc:
                result.error(f"COVERAGE CHECK — unreadable exposure context {args.exposure_context}: {exc}")
        validate_coverage_gate(
            merged_cfg, sql_functions, tfvars_path.read_text(), result,
            ddl_columns=ddl_columns, exposure=exposure,
            env_name=args.env_name, env_dir=tfvars_path.parent.parent,
        )

    result.print_report(args.summary_label, args.verbose)
    sys.exit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
