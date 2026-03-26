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
import argparse
from pathlib import Path

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

    def print_report(self):
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
    """
    text = path.read_text()
    pattern = re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+"
        r"(?:[\w]+\.[\w]+\.)?"   # optional catalog.schema. prefix
        r"([\w]+)\s*\(([^)]*)\)",
        re.IGNORECASE,
    )
    result = {}
    for m in pattern.finditer(text):
        name = m.group(1)
        args = m.group(2).strip()
        result[name] = 0 if not args else len([a for a in args.split(",") if a.strip()])
    return result


def _extract_tag_refs(condition: str) -> tuple[list[tuple[str, str]], list[str]]:
    """Return (hasTagValue refs, hasTag refs) from a condition string."""
    value_refs = re.findall(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", condition or "")
    key_refs = re.findall(r"hasTag\(\s*'([^']+)'\s*\)", condition or "")
    return value_refs, key_refs


def _condition_matches_tags(condition: str, tags: dict[str, set[str]]) -> bool:
    """Evaluate a limited ABAC condition against a tag context.

    Supported syntax is intentionally narrow and matches the prompt / validator:
    hasTagValue('k','v'), hasTag('k'), AND, OR, parentheses.
    """
    if not condition:
        return True

    expr = condition

    def repl_value(match: re.Match) -> str:
        key, value = match.group(1), match.group(2)
        return str(value in tags.get(key, set()))

    def repl_key(match: re.Match) -> str:
        key = match.group(1)
        return str(key in tags and bool(tags[key]))

    expr = re.sub(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", repl_value, expr)
    expr = re.sub(r"hasTag\(\s*'([^']+)'\s*\)", repl_key, expr)
    expr = re.sub(r"\bAND\b", " and ", expr)
    expr = re.sub(r"\bOR\b", " or ", expr)

    # Refuse anything outside the expected boolean grammar.
    if re.search(r"[^()\sA-Za-z]", expr):
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


def _value_requires_coverage(tag_value: str) -> bool:
    return tag_value.strip().lower() not in {"public", "general", "exact"}


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


# Country-specific hint→category mapping, populated when --country is used.
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
    if "card" in col or "cvv" in col or "pan" in col:
        categories.add("card")
    if "amount" in col or "balance" in col or "limit" in col:
        categories.add("amount")
    # Country-specific patterns (populated by --country flag)
    for hint, category in _country_hint_to_category.items():
        if hint in col:
            categories.add(category)
    return categories or {"generic"}


GENERIC_SAFE_FUNCTIONS = {"mask_pii_partial", "mask_redact", "mask_nullify", "mask_hash"}
FUNCTION_EXPECTED_CATEGORIES = {
    "mask_email": {"email"},
    "mask_phone": {"phone"},
    "mask_ssn": {"ssn"},
    "mask_full_name": {"name"},
    "mask_credit_card_full": {"card"},
    "mask_credit_card_last4": {"card"},
    "mask_amount_rounded": {"amount"},
    "mask_date_to_year": {"date"},
    "mask_timestamp_to_day": {"date"},
}


def validate_groups(cfg: dict, result: ValidationResult):
    groups = cfg.get("groups")
    if not groups:
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
    for i, tp in enumerate(policies):
        key = tp.get("key", "")
        if not key:
            result.error(f"tag_policies[{i}]: 'key' is missing")
            continue
        if key in seen_keys:
            result.error(f"tag_policies[{i}]: duplicate key '{key}'")
        seen_keys.add(key)
        values = tp.get("values", [])
        if not values:
            result.error(f"tag_policies[{i}] (key='{key}'): 'values' is empty")
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
            if ref_key not in tag_map:
                result.error(f"{prefix}: condition references undefined tag_key '{ref_key}'")
            elif ref_val not in tag_map.get(ref_key, set()):
                result.error(
                    f"{prefix}: condition references tag_value '{ref_val}' "
                    f"not in tag_policy '{ref_key}' — allowed: {sorted(tag_map[ref_key])}"
                )
        for tag_ref in re.findall(r"hasTag\(\s*'([^']+)'\s*\)", condition):
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
            # Row filters use `using = []` so the function must take 0 args.
            # Column masks get the column passed implicitly via on_column,
            # so the function must take exactly 1 arg.
            if sql_function_arg_counts and fn in sql_function_arg_counts:
                argc = sql_function_arg_counts[fn]
                if ptype == "POLICY_TYPE_ROW_FILTER" and argc != 0:
                    result.warn(
                        f"{prefix}: ROW_FILTER function '{fn}' takes {argc} argument(s) "
                        f"but row filters require 0-argument functions. Use a dedicated "
                        f"filter function (e.g. filter_<name>()) that returns BOOLEAN."
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
        "--country",
        metavar="CODE",
        help="Comma-separated region codes for country-specific column inference "
             "(e.g. ANZ, IN, SEA). Extends column category detection with "
             "region-specific identifier patterns. See shared/countries/.",
    )
    args = parser.parse_args()

    # ── Country/region overlay: extend column inference ──────────────────────
    if args.country:
        global _country_hint_to_category
        country_codes = [c.strip().upper() for c in args.country.split(",") if c.strip()]
        if country_codes:
            hints, func_cats = _load_country_categories(country_codes)
            _country_hint_to_category.update(hints)
            FUNCTION_EXPECTED_CATEGORIES.update(func_cats)
            if hints:
                print(f"  Country overlays loaded: {', '.join(country_codes)} "
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
        result.print_report()
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

    result.print_report()
    sys.exit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
