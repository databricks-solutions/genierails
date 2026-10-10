#!/usr/bin/env python3
"""
Generate ABAC masking_functions.sql and abac.auto.tfvars from table DDL files.

Reads DDL files from a folder (or fetches them live from Databricks),
combines them with the ABAC prompt template, sends to an LLM, and writes
the generated output files.  Optionally runs validate_abac.py on the result.

Authentication:
  The script reads auth.auto.tfvars for Databricks credentials and
  env.auto.tfvars for uc_catalog + uc_tables and environment config.  Catalog/schema
  for UDF deployment are auto-derived from uc_catalog (or the first table in uc_tables)
  (override with --catalog / --schema).

Supported LLM providers:
  - databricks (default) — Claude Sonnet via Databricks Foundation Model API
  - anthropic            — Claude via the Anthropic API
  - openai               — GPT-4o / o1 via OpenAI API

Usage:
  # One-time setup
  cp auth.auto.tfvars.example auth.auto.tfvars   # credentials (gitignored)
  cp env.auto.tfvars.example env.auto.tfvars     # tables + environment (checked in)
  # Edit env.auto.tfvars:
  #   uc_catalog = "prod_catalog"
  #   uc_tables  = ["sales.customers", "sales.orders", "finance.*"]

  # Generate (reads uc_catalog + uc_tables from env config; catalog/schema auto-derived)
  python generate_abac.py

  # Or override tables via CLI
  python generate_abac.py --tables prod.sales.customers prod.sales.orders

  # Use a specific provider / model
  python generate_abac.py --provider anthropic --model claude-sonnet-4-20250514

  # Fall back to local DDL files (legacy — requires --catalog / --schema)
  cp my_tables.sql ddl/
  python generate_abac.py --catalog my_catalog --schema my_schema
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

from tag_vocabulary import REGISTRY
from access_tier_groups import (
    SETTING as ACCESS_TIER_GROUPS_SETTING,
    display_path as _env_display_path,
    parse_groups_arg,
    persist_access_tier_groups,
    persisted_access_tier_groups,
    render as render_access_tier_groups,
)
from genie_space_placeholder import placeholder_error
from walkthrough_marker import follows_walkthrough
from sensitivity_source import (
    ClassificationSource,
    Finding,
    LLMSource,
    LLM as _SRC_LLM,
    SensitivitySource,
    select_findings,
)
from treatment_derivation import (
    ACL_NEUTRAL_FALLBACK_COMMENT,
    DERIVED_TREATMENT_MASK_COMMENT_PREFIX,
    derive_treatment_model,
    load_treatment_config,
)

PRODUCT_NAME = "genierails"
PRODUCT_VERSION = "0.1.0"

SCRIPT_DIR = Path(__file__).resolve().parent
WORK_DIR = Path.cwd()
PROMPT_TEMPLATE_PATH = SCRIPT_DIR / "ABAC_PROMPT.md"
DEFAULT_AUTH_FILE = WORK_DIR / "auth.auto.tfvars"
DEFAULT_ENV_FILE = WORK_DIR / "env.auto.tfvars"

# Pip spec -> import name. python-hcl2 8.x changed its parse output and breaks
# the access_tier_groups rewrite, so fresh installs stay on 7.x.
REQUIRED_PACKAGES = {
    "python-hcl2<8": "hcl2",
    "databricks-sdk": "databricks.sdk",
    "pyyaml": "yaml",
}


def _ensure_packages():
    """Auto-install required packages if missing."""
    missing = []
    for pip_name, import_name in REQUIRED_PACKAGES.items():
        try:
            __import__(import_name)
        except ImportError:
            missing.append(pip_name)
    if missing:
        print(f"  Installing missing packages: {', '.join(missing)}...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", *missing],
        )
    try:
        __import__("databricks.sdk.useragent")
    except (ImportError, ModuleNotFoundError):
        print("  Upgrading databricks-sdk (need databricks.sdk.useragent)...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", "--upgrade", "databricks-sdk"],
        )


_ensure_packages()

COUNTRIES_DIR = SCRIPT_DIR / "countries"
INDUSTRIES_DIR = SCRIPT_DIR / "industries"


def _canonical_tag_key(tag_key: str) -> str:
    return REGISTRY.canonical_key(tag_key)


def _tag_key_family(tag_key: str) -> str:
    family = REGISTRY.family_for_key(tag_key)
    if family:
        return family
    canonical = _canonical_tag_key(tag_key)
    return canonical.split("_", 1)[0] if "_" in canonical else canonical


def _canonical_tag_value(tag_key: str, tag_value: str) -> str:
    return REGISTRY.canonical_value(tag_key, tag_value)


def _normalize_has_tag_refs(condition: str) -> tuple[str, int]:
    return REGISTRY.normalize_condition_refs(condition)


def load_country_overlays(country_codes: list[str]) -> str:
    """Load country/region YAML overlays and return combined prompt text.

    Each code maps to a YAML file in shared/countries/ (e.g. "ANZ" -> ANZ.yaml).
    Returns the concatenated prompt_overlay text plus formatted masking function
    signatures, ready for injection into the LLM prompt.
    """
    import yaml

    parts: list[str] = []
    total_identifiers = 0

    for code in country_codes:
        code_upper = code.strip().upper()
        yaml_path = COUNTRIES_DIR / f"{code_upper}.yaml"
        if not yaml_path.exists():
            available = sorted(
                p.stem for p in COUNTRIES_DIR.glob("*.yaml") if not p.stem.startswith("_")
            )
            print(f"  ERROR: Country overlay file not found: {yaml_path}")
            print(f"  Available: {', '.join(available) or '(none)'}")
            sys.exit(1)

        with open(yaml_path) as f:
            data = yaml.safe_load(f)

        overlay_name = data.get("name", code_upper)
        identifiers = data.get("identifiers", [])
        prompt_overlay = data.get("prompt_overlay", "")
        total_identifiers += len(identifiers)

        if prompt_overlay:
            parts.append(prompt_overlay.rstrip())

        print(f"  Country overlay: {code_upper} ({overlay_name}) — "
              f"{len(identifiers)} identifier(s), "
              f"{len(data.get('masking_functions', []))} masking function(s)")

    if not parts:
        return ""

    return "\n\n".join(parts) + "\n"


def load_industry_overlays(industry_codes: list[str]) -> str:
    """Load industry YAML overlays and return combined prompt text.

    Each code maps to a YAML file in shared/industries/ (e.g. "healthcare" -> healthcare.yaml).
    Returns the concatenated prompt_overlay text plus group templates and access patterns,
    ready for injection into the LLM prompt.
    """
    import yaml

    parts: list[str] = []

    for code in industry_codes:
        code_lower = code.strip().lower()
        yaml_path = INDUSTRIES_DIR / f"{code_lower}.yaml"
        if not yaml_path.exists():
            available = sorted(
                p.stem for p in INDUSTRIES_DIR.glob("*.yaml") if not p.stem.startswith("_")
            )
            print(f"  ERROR: Industry overlay file not found: {yaml_path}")
            print(f"  Available: {', '.join(available) or '(none)'}")
            sys.exit(1)

        with open(yaml_path) as f:
            data = yaml.safe_load(f)

        overlay_name = data.get("name", code_lower)
        identifiers = data.get("identifiers", [])
        prompt_overlay = data.get("prompt_overlay", "")
        group_templates = data.get("group_templates", {})
        access_patterns = data.get("access_patterns", [])

        if prompt_overlay:
            parts.append(prompt_overlay.rstrip())

        if group_templates:
            lines = [f"\n**Suggested Group Templates ({overlay_name}):**"]
            for gname, gdef in group_templates.items():
                lines.append(f"- `{gname}`: {gdef.get('description', '')} (access: {gdef.get('access_level', '')})")
            parts.append("\n".join(lines))

        if access_patterns:
            lines = [f"\n**Access Patterns ({overlay_name}):**"]
            for ap in access_patterns:
                lines.append(f"- **{ap['name']}**: {ap.get('description', '')}. {ap.get('guidance', '')}")
            parts.append("\n".join(lines))

        print(f"  Industry overlay: {code_lower} ({overlay_name}) — "
              f"{len(identifiers)} identifier(s), "
              f"{len(data.get('masking_functions', []))} masking function(s), "
              f"{len(group_templates)} group template(s)")

    if not parts:
        return ""

    return "\n\n".join(parts) + "\n"


def build_industry_detection_guidance(ddl_text: str, industry_codes: list[str]) -> tuple[str, str]:
    """Detect overlay-specific columns in DDL and return prompt + comment guidance.

    This keeps multi-industry generations from collapsing to the dominant domain
    when the DDL clearly contains identifiers from several overlays.
    """
    try:
        import yaml
    except ImportError:
        return "", ""

    ddl_lower = ddl_text.lower()
    matched_lines: list[str] = []
    seen_functions: set[str] = set()
    seen_comments: set[str] = set()

    for code in industry_codes:
        yaml_path = INDUSTRIES_DIR / f"{code.strip().lower()}.yaml"
        if not yaml_path.exists():
            continue
        with open(yaml_path) as f:
            data = yaml.safe_load(f) or {}
        overlay_name = data.get("name", code)
        for ident in data.get("identifiers", []):
            hints = [h.lower() for h in ident.get("column_hints", [])]
            matched = [hint for hint in hints if hint and hint in ddl_lower]
            if not matched:
                continue
            fn = ident.get("masking_function") or ""
            comment_key = (
                overlay_name,
                ident.get("name", ""),
                fn,
                tuple(sorted(set(matched))),
            )
            if comment_key in seen_comments:
                continue
            seen_comments.add(comment_key)
            fn_text = f" use `{fn}`" if fn and fn != "null" else ""
            matched_lines.append(
                f"- `{overlay_name}` detected `{ident.get('name', '')}` via {sorted(set(matched))};{fn_text}"
            )
            if fn and fn != "null":
                seen_functions.add(fn)

    if not matched_lines:
        return "", ""

    prompt = (
        "\n### OVERLAY MATCHES DETECTED IN THE DDL\n\n"
        "The following overlay-specific identifiers are present in the schema. "
        "Your generated ABAC output must preserve coverage for each matched domain"
        " and include the corresponding masking concepts and functions when applicable.\n"
        + "\n".join(matched_lines)
        + "\n"
    )
    comments = (
        "# Overlay-specific identifiers detected in the DDL.\n"
        "# Preserve coverage for these domains in the generated config:\n"
        + "\n".join(f"# {line[2:]}" if line.startswith("- ") else f"# {line}" for line in matched_lines)
        + "\n"
    )
    return prompt, comments


def _load_tfvars(path: Path, label: str, *, strict: bool = False) -> dict:
    """Load a single .tfvars file. Returns empty dict if not found."""
    if not path.exists():
        return {}
    import hcl2
    try:
        with open(path) as f:
            cfg = hcl2.load(f)
        non_empty = {k: v for k, v in cfg.items() if v}
        if non_empty:
            print(f"  Loaded {label} from: {path}")
        return cfg
    except Exception as e:
        message = f"Failed to parse {label} file {path}: {e}"
        if strict:
            raise ValueError(message) from e
        print(f"  WARNING: {message}")
        return {}


def load_auth_config(
    auth_file: Path, env_file: Path | None = None, *, strict_env: bool = False
) -> dict:
    """Load config from auth + env tfvars files. Merges both; env overrides auth.

    Supports the new split format (uc_catalog + schema-relative uc_tables) as well as
    the legacy full-ref format (uc_tables = ["catalog.schema.table"]).  When uc_catalog
    is set, relative uc_tables entries are expanded into full 3-part refs before being
    returned so the rest of the script does not need to know about the split.
    """
    cfg = _load_tfvars(auth_file, "credentials")
    if env_file is None:
        env_file = auth_file.parent / "env.auto.tfvars"
    env_cfg = _load_tfvars(env_file, "environment", strict=strict_env)
    placeholder = placeholder_error(env_cfg, env_file)
    if placeholder:
        raise ValueError(placeholder)
    cfg.update(env_cfg)

    # Combine uc_catalog + relative uc_tables into full 3-part refs when the new
    # split format is used.  The --tables CLI flag always passes full refs directly
    # and bypasses this function, so only config-file values need expansion here.
    uc_catalog = cfg.get("uc_catalog", "")
    uc_tables = cfg.get("uc_tables", [])
    if uc_catalog and uc_tables:
        cfg["uc_tables"] = [f"{uc_catalog}.{t}" for t in uc_tables]

    if "uc_tables" in cfg and cfg["uc_tables"]:
        print(f"    uc_tables: {', '.join(cfg['uc_tables'])}")
    return cfg


def persist_discovered_uc_tables(
    path: Path,
    tables: list[str],
    *,
    table_agents: dict[str, list[str]] | None = None,
    merge_existing: bool = False,
) -> tuple[list[str], list[str], list[str]]:
    """Atomically persist the tool-owned Genie table footprint."""
    existing_cfg = _load_tfvars(
        path, "previously discovered UC tables", strict=True
    )
    existing = _validated_discovered_uc_tables(existing_cfg, path)
    existing_agents = _validated_discovered_table_agents(existing_cfg, path)
    current_agents = _validated_discovered_table_agents(
        {"discovered_table_agents": table_agents or {}}, path
    )
    current = list(dict.fromkeys(str(table) for table in tables if table))
    current = list(dict.fromkeys(current + list(current_agents)))
    desired = list(dict.fromkeys(existing + current)) if merge_existing else current
    if merge_existing:
        desired_agents = {
            table: list(dict.fromkeys(
                existing_agents.get(table, []) + current_agents.get(table, [])
            ))
            for table in desired
            if table in existing_agents or table in current_agents
        }
    else:
        desired_agents = {
            table: current_agents[table]
            for table in desired
            if table in current_agents
        }

    existing_set = set(existing)
    desired_set = set(desired)
    added = [table for table in desired if table not in existing_set]
    already_present = [table for table in desired if table in existing_set]
    disappeared = [table for table in existing if table not in desired_set]

    print("\n  Discovered UC table persistence:")
    for label, values in (
        ("added", added),
        ("already present", already_present),
        ("disappeared", disappeared),
    ):
        print(f"    {label}: {', '.join(values) if values else '(none)'}")

    rendered = "# Tool-owned: refreshed by make generate; do not edit manually.\n"
    rendered += "discovered_uc_tables = [\n"
    rendered += "".join(f"  {json.dumps(table)},\n" for table in desired)
    rendered += "]\n"
    rendered += "\n# Table FQN -> Genie agents that expose it.\n"
    rendered += "discovered_table_agents = {\n"
    for table, agents in desired_agents.items():
        rendered += f"  {json.dumps(table)} = {json.dumps(agents)}\n"
    rendered += "}\n"
    if path.exists() and path.read_text() == rendered:
        path.chmod(0o644)
        print(f"    unchanged: {path}")
        return added, already_present, disappeared

    path.parent.mkdir(parents=True, exist_ok=True)
    import tempfile
    with tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as tmp:
        tmp.write(rendered)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)
    path.chmod(0o644)
    print(f"    wrote: {path}")
    return added, already_present, disappeared


def preserve_discovered_tables_for_incomplete_run(
    path: Path, tables: list[str], footprint_entries: list
) -> list[str]:
    """Keep persisted tables governed and put them back in the masking footprint."""
    cfg = _load_tfvars(path, "previously discovered UC tables", strict=True)
    preserved = _validated_discovered_uc_tables(cfg, path)
    agents = _validated_discovered_table_agents(cfg, path)
    if preserved and not agents:
        print(
            "  WARNING: legacy discovered_uc_tables has no agent attribution; "
            "SELECT grants fall back to all access principals until generate re-derives it."
        )
    tables[:] = list(dict.fromkeys(tables + preserved))
    footprint_entries.extend(
        table for table in preserved if table not in footprint_entries
    )
    return preserved


def _validated_discovered_uc_tables(cfg: dict, path: Path) -> list[str]:
    """Return deduplicated persisted tables or reject malformed grant input."""
    discovered_value = cfg.get("discovered_uc_tables", [])
    if not isinstance(discovered_value, list):
        raise ValueError(
            f"Invalid discovered_uc_tables in {path}: expected a list"
        )
    if any(not isinstance(table, str) for table in discovered_value):
        raise ValueError(
            f"Invalid discovered_uc_tables in {path}: every entry must be a string"
        )
    return list(dict.fromkeys(discovered_value))


def _validated_discovered_table_agents(cfg: dict, path: Path) -> dict[str, list[str]]:
    """Return validated table-to-agent facts from the environment-local file."""
    value = cfg.get("discovered_table_agents", {})
    if not isinstance(value, dict):
        raise ValueError(
            f"Invalid discovered_table_agents in {path}: expected a map"
        )
    normalized: dict[str, list[str]] = {}
    for table, agents in value.items():
        if not isinstance(table, str) or not isinstance(agents, list) or any(
            not isinstance(agent, str) for agent in agents
        ):
            raise ValueError(
                f"Invalid discovered_table_agents in {path}: expected string table keys "
                "and lists of agent-name strings"
            )
        normalized[table] = list(dict.fromkeys(agents))
    return normalized


def configure_databricks_env(auth_cfg: dict):
    """Set Databricks SDK env vars from auth config."""
    mapping = {
        "databricks_workspace_host": "DATABRICKS_HOST",
        "databricks_client_id": "DATABRICKS_CLIENT_ID",
        "databricks_client_secret": "DATABRICKS_CLIENT_SECRET",
    }
    for tfvar_key, env_key in mapping.items():
        val = auth_cfg.get(tfvar_key, "")
        if val:
            os.environ[env_key] = val


def load_ddl_files(ddl_dir: Path) -> str:
    """Read all .sql files from ddl_dir and concatenate them."""
    sql_files = sorted(ddl_dir.glob("*.sql"))
    if not sql_files:
        print(f"ERROR: No .sql files found in {ddl_dir}")
        print("  Place your CREATE TABLE / DESCRIBE TABLE DDL in .sql files there.")
        sys.exit(1)

    parts = []
    for f in sql_files:
        content = f.read_text().strip()
        if content:
            parts.append(f"-- Source: {f.name}\n{content}")
            print(f"  Loaded DDL: {f.name} ({len(content)} chars)")

    combined = "\n\n".join(parts)
    print(f"  Total DDL: {len(combined)} chars from {len(sql_files)} file(s)\n")
    return combined


def _parse_table_ref(ref: str) -> tuple[str, str, str]:
    """Parse 'catalog.schema.table' or 'catalog.schema.*' into parts."""
    parts = ref.split(".")
    if len(parts) != 3:
        print(f"ERROR: Invalid table reference '{ref}'")
        print("  Expected format: catalog.schema.table or catalog.schema.*")
        sys.exit(1)
    return parts[0], parts[1], parts[2]


def format_table_info(table_info) -> str:
    """Format a TableInfo object into CREATE TABLE DDL text."""
    full_name = table_info.full_name
    lines = [f"-- Table: {full_name}"]
    lines.append(f"CREATE TABLE {full_name} (")
    if table_info.columns:
        col_parts = []
        for col in table_info.columns:
            type_text = col.type_text or "STRING"
            part = f"  {col.name} {type_text}"
            if col.comment:
                safe = col.comment.replace("'", "''")
                part += f" COMMENT '{safe}'"
            col_parts.append(part)
        lines.append(",\n".join(col_parts))
    lines.append(");")
    if table_info.comment:
        lines.append(f"-- Table comment: {table_info.comment}")
    return "\n".join(lines)


def _parse_str_field(val) -> str:
    """Safely extract the first string from a list-or-string field in serialized_space."""
    if isinstance(val, list):
        return val[0] if val else ""
    return val or ""


def _normalize_footprint_table(value: str) -> str:
    """Return a stable three-part table identifier, or an empty string."""
    value = (value or "").strip().strip("`\"")
    value = ".".join(part.strip("`\"") for part in value.split("."))
    return value if len(value.split(".")) == 3 else ""


def discover_agent_footprint(
    serialized_space: str | dict | None = None,
    declared_footprint: list | None = None,
    corroborated_footprint: list | None = None,
) -> list[dict]:
    """Enumerate the deterministic table/column footprint reachable by a Genie agent.

    Sources are additive: explicitly declared entries support greenfield spaces,
    while an existing space contributes its data sources and table/column
    references found in SQL definitions (benchmarks, snippets and joins).  A
    caller may pass workspace-corroborated entries (for example, successfully
    readable UC table metadata) without coupling this pure merge routine to the
    Databricks SDK.

    Entries may be strings (``catalog.schema.table`` or a fully-qualified
    column) or mappings with ``table``/``identifier`` and optional ``columns``.
    The result is sorted and de-duplicated.  An empty columns list means the
    whole table is reachable; otherwise columns are sorted and de-duplicated.
    """
    import json as _json

    tables: dict[str, set[str]] = {}
    table_display: dict[str, str] = {}
    whole_table: set[str] = set()

    def add(value, columns=None):
        if isinstance(value, dict):
            columns = value.get("columns", columns)
            value = value.get("table") or value.get("identifier") or value.get("name") or ""
        raw = str(value or "").strip().strip("`\"")
        parts = [p.strip("`\"") for p in raw.split(".")]
        table = _normalize_footprint_table(".".join(parts[:3])) if len(parts) >= 3 else ""
        if not table:
            return
        table_key = table.lower()
        tables.setdefault(table_key, set())
        table_display.setdefault(table_key, table)
        implied = parts[3] if len(parts) == 4 else ""
        col_values = columns if isinstance(columns, (list, tuple, set)) else ([columns] if columns else [])
        normalized_columns = {
            str(c).split(".")[-1].strip().strip("`\"") for c in col_values if str(c).strip()
        }
        if implied:
            normalized_columns.add(implied)
        if normalized_columns:
            tables[table_key].update(normalized_columns)
        else:
            # A whole-table declaration supersedes any named columns for it.
            whole_table.add(table_key)

    for entry in declared_footprint or []:
        add(entry)
    for entry in corroborated_footprint or []:
        add(entry)

    if isinstance(serialized_space, str):
        try:
            space = _json.loads(serialized_space)
        except Exception:
            space = {}
    else:
        space = serialized_space or {}

    for entry in space.get("data_sources", {}).get("tables", []) or []:
        add(entry)

    # Genie definitions contain executable SQL outside data_sources.  Capture
    # qualified table and column references so they cannot silently escape the
    # declared scan/grant boundary.
    sql_fragments: list[str] = []
    snippets = space.get("instructions", {}).get("sql_snippets", {}) or {}
    for kind in ("filters", "expressions", "measures"):
        for item in snippets.get(kind, []) or []:
            sql_fragments.append(_parse_str_field(item.get("sql", "")))
    for join in space.get("instructions", {}).get("join_specs", []) or []:
        add(join.get("left", {}).get("identifier", ""))
        add(join.get("right", {}).get("identifier", ""))
        sql_fragments.append(_parse_str_field(join.get("sql", "")))
    for benchmark in space.get("benchmarks", {}).get("questions", []) or []:
        for answer in benchmark.get("answer", []) or []:
            if str(answer.get("format", "")).upper() == "SQL":
                sql_fragments.append(_parse_str_field(answer.get("content", "")))

    qualified = re.compile(r"(?<![\w.])`?([A-Za-z_]\w*)`?\s*\.\s*`?([A-Za-z_]\w*)`?\s*\.\s*`?([A-Za-z_]\w*)`?(?:\s*\.\s*`?([A-Za-z_]\w*)`?)?")
    for sql in sql_fragments:
        for catalog, schema, table, column in qualified.findall(sql or ""):
            add(f"{catalog}.{schema}.{table}" + (f".{column}" if column else ""))

    return [
        {
            "table": table_display[table_key],
            "columns": [] if table_key in whole_table else sorted(tables[table_key], key=str.lower),
        }
        for table_key in sorted(tables)
    ]


def footprint_table_refs(footprint: list[dict]) -> list[str]:
    """Return the canonical table list consumed by fetch/classification paths."""
    return [entry["table"] for entry in footprint if entry.get("table")]


def footprint_contains_column(footprint: list[dict], column_fqn: str) -> bool:
    """Whether a classified column belongs to the coverage denominator."""
    parts = column_fqn.split(".")
    if len(parts) != 4:
        return False
    table, column = ".".join(parts[:3]).lower(), parts[3].lower()
    for entry in footprint:
        footprint_table = str(entry.get("table") or "").lower()
        if footprint_table.endswith(".*") and table.startswith(footprint_table[:-1]):
            return True
        if footprint_table == table:
            columns = [str(value).lower() for value in (entry.get("columns") or [])]
            if not columns or column in columns:
                return True
    return False


def scope_ddl_to_footprint(ddl_text: str, footprint: list[dict]) -> str:
    """Restrict fetched DDL columns to the canonical footprint when explicit."""
    explicit: dict[str, set[str]] = {}
    for item in footprint:
        if item.get("table") and item.get("columns"):
            table = str(item["table"]).lower()
            explicit.setdefault(table, set()).update(
                str(column).lower() for column in item["columns"]
            )
    whole_tables = {
        str(item["table"]).lower()
        for item in footprint if item.get("table") and not item.get("columns")
    }
    if not explicit:
        return ddl_text
    pattern = re.compile(
        r"(CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([\w.`]+)\s*\()(.*?)(\)\s*;)",
        re.IGNORECASE | re.DOTALL,
    )

    def replace(match: re.Match) -> str:
        table = ".".join(part.strip("`") for part in match.group(2).split(".")).lower()
        if table in whole_tables or any(
            name.endswith(".*") and table.startswith(name[:-1])
            for name in whole_tables | set(explicit)
        ):
            return match.group(0)
        wanted = explicit.get(table)
        if not wanted:
            return match.group(0)
        kept = []
        for line in match.group(3).splitlines():
            name = line.strip().lstrip(",").split(None, 1)[0].strip("`,") if line.strip() else ""
            if name.lower() in wanted:
                kept.append(line)
        if kept:
            kept[-1] = re.sub(r",\s*$", "", kept[-1])
        body = "\n" + "\n".join(kept) + "\n"
        return match.group(1) + body + match.group(4)

    return pattern.sub(replace, ddl_text)


def parse_genie_config_from_serialized_space(serialized: str, description: str = "") -> dict:
    """Parse a Genie agent's serialized_space JSON into a genie_space_configs dict.

    Returns a dict with keys: description, instructions, sample_questions,
    benchmarks, sql_filters, sql_expressions, sql_measures, join_specs.
    Only includes keys that have non-empty values.
    """
    import json as _json

    try:
        space_data = _json.loads(serialized)
    except Exception:
        return {}

    config: dict = {}

    if description:
        config["description"] = description

    # Instructions (free-text)
    text_instrs = space_data.get("instructions", {}).get("text_instructions", [])
    if text_instrs:
        content = _parse_str_field(text_instrs[0].get("content", ""))
        if content:
            config["instructions"] = content

    # Sample questions
    sq_items = space_data.get("config", {}).get("sample_questions", [])
    questions = [_parse_str_field(item.get("question", "")) for item in sq_items]
    questions = [q for q in questions if q]
    if questions:
        config["sample_questions"] = questions

    # Benchmarks
    bm_items = space_data.get("benchmarks", {}).get("questions", [])
    benchmarks = []
    for bm in bm_items:
        question = _parse_str_field(bm.get("question", ""))
        sql = ""
        for ans in bm.get("answer", []):
            if ans.get("format") == "SQL":
                sql = _parse_str_field(ans.get("content", ""))
                break
        if question and sql:
            benchmarks.append({"question": question, "sql": sql})
    if benchmarks:
        config["benchmarks"] = benchmarks

    snippets = space_data.get("instructions", {}).get("sql_snippets", {})

    # SQL filters
    filters = [
        {"sql": _parse_str_field(f.get("sql", "")), "display_name": f.get("display_name", "")}
        for f in snippets.get("filters", [])
        if _parse_str_field(f.get("sql", ""))
    ]
    if filters:
        config["sql_filters"] = filters

    # SQL expressions
    exprs = [
        {"alias": e.get("alias", ""), "sql": _parse_str_field(e.get("sql", ""))}
        for e in snippets.get("expressions", [])
        if _parse_str_field(e.get("sql", ""))
    ]
    if exprs:
        config["sql_expressions"] = exprs

    # SQL measures
    measures = [
        {"alias": m.get("alias", ""), "sql": _parse_str_field(m.get("sql", ""))}
        for m in snippets.get("measures", [])
        if _parse_str_field(m.get("sql", ""))
    ]
    if measures:
        config["sql_measures"] = measures

    # Join specs
    joins = [
        {
            "left_table": j.get("left", {}).get("identifier", ""),
            "right_table": j.get("right", {}).get("identifier", ""),
            "sql": _parse_str_field(j.get("sql", "")),
        }
        for j in space_data.get("instructions", {}).get("join_specs", [])
        if _parse_str_field(j.get("sql", ""))
    ]
    if joins:
        config["join_specs"] = joins

    return config


def _hcl_str(s: str) -> str:
    """Format a Python string as an HCL quoted string literal."""
    escaped = s.replace("\\", "\\\\").replace('"', '\\"').replace("${", "$${")
    return f'"{escaped}"'


def format_genie_space_configs_hcl(configs: dict[str, dict]) -> str:
    """Convert a dict of {space_key: config_dict} to the genie_space_configs HCL block.

    The space_key is the human-readable name used as the map key in abac.auto.tfvars.
    """
    lines = ["genie_space_configs = {"]

    for space_name, cfg in configs.items():
        lines.append(f"  {_hcl_str(space_name)} = {{")

        if cfg.get("title"):
            lines.append(f"    title = {_hcl_str(cfg['title'])}")

        if cfg.get("description"):
            lines.append(f"    description = {_hcl_str(cfg['description'])}")

        if cfg.get("instructions"):
            lines.append(f"    instructions = {_hcl_str(cfg['instructions'])}")

        if cfg.get("sample_questions"):
            lines.append("    sample_questions = [")
            for q in cfg["sample_questions"]:
                lines.append(f"      {_hcl_str(q)},")
            lines.append("    ]")

        if cfg.get("benchmarks"):
            lines.append("    benchmarks = [")
            for bm in cfg["benchmarks"]:
                if isinstance(bm, dict) and "question" in bm and "sql" in bm:
                    lines.append("      {")
                    lines.append(f"        question = {_hcl_str(bm['question'])}")
                    lines.append(f"        sql      = {_hcl_str(bm['sql'])}")
                    lines.append("      },")
                # Skip malformed benchmarks (e.g. plain strings from LLM)
            lines.append("    ]")

        if cfg.get("sql_filters"):
            lines.append("    sql_filters = [")
            for f in cfg["sql_filters"]:
                if isinstance(f, dict) and "sql" in f:
                    lines.append("      {")
                    lines.append(f"        sql          = {_hcl_str(f['sql'])}")
                    lines.append(f"        display_name = {_hcl_str(f.get('display_name', ''))}")
                    lines.append(f"        comment      = {_hcl_str(f.get('comment', ''))}")
                    lines.append(f"        instruction  = {_hcl_str(f.get('instruction', ''))}")
                    lines.append("      },")
                # Skip malformed filters (e.g. plain strings from LLM)
            lines.append("    ]")

        if cfg.get("sql_expressions"):
            lines.append("    sql_expressions = [")
            for e in cfg["sql_expressions"]:
                if isinstance(e, dict) and "alias" in e and "sql" in e:
                    lines.append("      {")
                    lines.append(f"        alias        = {_hcl_str(e['alias'])}")
                    lines.append(f"        sql          = {_hcl_str(e['sql'])}")
                    lines.append(f"        display_name = {_hcl_str(e.get('display_name', ''))}")
                    lines.append(f"        comment      = {_hcl_str(e.get('comment', ''))}")
                    lines.append(f"        instruction  = {_hcl_str(e.get('instruction', ''))}")
                    lines.append("      },")
                # Skip malformed expressions (e.g. plain strings from LLM)
            lines.append("    ]")

        if cfg.get("sql_measures"):
            lines.append("    sql_measures = [")
            for m in cfg["sql_measures"]:
                if isinstance(m, dict) and "alias" in m and "sql" in m:
                    lines.append("      {")
                    lines.append(f"        alias        = {_hcl_str(m['alias'])}")
                    lines.append(f"        sql          = {_hcl_str(m['sql'])}")
                    lines.append(f"        display_name = {_hcl_str(m.get('display_name', ''))}")
                    lines.append(f"        comment      = {_hcl_str(m.get('comment', ''))}")
                    lines.append(f"        instruction  = {_hcl_str(m.get('instruction', ''))}")
                    lines.append("      },")
                # Skip malformed measures (e.g. plain strings from LLM)
            lines.append("    ]")

        if cfg.get("join_specs"):
            lines.append("    join_specs = [")
            for j in cfg["join_specs"]:
                if isinstance(j, dict) and "left_table" in j and "right_table" in j and "sql" in j:
                    lines.append("      {")
                    lines.append(f"        left_table   = {_hcl_str(j['left_table'])}")
                    lines.append(f"        right_table  = {_hcl_str(j['right_table'])}")
                    lines.append(f"        sql          = {_hcl_str(j['sql'])}")
                    lines.append(f"        comment      = {_hcl_str(j.get('comment', ''))}")
                    lines.append(f"        instruction  = {_hcl_str(j.get('instruction', ''))}")
                    lines.append(f"        left_alias   = {_hcl_str(j.get('left_alias', ''))}")
                    lines.append(f"        right_alias  = {_hcl_str(j.get('right_alias', ''))}")
                    lines.append("      },")
                # Skip malformed join_specs (e.g. plain strings from LLM)
            lines.append("    ]")

        lines.append("  }")

    lines.append("}")
    return "\n".join(lines)


_REVIEWED_GENIE_FIELDS = (
    "benchmarks", "sql_filters", "sql_expressions", "sql_measures", "join_specs",
)


def keep_reviewed_genie_content(path: Path, fresh: dict[str, dict]) -> list[str]:
    """Keep reviewed benchmark/snippet values when refreshing an imported agent.

    The API is a seed, not authority over reviewed code. New spaces/fields are
    still imported, while an existing field remains untouched (including user
    additions) just like reviewed governance rules.
    """
    if not path.exists():
        return []
    try:
        existing = _load_tfvars(path, "reviewed Genie config", strict=True)
    except (ValueError, OSError):
        return []
    existing_spaces = existing.get("genie_space_configs") or {}
    kept = []
    for name, config in fresh.items():
        reviewed = existing_spaces.get(name) if isinstance(existing_spaces, dict) else None
        if not isinstance(reviewed, dict):
            continue
        for field in _REVIEWED_GENIE_FIELDS:
            if field in reviewed:
                config[field] = reviewed[field]
                kept.append(f"{name}.{field}")
    return kept


def _hcl_value_end(text: str, start: int) -> int:
    """End of one quoted or balanced HCL value (comments/strings preserved)."""
    opener = text[start]
    if opener not in "[{\"":
        return text.find("\n", start) if "\n" in text[start:] else len(text)
    closer = {
        "[": "]", "{": "}", '"': '"',
    }[opener]
    top_quote = opener == '"'
    depth, escaped, quoted = 0, False, top_quote
    for index in range(start, len(text)):
        char = text[index]
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"' and index > start:
                if top_quote:
                    return index + 1
                quoted = False
            continue
        if char == '"':
            quoted = True
        elif char == opener:
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0:
                return index + 1
    return len(text)


def _space_field_span(text: str, space_name: str, field: str) -> tuple[int, int] | None:
    space = re.search(rf"(?m)^  {re.escape(_hcl_str(space_name))}\s*=\s*\{{", text)
    if not space:
        return None
    space_end = _hcl_value_end(text, space.end() - 1)
    match = re.search(rf"(?m)^    {re.escape(field)}\s*=\s*", text[space.start():space_end])
    if not match:
        return None
    start = space.start() + match.start()
    value_start = space.start() + match.end()
    end = _hcl_value_end(text, value_start)
    return start, end


def restore_reviewed_genie_text(existing: str, rendered: str, spaces: list[str]) -> str:
    """Restore reviewed fields verbatim after rendering refreshed API config."""
    for name in spaces:
        for field in _REVIEWED_GENIE_FIELDS:
            old_span = _space_field_span(existing, name, field)
            new_span = _space_field_span(rendered, name, field)
            if old_span and new_span:
                old = existing[old_span[0]:old_span[1]]
                rendered = rendered[:new_span[0]] + old + rendered[new_span[1]:]
    return rendered


def format_string_map_hcl(name: str, values: dict[str, str]) -> str:
    """Render a deterministic tool-owned string map."""
    lines = [f"{name} = {{"]
    for key in sorted(values):
        lines.append(f"  {_hcl_str(key)} = {_hcl_str(values[key])}")
    lines.append("}")
    return "\n".join(lines)


def remove_hcl_top_level_block(text: str, key: str) -> str:
    """Remove a top-level HCL assignment block 'key = { ... }' from text.

    Uses brace counting to correctly handle nested objects.
    """
    import re as _re

    pattern = _re.compile(rf"^{_re.escape(key)}\s*=\s*\{{", _re.MULTILINE)
    m = pattern.search(text)
    if not m:
        return text

    start = m.start()
    depth = 0
    end = m.end() - 1  # position of the opening {

    for i in range(m.end() - 1, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                end = i
                break

    # Strip one surrounding newline to avoid double blank lines
    block_end = end + 1
    if block_end < len(text) and text[block_end] == "\n":
        block_end += 1

    return text[:start] + text[block_end:]


def remove_hcl_top_level_list(text: str, key: str) -> str:
    """Remove a top-level HCL assignment list 'key = [ ... ]' from text.

    Uses bracket counting to correctly handle nested structures.
    """
    import re as _re

    pattern = _re.compile(rf"^{_re.escape(key)}\s*=\s*\[", _re.MULTILINE)
    m = pattern.search(text)
    if not m:
        return text

    start = m.start()
    depth = 0
    end = m.end() - 1  # position of the opening [

    for i in range(m.end() - 1, len(text)):
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
            if depth == 0:
                end = i
                break

    block_end = end + 1
    if block_end < len(text) and text[block_end] == "\n":
        block_end += 1

    return text[:start] + text[block_end:]


def strip_multi_space_legacy_genie_keys(text: str) -> str:
    """Remove flat legacy Genie fields when env.auto.tfvars owns multi-space input."""
    for key in (
        "genie_benchmarks", "genie_sql_filters", "genie_sql_expressions",
        "genie_sql_measures", "genie_join_specs",
    ):
        text = remove_hcl_top_level_block(text, key)
    for key in ("genie_sample_questions", "genie_acl_groups"):
        text = remove_hcl_top_level_list(text, key)
    scalar_re = re.compile(
        r'^\s*(?:genie_space_title|genie_space_description|genie_instructions)\s*=\s*"[^"\n]*"\s*$',
        re.MULTILINE,
    )
    return scalar_re.sub("", text)


def strip_draft_genie_acl_fields(tfvars_path: Path) -> int:
    """Remove model/API ACL fields from multi-space semantic draft entries."""
    import hcl2

    text = tfvars_path.read_text()
    try:
        cfg = hcl2.loads(text)
    except Exception:
        return 0
    configs = cfg.get("genie_space_configs") or {}
    if isinstance(configs, list):
        configs = configs[0] if configs else {}
    if not isinstance(configs, dict):
        return 0
    cleaned: dict[str, dict] = {}
    removed = 0
    for name, value in configs.items():
        if isinstance(value, list):
            value = value[0] if value else {}
        if not isinstance(value, dict):
            cleaned[name] = value
            continue
        entry = dict(value)
        for key in ("acl_groups", "genie_acl_groups"):
            if key in entry:
                entry.pop(key)
                removed += 1
        cleaned[name] = entry
    if removed:
        text = remove_hcl_top_level_block(text, "genie_space_configs").rstrip()
        tfvars_path.write_text(text + "\n\n" + format_genie_space_configs_hcl(cleaned) + "\n")
    return removed


def strip_abac_for_genie_mode(text: str) -> str:
    """Keep only workspace/Genie configuration in self-service Genie mode."""
    for key in ("groups", "group_members"):
        text = remove_hcl_top_level_block(text, key)
    for key in ("tag_policies", "tag_assignments", "fgac_policies"):
        text = remove_hcl_top_level_list(text, key)
    return text


def _fetch_via_patch_fallback(w, space_id: str) -> dict:
    """Read a Genie agent via a no-op PATCH (workaround for Partner AI gate).

    On workspaces where Partner Powered AI hasn't propagated, GET
    /api/2.0/genie/spaces/{id} is blocked.  However, PATCH is not gated
    and returns the full response including serialized_space.  We send a
    PATCH that re-reads the current title (effectively a no-op) to
    retrieve the space config without modifying it.
    """
    # First, get the title from the list endpoint (not gated)
    title = ""
    try:
        list_resp = w.api_client.do("GET", "/api/2.0/genie/spaces")
        for s in (list_resp.get("spaces", []) if isinstance(list_resp, dict) else []):
            if s.get("space_id") == space_id:
                title = s.get("title", "")
                break
    except Exception:
        pass

    # PATCH with the same title — no-op change, but returns serialized_space.
    # Use space_id as fallback (not a generic string) to avoid duplicate-name
    # conflicts when multiple spaces are fetched in the same workspace.
    resp = w.api_client.do(
        "PATCH",
        f"/api/2.0/genie/spaces/{space_id}",
        body={"title": title or f"Space {space_id}"},
    )
    return resp if isinstance(resp, dict) else {}


def fetch_tables_from_genie_space(
    space_id: str,
    auth_cfg: dict,
    quick_check_only: bool = False,
) -> tuple[list[str], dict, str, bool]:
    """Fetch tables and config from an existing Genie agent via the REST API.

    Returns (table_identifiers, genie_config_dict, space_title, complete).
    Uses GET /api/2.0/genie/spaces/{space_id} and parses serialized_space.

    Falls back to PATCH when GET is blocked by Partner Powered AI / cross-geo
    restrictions (common on new AWS workspaces where the setting hasn't
    propagated).  PATCH is not gated and returns the full serialized_space.

    Retries up to 5 times with backoff when serialized_space is empty —
    Databricks may process it asynchronously immediately after creation.
    """
    import json as _json
    import time as _time

    from databricks.sdk import WorkspaceClient

    configure_databricks_env(auth_cfg)
    w = WorkspaceClient(product=PRODUCT_NAME, product_version=PRODUCT_VERSION)

    print(f"  Querying Genie agent {space_id}...")
    _used_patch_fallback = False
    try:
        resp = w.api_client.do(
            "GET",
            f"/api/2.0/genie/spaces/{space_id}",
            query={"include_serialized_space": "true"},
        )
    except Exception as e:
        err_msg = str(e)
        if "Partner Powered AI" in err_msg or "cross-Geo" in err_msg:
            # GET is gated behind Partner Powered AI on new workspaces.
            # PATCH is not gated and returns serialized_space in its response.
            print(f"  GET blocked by Partner Powered AI — falling back to PATCH...")
            try:
                resp = _fetch_via_patch_fallback(w, space_id)
            except Exception as patch_error:
                print(
                    f"  WARNING: Could not reach Genie agent {space_id} via "
                    f"PATCH fallback: {patch_error}"
                )
                return [], {}, "", False
            _used_patch_fallback = True
        else:
            print(f"  WARNING: Could not reach Genie agent {space_id}: {e}")
            return [], {}, "", False

    if not isinstance(resp, dict):
        print(f"  WARNING: Unexpected response type from Genie agent {space_id}.")
        return [], {}, "", False

    space_title = resp.get("title", "")
    description = resp.get("description", "")
    serialized = resp.get("serialized_space", "")

    # Genie agents may take 1-3 minutes after creation before serialized_space
    # is populated by the Databricks backend (async processing).
    # Skip retries when uc_tables is already provided (quick_check_only=True) —
    # in that case we only need the space config, not table discovery, and
    # it still reports incomplete so persisted governance is preserved.
    if not serialized and not quick_check_only:
        retry_delays = [5, 10, 20, 30, 45, 60, 90]
        for attempt, delay in enumerate(retry_delays, start=1):
            print(f"  Genie agent {space_id} has no serialized_space yet — "
                  f"retrying in {delay}s (attempt {attempt}/{len(retry_delays)})...")
            _time.sleep(delay)
            try:
                if _used_patch_fallback:
                    resp = _fetch_via_patch_fallback(w, space_id)
                else:
                    resp = w.api_client.do(
                        "GET",
                        f"/api/2.0/genie/spaces/{space_id}",
                        query={"include_serialized_space": "true"},
                    )
                space_title = resp.get("title", space_title)
                description = resp.get("description", description)
                serialized = resp.get("serialized_space", "")
            except Exception as e:
                print(f"  WARNING: Retry failed: {e}")
            if serialized:
                break

    if not serialized:
        print(f"  WARNING: Genie agent {space_id} returned no serialized_space after retries.")
        return [], {}, space_title, False

    # --- Tables ---
    try:
        space_data = _json.loads(serialized)
        identifiers = footprint_table_refs(discover_agent_footprint(space_data))
    except Exception as e:
        print(f"  WARNING: Could not parse table list from Genie agent {space_id}: {e}")
        return [], {}, space_title, False

    if identifiers:
        print(f"    Discovered {len(identifiers)} table(s): {', '.join(identifiers)}")
    else:
        print(f"  WARNING: Genie agent {space_id} has no tables configured yet.")

    # --- Config ---
    try:
        genie_config = parse_genie_config_from_serialized_space(
            serialized, description=description
        )
    except Exception as e:
        print(f"  WARNING: Could not parse config from Genie agent {space_id}: {e}")
        return [], {}, space_title, False
    n_benchmarks = len(genie_config.get("benchmarks", []))
    n_filters = len(genie_config.get("sql_filters", []))
    n_measures = len(genie_config.get("sql_measures", []))
    print(
        f"    Parsed config: {n_benchmarks} benchmark(s), "
        f"{n_filters} filter(s), {n_measures} measure(s)"
    )

    return identifiers, genie_config, space_title, True


def fetch_tables_from_databricks(
    table_refs: list[str],
    auth_cfg: dict,
) -> tuple[str, list[tuple[str, str]]]:
    """Fetch table DDLs from Databricks using the SDK.

    Returns (ddl_text, catalog_schema_pairs) where catalog_schema_pairs
    is a deduplicated list of (catalog, schema) tuples found.
    """
    from databricks.sdk import WorkspaceClient

    configure_databricks_env(auth_cfg)
    w = WorkspaceClient(product=PRODUCT_NAME, product_version=PRODUCT_VERSION)

    tables = []
    for ref in table_refs:
        catalog, schema, table = _parse_table_ref(ref)
        if table == "*":
            print(f"  Listing tables in {catalog}.{schema}...")
            for t in w.tables.list(
                catalog_name=catalog, schema_name=schema
            ):
                tables.append(t)
                print(f"    Found: {t.full_name}")
        else:
            full_name = f"{catalog}.{schema}.{table}"
            print(f"  Fetching: {full_name}...")
            t = w.tables.get(full_name=full_name)
            tables.append(t)

    if not tables:
        print("ERROR: No tables found for the given references.")
        sys.exit(1)

    seen_pairs: dict[tuple[str, str], list[str]] = {}
    parts = []
    for t in tables:
        parts.append(format_table_info(t))
        cat = t.catalog_name
        sch = t.schema_name
        pair = (cat, sch)
        seen_pairs.setdefault(pair, []).append(t.name)

    ddl_text = "\n\n".join(parts)
    catalog_schemas = list(seen_pairs.keys())

    print(
        f"  Fetched {len(tables)} table(s) from "
        f"{len(catalog_schemas)} catalog.schema pair(s)\n"
    )
    return ddl_text, catalog_schemas


def _organize_ddl_by_catalog(ddl_text: str) -> str:
    """Organize DDL text with catalog section headers.

    When DDL spans multiple catalogs, adds clear headers so the LLM
    tracks all catalogs and doesn't "forget" one in its output.
    Single-catalog DDL is returned as a plain code block unchanged.
    """
    # Parse catalog names from CREATE TABLE / DESCRIBE TABLE statements
    catalog_blocks: dict[str, list[str]] = {}
    current_block: list[str] = []
    current_catalog = ""

    for line in ddl_text.split("\n"):
        # Detect table statements like "CREATE ... TABLE catalog.schema.table"
        # or "-- Table: catalog.schema.table" from DESCRIBE output
        m = re.match(r"(?:CREATE\s.*TABLE\s+|--\s*Table:\s*)(\w+)\.\w+\.\w+", line, re.IGNORECASE)
        if m:
            if current_block and current_catalog:
                catalog_blocks.setdefault(current_catalog, []).append("\n".join(current_block))
            current_catalog = m.group(1)
            current_block = [line]
        else:
            current_block.append(line)

    if current_block and current_catalog:
        catalog_blocks.setdefault(current_catalog, []).append("\n".join(current_block))

    # Single catalog or no catalog detected: return as-is
    if len(catalog_blocks) <= 1:
        return f"```sql\n{ddl_text}\n```"

    # Multiple catalogs: add section headers
    parts = []
    for catalog, blocks in catalog_blocks.items():
        table_count = len(blocks)
        parts.append(f"#### CATALOG: {catalog} ({table_count} table{'s' if table_count != 1 else ''})")
        parts.append(f"**You MUST generate tag_assignments and fgac_policies for ALL tables in this catalog.**\n")
        joined = "\n\n".join(blocks)
        parts.append(f"```sql\n{joined}\n```")
    return "\n\n".join(parts)


def build_prompt(ddl_text: str,
                 catalog_schemas: list[tuple[str, str]] | None = None,
                 group_names: list[str] | None = None,
                 per_space_name: str | None = None,
                 space_names: list[str] | None = None,
                 mode: str = "full",
                 countries: list[str] | None = None,
                 industries: list[str] | None = None,
                 create_groups: bool = False) -> str:
    """Build the full prompt by injecting DDL and optional group names into the template.

    create_groups controls group ownership in the generated config:
      * False (DEFAULT, consume) — the LLM must reuse the supplied ``group_names``
        exactly and must NOT invent, propose, or rename groups. The template's
        group-invention instructions are stripped so the LLM cannot mint groups
        that the consume-by-default account layer would then fail to look up.
      * True (opt-in, demo/greenfield) — the LLM proposes its own access-tier
        groups and the account layer creates them.

    When countries is set, country-specific identifier overlays are loaded from
    shared/countries/ and injected into the prompt to teach the LLM about
    region-specific masking patterns and regulatory context.

    When industries is set, industry-specific overlays are loaded from
    shared/industries/ and injected into the prompt with masking patterns,
    group templates, and access patterns for the target industry.

    When per_space_name is set, an extra instruction is injected telling the LLM
    to generate ONLY config for that specific space (skip groups and tag_policies,
    which are shared state established by full generation).

    When space_names is set, the LLM is told to use exactly those names as the
    keys in genie_space_configs — preventing it from inventing its own titles.
    """
    template = PROMPT_TEMPLATE_PATH.read_text()

    # Consume-by-default: unless create mode is explicitly requested, strip the
    # template's group-invention instructions so the LLM reuses the supplied
    # IdP-synced groups instead of minting new ones. Keeping numbering intact,
    # each invention line is rewritten (not deleted) into a consume instruction.
    if not create_groups:
        template = template.replace(
            '1. Create **groups** (access tiers like "Junior_Analyst", "Admin")',
            '1. Use the **groups** provided under "REQUIRED GROUP NAMES" — existing '
            'IdP-synced access tiers. Do NOT invent, rename, or add groups.',
        ).replace(
            "3. Propose groups — typically 2-5 access tiers "
            "(e.g., restricted, standard, privileged, admin)",
            '3. Use the group names provided under "REQUIRED GROUP NAMES" exactly — '
            "do NOT propose or invent new groups (they are owned by the IdP and "
            "consumed by name, not created here).",
        )

    section_marker = "### MY TABLES"
    idx = template.find(section_marker)

    cs_lines = ""
    if catalog_schemas:
        cs_lines = "Tables span these catalog.schema pairs:\n"
        for cat, sch in catalog_schemas:
            cs_lines += f"  - {cat}.{sch}\n"
        cs_lines += (
            "\nFor each fgac_policy, set catalog, function_catalog, and function_schema "
            "to match the catalog.schema of the tables the policy applies to.\n"
        )

    groups_lines = ""
    if group_names:
        groups_lines = (
            "\n### REQUIRED GROUP NAMES\n\n"
            "Use EXACTLY these group names in the generated config (groups, "
            "fgac_policies to_principals, genie ACLs). Do NOT invent new names.\n\n"
        )
        for g in group_names:
            groups_lines += f"  - {g}\n"
        groups_lines += "\n"

    space_names_lines = ""
    if space_names:
        space_names_lines = (
            "\n### REQUIRED GENIE AGENT NAMES\n\n"
            "Use EXACTLY these name(s) as the keys in `genie_space_configs`. "
            "Do NOT rename, merge, or invent alternative titles. "
            "Each name must appear verbatim as a map key.\n\n"
        )
        for name in space_names:
            space_names_lines += f"  - \"{name}\"\n"
        space_names_lines += "\n"

    per_space_instruction = ""
    if per_space_name:
        per_space_instruction = (
            "\n### PER-SPACE GENERATION MODE\n\n"
            f"You are generating config for a SINGLE Genie agent named: \"{per_space_name}\"\n\n"
            "IMPORTANT CONSTRAINTS:\n"
            "- Generate ONLY: genie_space_configs (for this space), tag_assignments "
            "(for the tables listed below), fgac_policies, masking functions, and "
            "tag_policies for any NEW tag keys required by this space's data domain.\n"
            "- If you use a tag_key in tag_assignments or fgac_policies conditions, "
            "you MUST define the corresponding tag_policy in this output.\n"
            "- Do NOT generate 'groups' — those are established shared governance state.\n"
            "- Do NOT generate 'group_members' — those are established shared governance state.\n"
            "- The groups to use in fgac_policies and genie ACLs are listed under "
            "REQUIRED GROUP NAMES above. Use them exactly.\n\n"
        )
    elif mode == "governance":
        per_space_instruction = (
            "\n### GOVERNANCE-ONLY MODE\n\n"
            "You are generating ABAC governance configuration for a central Data Governance team.\n\n"
            "IMPORTANT CONSTRAINTS:\n"
            "- Generate: groups, tag_policies, tag_assignments, fgac_policies, and masking functions.\n"
            "- Do NOT generate 'genie_space_configs' — Genie agent content is managed independently "
            "by each BU team. Omit the genie_space_configs block entirely from your output.\n"
            "- Focus on data classification (tags), access policies (FGAC), and masking functions "
            "that apply to the governed tables regardless of which Genie agents query them.\n\n"
        )
    elif mode == "genie":
        per_space_instruction = (
            "\n### GENIE-ONLY MODE\n\n"
            "You are generating Genie agent configurations for a BU team. "
            "The ABAC governance (groups, tag policies, tag assignments, FGAC policies, masking "
            "functions) is managed by a central Data Governance team — do NOT generate any of that.\n\n"
            "IMPORTANT CONSTRAINTS:\n"
            "- Generate ONLY: genie_space_configs (with instructions, sample_questions, benchmarks, "
            "sql_measures, sql_filters, sql_expressions, and join_specs for each Genie agent).\n"
            "- Do NOT generate 'groups' — use the group names listed under REQUIRED GROUP NAMES.\n"
            "- Do NOT generate 'tag_policies' — those are managed by the governance team.\n"
            "- Do NOT generate 'tag_assignments' — those are managed by the governance team.\n"
            "- Do NOT generate 'fgac_policies' — those are managed by the governance team.\n"
            "- Do NOT generate any masking SQL functions — those are managed by the governance team.\n"
            "- Do NOT include any placeholder comments, commented-out examples, or stub lines for "
            "the omitted sections (e.g. do NOT write '# tag_assignments = []' or similar).\n"
            "- Output only the HCL code block (genie_space_configs). "
            "The SQL code block should be empty or omitted.\n\n"
        )

    vocabulary_instruction = REGISTRY.render_prompt_block() + "\n"

    country_instruction = ""
    if countries:
        country_instruction = load_country_overlays(countries)

    industry_instruction = ""
    if industries:
        industry_instruction = load_industry_overlays(industries)
    overlay_detection_prompt = ""
    if industries:
        overlay_detection_prompt, _ = build_industry_detection_guidance(
            ddl_text,
            industries,
        )

    if idx == -1:
        print("WARNING: Could not find '### MY TABLES' in ABAC_PROMPT.md")
        print("  Appending DDL at the end of the prompt instead.\n")
        prompt = template + (
            f"\n\n{per_space_instruction}{vocabulary_instruction}"
            f"{country_instruction}{industry_instruction}{overlay_detection_prompt}"
            f"{groups_lines}{space_names_lines}{cs_lines}\n\n{ddl_text}\n"
        )
    else:
        prompt_body = template[:idx].rstrip()
        user_input = (
            f"\n\n{per_space_instruction}"
            f"{vocabulary_instruction}"
            f"{country_instruction}"
            f"{industry_instruction}"
            f"{overlay_detection_prompt}"
            f"{groups_lines}"
            f"{space_names_lines}"
            f"### MY TABLES\n\n"
            f"{cs_lines}\n"
            f"{_organize_ddl_by_catalog(ddl_text)}\n"
        )
        prompt = prompt_body + user_input

    return prompt


def extract_code_blocks(response_text: str) -> tuple[str | None, str | None]:
    """Extract the SQL and HCL code blocks from the LLM response."""
    sql_block = None
    hcl_block = None

    blocks = re.findall(
        r"```[ \t]*([A-Za-z0-9_-]*)[^\n]*\n?(.*?)```",
        response_text,
        re.DOTALL,
    )

    def _looks_like_hcl(content: str) -> bool:
        markers = (
            "groups",
            "tag_policies",
            "tag_assignments",
            "fgac_policies",
            "genie_space_configs",
            "uc_tables",
        )
        hits = sum(1 for marker in markers if re.search(rf"(?m)^\s*{marker}\s*=", content))
        return hits >= 2 or (
            "genie_space_configs" in content and "sample_questions" in content
        )

    def _extract_hcl_fallback(text: str) -> str | None:
        hcl_fence = re.search(r"```[ \t]*(hcl|terraform|tfvars)[^\n]*\n?(.*)$", text, re.DOTALL | re.IGNORECASE)
        if hcl_fence:
            candidate = hcl_fence.group(2).strip()
            return candidate if _looks_like_hcl(candidate) else None

        lines = text.splitlines()
        start = None
        for idx, line in enumerate(lines):
            if re.match(
                r"^\s*(groups|tag_policies|tag_assignments|fgac_policies|genie_space_configs|uc_tables)\s*=",
                line,
            ):
                start = idx
                break
        if start is None:
            return None

        candidate_lines: list[str] = []
        for line in lines[start:]:
            stripped = line.strip()
            if candidate_lines and stripped.startswith("```"):
                break
            candidate_lines.append(line)
        candidate = "\n".join(candidate_lines).strip()
        return candidate if _looks_like_hcl(candidate) else None

    hcl_candidates: list[str] = []
    sql_candidates: list[str] = []
    for lang, content in blocks:
        content = content.strip()
        lang_lower = lang.lower()

        if lang_lower == "sql":
            sql_candidates.append(content)
        elif lang_lower in ("hcl", "terraform"):
            hcl_candidates.append(content)
        elif not lang and "CREATE" in content.upper() and "FUNCTION" in content.upper():
            sql_candidates.append(content)
        elif not lang and _looks_like_hcl(content):
            hcl_candidates.append(content)

    # Pick the largest SQL block (most CREATE FUNCTION statements)
    if sql_candidates:
        sql_block = max(sql_candidates, key=lambda c: (c.upper().count("CREATE"), len(c)))

    # Pick the most complete HCL block — the one with the most top-level keys
    # (groups, tag_policies, tag_assignments, fgac_policies, genie_space_configs).
    # The LLM often emits partial blocks before the final complete one.
    if hcl_candidates:
        def _key_count(c: str) -> int:
            keys = ("groups", "tag_policies", "tag_assignments", "fgac_policies",
                    "genie_space_configs", "genie_space_title", "group_members")
            return sum(1 for k in keys if re.search(rf"(?m)^\s*{k}\s*=", c))
        hcl_block = max(hcl_candidates, key=lambda c: (_key_count(c), len(c)))

    if hcl_block is None:
        hcl_block = _extract_hcl_fallback(response_text)

    return sql_block, hcl_block


TFVARS_STRIP_KEYS = {
    "databricks_account_id",
    "databricks_client_id",
    "databricks_client_secret",
    "databricks_workspace_id",
    "databricks_workspace_host",
    "uc_catalog_name",
    "uc_schema_name",
    "uc_tables",
}


def sanitize_tfvars_hcl(hcl_block: str) -> str:
    """
    Make AI-generated tfvars easier and safer to use:
    - Strip auth variables (these come from auth.auto.tfvars)
    - Insert section-level explanations and doc links
    """

    # --- Strip auth fields (and common adjacent headers) ---
    stripped_lines: list[str] = []
    for line in hcl_block.splitlines():
        if re.match(r"^\s*#\s*Authentication\b", line, re.IGNORECASE):
            continue
        if re.match(r"^\s*#\s*Databricks\s+Authentication\b", line, re.IGNORECASE):
            continue
        # Strip SQL-style comments that the LLM sometimes emits into HCL output
        if re.match(r"^\s*--", line):
            continue

        m = re.match(r"^\s*([A-Za-z0-9_]+)\s*=", line)
        if m and m.group(1) in TFVARS_STRIP_KEYS:
            continue

        stripped_lines.append(line)

    # Collapse excessive blank lines
    compact: list[str] = []
    last_blank = False
    for line in stripped_lines:
        blank = line.strip() == ""
        if blank and last_blank:
            continue
        compact.append(line)
        last_blank = blank

    text = "\n".join(compact).strip() + "\n"

    # --- Insert explanatory blocks before major sections ---
    docs = (
        "# Docs:\n"
        "# - Governed tags / tag policies: https://docs.databricks.com/en/database-objects/tags.html\n"
        "# - Unity Catalog ABAC overview: https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac\n"
        "# - ABAC policies (masks + filters): https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies\n"
        "# - Row filters + column masks: https://docs.databricks.com/en/tables/row-and-column-filters.html\n"
        "#\n"
    )

    groups_block = (
        "# ----------------------------------------------------------------------------\n"
        "# Groups (business roles)\n"
        "# ----------------------------------------------------------------------------\n"
        "# Keys are group names. Use these to represent business personas (e.g., Analyst,\n"
        "# Researcher, Compliance). These groups are used for workspace onboarding,\n"
        "# Databricks One consumer access, data grants, and optional Genie agent ACLs.\n"
        "#\n"
        + docs
    )

    tag_policies_block = (
        "# ----------------------------------------------------------------------------\n"
        "# Tag policies (governed tags)\n"
        "# ----------------------------------------------------------------------------\n"
        "# Each entry defines a governed tag key and the allowed values. You’ll assign\n"
        "# these tags to tables/columns below, then reference them in FGAC policies.\n"
        "#\n"
        + docs
    )

    tag_assignments_block = (
        "# ----------------------------------------------------------------------------\n"
        "# Tag assignments (classify tables/columns)\n"
        "# ----------------------------------------------------------------------------\n"
        "# Apply governed tags to Unity Catalog objects.\n"
        "# - entity_type: \"tables\" or \"columns\"\n"
        "# - entity_name: fully qualified three-level name\n"
        "#   - table:  \"catalog.schema.Table\"\n"
        "#   - column: \"catalog.schema.Table.Column\"\n"
        "# - Table-level tags are optional; use them to scope column masks or row filters\n"
        "#   to specific tables, or for governance.\n"
        "#\n"
        + docs
    )

    fgac_block = (
        "# ----------------------------------------------------------------------------\n"
        "# FGAC policies (who sees what, and how)\n"
        "# ----------------------------------------------------------------------------\n"
        "# Each entry creates either a COLUMN MASK or ROW FILTER policy.\n"
        "#\n"
        "# Common fields:\n"
        "# - name: logical name for the policy (must be unique)\n"
        "# - policy_type: POLICY_TYPE_COLUMN_MASK | POLICY_TYPE_ROW_FILTER\n"
        "# - catalog: catalog this policy is scoped to\n"
        "# - function_catalog: catalog where the masking UDF lives\n"
        "# - function_schema: schema where the masking UDF lives\n"
        "# - to_principals: list of group names who receive this policy\n"
        "# - except_principals: optional list of groups excluded (break-glass/admin)\n"
        "# - comment: human-readable intent (recommended)\n"
        "#\n"
        "# For COLUMN MASK:\n"
        "# - match_condition: ABAC condition, e.g. hasTagValue('phi_level','full_phi')\n"
        "# - match_alias: the column alias used by the ABAC engine\n"
        "# - function_name: masking UDF name (relative; Terraform prefixes catalog.schema)\n"
        "# - when_condition: (optional) scope to specific tagged tables\n"
        "#\n"
        "# For ROW FILTER:\n"
        "# - when_condition: (optional) scope to specific tagged tables\n"
        "# - function_name: row filter UDF name (relative; must be zero-argument)\n"
        "#\n"
        "# Example \u2014 column mask (mask SSN for analysts, exempt compliance):\n"
        "#   {\n"
        "#     name              = \"mask_ssn_analysts\"\n"
        "#     policy_type       = \"POLICY_TYPE_COLUMN_MASK\"\n"
        "#     to_principals     = [\"Junior_Analyst\", \"Senior_Analyst\"]\n"
        "#     except_principals = [\"Compliance_Officer\"]\n"
        "#     comment           = \"Mask SSN showing only last 4 digits\"\n"
        "#     match_condition   = \"hasTagValue('pii_level', 'highly_sensitive')\"\n"
        "#     match_alias       = \"masked_ssn\"\n"
        "#     function_name     = \"mask_ssn\"\n"
        "#   }\n"
        "#\n"
        "# Example \u2014 row filter (restrict regional staff to their rows):\n"
        "#   {\n"
        "#     name           = \"filter_us_region\"\n"
        "#     policy_type    = \"POLICY_TYPE_ROW_FILTER\"\n"
        "#     to_principals  = [\"US_Region_Staff\"]\n"
        "#     comment        = \"Only show rows where region = US\"\n"
        "#     when_condition = \"hasTagValue('region_scope', 'global')\"\n"
        "#     function_name  = \"filter_by_region_us\"\n"
        "#   }\n"
        "#\n"
        + docs
    )

    def insert_before(pattern: str, block: str, s: str) -> str:
        # Avoid double-inserting if the block already exists nearby
        if block.strip() in s:
            return s
        return re.sub(pattern, block + r"\g<0>", s, count=1, flags=re.MULTILINE)

    genie_configs_block = (
        "# ----------------------------------------------------------------------------\n"
        "# Genie agent configs (per-space semantic configuration)\n"
        "# ----------------------------------------------------------------------------\n"
        "# Each key is the human-readable space name matching genie_spaces[*].name in\n"
        "# env.auto.tfvars. Contains instructions, benchmarks, and SQL measures.\n"
        "#\n"
        "# ACL ownership: set optional acl_groups on the matching genie_spaces[] entry\n"
        "# in env.auto.tfvars. Explicit [] means nobody; omission derives from policies.\n"
        "# Any ACL fields in this generated draft are ignored.\n"
        "#\n"
        + docs
    )

    text = insert_before(r"^groups\s*=\s*\{", groups_block, text)
    text = insert_before(r"^tag_policies\s*=\s*\[", tag_policies_block, text)
    text = insert_before(r"^tag_assignments\s*=\s*\[", tag_assignments_block, text)
    text = insert_before(r"^fgac_policies\s*=\s*\[", fgac_block, text)
    text = insert_before(r"^genie_space_configs\s*=\s*\{", genie_configs_block, text)

    return text


def _trim_incomplete_genie_tail(hcl_block: str) -> tuple[str, int]:
    """Trim a truncated legacy Genie tail when the LLM cuts off mid-section.

    Overlay responses sometimes end partway through `genie_sample_questions`,
    `genie_benchmarks`, or similar legacy single-space keys. When that happens,
    the ABAC sections above are still useful, but the trailing incomplete Genie
    fragment makes the whole tfvars file unparsable. If we detect one of these
    trailing keys without a balanced closing structure, trim from that key onward.
    """
    trailing_keys = (
        "genie_space_title",
        "genie_space_description",
        "genie_sample_questions",
        "genie_instructions",
        "genie_benchmarks",
        "genie_sql_filters",
        "genie_sql_expressions",
        "genie_sql_measures",
        "genie_join_specs",
        "genie_acl_groups",
        "genie_space_configs",
    )
    pattern = re.compile(
        rf"(?m)^\s*(?:{'|'.join(re.escape(k) for k in trailing_keys)})\s*="
    )
    matches = list(pattern.finditer(hcl_block))
    if not matches:
        return hcl_block, 0

    last = matches[-1]
    prefix = hcl_block[:last.start()]
    suffix = hcl_block[last.start():]
    opens = suffix.count("[") + suffix.count("{")
    closes = suffix.count("]") + suffix.count("}")
    if opens <= closes:
        return hcl_block, 0
    trimmed = prefix.rstrip() + "\n"
    return trimmed, 1


def call_anthropic(prompt: str, model: str) -> str:
    """Call Claude via the Anthropic API."""
    try:
        import anthropic
    except ImportError:
        print("ERROR: anthropic package not installed. Run:")
        print("  pip install anthropic")
        sys.exit(2)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set.")
        print("  export ANTHROPIC_API_KEY='sk-ant-...'")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    print(f"  Calling Anthropic ({model})...")

    message = client.messages.create(
        model=model,
        max_tokens=32768,
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def call_openai(prompt: str, model: str) -> str:
    """Call GPT via the OpenAI API."""
    try:
        import openai
    except ImportError:
        print("ERROR: openai package not installed. Run:")
        print("  pip install openai")
        sys.exit(2)

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY environment variable not set.")
        print("  export OPENAI_API_KEY='sk-...'")
        sys.exit(1)

    client = openai.OpenAI(api_key=api_key)
    print(f"  Calling OpenAI ({model})...")

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a Databricks Unity Catalog ABAC expert."},
            {"role": "user", "content": prompt},
        ],
        max_tokens=32768,
        temperature=0,
    )
    return response.choices[0].message.content


def call_databricks(prompt: str, model: str) -> str:
    """Call a model via the Databricks Foundation Model API."""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
    except ImportError:
        print("ERROR: databricks-sdk package not installed. Run:")
        print("  pip install databricks-sdk")
        sys.exit(2)

    from databricks.sdk.config import Config

    cfg = Config(http_timeout_seconds=900, product=PRODUCT_NAME, product_version=PRODUCT_VERSION)
    w = WorkspaceClient(config=cfg)
    print(f"  Calling Databricks FMAPI ({model})...")

    query_args = dict(
        name=model,
        messages=[
            ChatMessage(role=ChatMessageRole.SYSTEM, content="You are a Databricks Unity Catalog ABAC expert."),
            ChatMessage(role=ChatMessageRole.USER, content=prompt),
        ],
        max_tokens=32768,
    )
    # Claude 5+ endpoints reject the temperature parameter. Keep deterministic
    # temperature=0 behavior for older/current models that support it.
    if not re.match(r"^databricks-claude-(?:sonnet|opus)-(?:[5-9]|\d{2})", model):
        query_args["temperature"] = 0

    response = w.serving_endpoints.query(**query_args)
    content = response.choices[0].message.content
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_blocks = []
        for block in content:
            block_type = block.get("type") if isinstance(block, dict) else getattr(block, "type", "")
            block_text = block.get("text", "") if isinstance(block, dict) else getattr(block, "text", "")
            if block_type == "text" and block_text:
                text_blocks.append(block_text)
        if text_blocks:
            return "\n".join(text_blocks)
    raise ValueError(f"Databricks FMAPI model {model} returned no text content")


PROVIDERS = {
    "databricks": {
        "call": call_databricks,
        "default_model": "databricks-claude-sonnet-4-6",
    },
    "anthropic": {
        "call": call_anthropic,
        "default_model": "claude-sonnet-4-20250514",
    },
    "openai": {
        "call": call_openai,
        "default_model": "gpt-4o",
    },
}


class Spinner:
    """Simple terminal spinner for long-running operations."""

    FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

    def __init__(self, message: str = "Working"):
        self._message = message
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._start_time = 0.0

    def __enter__(self):
        self._start_time = time.time()
        # Animate only on a terminal; in CI or redirected logs the frames
        # would pile up as thousands of characters on one line.
        if sys.stderr.isatty():
            self._thread = threading.Thread(target=self._spin, daemon=True)
            self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        if self._thread:
            self._thread.join()
        elapsed = time.time() - self._start_time
        prefix = "\r" if self._thread else ""
        sys.stderr.write(f"{prefix}  {self._message} — done ({elapsed:.1f}s)\n")
        sys.stderr.flush()

    def _spin(self):
        i = 0
        while not self._stop.is_set():
            elapsed = time.time() - self._start_time
            frame = self.FRAMES[i % len(self.FRAMES)]
            sys.stderr.write(f"\r  {frame} {self._message} ({elapsed:.0f}s)")
            sys.stderr.flush()
            i += 1
            self._stop.wait(0.1)


def call_with_retries(call_fn, prompt: str, model: str, max_retries: int) -> str:
    """Call an LLM provider with exponential backoff retries."""
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            with Spinner(f"Calling LLM (attempt {attempt}/{max_retries})"):
                return call_fn(prompt, model)
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                wait = min(2 ** attempt, 60)
                print(f"\n  Attempt {attempt} failed: {e}")
                print(f"  Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"\n  Attempt {attempt} failed: {e}")
    raise RuntimeError(f"All {max_retries} attempts failed. Last error: {last_error}")


def _cleanup_stray_commas(text: str) -> str:
    """Remove stray commas left behind by block removals in HCL text.

    Handles bare comma lines, consecutive commas, and trailing commas before ]
    (but preserves valid trailing commas after ``}`` or quoted strings).
    """
    text = re.sub(r'^\s*,\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r',(\s*,)+', ',', text)
    # Only strip a comma before ] when preceded by whitespace (bare/stray comma),
    # not when preceded by } or " (valid HCL trailing comma).
    text = re.sub(r'(?<![}"\']),(\s*\])', r'\1', text)
    return text


def fix_hcl_syntax(tfvars_path: Path) -> int:
    """Repair common HCL syntax errors introduced by the LLM.

    1. Missing commas between consecutive objects in a list.  The LLM
       sometimes omits the trailing comma after a closing ``}`` before the
       next ``{``.  Blank lines and comment lines between the two braces
       are handled correctly.
    2. Object-style tag_policy values (``values = [{name="v"}]`` →
       ``values = ["v"]``) — the LLM sometimes copies Terraform resource
       syntax into the plain-string values list of the ABAC config.

    Returns the number of repairs made.
    """
    text = tfvars_path.read_text()
    original = text
    repairs = 0

    # ------------------------------------------------------------------
    # Fix 1: missing commas between adjacent objects in a list.
    # Strategy: scan line by line.  When we find a line that ends with
    # just "}" (possibly indented) and the NEXT non-blank, non-comment
    # line starts with the same or less indentation and a "{", we add a
    # trailing comma to the "}" line.
    # ------------------------------------------------------------------
    lines = text.splitlines(keepends=True)
    out_lines: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.rstrip('\n').rstrip()
        # Check if this line ends with an un-trailed closing brace
        if stripped.endswith('}') and not stripped.endswith('},'):
            # Look ahead: find the next non-blank, non-comment line
            j = i + 1
            while j < len(lines) and (
                lines[j].strip() == '' or lines[j].lstrip().startswith('#') or lines[j].lstrip().startswith('--')
            ):
                j += 1
            if j < len(lines):
                next_stripped = lines[j].lstrip()
                if next_stripped.startswith('{'):
                    # Add the missing comma
                    line = line.rstrip('\n').rstrip() + ',\n'
                    repairs += 1
        out_lines.append(line)
        i += 1

    text = ''.join(out_lines)

    # ------------------------------------------------------------------
    # Fix 2: convert values = [{name = "v"}, ...] → values = ["v", ...]
    # ------------------------------------------------------------------
    def _object_vals_to_strings(m: re.Match) -> str:
        full = m.group(0)
        names = re.findall(r'name\s*=\s*"([^"]+)"', full)
        if names:
            return 'values = [' + ', '.join(f'"{n}"' for n in names) + ']'
        return full

    fixed2 = re.sub(
        r'values\s*=\s*\[\s*\{[^]]*?\}\s*(?:,\s*\{[^]]*?\}\s*)*\]',
        _object_vals_to_strings,
        text,
        flags=re.DOTALL,
    )
    if fixed2 != text:
        repairs += 1
        text = fixed2

    # ------------------------------------------------------------------
    # Fix 3: strip Terraform variable references ("${var.*}") from tfvars.
    # The LLM sometimes emits e.g. benchmarks = "${var.genie_benchmarks}"
    # which is illegal in .tfvars files.  Replace with an empty list.
    # ------------------------------------------------------------------
    fixed3 = re.sub(
        r'(\b(?:genie_)?(?:benchmarks|sql_filters|sql_expressions|sql_measures|join_specs)\s*=\s*)"?\$\$?\{var\.[^}]+\}"?',
        r'\1[]',
        text,
    )
    if fixed3 != text:
        repairs += 1
        text = fixed3

    # ------------------------------------------------------------------
    # Fix 4: remove LLM placeholder ellipses that cause HCL parse errors.
    #   - Standalone "..." lines
    #   - "[...]" placeholder lists → "[]"
    # ------------------------------------------------------------------
    fixed4 = re.sub(r'^\s*\.\.\..*$\n?', '', text, flags=re.MULTILINE)
    fixed4 = re.sub(r'\[\s*\.\.\.\s*\]', '[]', fixed4)
    if fixed4 != text:
        repairs += 1
        text = fixed4

    # ------------------------------------------------------------------
    # Fix 5: remove stray commas left by autofix block removals.
    # ------------------------------------------------------------------
    fixed5 = _cleanup_stray_commas(text)
    if fixed5 != text:
        repairs += 1
        text = fixed5

    if text != original:
        tfvars_path.write_text(text)
        print(f"  [AUTOFIX] Repaired {repairs} HCL syntax issue(s)")

    return repairs


def _fetch_live_tag_policy_values() -> dict[str, set[str]]:
    """Query Databricks for existing tag policy keys and their allowed values.

    Returns {tag_key: set(values)}.  Returns an empty dict on any failure
    (network, auth, API unavailable) so callers can proceed without live data.
    """
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(product="genierails", product_version="0.1.0")
        result: dict[str, set[str]] = {}
        for tp in w.tag_policies.list_tag_policies():
            tag_key = getattr(tp, "tag_key", "") or ""
            values = {
                getattr(v, "name", "") or ""
                for v in (getattr(tp, "values", None) or [])
                if getattr(v, "name", "") or ""
            }
            if tag_key:
                result[tag_key] = values
        return result
    except Exception as exc:
        print(f"  [AUTOFIX] Could not fetch live tag policies ({exc}); using file-only values")
        return {}


class NativeClassificationRequiredError(RuntimeError):
    """Raised when native classification is required but cannot be read."""


def _fetch_live_classification_source(
    table_refs: list[str] | None,
    auth_cfg: dict,
    *,
    require_native: bool = False,
) -> SensitivitySource | None:
    """Best-effort: build a ClassificationSource from native UC Data Classification.

    Queries ``system.information_schema.column_tags`` (``class.*`` namespace) —
    and ``system.data_classification.results`` when present — through a SQL
    warehouse, using the same statement-execution pattern as
    scripts/audit_schema_drift.py.

    Returns ``None`` for unavailable/empty native data only when native mode is
    not required.  With ``require_native=True`` every unreadable or empty scan
    is a hard failure, preventing a silent downgrade to LLM inference.
    """
    # Concrete tables and schema-scoped ``catalog.schema.*`` footprints are both
    # readable; dropping wildcards here silently skipped native classification.
    catalog = str(auth_cfg.get("uc_catalog") or "").strip().strip("`\"")
    table_fqns = []
    unresolved = []
    for ref in table_refs or []:
        normalized = ".".join(
            part.strip().strip("`\"") for part in str(ref).strip().split(".")
        )
        parts = normalized.split(".")
        if len(parts) == 2 and catalog:
            normalized = f"{catalog}.{normalized}"
            parts = normalized.split(".")
        if len(parts) == 3 and "*" not in parts[:2]:
            table_fqns.append(normalized)
        else:
            unresolved.append(str(ref))
    if unresolved and require_native:
        raise NativeClassificationRequiredError(
            "Required native classification footprint contains unresolvable "
            f"table references: {', '.join(unresolved)}"
        )
    if not table_fqns:
        if require_native:
            raise NativeClassificationRequiredError(
                "No resolvable catalog.schema.table footprint is available for "
                "required native classification"
            )
        return None
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import StatementState

        configure_databricks_env(auth_cfg)
        w = WorkspaceClient(product=PRODUCT_NAME, product_version=PRODUCT_VERSION)

        warehouse_id = str(auth_cfg.get("sql_warehouse_id") or "").strip()
        if not warehouse_id:
            for wh in w.warehouses.list():
                if wh.id:
                    warehouse_id = wh.id
                    break
        if not warehouse_id:
            message = "No SQL warehouse available for native classification read"
            if require_native:
                raise NativeClassificationRequiredError(message)
            print(f"  [SENSITIVITY] {message}; using DDL inference only")
            return None

        def _run_sql(sql: str) -> list:
            r = w.statement_execution.execute_statement(
                statement=sql, warehouse_id=warehouse_id, wait_timeout="50s",
            )
            while r.status and r.status.state in (StatementState.PENDING, StatementState.RUNNING):
                time.sleep(2)
                r = w.statement_execution.get_statement(r.statement_id)
            if r.status and r.status.state == StatementState.FAILED:
                err = getattr(r.status, "error", None)
                msg = getattr(err, "message", str(err)) if err else "unknown"
                raise RuntimeError(msg)
            if r.result and r.result.data_array:
                return r.result.data_array
            return []

        source = ClassificationSource.from_run_sql(_run_sql, table_fqns)
        if not source.has_native_data():
            message = (
                "Native classification read succeeded but returned 0 class.* "
                "findings for the declared footprint — in Catalog Explorer, open "
                "the catalog > Details tab > Data classification and turn it on. "
                "When the scan finishes, review the detections and exclude any false "
                "positives, then turn on auto-tagging and wait until the class.* tags appear"
            )
            if require_native:
                raise NativeClassificationRequiredError(message)
            print(f"  [SENSITIVITY] {message}; using DDL inference only")
            return None
        print(
            "  [SENSITIVITY] Native UC Data Classification found for "
            f"{len(source.classified_columns())} column(s) (class.* tags) — "
            "using as authoritative sensitivity source"
        )
        return source
    except Exception as exc:
        if require_native:
            if isinstance(exc, NativeClassificationRequiredError):
                raise
            raise NativeClassificationRequiredError(
                f"Could not read required native classification: {exc}"
            ) from exc
        print(f"  [SENSITIVITY] Could not read native classification ({exc}); using DDL inference only")
        return None


_LIVE_POLICIES_REPORTED = False


def autofix_tag_policies(tfvars_path: Path) -> int:
    """Add tag values used in assignments/policies but missing from tag_policies.

    When Databricks credentials are available (env vars set by configure_databricks_env),
    also seeds allowed values from live tag policies so the generated config doesn't
    reference values that don't exist in the live policy.
    """
    text = tfvars_path.read_text()

    live_policies = _fetch_live_tag_policy_values()
    global _LIVE_POLICIES_REPORTED
    if live_policies and (not _LIVE_POLICIES_REPORTED or os.environ.get("VERBOSE") == "1"):
        print(f"  [AUTOFIX] Loaded {len(live_policies)} live tag policy/ies from Databricks")
        _LIVE_POLICIES_REPORTED = True

    # Map key → (list_of_values, raw_values_text) preserving the EXACT text
    # from the file so that the replacement uses the original formatting.
    allowed: dict[str, list[str]] = {}
    raw_vals_text: dict[str, str] = {}  # key → exact captured text inside [...]
    for m in re.finditer(
        r'\{\s*key\s*=\s*"([^"]+)"[^}]*?values\s*=\s*\[([^\]]*)\]',
        text,
        re.DOTALL,
    ):
        key = m.group(1)
        raw = m.group(2)
        file_values = re.findall(r'"([^"]+)"', raw)
        live_values = live_policies.get(key, set())
        allowed[key] = list(dict.fromkeys(file_values + sorted(live_values - set(file_values))))
        raw_vals_text[key] = raw

    # Collect tag_key/tag_value pairs from tag_assignments using HCL parsing
    # (the regex approach with [^}]*? is fragile when blocks contain comments
    # with closing braces).
    used: dict[str, set[str]] = {}
    try:
        import hcl2 as _hcl2_tp
        import io as _io_tp
        cfg = _hcl2_tp.load(_io_tp.StringIO(text))
        for ta in cfg.get("tag_assignments", []):
            if isinstance(ta, dict):
                tk = ta.get("tag_key", "")
                tv = ta.get("tag_value", "")
                if tk and tv:
                    used.setdefault(tk, set()).add(tv)
    except Exception:
        # Fallback to regex if HCL parsing fails (e.g. syntax errors in draft)
        for m in re.finditer(
            r'tag_key\s*=\s*"([^"]+)"[^}]*?tag_value\s*=\s*"([^"]+)"',
            text, re.DOTALL,
        ):
            used.setdefault(m.group(1), set()).add(m.group(2))
        for m in re.finditer(
            r'tag_value\s*=\s*"([^"]+)"[^}]*?tag_key\s*=\s*"([^"]+)"',
            text, re.DOTALL,
        ):
            used.setdefault(m.group(2), set()).add(m.group(1))
    # NOTE: We intentionally do NOT add values from hasTagValue() in FGAC
    # conditions.  The LLM sometimes hallucinate tag values in conditions
    # (e.g. 'full_card' when the policy only defines 'masked_card_last4').
    # Adding these to the tag policy would promote hallucinations into valid
    # values.  Instead, autofix_invalid_condition_values() (below) removes
    # FGAC policies that reference values not in the tag policy.

    added_total = 0
    for key in used:
        if key not in allowed:
            continue
        missing = sorted(used[key] - set(allowed[key]))
        if not missing:
            continue
        # Use the RAW captured text as the search key (exact match) so
        # spacing/formatting differences in the original don't cause misses.
        # The replacement uses normalized ", " separators.
        raw_old = raw_vals_text[key]
        new_vals = ", ".join(f'"{v}"' for v in allowed[key] + missing)
        text = text.replace(
            f'values = [{raw_old}]',
            f'values = [{new_vals}]',
            1,
        )
        allowed[key].extend(missing)
        raw_vals_text[key] = new_vals
        added_total += len(missing)
        for val in missing:
            print(f"  [AUTOFIX] Added '{val}' to tag_policy '{key}'")

    if added_total:
        tfvars_path.write_text(text)

    return added_total


# ---------------------------------------------------------------------------
# Delta mode helpers (incremental schema-drift classification)
# ---------------------------------------------------------------------------

def validate_delta_assignments(
    assignments: list[dict],
    governed: dict[str, list[str]],
    drifted_columns: set[str],
) -> list[str]:
    """Validate LLM-generated tag_assignments against the governed key/value universe.

    Returns a list of error messages (empty if all valid).
    """
    errors = []
    for ta in assignments:
        key = ta.get("tag_key", "")
        value = ta.get("tag_value", "")
        entity = ta.get("entity_name", "")

        if key not in governed:
            errors.append(f"Unknown tag_key '{key}' (allowed: {sorted(governed.keys())})")
        elif value not in governed[key]:
            errors.append(f"Unknown tag_value '{value}' for key '{key}' (allowed: {governed[key]})")

        if entity not in drifted_columns:
            errors.append(f"entity_name '{entity}' is not in the set of drifted columns")

    return errors


def merge_delta_assignments(tfvars_path: Path, new_assignments: list[dict]) -> int:
    """Append new tag_assignments to an existing abac.auto.tfvars file.

    Deduplicates by (entity_type, entity_name, tag_key). Returns the count of
    assignments actually added.
    """
    text = tfvars_path.read_text()

    existing_keys: set[str] = set()
    for m in re.finditer(
        r'entity_type\s*=\s*"([^"]+)"[^}]*?entity_name\s*=\s*"([^"]+)"[^}]*?tag_key\s*=\s*"([^"]+)"',
        text, re.DOTALL,
    ):
        existing_keys.add((m.group(1), m.group(2), m.group(3)))

    to_add = []
    for ta in new_assignments:
        dedup_key = (ta["entity_type"], ta["entity_name"], ta["tag_key"])
        if dedup_key not in existing_keys:
            to_add.append(ta)
            existing_keys.add(dedup_key)

    if not to_add:
        return 0

    blocks = []
    for ta in to_add:
        blocks.append(
            "  {\n"
            f'    entity_type = "{ta["entity_type"]}"\n'
            f'    entity_name = "{ta["entity_name"]}"\n'
            f'    tag_key     = "{ta["tag_key"]}"\n'
            f'    tag_value   = "{ta["tag_value"]}"\n'
            "  },"
        )
    insert_text = "\n".join(blocks)

    # Find the closing ] of the tag_assignments list specifically.
    ta_match = re.search(r'tag_assignments\s*=\s*\[', text)
    if ta_match:
        start = ta_match.end()
        depth = 1
        i = start
        while i < len(text) and depth > 0:
            if text[i] == "[":
                depth += 1
            elif text[i] == "]":
                depth -= 1
            i += 1
        closing = i - 1  # position of the matching ]
        before_close = text[:closing].rstrip()
        if not before_close.endswith(","):
            before_close += ","
        text = before_close + "\n" + insert_text + "\n" + text[closing:]
    else:
        text += f"\ntag_assignments = [\n{insert_text}\n]\n"

    tfvars_path.write_text(text)
    return len(to_add)


def remove_stale_assignments(tfvars_path: Path, stale_entities: list[str]) -> int:
    """Remove tag_assignment blocks whose entity_name is in stale_entities.

    Returns the count of blocks removed.
    """
    if not stale_entities:
        return 0

    text = tfvars_path.read_text()
    removed = 0

    for entity in stale_entities:
        pattern = re.compile(
            r'entity_name\s*=\s*"' + re.escape(entity) + r'"'
        )
        while True:
            m = pattern.search(text)
            if not m:
                break
            pos = m.start()
            block_start = None
            i = pos - 1
            while i >= 0:
                if text[i] == "{":
                    block_start = i
                    break
                i -= 1
            if block_start is None:
                break
            block_end = None
            depth = 1
            j = pos
            while j < len(text):
                if text[j] == "{":
                    depth += 1
                elif text[j] == "}":
                    depth -= 1
                    if depth == 0:
                        block_end = j + 1
                        break
                j += 1
            if block_end is None:
                break
            trail = block_end
            while trail < len(text) and text[trail] in (" ", "\t"):
                trail += 1
            if trail < len(text) and text[trail] == ",":
                trail += 1
            while trail < len(text) and text[trail] in ("\n", "\r"):
                trail += 1
            text = text[:block_start] + text[trail:]
            removed += 1

    if removed:
        tfvars_path.write_text(text)
    return removed


def autofix_undefined_tag_refs(tfvars_path: Path) -> int:
    """Remove tag_assignments and fgac_policies that reference undefined tag_keys.

    The LLM may generate tag_assignments or fgac_policies that use tag_key values
    not defined in tag_policies.  These produce validation errors.  This function
    removes such entries so the config can pass validation without requiring manual
    editing.

    Returns the total number of items removed.
    """
    try:
        import hcl2 as _hcl2  # type: ignore
    except ImportError:
        return 0

    text = tfvars_path.read_text()

    try:
        cfg = _hcl2.loads(text)
    except Exception:
        return 0

    # Collect defined tag keys.
    defined_keys: set[str] = set()
    for tp in cfg.get("tag_policies", []):
        k = tp.get("key", "")
        if k:
            defined_keys.add(k)

    if not defined_keys:
        return 0  # nothing to validate against

    total_removed = 0

    # ── Remove tag_assignments with undefined tag_key ──────────────────────
    assignments = cfg.get("tag_assignments", [])
    bad_tag_keys_ta: set[str] = set()
    for ta in assignments:
        k = ta.get("tag_key", "")
        if k and k not in defined_keys:
            bad_tag_keys_ta.add(k)

    if bad_tag_keys_ta:
        # Remove each assignment block that contains `tag_key = "<bad_key>"`.
        # Each assignment is a brace-delimited block inside the tag_assignments list.
        for bad_key in sorted(bad_tag_keys_ta):
            pattern = re.compile(r'tag_key\s*=\s*"' + re.escape(bad_key) + r'"')
            while True:
                m = pattern.search(text)
                if not m:
                    break
                # Find the enclosing { ... } block.
                pos = m.start()
                # Walk backward to find the opening {.
                depth = 0
                block_start = None
                i = pos - 1
                while i >= 0:
                    c = text[i]
                    if c == "}":
                        depth += 1
                    elif c == "{":
                        if depth == 0:
                            block_start = i
                            break
                        depth -= 1
                    i -= 1
                if block_start is None:
                    break
                # Walk forward to find the matching }.
                depth = 0
                block_end = None
                i = block_start
                while i < len(text):
                    c = text[i]
                    if c == "{":
                        depth += 1
                    elif c == "}":
                        depth -= 1
                        if depth == 0:
                            block_end = i
                            break
                    i += 1
                if block_end is None:
                    break
                # Include any trailing comma and whitespace.
                end = block_end + 1
                while end < len(text) and text[end] in (",", " ", "\t"):
                    end += 1
                # Include a leading newline if present.
                start = block_start
                while start > 0 and text[start - 1] in (" ", "\t"):
                    start -= 1
                if start > 0 and text[start - 1] == "\n":
                    start -= 1
                text = text[:start] + text[end:]
                total_removed += 1
                print(f"  [AUTOFIX] Removed tag_assignment with undefined tag_key '{bad_key}'")

    # ── Remove fgac_policies that reference undefined tag_keys ────────────
    policies = cfg.get("fgac_policies", [])
    bad_policy_names: list[str] = []
    for p in policies:
        # Collect all condition expressions from this policy.
        # Policies may use either a top-level match_condition/when_condition
        # or a nested conditions list with condition fields.
        cond_exprs: list[str] = []
        for key in ("match_condition", "when_condition"):
            val = p.get(key, "")
            if isinstance(val, list):
                val = val[0] if val else ""
            if val:
                cond_exprs.append(val)
        for cond_block in p.get("conditions", []):
            cond_expr = cond_block.get("condition", "") if isinstance(cond_block, dict) else ""
            if cond_expr:
                cond_exprs.append(cond_expr)

        for cond_expr in cond_exprs:
            for m in re.finditer(r"hasTagValue\(\s*'([^']+)'", cond_expr):
                if m.group(1) not in defined_keys:
                    pname = p.get("name", "")
                    if pname and pname not in bad_policy_names:
                        bad_policy_names.append(pname)
                        print(
                            f"  [AUTOFIX] Removing fgac_policy '{pname}': "
                            f"references undefined tag_key '{m.group(1)}'"
                        )

    if bad_policy_names:
        # Reuse the _remove_block logic from autofix_fgac_policy_count inline.
        def _remove_block(txt: str, block_name: str) -> tuple[str, bool]:
            name_pat = re.compile(r'name\s*=\s*"' + re.escape(block_name) + r'"')
            bm = name_pat.search(txt)
            if not bm:
                return txt, False
            pos = bm.start()
            depth = 0
            block_start = None
            i = pos - 1
            while i >= 0:
                c = txt[i]
                if c == "}":
                    depth += 1
                elif c == "{":
                    if depth == 0:
                        block_start = i
                        break
                    depth -= 1
                i -= 1
            if block_start is None:
                return txt, False
            depth = 0
            block_end = None
            i = block_start
            while i < len(txt):
                c = txt[i]
                if c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0:
                        block_end = i
                        break
                i += 1
            if block_end is None:
                return txt, False
            end = block_end + 1
            while end < len(txt) and txt[end] in (",", " ", "\t"):
                end += 1
            start = block_start
            while start > 0 and txt[start - 1] in (" ", "\t"):
                start -= 1
            if start > 0 and txt[start - 1] == "\n":
                start -= 1
            return txt[:start] + txt[end:], True

        for pname in bad_policy_names:
            text, removed = _remove_block(text, pname)
            if removed:
                total_removed += 1

    if total_removed:
        tfvars_path.write_text(text)

    return total_removed


def autofix_invalid_tag_values(tfvars_path: Path) -> int:
    """Remove tag_assignments whose tag_value is not in the allowed values for its tag_key.

    The LLM may generate tag_assignments with tag_values that don't match
    the values defined in tag_policies (e.g. ``rounded`` instead of
    ``rounded_amounts``).  This function removes such entries.

    Uses block-level matching (``_find_bracket_section`` + ``_find_brace_blocks``)
    to avoid cross-block regex issues where a DOTALL pattern could span
    multiple ``{ … }`` blocks and accidentally remove valid assignments.

    Returns the total number of items removed.
    """
    try:
        import hcl2 as _hcl2  # type: ignore
    except ImportError:
        return 0

    text = tfvars_path.read_text()

    try:
        cfg = _hcl2.loads(text)
    except Exception:
        return 0

    # Build map: tag_key → set of allowed values.
    allowed: dict[str, set[str]] = {}
    for tp in cfg.get("tag_policies", []):
        k = tp.get("key", "")
        vals = tp.get("values", [])
        if k and vals:
            allowed[k] = set(vals)

    if not allowed:
        return 0

    # Find tag_assignments with invalid values via hcl2 (order-independent).
    bad_pairs: set[tuple[str, str]] = set()  # (tag_key, tag_value)
    for ta in cfg.get("tag_assignments", []):
        k = ta.get("tag_key", "")
        v = ta.get("tag_value", "")
        registry_allowed = REGISTRY.is_allowed_value(k, v)
        if registry_allowed is False:
            bad_pairs.add((k, v))
            continue
        if k in allowed and v and v not in allowed[k]:
            bad_pairs.add((k, v))

    if not bad_pairs:
        return 0

    # Use block-level matching to remove only the exact blocks that contain
    # the invalid (tag_key, tag_value) pair — no cross-block regex.
    section = _find_bracket_section(text, "tag_assignments")
    if section is None:
        return 0

    sec_start, sec_end = section
    section_text = text[sec_start:sec_end]
    blocks = _find_brace_blocks(section_text)
    if not blocks:
        return 0

    # Identify blocks to remove by checking each block individually.
    remove_indices: list[int] = []
    for idx, (blk_start, blk_end) in enumerate(blocks):
        block_text = section_text[blk_start:blk_end + 1]
        tag_key_m = re.search(r'tag_key\s*=\s*"([^"]+)"', block_text)
        tag_val_m = re.search(r'tag_value\s*=\s*"([^"]+)"', block_text)
        if not tag_key_m or not tag_val_m:
            continue
        pair = (tag_key_m.group(1), tag_val_m.group(1))
        if pair in bad_pairs:
            remove_indices.append(idx)

    if not remove_indices:
        return 0

    # Remove blocks in reverse order to preserve earlier offsets.
    rewritten = section_text
    for idx in reversed(remove_indices):
        blk_start, blk_end = blocks[idx]
        # Include trailing comma/whitespace.
        end = blk_end + 1
        while end < len(rewritten) and rewritten[end] in (",", " ", "\t"):
            end += 1
        # Include leading whitespace/newline.
        start = blk_start
        while start > 0 and rewritten[start - 1] in (" ", "\t"):
            start -= 1
        if start > 0 and rewritten[start - 1] == "\n":
            start -= 1
        bad_key = re.search(r'tag_key\s*=\s*"([^"]+)"', rewritten[blk_start:blk_end + 1]).group(1)
        bad_val = re.search(r'tag_value\s*=\s*"([^"]+)"', rewritten[blk_start:blk_end + 1]).group(1)
        rewritten = rewritten[:start] + rewritten[end:]
        print(
            f"  [AUTOFIX] Removed tag_assignment with invalid value "
            f"'{bad_val}' for tag_key '{bad_key}'"
        )

    text = text[:sec_start] + rewritten + text[sec_end:]
    tfvars_path.write_text(text)

    return len(remove_indices)


# Databricks platform limits for ABAC policies.
# Ref: https://docs.databricks.com/en/data-governance/unity-catalog/abac/policies#policy-quotas
#   Metastore: 10,000 | Catalog: 100 | Schema: 100 | Table: 50
#   Principals per policy: 20 (soft limits — contact Databricks to increase)
# The autofix enforces the per-catalog limit.  The per-table limit (50) is
# not enforced here because tag-based policies match columns indirectly;
# a single policy can cover multiple tables.  In practice, schemas rarely
# exceed 50 policies per table.
_FGAC_PER_CATALOG_LIMIT = 100  # max policies per catalog


def autofix_fgac_policy_count(tfvars_path: Path) -> int:
    """Fail when generated policies exceed Unity Catalog's catalog quota.

    Option-B treatment derivation normally emits only one mask per treatment
    and catalog, so quota pressure is substantially reduced.  It is never safe
    to resolve an excess by deleting policies (or the sensitive assignments
    they protect), however.
    """
    try:
        import hcl2  # type: ignore
    except ImportError:
        return 0

    text = tfvars_path.read_text()

    try:
        cfg = hcl2.loads(text)
    except Exception:
        return 0

    policies = cfg.get("fgac_policies", [])
    if not policies:
        return 0

    per_catalog_policies: dict[str, list[tuple[int, dict]]] = {}
    for idx, p in enumerate(policies):
        cat = p.get("catalog", "") or p.get("function_catalog", "")
        if cat:
            per_catalog_policies.setdefault(cat, []).append((idx, p))

    excess: list[str] = []
    for cat, indexed_policies in per_catalog_policies.items():
        if len(indexed_policies) > _FGAC_PER_CATALOG_LIMIT:
            names = [p.get("name", "<unnamed>") for _, p in indexed_policies]
            excess.append(
                f"catalog '{cat}' has {len(indexed_policies)} policies "
                f"(limit {_FGAC_PER_CATALOG_LIMIT}): {', '.join(names)}"
            )
    if excess:
        raise ValueError("FGAC policy quota exceeded; no policies were dropped. " + " | ".join(excess))
    return 0


def _find_bracket_section(text: str, section_name: str) -> tuple[int, int] | None:
    """Find the content range of ``section_name = [ ... ]`` using bracket-depth
    counting so that ``]`` inside quoted strings or nested structures is ignored.

    Returns (start, end) offsets of the *content* between the opening ``[`` and
    the matching closing ``]``, or None if the section is not found.
    """
    pattern = re.compile(rf"\b{re.escape(section_name)}\s*=\s*\[")
    m = pattern.search(text)
    if not m:
        return None
    depth = 1
    in_string = False
    escape_next = False
    content_start = m.end()
    for i in range(content_start, len(text)):
        ch = text[i]
        if escape_next:
            escape_next = False
            continue
        if ch == "\\":
            escape_next = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return (content_start, i)
    return None


def _find_brace_blocks(text: str) -> list[tuple[int, int]]:
    """Return (start, end) ranges for each top-level ``{ ... }`` block in *text*.

    Uses bracket-depth counting and respects quoted strings so that ``}``
    inside string values does not terminate the block prematurely.
    """
    blocks: list[tuple[int, int]] = []
    i = 0
    while i < len(text):
        if text[i] == "{":
            depth = 1
            in_string = False
            escape_next = False
            start = i
            j = i + 1
            while j < len(text) and depth > 0:
                ch = text[j]
                if escape_next:
                    escape_next = False
                    j += 1
                    continue
                if ch == "\\":
                    escape_next = True
                    j += 1
                    continue
                if ch == '"':
                    in_string = not in_string
                elif not in_string:
                    if ch == "{":
                        depth += 1
                    elif ch == "}":
                        depth -= 1
                j += 1
            if depth == 0:
                blocks.append((start, j - 1))  # j-1 points to closing }
                i = j
                continue
        i += 1
    return blocks


def _replace_bracket_section(text: str, section_name: str, new_items: list[str]) -> str:
    section = _find_bracket_section(text, section_name)
    if section is None:
        return text
    sec_start, sec_end = section
    if new_items:
        new_section = "\n" + ",\n".join(new_items) + "\n"
    else:
        new_section = "\n"
    return text[:sec_start] + new_section + text[sec_end:]


def _render_tag_policy_block(policy: dict) -> str:
    lines = ["  {"]
    lines.append(f'    key = "{policy.get("key", "")}"')
    description = policy.get("description", "")
    if description:
        lines.append(f'    description = "{description}"')
    values = policy.get("values", []) or []
    rendered_values = "[" + ", ".join(f'"{v}"' for v in values) + "]"
    lines.append(f"    values = {rendered_values}")
    lines.append("  }")
    return "\n".join(lines)


def _render_tag_assignment_block(assignment: dict) -> str:
    lines = ["  {"]
    ordered_keys = ["entity_type", "entity_name", "tag_key", "tag_value"]
    for key in ordered_keys:
        value = assignment.get(key, "")
        lines.append(f'    {key} = "{value}"')
    lines.append("  }")
    return "\n".join(lines)


def _render_treatment_override_block(override: dict) -> str:
    return "\n".join([
        "  {",
        f'    entity_name = "{override.get("entity_name", "")}"',
        f'    treatment   = "{override.get("treatment", "")}"',
        "  }",
    ])


def _render_fgac_policy_block(policy: dict) -> str:
    lines = ["  {"]
    ordered_keys = [
        "name",
        "policy_type",
        "catalog",
        "to_principals",
        "except_principals",
        "comment",
        "match_condition",
        "when_condition",
        "match_alias",
        "function_name",
        "function_catalog",
        "function_schema",
    ]
    for key in ordered_keys:
        value = policy.get(key)
        if value in (None, "", []):
            continue
        if isinstance(value, list):
            rendered = "[" + ", ".join(f'"{v}"' for v in value) + "]"
        else:
            rendered = f'"{value}"'
        lines.append(f"    {key:<16} = {rendered}")
    lines.append("  }")
    return "\n".join(lines)


def derive_enforcement_treatments(tfvars_path: Path) -> int:
    """Materialize one ``gr_treatment`` value and mask per sensitive column."""
    try:
        import hcl2
        text = tfvars_path.read_text()
        cfg = hcl2.loads(text)
    except Exception as exc:
        raise RuntimeError(
            f"Cannot derive enforcement treatments from {tfvars_path}: {exc}"
        ) from exc

    provenance = re.findall(
        r"^\s*#\s*gr\.classification_unmapped:[^\n]+$", text, re.MULTILINE
    )
    # Only an untransformed draft can contribute a source-less, model-authored
    # explicit treatment. Generated masks are durable provenance that this file
    # has already passed through derivation; without this guard, its own
    # gr_treatment output could become a sticky override on the next pass.
    already_derived = any(
        str(policy.get("name") or "").startswith("gr_mask_")
        and str(policy.get("match_alias") or "").startswith("gr_treatment_")
        and str(policy.get("comment") or "").startswith(
            DERIVED_TREATMENT_MASK_COMMENT_PREFIX
        )
        for policy in (cfg.get("fgac_policies") or [])
    )
    derived, changes = derive_treatment_model(
        cfg,
        load_treatment_config(),
        capture_source_less_explicit=not already_derived,
    )
    if not changes:
        return 0
    text = _replace_bracket_section(
        text, "tag_policies",
        [_render_tag_policy_block(item) for item in derived.get("tag_policies", [])],
    )
    text = _replace_bracket_section(
        text, "tag_assignments",
        [_render_tag_assignment_block(item) for item in derived.get("tag_assignments", [])],
    )
    overrides = derived.get("treatment_overrides") or []
    if overrides:
        rendered = [
            _render_treatment_override_block(item) for item in overrides
        ]
        if _find_bracket_section(text, "treatment_overrides") is None:
            block = "treatment_overrides = [\n" + ",\n".join(rendered) + "\n]\n\n"
            text = block + text
        else:
            text = _replace_bracket_section(text, "treatment_overrides", rendered)
    text = _replace_bracket_section(
        text, "fgac_policies",
        [_render_fgac_policy_block(item) for item in derived.get("fgac_policies", [])],
    )
    if provenance:
        text = "\n".join(provenance) + "\n" + text
    tfvars_path.write_text(text)
    return changes


def strip_native_source_assignments(tfvars_path: Path) -> int:
    """Keep only gr_treatment for native-classification enforcement.

    Native class.* tags already carry the authoritative sensitivity signal.
    Materializing intermediate pii_level/pci_level values is redundant and can
    conflict with centrally owned vocabularies; Option B needs only the single
    derived gr_treatment assignment.
    """
    import hcl2

    text = tfvars_path.read_text()
    cfg = hcl2.loads(text)
    treatment_cfg = load_treatment_config()
    mapped_sources = {
        source for treatment in treatment_cfg.treatments for source in treatment.sources
    }
    assignments = cfg.get("tag_assignments", []) or []
    sensitivity_keys = {key for key, _ in mapped_sources}
    retained = [
        assignment for assignment in assignments
        if not (
            assignment.get("entity_type") == "columns"
            and (assignment.get("tag_key"), assignment.get("tag_value")) in mapped_sources
        )
    ]
    removed = len(assignments) - len(retained)
    if removed:
        text = _replace_bracket_section(
            text,
            "tag_assignments",
            [_render_tag_assignment_block(item) for item in retained],
        )
        # Native class.* is the source vocabulary; do not emit centrally owned
        # sensitivity tag-policy definitions that this layer must not mutate.
        policies = [
            policy for policy in (cfg.get("tag_policies", []) or [])
            if policy.get("key") not in sensitivity_keys
        ]
        text = _replace_bracket_section(
            text,
            "tag_policies",
            [_render_tag_policy_block(item) for item in policies],
        )
        tfvars_path.write_text(text)
    return removed


def derive_and_finalize_treatments(
    tfvars_path: Path,
    *,
    native_authoritative: bool,
) -> tuple[int, int]:
    """Run the identical treatment finalization used by initial and retry paths."""
    derived = derive_enforcement_treatments(tfvars_path)
    stripped = strip_native_source_assignments(tfvars_path) if native_authoritative else 0
    return derived, stripped


def ensure_derived_treatment_functions(tfvars_path: Path, sql_path: Path | None) -> int:
    """Emit deterministic UDFs required by derived treatment policies.

    A derived ``gr_treatment`` policy is configuration-owned, so a missing or
    bodyless LLM function must not make the generic reference repair replace its
    configured function with ``mask_redact``.  Treatments with a deterministic
    definition in treatment_config.json are restored before reference repair.
    """
    if not sql_path or not sql_path.exists():
        return 0
    import hcl2

    cfg = hcl2.loads(tfvars_path.read_text())
    required_values = {
        value
        for policy in (cfg.get("fgac_policies", []) or [])
        for value in re.findall(
            r"hasTagValue\(\s*'gr_treatment'\s*,\s*'([^']+)'\s*\)",
            policy.get("match_condition", "") or "",
        )
    }
    existing = _parse_sql_function_names(sql_path)
    definitions = {
        treatment.value: treatment
        for treatment in load_treatment_config().treatments
        if treatment.udf_signature and treatment.udf_body
    }
    missing = [
        definitions[value] for value in sorted(required_values)
        if value in definitions and definitions[value].masking_function not in existing
    ]
    if not missing:
        return 0

    sql = sql_path.read_text().rstrip()
    blocks = ["\n\n-- === Treatment functions restored from treatment_config.json ==="]
    for treatment in missing:
        blocks.append(
            f"\nCREATE OR REPLACE FUNCTION {treatment.udf_signature}\n"
            f"RETURN {treatment.udf_body};"
        )
        print(
            f"  [AUTOFIX] Restored treatment function "
            f"'{treatment.masking_function}' from treatment_config.json"
        )
    sql_path.write_text(sql + "\n" + "\n".join(blocks) + "\n")
    return len(missing)


def _parse_sql_function_names(sql_path: Path | None) -> set[str]:
    if not sql_path or not sql_path.exists():
        return set()
    text = sql_path.read_text()
    pattern = re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+"
        r"(?:[\w]+\.[\w]+\.)?"
        r"([\w]+)\s*\(",
        re.IGNORECASE,
    )
    return {m.group(1) for m in pattern.finditer(text)}


def _parse_sql_functions_by_schema(sql_path: Path | None) -> dict[tuple[str, str], set[str]]:
    """Parse SQL file and return {(catalog, schema): {function_names}} mapping."""
    if not sql_path or not sql_path.exists():
        return {}
    text = sql_path.read_text()
    result: dict[tuple[str, str], set[str]] = {}
    catalog, schema = None, None
    for raw_stmt in re.split(r";\s*(?:--[^\n]*)?\n", text):
        lines = [
            line for line in raw_stmt.split("\n")
            if line.strip() and not line.strip().startswith("--")
        ]
        stmt = "\n".join(lines).strip()
        if not stmt:
            continue
        m = re.match(r"USE\s+CATALOG\s+(\S+)", stmt, re.IGNORECASE)
        if m:
            catalog = m.group(1)
            continue
        m = re.match(r"USE\s+SCHEMA\s+(\S+)", stmt, re.IGNORECASE)
        if m:
            schema = m.group(1)
            continue
        fn_m = re.search(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+"
            r"(?:[\w]+\.[\w]+\.)?([\w]+)\s*\(",
            stmt, re.IGNORECASE,
        )
        if fn_m and catalog and schema:
            result.setdefault((catalog, schema), set()).add(fn_m.group(1))
    return result


def _infer_column_categories(entity_name: str) -> set[str]:
    col = entity_name.split(".")[-1].lower()
    categories: set[str] = set()
    if "email" in col:
        categories.add("email")
    if "phone" in col or "mobile" in col:
        categories.add("phone")
    return categories


def autofix_ambiguous_tag_values(tfvars_path: Path) -> int:
    """Normalize ambiguous mixed-type tag values to type-safe concrete values."""
    text = tfvars_path.read_text()
    section = _find_bracket_section(text, "tag_assignments")
    if section is None:
        return 0

    sec_start, sec_end = section
    section_text = text[sec_start:sec_end]
    blocks = _find_brace_blocks(section_text)
    if not blocks:
        return 0

    rewritten = section_text
    updates = 0
    normalized_values: set[tuple[str, str]] = set()  # (tag_key, normalized_value)
    for blk_start, blk_end in reversed(blocks):
        block_text = rewritten[blk_start:blk_end + 1]
        entity_type_match = re.search(r'entity_type\s*=\s*"([^"]+)"', block_text)
        entity_name_match = re.search(r'entity_name\s*=\s*"([^"]+)"', block_text)
        tag_key_match = re.search(r'tag_key\s*=\s*"([^"]+)"', block_text)
        tag_value_match = re.search(r'tag_value\s*=\s*"([^"]+)"', block_text)
        if not (entity_type_match and entity_name_match and tag_key_match and tag_value_match):
            continue

        entity_type = entity_type_match.group(1)
        entity_name = entity_name_match.group(1)
        tag_key = tag_key_match.group(1)
        tag_value = tag_value_match.group(1)
        if entity_type != "columns" or tag_value != "masked_contact":
            continue

        categories = _infer_column_categories(entity_name)
        if categories == {"email"}:
            normalized_value = "masked_email"
        elif categories == {"phone"}:
            normalized_value = "masked_phone"
        else:
            continue

        updated_block = re.sub(
            r'(tag_value\s*=\s*")masked_contact(")',
            rf"\1{normalized_value}\2",
            block_text,
            count=1,
        )
        if updated_block == block_text:
            continue

        rewritten = rewritten[:blk_start] + updated_block + rewritten[blk_end + 1:]
        normalized_values.add((tag_key, normalized_value))
        updates += 1
        print(
            f"  [AUTOFIX] Normalized {tag_key} on '{entity_name}' "
            f"from 'masked_contact' to '{normalized_value}'"
        )

    if not updates:
        return 0

    text = text[:sec_start] + rewritten + text[sec_end:]

    # Also add normalized values to tag_policies so that
    # autofix_invalid_tag_values (which runs next) doesn't remove
    # the assignments we just normalized.
    for tag_key, norm_val in normalized_values:
        tp_pattern = re.compile(
            r'(\{\s*key\s*=\s*"' + re.escape(tag_key) + r'"[^}]*?values\s*=\s*\[)([^\]]*?)(\])',
            re.DOTALL,
        )
        tp_match = tp_pattern.search(text)
        if tp_match:
            existing_vals = re.findall(r'"([^"]+)"', tp_match.group(2))
            if norm_val not in existing_vals:
                new_vals = tp_match.group(2).rstrip()
                if new_vals and not new_vals.rstrip().endswith(","):
                    new_vals += ","
                new_vals += f' "{norm_val}"'
                text = text[:tp_match.start(2)] + new_vals + text[tp_match.end(2):]
                print(f"  [AUTOFIX] Added '{norm_val}' to tag_policy '{tag_key}'")

    tfvars_path.write_text(text)
    return updates


def autofix_canonical_tag_vocabulary(tfvars_path: Path) -> int:
    """Normalize tag keys/values and collapse duplicate tag_policies entries."""
    text = tfvars_path.read_text()
    try:
        import hcl2
        cfg = hcl2.loads(text)
    except Exception:
        return 0

    updates = 0

    policies = cfg.get("tag_policies", []) or []
    merged_policies: list[dict] = []
    merged_by_key: dict[str, dict] = {}
    for policy in policies:
        if isinstance(policy, list):
            policy = policy[0] if policy else {}
        key = policy.get("key", "")
        if not key:
            continue
        canonical_key = _canonical_tag_key(key)
        if canonical_key != key:
            updates += 1
            print(f"  [AUTOFIX] Normalized tag_policy key '{key}' to '{canonical_key}'")
        normalized_values: list[str] = []
        for value in policy.get("values", []) or []:
            canonical_value = _canonical_tag_value(canonical_key, value)
            if canonical_value != value:
                updates += 1
                print(
                    f"  [AUTOFIX] Normalized tag_policy value '{value}' "
                    f"to '{canonical_value}' for key '{canonical_key}'"
                )
            if REGISTRY.is_allowed_value(canonical_key, canonical_value) is False:
                updates += 1
                print(
                    f"  [AUTOFIX] Removed tag_policy value '{canonical_value}' "
                    f"for key '{canonical_key}' because it is not in the registry"
                )
                continue
            if canonical_value not in normalized_values:
                normalized_values.append(canonical_value)

        entry = merged_by_key.get(canonical_key)
        if entry is None:
            entry = {
                "key": canonical_key,
                "description": policy.get("description", ""),
                "values": [],
            }
            merged_by_key[canonical_key] = entry
            merged_policies.append(entry)
        elif key != canonical_key:
            updates += 1
        if not entry.get("description") and policy.get("description"):
            entry["description"] = policy.get("description", "")
        for value in normalized_values:
            if value not in entry["values"]:
                entry["values"].append(value)

    assignments = cfg.get("tag_assignments", []) or []
    normalized_assignments: list[dict] = []
    seen_assignments: set[tuple[str, str, str, str]] = set()
    for assignment in assignments:
        if isinstance(assignment, list):
            assignment = assignment[0] if assignment else {}
        normalized = dict(assignment)
        key = normalized.get("tag_key", "")
        value = normalized.get("tag_value", "")
        canonical_key = _canonical_tag_key(key)
        canonical_value = _canonical_tag_value(canonical_key, value)
        if canonical_key != key:
            updates += 1
            print(f"  [AUTOFIX] Normalized tag_assignment key '{key}' to '{canonical_key}'")
        if canonical_value != value:
            updates += 1
            print(
                f"  [AUTOFIX] Normalized tag_assignment value '{value}' "
                f"to '{canonical_value}' for key '{canonical_key}'"
            )
        normalized["tag_key"] = canonical_key
        normalized["tag_value"] = canonical_value
        signature = (
            normalized.get("entity_type", ""),
            normalized.get("entity_name", ""),
            canonical_key,
            canonical_value,
        )
        if signature in seen_assignments:
            updates += 1
            print(
                "  [AUTOFIX] Removed duplicate tag_assignment "
                f"'{signature[1]} {canonical_key}={canonical_value}'"
            )
            continue
        seen_assignments.add(signature)
        normalized_assignments.append(normalized)

    policies_cfg = cfg.get("fgac_policies", []) or []
    normalized_fgac_policies: list[dict] = []
    for policy in policies_cfg:
        if isinstance(policy, list):
            policy = policy[0] if policy else {}
        normalized = dict(policy)
        for field in ("match_condition", "when_condition"):
            condition = normalized.get(field, "")
            if not condition:
                continue
            normalized_condition, n_updates = _normalize_has_tag_refs(str(condition))
            if n_updates:
                updates += n_updates
                print(
                    f"  [AUTOFIX] Normalized {n_updates} tag ref(s) in "
                    f"fgac_policy '{normalized.get('name', '?')}' field '{field}'"
                )
                normalized[field] = normalized_condition
        normalized_fgac_policies.append(normalized)

    if not updates:
        return 0

    text = _replace_bracket_section(
        text,
        "tag_policies",
        [_render_tag_policy_block(policy) for policy in merged_policies],
    )
    text = _replace_bracket_section(
        text,
        "tag_assignments",
        [_render_tag_assignment_block(assignment) for assignment in normalized_assignments],
    )
    text = _replace_bracket_section(
        text,
        "fgac_policies",
        [_render_fgac_policy_block(policy) for policy in normalized_fgac_policies],
    )
    tfvars_path.write_text(text)
    return updates


def autofix_missing_fgac_policies(tfvars_path: Path, sql_path: Path | None = None) -> int:
    """Add fgac_policies for uncovered non-public tag assignments when possible."""
    try:
        import hcl2  # type: ignore
    except ImportError:
        return 0

    text = tfvars_path.read_text()
    try:
        cfg = hcl2.loads(text)
    except Exception:
        return 0

    policies = cfg.get("fgac_policies", []) or []
    assignments = cfg.get("tag_assignments", []) or []
    groups = list((cfg.get("groups") or {}).keys())
    if not assignments:
        return 0

    available_functions = _parse_sql_function_names(sql_path)

    # Parse arg counts so _infer_function can filter by policy-type compat.
    fn_arg_counts: dict[str, int] = {}
    if sql_path and sql_path.exists():
        try:
            from validate_abac import parse_sql_function_arg_counts
            fn_arg_counts = parse_sql_function_arg_counts(sql_path)
        except Exception:
            pass

    def _extract_tag_refs(condition: str) -> tuple[list[tuple[str, str]], list[str]]:
        value_refs = re.findall(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", condition or "")
        key_refs = re.findall(r"hasTag\(\s*'([^']+)'\s*\)", condition or "")
        return value_refs, key_refs

    def _condition_matches_tags(condition: str, tags: dict[str, set[str]]) -> bool:
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

    def _assignment_priority(assignment: dict) -> int:
        etype = assignment.get("entity_type", "")
        ename = assignment.get("entity_name", "")
        tkey = assignment.get("tag_key", "")
        tval = assignment.get("tag_value", "")
        blob = f"{ename} {tkey} {tval}".lower()
        if etype == "tables":
            if any(tok in blob for tok in ("aml", "hipaa", "pci", "compliance", "audit")):
                return 85
            return 50
        if any(tok in blob for tok in ("ssn", "mrn", "cvv", "pan", "government", "token", "secret")):
            return 100
        if any(tok in blob for tok in ("card_number", "credit_card", "iban", "account_number")):
            return 95
        if any(tok in blob for tok in ("address", "birth", "dob", "date_of_birth")):
            return 80
        if any(tok in blob for tok in ("email", "phone", "name")):
            return 70
        if any(tok in blob for tok in ("amount", "balance", "limit", "rounded")):
            return 20
        return 40

    def _normalize_name_component(value: str) -> str:
        return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value.lower())).strip("_")

    def _format_string_list(values: list[str]) -> str:
        return "[" + ", ".join(f'"{v}"' for v in values) + "]"

    def _policy_block(policy: dict) -> str:
        lines = ["  {"]
        ordered_keys = [
            "name",
            "policy_type",
            "catalog",
            "to_principals",
            "except_principals",
            "comment",
            "match_condition",
            "when_condition",
            "match_alias",
            "function_name",
            "function_catalog",
            "function_schema",
        ]
        for key in ordered_keys:
            if key not in policy or policy[key] in (None, "", []):
                continue
            value = policy[key]
            rendered = _format_string_list(value) if isinstance(value, list) else f'"{value}"'
            lines.append(f"    {key:<16} = {rendered}")
        lines.append("  }")
        return "\n".join(lines)

    def _infer_function(assignment: dict) -> str | None:
        ename = assignment.get("entity_name", "").lower()
        tkey = assignment.get("tag_key", "").lower()
        tval = assignment.get("tag_value", "").lower()
        blob = f"{ename} {tkey} {tval}"
        is_table = assignment.get("entity_type") == "tables"
        # Column masks need 1-arg functions; row filters need 0-arg functions.
        expected_args = 0 if is_table else 1

        def _arg_count_ok(fn_name: str) -> bool:
            """Return True if the function's arg count matches the policy type."""
            if fn_name not in fn_arg_counts:
                return True  # unknown — allow (other autofixes will catch)
            return fn_arg_counts[fn_name] == expected_args

        preferred: list[str] = []
        if is_table:
            if any(tok in blob for tok in ("pci", "card")):
                preferred.extend([
                    "filter_pci_authorized", "filter_pci_compliance_only",
                    "filter_pci_only", "filter_compliance_only",
                ])
            if any(tok in blob for tok in ("aml", "hipaa", "compliance", "audit")):
                preferred.extend([
                    "filter_compliance_only", "filter_hipaa_compliance",
                    "filter_aml_compliance", "filter_aml_only",
                ])
            if any(tok in blob for tok in ("phi", "hipaa", "clinical", "patient")):
                preferred.extend([
                    "filter_hipaa_compliance", "filter_phi_only",
                    "filter_clinical_only", "filter_compliance_only",
                ])
            if any(tok in blob for tok in ("pii", "personal")):
                preferred.extend(["filter_pii_authorized", "filter_compliance_only"])
        else:
            if any(tok in blob for tok in ("ssn", "social_security")):
                preferred.append("mask_ssn")
            if "email" in blob:
                preferred.append("mask_email")
            if "phone" in blob or "mobile" in blob:
                preferred.append("mask_phone")
            if "name" in blob:
                preferred.extend(["mask_full_name", "mask_pii_partial"])
            if "address" in blob:
                preferred.extend(["mask_redact", "mask_pii_partial"])
            if any(tok in blob for tok in ("birth", "dob", "date_of_birth")):
                preferred.append("mask_date_to_year")
            if "cvv" in blob:
                preferred.append("mask_redact")
            if any(tok in blob for tok in ("card_number", "credit_card", "pci")):
                if "last4" in blob:
                    preferred.extend(["mask_credit_card_last4", "mask_credit_card_full"])
                else:
                    preferred.extend(["mask_credit_card_full", "mask_credit_card_last4"])
            if any(tok in blob for tok in (
                "amount", "balance", "limit", "rounded", "salary", "wage", "compensation"
            )):
                preferred.append("mask_amount_rounded")
            if "diagnosis" in blob:
                preferred.append("mask_diagnosis_code")
            if any(tok in blob for tok in ("note", "notes", "desc", "description")):
                preferred.append("mask_redact")
        # Only add generic masking fallbacks for column-level assignments.
        # Row filter functions must take 0 arguments; masking functions take 1.
        # IMPORTANT: mask_redact returns STRING — never use it for columns tagged
        # with non-STRING values (amounts/dates).  These need type-specific masks.
        is_numeric_or_date = any(tok in blob for tok in (
            "amount", "balance", "credit_limit", "rounded", "price", "cost",
            "salary", "wage", "compensation",
            "dob", "birth", "date_of_birth", "opened_date", "expiry",
        ))
        if not is_table:
            if not is_numeric_or_date:
                preferred.extend(["mask_redact", "mask_nullify", "mask_pii_partial"])
        for fn in preferred:
            if (not available_functions or fn in available_functions) and _arg_count_ok(fn):
                return fn
        # Last-resort: pick any available function of the right type from the SQL
        # file. Use semantic matching (token overlap with tag_value) to avoid
        # picking arbitrary alphabetical first (which caused mask_abn to be
        # chosen for every uncategorized string column when ANZ overlay is loaded).
        # Skip last-resort for numeric/date columns — a wrong-type function is
        # worse than no policy (the blocking coverage gate will surface the gap).
        if available_functions and not is_numeric_or_date:
            # Extract semantic tokens from the tag value (strip common prefixes)
            tv_norm = tval.replace("masked_", "").replace("redacted_", "")
            tv_tokens = set(t for t in tv_norm.split("_") if t)

            def _score_fn(fn: str, fn_prefix: str) -> int:
                fn_tokens = set(fn.replace(fn_prefix, "", 1).split("_"))
                return len(fn_tokens & tv_tokens)

            if is_table:
                filter_fns = [f for f in available_functions if f.startswith("filter_") and _arg_count_ok(f)]
                if filter_fns:
                    ranked = sorted(filter_fns, key=lambda f: (-_score_fn(f, "filter_"), f))
                    if _score_fn(ranked[0], "filter_") > 0:
                        return ranked[0]
                    # No semantic match — don't pick random filter (would cause
                    # category mismatch at query time). Return None so the tag
                    # stays uncovered and is rejected by the coverage gate.
            else:
                mask_fns = [f for f in available_functions if f.startswith("mask_") and _arg_count_ok(f)]
                if mask_fns:
                    ranked = sorted(mask_fns, key=lambda f: (-_score_fn(f, "mask_"), f))
                    if _score_fn(ranked[0], "mask_") > 0:
                        return ranked[0]
                    # No semantic match — don't pick random mask (would cause
                    # category mismatch at query time).
        return None

    def _policy_matches_assignment(
        policy: dict,
        assignment: dict,
        entity_tags: dict[tuple[str, str], dict[str, set[str]]],
    ) -> bool:
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

    def _find_template_policy(catalog: str, policy_type: str, tag_key: str) -> dict | None:
        same_catalog = [
            p for p in policies
            if (p.get("catalog", "") or p.get("function_catalog", "")) == catalog
            and p.get("policy_type") == policy_type
        ]
        for p in same_catalog:
            value_refs, key_refs = _extract_tag_refs(
                (p.get("match_condition") or "") + " " + (p.get("when_condition") or "")
            )
            if any(key == tag_key for key, _ in value_refs) or tag_key in key_refs:
                return p
        return same_catalog[0] if same_catalog else None

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

    existing_names = {p.get("name", "") for p in policies if p.get("name")}
    uncovered = [
        ta for ta in assignments
        if _value_requires_coverage(ta.get("tag_value", ""))
        and not any(_policy_matches_assignment(p, ta, entity_tags) for p in policies)
    ]
    uncovered.sort(key=_assignment_priority, reverse=True)

    new_policies: list[dict] = []
    for ta in uncovered:
        entity_type = ta.get("entity_type", "")
        entity_name = ta.get("entity_name", "")
        tag_key = ta.get("tag_key", "")
        tag_value = ta.get("tag_value", "")
        if not (entity_type and entity_name and tag_key and tag_value):
            continue
        catalog, schema = entity_name.split(".")[:2]
        policy_type = "POLICY_TYPE_ROW_FILTER" if entity_type == "tables" else "POLICY_TYPE_COLUMN_MASK"
        fn = _infer_function(ta)
        if not fn:
            continue
        template = _find_template_policy(catalog, policy_type, tag_key)
        if template:
            to_principals = list(template.get("to_principals", []) or [])
            except_principals = list(template.get("except_principals", []) or [])
        else:
            admin_like = [g for g in groups if re.search(r"admin|compliance|authorized", g, re.IGNORECASE)]
            non_admin = [g for g in groups if g not in admin_like]
            # Fall back to "account users" when no groups are available (e.g.
            # per-space tfvars files where groups aren't declared).
            to_principals = non_admin or groups[:1] or ["account users"]
            except_principals = []

        action = "filter" if policy_type == "POLICY_TYPE_ROW_FILTER" else "mask"
        base_name = _normalize_name_component(f"auto_{action}_{catalog}_{tag_key}_{tag_value}")
        name = base_name
        suffix = 2
        while name in existing_names:
            name = f"{base_name}_{suffix}"
            suffix += 1
        existing_names.add(name)

        policy = {
            "name": name,
            "policy_type": policy_type,
            "catalog": catalog,
            "to_principals": to_principals,
            "comment": f"Auto-repaired coverage for {entity_name} ({tag_key} = '{tag_value}')",
            "function_name": fn,
            "function_catalog": catalog,
            "function_schema": schema,
        }
        if except_principals:
            policy["except_principals"] = except_principals
        if policy_type == "POLICY_TYPE_COLUMN_MASK":
            policy["match_condition"] = f"hasTagValue('{tag_key}', '{tag_value}')"
            policy["match_alias"] = _normalize_name_component(f"{tag_key}_{tag_value}")[:60]
        else:
            policy["when_condition"] = f"hasTagValue('{tag_key}', '{tag_value}')"

        new_policies.append(policy)
        policies.append(policy)

    if not new_policies:
        return 0

    section = _find_bracket_section(text, "fgac_policies")
    if section is None:
        return 0
    sec_start, sec_end = section
    section_text = text[sec_start:sec_end]
    trimmed = section_text.rstrip()
    blocks_text = ",\n".join(_policy_block(p) for p in new_policies)
    if trimmed.strip():
        separator = "\n" if trimmed.endswith(",") else ",\n"
        new_section_text = trimmed + separator + blocks_text + "\n"
    else:
        new_section_text = "\n" + blocks_text + "\n"
    text = text[:sec_start] + new_section_text + text[sec_end:]
    tfvars_path.write_text(text)

    for p in new_policies:
        print(
            f"  [AUTOFIX] Added fgac_policy '{p['name']}' for "
            f"{p.get('match_condition') or p.get('when_condition')}"
        )
    return len(new_policies)


def autofix_genie_config_fields(tfvars_path: Path) -> int:
    """Ensure sql_filters/sql_expressions/sql_measures/join_specs objects have
    all required fields (comment, instruction, display_name, etc.).

    The LLM sometimes omits optional-looking fields that Terraform actually requires.
    Uses bracket-depth counting (not regex) to correctly handle ``]`` or ``}``
    inside quoted SQL strings.
    Returns the number of fields added.
    """
    text = tfvars_path.read_text()
    added = 0

    # Required fields for each section type and the defaults to inject
    section_required: dict[str, list[str]] = {
        "sql_filters": ["sql", "display_name", "comment", "instruction"],
        "sql_expressions": ["alias", "sql", "display_name", "comment", "instruction"],
        "sql_measures": ["alias", "sql", "display_name", "comment", "instruction"],
        "join_specs": [
            "left_table", "right_table", "sql",
            "comment", "instruction", "left_alias", "right_alias",
        ],
    }

    for section, required_fields in section_required.items():
        # Find ALL occurrences of this section in the file (there may be one
        # per Genie agent in an assembled multi-space file).  Process in
        # reverse order so that earlier offsets are not shifted by edits.
        all_ranges: list[tuple[int, int]] = []
        search_start = 0
        while True:
            rng = _find_bracket_section(text[search_start:], section)
            if rng is None:
                break
            all_ranges.append((search_start + rng[0], search_start + rng[1]))
            search_start += rng[1] + 1  # skip past the closing ]

        for sec_start, sec_end in reversed(all_ranges):
            section_text = text[sec_start:sec_end]

            # Find each { ... } block inside the section content
            blocks = _find_brace_blocks(section_text)
            if not blocks:
                continue

            # Process blocks in reverse order so edits don't shift earlier offsets
            new_section = section_text
            for blk_start, blk_end in reversed(blocks):
                # block_content is the text BETWEEN { and }
                block_content = new_section[blk_start + 1 : blk_end]
                existing_keys = set(
                    m.group(1)
                    for m in re.finditer(r"^\s*(\w+)\s*=", block_content, re.MULTILINE)
                )
                missing = [f for f in required_fields if f not in existing_keys]
                if not missing:
                    continue
                # Find the indentation from an existing field
                indent_match = re.search(r"^(\s+)\w+\s*=", block_content, re.MULTILINE)
                indent = indent_match.group(1) if indent_match else "        "
                extra_lines = "\n".join(f'{indent}{f} = ""' for f in missing)
                # Preserve indentation: strip trailing whitespace from existing
                # content, append missing fields, then re-add proper indent before }
                stripped = block_content.rstrip()
                # Detect indent of the closing brace (one level less than field indent)
                brace_indent = indent[:-2] if len(indent) >= 2 else "      "
                new_block_content = stripped + "\n" + extra_lines + "\n" + brace_indent
                new_section = (
                    new_section[: blk_start + 1]
                    + new_block_content
                    + new_section[blk_end:]
                )
                added += len(missing)

            if new_section != section_text:
                text = text[:sec_start] + new_section + text[sec_end:]

    if added:
        tfvars_path.write_text(text)
    return added


def autofix_acl_groups(
    tfvars_path: Path,
    env_tfvars_path: Path | None = None,
    *,
    reject_draft_acls: bool = False,
    defer_unmapped: bool = False,
) -> int:
    """Resolve ACLs from user-owned env input or fresh policy derivation.

    ACL fields emitted by the model in the generated draft are deliberately
    ignored. Durable explicit intent belongs only in env.auto.tfvars.

    A space configured only by genie_space_id takes its catalogs from the
    agent attribution in data_access/discovered_uc_tables.auto.tfvars.
    defer_unmapped (genie mode, whose draft carries no policies) leaves a
    space with no policy groups out of the sidecar instead of failing; the
    next full generate derives it.
    """
    import hcl2

    text = tfvars_path.read_text()
    try:
        cfg = hcl2.loads(text)
    except Exception as exc:
        raise ValueError(
            f"Cannot derive Genie ACLs: generated HCL is invalid: {exc}"
        ) from exc

    genie_cfgs = cfg.get("genie_space_configs") or {}
    if isinstance(genie_cfgs, list):
        genie_cfgs = genie_cfgs[0] if genie_cfgs else {}
    groups = cfg.get("groups") or {}
    if isinstance(groups, list):
        groups = groups[0] if groups else {}
    fgac_policies = cfg.get("fgac_policies") or []
    id_to_name = cfg.get("genie_space_id_to_name") or {}
    if isinstance(fgac_policies, list) and len(fgac_policies) == 1 and isinstance(fgac_policies[0], list):
        fgac_policies = fgac_policies[0]

    # Build space_name → set of catalogs from env.auto.tfvars or from tag_assignments
    space_catalogs: dict[str, set[str]] = {}
    user_acls: dict[str, list[str]] = {}
    tier_order: list[str] = []
    active_space_names: set[str] | None = None
    has_configured_spaces = False

    # Try to get uc_tables per space from env.auto.tfvars
    if env_tfvars_path and env_tfvars_path.exists():
        try:
            env_cfg = hcl2.loads(env_tfvars_path.read_text())
            configured_spaces = env_cfg.get("genie_spaces") or []
            has_configured_spaces = bool(configured_spaces)
            tiers = env_cfg.get("access_tier_groups") or []
            if isinstance(tiers, list):
                tier_order = [tier for tier in tiers if isinstance(tier, str)]
            resolved_names: list[str] = []
            for space in configured_spaces:
                if isinstance(space, list):
                    space = space[0] if space else {}
                name = space.get("name", "") or id_to_name.get(
                    space.get("genie_space_id", ""), ""
                )
                if not name:
                    raise ValueError(
                        "Cannot derive Genie ACL: a genie_spaces[] entry has no "
                        "name and its genie_space_id has no canonical name mapping."
                    )
                resolved_names.append(name)
                if "acl_groups" in space:
                    acl = space["acl_groups"]
                    if acl is not None and (
                        not isinstance(acl, list) or not all(
                            isinstance(item, str) for item in acl
                        )
                    ):
                        raise ValueError(
                            f"User ACL for Genie space {name!r} must be a list "
                            "of group names (explicit [] means nobody)"
                        )
                    if acl is not None:
                        user_acls[name] = list(acl)
                tables = space.get("uc_tables") or []
                if isinstance(tables, list) and tables:
                    if isinstance(tables[0], list):
                        tables = tables[0]
                    cats = {t.split(".")[0] for t in tables if "." in t}
                    if cats:
                        space_catalogs[name] = cats
            duplicates = sorted({
                name for name in resolved_names if resolved_names.count(name) > 1
            })
            if duplicates:
                raise ValueError(
                    "Multiple Genie spaces resolve to the same canonical name: "
                    + ", ".join(repr(name) for name in duplicates)
                    + ". Set distinct names."
                )
            active_space_names = set(resolved_names)
            discovered_path = (
                env_tfvars_path.parent / "data_access" / "discovered_uc_tables.auto.tfvars"
            )
            if discovered_path.exists():
                table_agents = _validated_discovered_table_agents(
                    hcl2.loads(discovered_path.read_text()), discovered_path
                )
                discovered_catalogs: dict[str, set[str]] = {}
                for table, agents in table_agents.items():
                    for agent in agents:
                        if "." in table and agent in active_space_names:
                            discovered_catalogs.setdefault(agent, set()).add(
                                table.split(".")[0]
                            )
                for agent, cats in discovered_catalogs.items():
                    space_catalogs.setdefault(agent, cats)
        except ValueError:
            raise
        except Exception as exc:
            raise ValueError(
                f"Cannot derive Genie ACLs: environment HCL is invalid: {exc}"
            ) from exc

    # Build group → set of catalogs from fgac_policies
    group_catalogs: dict[str, set[str]] = {}
    for pol in fgac_policies:
        if isinstance(pol, list):
            pol = pol[0] if pol else {}
        # A treatment fallback borrows principals solely to ensure that a newly
        # classified column is masked.  It carries no model evidence that those
        # groups belong on this catalog, so it must not widen Genie/SELECT scope.
        if str(pol.get("comment", "")).startswith(
            ACL_NEUTRAL_FALLBACK_COMMENT
        ):
            continue
        catalog = pol.get("catalog") or pol.get("function_catalog") or ""
        if isinstance(catalog, list):
            catalog = catalog[0] if catalog else ""
        principals = pol.get("to_principals") or []
        if isinstance(principals, list) and principals:
            if isinstance(principals[0], list):
                principals = principals[0]
        for g in principals:
            group_catalogs.setdefault(g, set()).add(catalog)
        # Also include except_principals — they have elevated access
        except_p = pol.get("except_principals") or []
        if isinstance(except_p, list) and except_p:
            if isinstance(except_p[0], list):
                except_p = except_p[0]
        for g in except_p:
            group_catalogs.setdefault(g, set()).add(catalog)

    if reject_draft_acls:
        reject_unowned_draft_acls(tfvars_path, env_tfvars_path)

    # Every active space gets a sidecar entry. User-owned env ACLs win,
    # including explicit []; otherwise derive fresh from current policies.
    # Model-written ACL fields in genie_space_configs are never consulted.
    derived: dict[str, list[str]] = {}
    policy_derived_names: set[str] = set()
    candidate_names = (
        set(active_space_names)
        if active_space_names is not None
        else set(genie_cfgs)
    )
    for space_name in sorted(candidate_names):
        if space_name in user_acls:
            derived[str(space_name)] = user_acls[space_name]
            continue

        cats = space_catalogs.get(space_name, set())
        if not cats:
            raise ValueError(
                f"Cannot derive ACL for Genie space {space_name!r}: no mapped catalogs. "
                "Set acl_groups on this genie_spaces[] entry in env.auto.tfvars."
            )

        # Shared-catalog topology is ambiguous regardless of whether the draft
        # happened to emit recognizable account-group principals. Report the
        # ownership error before attempting policy-group resolution.
        for other_name in sorted(candidate_names):
            if other_name == space_name:
                continue
            shared_catalogs = cats & space_catalogs.get(other_name, set())
            if shared_catalogs:
                raise ValueError(
                    f"Cannot safely derive ACL for Genie space {space_name!r}: "
                    f"it shares catalog(s) {sorted(shared_catalogs)!r} with active "
                    f"space {other_name!r}. Catalog policies cannot distinguish "
                    "their table subsets. Set acl_groups on this genie_spaces[] "
                    "entry in env.auto.tfvars."
                )

        # Find groups that have policies on this space's catalogs
        space_groups = sorted({
            g for g, g_cats in group_catalogs.items()
            if g_cats & cats and g in groups
        })
        # access_tier_groups runs most- to least-privileged, so a tier above a
        # group the policies name is at least as entitled. Masks that list only
        # the masked tiers never name the full-access tier.
        ranked = [tier for tier in tier_order if tier in groups]
        named = [ranked.index(g) for g in space_groups if g in ranked]
        if named:
            space_groups = sorted(set(space_groups) | set(ranked[:max(named)]))

        if not space_groups and defer_unmapped:
            print(
                f"  NOTE: Genie ACL for {space_name!r} not derived yet (this draft has "
                "no access policies); the next full `make generate` derives it."
            )
            continue
        if not space_groups:
            raise ValueError(
                f"Cannot derive ACL for Genie space {space_name!r}: no policy groups "
                "map to its catalogs. Set acl_groups on this genie_spaces[] entry "
                "in env.auto.tfvars."
            )
        derived[str(space_name)] = space_groups
        policy_derived_names.add(str(space_name))

    # Catalog-scoped policies cannot distinguish table subsets belonging to two
    # active spaces in the same catalog. Any space derived from policy in that
    # topology must become an explicit user-owned ACL; already-explicit spaces
    # remain valid and are never blocked.
    all_active_names = sorted(candidate_names)
    for derived_name in sorted(policy_derived_names):
        for other_name in all_active_names:
            if other_name == derived_name:
                continue
            shared_catalogs = space_catalogs.get(derived_name, set()) & space_catalogs.get(
                other_name, set()
            )
            if shared_catalogs:
                raise ValueError(
                    f"Cannot safely derive ACL for Genie space {derived_name!r}: "
                    f"it shares catalog(s) {sorted(shared_catalogs)!r} with active "
                    f"space {other_name!r}. Catalog policies cannot distinguish "
                    "their table subsets. Set acl_groups on this genie_spaces[] "
                    "entry in env.auto.tfvars."
                )

    derived_path = tfvars_path.with_name(
        "genie_space_derived_acl_groups.auto.tfvars"
    )
    lines = [
        "# Tool-owned; resolved from env.auto.tfvars overrides or freshly derived policies.",
        f"genie_space_legacy_mode = {'false' if has_configured_spaces else 'true'}",
        "genie_space_derived_acl_groups = {",
    ]
    for space_name, acl_groups in derived.items():
        rendered = ", ".join(json.dumps(group) for group in acl_groups)
        lines.append(f"  {json.dumps(space_name)} = [{rendered}]")
    lines.append("}")
    rendered_text = "\n".join(lines) + "\n"
    if not derived_path.exists() or derived_path.read_text() != rendered_text:
        derived_path.write_text(rendered_text)
    return len(derived)


def reject_unowned_draft_acls(
    tfvars_path: Path,
    env_tfvars_path: Path | None,
) -> None:
    """Reject legacy ACL fields before any formatter can erase their intent."""
    import hcl2

    if not tfvars_path.exists() or not env_tfvars_path or not env_tfvars_path.exists():
        return
    try:
        cfg = hcl2.loads(tfvars_path.read_text())
        env_cfg = hcl2.loads(env_tfvars_path.read_text())
    except Exception as exc:
        raise ValueError(f"Cannot inspect legacy Genie ACLs: invalid HCL: {exc}") from exc

    configured_spaces = env_cfg.get("genie_spaces") or []
    if not configured_spaces:
        return
    id_to_name = cfg.get("genie_space_id_to_name") or {}
    user_acl_names: set[str] = set()
    for space in configured_spaces:
        if not isinstance(space, dict) or space.get("acl_groups") is None:
            continue
        name = space.get("name") or id_to_name.get(space.get("genie_space_id", ""), "")
        if name:
            user_acl_names.add(name)

    genie_cfgs = cfg.get("genie_space_configs") or {}
    if not isinstance(genie_cfgs, dict):
        return
    for space_name, cfg_entry in genie_cfgs.items():
        if isinstance(cfg_entry, list):
            cfg_entry = cfg_entry[0] if cfg_entry else {}
        if not isinstance(cfg_entry, dict):
            continue
        draft_key = next(
            (key for key in ("acl_groups", "genie_acl_groups") if key in cfg_entry),
            None,
        )
        if draft_key and space_name not in user_acl_names:
            raise ValueError(
                f"Genie space {space_name!r} has legacy draft {draft_key}="
                f"{cfg_entry[draft_key]!r}. Move this value to the matching "
                "genie_spaces[] entry in env.auto.tfvars (use [] for nobody), "
                "then remove it from generated/abac.auto.tfvars."
            )


def autofix_missing_genie_space_entries(tfvars_path: Path, auth_cfg: dict) -> int:
    """Ensure each configured Genie agent has a genie_space_configs entry."""
    configured_spaces = auth_cfg.get("genie_spaces", []) or []
    if not configured_spaces or not tfvars_path.exists():
        return 0

    try:
        import hcl2
        parsed = hcl2.loads(tfvars_path.read_text())
    except Exception:
        return 0

    genie_cfgs = parsed.get("genie_space_configs") or {}
    if isinstance(genie_cfgs, list):
        genie_cfgs = genie_cfgs[0] if genie_cfgs else {}
    genie_cfgs = dict(genie_cfgs)

    added = 0
    for space in configured_spaces:
        if isinstance(space, list):
            space = space[0] if space else {}
        if not isinstance(space, dict):
            continue
        name = str(space.get("name") or "").strip()
        if not name or name in genie_cfgs:
            continue
        genie_cfgs[name] = {}
        added += 1
        print(f"  [AUTOFIX] Added missing genie_space_configs entry for '{name}'")

    if not added:
        return 0

    text = tfvars_path.read_text()
    text = remove_hcl_top_level_block(text, "genie_space_configs")
    text = text.rstrip() + "\n\n" + format_genie_space_configs_hcl(genie_cfgs) + "\n"
    tfvars_path.write_text(text)
    return added


_GENERIC_FUNCTION_PREFS = ["mask_pii_partial", "mask_redact", "mask_nullify", "mask_hash"]
_GENERIC_SAFE_FUNCTIONS = {"mask_pii_partial", "mask_redact", "mask_nullify", "mask_hash"}
_FUNCTION_EXPECTED_CATEGORIES = {
    "mask_email": {"email"},
    "mask_phone": {"phone"},
    "mask_ssn": {"ssn"},
    "mask_full_name": {"name", "customer_pii", "customer_profile"},
    "mask_credit_card_full": {"card"},
    "mask_credit_card_last4": {"card"},
    "mask_amount_rounded": {"amount"},
    "mask_date_to_year": {"date"},
    "mask_timestamp_to_day": {"date"},
    # India-specific (IN overlay)
    "mask_aadhaar": {"government_id"},
    "mask_pan_india": {"government_id", "card", "payment_card"},
    # ANZ-specific
    "mask_tfn": {"government_id"},
    "mask_medicare": {"government_id"},
    "mask_bsb": {"financial_id"},
    # SEA-specific (SG/MY)
    "mask_nric": {"government_id"},
    "mask_fin": {"government_id"},
    "mask_mykad": {"government_id"},
    "mask_tin_my": {"government_id"},
    "mask_uen": {"business_id"},
    "mask_ssm": {"business_id"},
    "mask_epf": {"financial_id"},
    # SEA-specific (TH/ID/PH/VN)
    "mask_thai_id": {"government_id"},
    "mask_nik": {"government_id"},
    "mask_npwp": {"government_id"},
    "mask_bpjs": {"financial_id"},
    "mask_philsys": {"government_id"},
    "mask_tin_ph": {"government_id"},
    "mask_sss_ph": {"financial_id"},
    "mask_cccd": {"government_id"},
    "mask_mst": {"government_id"},
    # India-specific (additional)
    "mask_gstin": {"business_id"},
    "mask_voter_id": {"government_id"},
    "mask_dl_india": {"government_id"},
    "mask_uan": {"financial_id"},
    "mask_ration_card": {"government_id"},
    "mask_vehicle_reg": {"government_id"},
}


def _infer_column_categories_full(entity_name: str) -> set[str]:
    """Comprehensive column category inference matching validate_abac.py."""
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
    if ("card" in col and "cardholder" not in col and "card_holder" not in col) or "cvv" in col:
        categories.add("card")
    if _identifier_indicates_numeric_amount(col):
        categories.add("amount")
    # Government/financial IDs — country-specific columns
    if any(k in col for k in (
        # ANZ
        "tfn", "medicare", "nhi", "ird", "licence", "passport", "abn", "acn", "crn",
        # India
        "aadhaar", "aadhar", "pan", "gstin", "voter_id", "epic", "driving_licence",
        "uan", "ration_card",
        # SEA (SG/MY)
        "nric", "mykad", "mykas", "mytentera", "fin_number",
        # SEA (TH/ID/PH/VN)
        "thai_id", "nik", "ktp", "npwp", "bpjs", "philsys", "psn",
        "cccd", "cmnd", "can_cuoc",
    )):
        categories.add("government_id")
    if any(k in col for k in ("bsb", "routing", "epf", "kwsp", "cpf", "sss")):
        categories.add("financial_id")
    if any(k in col for k in ("uen", "ssm", "company_reg")):
        categories.add("business_id")
    # PAN can be both a card number and an Indian government ID
    if "pan" in col:
        categories.add("government_id")
    return categories or {"generic"}


def _identifier_indicates_numeric_amount(identifier: str) -> bool:
    """Classify one column/tag-value identifier, never a qualified FQN/blob."""
    tokens = set(re.split(r"[^a-z0-9]+", identifier.lower()))
    qualifiers = {
        "band", "type", "category", "code", "label", "description",
        "tier", "center", "history",
        "bands", "types", "categories", "codes", "labels", "descriptions",
        "tiers", "centers", "histories",
    }
    return bool(
        tokens & {
            "amount", "balance", "limit", "rounded", "price", "cost",
            "salary", "wage", "compensation",
        }
        and not tokens & qualifiers
    )


def _condition_numeric_tag_values(condition: str) -> bool:
    return any(
        _identifier_indicates_numeric_amount(value)
        for value in re.findall(
            r"hasTagValue\(\s*'[^']+'\s*,\s*'([^']+)'\s*\)", condition or ""
        )
    )


def autofix_canonical_function_names(tfvars_path: Path, sql_path: Path | None = None) -> int:
    """Normalize function names to canonical forms using the function registry.

    Renames both:
    - function_name references in FGAC policies (HCL)
    - CREATE FUNCTION definitions in masking SQL

    E.g., mask_card_last4 -> mask_credit_card_last4,
          filter_aml_clearance -> filter_aml_compliance

    Returns the total number of renames.
    """
    try:
        from function_registry import FUNCTION_REGISTRY
    except ImportError:
        return 0

    total = 0

    # Normalize HCL (function_name references in FGAC policies)
    text = tfvars_path.read_text()
    new_text, hcl_count = FUNCTION_REGISTRY.normalize_hcl(text)
    if hcl_count:
        tfvars_path.write_text(new_text)
        total += hcl_count
        print(f"  [AUTOFIX] Normalized {hcl_count} function name(s) in ABAC config")

    # Normalize SQL (CREATE FUNCTION definitions)
    if sql_path and sql_path.exists():
        sql_text = sql_path.read_text()
        new_sql, sql_count = FUNCTION_REGISTRY.normalize_sql(sql_text)
        if sql_count:
            sql_path.write_text(new_sql)
            total += sql_count
            print(f"  [AUTOFIX] Normalized {sql_count} function name(s) in masking SQL")

    return total


# fgac_policies whose function an autofix changed or removed during this run.
# A reviewed rule can then restore the policy, so policy_autofix_results
# reports what each one finally became.
_POLICY_AUTOFIXES: set[str] = set()


def policy_autofix_results(tfvars_path: Path, reviewed_cfg: dict) -> list[str]:
    """One line per autofixed policy: what generated/abac.auto.tfvars holds now."""
    if not _POLICY_AUTOFIXES:
        return []
    try:
        import hcl2
        final = {p.get("name", ""): p for p in hcl2.loads(tfvars_path.read_text()).get("fgac_policies") or []}
    except Exception:
        return []
    reviewed = {p.get("name", ""): p for p in reviewed_cfg.get("fgac_policies") or []}
    lines = []
    for name in sorted(_POLICY_AUTOFIXES):
        policy = final.get(name)
        if policy is None:
            lines.append(f"  result: policy '{name}' is not in {tfvars_path.name} (removed by the autofix above)")
            continue
        function = ".".join(
            part for part in (policy.get("function_catalog"), policy.get("function_schema"),
                              policy.get("function_name")) if part
        )
        how = ("reviewed rule kept; the autofix above was discarded"
               if json.dumps(reviewed.get(name), sort_keys=True) == json.dumps(policy, sort_keys=True)
               else "the autofix above applies")
        lines.append(f"  result: policy '{name}' uses {function} in {tfvars_path.name} ({how})")
    return lines


def autofix_invalid_function_refs(tfvars_path: Path, sql_path: Path | None = None) -> int:
    """Fix FGAC policies referencing functions that don't exist in the SQL file.

    Search order for a replacement when the declared (cat, sch, fn) is missing:
      1. Same function name in another schema of the same catalog.
      2. A generic fallback function in the target schema.
      3. A generic fallback function in another schema of the same catalog.
      4. Same function name in ANY other catalog (cross-catalog fallback).
      5. A generic fallback function in ANY other catalog (last resort).
    """
    if not sql_path or not sql_path.exists():
        return 0
    try:
        import hcl2
    except ImportError:
        return 0

    text = tfvars_path.read_text()
    try:
        cfg = hcl2.loads(text)
    except Exception:
        return 0

    policies = cfg.get("fgac_policies", []) or []
    if not policies:
        return 0

    functions_by_schema = _parse_sql_functions_by_schema(sql_path)
    if not functions_by_schema:
        return 0

    # Replacement: (policy_name, old_fn, new_fn, old_sch, new_sch, old_cat, new_cat)
    replacements: list[tuple[str, str, str, str, str, str, str]] = []
    removals: list[str] = []  # policy names to remove (no replacement found)
    for p in policies:
        fn = p.get("function_name", "")
        fn_cat = p.get("function_catalog", "")
        fn_sch = p.get("function_schema", "")
        pname = p.get("name", "")
        if not fn or not fn_cat or not fn_sch:
            continue

        target_fns = functions_by_schema.get((fn_cat, fn_sch), set())
        if fn in target_fns:
            continue  # already valid

        # 1. Same function in another schema of the same catalog
        found = False
        for (cat, sch), fns in functions_by_schema.items():
            if cat == fn_cat and sch != fn_sch and fn in fns:
                replacements.append((pname, fn, fn, fn_sch, sch, fn_cat, fn_cat))
                found = True
                break
        if found:
            continue

        # 2. Generic fallback in target (fn_cat, fn_sch)
        new_fn: str | None = None
        new_sch = fn_sch
        new_cat = fn_cat
        for gfn in _GENERIC_FUNCTION_PREFS:
            if gfn in target_fns:
                new_fn = gfn
                break

        # 3. Generic fallback in another schema of the same catalog
        if not new_fn:
            for (cat, sch), fns in functions_by_schema.items():
                if cat != fn_cat:
                    continue
                for gfn in _GENERIC_FUNCTION_PREFS:
                    if gfn in fns:
                        new_fn = gfn
                        new_sch = sch
                        break
                if new_fn:
                    break

        # 4. Exact function name in ANY other catalog (cross-catalog)
        if not new_fn:
            for (cat, sch), fns in functions_by_schema.items():
                if cat == fn_cat:
                    continue
                if fn in fns:
                    new_fn = fn
                    new_sch = sch
                    new_cat = cat
                    break

        # 5. Generic fallback in ANY other catalog (last resort)
        if not new_fn:
            for (cat, sch), fns in functions_by_schema.items():
                if cat == fn_cat:
                    continue
                for gfn in _GENERIC_FUNCTION_PREFS:
                    if gfn in fns:
                        new_fn = gfn
                        new_sch = sch
                        new_cat = cat
                        break
                if new_fn:
                    break

        if new_fn:
            replacements.append((pname, fn, new_fn, fn_sch, new_sch, fn_cat, new_cat))
        else:
            # No replacement found — mark for removal
            removals.append(pname)
            _POLICY_AUTOFIXES.add(pname)
            print(f"  [AUTOFIX] Removing fgac_policy '{pname}': function '{fn}' not found "
                  f"in SQL file and no suitable replacement available")

    if not replacements and not removals:
        return 0

    section = _find_bracket_section(text, "fgac_policies")
    if section is None:
        return 0
    sec_start, sec_end = section
    section_text = text[sec_start:sec_end]
    blocks = _find_brace_blocks(section_text)

    rewritten = section_text
    fixes = 0
    for blk_start, blk_end in reversed(blocks):
        block_text = rewritten[blk_start:blk_end + 1]
        name_m = re.search(r'^\s*name\s*=\s*"([^"]+)"', block_text, re.MULTILINE)
        if not name_m:
            continue
        pname = name_m.group(1)

        # Remove policies with no replacement available
        if pname in removals:
            # Remove the entire block (including leading comma/whitespace)
            pre = rewritten[:blk_start].rstrip(" \t")
            if pre.endswith(","):
                pre = pre[:-1]
            rewritten = pre + rewritten[blk_end + 1:]
            fixes += 1
            continue

        matching = [r for r in replacements if r[0] == pname]
        if not matching:
            continue
        _, old_fn, new_fn, old_sch, new_sch, old_cat, new_cat = matching[0]

        updated = block_text
        if old_fn != new_fn:
            updated = re.sub(
                rf'(^\s*function_name\s*=\s*"){re.escape(old_fn)}(")',
                rf"\g<1>{new_fn}\g<2>",
                updated, count=1, flags=re.MULTILINE,
            )
        if old_sch != new_sch:
            updated = re.sub(
                rf'(^\s*function_schema\s*=\s*"){re.escape(old_sch)}(")',
                rf"\g<1>{new_sch}\g<2>",
                updated, count=1, flags=re.MULTILINE,
            )
        if old_cat != new_cat:
            updated = re.sub(
                rf'(^\s*function_catalog\s*=\s*"){re.escape(old_cat)}(")',
                rf"\g<1>{new_cat}\g<2>",
                updated, count=1, flags=re.MULTILINE,
            )
        if updated != block_text:
            rewritten = rewritten[:blk_start] + updated + rewritten[blk_end + 1:]
            fixes += 1
            loc_old = f"{old_fn}@{old_cat}.{old_sch}"
            loc_new = f"{new_fn}@{new_cat}.{new_sch}"
            _POLICY_AUTOFIXES.add(pname)
            print(f"  [AUTOFIX] Fixed function ref in policy '{pname}': {loc_old} -> {loc_new}")

    if not fixes:
        return 0

    # Clean up stray commas left behind by block removals
    rewritten = _cleanup_stray_commas(rewritten)

    text = text[:sec_start] + rewritten + text[sec_end:]

    # --- Remove orphaned tag_assignments left by removed policies ----------
    # Collect tag key/value pairs that the removed policies covered.
    removed_tag_pairs: set[tuple[str, str]] = set()
    for pname in removals:
        # Find the original policy in the parsed list and extract its match condition
        for p in policies:
            if p.get("name") == pname:
                for field in ("match_condition", "when_condition"):
                    cond = p.get(field, "") or ""
                    for m in re.finditer(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", cond):
                        removed_tag_pairs.add((m.group(1), m.group(2)))

    if removed_tag_pairs:
        # Check which of these pairs are still covered by a surviving policy
        surviving_pairs: set[tuple[str, str]] = set()
        for p in policies:
            if p.get("name") in removals:
                continue
            for field in ("match_condition", "when_condition"):
                cond = p.get(field, "") or ""
                for m in re.finditer(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", cond):
                    surviving_pairs.add((m.group(1), m.group(2)))

        orphaned_pairs = removed_tag_pairs - surviving_pairs
        if orphaned_pairs:
            for tag_key, tag_value in orphaned_pairs:
                # Remove matching tag_assignment lines
                pattern = re.compile(
                    r'[ \t]*\{[^}]*tag_key\s*=\s*"' + re.escape(tag_key)
                    + r'"[^}]*tag_value\s*=\s*"' + re.escape(tag_value)
                    + r'"[^}]*\}\s*,?\s*\n?',
                )
                new_text, n = pattern.subn('', text)
                if n:
                    text = new_text
                    print(f"  [AUTOFIX] Removed {n} orphaned tag_assignment(s) for "
                          f"{tag_key} = '{tag_value}' (policy removed)")

    tfvars_path.write_text(text)
    return fixes


def autofix_fgac_arg_count_mismatch(tfvars_path: Path, sql_path: Path | None = None) -> int:
    """Remove FGAC policies whose function arg count mismatches the policy type.

    Column masks (POLICY_TYPE_COLUMN_MASK) require exactly 1 argument.
    Row filters (POLICY_TYPE_ROW_FILTER) require exactly 0 arguments.

    When the LLM generates a row_filter referencing a column mask function (or
    vice versa), Terraform apply fails with 'policy definition requires N
    argument(s) but the referred function takes M argument(s)'.
    """
    if not sql_path or not sql_path.exists():
        return 0
    try:
        import hcl2 as _hcl2
    except ImportError:
        return 0

    text = tfvars_path.read_text()
    try:
        cfg = _hcl2.loads(text)
    except Exception:
        return 0

    policies = cfg.get("fgac_policies", []) or []
    if not policies:
        return 0

    # Parse arg counts from SQL.  Also infer from function name prefix as fallback.
    from validate_abac import parse_sql_function_arg_counts
    fn_arg_counts = parse_sql_function_arg_counts(sql_path)

    # Build lookup of functions by arg count for replacement candidates.
    available_functions = _parse_sql_function_names(sql_path)
    fns_by_args: dict[int, list[str]] = {}
    for fn_name, argc in fn_arg_counts.items():
        fns_by_args.setdefault(argc, []).append(fn_name)

    bad_policies: list[tuple[str, str, str, int]] = []  # (name, fn, ptype, expected_args)
    for p in policies:
        ptype = p.get("policy_type", "")
        fn = p.get("function_name", "")
        pname = p.get("name", "")
        if not fn or not pname:
            continue

        # Expected arg count for the policy type.
        if ptype == "POLICY_TYPE_COLUMN_MASK":
            expected_args = 1
        elif ptype == "POLICY_TYPE_ROW_FILTER":
            expected_args = 0
        else:
            continue

        actual_args = fn_arg_counts.get(fn)
        if actual_args is None:
            continue  # unknown function — let other autofixes handle it
        if actual_args != expected_args:
            bad_policies.append((pname, fn, ptype, expected_args))

    if not bad_policies:
        return 0

    bad_policy_names = {bp[0] for bp in bad_policies}
    # Build per-schema function lookup so replacements stay in the same catalog/schema.
    functions_by_schema = _parse_sql_functions_by_schema(sql_path)

    # Map policy name → (replacement_fn, catalog, schema) if one can be found.
    replacements: dict[str, str] = {}
    for pname, old_fn, ptype, expected_args in bad_policies:
        # Find the policy's catalog/schema to scope replacement candidates
        policy_cat, policy_sch = "", ""
        for p in policies:
            if p.get("name") == pname:
                policy_cat = p.get("function_catalog", "")
                policy_sch = p.get("function_schema", "")
                break

        # Prefer functions in the SAME catalog/schema with the right arg count
        same_schema_fns = functions_by_schema.get((policy_cat, policy_sch), set())
        candidates = sorted(fn for fn in same_schema_fns
                           if fn_arg_counts.get(fn) == expected_args)
        if not candidates:
            # Fallback to any function with the right arg count
            candidates = sorted(fns_by_args.get(expected_args, []))
        # Check if this policy covers numeric/date columns — use type-specific replacement
        policy_match = ""
        for p in policies:
            if p.get("name") == pname:
                policy_match = (p.get("match_condition", "") or "") + " " + (p.get("when_condition", "") or "")
                break
        is_numeric = _condition_numeric_tag_values(policy_match)
        is_date = any(tok in policy_match.lower() for tok in ("dob", "birth", "date"))

        if is_numeric:
            # Numeric columns need type-safe replacement — never fall through
            # to alphabetical (which could pick mask_credit_card_full for amounts).
            if "mask_amount_rounded" in (available_functions or set()):
                replacements[pname] = "mask_amount_rounded"
            # else: leave out of replacements → policy will be removed
        elif is_date:
            # Same rule for DATE columns — mask_date_to_year or nothing.
            if "mask_date_to_year" in (available_functions or set()):
                replacements[pname] = "mask_date_to_year"
            # else: leave out of replacements → policy will be removed
        else:
            # Non-numeric, non-date: semantic matching by tag_value tokens.
            # The naive "first alphabetical mask_*" pick causes systematic
            # category mismatches (e.g. mask_abn used for every string column
            # because it sorts first). Instead, prefer functions whose names
            # share tokens with the tag_value or match_condition.
            prefix = "filter_" if ptype == "POLICY_TYPE_ROW_FILTER" else "mask_"
            typed_candidates = [c for c in candidates if c.startswith(prefix)]
            if typed_candidates:
                # Extract tokens from the policy's tag_value references
                tag_values = re.findall(r"hasTagValue\(\s*'[^']+'\s*,\s*'([^']+)'\s*\)", policy_match)
                tokens = set()
                for tv in tag_values:
                    # Strip common prefixes to get the meaningful tokens
                    normalized = tv.lower().replace("masked_", "").replace("redacted_", "")
                    tokens.update(normalized.split("_"))
                # Score candidates: how many of their name tokens overlap with tag tokens
                def _semantic_score(fn: str) -> int:
                    fn_tokens = set(fn.lower().replace(prefix, "").split("_"))
                    return len(fn_tokens & tokens)
                scored = sorted(typed_candidates, key=lambda fn: (-_semantic_score(fn), fn))
                best = scored[0]
                # Only use it if there's a positive semantic match — otherwise
                # remove the policy rather than pick a random (likely miscategorized)
                # function. The uncovered-tag cleanup will then drop the tag.
                if _semantic_score(best) > 0:
                    replacements[pname] = best
                # else: leave out of replacements → policy will be removed

    # Apply replacements or removals in the file.
    section = _find_bracket_section(text, "fgac_policies")
    if section is None:
        return 0

    sec_start, sec_end = section
    section_text = text[sec_start:sec_end]
    blocks = _find_brace_blocks(section_text)
    if not blocks:
        return 0

    remove_indices: list[int] = []
    rewritten = section_text
    # First pass: replace functions where possible, mark for removal otherwise.
    # Process in reverse to preserve offsets.
    for idx in range(len(blocks) - 1, -1, -1):
        blk_start, blk_end = blocks[idx]
        block_text = rewritten[blk_start:blk_end + 1]
        name_m = re.search(r'name\s*=\s*"([^"]+)"', block_text)
        if not name_m or name_m.group(1) not in bad_policy_names:
            continue
        pname = name_m.group(1)
        if pname in replacements:
            new_fn = replacements[pname]
            fn_m = re.search(r'(function_name\s*=\s*")([^"]+)(")', block_text)
            if fn_m:
                old_fn = fn_m.group(2)
                new_block = block_text[:fn_m.start(2)] + new_fn + block_text[fn_m.end(2):]
                rewritten = rewritten[:blk_start] + new_block + rewritten[blk_end + 1:]
                _POLICY_AUTOFIXES.add(pname)
                print(
                    f"  [AUTOFIX] Replaced function '{old_fn}' → '{new_fn}' in fgac_policy "
                    f"'{pname}' (arg count mismatch)"
                )
            else:
                remove_indices.append(idx)
        else:
            remove_indices.append(idx)

    # Second pass: remove policies that couldn't be fixed.
    # Re-find blocks since replacements may have shifted offsets slightly.
    if remove_indices:
        blocks2 = _find_brace_blocks(rewritten)
        for idx in sorted(remove_indices, reverse=True):
            if idx >= len(blocks2):
                continue
            blk_start, blk_end = blocks2[idx]
            block_text = rewritten[blk_start:blk_end + 1]
            end = blk_end + 1
            while end < len(rewritten) and rewritten[end] in (",", " ", "\t"):
                end += 1
            start = blk_start
            while start > 0 and rewritten[start - 1] in (" ", "\t"):
                start -= 1
            if start > 0 and rewritten[start - 1] == "\n":
                start -= 1
            pname_m = re.search(r'name\s*=\s*"([^"]+)"', block_text)
            fn_m = re.search(r'function_name\s*=\s*"([^"]+)"', block_text)
            pname = pname_m.group(1) if pname_m else "?"
            fn_name = fn_m.group(1) if fn_m else "?"
            rewritten = rewritten[:start] + rewritten[end:]
            _POLICY_AUTOFIXES.add(pname)
            print(
                f"  [AUTOFIX] Removed fgac_policy '{pname}' — function '{fn_name}' "
                f"arg count does not match policy type"
            )

    text = text[:sec_start] + rewritten + text[sec_end:]
    tfvars_path.write_text(text)
    return len(bad_policies)


def autofix_row_filter_column_refs(
    tfvars_path: Path,
    sql_path: Path | None = None,
    ddl_path: Path | None = None,
) -> int:
    """Remove row filter policies whose functions reference columns not in the target table.

    The LLM sometimes generates row filter functions that reference columns
    from a different table (e.g., ``filter_aml_compliance`` references
    ``aml_flag`` from the transactions table but the policy binds it to
    the customers table).  Databricks rejects these at policy-creation time
    with UNRESOLVED_COLUMN.

    We parse the fetched DDL to build a per-table column set, extract
    identifiers from each row filter function body, and remove any FGAC
    policy where the function references a column that doesn't exist in
    the target table.
    """
    if not sql_path or not sql_path.exists():
        return 0
    # Find the DDL file — check explicit path, then common locations
    if not ddl_path or not ddl_path.exists():
        ddl_path = sql_path.parent / "ddl" / "_fetched.sql"  # generated/ddl/_fetched.sql
    if not ddl_path or not ddl_path.exists():
        ddl_path = sql_path.parent.parent / "ddl" / "_fetched.sql"
    if not ddl_path or not ddl_path.exists():
        return 0  # no DDL available — skip

    try:
        import hcl2 as _hcl2
    except ImportError:
        return 0

    text = tfvars_path.read_text()
    try:
        cfg = _hcl2.loads(text)
    except Exception:
        return 0

    policies = cfg.get("fgac_policies", []) or []
    row_filters = [p for p in policies if p.get("policy_type") == "POLICY_TYPE_ROW_FILTER"]
    if not row_filters:
        return 0

    # Parse per-table columns from DDL
    ddl_text = ddl_path.read_text()
    table_columns: dict[str, set[str]] = {}  # full_name -> {col_names}
    for m in re.finditer(
        r"CREATE\s+TABLE\s+([\w.]+)\s*\((.*?)\)\s*;",
        ddl_text, re.IGNORECASE | re.DOTALL,
    ):
        table_name = m.group(1).lower()
        body = m.group(2)
        cols = set()
        for col_m in re.finditer(r"^\s*(\w+)\s+\w+", body, re.MULTILINE):
            cols.add(col_m.group(1).lower())
        if cols:
            table_columns[table_name] = cols

    if not table_columns:
        return 0

    # Parse function bodies from SQL
    sql_text = sql_path.read_text()
    fn_bodies: dict[str, str] = {}  # function_name -> body text
    for m in re.finditer(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:\S+\.)*(\w+)\s*\([^)]*\).*?RETURN\s+(.*?)(?:;|\Z)",
        sql_text, re.IGNORECASE | re.DOTALL,
    ):
        fn_bodies[m.group(1).lower()] = m.group(2)

    # SQL keywords to exclude from column-reference detection
    _SQL_KEYWORDS = {
        "select", "from", "where", "and", "or", "not", "in", "is", "null",
        "true", "false", "case", "when", "then", "else", "end", "as", "on",
        "join", "left", "right", "inner", "outer", "group", "by", "order",
        "having", "limit", "between", "like", "exists", "distinct", "union",
        "all", "any", "if", "return", "returns", "string", "int", "integer",
        "boolean", "double", "float", "date", "timestamp", "bigint", "void",
        "current_user",
    }

    # Check each row filter policy
    bad_policy_names: list[str] = []
    for p in row_filters:
        fn_name = (p.get("function_name") or "").lower()
        entity = (p.get("entity_name") or "").lower()
        pname = p.get("name", "")

        if fn_name not in fn_bodies or entity not in table_columns:
            continue

        body = fn_bodies[fn_name]
        table_cols = table_columns[entity]

        # Extract identifiers from the function body
        identifiers = set(re.findall(r"\b([a-z_]\w*)\b", body.lower()))
        identifiers -= _SQL_KEYWORDS
        # Also remove the function's own parameter names
        identifiers -= {fn_name}
        # Remove known SQL function names
        identifiers -= {"concat", "substring", "length", "upper", "lower",
                        "trim", "coalesce", "cast", "abs", "round", "sum",
                        "count", "avg", "min", "max", "date_format",
                        "date_trunc", "datediff", "current_date",
                        "current_timestamp"}

        # Check for identifiers that look like column references but aren't in the table
        # Only flag if the identifier IS a column in some OTHER table (cross-table hallucination)
        all_known_cols = set()
        for cols in table_columns.values():
            all_known_cols |= cols

        bad_refs = identifiers & all_known_cols - table_cols
        # Also check for hallucinated columns: identifiers that look like
        # column names (contain _ and are not SQL builtins) but don't exist
        # in the target table.  Only flag identifiers that are plausible
        # column names (contain underscore, typical of generated schemas).
        hallucinated = {
            ident for ident in identifiers - table_cols - all_known_cols
            if "_" in ident and ident not in {
                "is_account_group_member", "is_member", "current_user",
                "account_group_member",
            }
        }
        all_bad = bad_refs | hallucinated
        if all_bad:
            bad_policy_names.append(pname)
            desc = "cross-table" if bad_refs else "hallucinated"
            print(
                f"  [AUTOFIX] Row filter '{pname}' references {desc} column(s) {sorted(all_bad)} "
                f"not in {entity} — removing policy"
            )

    if not bad_policy_names:
        return 0

    # Remove the bad policies from the HCL text
    bad_set = set(bad_policy_names)
    section = _find_bracket_section(text, "fgac_policies")
    if section is None:
        return 0

    sec_start, sec_end = section
    rewritten = text[sec_start:sec_end]
    blocks = _find_brace_blocks(rewritten)

    for idx in range(len(blocks) - 1, -1, -1):
        blk_start, blk_end = blocks[idx]
        block_text = rewritten[blk_start:blk_end + 1]
        name_m = re.search(r'name\s*=\s*"([^"]+)"', block_text)
        if not name_m or name_m.group(1) not in bad_set:
            continue
        end = blk_end + 1
        while end < len(rewritten) and rewritten[end] in (",", " ", "\t"):
            end += 1
        start = blk_start
        while start > 0 and rewritten[start - 1] in (" ", "\t"):
            start -= 1
        if start > 0 and rewritten[start - 1] == "\n":
            start -= 1
        rewritten = rewritten[:start] + rewritten[end:]

    text = text[:sec_start] + rewritten + text[sec_end:]
    tfvars_path.write_text(text)
    return len(bad_policy_names)


def autofix_function_category_mismatch(tfvars_path: Path, sql_path: Path | None = None) -> int:
    """Fix policies using type-specific functions for columns with mismatched categories."""
    try:
        import hcl2
    except ImportError:
        return 0

    text = tfvars_path.read_text()
    try:
        cfg = hcl2.loads(text)
    except Exception:
        return 0

    policies = cfg.get("fgac_policies", []) or []
    assignments = cfg.get("tag_assignments", []) or []
    if not policies or not assignments:
        return 0

    available_functions = _parse_sql_function_names(sql_path)

    assignments_by_tag: dict[tuple[str, str], list[dict]] = {}
    for ta in assignments:
        if ta.get("entity_type") != "columns":
            continue
        assignments_by_tag.setdefault(
            (ta.get("tag_key", ""), ta.get("tag_value", "")), []
        ).append(ta)

    def _extract_tag_refs(condition: str) -> tuple[list[tuple[str, str]], list[str]]:
        value_refs = re.findall(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", condition or "")
        key_refs = re.findall(r"hasTag\(\s*'([^']+)'\s*\)", condition or "")
        return value_refs, key_refs

    replacements: list[tuple[str, str, str]] = []
    for p in policies:
        if p.get("policy_type") != "POLICY_TYPE_COLUMN_MASK":
            continue
        fn = p.get("function_name", "")
        if fn in _GENERIC_SAFE_FUNCTIONS and fn != "mask_redact":
            continue
        expected = {"generic"} if fn == "mask_redact" else _FUNCTION_EXPECTED_CATEGORIES.get(fn)
        if not expected:
            continue

        value_refs, key_refs = _extract_tag_refs(p.get("match_condition", ""))
        treatment_functions = {
            item.value: item.masking_function
            for item in load_treatment_config().treatments
        }
        # A gr_treatment policy is already type-checked by the treatment
        # contract. Do not let the generic category repair override its
        # canonical function (which the coverage gate intentionally enforces).
        if any(
            key == "gr_treatment" and treatment_functions.get(value) == fn
            for key, value in value_refs
        ):
            continue
        matched: list[dict] = []
        for key, value in value_refs:
            matched.extend(assignments_by_tag.get((key, value), []))
        for key in key_refs:
            for (tag_key, _), items in assignments_by_tag.items():
                if tag_key == key:
                    matched.extend(items)
        if not matched:
            # No matching tag assignments — but the policy may still have a wrong
            # function (e.g. duplicate _2/_3 policies).  Check the condition keywords
            # to detect type mismatches even without matched assignments.
            cond_raw = p.get("match_condition", "") or ""
            cond = cond_raw.lower()
            cond_is_numeric = _condition_numeric_tag_values(cond_raw)
            cond_is_date = any(tok in cond for tok in ("dob", "birth", "date"))
            if cond_is_numeric and fn != "mask_amount_rounded" and "mask_amount_rounded" in (available_functions or set()):
                replacements.append((p.get("name", ""), fn, "mask_amount_rounded"))
            elif cond_is_date and fn != "mask_date_to_year" and "mask_date_to_year" in (available_functions or set()):
                replacements.append((p.get("name", ""), fn, "mask_date_to_year"))
            continue

        categories = set()
        for ta in matched:
            categories.update(_infer_column_categories_full(ta.get("entity_name", "")))
        is_numeric = "amount" in categories or any(
            _identifier_indicates_numeric_amount(
                str(ta.get("entity_name", "")).rsplit(".", 1)[-1]
            )
            or _identifier_indicates_numeric_amount(str(ta.get("tag_value", "")))
            for ta in matched
        )
        if categories.issubset(expected) and not is_numeric:
            continue

        # Check if the matched columns are numeric/date — if so, replace
        # with the correct type-specific function, not mask_redact (STRING).
        matched_leaf_values = " ".join(
            str(ta.get("entity_name", "")).rsplit(".", 1)[-1]
            + " " + str(ta.get("tag_value", ""))
            for ta in matched
        ).lower()
        is_date = any(tok in matched_leaf_values for tok in (
            "dob", "birth", "date_of_birth", "opened_date", "expiry",
        ))
        if is_numeric:
            type_fn = "mask_amount_rounded"
            if not available_functions or type_fn in available_functions:
                replacements.append((p.get("name", ""), fn, type_fn))
            continue
        if is_date:
            type_fn = "mask_date_to_year"
            if not available_functions or type_fn in available_functions:
                replacements.append((p.get("name", ""), fn, type_fn))
            continue

        generic_fn = None
        for gfn in _GENERIC_FUNCTION_PREFS:
            if not available_functions or gfn in available_functions:
                generic_fn = gfn
                break
        if generic_fn:
            replacements.append((p.get("name", ""), fn, generic_fn))

    if not replacements:
        return 0

    section = _find_bracket_section(text, "fgac_policies")
    if section is None:
        return 0
    sec_start, sec_end = section
    section_text = text[sec_start:sec_end]
    blocks = _find_brace_blocks(section_text)

    rewritten = section_text
    fixes = 0
    for blk_start, blk_end in reversed(blocks):
        block_text = rewritten[blk_start:blk_end + 1]
        name_m = re.search(r'^\s*name\s*=\s*"([^"]+)"', block_text, re.MULTILINE)
        if not name_m:
            continue
        pname = name_m.group(1)

        matching = [r for r in replacements if r[0] == pname]
        if not matching:
            continue
        _, old_fn, new_fn = matching[0]

        updated = re.sub(
            rf'(^\s*function_name\s*=\s*"){re.escape(old_fn)}(")',
            rf"\g<1>{new_fn}\g<2>",
            block_text, count=1, flags=re.MULTILINE,
        )
        if updated != block_text:
            rewritten = rewritten[:blk_start] + updated + rewritten[blk_end + 1:]
            fixes += 1
            _POLICY_AUTOFIXES.add(pname)
            print(
                f"  [AUTOFIX] Fixed function category mismatch in policy '{pname}': "
                f"'{old_fn}' -> '{new_fn}'"
            )

    if not fixes:
        return 0

    text = text[:sec_start] + rewritten + text[sec_end:]
    tfvars_path.write_text(text)
    return fixes


def _remove_policy_block_by_name(text: str, policy_name: str, section: str = "fgac_policies") -> tuple[str, bool]:
    """Remove a policy block from HCL text using brace-counting (handles nested blocks).

    Unlike regex with ``[^}]*``, this correctly handles nested structures like
    ``column_mask = { ... }`` inside policy blocks.

    Returns (new_text, was_removed).
    """
    sec = _find_bracket_section(text, section)
    if not sec:
        return text, False
    sec_start, sec_end = sec
    sec_txt = text[sec_start:sec_end]
    blocks = _find_brace_blocks(sec_txt)
    name_pat = re.compile(r'name\s*=\s*"' + re.escape(policy_name) + r'"')
    for bs, be in reversed(blocks):
        bt = sec_txt[bs:be + 1]
        if name_pat.search(bt):
            abs_s = sec_start + bs
            abs_e = sec_start + be + 1
            # Include trailing comma and whitespace
            while abs_e < len(text) and text[abs_e] in (",", " ", "\t"):
                abs_e += 1
            # Include leading whitespace and newline
            while abs_s > 0 and text[abs_s - 1] in (" ", "\t"):
                abs_s -= 1
            if abs_s > 0 and text[abs_s - 1] == "\n":
                abs_s -= 1
            return text[:abs_s] + text[abs_e:], True
    return text, False


def autofix_duplicate_column_masks(tfvars_path: Path) -> int:
    """Remove FGAC policies that would cause multiple column masks on the same column.

    Databricks does not allow multiple column masks on the same column.  When the
    LLM generates two COLUMN_MASK policies that both match the same tagged columns
    (e.g. one via ``phi_level`` and another via ``clinical_type``), the Terraform
    apply succeeds but queries fail with MULTIPLE_MASKS.

    This autofix:
    1. Builds a map of column → set of tag values from tag_assignments
    2. For each COLUMN_MASK policy, determines which columns it matches
    3. If two policies match the same column, removes the more generic one
       (prefers domain-specific functions over ``mask_redact`` / ``mask_nullify``)

    Returns the number of policies removed.
    """
    try:
        import hcl2
    except ImportError:
        return 0

    text = tfvars_path.read_text()
    try:
        cfg = hcl2.loads(text)
    except Exception:
        return 0

    policies = cfg.get("fgac_policies") or []
    if isinstance(policies, list) and len(policies) == 1 and isinstance(policies[0], list):
        policies = policies[0]
    assignments = cfg.get("tag_assignments") or []
    if isinstance(assignments, list) and len(assignments) == 1 and isinstance(assignments[0], list):
        assignments = assignments[0]

    if not policies or not assignments:
        return 0

    _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()

    # Build column → {tag_key: tag_value} map from tag_assignments
    col_tags: dict[str, dict[str, str]] = {}
    for a in assignments:
        if isinstance(a, list):
            a = a[0] if a else {}
        if _s(a.get("entity_type", "")) != "columns":
            continue
        entity = _s(a.get("entity_name", ""))
        key = _s(a.get("tag_key", ""))
        val = _s(a.get("tag_value", ""))
        if entity and key:
            col_tags.setdefault(entity, {})[key] = val

    # For each COLUMN_MASK policy, find which columns it matches
    generic_fns = {"mask_redact", "mask_nullify", "mask_hash", "mask_pii_partial"}

    policy_columns: list[tuple[int, dict, set[str]]] = []
    for i, p in enumerate(policies):
        if isinstance(p, list):
            p = p[0] if p else {}
        if _s(p.get("policy_type", "")) != "POLICY_TYPE_COLUMN_MASK":
            continue
        condition = _s(p.get("match_condition", ""))
        if not condition:
            continue

        # Extract hasTagValue('key', 'value') pairs from condition
        import re
        tag_matches = re.findall(r"hasTagValue\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", condition)
        if not tag_matches:
            continue

        # Find columns that match ALL conditions (AND) or ANY condition (OR)
        is_or = " OR " in condition.upper() or " || " in condition
        matched_cols: set[str] = set()
        for col, tags in col_tags.items():
            if is_or:
                if any(tags.get(k) == v for k, v in tag_matches):
                    matched_cols.add(col)
            else:
                if all(tags.get(k) == v for k, v in tag_matches):
                    matched_cols.add(col)

        if matched_cols:
            policy_columns.append((i, p, matched_cols))

    # Find columns covered by multiple policies
    col_to_policies: dict[str, list[int]] = {}
    for idx, (i, p, cols) in enumerate(policy_columns):
        for col in cols:
            col_to_policies.setdefault(col, []).append(idx)

    # Identify policies to remove (prefer specific over generic)
    remove_indices: set[int] = set()
    for col, pidxs in col_to_policies.items():
        if len(pidxs) <= 1:
            continue
        # Sort: generic functions last
        entries = [(pidx, policy_columns[pidx]) for pidx in pidxs]
        specific = [(pidx, e) for pidx, e in entries if _s(e[1].get("function_name", "")) not in generic_fns]
        generic = [(pidx, e) for pidx, e in entries if _s(e[1].get("function_name", "")) in generic_fns]
        if specific and generic:
            # Remove generic ones — specific function is preferred
            for pidx, e in generic:
                remove_indices.add(e[0])  # original policy index
        elif len(specific) > 1:
            # Multiple specific functions overlap on the same column.
            # Keep the one whose function name best matches the column name
            # (e.g. mask_date_to_year for date_of_birth, mask_diagnosis_code
            # for diagnosis_code).  Fall back to keeping the one that covers
            # fewer columns (most targeted).
            col_lower = col.split(".")[-1].lower()
            scored: list[tuple[int, int, int, tuple]] = []
            for pidx, e in specific:
                fn = _s(e[1].get("function_name", "")).lower()
                # Score: how many words in the column name appear in the function name
                col_words = set(col_lower.replace("_", " ").split())
                fn_words = set(fn.replace("mask_", "").replace("_", " ").split())
                name_overlap = len(col_words & fn_words)
                num_cols = len(e[2])  # number of columns matched by this policy
                scored.append((-name_overlap, num_cols, pidx, e))
            scored.sort()
            # Keep the best match (first after sort), remove the rest
            for _, _, pidx, e in scored[1:]:
                remove_indices.add(e[0])

    if not remove_indices:
        return 0

    # Remove the policies from the HCL text by name
    removed = 0
    for idx in sorted(remove_indices, reverse=True):
        p = policies[idx]
        if isinstance(p, list):
            p = p[0] if p else {}
        name = _s(p.get("name", ""))
        if not name:
            continue
        text, was_removed = _remove_policy_block_by_name(text, name)
        if was_removed:
            removed += 1
            print(f"    Removed duplicate mask policy '{name}' (generic function on column already covered by specific policy)")

    if removed:
        text = _cleanup_stray_commas(text)
        tfvars_path.write_text(text)
    return removed


def autofix_forbidden_conditions(tfvars_path: Path) -> int:
    """Remove FGAC policies that use unsupported condition functions.

    Databricks ABAC only supports hasTagValue() and hasTag() in conditions.
    The LLM sometimes generates conditions with columnName(), tableName(),
    or IN() which cause validation failures.

    Returns the number of policies removed.
    """
    import re as _re_fc

    _FORBIDDEN = ["columnName(", "tableName(", " IN (", " IN("]

    text = tfvars_path.read_text()

    try:
        import hcl2
        cfg = hcl2.loads(text)
    except Exception:
        return 0

    policies = cfg.get("fgac_policies") or []
    if isinstance(policies, list) and len(policies) == 1 and isinstance(policies[0], list):
        policies = policies[0]

    _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()

    removed = 0
    for p in policies:
        if isinstance(p, list):
            p = p[0] if p else {}
        condition = _s(p.get("match_condition", "")) or _s(p.get("when_condition", ""))
        if not any(f in condition for f in _FORBIDDEN):
            continue
        name = _s(p.get("name", ""))
        if not name:
            continue
        text, was_removed = _remove_policy_block_by_name(text, name)
        if was_removed:
            removed += 1

    if removed:
        text = _cleanup_stray_commas(text)
        tfvars_path.write_text(text)
    return removed


def autofix_invalid_condition_values(tfvars_path: Path) -> int:
    """Remove FGAC policies whose conditions reference tag values not in tag_policies.

    The LLM sometimes generates hasTagValue('pci_level', 'full_card') in conditions
    when the tag_policy only defines ['public', 'masked_card_last4', 'redacted_cvv'].
    Databricks rejects these at query time with INVALID_TAG_POLICY_VALUE.

    Also removes policies that reference tag_keys not defined in tag_policies
    (complementing autofix_undefined_tag_refs which may miss condition-only refs).

    Returns the number of policies removed.
    """
    try:
        import hcl2
        cfg = hcl2.loads(tfvars_path.read_text())
    except Exception:
        return 0

    # Build allowed map: tag_key → set of allowed values
    allowed: dict[str, set[str]] = {}
    for tp in cfg.get("tag_policies", []):
        k = tp.get("key", "")
        vals = tp.get("values", [])
        if k and vals:
            allowed[k] = set(vals)

    if not allowed:
        return 0

    policies = cfg.get("fgac_policies") or []
    if isinstance(policies, list) and len(policies) == 1 and isinstance(policies[0], list):
        policies = policies[0]

    _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()

    # Find policies with invalid tag refs in conditions
    bad_names: list[str] = []
    for p in policies:
        if isinstance(p, list):
            p = p[0] if p else {}
        name = _s(p.get("name", ""))
        if not name:
            continue
        condition = _s(p.get("match_condition", "")) + " " + _s(p.get("when_condition", ""))
        # Check each hasTagValue('key', 'value') reference
        for m in re.finditer(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", condition):
            ref_key, ref_val = m.group(1), m.group(2)
            if ref_key not in allowed:
                bad_names.append(name)
                print(f"  [AUTOFIX] Removing fgac_policy '{name}': "
                      f"condition references undefined tag_key '{ref_key}'")
                break
            if ref_val not in allowed[ref_key]:
                bad_names.append(name)
                print(f"  [AUTOFIX] Removing fgac_policy '{name}': "
                      f"condition uses tag_value '{ref_val}' not in "
                      f"tag_policy '{ref_key}' {sorted(allowed[ref_key])}")
                break
        # Check hasTag('key') references
        for m in re.finditer(r"hasTag\(\s*'([^']+)'\s*\)", condition):
            ref_key = m.group(1)
            if ref_key not in allowed and name not in bad_names:
                bad_names.append(name)
                print(f"  [AUTOFIX] Removing fgac_policy '{name}': "
                      f"condition references undefined tag_key '{ref_key}' in hasTag()")
                break

    if not bad_names:
        return 0

    text = tfvars_path.read_text()
    for name in bad_names:
        text, _ = _remove_policy_block_by_name(text, name)

    text = _cleanup_stray_commas(text)
    tfvars_path.write_text(text)
    return len(bad_names)


def autofix_malformed_conditions(tfvars_path: Path) -> int:
    """Remove FGAC policies with syntactically invalid conditions.

    Catches compilation errors that would fail at Terraform apply time:
    - Unbalanced parentheses
    - Conditions that are just bare strings (not function calls)
    - Empty conditions for column mask policies (match_condition required)

    Returns the number of policies removed.
    """
    try:
        import hcl2
        cfg = hcl2.loads(tfvars_path.read_text())
    except Exception:
        return 0

    policies = cfg.get("fgac_policies") or []
    if isinstance(policies, list) and len(policies) == 1 and isinstance(policies[0], list):
        policies = policies[0]

    _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()

    bad_names: list[str] = []
    for p in policies:
        if isinstance(p, list):
            p = p[0] if p else {}
        name = _s(p.get("name", ""))
        policy_type = _s(p.get("policy_type", ""))
        if not name:
            continue

        match_cond = _s(p.get("match_condition", ""))
        when_cond = _s(p.get("when_condition", ""))

        # Column masks require a non-empty match_condition
        if policy_type == "POLICY_TYPE_COLUMN_MASK" and not match_cond:
            bad_names.append(name)
            print(f"  [AUTOFIX] Removing fgac_policy '{name}': "
                  f"COLUMN_MASK missing required match_condition")
            continue

        for cond_label, condition in [("match_condition", match_cond), ("when_condition", when_cond)]:
            if not condition:
                continue
            # Unbalanced parentheses
            if condition.count("(") != condition.count(")"):
                bad_names.append(name)
                print(f"  [AUTOFIX] Removing fgac_policy '{name}': "
                      f"unbalanced parentheses in {cond_label}")
                break
            # Condition must contain at least one hasTagValue() or hasTag() call
            if "hasTagValue(" not in condition and "hasTag(" not in condition:
                bad_names.append(name)
                print(f"  [AUTOFIX] Removing fgac_policy '{name}': "
                      f"{cond_label} has no hasTagValue()/hasTag() calls")
                break

    if not bad_names:
        return 0

    text = tfvars_path.read_text()
    for name in bad_names:
        text, _ = _remove_policy_block_by_name(text, name)

    text = _cleanup_stray_commas(text)
    tfvars_path.write_text(text)
    return len(bad_names)


def autofix_unsafe_row_filters(sql_path: Path) -> int:
    """Rewrite row filter functions that reference column names to use only group-based access.

    The LLM sometimes generates row filters like:
        RETURN aml_flag = 'CLEAR' OR is_account_group_member('Compliance_Officer')
    where 'aml_flag' is a hallucinated column. These break at apply time with
    UNRESOLVED_COLUMN errors.

    Safe row filters use only is_account_group_member() and current_user() — no
    column references. This autofix detects column-referencing row filters and
    rewrites them to keep only the is_account_group_member() calls.

    Returns the number of functions rewritten.
    """
    if not sql_path or not sql_path.exists():
        return 0

    text = sql_path.read_text()
    rewritten = 0

    # Match zero-arg RETURNS BOOLEAN functions (row filters)
    pattern = re.compile(
        r"(CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(\S+)\s*\(\s*\)\s*"
        r"RETURNS\s+BOOLEAN\s*"
        r"(?:COMMENT\s+'[^']*'\s*)?"
        r"RETURN\s+)(.*?)(?=;\s*$|\s*;?\s*(?:CREATE\s|$))",
        re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )

    def _rewrite_body(match: re.Match) -> str:
        prefix = match.group(1)
        fn_name = match.group(2).split(".")[-1]
        body = match.group(3).strip().rstrip(";")

        # Extract is_account_group_member('...') calls
        group_calls = re.findall(
            r"is_account_group_member\s*\(\s*'[^']+'\s*\)",
            body, re.IGNORECASE,
        )

        # Check if body has bare identifiers that could be column references.
        # Strip string literals and function calls to isolate bare identifiers.
        body_clean = re.sub(r"'[^']*'", "", body)  # remove string literals
        body_clean = re.sub(r"is_account_group_member\s*\([^)]*\)", "", body_clean, flags=re.IGNORECASE)
        body_clean = re.sub(r"current_user\s*\(\s*\)", "", body_clean, flags=re.IGNORECASE)

        # What's left should be only SQL keywords and operators
        safe_tokens = {
            "return", "case", "when", "then", "else", "end", "and", "or", "not",
            "true", "false", "null", "is", "",
        }
        remaining = re.findall(r"\b([a-z][a-z0-9_]*)\b", body_clean.lower())
        unsafe = [t for t in remaining if t not in safe_tokens]

        if not unsafe:
            # Body is safe — no column references
            return match.group(0)

        nonlocal rewritten
        rewritten += 1

        if group_calls:
            # Has group-based fallback — keep those, drop column refs
            new_body = " OR ".join(group_calls)
            print(f"  [AUTOFIX] Rewrote row filter '{fn_name}': removed column reference(s) "
                  f"{unsafe[:3]}, keeping group-based access only")
        else:
            # No group-based fallback — replace with TRUE (permissive no-op).
            # TRUE means "no filter applied" at query time. The row filter policy
            # still exists but has no effect. Safer than leaving the unresolved
            # column reference which causes UNRESOLVED_COLUMN at deploy time.
            new_body = "TRUE"
            print(f"  [AUTOFIX] Replaced row filter '{fn_name}' body with TRUE "
                  f"(had unresolved column reference(s) {unsafe[:3]} and no group fallback)")
        return f"{prefix}{new_body};"

    new_text = pattern.sub(_rewrite_body, text)
    if rewritten:
        sql_path.write_text(new_text)

    return rewritten


def autofix_remove_bodyless_functions(sql_path: Path) -> int:
    """Remove CREATE FUNCTION statements that lack a body (no RETURN clause).

    The LLM occasionally emits a function header followed only by a semicolon —
    e.g. ``CREATE OR REPLACE FUNCTION filter_aml_compliance(aml_flag BOOLEAN);``.
    Databricks rejects these at deploy time with "SQL functions should have a
    function definition" and the entire deploy fails. Strip them so any overlay
    injection (or simple removal) can recover.

    Detection: a CREATE FUNCTION statement is "bodyless" if, after stripping
    line and block comments, it contains no standalone ``RETURN`` keyword.
    ``RETURNS`` (the type declaration) is excluded by the word boundary —
    ``\\bRETURN\\b`` does not match ``RETURNS`` because ``S`` is a word char.
    """
    if not sql_path or not sql_path.exists():
        return 0

    sql_text = sql_path.read_text()

    # Split into (statement, separator) pairs using the same separator pattern
    # as deploy_masking_functions.parse_sql_blocks. The capture group preserves
    # the original separators so unaffected text stays byte-identical.
    parts = re.split(r"(;\s*(?:--[^\n]*)?\n)", sql_text)

    out: list[str] = []
    removed: list[str] = []
    i = 0
    while i < len(parts):
        stmt = parts[i]
        sep = parts[i + 1] if i + 1 < len(parts) else ""

        m = re.search(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:\S+\.)*(\w+)\s*\(",
            stmt, re.IGNORECASE,
        )
        if m:
            cleaned = re.sub(r"--[^\n]*", "", stmt)
            cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
            if not re.search(r"\bRETURN\b", cleaned, re.IGNORECASE):
                removed.append(m.group(1))
                i += 2
                continue

        out.append(stmt)
        out.append(sep)
        i += 2

    if removed:
        for name in removed:
            print(f"  [AUTOFIX] Removed bodyless function '{name}' from masking_functions.sql")
        sql_path.write_text("".join(out))

    return len(removed)


# ---------------------------------------------------------------------------
# PII column patterns → tag assignments.  Used by autofix_untagged_pii_columns
# to detect columns that the LLM forgot to tag.
# ---------------------------------------------------------------------------
_PII_COLUMN_TAG_MAP: list[tuple[list[str], str, str]] = [
    # (column name substrings, tag_key, tag_value)
    (["email", "email_address", "contact_email"],            "pii_level", "masked_email"),
    (["phone", "mobile", "telephone", "contact_number"],     "pii_level", "masked_phone"),
    (["address", "street_address", "residential_address"],   "pii_level", "redacted_address"),
    (["date_of_birth", "dob", "birth_date", "birthdate"],    "pii_level", "masked_dob"),
    (["ssn", "social_security"],                             "pii_level", "masked_ssn"),
    # Person names (backstop only — realistic data is tagged natively as class.name).
    # Bare "name" is matched exactly via _PII_EXACT_COLUMN_TAG_MAP, not as a
    # substring, so username/table_name/file_name are not swept in.
    (["full_name", "first_name", "last_name", "given_name", "surname",
      "family_name", "middle_name"],                         "pii_level", "masked_name"),
    # ANZ
    (["tfn", "tax_file_number"],                             "pii_level", "masked_tfn"),
    (["medicare", "medicare_number"],                        "pii_level", "masked_medicare"),
    (["bsb", "bsb_number"],                                 "pii_level", "masked_bsb"),
    # India
    (["aadhaar", "aadhar"],                                  "pii_level", "masked_aadhaar"),
    # SEA
    (["nric"],                                               "pii_level", "masked_nric"),
    (["mykad"],                                              "pii_level", "masked_mykad"),
    # Financial
    (["account_number", "bank_account"],                     "pii_level", "masked_account"),
    (["card_number", "credit_card", "pan"],                  "pci_level", "masked_card_last4"),
    (["cvv", "cvc", "card_verification"],                    "pci_level", "redacted_cvv"),
]

# Exact-column-name patterns (no substring matching) for hints too generic to
# match as substrings.
_PII_EXACT_COLUMN_TAG_MAP: list[tuple[list[str], str, str]] = [
    (["name"],                                               "pii_level", "masked_name"),
]

# Columns that look like amounts/balances → financial_sensitivity tag
_FINANCIAL_COLUMN_TAG_MAP: list[tuple[list[str], str, str]] = [
    (["balance", "amount", "credit_limit", "debit_amount",
      "transaction_amount", "transfer_amount"],              "financial_sensitivity", "rounded_amounts"),
]

# Maps each tag_value added by PII autofix to the list of masking functions
# that could cover it. A tag is only added if at least one covering function
# is available in the SQL file — otherwise the tag would be uncovered and
# fail validation.
_PII_TAG_REQUIRED_FUNCTIONS: dict[str, list[str]] = {
    "masked_email":     ["mask_email", "mask_pii_partial", "mask_redact"],
    "masked_phone":     ["mask_phone", "mask_pii_partial", "mask_redact"],
    "redacted_address": ["mask_redact", "mask_pii_partial"],
    "masked_dob":       ["mask_date_to_year"],
    "masked_ssn":       ["mask_ssn", "mask_redact", "mask_pii_partial"],
    "masked_name":      ["mask_full_name", "mask_name", "mask_redact", "mask_pii_partial"],
    "masked_tfn":       ["mask_tfn", "mask_redact"],
    "masked_medicare":  ["mask_medicare", "mask_redact"],
    "masked_bsb":       ["mask_bsb", "mask_redact"],
    "masked_aadhaar":   ["mask_aadhaar", "mask_pan_india", "mask_redact"],
    "masked_nric":      ["mask_nric", "mask_redact"],
    "masked_mykad":     ["mask_mykad", "mask_redact"],
    "masked_account":   ["mask_redact", "mask_pii_partial"],
    "masked_card_last4": ["mask_credit_card_last4", "mask_credit_card_full", "mask_redact"],
    "redacted_cvv":     ["mask_redact", "mask_nullify"],
}


def autofix_untagged_pii_columns(
    tfvars_path: Path,
    ddl_path: Path | None = None,
    sql_path: Path | None = None,
    classification_source: SensitivitySource | None = None,
    agent_footprint: list[dict] | None = None,
    authoritative_classification: bool = False,
) -> int:
    """Detect PII/sensitive columns in the DDL that the LLM forgot to tag.

    Scans the fetched DDL for column names matching known PII patterns and
    adds tag_assignment entries for any that are missing.  This prevents the
    common failure where the LLM generates groups and policies but omits
    tag assignments for obvious columns like email, phone, or address.

    Sensitivity per column is resolved through the :class:`SensitivitySource`
    interface: when ``classification_source`` supplies native UC Data
    Classification (``class.*``) findings for a column, those are authoritative;
    otherwise the deterministic DDL name-pattern inference (the LLM path's
    backstop, wrapped as :class:`LLMSource`) decides.  Every added assignment is
    logged with the source that produced it.  When ``classification_source`` is
    ``None`` (the default), the result is identical to the legacy name-pattern
    behaviour.

    Returns the number of tag assignments added.
    """
    # Find DDL file
    if not ddl_path or not ddl_path.exists():
        if sql_path and sql_path.exists():
            ddl_path = sql_path.parent / "ddl" / "_fetched.sql"
            if not ddl_path.exists():
                ddl_path = sql_path.parent.parent / "ddl" / "_fetched.sql"
    if not ddl_path or not ddl_path.exists():
        return 0

    try:
        import hcl2 as _hcl2
    except ImportError:
        return 0

    text = tfvars_path.read_text()
    try:
        cfg = _hcl2.loads(text)
    except Exception:
        return 0

    # Build set of already-tagged columns
    existing_tags: set[str] = set()
    for ta in cfg.get("tag_assignments", []) or []:
        if ta.get("entity_type") == "columns":
            existing_tags.add(ta.get("entity_name", ""))

    # Parse DDL to extract catalog.schema.table.column names
    ddl_text = ddl_path.read_text()
    # Match CREATE TABLE catalog.schema.table (...columns...)
    table_pattern = re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([\w.]+)\s*\((.*?)\)\s*(?:USING|;|\Z)",
        re.IGNORECASE | re.DOTALL,
    )

    all_columns: list[tuple[str, str]] = []  # (full_name, column_name)
    for m in table_pattern.finditer(ddl_text):
        table_fqn = m.group(1)  # catalog.schema.table
        cols_block = m.group(2)
        for line in cols_block.split("\n"):
            line = line.strip().rstrip(",")
            if not line or line.startswith("--") or line.startswith(")"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                col_name = parts[0].strip("`\"")
                if col_name.upper() in ("CONSTRAINT", "PRIMARY", "FOREIGN", "UNIQUE", "CHECK"):
                    continue
                full_name = f"{table_fqn}.{col_name}"
                all_columns.append((full_name, col_name.lower()))

    if not all_columns:
        return 0

    # A live native-classification run is authoritative for the whole declared
    # footprint.  Remove LLM-produced sensitivity assignments first so they
    # cannot suppress or augment the real class.* inventory.  Non-sensitivity
    # governance assignments (for example row-scope tags) are preserved.
    if authoritative_classification and classification_source is not None:
        treatment_cfg = load_treatment_config()
        sensitivity_sources = {
            source for treatment in treatment_cfg.treatments for source in treatment.sources
        }
        sensitivity_keys = {key for key, _ in sensitivity_sources}
        all_column_names = {full for full, _ in all_columns}
        assignments = cfg.get("tag_assignments", []) or []
        retained_assignments = [
            assignment for assignment in assignments
            if not (
                assignment.get("entity_type") == "columns"
                and assignment.get("entity_name") in all_column_names
                and assignment.get("tag_key") in sensitivity_keys
            )
        ]
        if len(retained_assignments) != len(assignments):
            text = _replace_bracket_section(
                text,
                "tag_assignments",
                [_render_tag_assignment_block(item) for item in retained_assignments],
            )
            assignments = retained_assignments
            existing_tags = {
                item.get("entity_name", "") for item in assignments
                if item.get("entity_type") == "columns"
            }

    # Check which masking functions are available in the SQL file.
    # Only add financial tags (rounded_amounts) if mask_amount_rounded is available,
    # and only add date tags (masked_dob) if mask_date_to_year is available.
    # Without the required function, the tag assignment would be uncovered.
    available_fns: set[str] = set()
    if sql_path and sql_path.exists():
        available_fns = set(re.findall(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:\S+\.)*(\w+)\s*\(",
            sql_path.read_text(), re.IGNORECASE,
        ))

    # Filter patterns: only include tags that have at least one covering function
    # available in the SQL file. Without a covering function, the tag assignment
    # would be uncovered and cause validation failure.
    def _any_covering_fn_available(tag_value: str) -> bool:
        required = _PII_TAG_REQUIRED_FUNCTIONS.get(tag_value)
        if required is None:
            return True  # unknown tag — pass through
        if not available_fns:
            return True  # no SQL file yet — don't over-filter
        return any(fn in available_fns for fn in required)

    active_patterns = [
        (hints, key, val) for hints, key, val in _PII_COLUMN_TAG_MAP
        if _any_covering_fn_available(val)
    ]
    if "mask_amount_rounded" in available_fns:
        active_patterns.extend(_FINANCIAL_COLUMN_TAG_MAP)
    active_exact_patterns = [
        (hints, key, val) for hints, key, val in _PII_EXACT_COLUMN_TAG_MAP
        if _any_covering_fn_available(val)
    ]

    # Resolve each untagged column's sensitivity through the SensitivitySource
    # interface.  The deterministic DDL name-pattern matcher below is the LLM
    # path's backstop, wrapped as an LLMSource; native classification (when
    # supplied) takes precedence per column via select_findings.
    col_name_by_full = dict(all_columns)
    candidate_columns = [
        full for full, _ in all_columns
        if authoritative_classification or full not in existing_tags
    ]
    if agent_footprint is not None:
        # The same canonical footprint that selected the classification tables
        # also supplies the coverage-gate denominator at column granularity.
        candidate_columns = [
            column for column in candidate_columns
            if footprint_contains_column(agent_footprint, column)
        ]

    def _ddl_pattern_infer(cols: list[str]) -> list[Finding]:
        out: list[Finding] = []
        for full_name in cols:
            col_name = col_name_by_full.get(full_name, full_name.split(".")[-1].lower())
            exact = next((p for p in active_exact_patterns if col_name in p[0]), None)
            if exact is not None:
                out.append(Finding(
                    entity_name=full_name, tag_key=exact[1], tag_value=exact[2],
                    source=_SRC_LLM, detail=col_name,
                ))
                continue
            for hints, tag_key, tag_value in active_patterns:
                if col_name in hints or any(h in col_name for h in hints):
                    out.append(Finding(
                        entity_name=full_name,
                        tag_key=tag_key,
                        tag_value=tag_value,
                        source=_SRC_LLM,
                        detail=col_name,
                    ))
                    break  # first match wins
        return out

    llm_source = LLMSource(_ddl_pattern_infer)
    if authoritative_classification and classification_source is not None:
        findings = classification_source.findings_for(candidate_columns)
    else:
        findings = select_findings(candidate_columns, classification_source, llm_source)

    # Never discard an authoritative classification finding because protection
    # is incomplete. Preserve it in generated config so the blocking coverage
    # gate can reject the gap and show the operator exactly what must be fixed.

    # Surface natively-classified columns we could not tag (unmapped class.*
    # semantic), so a reviewer / coverage gate sees that authority was claimed
    # but no governed tag applied.  These columns are still NOT handed to the LLM
    # (select_findings claimed them), preventing a silent LLM override.
    unmapped_findings: list[tuple[str, str]] = []
    if classification_source is not None and hasattr(classification_source, "unmapped_columns"):
        unmapped_findings = classification_source.unmapped_columns(candidate_columns)
        for entity_name, semantic in unmapped_findings:
            print(
                f"  [SENSITIVITY] Column {entity_name} carries native "
                f"class.{semantic} with no governed mapping — left untagged "
                "(LLM override suppressed)"
            )

    if not findings and not unmapped_findings:
        return 0

    # Native classification can report multiple semantics for one column (for
    # example email + phone + name in free text). Several semantics can map to
    # the same governed tag key, but UC permits only one value per key/entity.
    # Collapse same-key collisions using Option-B's configured precedence.
    # Findings from different tag families remain intact so the later treatment
    # derivation can still resolve cross-family precedence (for example PII + PCI).
    treatment_cfg = load_treatment_config()
    treatment_rank = {
        source: rank
        for rank, treatment in enumerate(treatment_cfg.treatments)
        for source in treatment.sources
    }
    findings_by_key: dict[tuple[str, str], list[Finding]] = {}
    for finding in findings:
        key = (finding.entity_name, finding.tag_key)
        findings_by_key.setdefault(key, []).append(finding)
    collapsed: dict[tuple[str, str], Finding] = {}
    for key, candidates in findings_by_key.items():
        current = min(
            candidates,
            key=lambda finding: treatment_rank.get(
                (finding.tag_key, finding.tag_value), len(treatment_rank)
            ),
        )
        # Domain-specific partial masks are unsafe for narrative text that the
        # native classifier says contains several different PII semantics. Use
        # a canonical full-redaction value instead of applying (for example)
        # an email parser to a sentence containing an embedded email and phone.
        if key[1] == "pii_level" and len({f.tag_value for f in candidates}) > 1:
            current = Finding(
                entity_name=current.entity_name,
                tag_key="pii_level",
                tag_value="redacted_mixed",
                source=current.source,
                detail="multi-semantic native PII",
            )
        collapsed[key] = current
    collapsed_findings = list(collapsed.values())
    new_assignments: list[dict] = [f.as_assignment() for f in collapsed_findings]

    # Inject new tag assignments into the HCL text
    # Find the tag_assignments section and append before the closing ]
    ta_section = re.search(r"(tag_assignments\s*=\s*\[)(.*?)(\])", text, re.DOTALL)
    if not ta_section:
        return 0

    insert_pos = ta_section.end(2)  # before the ]

    # Ensure the last existing entry has a trailing comma to prevent
    # "missing comma" HCL syntax errors when we append new entries.
    preceding = text[ta_section.end(1):insert_pos].rstrip()
    if preceding and preceding.endswith("}") and not preceding.endswith("},"):
        last_brace_idx = ta_section.end(1) + len(preceding) - 1
        text = text[:last_brace_idx + 1] + "," + text[last_brace_idx + 1:]
        # Re-find section since text shifted by 1 character
        ta_section = re.search(r"(tag_assignments\s*=\s*\[)(.*?)(\])", text, re.DOTALL)
        insert_pos = ta_section.end(2)

    lines = []
    for f in collapsed_findings:
        lines.append(
            f'  {{ entity_type = "columns", entity_name = "{f.entity_name}", '
            f'tag_key = "{f.tag_key}", tag_value = "{f.tag_value}" }},'
        )
        print(
            f"  [AUTOFIX] Added tag_assignment: {f.entity_name} "
            f"({f.tag_key} = '{f.tag_value}') [source: {f.source}]"
        )

    markers = [
        f"# gr.classification_unmapped: {entity_name}|class.{semantic}"
        for entity_name, semantic in unmapped_findings
    ]
    injection = "\n" + "\n".join(markers + lines) + "\n"
    text = text[:insert_pos] + injection + text[insert_pos:]
    tfvars_path.write_text(text)
    return len(collapsed_findings)


def autofix_remove_uncovered_tags(tfvars_path: Path, sql_path: Path | None = None) -> int:
    """Deprecated safety shim: uncovered sensitive tags are never removed.

    Coverage gaps are intentionally preserved for the blocking coverage gate.
    """
    return 0

    """Last-resort: remove tag_assignments that no active FGAC policy covers.

    After all other autofixes have had a chance, any non-public tag_assignment
    that is not matched by any COLUMN_MASK or ROW_FILTER policy will fail
    validation with "is not covered by any active fgac_policy".  Removing the
    tag is safer than blocking the apply — the governance surface is reduced
    but deploy succeeds.

    A policy is considered "inactive" (doesn't cover tags) if:
    - It has no match_condition/when_condition for the entity type
    - Its function_name is not defined in the SQL file (deploy would fail)

    This catches cases the targeted cleanups miss:
    - PII tags whose covering functions weren't generated by the LLM
    - LLM-generated row-filter tags without matching filter_* functions
    - Financial tags that escaped the financial_sensitivity cleanup
    - Tags covered by policies whose function is missing from SQL
    """
    try:
        import hcl2
    except ImportError:
        return 0

    text = tfvars_path.read_text()
    try:
        cfg = hcl2.loads(text)
    except Exception:
        return 0

    assignments = cfg.get("tag_assignments", []) or []
    if isinstance(assignments, list) and len(assignments) == 1 and isinstance(assignments[0], list):
        assignments = assignments[0]
    policies = cfg.get("fgac_policies", []) or []
    if isinstance(policies, list) and len(policies) == 1 and isinstance(policies[0], list):
        policies = policies[0]

    if not assignments:
        return 0

    # Parse available SQL functions — policies referencing missing functions
    # are "inactive" (the validator treats them the same way).
    available_fns: set[str] = set()
    if sql_path and sql_path.exists():
        available_fns = set(re.findall(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:\S+\.)*(\w+)\s*\(",
            sql_path.read_text(), re.IGNORECASE,
        ))

    _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()

    def _requires_coverage(tag_value: str) -> bool:
        return tag_value.strip().lower() not in {"public", "general", "exact"}

    # Normalize assignments to dicts
    flat_assignments = []
    for ta in assignments:
        if isinstance(ta, list):
            ta = ta[0] if ta else {}
        flat_assignments.append(ta)

    flat_policies = []
    for p in policies:
        if isinstance(p, list):
            p = p[0] if p else {}
        flat_policies.append(p)

    # Build column/table tag maps (for condition evaluation)
    col_tags: dict[str, dict[str, set[str]]] = {}
    table_tags: dict[str, dict[str, set[str]]] = {}
    for ta in flat_assignments:
        etype = _s(ta.get("entity_type", ""))
        ename = _s(ta.get("entity_name", ""))
        tkey = _s(ta.get("tag_key", ""))
        tval = _s(ta.get("tag_value", ""))
        if not ename or not tkey:
            continue
        if etype == "columns":
            col_tags.setdefault(ename, {}).setdefault(tkey, set()).add(tval)
        elif etype == "tables":
            table_tags.setdefault(ename, {}).setdefault(tkey, set()).add(tval)

    def _condition_matches(condition: str, tags: dict[str, set[str]]) -> bool:
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
        if re.search(r"[^()\sA-Za-z]", expr):
            return False
        try:
            return bool(eval(expr, {"__builtins__": {}}, {}))
        except Exception:
            return False

    def _table_of(etype: str, ename: str) -> str:
        if etype == "tables":
            return ename
        if etype == "columns":
            return ".".join(ename.split(".")[:3])
        return ""

    def _policy_covers(policy: dict, ta: dict) -> bool:
        etype = _s(ta.get("entity_type", ""))
        ename = _s(ta.get("entity_name", ""))
        ptype = _s(policy.get("policy_type", ""))
        p_cat = _s(policy.get("catalog", "")) or _s(policy.get("function_catalog", ""))
        table_fqn = _table_of(etype, ename)
        if not table_fqn:
            return False
        entity_catalog = table_fqn.split(".")[0] if "." in table_fqn else table_fqn
        if p_cat and entity_catalog != p_cat:
            return False

        # Policy is inactive if its function isn't in the SQL file —
        # the validator skips these, so we should too.
        if available_fns:
            pfn = _s(policy.get("function_name", "")).split(".")[-1]
            if pfn and pfn not in available_fns:
                return False

        match_cond = _s(policy.get("match_condition", ""))
        when_cond = _s(policy.get("when_condition", ""))

        if etype == "columns":
            if ptype != "POLICY_TYPE_COLUMN_MASK":
                return False
            ctags = col_tags.get(ename, {})
            ttags = table_tags.get(table_fqn, {})
            if not _condition_matches(match_cond, ctags):
                return False
            return _condition_matches(when_cond, ttags)

        if etype == "tables":
            if ptype != "POLICY_TYPE_ROW_FILTER":
                return False
            if not when_cond:
                return False
            return _condition_matches(when_cond, table_tags.get(ename, {}))

        return False

    # Identify uncovered non-public tag_assignments
    uncovered: list[dict] = []
    for ta in flat_assignments:
        tval = _s(ta.get("tag_value", ""))
        if not _requires_coverage(tval):
            continue
        if not any(_policy_covers(p, ta) for p in flat_policies):
            uncovered.append(ta)

    if not uncovered:
        return 0

    # Remove uncovered blocks via brace-counting (handles nested blocks safely)
    section = _find_bracket_section(text, "tag_assignments")
    if not section:
        return 0

    sec_start, sec_end = section
    sec_txt = text[sec_start:sec_end]
    blocks = _find_brace_blocks(sec_txt)

    def _block_matches_ta(block_text: str, ta: dict) -> bool:
        ename = _s(ta.get("entity_name", ""))
        tkey = _s(ta.get("tag_key", ""))
        tval = _s(ta.get("tag_value", ""))
        return bool(
            re.search(r'entity_name\s*=\s*"' + re.escape(ename) + r'"', block_text)
            and re.search(r'tag_key\s*=\s*"' + re.escape(tkey) + r'"', block_text)
            and re.search(r'tag_value\s*=\s*"' + re.escape(tval) + r'"', block_text)
        )

    # Process blocks in reverse so earlier offsets stay valid
    remaining_uncovered = list(uncovered)
    removed = 0
    for bs, be in reversed(blocks):
        if not remaining_uncovered:
            break
        bt = sec_txt[bs:be + 1]
        matched_ta = None
        for ta in remaining_uncovered:
            if _block_matches_ta(bt, ta):
                matched_ta = ta
                break
        if not matched_ta:
            continue
        abs_s = sec_start + bs
        abs_e = sec_start + be + 1
        while abs_e < len(text) and text[abs_e] in (",", " ", "\t"):
            abs_e += 1
        while abs_s > 0 and text[abs_s - 1] in (" ", "\t"):
            abs_s -= 1
        if abs_s > 0 and text[abs_s - 1] == "\n":
            abs_s -= 1
        text = text[:abs_s] + text[abs_e:]
        removed += 1
        ename = _s(matched_ta.get("entity_name", ""))
        tkey = _s(matched_ta.get("tag_key", ""))
        tval = _s(matched_ta.get("tag_value", ""))
        print(f"  [AUTOFIX] Removed uncovered tag_assignment: {ename} ({tkey}={tval})")
        remaining_uncovered.remove(matched_ta)

    if removed:
        tfvars_path.write_text(text)
    return removed


def autofix_inject_overlay_functions(
    sql_path: Path,
    countries: list[str] | None = None,
    industries: list[str] | None = None,
    catalog_schemas: list[tuple[str, str]] | None = None,
) -> int:
    """Inject overlay-provided masking functions missing from the LLM's SQL output.

    Country and industry overlays define complete SQL function bodies. When the
    LLM omits these functions, inject them deterministically from the YAML source.
    This ensures overlay-specific masking (e.g., mask_tfn, mask_medicare) is always
    available regardless of LLM non-determinism.

    Returns the number of functions injected.
    """
    if not sql_path or not sql_path.exists():
        return 0
    if not countries and not industries:
        return 0

    import yaml

    # Collect overlay function definitions
    overlay_fns: dict[str, dict] = {}  # name -> {signature, body, comment}
    for source_dir, codes in [(COUNTRIES_DIR, countries or []), (INDUSTRIES_DIR, industries or [])]:
        for code in codes:
            yaml_path = source_dir / f"{code.strip().upper() if source_dir == COUNTRIES_DIR else code.strip().lower()}.yaml"
            if not yaml_path.exists():
                continue
            with open(yaml_path) as f:
                data = yaml.safe_load(f)
            for fn in data.get("masking_functions", []):
                name = fn.get("name", "")
                sig = fn.get("signature", "")
                body = fn.get("body", "")
                comment = fn.get("comment", "")
                if name and sig and body:
                    overlay_fns[name] = {"signature": sig, "body": body, "comment": comment}

    if not overlay_fns:
        return 0

    # Check which functions are already in the SQL file
    sql_text = sql_path.read_text()
    existing = set(re.findall(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:\S+\.)*(\w+)\s*\(",
        sql_text, re.IGNORECASE,
    ))

    # Also check arg counts of existing functions — if the LLM generated a
    # function with the wrong number of arguments (e.g. 2-arg mask_amount_rounded
    # instead of 1-arg), the overlay's correct version should override it.
    try:
        from validate_abac import parse_sql_function_arg_counts
        existing_arg_counts = parse_sql_function_arg_counts(sql_path)
    except Exception:
        existing_arg_counts = {}

    # Overlay functions that return non-STRING types (DECIMAL, DATE, BOOLEAN)
    # are type-critical — the return type must exactly match the column type.
    # Always override the LLM's version for these, since the LLM often gets
    # the return type wrong (e.g. RETURNS STRING instead of RETURNS DECIMAL).
    _NON_STRING_RETURNS = re.compile(
        r"RETURNS\s+(DECIMAL|DATE|TIMESTAMP|BOOLEAN|INT|BIGINT|DOUBLE|FLOAT)",
        re.IGNORECASE,
    )

    missing = {}
    for name, defn in overlay_fns.items():
        if name not in existing:
            missing[name] = defn
        else:
            overlay_sig = defn.get("signature", "")
            # Always override for type-critical functions (non-STRING return)
            if _NON_STRING_RETURNS.search(overlay_sig):
                missing[name] = defn
            elif name in existing_arg_counts:
                # For STRING functions, only override if arg count is wrong
                overlay_args = overlay_sig.split("(", 1)[-1].rsplit(")", 1)[0].strip()
                overlay_arg_count = len([a for a in overlay_args.split(",") if a.strip()]) if overlay_args else 0
                if existing_arg_counts[name] != overlay_arg_count:
                    missing[name] = defn

    if not missing:
        return 0

    # Determine the catalog.schema for the injected functions
    # Prefer the explicit catalog_schemas from env config (authoritative),
    # fall back to parsing the SQL file's USE CATALOG/USE SCHEMA blocks.
    if catalog_schemas:
        target_catalog, target_schema = catalog_schemas[0]
    else:
        schema_match = re.search(r"USE CATALOG\s+(\S+).*?USE SCHEMA\s+(\S+)", sql_text, re.IGNORECASE | re.DOTALL)
        if schema_match:
            target_catalog = schema_match.group(1).rstrip(";")
            target_schema = schema_match.group(2).rstrip(";")
        else:
            return 0  # can't determine where to put functions

    # Inject missing functions at the end of the SQL file with explicit catalog/schema context
    injected = [
        f"\n-- === Overlay functions injected (LLM omitted these) ===\n"
        f"USE CATALOG {target_catalog};\n"
        f"USE SCHEMA {target_schema};\n"
    ]
    for name, defn in sorted(missing.items()):
        fn_sql = (
            f"\n-- Injected from overlay (LLM omitted this function)\n"
            f"CREATE OR REPLACE FUNCTION {defn['signature']}\n"
        )
        if defn["comment"]:
            fn_sql += f"COMMENT '{defn['comment']}'\n"
        fn_sql += f"RETURN {defn['body'].rstrip().rstrip(';')};\n"
        injected.append(fn_sql)
        print(f"  [AUTOFIX] Injected overlay function '{name}' into masking_functions.sql")

    if injected:
        sql_text = sql_text.rstrip() + "\n" + "\n".join(injected) + "\n"
        sql_path.write_text(sql_text)

    return len(injected)


def autofix_cross_catalog_function_deployment(tfvars_path: Path, sql_path: Path | None = None) -> int:
    """Ensure masking functions are deployed to all catalog.schema pairs referenced in FGAC policies.

    When the LLM generates FGAC policies for multiple catalogs but only defines
    masking functions under one catalog's USE CATALOG/USE SCHEMA block, the
    functions won't be deployed to the other catalog. This autofix duplicates
    function definitions to all required catalog.schema pairs.

    Returns the number of function definitions added.
    """
    if not sql_path or not sql_path.exists() or not tfvars_path.exists():
        return 0
    try:
        import hcl2
        cfg = hcl2.loads(tfvars_path.read_text())
    except Exception:
        return 0

    policies = cfg.get("fgac_policies", []) or []
    if not policies:
        return 0

    # Collect (catalog, schema, function_name) triples from policies
    needed: dict[tuple[str, str], set[str]] = {}  # (cat, sch) -> {fn_names}
    for p in policies:
        fn = p.get("function_name", "")
        fn_cat = p.get("function_catalog", "")
        fn_sch = p.get("function_schema", "")
        if fn and fn_cat and fn_sch:
            needed.setdefault((fn_cat, fn_sch), set()).add(fn)

    if not needed:
        return 0

    sql_text = sql_path.read_text()

    # Parse which functions exist in which schema
    functions_by_schema = _parse_sql_functions_by_schema(sql_path)

    # Find schemas where functions are needed but missing
    added = 0
    for (cat, sch), fn_names in needed.items():
        existing = functions_by_schema.get((cat, sch), set())
        missing = fn_names - existing
        if not missing:
            continue

        # Find function definitions from other schemas to copy
        fn_bodies: dict[str, str] = {}
        for fn_name in missing:
            for (src_cat, src_sch), src_fns in functions_by_schema.items():
                if fn_name in src_fns:
                    # Extract the function definition from the SQL
                    pattern = re.compile(
                        rf"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+{re.escape(fn_name)}\s*\(.*?\).*?;"
                        , re.IGNORECASE | re.DOTALL,
                    )
                    m = pattern.search(sql_text)
                    if m:
                        fn_bodies[fn_name] = m.group(0)
                    break

        if not fn_bodies:
            continue

        # Append a new catalog/schema section with the missing functions
        section = f"\n-- === {cat}.{sch} functions (auto-deployed for cross-catalog FGAC policies) ===\n"
        section += f"USE CATALOG {cat};\nUSE SCHEMA {sch};\n\n"
        for fn_name, body in sorted(fn_bodies.items()):
            section += body + "\n\n"
            added += 1
            print(f"  [AUTOFIX] Deployed '{fn_name}' to {cat}.{sch} (cross-catalog FGAC policy)")

        sql_text = sql_text.rstrip() + "\n" + section
        sql_path.write_text(sql_text)

    return added


def sanitize_space_key(name: str) -> str:
    """Convert a human-readable space name to a safe directory/Terraform key.

    Mirrors the sanitization applied in Terraform locals:
      'Finance Analytics' -> 'finance_analytics'
      'Exec Dashboard (Q1)' -> 'exec_dashboard_q1'
    """
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def load_groups_from_account_config(*, announce: bool = True) -> list[str]:
    """Load existing group names from the shared account abac.auto.tfvars.

    Returns an empty list if the file doesn't exist or has no groups.
    The account config lives at <env_parent>/account/abac.auto.tfvars
    relative to the current working directory.
    """
    account_abac = WORK_DIR.parent / "account" / "abac.auto.tfvars"
    if not account_abac.exists():
        return []
    try:
        import hcl2
        with open(account_abac) as f:
            cfg = hcl2.load(f)
        groups = cfg.get("groups") or {}
        if isinstance(groups, dict) and groups:
            names = list(groups.keys())
            if announce:
                print(f"  Auto-loaded {len(names)} group(s) from account config.")
            return names
    except Exception as e:
        print(f"  WARNING: Could not read account groups from {account_abac}: {e}")
    return []


# ---------------------------------------------------------------------------
# IdP group preflight (consume-by-default)
#
# GenieRails consumes existing IdP-synced access-tier groups by default and does
# not mint them. Before we hand a group->tier mapping to Terraform, verify every
# referenced group actually exists as an account-level (IdP-synced) group, so a
# typo or an un-synced group fails loudly here instead of silently producing a
# grant that matches nobody.
# ---------------------------------------------------------------------------
class GroupPreflightError(RuntimeError):
    """Raised when a referenced access-tier group is not synced from the IdP."""


def find_missing_idp_groups(referenced, existing) -> list[str]:
    """Return the referenced group names that are absent from ``existing``.

    Pure helper: comparison is exact on display name. ``existing`` is any
    iterable of account-level group display names. Order and de-duplication of
    the returned list follow first appearance in ``referenced``.
    """
    existing_set = {g for g in existing if g}
    missing: list[str] = []
    seen: set[str] = set()
    for name in referenced:
        if name and name not in existing_set and name not in seen:
            missing.append(name)
            seen.add(name)
    return missing


def preflight_consume_groups(referenced, existing, *, create_groups: bool = False) -> None:
    """Consume-by-default group-existence preflight.

    In the default (consume) path, every referenced access-tier group must
    already exist as an IdP-synced account group; otherwise raise
    :class:`GroupPreflightError` naming the missing group(s) with an actionable
    remediation. In the opt-in create/greenfield path (``create_groups=True``)
    this is a no-op, since GenieRails mints the groups itself.
    """
    if create_groups:
        return
    missing = find_missing_idp_groups(referenced, existing)
    if not missing:
        return
    bullets = "".join(f"    - {g}\n" for g in missing)
    raise GroupPreflightError(
        "IdP group preflight failed — the following access-tier group(s) are "
        "referenced but are NOT synced from your identity provider into the "
        "Databricks account:\n"
        f"{bullets}"
        "\nGenieRails consumes IdP-owned groups by default and does not create "
        "them. To fix this:\n"
        "  - Enable AIM (or SCIM where AIM is unavailable) so your IdP syncs "
        "these groups into the account, then re-run; or\n"
        "  - Pass the exact existing group names via --groups "
        "'<tier1>,<tier2>,...' (the group->tier mapping); or\n"
        "  - For a demo/greenfield deployment with no IdP, re-run with "
        "--create-groups so GenieRails mints the groups (account layer "
        "manage_groups = true)."
    )


def list_account_group_names(auth_cfg: dict) -> list[str] | None:
    """Best-effort list of account-level group display names via the Databricks SDK.

    Returns ``None`` (preflight is then skipped with a warning) when the SDK or
    account credentials are unavailable, so generation still works offline / in
    workspace-only setups. Returns a (possibly empty) list on success.
    """
    try:
        from databricks.sdk import AccountClient
    except Exception:
        return None

    # Same default as the Terraform roots; Azure sets databricks_account_host.
    host = auth_cfg.get("databricks_account_host") or "https://accounts.cloud.databricks.com"
    account_id = auth_cfg.get("databricks_account_id")
    client_id = auth_cfg.get("databricks_client_id")
    client_secret = auth_cfg.get("databricks_client_secret")
    if not (host and account_id and client_id and client_secret):
        return None

    try:
        ac = AccountClient(
            host=host,
            account_id=account_id,
            client_id=client_id,
            client_secret=client_secret,
        )
        return [g.display_name for g in ac.groups.list() if g.display_name]
    except Exception as e:
        print(f"  WARNING: Could not list account groups for IdP preflight: {e}")
        return None


def bootstrap_per_space_dirs(out_dir: Path, auth_cfg: dict, hcl_text: str) -> None:
    """After a full generation, extract each space's genie_space_configs entry
    and write it to generated/spaces/<key>/abac.auto.tfvars.

    This bootstraps the per-space directory structure so that subsequent
    per-space generation runs can patch individual spaces without touching others.
    """
    import hcl2
    import io

    # Prefer reading from the assembled on-disk file so that autofix-added
    # fields (e.g. from autofix_genie_config_fields) are included.  Fall back
    # to the raw hcl_text if the file hasn't been written yet.
    assembled_path = out_dir / "abac.auto.tfvars"
    source_text = assembled_path.read_text() if assembled_path.exists() else hcl_text

    try:
        parsed = hcl2.load(io.StringIO(source_text))
        genie_cfgs = parsed.get("genie_space_configs") or {}
    except Exception as e:
        print(f"  WARNING: Could not parse genie_space_configs for bootstrap: {e}")
        return

    if isinstance(genie_cfgs, list):
        genie_cfgs = genie_cfgs[0] if genie_cfgs and isinstance(genie_cfgs[0], dict) else {}

    if not isinstance(genie_cfgs, dict):
        genie_cfgs = {}

    id_to_name = parsed.get("genie_space_id_to_name") or {}
    if isinstance(id_to_name, list):
        id_to_name = id_to_name[0] if id_to_name and isinstance(id_to_name[0], dict) else {}

    configured_spaces = auth_cfg.get("genie_spaces", []) or []
    spaces_dir = out_dir / "spaces"
    try:
        folders, notes = resolve_space_folders(configured_spaces, id_to_name, spaces_dir, genie_cfgs)
    except SpaceFolderError as e:
        print(f"\nERROR: {e}")
        print("  generated/abac.auto.tfvars was written; only the per-agent folders were not updated.")
        sys.exit(1)
    if not folders:
        return
    for note in notes:
        print(f"  NOTE: {note}")

    written: list[Path] = []
    for folder in folders:
        space_name = folder.name
        cfg = genie_cfgs.get(space_name)
        if not isinstance(cfg, dict):
            cfg = dict(folder.config or {})
            cfg.setdefault("title", space_name)
        space_dir = spaces_dir / folder.key
        space_dir.mkdir(parents=True, exist_ok=True)

        space_abac = space_dir / "abac.auto.tfvars"
        written.append(space_abac)
        content = (
            "# ============================================================================\n"
            f"# Per-space config for: {space_name}\n"
            + _BOOTSTRAP_MARKER + " Re-run: make generate SPACE=\"" + space_name + "\"\n"
            "# to regenerate only this space without touching others.\n"
            "# ============================================================================\n\n"
            + format_genie_space_configs_hcl({space_name: cfg})
            + "\n"
        )
        space_abac.write_text(content)

    for stale_dir in {spaces_dir / key for f in folders for key in f.stale_keys}:
        _remove_bootstrap_stub(stale_dir)

    base = out_dir.parent
    print("  Per-agent config: " + ", ".join(
        str(p.relative_to(base)) if p.is_relative_to(base) else str(p) for p in written
    ))


_BOOTSTRAP_MARKER = "# Bootstrapped by full generation."


class SpaceFolderError(Exception):
    """The per-agent folders under generated/spaces/ can't be chosen safely."""


class SpaceFolder:
    """Where one agent's per-space draft lives: generated/spaces/<key>/."""

    def __init__(self, name: str, space_id: str, key: str, config: dict | None):
        self.name = name            # canonical name (genie_space_configs key)
        self.space_id = space_id
        self.key = key
        self.config = config
        # Sibling folders to drop if they are only a bootstrap stub.
        self.stale_keys: list[str] = []


def _is_bootstrap_stub(space_dir: Path) -> bool:
    """True if space_dir holds nothing but a bootstrap-written abac.auto.tfvars."""
    try:
        return (
            [p.name for p in space_dir.iterdir()] == ["abac.auto.tfvars"]
            and _BOOTSTRAP_MARKER in (space_dir / "abac.auto.tfvars").read_text()
        )
    except OSError:
        return False


def _bootstrap_stub_owner(space_dir: Path) -> str | None:
    """The agent name a bootstrap stub was written for (None if not a stub)."""
    if not _is_bootstrap_stub(space_dir):
        return None
    m = re.search(r"^# Per-space config for: (.*)$", (space_dir / "abac.auto.tfvars").read_text(), re.M)
    return m.group(1) if m else None


def _has_real_content(space_dir: Path) -> bool:
    """Anything beyond an empty folder or a bootstrap stub (e.g. a SPACE= draft)."""
    try:
        return space_dir.is_dir() and any(space_dir.iterdir()) and not _is_bootstrap_stub(space_dir)
    except OSError:
        return False


def _remove_bootstrap_stub(space_dir: Path) -> None:
    if _is_bootstrap_stub(space_dir):
        try:
            (space_dir / "abac.auto.tfvars").unlink()
            space_dir.rmdir()
        except OSError:
            pass


def resolve_space_folders(
    configured_spaces: list, id_to_name: dict, spaces_dir: Path, extra_names=()
) -> tuple[list[SpaceFolder], list[str]]:
    """Pick one generated/spaces/<key>/ folder per agent, or raise SpaceFolderError.

    The key is the sanitized canonical name (an imported agent's title, from
    genie_space_id_to_name). When distinct names sanitize to the same key,
    every agent in that group gets "<key>--<genie_space_id>" (or
    "--new_<name hash>" for an agent not created yet), so the choice depends
    only on identity, never list order; a shared bare folder with real
    content is an error.

    An agent's folder name can change between runs: an imported agent was
    once named after its raw ID, a new agent's "--new_<hash>" becomes
    "--<id>" once it is created, and a collision group can shrink back to a
    bare key. Every name the agent's identity could have produced is a
    candidate. A folder with real content claimed by more than one agent is
    an error (ownership is never guessed). Otherwise, per agent: if exactly
    one candidate has real content, keep using it (with a note); if none
    does, use the current name; if two or more do, it is an error. Errors are
    raised before anything is written. A bootstrap stub is removed only by
    the agent that owns it, and never while any agent uses it. Folder names are local to generate/merge only: no Terraform root
    reads generated/spaces/, so they never affect resource keys (Terraform's
    own "--" keys are separate).
    """
    folders: list[SpaceFolder] = []
    notes: list[str] = []
    seen_ids: dict[str, str] = {}
    for sp in configured_spaces:
        name = canonical_space_name(sp, id_to_name)
        if not name:
            continue
        space_id = sp.get("genie_space_id") or ""
        if name in seen_ids:
            raise SpaceFolderError(
                f"Genie agents {seen_ids[name] or '(new)'} and {space_id or '(new)'} are both "
                f"named {name!r}. Give one of them a distinct name in its genie_spaces entry "
                "(name = \"...\") in env.auto.tfvars, then re-run make generate."
            )
        seen_ids[name] = space_id
        folders.append(SpaceFolder(name, space_id, sanitize_space_key(name), sp.get("config")))

    for name in extra_names:
        if name not in seen_ids:
            seen_ids[name] = ""
            folders.append(SpaceFolder(name, "", sanitize_space_key(name), None))

    # Collisions: suffix EVERY member of the group with its stable identity, so
    # reordering genie_spaces never swaps which agent owns which folder.
    groups: dict[str, list[SpaceFolder]] = {}
    for folder in folders:
        groups.setdefault(folder.key, []).append(folder)
    for key, members in groups.items():
        if len(members) > 1:
            for member in members:
                member.key = f"{key}--{_identity_suffix(member)}"

    # Every folder name each agent's identity could have produced, and which
    # agents claim each name. All checks run before anything is written.
    candidates = {id(f): _candidate_keys(f) for f in folders}
    claims: dict[str, list[SpaceFolder]] = {}
    for folder in folders:
        for key in candidates[id(folder)]:
            claims.setdefault(key, []).append(folder)
    for key, owners in claims.items():
        if len(owners) > 1 and _has_real_content(spaces_dir / key):
            current = next((o for o in owners if o.key == key), None)
            if current:
                others = [o for o in owners if o is not current]
                raise SpaceFolderError(
                    f"generated/spaces/{key}/ has per-agent content and is the folder for "
                    f"{_describe(current)}, but it is also an earlier folder name for "
                    + ", ".join(_describe(o) for o in others)
                    + ", so it is unclear whose content it is. If it belongs to "
                    + " or ".join(f"{o.name!r}, move it to generated/spaces/{o.key}/" for o in others)
                    + f". If it belongs to {current.name!r}, give that agent a distinct name "
                    "(name = \"...\") in env.auto.tfvars and rename the folder to match. "
                    "Then re-run make generate."
                )
            who = ", ".join(_describe(o) for o in owners)
            targets = ", ".join(f"generated/spaces/{o.key}/" for o in owners)
            raise SpaceFolderError(
                f"generated/spaces/{key}/ has per-agent content but is shared by {who}. "
                f"Each now gets its own folder ({targets}). Move the content into the folder "
                f"of the agent it belongs to, delete generated/spaces/{key}/, then re-run make generate."
            )

    for folder in folders:
        earlier = [k for k in candidates[id(folder)] if k != folder.key]
        real = [k for k in [folder.key, *earlier] if _has_real_content(spaces_dir / k)]
        if len(real) > 1:
            keep, others = real[0], real[1:]
            listed = [f"generated/spaces/{k}/" for k in [*others, keep]]
            listed = ", ".join(listed[:-1]) + f" and {listed[-1]} " + ("both" if len(listed) == 2 else "all")
            others_text = ", ".join(f"generated/spaces/{k}/" for k in others)
            raise SpaceFolderError(
                f"{listed} hold per-agent content for {_describe(folder)}. Keep generated/spaces/{keep}/: "
                f"move or merge anything you still need from {others_text} into it, "
                f"delete {others_text}, then re-run make generate."
            )
        if real and real[0] != folder.key:
            notes.append(
                f"keeping generated/spaces/{real[0]}/ for {folder.name!r} (it has per-agent content)."
            )
            folder.key = real[0]

    in_use: dict[str, SpaceFolder] = {}
    for folder in folders:
        if folder.key in in_use:
            raise SpaceFolderError(
                f"Genie agents {_describe(in_use[folder.key])} and {_describe(folder)} would share "
                f"generated/spaces/{folder.key}/. Check their genie_space_id values in env.auto.tfvars, "
                "or give one of them a distinct name (name = \"...\"), then re-run make generate."
            )
        in_use[folder.key] = folder

    # A leftover name is cleaned up only by the one agent that owns it: its
    # sole claimant, or the claimant its bootstrap stub was written for.
    for key, owners in claims.items():
        if key in in_use:
            continue
        if len(owners) > 1:
            stub_for = _bootstrap_stub_owner(spaces_dir / key)
            owners = [o for o in owners if o.name == stub_for]
        if len(owners) == 1:
            owners[0].stale_keys.append(key)
    return folders, notes


def _candidate_keys(folder: "SpaceFolder") -> list[str]:
    """The agent's current folder name, then names it may have had before:
    its raw ID (before folders used titles), its "--new_<hash>" collision
    name (before it was created), and its bare or "--<id>" name (before its
    collision group grew or shrank)."""
    base = sanitize_space_key(folder.name)
    keys = [folder.key]
    if folder.space_id:
        keys += [sanitize_space_key(folder.space_id), f"{base}--new_{_name_hash(folder.name)}"]
    keys.append(f"{base}--{_identity_suffix(folder)}" if folder.key == base else base)
    return list(dict.fromkeys(keys))


def _describe(folder: "SpaceFolder") -> str:
    return f"{folder.name!r} ({folder.space_id or 'new'})"


def _identity_suffix(folder: "SpaceFolder") -> str:
    """Stable per-agent folder suffix: the genie_space_id, else a hash of the
    (unique) canonical name for an agent that doesn't exist yet."""
    if folder.space_id:
        return sanitize_space_key(folder.space_id)
    return "new_" + _name_hash(folder.name)


def _name_hash(name: str) -> str:
    return hashlib.sha256(name.encode()).hexdigest()[:8]


def canonical_space_name(space: dict, id_to_name: dict) -> str:
    """The name a genie_spaces entry is keyed by (mirrors Terraform's
    canonical_space_names): its name, else the imported title recorded in
    genie_space_id_to_name, else its raw genie_space_id."""
    space_id = space.get("genie_space_id") or ""
    return space.get("name") or (id_to_name.get(space_id) if space_id else "") or space_id


def load_genie_space_id_to_name(tfvars_path: Path) -> dict:
    """genie_space_id_to_name from a generated abac.auto.tfvars ({} if absent)."""
    try:
        import hcl2
        id_to_name = hcl2.loads(tfvars_path.read_text()).get("genie_space_id_to_name") or {}
    except Exception:
        return {}
    if isinstance(id_to_name, list):
        id_to_name = id_to_name[0] if id_to_name and isinstance(id_to_name[0], dict) else {}
    return id_to_name if isinstance(id_to_name, dict) else {}


def run_validation(
    out_dir: Path,
    countries: list[str] | None = None,
    industries: list[str] | None = None,
) -> bool:
    """Run validate_abac.py on the generated files. Returns True if passed."""
    validator = SCRIPT_DIR / "validate_abac.py"
    resolved_out_dir = out_dir.resolve()
    tfvars_path = resolved_out_dir / "abac.auto.tfvars"
    sql_path = resolved_out_dir / "masking_functions.sql"

    if not validator.exists():
        print("\n  [SKIP] validate_abac.py not found — skipping validation")
        return True

    cmd = [sys.executable, str(validator), str(tfvars_path)]
    if sql_path.exists():
        cmd.append(str(sql_path))
    # Keep both positional paths before options. Python <=3.11 argparse does
    # not reliably resume an optional positional after parsing options.
    cmd.extend(["--summary-label", "generate"])
    if os.environ.get("VERBOSE") == "1":
        cmd.append("--verbose")
    if countries:
        cmd.extend(["--country", ",".join(countries)])
    if industries:
        cmd.extend(["--industry", ",".join(industries)])

    print("\n  Running validation...\n")
    result = subprocess.run(cmd, cwd=str(WORK_DIR))
    return result.returncode == 0


def _run_delta_mode(auth_file: Path) -> None:
    """Incremental schema-drift classification: detect drift, remove stale, classify new."""
    from scripts.audit_schema_drift import (
        extract_managed_tables, resolve_governed_keys,
        extract_config_tag_assignments, detect_forward_drift,
        detect_reverse_drift, _get_sdk_client, _get_warehouse_id,
    )

    env_dir = Path.cwd()
    gen_abac = env_dir / "generated" / "abac.auto.tfvars"
    da_abac = env_dir / "data_access" / "abac.auto.tfvars"
    target_abac = gen_abac if gen_abac.exists() else da_abac

    print("=" * 60)
    print("  ABAC Delta Generator (incremental schema-drift mode)")
    print("=" * 60)

    managed_tables = extract_managed_tables(env_dir)
    if not managed_tables:
        print("  No managed tables found — nothing to do.")
        return

    governed_keys = resolve_governed_keys(env_dir)
    print(f"  Governed keys: {governed_keys}")

    config_assignments = extract_config_tag_assignments(env_dir)

    w = _get_sdk_client(env_dir)
    warehouse_id = _get_warehouse_id(env_dir, w)
    if not warehouse_id:
        print("  ERROR: No SQL warehouse available.")
        sys.exit(1)

    # ── Reverse drift: remove stale assignments ──────────────────────────
    reverse = detect_reverse_drift(w, warehouse_id, managed_tables, config_assignments)
    if reverse and target_abac.exists():
        removed = remove_stale_assignments(target_abac, reverse)
        if removed:
            print(f"\n  Removed {removed} stale tag_assignment(s) from {target_abac.name}:")
            for entity in reverse:
                print(f"    {entity}")

    # ── Forward drift: detect new untagged columns ───────────────────────
    forward = detect_forward_drift(w, warehouse_id, managed_tables, governed_keys)
    if not forward:
        if reverse:
            print("\n  No new untagged columns — done.")
            print("  Run 'make apply' to deploy the stale-removal changes.")
        else:
            print("\n  No schema drift detected — nothing to do.")
        return

    print(f"\n  Detected {len(forward)} untagged sensitive column(s):")
    for cat, sch, tbl, col, _ in forward:
        print(f"    {cat}.{sch}.{tbl}.{col}")

    # ── Load governed key/value universe for LLM constraint ──────────────
    governed_kv: dict[str, list[str]] = {}
    for source_path in [
        env_dir.parent / "account" / "abac.auto.tfvars",
        da_abac,
        env_dir / "generated" / "abac.auto.tfvars",
    ]:
        if not source_path.exists():
            continue
        try:
            import hcl2
            with open(source_path) as f:
                cfg = hcl2.load(f)
            for tp in cfg.get("tag_policies", []):
                k = tp.get("key", "")
                if k and k not in governed_kv:
                    governed_kv[k] = tp.get("values", [])
            if not governed_kv:
                for ta in cfg.get("tag_assignments", []):
                    k = ta.get("tag_key", "")
                    v = ta.get("tag_value", "")
                    if k:
                        governed_kv.setdefault(k, [])
                        if v and v not in governed_kv[k]:
                            governed_kv[k].append(v)
            if governed_kv:
                break
        except Exception:
            continue

    if not governed_kv:
        print("  ERROR: Could not resolve governed key/value universe from any config.")
        sys.exit(1)

    drifted_column_fqns = {
        f"{cat}.{sch}.{tbl}.{col}" for cat, sch, tbl, col, _ in forward
    }

    # ── Build constrained LLM prompt ────────────────────────────────────
    column_lines = "\n".join(
        f"  - {cat}.{sch}.{tbl}.{col}" + (f"  (comment: {cmt})" if cmt else "")
        for cat, sch, tbl, col, cmt in forward
    )
    kv_lines = "\n".join(
        f"  {k}: {v}" for k, v in governed_kv.items()
    )

    prompt = (
        "You are a data governance assistant. Classify the following new columns.\n\n"
        "Output ONLY a list of tag_assignment HCL blocks. Do not output tag_policies, "
        "groups, fgac_policies, or any other sections.\n\n"
        f"Use ONLY these tag keys and their allowed values:\n{kv_lines}\n\n"
        "Do not invent new keys or values.\n\n"
        f"Columns to classify:\n{column_lines}\n\n"
        "Output format (HCL, one block per column):\n"
        "  {\n"
        '    entity_type = "columns"\n'
        '    entity_name = "catalog.schema.table.column"\n'
        '    tag_key     = "<key>"\n'
        '    tag_value   = "<value>"\n'
        "  },\n"
    )

    auth_cfg = load_auth_config(auth_file)
    configure_databricks_env(auth_cfg)

    print(f"\n  Classifying via LLM (incremental, constrained to {len(governed_kv)} governed keys)...")

    provider_cfg = PROVIDERS["databricks"]
    call_fn = provider_cfg["call"]
    model = provider_cfg["default_model"]
    response_text = call_with_retries(call_fn, prompt, model, 3)

    # ── Parse LLM response into tag_assignments ─────────────────────────
    new_assignments: list[dict] = []
    for m in re.finditer(
        r'entity_type\s*=\s*"([^"]+)"[^}]*?'
        r'entity_name\s*=\s*"([^"]+)"[^}]*?'
        r'tag_key\s*=\s*"([^"]+)"[^}]*?'
        r'tag_value\s*=\s*"([^"]+)"',
        response_text, re.DOTALL,
    ):
        new_assignments.append({
            "entity_type": m.group(1),
            "entity_name": m.group(2),
            "tag_key": m.group(3),
            "tag_value": m.group(4),
        })

    if not new_assignments:
        print("  WARNING: LLM returned no parseable tag_assignments.")
        return

    # ── Validate ─────────────────────────────────────────────────────────
    errors = validate_delta_assignments(new_assignments, governed_kv, drifted_column_fqns)
    if errors:
        print(f"\n  ERROR: LLM output failed validation ({len(errors)} issue(s)):")
        for err in errors:
            print(f"    - {err}")
        sys.exit(1)

    print(f"  Validated: {len(new_assignments)} new tag_assignment(s), all keys/values within policy")

    # ── Merge ────────────────────────────────────────────────────────────
    if not target_abac.exists():
        target_abac.parent.mkdir(parents=True, exist_ok=True)
        target_abac.write_text("tag_assignments = [\n]\n")

    added = merge_delta_assignments(target_abac, new_assignments)
    print(f"  Merged {added} new assignment(s) into {target_abac}")
    print("  Run 'make apply' to deploy.")
    print("=" * 60)


def post_generate_semantic_check(tfvars_path: Path, auth_cfg: dict, mode: str = "") -> list[str]:
    """Check generated config for known LLM failure modes that autofix can't handle.

    Returns a list of error strings (empty = all checks passed).
    Called after autofixes but before validation to catch issues early
    and allow the caller to retry the LLM call.
    """
    errors: list[str] = []

    try:
        import hcl2 as _hcl2
        cfg = _hcl2.loads(tfvars_path.read_text())
    except Exception:
        return errors, []  # can't parse — let validation handle it

    # Check 1: genie_space_configs present when genie_spaces is configured.
    # This is a WARNING not an error — autofix_missing_genie_space_entries
    # handles it.  Don't trigger a retry for this since the autofix adds the
    # entries and the retry would overwrite them.
    warnings: list[str] = []
    genie_spaces = auth_cfg.get("genie_spaces", [])
    if genie_spaces:
        gsc = cfg.get("genie_space_configs") or {}
        if not gsc:
            warnings.append(
                "genie_space_configs section missing from LLM output "
                f"(expected for {len(genie_spaces)} configured genie_space(s))"
            )

    # Check 1b: tag_assignments and fgac_policies must not be empty when tables
    # are configured.  An empty governance config means the LLM produced an
    # incomplete response (e.g. only groups + tag_policies without the FGAC
    # sections), which would wipe all existing governance on apply.
    # Skip in genie mode — 0 tag_assignments is correct (governance team manages ABAC).
    uc_tables = list(auth_cfg.get("uc_tables") or [])
    if not uc_tables:
        for gs in auth_cfg.get("genie_spaces", []):
            uc_tables.extend(gs.get("uc_tables", []))
    if uc_tables and mode != "genie":
        ta = cfg.get("tag_assignments", []) or []
        fp = cfg.get("fgac_policies", []) or []
        if not ta and not fp:
            errors.append(
                f"Generated config has 0 tag_assignments and 0 fgac_policies "
                f"but {len(uc_tables)} table(s) are configured. "
                f"This is incomplete output — regenerate with full content."
            )

    # Check 2: tag_assignment values are valid for their key in the live policy
    live = _fetch_live_tag_policy_values()
    if live:
        for ta in cfg.get("tag_assignments", []):
            key = ta.get("tag_key", "")
            val = ta.get("tag_value", "")
            if key in live and val and val not in live[key]:
                file_policies = cfg.get("tag_policies", [])
                file_vals = set()
                for tp in file_policies:
                    if tp.get("key") == key:
                        file_vals = set(tp.get("values", []))
                        break
                if val not in file_vals:
                    errors.append(
                        f"tag_assignment uses '{val}' for key '{key}' — "
                        f"not in live policy {sorted(live[key])} or file policy {sorted(file_vals)}"
                    )

    # Check 3: masking SQL has basic syntactic validity
    sql_path = tfvars_path.parent / "masking_functions.sql"
    fgac_policies = cfg.get("fgac_policies", []) or []
    if sql_path.exists():
        sql_text = sql_path.read_text()
        # If FGAC policies exist but SQL has no functions, the output is incomplete
        sql_fn_count = len(re.findall(r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION', sql_text, re.IGNORECASE))
        if fgac_policies and sql_fn_count == 0:
            errors.append(
                f"fgac_policies reference functions but masking_functions.sql has "
                f"0 CREATE FUNCTION statements — SQL output is incomplete."
            )
        # Check for common LLM SQL errors
        if sql_text.strip():
            # Unmatched CASE/END
            case_count = len(re.findall(r'\bCASE\b', sql_text, re.IGNORECASE))
            end_count = len(re.findall(r'\bEND\b', sql_text, re.IGNORECASE))
            # Each CREATE FUNCTION has an END too, so allow some slack
            create_count = len(re.findall(r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION', sql_text, re.IGNORECASE))
            if case_count > 0 and end_count < case_count:
                errors.append(
                    f"masking_functions.sql has {case_count} CASE but only {end_count} END — "
                    f"likely incomplete or malformed SQL"
                )
            # Check each function has RETURNS
            functions = re.findall(r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+\S+\s*\([^)]*\)', sql_text, re.IGNORECASE)
            returns = re.findall(r'\bRETURNS\b', sql_text, re.IGNORECASE)
            if len(functions) > 0 and len(returns) < len(functions):
                errors.append(
                    f"masking_functions.sql has {len(functions)} function(s) but only "
                    f"{len(returns)} RETURNS clause(s) — likely malformed function definition"
                )

    # Check 3b: HCL contains '...' ellipsis placeholder (LLM laziness)
    # The LLM sometimes outputs '...' instead of generating full content.
    hcl_text = tfvars_path.read_text()
    ellipsis_lines = [
        i + 1 for i, line in enumerate(hcl_text.split("\n"))
        if re.match(r"^\s*\.{3}\s*$", line)  # line is just "..."
        or re.search(r'=\s*"[^"]*\.\.\.[^"]*"', line)  # value contains "..."
    ]
    if ellipsis_lines:
        errors.append(
            f"Generated HCL contains '...' ellipsis placeholder at line(s) {ellipsis_lines[:5]}. "
            f"This is incomplete output — regenerate with full content."
        )

    # Check 4: no forbidden condition functions in FGAC policies
    _forbidden_funcs = ["columnName(", "tableName(", " IN (", " IN("]
    for pol in cfg.get("fgac_policies", []):
        if isinstance(pol, list):
            pol = pol[0] if pol else {}
        condition = pol.get("match_condition", "") or pol.get("when_condition", "")
        if isinstance(condition, list):
            condition = condition[0] if condition else ""
        for forbidden in _forbidden_funcs:
            if forbidden in condition:
                errors.append(
                    f"fgac_policy '{pol.get('name', '?')}' uses unsupported '{forbidden.strip()}' "
                    f"in condition — only hasTagValue() and hasTag() are allowed"
                )
                break

    # Check 5: no non-canonical tag keys / values remain for normalized families
    for tp in cfg.get("tag_policies", []):
        key = tp.get("key", "")
        canonical_key = _canonical_tag_key(key)
        if canonical_key != key:
            errors.append(
                f"tag_policy key '{key}' is non-canonical; expected '{canonical_key}'"
            )
        for value in tp.get("values", []) or []:
            canonical_value = _canonical_tag_value(canonical_key, value)
            if canonical_value != value:
                errors.append(
                    f"tag_policy '{canonical_key}' uses non-canonical value '{value}' "
                    f"(expected '{canonical_value}')"
                )
            elif REGISTRY.is_allowed_value(canonical_key, value) is False:
                allowed_values = sorted(REGISTRY.canonical_values_for_key(canonical_key) or [])
                errors.append(
                    f"tag_policy '{canonical_key}' uses unknown canonical value '{value}' "
                    f"(allowed: {allowed_values})"
                )

    for ta in cfg.get("tag_assignments", []):
        key = ta.get("tag_key", "")
        value = ta.get("tag_value", "")
        canonical_key = _canonical_tag_key(key)
        canonical_value = _canonical_tag_value(canonical_key, value)
        if canonical_key != key:
            errors.append(
                f"tag_assignment key '{key}' is non-canonical; expected '{canonical_key}'"
            )
        if canonical_value != value:
            errors.append(
                f"tag_assignment '{key}={value}' is non-canonical; "
                f"expected '{canonical_key}={canonical_value}'"
            )
        elif REGISTRY.is_allowed_value(canonical_key, value) is False:
            allowed_values = sorted(REGISTRY.canonical_values_for_key(canonical_key) or [])
            errors.append(
                f"tag_assignment '{canonical_key}={value}' uses unknown canonical value "
                f"(allowed: {allowed_values})"
            )

    for pol in cfg.get("fgac_policies", []):
        if isinstance(pol, list):
            pol = pol[0] if pol else {}
        condition = " ".join(
            str(pol.get(field, "") or "")
            for field in ("match_condition", "when_condition")
        )
        for key, value in re.findall(
            r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)",
            condition,
        ):
            canonical_key = _canonical_tag_key(key)
            canonical_value = _canonical_tag_value(canonical_key, value)
            if canonical_key != key or canonical_value != value:
                errors.append(
                    f"fgac_policy '{pol.get('name', '?')}' uses non-canonical "
                    f"hasTagValue('{key}', '{value}')"
                )
            elif REGISTRY.is_allowed_value(canonical_key, value) is False:
                allowed_values = sorted(REGISTRY.canonical_values_for_key(canonical_key) or [])
                errors.append(
                    f"fgac_policy '{pol.get('name', '?')}' uses unknown canonical "
                    f"hasTagValue('{canonical_key}', '{value}') (allowed: {allowed_values})"
                )
        for key in re.findall(r"hasTag\(\s*'([^']+)'\s*\)", condition):
            canonical_key = _canonical_tag_key(key)
            if canonical_key != key:
                errors.append(
                    f"fgac_policy '{pol.get('name', '?')}' uses non-canonical "
                    f"hasTag('{key}')"
                )

    # Check 6: all input catalogs are represented in tag_assignments
    # When DDL spans multiple catalogs, the LLM sometimes "forgets" one.
    uc_tables = list(auth_cfg.get("uc_tables") or [])
    # Also collect tables from genie_spaces[].uc_tables
    for sp in auth_cfg.get("genie_spaces", []) or []:
        if isinstance(sp, dict):
            sp_tables = sp.get("uc_tables", []) or []
            uc_tables = list(uc_tables) + list(sp_tables)
    if isinstance(uc_tables, list) and len(uc_tables) > 0:
        # Extract unique catalogs from input table refs
        input_catalogs = set()
        for ref in uc_tables:
            if isinstance(ref, str):
                parts = ref.split(".")
                if len(parts) >= 3:
                    input_catalogs.add(parts[0])

        if len(input_catalogs) > 1:
            # Check which catalogs have tag_assignments
            covered_catalogs = set()
            for ta in cfg.get("tag_assignments", []):
                entity = ta.get("entity_name", "")
                if isinstance(entity, str):
                    parts = entity.split(".")
                    if len(parts) >= 3:
                        covered_catalogs.add(parts[0])

            missing = input_catalogs - covered_catalogs
            if missing:
                errors.append(
                    f"tag_assignments missing for catalog(s): {sorted(missing)}. "
                    f"Input had {sorted(input_catalogs)} but output only covers {sorted(covered_catalogs)}. "
                    f"Generate tag_assignments for ALL catalogs."
                )

    return errors, warnings


def champion_genie_next_steps(env_name: str) -> list[str]:
    """Next steps after the Dev import (`make generate MODE=genie`)."""
    return [
        "  Next steps (the walkthrough's Dev section):",
        "    1. In Catalog Explorer, open the catalog > Details tab > Data classification: turn it on.",
        "       When the scan finishes, review the detections and exclude any false positives, then turn on",
        "       auto-tagging. Wait until the class.* tags appear.",
        f"       Prefer a script? make enable-classification ENV={env_name} turns it on (you still review detections in the UI).",
        f"    2. make generate ENV={env_name}",
        f"    3. make rehearse ENV={env_name}   (picks a verification key automatically)",
    ]


def native_classification_stop(error: Exception, table_refs: list[str], env_name: str) -> list[str]:
    """Fail-closed stop (before any model call) when class.* tags can't be read."""
    lines = [f"ERROR: {error}"]
    if table_refs:
        lines.append(f"  Tables found ({len(table_refs)}): {', '.join(table_refs)} (saved). No model was called.")
    return lines + [
        "  Next: in Catalog Explorer, open the catalog > Details tab > Data classification: turn it on.",
        "  When the scan finishes, review the detections and exclude any false positives, then turn on",
        "  auto-tagging. Wait until the class.* tags appear.",
        f"  Prefer a script? make enable-classification ENV={env_name} turns it on (you still review detections in the UI).",
        f"  Then re-run make generate ENV={env_name}.",
        "  Re-run with --allow-llm-sensitivity only to explicitly accept LLM/DDL inference.",
    ]


def main():
    parser = argparse.ArgumentParser(
        description="Generate ABAC configuration from table DDL using AI",
        epilog=(
            "Examples:\n"
            "  python generate_abac.py                       # reads uc_tables from env.auto.tfvars\n"
            "  python generate_abac.py --tables 'prod.sales.*'  # CLI override\n"
            "  python generate_abac.py --promote              # generate + validate + split into account + env data_access + workspace\n"
            "  python generate_abac.py --dry-run              # print prompt without calling LLM\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--tables", nargs="+", metavar="CATALOG.SCHEMA.TABLE",
        help="Fully-qualified table refs to fetch from Databricks "
             "(overrides uc_tables in env.auto.tfvars). "
             "E.g. prod.sales.customers or prod.sales.* for all tables in a schema",
    )
    parser.add_argument(
        "--footprint", nargs="+", metavar="CATALOG.SCHEMA.TABLE[.COLUMN]",
        help="Declared reachable footprint for a not-yet-created Genie agent. "
             "Accepts table or column FQNs and overrides declared_footprint/uc_tables.",
    )
    parser.add_argument("--catalog", help="Catalog for masking UDFs (auto-derived from first uc_tables entry if omitted)")
    parser.add_argument("--schema", help="Schema for masking UDFs (auto-derived from first uc_tables entry if omitted)")
    parser.add_argument(
        "--auth-file",
        default=str(DEFAULT_AUTH_FILE),
        help="Path to auth tfvars file (default: auth.auto.tfvars)",
    )
    parser.add_argument(
        "--provider",
        choices=list(PROVIDERS.keys()),
        default="databricks",
        help="LLM provider (default: databricks)",
    )
    parser.add_argument("--model", help="Model name (defaults depend on provider)")
    parser.add_argument(
        "--ddl-dir",
        default="ddl",
        help="Directory containing .sql DDL files (default: ./ddl/)",
    )
    parser.add_argument(
        "--out-dir",
        default="generated",
        help="Output directory for generated files (default: ./generated/)",
    )
    parser.add_argument("--max-retries", type=int, default=3, help="Max LLM call attempts with exponential backoff (default: 3)")
    parser.add_argument("--skip-validation", action="store_true", help="Skip running validate_abac.py")
    parser.add_argument("--promote", action="store_true",
        help="Auto-split validated output into account + env data_access + workspace configs")
    parser.add_argument("--dry-run", action="store_true", help="Build the prompt and print it without calling the LLM")
    parser.add_argument(
        "--allow-rule-changes",
        action="store_true",
        help=(
            "Accept the model's changes to rules already in the reviewed draft "
            "(generated/). Without it, a re-run keeps every existing rule exactly "
            "as reviewed and only adds rules for newly uncovered tags or columns."
        ),
    )
    parser.add_argument(
        "--allow-llm-sensitivity",
        action="store_true",
        help=(
            "Explicitly allow LLM/DDL sensitivity inference when native "
            "classification is enabled but unreadable or empty. Without this "
            "opt-out, enabled native classification is fail-closed."
        ),
    )
    # --groups (consume) and --create-groups (invent) are mutually exclusive:
    # you either point GenieRails at existing IdP-synced groups OR opt into
    # minting new ones — never both (that would aim the create path at an
    # IdP-owned group, i.e. mixed ownership). argparse rejects the combination.
    group_mode = parser.add_mutually_exclusive_group()
    group_mode.add_argument(
        "--groups",
        help="Comma-separated existing group names to consume, one per access tier "
             "(the group->tier mapping). When set, the LLM uses these exact IdP-synced "
             "names instead of inventing new ones. This is the primary, expected input "
             "for the default consume path (e.g. --groups 'Finance_Analyst,Clinical_Staff'). "
             "Mutually exclusive with --create-groups.",
    )
    group_mode.add_argument(
        "--create-groups",
        action="store_true",
        help="OPT-IN (demo/greenfield only): let the LLM invent access-tier group names "
             "and have the account layer CREATE them (set manage_groups = true in "
             "envs/account/env.auto.tfvars). Off by default — GenieRails consumes existing "
             "IdP-synced groups and does not mint them. Mutually exclusive with --groups.",
    )
    parser.add_argument(
        "--space",
        metavar="SPACE_NAME",
        help="Name of a single Genie agent to (re)generate. "
             "Fetches only that space's tables, instructs the LLM to skip groups and "
             "tag_policies (shared state), and writes output to generated/spaces/<key>/. "
             "Existing groups are auto-loaded from envs/account/abac.auto.tfvars. "
             "After writing, the assembled generated/abac.auto.tfvars is patched with "
             "the new space's content — other spaces are untouched. "
             "Example: make generate SPACE=\"Finance Analytics\"",
    )
    parser.add_argument(
        "--delta",
        action="store_true",
        help=(
            "Incremental schema-drift mode: detect new untagged columns and stale "
            "tag_assignments, classify new columns using the LLM (constrained to "
            "existing governed keys/values), and merge into data_access/abac.auto.tfvars. "
            "No full regeneration — existing config is untouched. "
            "Example: make generate-delta ENV=prod"
        ),
    )
    parser.add_argument(
        "--country",
        metavar="CODE",
        help="Comma-separated region codes for country-specific identifier awareness "
             "(e.g. ANZ, IN, SEA). Injects masking patterns and regulatory context for "
             "the specified regions into the LLM prompt. Overrides the 'country' field "
             "in env.auto.tfvars if both are set. See shared/countries/ for available overlays.",
    )
    parser.add_argument(
        "--industry",
        metavar="CODE",
        help="Comma-separated industry codes for industry-specific identifier awareness "
             "(e.g. financial_services, healthcare, retail). Injects masking patterns, "
             "group templates, and regulatory context into the LLM prompt. Overrides the "
             "'industry' field in env.auto.tfvars if both are set. "
             "See shared/industries/ for available overlays.",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "governance", "genie"],
        default="full",
        help=(
            "Generation mode for self-service Genie deployments (default: full). "
            "governance — generate ABAC only (groups, tag policies, tag assignments, "
            "FGAC policies, masking functions); genie_space_configs is suppressed. "
            "Use this for the central Data Governance team. "
            "genie — generate Genie agent configs only (instructions, benchmarks, "
            "SQL measures/filters/expressions, join specs); all ABAC output and SQL "
            "masking functions are suppressed. Existing groups are auto-loaded. "
            "Use this for BU teams that consume pre-existing governance. "
            "full — generate everything (default, backward-compatible). "
            "Example: make generate MODE=governance  (governance team) "
            "         make generate MODE=genie       (BU team)"
        ),
    )

    args = parser.parse_args()

    # ── Delta mode: incremental schema-drift classification ─────────────
    if args.delta:
        try:
            _run_delta_mode(Path(args.auth_file))
        except Exception as exc:
            print(f"ERROR: generate --delta could not complete: {exc}", file=sys.stderr)
            sys.exit(2)
        return

    ddl_dir = Path(args.ddl_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    auth_file = Path(args.auth_file)

    print("=" * 60)
    print("  ABAC Configuration Generator")
    if args.space:
        print(f"  Mode: per-space — '{args.space}'")
    elif args.mode != "full":
        mode_labels = {
            "governance": "governance — ABAC only (no genie_space_configs)",
            "genie":      "genie — Genie configs only (no ABAC, no masking SQL)",
        }
        print(f"  Mode: {mode_labels.get(args.mode, args.mode)}")
    print("=" * 60)

    try:
        auth_cfg = load_auth_config(
            auth_file, strict_env=not args.tables and not args.dry_run
        )
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    # ── Country/region overlay: resolve from CLI --country or env config ─────
    # Priority: CLI --country > env.auto.tfvars country field > empty (global)
    country_raw = args.country or auth_cfg.get("country", "")
    countries: list[str] | None = None
    if country_raw:
        countries = [c.strip().upper() for c in country_raw.split(",") if c.strip()]
        if countries:
            print(f"  Country: {', '.join(countries)}")
        else:
            countries = None

    # ── Industry overlay: resolve from CLI --industry or env config ──────────
    # Priority: CLI --industry > env.auto.tfvars industry field > empty (global)
    industry_raw = args.industry or auth_cfg.get("industry", "")
    industries: list[str] | None = None
    if industry_raw:
        industries = [i.strip().lower() for i in industry_raw.split(",") if i.strip()]
        if industries:
            print(f"  Industry: {', '.join(industries)}")
        else:
            industries = None

    # ── Per-space mode: resolve the target space and redirect out_dir ────────
    # When --space is given, we only generate config for that one space.
    # The output goes to generated/spaces/<key>/ instead of generated/, and
    # after writing we merge the new content back into generated/abac.auto.tfvars.
    target_space_cfg: dict | None = None
    space_key: str = ""
    # ── Consumed group->tier mapping, in every mode ──────────────────────────
    # Precedence: explicit --groups > persisted access_tier_groups in
    # env.auto.tfvars > account-config auto-load (per-space / genie only, below)
    # > the consume-by-default error. The first explicit --groups is saved to
    # the setting; a differing --groups never rewrites an existing order.
    env_file = auth_file.parent / "env.auto.tfvars"
    try:
        persisted_groups = persisted_access_tier_groups(auth_cfg, env_file)
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    groups_source = "--groups CLI"
    persist_cli_groups = False
    if args.create_groups:
        if persisted_groups:
            print(f"  NOTE: ignoring {ACCESS_TIER_GROUPS_SETTING} "
                  "(--create-groups invents its own groups).")
    elif args.groups:
        cli_groups = parse_groups_arg(args.groups)
        if not persisted_groups:
            persist_cli_groups = bool(cli_groups)
        elif cli_groups != persisted_groups:
            print(
                f"  WARNING: --groups ({', '.join(cli_groups)}) differs from "
                f"{ACCESS_TIER_GROUPS_SETTING} ({', '.join(persisted_groups)}) in "
                f"{_env_display_path(env_file)}; using --groups for this run only.\n"
                f"    To change the saved tiers, edit {ACCESS_TIER_GROUPS_SETTING} "
                "there (most to least privileged), then drop --groups."
            )
    elif persisted_groups:
        args.groups = ",".join(persisted_groups)
        groups_source = f"{ACCESS_TIER_GROUPS_SETTING} in {_env_display_path(env_file)}"
    # Walkthrough advice only behind the explicit template/promote marker;
    # access_tier_groups alone is a general setting, not a discriminator.
    champion_flow = follows_walkthrough(env_file)
    if not args.space:
        # Fail on per-agent folder conflicts before any model call, not after.
        try:
            resolve_space_folders(
                auth_cfg.get("genie_spaces", []) or [],
                load_genie_space_id_to_name(Path(args.out_dir) / "abac.auto.tfvars"),
                Path(args.out_dir) / "spaces",
            )
        except SpaceFolderError as e:
            print(f"ERROR: {e}")
            sys.exit(1)

    if args.space:
        genie_spaces_cfg_all = auth_cfg.get("genie_spaces", [])
        assembled_id_to_name = load_genie_space_id_to_name(Path(args.out_dir) / "abac.auto.tfvars")
        for sp in genie_spaces_cfg_all:
            # Match the configured name, the raw ID, or an imported agent's
            # title (the key bootstrap_per_space_dirs used for its folder).
            candidates = {sp.get("name") or "", sp.get("genie_space_id") or "",
                          canonical_space_name(sp, assembled_id_to_name)} - {""}
            if any(c == args.space or sanitize_space_key(c) == sanitize_space_key(args.space)
                   for c in candidates):
                target_space_cfg = sp
                break

        if target_space_cfg is None:
            print(f"ERROR: No Genie agent named '{args.space}' found in env.auto.tfvars.")
            print("  Available spaces:")
            for sp in genie_spaces_cfg_all:
                print(f"    - {canonical_space_name(sp, assembled_id_to_name) or '(unnamed)'}")
            sys.exit(1)

        # Same folder choice as the full-generation bootstrap.
        try:
            space_folders, _notes = resolve_space_folders(
                genie_spaces_cfg_all, assembled_id_to_name, Path(args.out_dir) / "spaces"
            )
        except SpaceFolderError as e:
            print(f"ERROR: {e}")
            sys.exit(1)
        target_name = canonical_space_name(target_space_cfg, assembled_id_to_name)
        space_key = next(
            (f.key for f in space_folders if f.name == target_name),
            sanitize_space_key(target_name or args.space),
        )
        # Redirect output to the per-space directory
        base_out_dir = Path(args.out_dir)
        out_dir = base_out_dir / "spaces" / space_key
        print(f"  Space key:  {space_key}")
        print(f"  Out dir:    {out_dir}")

        # Auto-load existing groups from the account config so the LLM reuses
        # them (consume path only — never in the opt-in --create-groups path).
        if not args.groups and not args.create_groups:
            existing_groups = load_groups_from_account_config()
            if existing_groups:
                args.groups = ",".join(existing_groups)
                groups_source = "auto-loaded from account config"

    # In genie mode, also auto-load groups from account config so the LLM knows
    # which pre-existing groups are available for space ACLs (consume path only).
    #
    # Full and governance runs deliberately do NOT auto-load: they author the
    # tier policies, and envs/account/abac.auto.tfvars is their own promoted
    # output (merged across every env that promotes into the account), not a
    # user-declared, ordered tier list. Consuming it would silently freeze
    # whatever groups/order a previous run produced. The error below shows
    # those groups as a paste-ready access_tier_groups line instead.
    if args.mode == "genie" and not args.space and not args.groups and not args.create_groups:
        existing_groups = load_groups_from_account_config()
        if existing_groups:
            args.groups = ",".join(existing_groups)
            groups_source = "auto-loaded from account config"
            print(f"  Auto-loaded {len(existing_groups)} group(s) from account config (genie mode)")

    # ── Resolve group-generation mode (consume-by-default) ───────────────────
    # Done BEFORE any table fetch / LLM call so the default path can never
    # silently invent groups. Two modes:
    #   consume (DEFAULT): use the supplied group->tier mapping exactly; the
    #     account layer looks these IdP-synced groups up (does not mint them).
    #   create (--create-groups, opt-in demo/greenfield): the LLM invents names
    #     and the account layer creates them (manage_groups = true).
    group_names: list[str] | None = None
    if args.groups:
        group_names = parse_groups_arg(args.groups)
        print(f"  Groups:   {', '.join(group_names)} ({groups_source})")

    if args.create_groups:
        print("  Group mode: CREATE (opt-in demo/greenfield) — the LLM proposes "
              "access-tier groups and the account layer will mint them; set "
              "manage_groups = true in envs/account.")
    elif not group_names:
        # Consume mode with no mapping: refuse rather than let the LLM invent.
        account_groups = (
            load_groups_from_account_config(announce=False)
            if args.mode in ("full", "governance") and not args.space
            else []
        )
        print(
            "ERROR: consume-by-default requires a group->tier mapping.\n"
            "  GenieRails consumes existing IdP-synced groups by default and will "
            "not invent them.\n"
            "  Fix one of:\n"
            f"    - Set {ACCESS_TIER_GROUPS_SETTING} in {_env_display_path(env_file)} "
            "to the existing group names, one per access tier, most to least "
            "privileged\n"
            f"      (e.g. {ACCESS_TIER_GROUPS_SETTING} = "
            "[\"Finance_Analyst\", \"Clinical_Staff\"]); or\n"
            "    - Pass them once via --groups '<tier1>,<tier2>,...' (saved to "
            f"{ACCESS_TIER_GROUPS_SETTING} for later runs)\n"
            "      (e.g. make generate GENERATE_ARGS='--groups "
            "\"Finance_Analyst,Clinical_Staff\"'); or\n"
            "    - For a demo/greenfield account with no IdP-synced groups, opt in "
            "with --create-groups\n"
            "      (e.g. make generate GENERATE_ARGS='--create-groups') and set "
            "manage_groups = true in envs/account."
        )
        if account_groups:
            print(
                f"  envs/account/abac.auto.tfvars lists {len(account_groups)} group(s) "
                f"from an earlier promote; {args.mode} mode does not auto-load them "
                "(that file is this mode's own output, not a declared tier order).\n"
                "  To adopt them, check the order (most to least privileged) and set:\n"
                f"    {render_access_tier_groups(account_groups)}"
            )
        sys.exit(1)
    else:
        # Consume mode with a mapping: verify every referenced access-tier group
        # already exists as an IdP-synced account group. Runs on ALL consume
        # paths (literal --groups and auto-loaded config names). A missing group
        # fails loudly here instead of producing a grant that matches nobody.
        # Skipped with a note only when account creds are unavailable, so
        # offline / workspace-only generation still works.
        existing = list_account_group_names(auth_cfg)
        preflight_passed = False
        if existing is None:
            print("  NOTE: skipping IdP group preflight — no account credentials "
                  "available; ensure these groups are IdP-synced before apply.")
        else:
            try:
                preflight_consume_groups(group_names, existing, create_groups=False)
            except GroupPreflightError as e:
                print(f"ERROR: {e}")
                sys.exit(1)
            print(f"  IdP preflight: all {len(group_names)} referenced group(s) "
                  "found in the account.")
            preflight_passed = True
        if persist_cli_groups:
            if not preflight_passed:
                print(f"  NOTE: --groups not saved to {ACCESS_TIER_GROUPS_SETTING} — "
                      "the groups couldn't be verified (IdP preflight skipped).")
            elif args.dry_run:
                print(f"  NOTE: dry run — not saving --groups to {ACCESS_TIER_GROUPS_SETTING}.")
            elif not env_file.exists():
                print(f"  NOTE: {_env_display_path(env_file)} not found — "
                      f"--groups not saved to {ACCESS_TIER_GROUPS_SETTING}.")
            else:
                try:
                    persist_access_tier_groups(env_file, group_names)
                except ValueError as e:
                    print(f"  NOTE: --groups not saved: {e}.")
                else:
                    print(f"  Saved --groups to {ACCESS_TIER_GROUPS_SETTING} in "
                          f"{_env_display_path(env_file)}; later runs can omit --groups.")

    catalog = args.catalog or ""
    schema = args.schema or ""

    catalog_schemas: list[tuple[str, str]] | None = None

    # ── Auto-discover the canonical reachable footprint ─────────────────────
    # For spaces where genie_space_id is set but uc_tables is empty, query the
    # Genie agent API to learn what tables and config that space contains.
    # The existing space's genie_space_configs is parsed verbatim from the API
    # (no LLM involvement) and injected into the generated abac.auto.tfvars
    # after the LLM runs, replacing whatever the LLM generated for that space.
    api_genie_configs: dict[str, dict] = {}  # space_name -> config parsed from API
    footprint_entries: list = list(auth_cfg.get("declared_footprint", []) or [])
    preserved_discovery: list[str] = []
    all_space_tables: list[str] = []
    discovered_table_agents: dict[str, list[str]] = {}
    genie_space_id_to_name: dict[str, str] = {}

    if not args.tables:
        genie_spaces_cfg = auth_cfg.get("genie_spaces", [])
        discovery_incomplete = False
        preserve_existing_discovery = bool(args.footprint)
        # In per-space mode, restrict scanning to only the target space
        if target_space_cfg is not None:
            genie_spaces_cfg = [target_space_cfg]
        if genie_spaces_cfg:
            discovered_from_api: list[str] = []

            for space in genie_spaces_cfg:
                space_tables = space.get("uc_tables") or []
                space_declared = space.get("declared_footprint") or []
                footprint_entries.extend(space_declared or space_tables)
                space_id = space.get("genie_space_id") or ""
                space_name = space.get("name") or space_id
                agent_name = space_name
                exposed_tables = list(space_tables)

                if space_id:
                    # Always query the API for existing spaces to get config.
                    # Tables are also discovered here if uc_tables is not set.
                    # When uc_tables IS set, use quick_check_only to skip long
                    # retries — serialized_space is optional in that case.
                    if not space_tables:
                        print(f"\n  Genie agent '{space_name}' has no uc_tables — querying API...")
                    else:
                        print(f"\n  Querying existing Genie agent '{space_name}' for config...")

                    tables, genie_cfg, api_title, discovery_ok = fetch_tables_from_genie_space(
                        space_id, auth_cfg, quick_check_only=bool(space_tables)
                    )
                    if not discovery_ok:
                        discovery_incomplete = True

                    # Use the API title as the canonical name if no name was given
                    effective_name = space_name if space_name != space_id else (api_title or space_id)
                    if not discovery_ok and effective_name == space_id:
                        print(
                            f"  WARNING: Genie API lookup failed for {space_id!r}; "
                            "using the raw workspace-specific ID as its temporary canonical "
                            "name. Re-run generate when the API is reachable before promoting."
                        )
                    agent_name = effective_name
                    genie_space_id_to_name[space_id] = effective_name

                    if not space_tables:
                        exposed_tables = list(tables)
                        all_space_tables.extend(tables)
                        discovered_from_api.extend(tables)
                        footprint_entries.extend(tables)
                    else:
                        all_space_tables.extend(space_tables)

                    if genie_cfg:
                        if effective_name in api_genie_configs:
                            print(
                                "ERROR: Multiple Genie spaces resolve to the same canonical "
                                f"name {effective_name!r}. Set distinct names."
                            )
                            sys.exit(1)
                        api_genie_configs[effective_name] = genie_cfg
                else:
                    all_space_tables.extend(space_tables)

                for table in exposed_tables:
                    if agent_name:
                        owners = discovered_table_agents.setdefault(table, [])
                        if agent_name not in owners:
                            owners.append(agent_name)

            # An explicit CLI footprint bounds what is scanned on this run; it
            # does not prove that previously discovered, still-granted tables
            # disappeared. Preserve them and put them back into the masking
            # footprint just as for an incomplete API discovery.
            preserve_existing_discovery = discovery_incomplete or preserve_existing_discovery

            if discovered_from_api:
                print(
                    "\n  Auto-discovered tables from existing Genie agent(s):\n"
                    + "".join(f"    - {t}\n" for t in discovered_from_api)
                    + "\n  Persisted for automatic UC grants, classification, and masking scope."
                )

        if preserve_existing_discovery:
            try:
                preserved_discovery = preserve_discovered_tables_for_incomplete_run(
                    auth_file.parent / "data_access" / "discovered_uc_tables.auto.tfvars",
                    all_space_tables,
                    footprint_entries,
                )
            except ValueError as e:
                print(f"ERROR: {e}")
                sys.exit(1)
            if preserved_discovery:
                print(
                    "\n  Discovery incomplete or explicitly bounded; preserving and "
                    f"regenerating governance for {len(preserved_discovery)} persisted table(s)."
                )

        if not args.dry_run:
            try:
                persist_discovered_uc_tables(
                    auth_file.parent / "data_access" / "discovered_uc_tables.auto.tfvars",
                    all_space_tables,
                    table_agents=discovered_table_agents,
                    merge_existing=target_space_cfg is not None or preserve_existing_discovery,
                )
            except ValueError as e:
                print(f"ERROR: {e}")
                sys.exit(1)
    else:
        # --tables narrows the requested scan, but it cannot narrow Terraform's
        # persisted grant input. Keep every already-discovered grant target in
        # masking/coverage and in the downstream semantic checks.
        try:
            preserved_discovery = preserve_discovered_tables_for_incomplete_run(
                auth_file.parent / "data_access" / "discovered_uc_tables.auto.tfvars",
                all_space_tables,
                footprint_entries,
            )
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)

    # Downstream fail-closed checks consume auth_cfg["uc_tables"]. Perform this
    # merge only after preservation so no granted table bypasses those checks.
    existing_top = list(auth_cfg.get("uc_tables") or [])
    auth_cfg["uc_tables"] = list(dict.fromkeys(all_space_tables + existing_top))

    # This canonical object is the single source for DDL/classification scan
    # scope and, consequently, the classified-column coverage denominator.
    configured = list(auth_cfg.get("uc_tables") or []) + footprint_entries
    declared = (
        list(dict.fromkeys(args.footprint + all_space_tables))
        if args.footprint
        else (
            list(dict.fromkeys(args.tables + all_space_tables))
            if args.tables
            else configured
        )
    )
    # Deterministic envs keep governing tables every agent dropped (until
    # make ungovern), so their tags are regenerated rather than removed.
    from scripts.sticky_governance import GovernedTablesError, load_governed_tables
    try:
        declared = list(dict.fromkeys(list(declared) + load_governed_tables(auth_file.parent)))
    except GovernedTablesError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    agent_footprint = discover_agent_footprint(declared_footprint=declared)
    table_refs = footprint_table_refs(agent_footprint) or None

    if table_refs:
        source = "--footprint CLI" if args.footprint else ("--tables CLI" if args.tables else "agent footprint")
        column_count = sum(len(item.get("columns") or []) for item in agent_footprint)
        print(f"  Footprint: {len(agent_footprint)} table(s), {column_count} explicitly scoped column(s)")
        print(f"  Provider: {args.provider}")
        print(f"  Out dir:  {out_dir}")
        print(f"  Tables:   {', '.join(table_refs)} (from {source})")
        print()

        ddl_text, catalog_schemas = fetch_tables_from_databricks(
            table_refs, auth_cfg,
        )
        ddl_text = scope_ddl_to_footprint(ddl_text, agent_footprint)

        if not catalog or not schema:
            if not catalog_schemas:
                print("ERROR: No tables found — cannot determine UDF deployment location.")
                print("  Use --catalog and --schema to specify explicitly.")
                sys.exit(1)
            catalog = catalog or catalog_schemas[0][0]
            schema = schema or catalog_schemas[0][1]

        if catalog_schemas and len(catalog_schemas) > 1:
            print("  Masking UDFs will be deployed to:")
            for cat, sch in catalog_schemas:
                print(f"    - {cat}.{sch}")
        else:
            print(f"  Masking UDFs will be deployed to: {catalog}.{schema}")

        # Save fetched DDLs for inspection
        ddl_dir.mkdir(parents=True, exist_ok=True)
        fetched_path = ddl_dir / "_fetched.sql"
        fetched_path.write_text(ddl_text + "\n")
        print(f"  Fetched DDLs saved to: {fetched_path}")
    else:
        # Legacy mode: read from ddl/ directory
        if not catalog:
            print("ERROR: --catalog is required when using DDL files (no uc_tables configured).")
            sys.exit(1)
        if not schema:
            print("ERROR: --schema is required when using DDL files (no uc_tables configured).")
            sys.exit(1)

        if not ddl_dir.exists():
            print(f"\nERROR: DDL directory '{ddl_dir}' does not exist.")
            print(f"  mkdir -p {ddl_dir}")
            print("  # Then place your CREATE TABLE .sql files there")
            sys.exit(1)

        print(f"  Catalog:  {catalog}")
        print(f"  Schema:   {schema}")
        print(f"  Provider: {args.provider}")
        print(f"  DDL dir:  {ddl_dir}")
        print(f"  Out dir:  {out_dir}")
        print()

        ddl_text = load_ddl_files(ddl_dir)

    # group_names + consume/create mode were resolved (and preflighted) above,
    # before any table fetch, so the default path can never silently invent.

    # Collect space names from config so the LLM uses them verbatim as
    # genie_space_configs keys instead of inventing its own titles.
    configured_space_names: list[str] | None = None
    if not args.tables:
        _spaces = auth_cfg.get("genie_spaces", [])
        if target_space_cfg is not None:
            # Per-space mode: only the target space name matters
            _spaces = [target_space_cfg]
        names = [s.get("name") for s in _spaces if s.get("name")]
        if names:
            configured_space_names = names

    overlay_detection_comments = ""
    if industries:
        _, overlay_detection_comments = build_industry_detection_guidance(
            ddl_text,
            industries,
        )

    prompt = build_prompt(
        ddl_text,
        catalog_schemas=catalog_schemas,
        group_names=group_names,
        per_space_name=args.space if args.space else None,
        space_names=configured_space_names,
        mode=args.mode,
        countries=countries,
        industries=industries,
        create_groups=args.create_groups,
    )

    if args.dry_run:
        print("=" * 60)
        print("  DRY RUN — Prompt that would be sent:")
        print("=" * 60)
        print(prompt)
        sys.exit(0)

    # Resolve the sensitivity source before any model call. When classification
    # is enabled, native class.* is required unless the operator explicitly opts
    # out; a footprint whose tags haven't landed yet stops here, with the agent's
    # discovered tables already persisted above and no model call spent.
    classification_source = None
    if args.mode != "genie":
        native_expected = bool(auth_cfg.get("enable_classification"))
        try:
            classification_source = _fetch_live_classification_source(
                table_refs,
                auth_cfg,
                require_native=native_expected and not args.allow_llm_sensitivity,
            )
        except NativeClassificationRequiredError as exc:
            for line in native_classification_stop(exc, table_refs or [], WORK_DIR.name):
                print(line)
            sys.exit(1)

    # ── Reviewed rules from a prior run stick (additive merge after drafting) ──
    # The assembled generated/ draft is the reviewed rule set, also in --space
    # mode. Genie mode drafts no rules, so it has nothing to merge.
    from scripts.merge_space_configs import (
        extract_function_names, footprint_from_ddl, keep_reviewed_group_descriptions,
        keep_reviewed_rules, load_reviewed_rules, prune_unused_new_functions,
    )
    reviewed_rules = None
    # Functions the masking SQL defined before this run; only ones added since
    # are pruned when no policy uses them.
    _prior_sql_path = Path(args.out_dir).resolve() / "masking_functions.sql"
    try:
        prior_function_names = extract_function_names(_prior_sql_path.read_text())
    except FileNotFoundError:
        prior_function_names = set()
    if args.mode != "genie":
        try:
            reviewed_rules = load_reviewed_rules(Path(args.out_dir).resolve())
        except ValueError as e:
            if not args.allow_rule_changes:
                print(f"ERROR: {e}")
                sys.exit(1)

    if args.provider == "databricks":
        configure_databricks_env(auth_cfg)

    provider_cfg = PROVIDERS[args.provider]
    model = args.model or provider_cfg["default_model"]
    call_fn = provider_cfg["call"]

    _semantic_retry_count = 0
    _semantic_max_retries = args.max_retries

    response_text = call_with_retries(call_fn, prompt, model, args.max_retries)

    sql_block, hcl_block = extract_code_blocks(response_text)

    if not sql_block:
        print("\nWARNING: Could not extract SQL code block from the response.")
        print("  The full response will be saved to generated_response.md for manual extraction.\n")
    if not hcl_block:
        print("\nWARNING: Could not extract HCL code block from the response.")
        print("  The full response will be saved to generated_response.md for manual extraction.\n")

    out_dir.mkdir(parents=True, exist_ok=True)

    response_path = out_dir / "generated_response.md"
    response_path.write_text(response_text)
    print(f"\n  Full LLM response saved to: {response_path}")

    tuning_md = f"""# Review & Tune (Before Apply)

This folder contains a **first draft** of:
- `masking_functions.sql` — masking UDFs + row filter functions
- `abac.auto.tfvars` — groups, tags, FGAC policies, and Genie agent config

Before you apply, tune for your business roles, security requirements, and Genie accuracy:

## Checklist — Genie Accuracy (review first)

- **Benchmarks**: Each benchmark question must be **unambiguous and self-contained**. The natural-language question and its ground-truth SQL must agree on the exact scope — e.g., "What is the average risk score for **active** customers?" (not "What is the average customer risk score?"). Run benchmarks in the Genie UI after apply to verify accuracy.
- **SQL filters**: Do the default WHERE clauses match your business definitions? (e.g., "active customers" = `CustomerStatus = 'Active'`, "completed transactions" = `TransactionStatus = 'Completed'`). These filters guide Genie's SQL generation.
- **SQL measures**: Are the standard metrics correct? (e.g., total revenue = `SUM(Amount)`, average risk = `AVG(RiskScore)`).
- **SQL expressions**: Are the computed dimensions useful? (e.g., transaction year, age bucket).
- **Join specs**: Do the join conditions between tables use the correct keys? Incorrect joins cause wrong results across all multi-table queries.
- **Instructions**: Does the instruction text define business defaults (e.g., "customer" means active by default) and domain conventions (date handling, metric calculations)?

## Checklist — ABAC & Masking

- **Groups and personas**: Do the groups map to real business roles?
- **Sensitive columns**: Are the right columns tagged (PII/PHI/financial/etc.)?
- **Masking behavior**: Are you using the right approach (partial, redact, hash) per sensitivity and use case?
- **Row filters and exceptions**: Are filters too broad/strict? Are exceptions minimal and intentional?

## Checklist — Genie agent Metadata & ACLs

- **Genie title & description**: Does the AI-generated title/description accurately represent the space?
- **Genie sample questions**: Do the sample questions reflect what business users will ask?
- **Per-space ACLs (`acl_groups`)**: Each space lists which groups get `CAN_RUN` access. Verify that:
  - Each space includes all groups that need access
  - Groups that should NOT see this space are excluded
  - Set durable per-agent `acl_groups` only on `genie_spaces[]` in `env.auto.tfvars`
  - Explicit `acl_groups = []` means nobody; omission derives fresh from policies
  - ACL fields in the generated draft are ignored
- **Validate before apply**: Run validation before `terraform apply`.

## Suggested workflow

1. Review and edit `masking_functions.sql` and `abac.auto.tfvars` in `generated/`.
2. Validate after each change:
   ```bash
   make validate-generated
   ```
3. When ready, apply (validates again, promotes shared account + workspace config, then runs terraform):
   ```bash
   make apply
   ```

"""

    tuning_path = out_dir / "TUNING.md"
    tuning_path.write_text(tuning_md)
    print(f"  Tuning checklist written to: {tuning_path}")

    if sql_block and args.mode == "genie":
        # Genie mode: masking functions are owned by the governance team; discard SQL output.
        print("  [genie mode] Skipping masking_functions.sql (managed by governance team)")
        sql_block = None

    all_cs = catalog_schemas if catalog_schemas else [(catalog, schema)] if catalog and schema else []

    if sql_block:
        targets = ", ".join(f"{c}.{s}" for c, s in all_cs)
        sql_header = (
            "-- ============================================================================\n"
            "-- GENERATED MASKING FUNCTIONS (FIRST DRAFT)\n"
            "-- ============================================================================\n"
            f"-- Target(s): {targets}\n"
            "-- Next: review generated/TUNING.md, tune if needed, then run this SQL.\n"
            "-- ============================================================================\n\n"
        )

        # Ensure USE CATALOG/USE SCHEMA directives are present at the top.
        # The LLM may omit them, causing deploy_masking_functions.py to
        # deploy into the default catalog (main) which may not exist.
        sql_prefix = ""
        if all_cs and not re.search(r"USE\s+CATALOG\s+\S+", sql_block, re.IGNORECASE):
            cat0, sch0 = all_cs[0]
            sql_prefix = f"USE CATALOG {cat0};\nUSE SCHEMA {sch0};\n\n"

        final_sql = sql_header + sql_prefix + sql_block
        sql_path = out_dir / "masking_functions.sql"
        sql_path.write_text(final_sql + "\n")
        print(f"  masking_functions.sql written to: {sql_path}")
        print(f"    Target schemas: {targets}")

    if hcl_block:
        if args.mode == "genie" and champion_flow:
            hcl_header = (
                "# ============================================================================\n"
                "# IMPORTED GENIE AGENT CONFIG (walkthrough Dev section — genie mode)\n"
                "# ============================================================================\n"
                "# Only genie_space_configs is produced in this mode. The protection rules\n"
                "# are drafted later in the Dev section by: make generate ENV=" + WORK_DIR.name + "\n"
                "# ============================================================================\n\n"
            )
        elif args.mode == "genie":
            hcl_header = (
                "# ============================================================================\n"
                "# GENERATED GENIE CONFIG (FIRST DRAFT — genie mode)\n"
                "# ============================================================================\n"
                "# NOTE: ABAC governance (groups, tag policies, tag assignments, masking\n"
                "# functions) is owned by the central Data Governance team and is NOT\n"
                "# generated here.  Only genie_space_configs is produced in this mode.\n"
                "# Tune the following before apply:\n"
                "# - genie_space_configs (titles, instructions, sample questions, SQL)\n"
                "# Then run: make apply-genie ENV=...\n"
                "# ============================================================================\n\n"
            )
        else:
            hcl_header = (
                "# ============================================================================\n"
                "# GENERATED ABAC CONFIG (FIRST DRAFT)\n"
                "# ============================================================================\n"
                "# NOTE: Authentication comes from auth.auto.tfvars, environment from env.auto.tfvars.\n"
                "# Tune the following before apply:\n"
                "# - groups (business roles)\n"
                "# - tag_assignments (what data is considered sensitive)\n"
                "# - fgac_policies (who sees what, and how)\n"
                "# Then validate before promoting into shared account + workspace config:\n"
                "#   python validate_abac.py generated/abac.auto.tfvars generated/masking_functions.sql\n"
                "# ============================================================================\n\n"
            )

        hcl_block = sanitize_tfvars_hcl(hcl_block)
        trimmed_hcl, genie_tail_repairs = _trim_incomplete_genie_tail(hcl_block)
        if genie_tail_repairs:
            hcl_block = trimmed_hcl
            print(
                "  [AUTOFIX] Trimmed incomplete trailing Genie section from generated tfvars"
            )

        # ── Mode-based output filtering ───────────────────────────────────────
        if args.mode == "governance":
            # Strip genie_space_configs — BU teams manage Genie content independently.
            hcl_block = remove_hcl_top_level_block(hcl_block, "genie_space_configs")
            print("  [governance mode] Stripped genie_space_configs from output")
        elif args.mode == "genie":
            # Strip all ABAC sections — governance team manages them centrally.
            hcl_block = strip_abac_for_genie_mode(hcl_block)
            # Remove any LLM-generated comment placeholders for the omitted sections
            # (e.g. "# tag_assignments = [] — managed centrally").  The LLM sometimes
            # acknowledges suppressed sections via commented-out examples despite the
            # prompt instructions.
            _abac_comment_re = re.compile(
                r"^#[^\n]*(tag_assignments|fgac_policies|tag_policies|group_members)[^\n]*\n",
                re.MULTILINE,
            )
            hcl_block = _abac_comment_re.sub("", hcl_block)
            print("  [genie mode] Stripped ABAC sections from output (groups, tag_policies, tag_assignments, fgac_policies)")
            print("  [genie mode] Tip: set genie_only = true in env.auto.tfvars for least-privilege SP access (Workspace Admin only)")

        # ── Strip legacy Genie keys according to environment ownership ────────
        # The LLM sometimes hallucinates legacy single-space keys (genie_space_title,
        # genie_space_description, etc.) even when env.auto.tfvars has no genie_spaces.
        # Strip them to prevent Terraform from creating an unexpected Genie agent.
        _configured_spaces = auth_cfg.get("genie_spaces", [])
        if _configured_spaces:
            _before = hcl_block
            hcl_block = strip_multi_space_legacy_genie_keys(hcl_block)
            if hcl_block != _before:
                print("  [auto-strip] Removed flat legacy Genie fields from multi-space draft")
        elif args.mode not in ("genie",) and not args.space:
            _legacy_genie_block_keys = (
                "genie_space_configs",
                "genie_benchmarks",
                "genie_sql_filters",
                "genie_sql_expressions",
                "genie_sql_measures",
                "genie_join_specs",
            )
            _legacy_genie_list_keys = (
                "genie_sample_questions",
                "genie_acl_groups",
            )
            # Scalar string assignments (genie_space_title = "...", etc.)
            _legacy_genie_scalar_re = re.compile(
                r'^\s*(?:genie_space_title|genie_space_description|genie_instructions)\s*=\s*"[^"]*"\s*$',
                re.MULTILINE,
            )
            _stripped_any = False
            for _key in _legacy_genie_block_keys:
                _before = hcl_block
                hcl_block = remove_hcl_top_level_block(hcl_block, _key)
                if hcl_block != _before:
                    _stripped_any = True
            for _key in _legacy_genie_list_keys:
                _before = hcl_block
                hcl_block = remove_hcl_top_level_list(hcl_block, _key)
                if hcl_block != _before:
                    _stripped_any = True
            _before = hcl_block
            hcl_block = _legacy_genie_scalar_re.sub("", hcl_block)
            if hcl_block != _before:
                _stripped_any = True
            if _stripped_any:
                print("  [auto-strip] Removed LLM-generated Genie config (no genie_spaces configured in env.auto.tfvars)")

        # ── Inject API-parsed genie_space_configs for existing spaces ─────────
        # The LLM generates genie_space_configs from DDL, but for spaces with a
        # genie_space_id the UI config is authoritative. Replace the LLM-generated
        # block with the verbatim parse from the Genie agent API.
        # Skip in governance mode — genie_space_configs is managed by BU teams.
        if api_genie_configs and args.mode != "governance":
            reviewed_genie_text = (
                (out_dir / "abac.auto.tfvars").read_text()
                if (out_dir / "abac.auto.tfvars").exists() else ""
            )
            kept_genie_fields = keep_reviewed_genie_content(
                out_dir / "abac.auto.tfvars", api_genie_configs
            )
            if kept_genie_fields:
                print(
                    "  Kept reviewed Genie benchmarks/snippets: "
                    + ", ".join(kept_genie_fields)
                )
            hcl_block = remove_hcl_top_level_block(hcl_block, "genie_space_configs")
            injected_hcl = (
                "\n# genie_space_configs parsed verbatim from the existing Genie agent(s).\n"
                "# Edit here to manage space config as code; make apply pushes changes back.\n"
                + format_genie_space_configs_hcl(api_genie_configs)
            )
            injected_hcl = restore_reviewed_genie_text(
                reviewed_genie_text, injected_hcl, list(api_genie_configs)
            )
            hcl_block = hcl_block.rstrip() + "\n" + injected_hcl + "\n"
            print(
                f"  Injected genie_space_configs from Genie API for: "
                f"{', '.join(api_genie_configs)}"
            )

        if genie_space_id_to_name and args.mode != "governance":
            hcl_block = remove_hcl_top_level_block(hcl_block, "genie_space_id_to_name")
            hcl_block = (
                hcl_block.rstrip()
                + "\n\n# Tool-owned canonical identity for id-only imported spaces.\n"
                + format_string_map_hcl("genie_space_id_to_name", genie_space_id_to_name)
                + "\n"
            )

        tfvars_path = out_dir / "abac.auto.tfvars"
        extra_comments = overlay_detection_comments if overlay_detection_comments else ""
        tfvars_path.write_text(hcl_header + extra_comments + hcl_block + "\n")
        if _configured_spaces:
            n_draft_acls = strip_draft_genie_acl_fields(tfvars_path)
            if n_draft_acls:
                print(
                    f"  [auto-strip] Removed {n_draft_acls} model/API ACL field(s); "
                    "set durable ACLs on genie_spaces[] in env.auto.tfvars"
                )
        print(f"  abac.auto.tfvars written to: {tfvars_path}")

        fix_hcl_syntax(tfvars_path)
        if _configured_spaces:
            strip_draft_genie_acl_fields(tfvars_path)

        n_canonical = autofix_canonical_tag_vocabulary(tfvars_path)
        if n_canonical:
            print(f"  Auto-fixed: normalized {n_canonical} tag vocabulary reference(s)")

        n_normalized = autofix_ambiguous_tag_values(tfvars_path)
        if n_normalized:
            print(f"  Auto-fixed: normalized {n_normalized} ambiguous tag_assignment value(s)")

        # Run autofix_invalid_tag_values BEFORE autofix_tag_policies so that
        # LLM typos (e.g. "masked_card" when the policy defines "masked_card_last4")
        # are removed rather than being promoted into the policy's allowed-values list
        # by the subsequent autofix_tag_policies call.
        n_bad_vals = autofix_invalid_tag_values(tfvars_path)
        if n_bad_vals:
            print(f"  Auto-fixed: removed {n_bad_vals} tag_assignment(s) with invalid tag_value(s)")

        # autofix_tag_policies adds values that are genuinely used in assignments
        # but were accidentally omitted from the policy definition.  Running it
        # after autofix_invalid_tag_values ensures it only promotes real values,
        # not LLM typos that were already stripped above.
        n_fixed = autofix_tag_policies(tfvars_path)
        if n_fixed:
            print(f"  Auto-fixed {n_fixed} missing tag_policy value(s)")

        n_undef = autofix_undefined_tag_refs(tfvars_path)
        if n_undef:
            print(f"  Auto-fixed: removed {n_undef} item(s) referencing undefined tag_key(s)")

        # Strip bodyless CREATE FUNCTION statements (LLM sometimes emits a
        # signature with no RETURN). They fail Databricks deploy with "SQL
        # functions should have a function definition" — must run BEFORE
        # overlay injection so the overlay's correct version replaces them.
        n_bodyless = autofix_remove_bodyless_functions(sql_path if sql_block else None)
        if n_bodyless:
            print(f"  Auto-fixed: removed {n_bodyless} bodyless CREATE FUNCTION statement(s)")

        # Inject overlay-provided functions BEFORE adding missing FGAC policies,
        # so that row filter functions (e.g. filter_aml_compliance) are available
        # when autofix_missing_fgac_policies looks for a function to cover
        # uncovered tag assignments.
        n_overlay_fns = autofix_inject_overlay_functions(
            sql_path if sql_block else None,
            countries=countries, industries=industries,
            catalog_schemas=catalog_schemas,
        )
        if n_overlay_fns:
            print(f"  Auto-fixed: injected {n_overlay_fns} overlay-provided masking function(s)")

        # Skip PII autofix in genie mode — tag_assignments are managed by the governance team
        if args.mode != "genie":
            n_pii_tags = autofix_untagged_pii_columns(
                tfvars_path,
                ddl_path=out_dir / "ddl" / "_fetched.sql" if out_dir else None,
                sql_path=sql_path if sql_block else None,
                classification_source=classification_source,
                agent_footprint=agent_footprint,
                authoritative_classification=classification_source is not None,
            )
            if n_pii_tags:
                print(f"  Auto-fixed: added {n_pii_tags} tag_assignment(s) for untagged PII columns")
                # PII autofix may introduce new tag keys (e.g. financial_sensitivity)
                # that don't exist in tag_policies yet.  Run tag_policies autofix now
                # so the new keys/values are registered before missing-policy detection.
                n_pii_tp = autofix_tag_policies(tfvars_path)
                if n_pii_tp:
                    print(f"  Auto-fixed: added {n_pii_tp} tag_policy value(s) for PII-detected tags")

        # Repair HCL before missing-policy detection — autofix_untagged_pii_columns
        # inserts tag_assignments via regex which can leave missing commas between
        # the last existing entry and the new entries, causing hcl2 parse failures.
        fix_hcl_syntax(tfvars_path)

        n_repaired = autofix_missing_fgac_policies(tfvars_path, sql_path if sql_block else None)
        if n_repaired:
            print(f"  Auto-fixed: added {n_repaired} fgac_policy/ies for uncovered sensitive tags")

        # Second pass: autofix_missing_fgac_policies (above) may have injected
        # new hasTagValue() conditions referencing values that weren't in the
        # original tag_policies.  Re-run autofix_tag_policies to pick them up.
        n_fixed_2 = autofix_tag_policies(tfvars_path)
        if n_fixed_2:
            print(f"  Auto-fixed {n_fixed_2} additional missing tag_policy value(s) (second pass)")

        # Skip Genie config autofixes in governance mode — genie_space_configs
        # was already stripped and these autofixes would re-add it.
        if args.mode != "governance":
            n_fields = autofix_genie_config_fields(tfvars_path)
            if n_fields:
                print(f"  Auto-fixed: added {n_fields} missing required field(s) in genie_space_configs")

            n_missing_spaces = autofix_missing_genie_space_entries(tfvars_path, auth_cfg)
            if n_missing_spaces:
                print(f"  Auto-fixed: added {n_missing_spaces} missing genie_space_configs entr(y/ies)")

            if target_space_cfg is None:
                env_tfvars = tfvars_path.parent.parent / "env.auto.tfvars"
                n_acl = autofix_acl_groups(
                    tfvars_path,
                    env_tfvars if env_tfvars.exists() else None,
                    defer_unmapped=args.mode == "genie",
                )
                if n_acl:
                    print(f"  Derived ACL sidecar for {n_acl} Genie agent(s)")

        if args.mode != "genie":
            n_treatments, n_native_sources = derive_and_finalize_treatments(
                tfvars_path,
                native_authoritative=classification_source is not None,
            )
            if n_treatments:
                print(f"  Derived GenieRails enforcement treatments ({n_treatments} change(s))")
            if n_native_sources:
                print(
                    "  Native classification authoritative: removed "
                    f"{n_native_sources} redundant source-family assignment(s)"
                )

            ensure_derived_treatment_functions(
                tfvars_path, sql_path if sql_block else None,
            )

        # Check the hard UC quota after Option-B has collapsed masks to one
        # policy per treatment/catalog. Never delete policies to fit the cap.
        autofix_fgac_policy_count(tfvars_path)

        n_fn_canonical = autofix_canonical_function_names(tfvars_path, sql_path if sql_block else None)
        if n_fn_canonical:
            print(f"  Auto-fixed: normalized {n_fn_canonical} function name(s) to canonical forms")

        n_fn_refs = autofix_invalid_function_refs(tfvars_path, sql_path if sql_block else None)
        if n_fn_refs:
            print(f"  Auto-fixed: corrected {n_fn_refs} invalid function reference(s) in fgac_policies")

        n_arg_mismatch = autofix_fgac_arg_count_mismatch(tfvars_path, sql_path if sql_block else None)
        if n_arg_mismatch:
            print(f"  Auto-fixed: removed {n_arg_mismatch} fgac_policy/ies with function arg count mismatch")

        n_bad_col_refs = autofix_row_filter_column_refs(tfvars_path, sql_path if sql_block else None)
        if n_bad_col_refs:
            print(f"  Auto-fixed: removed {n_bad_col_refs} row filter policy/ies referencing non-existent columns")

        n_cat_mismatch = autofix_function_category_mismatch(tfvars_path, sql_path if sql_block else None)
        if n_cat_mismatch:
            print(f"  Auto-fixed: corrected {n_cat_mismatch} function/category mismatch(es) in fgac_policies")

        # Remove policies that reference type-specific tags (e.g. financial_sensitivity)
        # when the required masking function isn't available. The LLM sometimes generates
        # these tags even without the corresponding overlay — leave them uncovered so
        # the cleanup removes both the policy and the tag assignment.
        # Repair HCL first — intermediate autofixes may have introduced syntax issues
        # that would cause hcl2.loads() to fail silently in the cleanup below.
        fix_hcl_syntax(tfvars_path)
        _avail = _parse_sql_function_names(sql_path if sql_block else None)
        # Map: (tag_key, tag_value) → required function.
        # If the function isn't available, remove both policies AND tag assignments.
        _type_tag_fn_map = [
            ("financial_sensitivity", "rounded_amounts", "mask_amount_rounded"),
            ("pii_level", "masked_dob", "mask_date_to_year"),
        ]
        for _tag_key, _tag_val, _required_fn in _type_tag_fn_map:
            if _avail and _required_fn not in _avail:
                try:
                    import hcl2 as _hcl_tc
                    _tc_cfg = _hcl_tc.loads(tfvars_path.read_text())
                    _tc_text = tfvars_path.read_text()
                    _tc_removed = 0
                    # Remove policies referencing this tag — use brace-counting
                    # (nested blocks like column_mask = { ... } break [^}]* regex)
                    for _p in _tc_cfg.get("fgac_policies", []):
                        _cond = (_p.get("match_condition", "") or "") + " " + (_p.get("when_condition", "") or "")
                        if _tag_val in _cond:
                            _pname = _p.get("name", "")
                            if _pname:
                                _sec = _find_bracket_section(_tc_text, "fgac_policies")
                                if _sec:
                                    _sec_start, _sec_end = _sec
                                    _sec_txt = _tc_text[_sec_start:_sec_end]
                                    _blks = _find_brace_blocks(_sec_txt)
                                    for _bs, _be in reversed(_blks):
                                        _bt = _sec_txt[_bs:_be + 1]
                                        if re.search(r'name\s*=\s*"' + re.escape(_pname) + r'"', _bt):
                                            # Determine removal range including trailing comma/whitespace
                                            _abs_s = _sec_start + _bs
                                            _abs_e = _sec_start + _be + 1
                                            while _abs_e < len(_tc_text) and _tc_text[_abs_e] in (",", " ", "\t"):
                                                _abs_e += 1
                                            while _abs_s > 0 and _tc_text[_abs_s - 1] in (" ", "\t"):
                                                _abs_s -= 1
                                            if _abs_s > 0 and _tc_text[_abs_s - 1] == "\n":
                                                _abs_s -= 1
                                            _tc_text = _tc_text[:_abs_s] + _tc_text[_abs_e:]
                                            _tc_removed += 1
                                            break
                    # Always remove tag assignments for this tag_key+tag_value
                    # (even if 0 policies removed — LLM may generate tags without policies)
                    _ta_sec = _find_bracket_section(_tc_text, "tag_assignments")
                    _ta_removed = False
                    if _ta_sec:
                        _ta_start, _ta_end = _ta_sec
                        _ta_sec_txt = _tc_text[_ta_start:_ta_end]
                        _ta_blks = _find_brace_blocks(_ta_sec_txt)
                        for _bs, _be in reversed(_ta_blks):
                            _bt = _ta_sec_txt[_bs:_be + 1]
                            if (re.search(r'tag_key\s*=\s*"' + re.escape(_tag_key) + r'"', _bt)
                                    and re.search(r'tag_value\s*=\s*"' + re.escape(_tag_val) + r'"', _bt)):
                                _abs_s = _ta_start + _bs
                                _abs_e = _ta_start + _be + 1
                                while _abs_e < len(_tc_text) and _tc_text[_abs_e] in (",", " ", "\t"):
                                    _abs_e += 1
                                while _abs_s > 0 and _tc_text[_abs_s - 1] in (" ", "\t"):
                                    _abs_s -= 1
                                if _abs_s > 0 and _tc_text[_abs_s - 1] == "\n":
                                    _abs_s -= 1
                                _tc_text = _tc_text[:_abs_s] + _tc_text[_abs_e:]
                                _ta_removed = True
                    if _tc_removed or _ta_removed:
                        tfvars_path.write_text(_tc_text)
                        print(f"  [AUTOFIX] Removed {_tag_key}={_tag_val} policies/tags (function '{_required_fn}' not in SQL)")
                except Exception:
                    pass

        # Repair HCL syntax before condition-checking autofixes — intermediate
        # autofixes (e.g. autofix_missing_fgac_policies) may introduce issues
        # that cause hcl2.loads() to fail, silently skipping these checks.
        fix_hcl_syntax(tfvars_path)

        n_dup_masks = autofix_duplicate_column_masks(tfvars_path)
        if n_dup_masks:
            print(f"  Auto-fixed: removed {n_dup_masks} duplicate column mask policy/ies")

        n_forbidden = autofix_forbidden_conditions(tfvars_path)
        if n_forbidden:
            print(f"  Auto-fixed: removed {n_forbidden} fgac_policy/ies with unsupported condition functions")

        n_bad_cond_vals = autofix_invalid_condition_values(tfvars_path)
        if n_bad_cond_vals:
            print(f"  Auto-fixed: removed {n_bad_cond_vals} fgac_policy/ies with invalid tag values in conditions")

        n_malformed = autofix_malformed_conditions(tfvars_path)
        if n_malformed:
            print(f"  Auto-fixed: removed {n_malformed} fgac_policy/ies with malformed conditions")

        n_unsafe_filters = autofix_unsafe_row_filters(sql_path if sql_block else None)
        if n_unsafe_filters:
            print(f"  Auto-fixed: rewrote {n_unsafe_filters} row filter(s) with hallucinated column references")

        n_cross_cat = autofix_cross_catalog_function_deployment(tfvars_path, sql_path if sql_block else None)
        if n_cross_cat:
            print(f"  Auto-fixed: deployed {n_cross_cat} function(s) to additional catalog.schema pairs")

        # Final cleanup: re-run invalid tag values + ambiguous to catch anything
        # introduced by intermediate autofixes (e.g. autofix_missing_fgac_policies).
        n_final_bad = autofix_invalid_tag_values(tfvars_path)
        if n_final_bad:
            print(f"  Auto-fixed (final pass): removed {n_final_bad} tag_assignment(s) with invalid values")
        n_final_ambig = autofix_ambiguous_tag_values(tfvars_path)
        if n_final_ambig:
            print(f"  Auto-fixed (final pass): normalized {n_final_ambig} ambiguous tag_assignment value(s)")
        n_final_canonical = autofix_canonical_tag_vocabulary(tfvars_path)
        if n_final_canonical:
            print(f"  Auto-fixed (final pass): normalized {n_final_canonical} tag vocabulary reference(s)")

        # Uncovered sensitive assignments remain intact: validation/coverage
        # must block rather than silently shrinking the protected surface.

        # ── Final governance mode safety strip ─────────────────────────────────
        # Multiple code paths (autofixes, semantic retries) can re-introduce
        # genie_space_configs after the initial strip. This final pass ensures
        # the file is clean before validation.
        if args.mode == "governance":
            _gov_text = tfvars_path.read_text()
            _gov_cleaned = remove_hcl_top_level_block(_gov_text, "genie_space_configs")
            if _configured_spaces:
                _gov_cleaned = strip_multi_space_legacy_genie_keys(_gov_cleaned)
            if _gov_cleaned != _gov_text:
                tfvars_path.write_text(_gov_cleaned)
                print("  [governance mode] Final strip: removed genie_space_configs re-introduced by autofixes")

        # ── Semantic quality check (catches LLM issues that autofix can't fix) ──
        semantic_errors, semantic_warnings = post_generate_semantic_check(tfvars_path, auth_cfg, mode=args.mode)
        if semantic_errors:
            _semantic_retry_count += 1
            if _semantic_retry_count < _semantic_max_retries:
                print(f"\n  [SEMANTIC CHECK FAILED] (attempt {_semantic_retry_count}/{_semantic_max_retries}):")
                for err in semantic_errors:
                    print(f"    - {err}")
                print(f"  Re-generating with LLM...")
                response_text = call_with_retries(call_fn, prompt, model, 1)
                new_sql, new_hcl = extract_code_blocks(response_text)
                # Write new SQL FIRST so that HCL autofixes can see the
                # correct set of available functions (including overlay injects).
                if new_sql and args.mode != "genie":
                    sql_block = new_sql
                    # Ensure USE CATALOG/USE SCHEMA present (LLM may omit)
                    if all_cs and not re.search(r"USE\s+CATALOG\s+\S+", sql_block, re.IGNORECASE):
                        cat0, sch0 = all_cs[0]
                        sql_block = f"USE CATALOG {cat0};\nUSE SCHEMA {sch0};\n\n" + sql_block
                    sql_path = out_dir / "masking_functions.sql"
                    sql_path.write_text(sql_block + "\n")
                if new_hcl:
                    hcl_block = new_hcl
                    # Re-apply mode-specific stripping on retry output
                    if args.mode == "governance":
                        hcl_block = remove_hcl_top_level_block(hcl_block, "genie_space_configs")
                    elif args.mode == "genie":
                        for _gk in ("groups", "tag_policies", "group_members"):
                            hcl_block = remove_hcl_top_level_block(hcl_block, _gk)
                        for _gk in ("tag_assignments", "fgac_policies"):
                            hcl_block = remove_hcl_top_level_list(hcl_block, _gk)
                    if genie_space_id_to_name and args.mode != "governance":
                        hcl_block = remove_hcl_top_level_block(hcl_block, "genie_space_id_to_name")
                        hcl_block = (
                            hcl_block.rstrip() + "\n\n"
                            + format_string_map_hcl("genie_space_id_to_name", genie_space_id_to_name)
                            + "\n"
                        )
                    extra_comments = overlay_detection_comments if overlay_detection_comments else ""
                    tfvars_path.write_text(hcl_header + extra_comments + hcl_block + "\n")
                    if _configured_spaces:
                        strip_draft_genie_acl_fields(tfvars_path)
                    fix_hcl_syntax(tfvars_path)
                    if _configured_spaces:
                        strip_draft_genie_acl_fields(tfvars_path)
                    autofix_canonical_tag_vocabulary(tfvars_path)
                    autofix_ambiguous_tag_values(tfvars_path)
                    autofix_invalid_tag_values(tfvars_path)
                    autofix_tag_policies(tfvars_path)
                    autofix_undefined_tag_refs(tfvars_path)
                    # Strip bodyless CREATE FUNCTION statements before
                    # overlay injection — see main path comment above.
                    autofix_remove_bodyless_functions(sql_path if sql_block else None)
                    # Re-inject overlay functions (retry LLM may omit them)
                    autofix_inject_overlay_functions(
                        sql_path if sql_block else None,
                        countries=countries, industries=industries,
                        catalog_schemas=catalog_schemas,
                    )
                    # Rewrite row filter fns with hallucinated column refs — must
                    # run before deploy to prevent UNRESOLVED_COLUMN errors.
                    autofix_unsafe_row_filters(sql_path if sql_block else None)
                    # Add PII tags for untagged columns (BEFORE missing_fgac_policies
                    # so the new tags get coverage).
                    if args.mode != "genie":
                        autofix_untagged_pii_columns(
                            tfvars_path,
                            ddl_path=out_dir / "ddl" / "_fetched.sql" if out_dir else None,
                            sql_path=sql_path if sql_block else None,
                            classification_source=classification_source,
                            agent_footprint=agent_footprint,
                            authoritative_classification=classification_source is not None,
                        )
                        autofix_tag_policies(tfvars_path)  # register new PII tag values
                    autofix_missing_fgac_policies(tfvars_path, sql_path if sql_block else None)
                    if args.mode != "governance":
                        autofix_genie_config_fields(tfvars_path)
                        # CRITICAL: Ensure every configured genie_space has a
                        # genie_space_configs entry. Retry LLM output may drop
                        # space names that the test assertion checks for.
                        autofix_missing_genie_space_entries(tfvars_path, auth_cfg)
                        if target_space_cfg is None:
                            env_tfvars = tfvars_path.parent.parent / "env.auto.tfvars"
                            autofix_acl_groups(
                                tfvars_path,
                                env_tfvars if env_tfvars.exists() else None,
                            )
                    if args.mode != "genie":
                        derive_and_finalize_treatments(
                            tfvars_path,
                            native_authoritative=classification_source is not None,
                        )
                        ensure_derived_treatment_functions(
                            tfvars_path, sql_path if sql_block else None,
                        )
                    autofix_fgac_policy_count(tfvars_path)
                    autofix_canonical_function_names(tfvars_path, sql_path if sql_block else None)
                    autofix_invalid_function_refs(tfvars_path, sql_path if sql_block else None)
                    autofix_fgac_arg_count_mismatch(tfvars_path, sql_path if sql_block else None)
                    autofix_row_filter_column_refs(tfvars_path, sql_path if sql_block else None)
                    autofix_function_category_mismatch(tfvars_path, sql_path if sql_block else None)
                    # Repair HCL before condition autofixes (same as main path)
                    fix_hcl_syntax(tfvars_path)
                    autofix_duplicate_column_masks(tfvars_path)
                    autofix_forbidden_conditions(tfvars_path)
                    autofix_invalid_condition_values(tfvars_path)
                    autofix_malformed_conditions(tfvars_path)
                    # Deploy overlay-injected fns cross-catalog (multi-catalog support)
                    autofix_cross_catalog_function_deployment(tfvars_path, sql_path if sql_block else None)
                    # Preserve uncovered assignments for the blocking gate.
                    # Final governance strip after retry autofixes
                    if args.mode == "governance":
                        _retry_text = tfvars_path.read_text()
                        _retry_cleaned = remove_hcl_top_level_block(_retry_text, "genie_space_configs")
                        if _configured_spaces:
                            _retry_cleaned = strip_multi_space_legacy_genie_keys(_retry_cleaned)
                        if _retry_cleaned != _retry_text:
                            tfvars_path.write_text(_retry_cleaned)
                            print("  [governance mode] Final strip (retry): removed genie_space_configs")
                # Re-check after retry
                semantic_errors, semantic_warnings = post_generate_semantic_check(tfvars_path, auth_cfg, mode=args.mode)
            # Critical errors (empty governance output) should NOT be downgraded
            # to warnings — exit with failure so the outer retry loop can kick in.
            _CRITICAL_PATTERNS = [
                "0 tag_assignments and 0 fgac_policies",
                "SQL output is incomplete",
            ]
            _critical_errors = [
                e for e in (semantic_errors or [])
                if any(p in e for p in _CRITICAL_PATTERNS)
            ]
            _non_critical_errors = [
                e for e in (semantic_errors or [])
                if not any(p in e for p in _CRITICAL_PATTERNS)
            ]
            if _critical_errors:
                print(f"\n  [SEMANTIC CHECK FAILED] Critical errors after {_semantic_retry_count + 1} attempt(s):")
                for err in _critical_errors:
                    print(f"    - {err}")
                print(f"  Exiting with failure — LLM produced incomplete output.")
                sys.exit(1)
            all_warnings = (semantic_warnings or []) + _non_critical_errors
            if all_warnings:
                print(f"\n  [SEMANTIC CHECK] Warnings after {_semantic_retry_count + 1} attempt(s):")
                for w in all_warnings:
                    print(f"    - {w}")
                print(f"  Proceeding with best effort.")

        # ── Per-space mode: bootstrap per-space dir, then merge into assembled ──
        if target_space_cfg is not None and space_key:
            # The per-space dir is already out_dir; merge its content into
            # the assembled generated/abac.auto.tfvars one level up.
            assembled_dir = out_dir.parent.parent  # generated/spaces/<key>/../.. = generated/
            merge_script = SCRIPT_DIR / "scripts" / "merge_space_configs.py"
            subprocess.check_call(
                [sys.executable, str(merge_script), str(assembled_dir), space_key]
            )
            # Safety-net autofix on the assembled abac in case the merge
            # introduced any cross-space tag_key inconsistencies.
            assembled_abac_path = assembled_dir / "abac.auto.tfvars"
            if assembled_abac_path.exists():
                assembled_sql_path = assembled_dir / "masking_functions.sql"
                n_canonical_assembled = autofix_canonical_tag_vocabulary(assembled_abac_path)
                if n_canonical_assembled:
                    print(f"  Auto-fixed assembled abac: normalized {n_canonical_assembled} tag vocabulary reference(s)")
                n_normalized_assembled = autofix_ambiguous_tag_values(assembled_abac_path)
                if n_normalized_assembled:
                    print(f"  Auto-fixed assembled abac: normalized {n_normalized_assembled} ambiguous tag_assignment value(s)")
                n_bad_vals_assembled = autofix_invalid_tag_values(assembled_abac_path)
                if n_bad_vals_assembled:
                    print(f"  Auto-fixed assembled abac: removed {n_bad_vals_assembled} tag_assignment(s) with invalid tag_value(s)")
                n_fixed_assembled = autofix_tag_policies(assembled_abac_path)
                if n_fixed_assembled:
                    print(f"  Auto-fixed assembled abac: added {n_fixed_assembled} missing tag_policy value(s)")
                n_undef_assembled = autofix_undefined_tag_refs(assembled_abac_path)
                if n_undef_assembled:
                    print(f"  Auto-fixed assembled abac: removed {n_undef_assembled} item(s) referencing undefined tag_key(s)")
                n_repaired_assembled = autofix_missing_fgac_policies(
                    assembled_abac_path,
                    assembled_sql_path if assembled_sql_path.exists() else None,
                )
                if n_repaired_assembled:
                    print(f"  Auto-fixed assembled abac: added {n_repaired_assembled} fgac_policy/ies for uncovered sensitive tags")
                autofix_fgac_policy_count(assembled_abac_path)
                n_fields_assembled = autofix_genie_config_fields(assembled_abac_path)
                if n_fields_assembled:
                    print(f"  Auto-fixed assembled abac: added {n_fields_assembled} missing required field(s) in genie_space_configs")
                n_missing_spaces_assembled = autofix_missing_genie_space_entries(assembled_abac_path, auth_cfg)
                if n_missing_spaces_assembled:
                    print(f"  Auto-fixed assembled abac: added {n_missing_spaces_assembled} missing genie_space_configs entr(y/ies)")
                n_fn_refs_assembled = autofix_invalid_function_refs(
                    assembled_abac_path,
                    assembled_sql_path if assembled_sql_path.exists() else None,
                )
                if n_fn_refs_assembled:
                    print(f"  Auto-fixed assembled abac: corrected {n_fn_refs_assembled} invalid function reference(s)")
                n_cat_mismatch_assembled = autofix_function_category_mismatch(
                    assembled_abac_path,
                    assembled_sql_path if assembled_sql_path.exists() else None,
                )
                if n_cat_mismatch_assembled:
                    print(f"  Auto-fixed assembled abac: corrected {n_cat_mismatch_assembled} function/category mismatch(es)")
                n_arg_assembled = autofix_fgac_arg_count_mismatch(
                    assembled_abac_path,
                    assembled_sql_path if assembled_sql_path.exists() else None,
                )
                if n_arg_assembled:
                    print(f"  Auto-fixed assembled abac: fixed {n_arg_assembled} function/arg-count mismatch(es)")
                n_bad_col_assembled = autofix_row_filter_column_refs(
                    assembled_abac_path,
                    assembled_sql_path if assembled_sql_path.exists() else None,
                )
                if n_bad_col_assembled:
                    print(f"  Auto-fixed assembled abac: removed {n_bad_col_assembled} row filter(s) with bad column refs")
                # Final HCL syntax pass on assembled config
                fix_hcl_syntax(assembled_abac_path)
                assembled_env_tfvars = assembled_dir.parent / "env.auto.tfvars"
                n_acl_assembled = autofix_acl_groups(
                    assembled_abac_path,
                    assembled_env_tfvars if assembled_env_tfvars.exists() else None,
                )
                if n_acl_assembled:
                    print(
                        "  Derived assembled ACL sidecar for "
                        f"{n_acl_assembled} Genie agent(s)"
                    )

        # ── Full generation: bootstrap per-space dirs from the assembled output ─
        elif target_space_cfg is None and not args.space:
            # Re-run syntax fix — autofixes above may have re-introduced
            # missing commas that would cause the bootstrap HCL parse to fail.
            if tfvars_path.exists():
                fix_hcl_syntax(tfvars_path)
            _bootstrap_text = tfvars_path.read_text() if tfvars_path.exists() else hcl_block
            bootstrap_per_space_dirs(out_dir, auth_cfg, _bootstrap_text)

    # In per-space mode, validate the assembled generated/ dir (what apply uses),
    # not the per-space subdirectory.
    validation_dir = out_dir.parent.parent if (target_space_cfg is not None and space_key) else out_dir

    if reviewed_rules is not None and hcl_block:
        rules_abac = validation_dir / "abac.auto.tfvars"
        try:
            # Reviewed rules for tables/columns gone from this run's DDL are
            # stale. SPACE= and --tables scan only part of the footprint, so
            # there only the scanned tables are checked.
            rule_messages = keep_reviewed_rules(
                reviewed_rules, rules_abac, validation_dir / "masking_functions.sql",
                footprint=footprint_from_ddl(ddl_text),
                partial_footprint=bool(args.space or args.tables),
                allow_changes=args.allow_rule_changes,
            )
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)
        for line in rule_messages:
            print(line)
        if rule_messages and not args.allow_rule_changes:
            fix_hcl_syntax(rules_abac)
            if args.mode != "governance":
                # Restored policies feed the derived Genie ACL sidecar.
                rules_env = validation_dir.parent / "env.auto.tfvars"
                autofix_acl_groups(rules_abac, rules_env if rules_env.exists() else None)
        if not args.allow_rule_changes:
            for line in keep_reviewed_group_descriptions(reviewed_rules[0], rules_abac):
                print(line)
        for line in policy_autofix_results(rules_abac, reviewed_rules[0]):
            print(line)

    if sql_block and hcl_block and args.mode != "genie":
        pruned = prune_unused_new_functions(
            validation_dir / "masking_functions.sql", validation_dir / "abac.auto.tfvars",
            prior_function_names,
        )
        if pruned:
            print(f"  [AUTOFIX] Removed {len(pruned)} new masking function(s) no policy uses: "
                  f"{', '.join(pruned)}")

    # Genie mode: skip validation — the output intentionally has no groups/ABAC sections
    # and validate_abac.py would incorrectly report "groups is missing".
    # Governance mode + full mode: validate when both HCL and SQL blocks are present.
    _can_validate = hcl_block and sql_block and args.mode != "genie"
    if _can_validate and not args.skip_validation:
        # Final HCL syntax repair — autofixes may have re-introduced issues
        # (e.g. missing commas between objects added by autofix_missing_fgac_policies).
        _final_tfvars = validation_dir / "abac.auto.tfvars"
        if _final_tfvars.exists():
            # Clean stray commas left by policy/assignment removals
            _ft = _final_tfvars.read_text()
            _ft_cleaned = re.sub(r'^\s*,\s*$', '', _ft, flags=re.MULTILINE)
            _ft_cleaned = re.sub(r',([ \t]*,)+', ',', _ft_cleaned)
            _ft_cleaned = re.sub(r'(?<![}"\']),([ \t]*\])', r'\1', _ft_cleaned)
            if _ft_cleaned != _ft:
                _final_tfvars.write_text(_ft_cleaned)
            fix_hcl_syntax(_final_tfvars)
        passed = run_validation(validation_dir, countries=countries, industries=industries)
        if not passed:
            print("\n  Validation found errors. Review the output above and fix before running terraform apply.")
            sys.exit(1)

        if args.promote and passed:  # type: ignore[possibly-unbound]
            if WORK_DIR.name in {"account", "data_access"}:
                print("\n  [SKIP] --promote requires a workspace env directory (e.g. envs/dev).")
            elif target_space_cfg is not None and space_key:
                print("\n  [SKIP] --promote is not supported with --space. Run make apply after reviewing.")
            else:
                split_script = SCRIPT_DIR / "scripts" / "split_abac_config.py"
                account_path = WORK_DIR.parent / "account" / "abac.auto.tfvars"
                data_access_dir = WORK_DIR / "data_access"
                data_access_dir.mkdir(parents=True, exist_ok=True)
                workspace_path = WORK_DIR / "abac.auto.tfvars"
                subprocess.check_call(
                    [
                        sys.executable,
                        str(split_script),
                        str(tfvars_path),
                        str(account_path),
                        str(data_access_dir / "abac.auto.tfvars"),
                        str(workspace_path),
                    ]
                )
                if sql_block:
                    shutil.copy2(sql_path, data_access_dir / "masking_functions.sql")
                print(
                    "\n  Promoted into shared account + env-scoped data_access + workspace configs."
                )
    elif not hcl_block or (not sql_block and args.mode == "full"):
        print("\n  [ERROR] Could not extract both code blocks from LLM response.")
        print(f"  Review {response_path} and manually extract the files.")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("  Done!")
    if hcl_block:
        if args.promote:
            env_name = WORK_DIR.name
            env_suffix = f" ENV={env_name}" if env_name != "dev" else ""
            print("  Files promoted into the current env workspace. Next step:")
            print(f"    make apply{env_suffix}   (or: terraform init && terraform apply -parallelism=1)")
        elif args.space:
            env_name = WORK_DIR.name
            env_suffix = f" ENV={env_name}" if env_name != "dev" else ""
            assembled_dir = out_dir.parent.parent
            print(f"  Per-space output: {out_dir.resolve()}")
            print(f"  Merged into:      {(assembled_dir / 'abac.auto.tfvars').resolve()}")
            print("  Next steps:")
            print(f"    1. Review generated/spaces/{space_key}/abac.auto.tfvars  (space-specific draft)")
            print(f"    2. Review generated/abac.auto.tfvars  (assembled — what apply uses)")
            print(f"    3. make validate-generated{env_suffix}")
            print(f"    4. make apply{env_suffix}")
        else:
            for line in generate_next_steps(
                out_dir.resolve(), args.mode, WORK_DIR.name,
                has_sql=bool(sql_block), champion_flow=champion_flow,
            ):
                print(line)
    print("=" * 60)


def generate_next_steps(
    out_dir: Path, mode: str, env_name: str, *, has_sql: bool, champion_flow: bool
) -> list[str]:
    """Next steps printed after a full (non --space, non --promote) generate."""
    if champion_flow and mode == "genie":
        return champion_genie_next_steps(env_name)
    if champion_flow and mode == "full":
        # In the walkthrough's Dev section, rehearse already runs validate-generated +
        # apply. Prod never rehearses (the Makefile refuses); it releases.
        next_cmd = (
            f"make release ENV={env_name}" if env_name == "prod"
            else f"make rehearse ENV={env_name}"
        )
        lines = ["  Next steps:", "    1. Review the draft:"]
        if has_sql:
            lines.append(f"       {out_dir}/masking_functions.sql")
        lines += [f"       {out_dir}/abac.auto.tfvars", f"    2. {next_cmd}"]
        return lines
    env_suffix = f" ENV={env_name}" if env_name != "dev" else ""
    lines = [
        "  Next steps:",
        "    1. Review the tuning checklist:",
        f"       {out_dir}/TUNING.md",
        "    2. Review and tune generated files:",
    ]
    if has_sql:
        lines.append(f"       {out_dir}/masking_functions.sql")
    lines += [
        f"       {out_dir}/abac.auto.tfvars",
        f"    3. make validate-generated{env_suffix}   (check your changes anytime)",
    ]
    if mode == "governance":
        lines.append(f"    4. make apply-governance{env_suffix}   (applies account + data_access layers only)")
    elif mode == "genie":
        lines.append(f"    4. make apply-genie{env_suffix}   (applies workspace layer only)")
    else:
        lines.append(f"    4. make apply{env_suffix}   (validates, splits shared account/workspace config, runs terraform apply)")
    return lines


if __name__ == "__main__":
    main()
