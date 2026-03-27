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
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

PRODUCT_NAME = "genierails"
PRODUCT_VERSION = "0.1.0"

SCRIPT_DIR = Path(__file__).resolve().parent
WORK_DIR = Path.cwd()
PROMPT_TEMPLATE_PATH = SCRIPT_DIR / "ABAC_PROMPT.md"
DEFAULT_AUTH_FILE = WORK_DIR / "auth.auto.tfvars"
DEFAULT_ENV_FILE = WORK_DIR / "env.auto.tfvars"

REQUIRED_PACKAGES = {
    "python-hcl2": "hcl2",
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


def _load_tfvars(path: Path, label: str) -> dict:
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
        print(f"  WARNING: Failed to parse {path}: {e}")
        return {}


def load_auth_config(auth_file: Path, env_file: Path | None = None) -> dict:
    """Load config from auth + env tfvars files. Merges both; env overrides auth.

    Supports the new split format (uc_catalog + schema-relative uc_tables) as well as
    the legacy full-ref format (uc_tables = ["catalog.schema.table"]).  When uc_catalog
    is set, relative uc_tables entries are expanded into full 3-part refs before being
    returned so the rest of the script does not need to know about the split.
    """
    cfg = _load_tfvars(auth_file, "credentials")
    if env_file is None:
        env_file = auth_file.parent / "env.auto.tfvars"
    env_cfg = _load_tfvars(env_file, "environment")
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


def configure_databricks_env(auth_cfg: dict):
    """Set Databricks SDK env vars from auth config if not already set."""
    mapping = {
        "databricks_workspace_host": "DATABRICKS_HOST",
        "databricks_client_id": "DATABRICKS_CLIENT_ID",
        "databricks_client_secret": "DATABRICKS_CLIENT_SECRET",
    }
    for tfvar_key, env_key in mapping.items():
        val = auth_cfg.get(tfvar_key, "")
        if val and not os.environ.get(env_key):
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


def parse_genie_config_from_serialized_space(serialized: str, description: str = "") -> dict:
    """Parse a Genie Space's serialized_space JSON into a genie_space_configs dict.

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

        if cfg.get("acl_groups"):
            lines.append("    acl_groups = [")
            for g in cfg["acl_groups"]:
                lines.append(f"      {_hcl_str(g)},")
            lines.append("    ]")

        lines.append("  }")

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


def fetch_tables_from_genie_space(
    space_id: str,
    auth_cfg: dict,
    quick_check_only: bool = False,
) -> tuple[list[str], dict, str]:
    """Fetch tables and config from an existing Genie Space via the REST API.

    Returns (table_identifiers, genie_config_dict, space_title).
    Uses GET /api/2.0/genie/spaces/{space_id} and parses serialized_space.

    Retries up to 5 times with backoff when serialized_space is empty —
    Databricks may process it asynchronously immediately after creation.
    """
    import json as _json
    import time as _time

    from databricks.sdk import WorkspaceClient

    configure_databricks_env(auth_cfg)
    w = WorkspaceClient(product=PRODUCT_NAME, product_version=PRODUCT_VERSION)

    print(f"  Querying Genie Space {space_id}...")
    try:
        resp = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}")
    except Exception as e:
        print(f"  WARNING: Could not reach Genie Space {space_id}: {e}")
        return [], {}, ""

    if not isinstance(resp, dict):
        print(f"  WARNING: Unexpected response type from Genie Space {space_id}.")
        return [], {}, ""

    space_title = resp.get("title", "")
    description = resp.get("description", "")
    serialized = resp.get("serialized_space", "")

    # Genie Spaces may take 1-3 minutes after creation before serialized_space
    # is populated by the Databricks backend (async processing).
    # Skip retries when uc_tables is already provided (quick_check_only=True) —
    # in that case we only need the space config, not table discovery, and
    # a missing serialized_space is acceptable (config will just be omitted).
    if not serialized and not quick_check_only:
        retry_delays = [5, 10, 20, 30, 45, 60, 90]
        for attempt, delay in enumerate(retry_delays, start=1):
            print(f"  Genie Space {space_id} has no serialized_space yet — "
                  f"retrying in {delay}s (attempt {attempt}/{len(retry_delays)})...")
            _time.sleep(delay)
            try:
                resp = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}")
                space_title = resp.get("title", space_title)
                description = resp.get("description", description)
                serialized = resp.get("serialized_space", "")
            except Exception as e:
                print(f"  WARNING: Retry failed: {e}")
            if serialized:
                break

    if not serialized:
        print(f"  WARNING: Genie Space {space_id} returned no serialized_space after retries.")
        return [], {}, space_title

    # --- Tables ---
    try:
        space_data = _json.loads(serialized)
        tables = space_data.get("data_sources", {}).get("tables", [])
        identifiers = [t["identifier"] for t in tables if "identifier" in t]
    except Exception as e:
        print(f"  WARNING: Could not parse table list from Genie Space {space_id}: {e}")
        identifiers = []

    if identifiers:
        print(f"    Discovered {len(identifiers)} table(s): {', '.join(identifiers)}")
    else:
        print(f"  WARNING: Genie Space {space_id} has no tables configured yet.")

    # --- Config ---
    genie_config = parse_genie_config_from_serialized_space(serialized, description=description)
    n_benchmarks = len(genie_config.get("benchmarks", []))
    n_filters = len(genie_config.get("sql_filters", []))
    n_measures = len(genie_config.get("sql_measures", []))
    print(
        f"    Parsed config: {n_benchmarks} benchmark(s), "
        f"{n_filters} filter(s), {n_measures} measure(s)"
    )

    return identifiers, genie_config, space_title


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


def build_prompt(ddl_text: str,
                 catalog_schemas: list[tuple[str, str]] | None = None,
                 group_names: list[str] | None = None,
                 per_space_name: str | None = None,
                 space_names: list[str] | None = None,
                 mode: str = "full",
                 countries: list[str] | None = None,
                 industries: list[str] | None = None) -> str:
    """Build the full prompt by injecting DDL and optional group names into the template.

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
            "\n### REQUIRED GENIE SPACE NAMES\n\n"
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
            f"You are generating config for a SINGLE Genie Space named: \"{per_space_name}\"\n\n"
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
            "- Do NOT generate 'genie_space_configs' — Genie space content is managed independently "
            "by each BU team. Omit the genie_space_configs block entirely from your output.\n"
            "- Focus on data classification (tags), access policies (FGAC), and masking functions "
            "that apply to the governed tables regardless of which Genie spaces query them.\n\n"
        )
    elif mode == "genie":
        per_space_instruction = (
            "\n### GENIE-ONLY MODE\n\n"
            "You are generating Genie Space configurations for a BU team. "
            "The ABAC governance (groups, tag policies, tag assignments, FGAC policies, masking "
            "functions) is managed by a central Data Governance team — do NOT generate any of that.\n\n"
            "IMPORTANT CONSTRAINTS:\n"
            "- Generate ONLY: genie_space_configs (with instructions, sample_questions, benchmarks, "
            "sql_measures, sql_filters, sql_expressions, and join_specs for each Genie Space).\n"
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

    country_instruction = ""
    if countries:
        country_instruction = load_country_overlays(countries)

    industry_instruction = ""
    if industries:
        industry_instruction = load_industry_overlays(industries)

    if idx == -1:
        print("WARNING: Could not find '### MY TABLES' in ABAC_PROMPT.md")
        print("  Appending DDL at the end of the prompt instead.\n")
        prompt = template + f"\n\n{per_space_instruction}{country_instruction}{industry_instruction}{groups_lines}{space_names_lines}{cs_lines}\n\n{ddl_text}\n"
    else:
        prompt_body = template[:idx].rstrip()
        user_input = (
            f"\n\n{per_space_instruction}"
            f"{country_instruction}"
            f"{industry_instruction}"
            f"{groups_lines}"
            f"{space_names_lines}"
            f"### MY TABLES\n\n"
            f"{cs_lines}\n"
            f"```sql\n{ddl_text}\n```\n"
        )
        prompt = prompt_body + user_input

    return prompt


def extract_code_blocks(response_text: str) -> tuple[str | None, str | None]:
    """Extract the SQL and HCL code blocks from the LLM response."""
    sql_block = None
    hcl_block = None

    blocks = re.findall(r"```(\w*)\n(.*?)```", response_text, re.DOTALL)

    for lang, content in blocks:
        content = content.strip()
        lang_lower = lang.lower()

        if lang_lower == "sql" and sql_block is None:
            sql_block = content
        elif lang_lower in ("hcl", "terraform") and hcl_block is None:
            hcl_block = content
        elif not lang and sql_block is None and "CREATE" in content.upper() and "FUNCTION" in content.upper():
            sql_block = content
        elif not lang and hcl_block is None and "groups" in content and "tag_policies" in content:
            hcl_block = content

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
        "# Databricks One consumer access, data grants, and optional Genie Space ACLs.\n"
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
        "# Genie Space configs (per-space semantic configuration + ACLs)\n"
        "# ----------------------------------------------------------------------------\n"
        "# Each key is the human-readable space name matching genie_spaces[*].name in\n"
        "# env.auto.tfvars. Contains instructions, benchmarks, SQL measures, and ACLs.\n"
        "#\n"
        "# acl_groups: controls which groups get CAN_RUN on this Genie Space.\n"
        "#   - List the group names that should have access to this specific space\n"
        "#   - Groups NOT listed are excluded from the space\n"
        "#   - Empty list or omitted = all groups get access (backward compatible)\n"
        "#   - In multi-space setups, use this to ensure Finance groups only see\n"
        "#     the Finance space, Clinical groups only see the Clinical space, etc.\n"
        "#\n"
        + docs
    )

    text = insert_before(r"^groups\s*=\s*\{", groups_block, text)
    text = insert_before(r"^tag_policies\s*=\s*\[", tag_policies_block, text)
    text = insert_before(r"^tag_assignments\s*=\s*\[", tag_assignments_block, text)
    text = insert_before(r"^fgac_policies\s*=\s*\[", fgac_block, text)
    text = insert_before(r"^genie_space_configs\s*=\s*\{", genie_configs_block, text)

    return text


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
        max_tokens=8192,
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
        max_tokens=8192,
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

    cfg = Config(http_timeout_seconds=600, product=PRODUCT_NAME, product_version=PRODUCT_VERSION)
    w = WorkspaceClient(config=cfg)
    print(f"  Calling Databricks FMAPI ({model})...")

    response = w.serving_endpoints.query(
        name=model,
        messages=[
            ChatMessage(role=ChatMessageRole.SYSTEM, content="You are a Databricks Unity Catalog ABAC expert."),
            ChatMessage(role=ChatMessageRole.USER, content=prompt),
        ],
        max_tokens=8192,
        temperature=0,
    )
    return response.choices[0].message.content


PROVIDERS = {
    "databricks": {
        "call": call_databricks,
        "default_model": "databricks-claude-sonnet-4",
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
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        if self._thread:
            self._thread.join()
        elapsed = time.time() - self._start_time
        sys.stderr.write(f"\r  {self._message} — done ({elapsed:.1f}s)\n")
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
                lines[j].strip() == '' or lines[j].lstrip().startswith('#')
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

    if text != original:
        tfvars_path.write_text(text)
        print(f"  [AUTOFIX] Repaired {repairs} HCL syntax issue(s)")

    return repairs


def _fetch_live_tag_policy_values() -> dict[str, set[str]]:
    """Query Databricks for existing tag policy keys and their allowed values.

    Returns {tag_key: set(values)}.  Returns an empty dict on any failure
    (network, auth, API unavailable) so callers can proceed without live data.
    """
    import ssl
    import urllib.request
    import json as _json

    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(product="genierails", product_version="0.1.0")
        token = w.config.authenticate()
        host = (os.environ.get("DATABRICKS_HOST") or "").rstrip("/")
        if not host:
            return {}

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        req = urllib.request.Request(
            f"{host}/api/2.1/unity-catalog/tag-policies", headers=token,
        )
        with urllib.request.urlopen(req, timeout=30, context=ssl_ctx) as resp:
            data = _json.loads(resp.read())

        result: dict[str, set[str]] = {}
        for tp in data.get("tag_policies", []):
            tag_key = tp.get("tag_key", "")
            values = {v.get("name", "") for v in (tp.get("values") or []) if v.get("name")}
            if tag_key:
                result[tag_key] = values
        return result
    except Exception as exc:
        print(f"  [AUTOFIX] Could not fetch live tag policies ({exc}); using file-only values")
        return {}


def autofix_tag_policies(tfvars_path: Path) -> int:
    """Add tag values used in assignments/policies but missing from tag_policies.

    When Databricks credentials are available (env vars set by configure_databricks_env),
    also seeds allowed values from live tag policies so the generated config doesn't
    reference values that don't exist in the live policy.
    """
    text = tfvars_path.read_text()

    live_policies = _fetch_live_tag_policy_values()
    if live_policies:
        print(f"  [AUTOFIX] Loaded {len(live_policies)} live tag policy/ies from Databricks")

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
    # Also collect from hasTagValue() in fgac_policies conditions
    for m in re.finditer(r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", text):
        used.setdefault(m.group(1), set()).add(m.group(2))

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


# Databricks platform limit for ABAC column-mask/row-filter policies per catalog.
_FGAC_PER_CATALOG_LIMIT = 10  # Databricks platform hard cap


def autofix_fgac_policy_count(tfvars_path: Path) -> int:
    """Trim fgac_policies to at most _FGAC_PER_CATALOG_LIMIT per catalog.

    When trimming is required, preserve coverage of non-public tag assignments
    first, then drop lower-priority policies such as amount rounding.

    Returns the number of policies removed.
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

    def _value_requires_coverage(tag_value: str) -> bool:
        return tag_value.strip().lower() not in {"public", "general", "exact"}

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

    def _assignment_priority(entity_name: str, tag_key: str, tag_value: str, entity_type: str) -> int:
        if entity_type == "tables":
            key_blob = f"{tag_key} {tag_value} {entity_name}".lower()
            if any(tok in key_blob for tok in ("aml", "hipaa", "pci", "compliance", "audit")):
                return 85
            return 50

        blob = f"{entity_name} {tag_key} {tag_value}".lower()
        if any(tok in blob for tok in ("ssn", "mrn", "cvv", "pan", "government", "token", "secret", "account_number", "iban")):
            return 100
        if any(tok in blob for tok in ("card_number", "credit_card")):
            return 95
        if any(tok in blob for tok in ("address", "birth", "dob", "date_of_birth")):
            return 80
        if any(tok in blob for tok in ("email", "phone", "name")):
            return 70
        if any(tok in blob for tok in ("amount", "balance", "limit", "rounded")):
            return 20
        return 40

    assignments = cfg.get("tag_assignments", []) or []
    entity_tags: dict[tuple[str, str], dict[str, set[str]]] = {}
    assignment_meta: dict[str, dict] = {}
    for ta in assignments:
        etype = ta.get("entity_type", "")
        ename = ta.get("entity_name", "")
        tkey = ta.get("tag_key", "")
        tval = ta.get("tag_value", "")
        if not (etype and ename and tkey and tval):
            continue
        per_entity = entity_tags.setdefault((etype, ename), {})
        per_entity.setdefault(tkey, set()).add(tval)
        if not _value_requires_coverage(tval):
            continue
        assignment_id = f"{etype}|{ename}|{tkey}|{tval}"
        assignment_meta[assignment_id] = {
            "entity_type": etype,
            "entity_name": ename,
            "tag_key": tkey,
            "tag_value": tval,
            "catalog": ename.split(".")[0],
            "priority": _assignment_priority(ename, tkey, tval, etype),
        }

    def _policy_matches_assignment(policy: dict, assignment: dict) -> bool:
        policy_catalog = policy.get("catalog", "") or policy.get("function_catalog", "")
        entity_name = assignment["entity_name"]
        entity_type = assignment["entity_type"]
        if policy_catalog and policy_catalog != assignment["catalog"]:
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

    per_catalog_policies: dict[str, list[tuple[int, dict]]] = {}
    for idx, p in enumerate(policies):
        cat = p.get("catalog", "") or p.get("function_catalog", "")
        if cat:
            per_catalog_policies.setdefault(cat, []).append((idx, p))

    to_drop: set[str] = set()
    assignments_to_remove: list[tuple[str, str, str]] = []  # (tag_key, tag_value, entity_name)
    for cat, indexed_policies in per_catalog_policies.items():
        if len(indexed_policies) <= _FGAC_PER_CATALOG_LIMIT:
            continue

        protected_assignments = {
            aid: meta for aid, meta in assignment_meta.items() if meta["catalog"] == cat
        }
        selected_names: list[str] = []
        covered_ids: set[str] = set()
        remaining = list(indexed_policies)

        while remaining and len(selected_names) < _FGAC_PER_CATALOG_LIMIT:
            best_tuple = None
            best_idx = None
            for list_idx, (original_idx, policy) in enumerate(remaining):
                policy_name = policy.get("name", "")
                policy_type = policy.get("policy_type", "")
                coverage = {
                    aid for aid, meta in protected_assignments.items() if _policy_matches_assignment(policy, meta)
                }
                new_coverage = coverage - covered_ids
                coverage_score = sum(protected_assignments[aid]["priority"] for aid in new_coverage)
                base_priority = max(
                    (protected_assignments[aid]["priority"] for aid in coverage),
                    default=(60 if policy_type == "POLICY_TYPE_ROW_FILTER" else 10),
                )
                score = (coverage_score, len(new_coverage), base_priority, -original_idx)
                if best_tuple is None or score > best_tuple:
                    best_tuple = score
                    best_idx = list_idx

            if best_idx is None:
                break
            original_idx, chosen = remaining.pop(best_idx)
            chosen_name = chosen.get("name", "")
            if not chosen_name:
                continue
            selected_names.append(chosen_name)
            covered_ids.update(
                aid for aid, meta in protected_assignments.items() if _policy_matches_assignment(chosen, meta)
            )

        if len(selected_names) < _FGAC_PER_CATALOG_LIMIT:
            extras = [
                p.get("name", "")
                for _idx, p in indexed_policies
                if p.get("name", "") and p.get("name", "") not in selected_names
            ]
            selected_names.extend(extras[: _FGAC_PER_CATALOG_LIMIT - len(selected_names)])

        kept = set(selected_names)
        dropped = [p.get("name", "") for _idx, p in indexed_policies if p.get("name", "") not in kept]
        if dropped:
            to_drop.update(dropped)
            print(
                f"  [AUTOFIX] Catalog '{cat}': {len(indexed_policies)} fgac_policies exceeds "
                f"limit of {_FGAC_PER_CATALOG_LIMIT}. Dropping {len(dropped)}: "
                + ", ".join(dropped)
            )
            uncovered_assignments = [
                meta
                for aid, meta in protected_assignments.items()
                if aid not in covered_ids
            ]
            if uncovered_assignments:
                uncovered_desc = [
                    f"{meta['entity_name']} ({meta['tag_key']} = '{meta['tag_value']}')"
                    for meta in uncovered_assignments
                ]
                print(
                    "  [AUTOFIX] Policy cap leaves uncovered sensitive assignments, removing them: "
                    + ", ".join(uncovered_desc)
                )
                assignments_to_remove.extend(
                    (meta["tag_key"], meta["tag_value"], meta["entity_name"])
                    for meta in uncovered_assignments
                )

    if not to_drop and not assignments_to_remove:
        return 0

    # Remove each excess policy block from the HCL text using brace counting
    # so that nested blocks (column_mask = { ... }, match_columns = [...]) are
    # handled correctly.  A plain regex can't handle nested braces.
    def _remove_block(txt: str, block_name: str) -> tuple[str, bool]:
        """Find the policy block with `name = "block_name"` and remove it."""
        name_pat = re.compile(r'name\s*=\s*"' + re.escape(block_name) + r'"')
        m = name_pat.search(txt)
        if not m:
            return txt, False

        pos = m.start()

        # Walk backward from `name =` to find the opening { of the block.
        depth = 0
        block_start = None
        i = pos - 1
        while i >= 0:
            c = txt[i]
            if c == '}':
                depth += 1
            elif c == '{':
                if depth == 0:
                    block_start = i
                    break
                depth -= 1
            i -= 1

        if block_start is None:
            return txt, False

        # Walk forward from block_start to find the matching }.
        depth = 0
        block_end = None
        i = block_start
        while i < len(txt):
            c = txt[i]
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    block_end = i
                    break
            i += 1

        if block_end is None:
            return txt, False

        # Determine slice boundaries that include surrounding whitespace and
        # the trailing comma (same algorithm as autofix_invalid_tag_values so
        # that the removal always leaves well-formed HCL).
        end = block_end + 1
        while end < len(txt) and txt[end] in (",", " ", "\t"):
            end += 1
        start = block_start
        while start > 0 and txt[start - 1] in (" ", "\t"):
            start -= 1
        if start > 0 and txt[start - 1] == "\n":
            start -= 1

        return txt[:start] + txt[end:], True

    removed = 0
    for name in to_drop:
        text, did_remove = _remove_block(text, name)
        if did_remove:
            removed += 1

    # Remove tag_assignments left uncovered by dropped policies
    assignments_removed = 0
    for tag_key, tag_value, entity_name in assignments_to_remove:
        # Match blocks containing all three: entity_name, tag_key, tag_value
        pattern = re.compile(
            r'entity_name\s*=\s*"' + re.escape(entity_name) + r'"'
            r'.*?'
            r'tag_key\s*=\s*"' + re.escape(tag_key) + r'"'
            r'.*?'
            r'tag_value\s*=\s*"' + re.escape(tag_value) + r'"',
            re.DOTALL,
        )
        # Also check alternate field orderings
        pattern_rev = re.compile(
            r'tag_key\s*=\s*"' + re.escape(tag_key) + r'"'
            r'.*?'
            r'tag_value\s*=\s*"' + re.escape(tag_value) + r'"'
            r'.*?'
            r'entity_name\s*=\s*"' + re.escape(entity_name) + r'"',
            re.DOTALL,
        )
        m = pattern.search(text) or pattern_rev.search(text)
        if not m:
            continue
        pos = m.start()
        # Walk backward to find the opening {
        depth = 0
        block_start = None
        i = pos - 1
        while i >= 0:
            c = text[i]
            if c == '}':
                depth += 1
            elif c == '{':
                if depth == 0:
                    block_start = i
                    break
                depth -= 1
            i -= 1
        if block_start is None:
            continue
        # Walk forward to find the matching }
        depth = 0
        block_end = None
        i = block_start
        while i < len(text):
            c = text[i]
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    block_end = i
                    break
            i += 1
        if block_end is None:
            continue
        end = block_end + 1
        while end < len(text) and text[end] in (",", " ", "\t"):
            end += 1
        start = block_start
        while start > 0 and text[start - 1] in (" ", "\t"):
            start -= 1
        if start > 0 and text[start - 1] == "\n":
            start -= 1
        text = text[:start] + text[end:]
        assignments_removed += 1
        print(
            f"  [AUTOFIX] Removed uncovered tag_assignment: "
            f"{entity_name} ({tag_key} = '{tag_value}')"
        )

    if removed or assignments_removed:
        # Clean up double-blank lines left by removal
        text = re.sub(r"\n{3,}", "\n\n", text)
        tfvars_path.write_text(text)

    return removed


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
            if any(tok in blob for tok in ("amount", "balance", "limit", "rounded")):
                preferred.append("mask_amount_rounded")
            if "diagnosis" in blob:
                preferred.append("mask_diagnosis_code")
            if any(tok in blob for tok in ("note", "notes", "desc", "description")):
                preferred.append("mask_redact")
        # Only add generic masking fallbacks for column-level assignments.
        # Row filter functions must take 0 arguments; masking functions take 1.
        if not is_table:
            preferred.extend(["mask_redact", "mask_nullify", "mask_pii_partial"])
        for fn in preferred:
            if (not available_functions or fn in available_functions) and _arg_count_ok(fn):
                return fn
        # Last-resort: pick any available function of the right type from the SQL
        # file rather than returning None (which causes the autofix to skip adding
        # coverage and lets validation fail). Row filters take 0 args; column masks
        # take 1, so we look for the appropriate naming convention AND verify arg count.
        if available_functions:
            if is_table:
                filter_fns = sorted(f for f in available_functions if f.startswith("filter_") and _arg_count_ok(f))
                if filter_fns:
                    return filter_fns[0]
            else:
                mask_fns = sorted(f for f in available_functions if f.startswith("mask_") and _arg_count_ok(f))
                if mask_fns:
                    return mask_fns[0]
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
        # per genie space in an assembled multi-space file).  Process in
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


def autofix_acl_groups(tfvars_path: Path, env_tfvars_path: Path | None = None) -> int:
    """Populate acl_groups in genie_space_configs from FGAC policy analysis.

    For each space, finds which groups have FGAC policies on that space's tables
    and adds them to acl_groups. If acl_groups is already set, it's left unchanged.

    Returns the number of spaces that had acl_groups populated.
    """
    import hcl2

    text = tfvars_path.read_text()
    try:
        cfg = hcl2.loads(text)
    except Exception:
        return 0

    genie_cfgs = cfg.get("genie_space_configs") or {}
    if isinstance(genie_cfgs, list):
        genie_cfgs = genie_cfgs[0] if genie_cfgs else {}
    groups = cfg.get("groups") or {}
    if isinstance(groups, list):
        groups = groups[0] if groups else {}
    fgac_policies = cfg.get("fgac_policies") or []
    if isinstance(fgac_policies, list) and len(fgac_policies) == 1 and isinstance(fgac_policies[0], list):
        fgac_policies = fgac_policies[0]

    # Build space_name → set of catalogs from env.auto.tfvars or from tag_assignments
    space_catalogs: dict[str, set[str]] = {}

    # Try to get uc_tables per space from env.auto.tfvars
    if env_tfvars_path and env_tfvars_path.exists():
        try:
            env_cfg = hcl2.loads(env_tfvars_path.read_text())
            for space in (env_cfg.get("genie_spaces") or []):
                if isinstance(space, list):
                    space = space[0] if space else {}
                name = space.get("name", "")
                tables = space.get("uc_tables") or []
                if isinstance(tables, list) and tables:
                    if isinstance(tables[0], list):
                        tables = tables[0]
                    cats = {t.split(".")[0] for t in tables if "." in t}
                    if cats:
                        space_catalogs[name] = cats
        except Exception:
            pass

    if not space_catalogs:
        # Fallback: if we can't determine per-space catalogs, assign all groups to all spaces
        return 0

    # Build group → set of catalogs from fgac_policies
    group_catalogs: dict[str, set[str]] = {}
    for pol in fgac_policies:
        if isinstance(pol, list):
            pol = pol[0] if pol else {}
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

    # For each space, find groups whose FGAC catalogs overlap with the space's catalogs
    fixed = 0
    for space_name, cfg_entry in genie_cfgs.items():
        if isinstance(cfg_entry, list):
            cfg_entry = cfg_entry[0] if cfg_entry else {}
        existing_acl = cfg_entry.get("acl_groups") or []
        if isinstance(existing_acl, list) and existing_acl:
            if isinstance(existing_acl[0], list):
                existing_acl = existing_acl[0]
            if existing_acl:
                continue  # already set, don't override

        cats = space_catalogs.get(space_name, set())
        if not cats:
            continue

        # Find groups that have policies on this space's catalogs
        space_groups = sorted({
            g for g, g_cats in group_catalogs.items()
            if g_cats & cats and g in groups
        })

        if not space_groups:
            # If no specific groups found, use all groups (backward compat)
            space_groups = sorted(groups.keys())

        # Insert acl_groups into the HCL text for this space
        # Find the space's config block and add acl_groups before the closing }
        import re
        # Match the space's config block: "Space Name" = { ... }
        escaped_name = re.escape(space_name)
        pattern = rf'("{escaped_name}"\s*=\s*\{{[^}}]*?)(\n\s*\}})'
        acl_line = "\n    acl_groups = [\n" + "".join(f'      "{g}",\n' for g in space_groups) + "    ]"
        new_text, count = re.subn(pattern, rf'\1{acl_line}\2', text, count=1, flags=re.DOTALL)
        if count > 0:
            text = new_text
            fixed += 1

    if fixed:
        tfvars_path.write_text(text)
    return fixed


_GENERIC_FUNCTION_PREFS = ["mask_pii_partial", "mask_redact", "mask_nullify", "mask_hash"]
_GENERIC_SAFE_FUNCTIONS = {"mask_pii_partial", "mask_redact", "mask_nullify", "mask_hash"}
_FUNCTION_EXPECTED_CATEGORIES = {
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
    if "card" in col or "cvv" in col or "pan" in col:
        categories.add("card")
    if "amount" in col or "balance" in col or "limit" in col:
        categories.add("amount")
    return categories or {"generic"}


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
            print(f"  [AUTOFIX] Fixed function ref in policy '{pname}': {loc_old} -> {loc_new}")

    if not fixes:
        return 0

    text = text[:sec_start] + rewritten + text[sec_end:]
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
    # Map policy name → replacement function (if one can be found).
    replacements: dict[str, str] = {}
    for pname, old_fn, ptype, expected_args in bad_policies:
        candidates = sorted(fns_by_args.get(expected_args, []))
        # Prefer functions matching the naming convention (mask_ for columns, filter_ for rows).
        prefix = "filter_" if ptype == "POLICY_TYPE_ROW_FILTER" else "mask_"
        typed_candidates = [c for c in candidates if c.startswith(prefix)]
        # Pick a generic fallback from typed candidates.
        if typed_candidates:
            replacements[pname] = typed_candidates[0]
        elif candidates:
            replacements[pname] = candidates[0]

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
            print(
                f"  [AUTOFIX] Removed fgac_policy '{pname}' — function '{fn_name}' "
                f"arg count does not match policy type"
            )

    text = text[:sec_start] + rewritten + text[sec_end:]
    tfvars_path.write_text(text)
    return len(bad_policies)


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
        if fn in _GENERIC_SAFE_FUNCTIONS:
            continue
        expected = _FUNCTION_EXPECTED_CATEGORIES.get(fn)
        if not expected:
            continue

        value_refs, key_refs = _extract_tag_refs(p.get("match_condition", ""))
        matched: list[dict] = []
        for key, value in value_refs:
            matched.extend(assignments_by_tag.get((key, value), []))
        for key in key_refs:
            for (tag_key, _), items in assignments_by_tag.items():
                if tag_key == key:
                    matched.extend(items)
        if not matched:
            continue

        categories = set()
        for ta in matched:
            categories.update(_infer_column_categories_full(ta.get("entity_name", "")))
        if categories.issubset(expected):
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
            print(
                f"  [AUTOFIX] Fixed function category mismatch in policy '{pname}': "
                f"'{old_fn}' -> '{new_fn}'"
            )

    if not fixes:
        return 0

    text = text[:sec_start] + rewritten + text[sec_end:]
    tfvars_path.write_text(text)
    return fixes


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
        # Remove the policy block from fgac_policies list
        escaped = re.escape(name)
        pattern = re.compile(
            r',?\s*\{[^}]*?name\s*=\s*"' + escaped + r'"[^}]*?\}\s*,?',
            re.DOTALL,
        )
        new_text = pattern.sub("", text, count=1)
        if new_text != text:
            text = new_text
            removed += 1
            print(f"    Removed duplicate mask policy '{name}' (generic function on column already covered by specific policy)")

    if removed:
        # Clean up any leftover double commas or trailing commas before ]
        text = re.sub(r',\s*,', ',', text)
        text = re.sub(r',\s*\]', '\n  ]', text)
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
        escaped = _re_fc.escape(name)
        pattern = _re_fc.compile(
            r',?\s*\{[^}]*?name\s*=\s*"' + escaped + r'"[^}]*?\}\s*,?',
            re.DOTALL,
        )
        new_text = pattern.sub("", text, count=1)
        if new_text != text:
            text = new_text
            removed += 1

    if removed:
        text = re.sub(r',\s*,', ',', text)
        text = re.sub(r',\s*\]', '\n  ]', text)
        tfvars_path.write_text(text)
    return removed


def sanitize_space_key(name: str) -> str:
    """Convert a human-readable space name to a safe directory/Terraform key.

    Mirrors the sanitization applied in Terraform locals:
      'Finance Analytics' -> 'finance_analytics'
      'Exec Dashboard (Q1)' -> 'exec_dashboard_q1'
    """
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def load_groups_from_account_config() -> list[str]:
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
            print(f"  Auto-loaded {len(names)} group(s) from account config.")
            return names
    except Exception as e:
        print(f"  WARNING: Could not read account groups from {account_abac}: {e}")
    return []


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
        genie_cfgs: dict = parsed.get("genie_space_configs") or {}
    except Exception as e:
        print(f"  WARNING: Could not parse genie_space_configs for bootstrap: {e}")
        return

    if not genie_cfgs:
        return

    spaces_dir = out_dir / "spaces"
    for space_name, cfg in genie_cfgs.items():
        key = sanitize_space_key(space_name)
        space_dir = spaces_dir / key
        space_dir.mkdir(parents=True, exist_ok=True)

        space_abac = space_dir / "abac.auto.tfvars"
        content = (
            "# ============================================================================\n"
            f"# Per-space config for: {space_name}\n"
            "# Bootstrapped by full generation. Re-run: make generate SPACE=\"" + space_name + "\"\n"
            "# to regenerate only this space without touching others.\n"
            "# ============================================================================\n\n"
            + format_genie_space_configs_hcl({space_name: cfg})
            + "\n"
        )
        space_abac.write_text(content)

    print(
        f"  Bootstrapped {len(genie_cfgs)} per-space dir(s) under {spaces_dir.relative_to(out_dir.parent) if out_dir.parent != out_dir else spaces_dir}"
    )


def run_validation(out_dir: Path) -> bool:
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
    response_text = call_fn(prompt, model)

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


def post_generate_semantic_check(tfvars_path: Path, auth_cfg: dict) -> list[str]:
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
        return errors  # can't parse — let validation handle it

    # Check 1: genie_space_configs present when genie_spaces is configured
    genie_spaces = auth_cfg.get("genie_spaces", [])
    if genie_spaces:
        gsc = cfg.get("genie_space_configs") or {}
        if not gsc:
            errors.append(
                "genie_space_configs section missing from LLM output "
                f"(expected for {len(genie_spaces)} configured genie_space(s))"
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

    return errors


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
        "--groups",
        help="Comma-separated group names to use in generated config. "
             "When set, the LLM uses these exact names instead of inventing new ones. "
             "Useful for IDP-synced groups (e.g. --groups 'Finance_Analyst,Clinical_Staff').",
    )
    parser.add_argument(
        "--space",
        metavar="SPACE_NAME",
        help="Name of a single Genie Space to (re)generate. "
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
            "genie — generate Genie space configs only (instructions, benchmarks, "
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
        _run_delta_mode(Path(args.auth_file))
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

    auth_cfg = load_auth_config(auth_file)

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

    if args.space:
        genie_spaces_cfg_all = auth_cfg.get("genie_spaces", [])
        for sp in genie_spaces_cfg_all:
            sp_name = sp.get("name") or sp.get("genie_space_id") or ""
            if sp_name == args.space or sanitize_space_key(sp_name) == sanitize_space_key(args.space):
                target_space_cfg = sp
                break

        if target_space_cfg is None:
            print(f"ERROR: No Genie Space named '{args.space}' found in env.auto.tfvars.")
            print("  Available spaces:")
            for sp in genie_spaces_cfg_all:
                print(f"    - {sp.get('name') or sp.get('genie_space_id') or '(unnamed)'}")
            sys.exit(1)

        space_key = sanitize_space_key(
            target_space_cfg.get("name") or target_space_cfg.get("genie_space_id") or args.space
        )
        # Redirect output to the per-space directory
        base_out_dir = Path(args.out_dir)
        out_dir = base_out_dir / "spaces" / space_key
        print(f"  Space key:  {space_key}")
        print(f"  Out dir:    {out_dir}")

        # Auto-load existing groups from the account config so the LLM reuses them
        if not args.groups:
            existing_groups = load_groups_from_account_config()
            if existing_groups:
                args.groups = ",".join(existing_groups)

    # In genie mode, also auto-load groups from account config so the LLM knows
    # which pre-existing groups are available for space ACLs.
    if args.mode == "genie" and not args.space and not args.groups:
        existing_groups = load_groups_from_account_config()
        if existing_groups:
            args.groups = ",".join(existing_groups)
            print(f"  Auto-loaded {len(existing_groups)} group(s) from account config (genie mode)")

    catalog = args.catalog or ""
    schema = args.schema or ""

    catalog_schemas: list[tuple[str, str]] | None = None

    # ── Auto-discover tables and config from genie_spaces entries ────────────
    # For spaces where genie_space_id is set but uc_tables is empty, query the
    # Genie Space API to learn what tables and config that space contains.
    # The existing space's genie_space_configs is parsed verbatim from the API
    # (no LLM involvement) and injected into the generated abac.auto.tfvars
    # after the LLM runs, replacing whatever the LLM generated for that space.
    api_genie_configs: dict[str, dict] = {}  # space_name -> config parsed from API

    if not args.tables:
        genie_spaces_cfg = auth_cfg.get("genie_spaces", [])
        # In per-space mode, restrict scanning to only the target space
        if target_space_cfg is not None:
            genie_spaces_cfg = [target_space_cfg]
        if genie_spaces_cfg:
            all_space_tables: list[str] = []
            discovered_from_api: list[str] = []

            for space in genie_spaces_cfg:
                space_tables = space.get("uc_tables") or []
                space_id = space.get("genie_space_id") or ""
                space_name = space.get("name") or space_id

                if space_id:
                    # Always query the API for existing spaces to get config.
                    # Tables are also discovered here if uc_tables is not set.
                    # When uc_tables IS set, use quick_check_only to skip long
                    # retries — serialized_space is optional in that case.
                    if not space_tables:
                        print(f"\n  Genie Space '{space_name}' has no uc_tables — querying API...")
                    else:
                        print(f"\n  Querying existing Genie Space '{space_name}' for config...")

                    tables, genie_cfg, api_title = fetch_tables_from_genie_space(
                        space_id, auth_cfg, quick_check_only=bool(space_tables)
                    )

                    # Use the API title as the canonical name if no name was given
                    effective_name = space_name if space_name != space_id else (api_title or space_id)

                    if not space_tables:
                        all_space_tables.extend(tables)
                        discovered_from_api.extend(tables)
                    else:
                        all_space_tables.extend(space_tables)

                    if genie_cfg:
                        api_genie_configs[effective_name] = genie_cfg
                else:
                    all_space_tables.extend(space_tables)

            # Merge space tables with any top-level uc_tables (dedup, space tables first)
            existing_top = auth_cfg.get("uc_tables") or []
            merged = list(dict.fromkeys(all_space_tables + existing_top))
            if merged:
                auth_cfg["uc_tables"] = merged

            if discovered_from_api:
                print(
                    "\n  Auto-discovered tables from existing Genie Space(s):\n"
                    + "".join(f"    - {t}\n" for t in discovered_from_api)
                    + "\n  NOTE: Add these tables to data_access/env.auto.tfvars so that\n"
                    "  UC grants and masking functions are applied to them as well."
                )

    # Resolve table refs: CLI --tables overrides uc_tables from config
    table_refs = args.tables or auth_cfg.get("uc_tables") or None

    if table_refs:
        source = "--tables CLI" if args.tables else "uc_tables in auth config"
        print(f"  Provider: {args.provider}")
        print(f"  Out dir:  {out_dir}")
        print(f"  Tables:   {', '.join(table_refs)} (from {source})")
        print()

        ddl_text, catalog_schemas = fetch_tables_from_databricks(
            table_refs, auth_cfg,
        )

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

    group_names = None
    if args.groups:
        group_names = [g.strip() for g in args.groups.split(",") if g.strip()]
        src = "auto-loaded from account config" if target_space_cfg is not None and not args.groups.startswith(args.groups) else "--groups CLI"
        print(f"  Groups:   {', '.join(group_names)} ({src})")

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

    prompt = build_prompt(
        ddl_text,
        catalog_schemas=catalog_schemas,
        group_names=group_names,
        per_space_name=args.space if args.space else None,
        space_names=configured_space_names,
        mode=args.mode,
        countries=countries,
        industries=industries,
    )

    if args.dry_run:
        print("=" * 60)
        print("  DRY RUN — Prompt that would be sent:")
        print("=" * 60)
        print(prompt)
        sys.exit(0)

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
- `abac.auto.tfvars` — groups, tags, FGAC policies, and Genie Space config

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

## Checklist — Genie Space Metadata & ACLs

- **Genie title & description**: Does the AI-generated title/description accurately represent the space?
- **Genie sample questions**: Do the sample questions reflect what business users will ask?
- **Per-space ACLs (`acl_groups`)**: Each space lists which groups get `CAN_RUN` access. Verify that:
  - Each space includes all groups that need access
  - Groups that should NOT see this space are excluded
  - In multi-space setups, Finance groups should only be in the Finance space, Clinical groups in the Clinical space, etc.
  - Empty `acl_groups` means all groups get access (backward compatible)
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

    if sql_block:
        all_cs = catalog_schemas if catalog_schemas else [(catalog, schema)]
        targets = ", ".join(f"{c}.{s}" for c, s in all_cs)
        sql_header = (
            "-- ============================================================================\n"
            "-- GENERATED MASKING FUNCTIONS (FIRST DRAFT)\n"
            "-- ============================================================================\n"
            f"-- Target(s): {targets}\n"
            "-- Next: review generated/TUNING.md, tune if needed, then run this SQL.\n"
            "-- ============================================================================\n\n"
        )

        final_sql = sql_header + sql_block
        sql_path = out_dir / "masking_functions.sql"
        sql_path.write_text(final_sql + "\n")
        print(f"  masking_functions.sql written to: {sql_path}")
        print(f"    Target schemas: {targets}")

    if hcl_block:
        if args.mode == "genie":
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

        # ── Mode-based output filtering ───────────────────────────────────────
        if args.mode == "governance":
            # Strip genie_space_configs — BU teams manage Genie content independently.
            hcl_block = remove_hcl_top_level_block(hcl_block, "genie_space_configs")
            print("  [governance mode] Stripped genie_space_configs from output")
        elif args.mode == "genie":
            # Strip all ABAC sections — governance team manages them centrally.
            for key in ("groups", "tag_policies", "group_members"):
                hcl_block = remove_hcl_top_level_block(hcl_block, key)
            for key in ("tag_assignments", "fgac_policies"):
                hcl_block = remove_hcl_top_level_list(hcl_block, key)
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

        # ── Strip legacy Genie keys when no genie_spaces are configured ───────
        # The LLM sometimes hallucinates legacy single-space keys (genie_space_title,
        # genie_space_description, etc.) even when env.auto.tfvars has no genie_spaces.
        # Strip them to prevent Terraform from creating an unexpected Genie Space.
        _configured_spaces = auth_cfg.get("genie_spaces", [])
        if args.mode not in ("genie",) and not _configured_spaces and not args.space:
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
        # block with the verbatim parse from the Genie Space API.
        if api_genie_configs:
            hcl_block = remove_hcl_top_level_block(hcl_block, "genie_space_configs")
            injected_hcl = (
                "\n# genie_space_configs parsed verbatim from the existing Genie Space(s).\n"
                "# Edit here to manage space config as code; make apply pushes changes back.\n"
                + format_genie_space_configs_hcl(api_genie_configs)
            )
            hcl_block = hcl_block.rstrip() + "\n" + injected_hcl + "\n"
            print(
                f"  Injected genie_space_configs from Genie API for: "
                f"{', '.join(api_genie_configs)}"
            )

        tfvars_path = out_dir / "abac.auto.tfvars"
        tfvars_path.write_text(hcl_header + hcl_block + "\n")
        print(f"  abac.auto.tfvars written to: {tfvars_path}")

        fix_hcl_syntax(tfvars_path)

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

        n_repaired = autofix_missing_fgac_policies(tfvars_path, sql_path if sql_block else None)
        if n_repaired:
            print(f"  Auto-fixed: added {n_repaired} fgac_policy/ies for uncovered sensitive tags")

        n_dropped = autofix_fgac_policy_count(tfvars_path)
        if n_dropped:
            print(f"  Auto-fixed: dropped {n_dropped} fgac_policy/ies exceeding per-catalog limit ({_FGAC_PER_CATALOG_LIMIT})")

        # Second pass: autofix_missing_fgac_policies (above) may have injected
        # new hasTagValue() conditions referencing values that weren't in the
        # original tag_policies.  Re-run autofix_tag_policies to pick them up.
        n_fixed_2 = autofix_tag_policies(tfvars_path)
        if n_fixed_2:
            print(f"  Auto-fixed {n_fixed_2} additional missing tag_policy value(s) (second pass)")

        n_fields = autofix_genie_config_fields(tfvars_path)
        if n_fields:
            print(f"  Auto-fixed: added {n_fields} missing required field(s) in genie_space_configs")

        env_tfvars = tfvars_path.parent.parent / "env.auto.tfvars"
        n_acl = autofix_acl_groups(tfvars_path, env_tfvars if env_tfvars.exists() else None)
        if n_acl:
            print(f"  Auto-fixed: populated acl_groups for {n_acl} genie space(s)")

        n_fn_refs = autofix_invalid_function_refs(tfvars_path, sql_path if sql_block else None)
        if n_fn_refs:
            print(f"  Auto-fixed: corrected {n_fn_refs} invalid function reference(s) in fgac_policies")

        n_arg_mismatch = autofix_fgac_arg_count_mismatch(tfvars_path, sql_path if sql_block else None)
        if n_arg_mismatch:
            print(f"  Auto-fixed: removed {n_arg_mismatch} fgac_policy/ies with function arg count mismatch")

        n_cat_mismatch = autofix_function_category_mismatch(tfvars_path, sql_path if sql_block else None)
        if n_cat_mismatch:
            print(f"  Auto-fixed: corrected {n_cat_mismatch} function/category mismatch(es) in fgac_policies")

        n_dup_masks = autofix_duplicate_column_masks(tfvars_path)
        if n_dup_masks:
            print(f"  Auto-fixed: removed {n_dup_masks} duplicate column mask policy/ies")

        n_forbidden = autofix_forbidden_conditions(tfvars_path)
        if n_forbidden:
            print(f"  Auto-fixed: removed {n_forbidden} fgac_policy/ies with unsupported condition functions")

        # ── Semantic quality check (catches LLM issues that autofix can't fix) ──
        semantic_errors = post_generate_semantic_check(tfvars_path, auth_cfg)
        if semantic_errors:
            _semantic_retry_count += 1
            if _semantic_retry_count < _semantic_max_retries:
                print(f"\n  [SEMANTIC CHECK FAILED] (attempt {_semantic_retry_count}/{_semantic_max_retries}):")
                for err in semantic_errors:
                    print(f"    - {err}")
                print(f"  Re-generating with LLM...")
                response_text = call_with_retries(call_fn, prompt, model, 1)
                new_sql, new_hcl = extract_code_blocks(response_text)
                if new_hcl:
                    hcl_block = new_hcl
                    tfvars_path.write_text(hcl_header + hcl_block + "\n")
                    fix_hcl_syntax(tfvars_path)
                    autofix_ambiguous_tag_values(tfvars_path)
                    autofix_invalid_tag_values(tfvars_path)
                    autofix_tag_policies(tfvars_path)
                    autofix_undefined_tag_refs(tfvars_path)
                    autofix_missing_fgac_policies(tfvars_path, sql_path if sql_block else None)
                    autofix_fgac_policy_count(tfvars_path)
                    autofix_genie_config_fields(tfvars_path)
                    autofix_invalid_function_refs(tfvars_path, sql_path if sql_block else None)
                    autofix_fgac_arg_count_mismatch(tfvars_path, sql_path if sql_block else None)
                    autofix_function_category_mismatch(tfvars_path, sql_path if sql_block else None)
                if new_sql:
                    sql_block = new_sql
                    sql_path = out_dir / "masking_functions.sql"
                    sql_path.write_text(sql_block + "\n")
                # Re-check after retry
                semantic_errors = post_generate_semantic_check(tfvars_path, auth_cfg)
            if semantic_errors:
                print(f"\n  [SEMANTIC CHECK] Warnings after {_semantic_retry_count + 1} attempt(s):")
                for err in semantic_errors:
                    print(f"    - {err}")
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
                n_dropped_assembled = autofix_fgac_policy_count(assembled_abac_path)
                if n_dropped_assembled:
                    print(f"  Auto-fixed assembled abac: dropped {n_dropped_assembled} fgac_policy/ies exceeding per-catalog limit ({_FGAC_PER_CATALOG_LIMIT})")
                n_fields_assembled = autofix_genie_config_fields(assembled_abac_path)
                if n_fields_assembled:
                    print(f"  Auto-fixed assembled abac: added {n_fields_assembled} missing required field(s) in genie_space_configs")
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

        # ── Full generation: bootstrap per-space dirs from the assembled output ─
        elif target_space_cfg is None and not args.space:
            bootstrap_per_space_dirs(out_dir, auth_cfg, hcl_block)

    # In per-space mode, validate the assembled generated/ dir (what apply uses),
    # not the per-space subdirectory.
    validation_dir = out_dir.parent.parent if (target_space_cfg is not None and space_key) else out_dir

    # Genie mode: skip validation — the output intentionally has no groups/ABAC sections
    # and validate_abac.py would incorrectly report "groups is missing".
    # Governance mode + full mode: validate when both HCL and SQL blocks are present.
    _can_validate = hcl_block and sql_block and args.mode != "genie"
    if _can_validate and not args.skip_validation:
        passed = run_validation(validation_dir)
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
            env_name = WORK_DIR.name
            env_suffix = f" ENV={env_name}" if env_name != "dev" else ""
            print("  Next steps:")
            print(f"    1. Review the tuning checklist:")
            print(f"       {out_dir.resolve()}/TUNING.md")
            print(f"    2. Review and tune generated files:")
            if sql_block:
                print(f"       {out_dir.resolve()}/masking_functions.sql")
            print(f"       {out_dir.resolve()}/abac.auto.tfvars")
            print(f"    3. make validate-generated{env_suffix}   (check your changes anytime)")
            if args.mode == "governance":
                print(f"    4. make apply-governance{env_suffix}   (applies account + data_access layers only)")
            elif args.mode == "genie":
                print(f"    4. make apply-genie{env_suffix}   (applies workspace layer only)")
            else:
                print(f"    4. make apply{env_suffix}   (validates, splits shared account/workspace config, runs terraform apply)")
    print("=" * 60)


if __name__ == "__main__":
    main()
