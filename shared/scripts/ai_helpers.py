#!/usr/bin/env python3
"""Optional AI helpers (design 18.4, decision 5): drafts for review, never governance.

    suggest-treatment    make suggest-treatment ENV=dev CLASS=<class>: proposes a
                         mask-library treatment for a class nothing maps yet.
                         Writes drafts/treatments/<class>.json.
    draft-genie-content  make draft-genie-content ENV=dev SPACE=<agent>: drafts the
                         instructions, sample questions and benchmarks a captured
                         agent doesn't have. Writes drafts/genie/<agent>.json.
    draft-custom-mask    make draft-custom-mask ENV=dev TREATMENT=<name> TYPE=<type>:
                         drafts one mask expression and its exact-output fixture.
                         Writes drafts/custom_masks/<name>.sql and <name>.json.
    test-custom-mask     make test-custom-mask ENV=dev TREATMENT=<name>: runs the
                         mask library's exact-output checks on that draft (static
                         rules offline, then every fixture case on the dev SQL
                         warehouse, read-only). On a pass writes <name>.passed.json
                         with the digests of the files it tested. Calls no model.

These are the only commands that call a model, and only through ModelClient
below. Every one refuses under CI and runs only with ENV=dev whose physical
path is <cloud>/envs/dev; it writes only inside the real envs/dev/drafts/
(no symlink in any path component, files staged and moved into place). The
model sees only what a draft needs: class and treatment names, the mask
library, table names and column types; never data and never an agent's
existing instructions, questions or benchmarks. Fixture inputs are typed
literals only, so test-custom-mask never runs SQL a fixture supplies. Nothing in GenieRails
reads drafts/: generate, rehearse, release, maintain and check never import
this module, the rehearse fingerprint and make check's scratch copy skip the
folder, and a draft reaches config only when a person copies it there in a
reviewed PR (tag_treatments, the dev Genie UI then make capture).

The model endpoint is ai_helper_endpoint in envs/<env>/env.auto.tfvars (a
Databricks model serving endpoint). Without it the drafting commands stop with
a clear error; nothing else needs it.
"""

from __future__ import annotations

import argparse
import json
import os  # noqa: F401 (the os module tests reach through ai_helpers.os)
import re
import sys
from decimal import Decimal
from pathlib import Path
from typing import Callable, Protocol

SHARED_ROOT = Path(__file__).resolve().parents[1]
for _path in (SHARED_ROOT, SHARED_ROOT / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from scripts.hcl2_guard import import_hcl2  # noqa: E402
from governance_policies import (  # noqa: E402
    CUSTOM_TREATMENT, DeterministicGovernanceError, normalize_sql_type, tag_treatment_errors,
)
from mask_library import load_library, type_family  # noqa: E402
# The guards, contained file access and offline mask rules are shared with
# make adopt-custom-mask, which governance may import (it calls no model).
from scripts.custom_masks import (  # noqa: E402,F401 (re-exported)
    CALLER_FUNCTIONS, CI_MARKERS, HELPER_ENV, MASKABLE_FAMILIES, NONDETERMINISTIC_FUNCTIONS,
    SIDE_EFFECT_FUNCTIONS, STATEMENT_WORDS, TYPED_LITERAL, Refused, ci_name, contained, expression_problems,
    fixture_problems, helper_env, mask_expression, mask_paths, null_literal, passed_record, read_text,
    refuse_ci, render, run_cases, typed_expression, typed_literal, workspace_client, write_files,
)
from scripts.custom_masks import digest as _digest  # noqa: E402
from scripts.custom_masks import rel as _rel  # noqa: E402

DRAFTS_DIR = "drafts"
ENDPOINT_KEY = "ai_helper_endpoint"
SYSTEM_PROMPT = (
    "You draft GenieRails configuration for a person to review before anything uses it. "
    "Reply with exactly one JSON object and nothing else."
)
MAX_ITEMS = 10


# ── Guards: ci_name, refuse_ci and helper_env (scripts/custom_masks.py) ────

def _env_cfg(env_dir: Path) -> dict:
    try:
        hcl2 = import_hcl2()
        return hcl2.loads((env_dir / "env.auto.tfvars").read_text())
    except Exception as exc:  # hcl2 raises lark errors
        raise Refused(f"envs/{env_dir.name}/env.auto.tfvars is not valid HCL: {exc}") from exc


def endpoint(env_dir: Path, command: str) -> str:
    """ai_helper_endpoint from the env's config, or a clear error."""
    value = _env_cfg(env_dir).get(ENDPOINT_KEY)
    if isinstance(value, list):
        value = value[0] if value else ""
    value = str(value or "").strip()
    if not value:
        raise Refused(
            f"make {command} needs a model endpoint, and envs/{env_dir.name}/env.auto.tfvars has no "
            f"{ENDPOINT_KEY}. Set it to a Databricks model serving endpoint, e.g.\n"
            f'  {ENDPOINT_KEY} = "databricks-claude-sonnet-4-6"\n'
            "Only the AI helpers use it; generate, rehearse, release and maintain never call a model.")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", value):
        raise Refused(f"{ENDPOINT_KEY} = {value!r} is not a serving endpoint name")
    return value


# ── The model client: the one place GenieRails calls a model ────────────────

class ModelClient(Protocol):
    endpoint: str

    def complete(self, system: str, prompt: str) -> str: ...


class DatabricksModel:
    """A Databricks model serving endpoint, called with the env's workspace credentials."""

    def __init__(self, endpoint_name: str, auth_file: Path):
        refuse_ci("<AI helper>")  # also here, so no other caller can reach a model in CI
        self.endpoint = endpoint_name
        self._auth_file = auth_file

    def complete(self, system: str, prompt: str) -> str:
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

        try:
            response = workspace_client(self._auth_file).serving_endpoints.query(
                name=self.endpoint, max_tokens=8192,
                messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=system),
                          ChatMessage(role=ChatMessageRole.USER, content=prompt)])
        except Refused:
            raise
        except Exception as exc:
            raise Refused(f"model endpoint {self.endpoint} failed: {exc}") from exc
        content = response.choices[0].message.content if response.choices else None
        if isinstance(content, list):
            content = "\n".join(
                (block.get("text", "") if isinstance(block, dict) else getattr(block, "text", "")) or ""
                for block in content)
        if not isinstance(content, str) or not content.strip():
            raise Refused(f"model endpoint {self.endpoint} returned no text")
        return content


def reply_json(text: str) -> dict:
    """The JSON object a model replied with (a ```json fence is allowed)."""
    body = text.strip()
    fence = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", body, flags=re.DOTALL)
    try:
        value = json.loads(fence.group(1) if fence else body)
    except json.JSONDecodeError as exc:
        raise Refused(f"the model's reply is not one JSON object ({exc}); nothing was written. Re-run to retry.") from exc
    if not isinstance(value, dict):
        raise Refused("the model's reply is not a JSON object; nothing was written. Re-run to retry.")
    return value


# ── Drafts ──────────────────────────────────────────────────────────────────

# ── Contained file access under envs/dev ─────────────────────────────────────
# helper_env proved envs/dev is a real directory at its physical path. Every
# path below it that a helper reads, writes or removes must also have no
# symlink in any component (scripts/custom_masks.contained), and writes land
# only inside the real envs/dev/drafts/.

def _contained(env_dir: Path, path: Path, *, under_drafts: bool, directory: bool = False) -> Path:
    return contained(env_dir, path, under=DRAFTS_DIR if under_drafts else None, directory=directory)


def read_file(env_dir: Path, path: Path, *, under_drafts: bool = True) -> str:
    """A regular file's text, refusing symlinks (also at open time, O_NOFOLLOW)."""
    return read_text(env_dir, path, under=DRAFTS_DIR if under_drafts else None)


def remove_draft(env_dir: Path, path: Path) -> None:
    _contained(env_dir, path, under_drafts=True)
    if path.exists():
        path.unlink()


def write_drafts(env_dir: Path, files: dict[Path, str], force: bool) -> None:
    """Write every file or none, inside envs/dev/drafts/ only; a changed draft needs FORCE=1."""
    for path in files:
        _contained(env_dir, path, under_drafts=True)
    clash = [p for p, text in files.items() if p.exists() and read_file(env_dir, p) != text]
    if clash and not force:
        raise Refused(
            "a draft already exists and would change: " + ", ".join(_rel(env_dir, p) for p in clash)
            + ". Review or commit it first, or re-run with FORCE=1 to replace it.")
    write_files(env_dir, files, under=DRAFTS_DIR)


def _drafted_by(command: str, client: ModelClient) -> dict:
    return {"command": f"make {command}", "model_endpoint": client.endpoint,
            "status": "draft for review: GenieRails never reads drafts/"}


# ── suggest-treatment ───────────────────────────────────────────────────────

def _class_label(value: str) -> str:
    name = (value or "").strip().removeprefix("class.")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_]*", name):
        raise Refused(f"CLASS={value!r} is not a class name (lowercase, words joined by '_', e.g. CLASS=loyalty_id)")
    return f"class.{name}"


def suggest_treatment(env_dir: Path, class_name: str, client: ModelClient, force: bool = False) -> Path:
    label = _class_label(class_name)
    library = load_library()
    owner = {name: treatment for treatment, item in library["treatments"].items() for name in item["classes"]}
    name = label.removeprefix("class.")
    if name in owner:
        raise Refused(f"{label} is already mapped by the mask library (to {owner[name]}); nothing to suggest.")
    mapped = _env_cfg(env_dir).get("tag_treatments") or {}
    if isinstance(mapped, dict) and label in mapped:
        raise Refused(f"{label} is already mapped in envs/{env_dir.name}/env.auto.tfvars tag_treatments "
                      f"(to {mapped[label]}); nothing to suggest.")
    catalog = {t: {"classes": item["classes"], "partial": item.get("partial_by_type") or item["partial"],
                   "full": item["full"], "types": item["types"]}
               for t, item in sorted(library["treatments"].items())}
    prompt = (
        f"A Unity Catalog column carries the data classification tag {label}, which the GenieRails mask "
        "library does not map. Pick the existing mask-library treatment that protects it best. Tier 2 sees "
        "the partial version and tier 3 the full one; when unsure, prefer the stricter treatment.\n"
        f"Mask-library treatments (JSON): {json.dumps(catalog, sort_keys=True)}\n"
        'Reply as {"treatment": "<one treatment name above, or null if none fits>", "rationale": "<two '
        'sentences>", "custom_mask": null or {"name": "<lowercase_name>", "type": "<SQL type>", '
        '"description": "<the partial version to draft>"}}. Set custom_mask only when treatment is null.')
    reply = reply_json(client.complete(SYSTEM_PROMPT, prompt))
    treatment, rationale, custom = reply.get("treatment"), reply.get("rationale"), reply.get("custom_mask")
    if not isinstance(rationale, str) or not rationale.strip():
        raise Refused("the model gave no rationale; nothing was written. Re-run to retry.")
    draft = {"class": label, "rationale": rationale.strip(), "drafted_by": _drafted_by("suggest-treatment", client)}
    if treatment is not None:
        problems = tag_treatment_errors({label: treatment}, library)
        if problems or custom:
            raise Refused("the model's suggestion is not usable; nothing was written:\n  - "
                          + "\n  - ".join(problems or ["it set both treatment and custom_mask"]))
        item = library["treatments"][treatment]
        draft |= {
            "suggested_treatment": treatment,
            "library_versions": {"partial": item.get("partial_by_type") or item["partial"], "full": item["full"],
                                 "types": item["types"]},
            "to_adopt": (f'add "{label}" = "{treatment}" to tag_treatments in envs/{env_dir.name}/env.auto.tfvars '
                         f"(or make scaffold-treatments ENV={env_dir.name} TREATMENT={treatment}) in a PR, "
                         f"then make generate ENV={env_dir.name}"),
        }
    else:
        if not isinstance(custom, dict) or not all(isinstance(custom.get(k), str) and custom[k].strip()
                                                   for k in ("name", "type", "description")):
            raise Refused("the model found no library treatment and gave no custom_mask {name, type, description}; "
                          "nothing was written. Re-run to retry.")
        suggested = custom["name"].strip()
        mask_name = _mask_name(suggested if suggested.startswith("custom_") else f"custom_{suggested}")
        sql_type = _mask_type(custom["type"])
        draft |= {
            "suggested_treatment": None,
            "custom_mask": {"name": mask_name, "type": sql_type, "description": custom["description"].strip()},
            "to_adopt": (f"no mask-library treatment fits. Draft one with make draft-custom-mask ENV={env_dir.name} "
                         f"TREATMENT={mask_name} TYPE={sql_type}, then make test-custom-mask ENV={env_dir.name} "
                         f"TREATMENT={mask_name}. Until a mapping is adopted the column keeps the fail-safe "
                         "never-raw redaction."),
        }
    path = env_dir / DRAFTS_DIR / "treatments" / f"{label.removeprefix('class.')}.json"
    write_drafts(env_dir, {path: render(draft)}, force)
    return path


# ── draft-genie-content ─────────────────────────────────────────────────────

def _text(value) -> str:
    if isinstance(value, list):
        return "".join(str(v) for v in value).strip()
    return str(value or "").strip()


def genie_content(payload: dict) -> dict:
    """The instructions, sample questions and benchmarks a captured agent already has."""
    instructions = [_text(item.get("content")) for item in
                    ((payload.get("instructions") or {}).get("text_instructions") or []) if isinstance(item, dict)]
    questions = [_text(item.get("question")) for item in
                 ((payload.get("config") or {}).get("sample_questions") or []) if isinstance(item, dict)]
    benchmarks = [_text(item.get("question")) for item in
                  ((payload.get("benchmarks") or {}).get("questions") or []) if isinstance(item, dict)]
    return {"instructions": [t for t in instructions if t], "sample_questions": [q for q in questions if q],
            "benchmarks": [b for b in benchmarks if b]}


def _table_columns(env_dir: Path, tables: list[str]) -> dict[str, dict[str, str]]:
    """Column names and types of the agent's tables, from the DDL generate fetched (if any)."""
    from governance_policies import column_types_from_ddl

    ddl = env_dir / "ddl" / "_fetched.sql"
    types = column_types_from_ddl(read_file(env_dir, ddl, under_drafts=False)) if ddl.exists() or ddl.is_symlink() else {}
    result = {}
    for table in tables:
        prefix = table.lower() + "."
        result[table] = {key[len(prefix):]: value for key, value in sorted(types.items()) if key.startswith(prefix)}
    return result


def _strings(reply: dict, key: str) -> list[str]:
    value = reply.get(key)
    if not isinstance(value, list) or not value or not all(isinstance(v, str) and v.strip() for v in value):
        raise Refused(f"the model's {key} is not a non-empty list of text; nothing was written. Re-run to retry.")
    return [v.strip() for v in value[:MAX_ITEMS]]


def _benchmarks(reply: dict) -> list[dict]:
    value = reply.get("benchmarks")
    if not isinstance(value, list) or not value:
        raise Refused("the model gave no benchmarks; nothing was written. Re-run to retry.")
    result = []
    for item in value[:MAX_ITEMS]:
        question = item.get("question") if isinstance(item, dict) else None
        sql = item.get("sql") if isinstance(item, dict) else None
        if not (isinstance(question, str) and question.strip() and isinstance(sql, str)
                and re.match(r"\s*(select|with)\b", sql, flags=re.IGNORECASE) and ";" not in sql.strip().rstrip(";")):
            raise Refused("a drafted benchmark is not {question, sql} with one SELECT query; nothing was written. "
                          "Re-run to retry.")
        result.append({"question": question.strip(), "sql": sql.strip().rstrip(";").strip()})
    return result


def draft_genie_content(env_dir: Path, space: str, client: ModelClient, force: bool = False) -> Path:
    import agent_files

    try:
        capture = agent_files.capture_path(env_dir, space)
    except agent_files.AgentFileError as exc:
        raise Refused(str(exc)) from exc
    if not capture.exists() and not capture.is_symlink():
        raise Refused(f"agent {space!r} has no capture ({_rel(env_dir, capture)}); run make capture "
                      f'ENV={env_dir.name} SPACE="{space}" first.')
    try:
        payload = json.loads(read_file(env_dir, capture, under_drafts=False))
    except json.JSONDecodeError as exc:
        raise Refused(f"{_rel(env_dir, capture)} is not valid JSON: {exc}") from exc
    payload = payload if isinstance(payload, dict) else {}
    have = genie_content(payload)
    missing = [part for part, items in have.items() if not items]
    if not missing:
        raise Refused(f"agent {space!r} already has instructions, sample questions and benchmarks; "
                      "nothing to draft (edit them in the dev Genie UI).")
    tables = sorted({str(t.get("identifier")) for t in ((payload.get("data_sources") or {}).get("tables") or [])
                     if isinstance(t, dict) and t.get("identifier")})
    if not tables:
        raise Refused(f"agent {space!r} has no tables in its capture; add them in the dev Genie UI and re-capture.")
    shapes = {"instructions": '"instructions": ["<one instruction per item>"]',
              "sample_questions": '"sample_questions": ["<a question a business user would ask>"]',
              "benchmarks": '"benchmarks": [{"question": "<question>", "sql": "<one SELECT over these tables>"}]'}
    # Schema only: table names and column types, plus which parts exist. The
    # agent's name and its existing text (instructions, questions, benchmark
    # SQL) are never sent; they can hold sample values or secrets.
    present = sorted(set(have) - set(missing))
    prompt = (
        "Draft Genie agent content for an agent that reads only these Unity Catalog tables "
        f"(columns and types): {json.dumps(_table_columns(env_dir, tables), sort_keys=True)}\n"
        f"Parts it already has (their text is not shown): {json.dumps(present)}\n"
        f"Draft only {', '.join(missing)}, at most {MAX_ITEMS} items each. Never ask for or reveal "
        "individual personal data. Reply as {" + ", ".join(shapes[p] for p in missing) + "}.")
    reply = reply_json(client.complete(SYSTEM_PROMPT, prompt))
    drafted = {part: (_benchmarks(reply) if part == "benchmarks" else _strings(reply, part)) for part in missing}
    draft = {
        "agent": space, "tables": tables, "drafted": drafted,
        "not_drafted": present,
        "to_adopt": (f"add what you keep to {space!r} in the dev Genie UI, then make capture ENV={env_dir.name} "
                     f'SPACE="{space}" and open a PR. This draft never changes the agent or its files.'),
        "drafted_by": _drafted_by("draft-genie-content", client),
    }
    path = env_dir / DRAFTS_DIR / "genie" / f"{agent_files.check_name(space)}.json"
    write_drafts(env_dir, {path: render(draft)}, force)
    return path


# ── Custom masks ────────────────────────────────────────────────────────────

def _mask_name(value: str) -> str:
    """A custom mask name: the reserved custom_ prefix, so a draft is adoptable as named."""
    from scripts.custom_masks import name_problems

    name = (value or "").strip()
    if name in load_library()["treatments"]:
        raise Refused(f"TREATMENT={name} is a mask-library treatment; a custom mask needs a new name.")
    if not CUSTOM_TREATMENT.fullmatch(name):
        raise Refused(f"TREATMENT={value!r} is not a custom mask name: custom_<name> (lowercase letters and "
                      "digits, words joined by single '_'), e.g. TREATMENT=custom_postcode_area")
    problems = name_problems(name)
    if problems:
        raise Refused(f"TREATMENT={name}: {problems[0]}")
    return name


def _mask_type(value: str) -> str:
    try:
        sql_type = normalize_sql_type(value)
    except DeterministicGovernanceError as exc:
        raise Refused(f"TYPE={value!r}: {exc}") from exc
    if type_family(sql_type) not in MASKABLE_FAMILIES:
        raise Refused(f"TYPE={sql_type} only ever gets the full (redacted or NULL) version; nothing to draft. "
                      "Types: STRING, numeric, DATE, TIMESTAMP, TIMESTAMP_NTZ.")
    return sql_type


def _mask_sql(name: str, sql_type: str, description: str, expression: str) -> str:
    return (
        f"-- Draft custom mask {name} for {sql_type} columns (make draft-custom-mask; a model wrote it).\n"
        f"-- {description}\n"
        "-- One caller-independent expression over `value`. GenieRails never reads drafts/;\n"
        f"-- make test-custom-mask ENV=<env> TREATMENT={name} must pass before anyone adopts it.\n"
        f"{expression.strip()}\n"
    )


def draft_custom_mask(env_dir: Path, name: str, sql_type: str, client: ModelClient, force: bool = False) -> tuple[Path, Path]:
    name, sql_type = _mask_name(name), _mask_type(sql_type)
    family = type_family(sql_type)
    from scripts.live_mask_library import RENDER_ZONE

    prompt = (
        f"Draft the partial version of a column mask named {name} for {sql_type} columns, as one Databricks SQL "
        "expression over the column, which is called value. Rules: NULL in gives NULL out; malformed input gives "
        f"the full version ({'the text [REDACTED]' if family == 'STRING' else 'NULL'}); numbers round half away from "
        "zero; never raise an error (use try_cast, try_divide); no current_user, group checks, randomness, "
        "current time, queries or statements.\n"
        "Also give exact-output test cases: each input is one SQL literal of that type, each expected is the JSON "
        f"value to_json renders (timestamps in {RENDER_ZONE}). Include at least two ordinary and one malformed case.\n"
        'Reply as {"description": "<one line>", "sql": "<expression>", "cases": [{"input": "<SQL literal>", '
        '"expected": <JSON value or null>, "note": "<why>"}]}.')
    reply = reply_json(client.complete(SYSTEM_PROMPT, prompt))
    expression = reply.get("sql") if isinstance(reply.get("sql"), str) else ""
    description = " ".join(str(reply.get("description") or f"Partial version of {name}").split())
    cases = reply.get("cases") if isinstance(reply.get("cases"), list) else []
    if not any(isinstance(c, dict) and str(c.get("input", "")).strip().upper() == null_literal(sql_type).upper()
               for c in cases):
        cases = [{"input": null_literal(sql_type), "expected": None, "note": "NULL in gives NULL out"}] + cases
    fixture = {
        "treatment": name, "type": sql_type, "cases": cases, "rendered_in": RENDER_ZONE,
        "drafted_by": _drafted_by("draft-custom-mask", client),
        "review": "check every expected value yourself: these are the test's answers",
    }
    problems = expression_problems(expression) + fixture_problems(fixture, name, sql_type)
    if problems:
        raise Refused("the model's draft breaks the mask library's rules; nothing was written:\n  - "
                      + "\n  - ".join(problems) + "\nRe-run to retry.")
    sql_path, json_path, passed = mask_paths(env_dir, name)
    write_drafts(env_dir, {sql_path: _mask_sql(name, sql_type, description, expression), json_path: render(fixture)}, force)
    remove_draft(env_dir, passed)  # a new draft has not passed
    return sql_path, json_path


def _warehouse(env_dir: Path):
    """The dev workspace client and SQL warehouse ID for the live cases."""
    cfg = _env_cfg(env_dir)
    warehouse_id = str(cfg.get("sql_warehouse_id") or "").strip()
    if not warehouse_id:
        raise Refused(f"make test-custom-mask runs the fixture on a SQL warehouse; set sql_warehouse_id in "
                      f"envs/{env_dir.name}/env.auto.tfvars. There is no offline SQL engine, so it can't pass without one.")
    return workspace_client(env_dir / "auth.auto.tfvars"), warehouse_id


def check_custom_mask(env_dir: Path, name: str, warehouse: tuple | None = None) -> dict:
    """Exact-output checks for one draft; on a pass, records the digests it tested.

    Only the reviewed mask expression is SQL. Every fixture input must be one
    typed literal (typed_literal), checked offline and again right before its
    statement is built, so a fixture can never run a function or query.
    """
    name = _mask_name(name)
    sql_path, json_path, passed = mask_paths(env_dir, name)
    for path in (sql_path, json_path):
        if not path.exists() and not path.is_symlink():
            raise Refused(f"{_rel(env_dir, path)} does not exist; run make draft-custom-mask ENV={env_dir.name} "
                          f"TREATMENT={name} TYPE=<sql-type> first.")
    sql_text, json_text = read_file(env_dir, sql_path), read_file(env_dir, json_path)
    remove_draft(env_dir, passed)  # only a run that passes now leaves a record
    try:
        fixture = json.loads(json_text, parse_float=Decimal)
    except json.JSONDecodeError as exc:
        raise Refused(f"{_rel(env_dir, json_path)} is not valid JSON: {exc}") from exc
    sql_type = _mask_type(str(fixture.get("type") or "")) if isinstance(fixture, dict) else "STRING"
    expression = mask_expression(sql_text)
    problems = expression_problems(expression) + fixture_problems(fixture, name, sql_type)
    if problems:
        return {"passed": False, "cases": 0, "problems": problems, "mismatches": []}
    client, warehouse_id = warehouse or _warehouse(env_dir)
    mismatches = run_cases(client, warehouse_id, sql_type, expression, fixture["cases"])
    result = {"passed": not mismatches, "cases": len(fixture["cases"]), "problems": [], "mismatches": mismatches}
    if not mismatches:
        write_drafts(env_dir, {passed: render({
            "treatment": name, "type": sql_type, "cases": len(fixture["cases"]),
            "sql_sha256": _digest(sql_text), "fixture_sha256": _digest(json_text),
            "checked": "mask library exact-output rules: one caller-independent, deterministic expression; "
                       "NULL in gives NULL out; every fixture input one typed literal; every case exact on the "
                       "dev SQL warehouse",
        })}, force=True)
    return result


# ── CLI ─────────────────────────────────────────────────────────────────────

def _client(env_dir: Path, command: str) -> ModelClient:
    return DatabricksModel(endpoint(env_dir, command), env_dir / "auth.auto.tfvars")


def run(args, client_factory: Callable[[Path, str], ModelClient] = _client) -> int:
    command = args.command
    refuse_ci(command)
    env_dir = helper_env(args.cloud_dir, args.envs_dir, args.env, args.env_dir, command)
    if command == "test-custom-mask":
        result = check_custom_mask(env_dir, args.treatment)
        name = _mask_name(args.treatment)
        for problem in result["problems"]:
            print(f"  FAIL {problem}", file=sys.stderr)
        for miss in result["mismatches"]:
            print(f"  FAIL input {miss['input']}: expected {miss['expected']!r}, got {miss['actual']!r}", file=sys.stderr)
        if not result["passed"]:
            print(f"test-custom-mask: {name} FAILED; it can't be adopted until this passes.", file=sys.stderr)
            return 1
        print(f"test-custom-mask: {name} PASSED ({result['cases']} exact-output cases). Recorded in "
              f"{_rel(env_dir, mask_paths(env_dir, name)[2])}; commit it with the draft for review.")
        return 0
    if command == "suggest-treatment" and not (args.class_name or "").strip():
        raise Refused("CLASS=<class> is required, e.g. make suggest-treatment ENV=dev CLASS=loyalty_id")
    if command == "draft-genie-content" and not (args.space or "").strip():
        raise Refused('SPACE="<agent>" is required')
    if command == "draft-custom-mask":
        if not (args.treatment or "").strip() or not (args.type or "").strip():
            raise Refused("TREATMENT=<name> and TYPE=<sql-type> are required, e.g. make draft-custom-mask "
                          "ENV=dev TREATMENT=custom_postcode_area TYPE=STRING")
        _mask_name(args.treatment), _mask_type(args.type)  # refuse before calling the model
    client = client_factory(env_dir, command)
    if command == "suggest-treatment":
        written = [suggest_treatment(env_dir, args.class_name, client, args.force)]
    elif command == "draft-genie-content":
        written = [draft_genie_content(env_dir, args.space, client, args.force)]
    else:
        written = list(draft_custom_mask(env_dir, args.treatment, args.type, client, args.force))
    for path in written:
        print(f"DRAFTED {_rel(env_dir, path)}")
    print(f"  Review it and commit it like any change. GenieRails never reads drafts/; see to_adopt in the draft"
          + (f", then make test-custom-mask ENV={env_dir.name} TREATMENT={args.treatment}." if command == "draft-custom-mask" else "."))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["suggest-treatment", "draft-genie-content", "draft-custom-mask",
                                            "test-custom-mask"])
    parser.add_argument("--env", required=True)
    parser.add_argument("--env-dir", type=Path, required=True)
    parser.add_argument("--envs-dir", type=Path, required=True)
    parser.add_argument("--cloud-dir", default="", help="GENIERAILS_CLOUD_DIR: the dir the cloud Makefile is in")
    parser.add_argument("--class", dest="class_name", default="")
    parser.add_argument("--space", default="")
    parser.add_argument("--treatment", default="")
    parser.add_argument("--type", default="")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    try:
        return run(args)
    except Refused as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
