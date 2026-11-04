#!/usr/bin/env python3
"""Adopted custom masks (design 18.4.1): governance config, and never a model.

    adopt-custom-mask  make adopt-custom-mask ENV=dev TREATMENT=custom_<name>:
                       copies a draft that make test-custom-mask passed into
                       envs/dev/custom_masks/custom_<name>.json, and prints the
                       tag_treatments line to add. Refuses under CI (no
                       opt-out) and anywhere but this checkout's real
                       envs/dev; it calls no model and edits nothing else.
    live-check         rehearse and release: re-run every adopted mask's cases,
                       read-only, on the env's SQL warehouse.
    promote            promote-to: copy envs/<source>/custom_masks/ to the
                       destination byte for byte (governance-owned).

An adopted mask is one governance-owned JSON file per treatment:

    {"schema_version": 1, "treatment": "custom_<name>", "description": "...",
     "never_raw": false, "full": "redacted",
     "partial": {"<family>": {"sql": "<one expression over value>",
                              "tested_type": "<SQL type>",
                              "cases": [{"input": "<SQL literal>", "expected": <JSON value>}],
                              "tested_sha256": "<sha256 of [treatment, tested_type, sql, cases]>",
                              "adopted_from": {"sql_sha256": "...", "fixture_sha256": "..."}}}}

full is always the library's redacted version ('[REDACTED]', or NULL for
non-string types), so tier 3 and anyone outside the tiers never meet custom
SQL; never_raw is always false (a never-raw class maps to the library's
secret). validate and check recompute tested_sha256, so SQL edited by hand
can't govern until it is re-tested and re-adopted.

effective_library(env_dir) is the one lookup generate, validate, scaffold and
verify-access use: the mask library plus the env's adopted masks. An env with
no custom_masks/ gets the mask library unchanged.

The offline mask rules (expression_problems, fixture_problems) live here so
governance can run them without importing scripts/ai_helpers.py, the module
that holds the model client.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import sys
from decimal import Decimal
from pathlib import Path

import hcl2

SHARED_ROOT = Path(__file__).resolve().parents[1]
for _path in (SHARED_ROOT, SHARED_ROOT / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from governance_policies import (  # noqa: E402
    CUSTOM_TREATMENT, DeterministicGovernanceError, normalize_sql_type,
)
from mask_library import load_library, type_family, typed_body  # noqa: E402

CUSTOM_DIR = "custom_masks"
DRAFTS_DIR = "drafts"
SCHEMA_VERSION = 1
# Long enough for a readable name; short enough that the policy name
# gr_mask_<catalog>_<name>__<type>__<generation>_partial stays inside UC's limit.
MAX_NAME_LENGTH = 64
HELPER_ENV = "dev"
# CI providers the applying-target guard knows, plus other common runners'
# markers (defense in depth; most also set CI). No opt-out.
CI_MARKERS = ("TF_BUILD", "GITHUB_ACTIONS", "JENKINS_URL", "GITLAB_CI", "BUILDKITE", "CIRCLECI",
              "BUILD_BUILDID", "SYSTEM_TEAMFOUNDATIONCOLLECTIONURI", "TEAMCITY_VERSION", "TRAVIS",
              "BITBUCKET_BUILD_NUMBER", "CODEBUILD_BUILD_ID", "DRONE", "SEMAPHORE", "APPVEYOR")
# Families the mask library types its versions for; any other type only ever
# gets the full version, so there is nothing to draft.
MASKABLE_FAMILIES = ("STRING", "NUMERIC", "DATE", "TIMESTAMP", "TIMESTAMP_NTZ")
# A mask is one caller-independent, deterministic expression over `value`.
STATEMENT_WORDS = ("select", "create", "drop", "alter", "insert", "update", "delete", "merge",
                   "grant", "revoke", "truncate", "call", "execute")
CALLER_FUNCTIONS = ("current_user", "session_user", "user", "is_account_group_member", "is_member",
                    "current_groups", "current_principal")
NONDETERMINISTIC_FUNCTIONS = ("rand", "randn", "random", "uuid", "now", "current_timestamp",
                              "current_date", "current_timezone", "shuffle")
# Functions that read secrets, reach the network or other data, or resolve
# names at run time: never in a mask (ai_* is refused by prefix).
SIDE_EFFECT_FUNCTIONS = ("secret", "try_secret", "http_request", "read_files", "read_kafka", "read_kinesis",
                         "read_pubsub", "read_pulsar", "read_statestore", "identifier", "java_method",
                         "reflect", "try_reflect", "vector_search", "url_decode_to_request")
NEVER_RAW_ADVICE = ("if a class must never be raw, map it to the mask library's secret, not a custom mask: "
                    "tier 1, raw_exempt_principals and the deployer see a custom mask's column raw")


class Refused(RuntimeError):
    """A command stopped; nothing was written."""


class CustomMaskError(DeterministicGovernanceError):
    """An adopted custom mask file governance refuses to use."""


# ── Guards (shared with scripts/ai_helpers.py) ──────────────────────────────

def ci_name(environ: dict | None = None) -> str | None:
    """The CI marker that is set, or None."""
    environ = os.environ if environ is None else environ
    if str(environ.get("CI", "")).strip().lower() in {"true", "1", "yes"}:
        return "CI"
    return next((name for name in CI_MARKERS if environ.get(name)), None)


def refuse_ci(command: str, why: str = "the AI helpers never run in CI (design decision 5)") -> None:
    marker = ci_name()
    if marker:
        raise Refused(f"make {command} refused: {marker} is set, and {why}. "
                      "Run it on your own machine against the dev env.")


def helper_env(cloud_dir: Path, envs_dir: Path, env: str, env_dir: Path, command: str,
               who: str = "the AI helpers") -> Path:
    """The env to work in: exactly ENV=dev, whose physical path is <cloud>/envs/dev.

    cloud_dir is the dir the cloud Makefile records from its own location
    (GENIERAILS_CLOUD_DIR); envs/ and envs/dev must be real directories there,
    with no symlink and no ENV_DIR or CLOUD_ROOT redirection, and dev must not
    be a promotion target.
    """
    from scripts.coverage_fix import promote_source

    if env != HELPER_ENV:
        why = "it is promoted" if env == "prod" else f"{who} run only in the dev env"
        raise Refused(f"make {command} refused: ENV={env} is not allowed ({why}). Run make {command} "
                      f"ENV={HELPER_ENV}, review the result, and let promotion carry what you adopt.")
    if not str(cloud_dir or "").strip():
        raise Refused(f"make {command} refused: run it through the cloud Makefile (aws/ or azure/), "
                      "which records where envs/ is")
    cloud = Path(os.path.abspath(cloud_dir))
    expected_envs, expected_env = cloud / "envs", cloud / "envs" / HELPER_ENV
    if Path(os.path.abspath(envs_dir)) != expected_envs or Path(os.path.abspath(env_dir)) != expected_env:
        raise Refused(f"make {command} refused: ENV_DIR={env_dir} is not envs/{HELPER_ENV} of this checkout "
                      f"({expected_env}); drop ENV_DIR and CLOUD_ROOT")
    for path in (expected_envs, expected_env):
        if path.is_symlink() or not path.is_dir():
            raise Refused(f"make {command} refused: {path} is a symlink or not a directory; {who} "
                          "write only inside a real envs/dev")
    if os.path.realpath(expected_env) != os.path.join(os.path.realpath(cloud), "envs", HELPER_ENV):
        raise Refused(f"make {command} refused: {expected_env} does not resolve to envs/{HELPER_ENV} of this checkout")
    env_file = expected_env / "env.auto.tfvars"
    if env_file.is_symlink() or not env_file.is_file():
        raise Refused(f"make {command} refused: envs/{HELPER_ENV}/env.auto.tfvars is missing or a symlink; "
                      f"run make setup ENV={HELPER_ENV}.")
    source = promote_source(expected_env)
    if source:
        raise Refused(f"make {command} refused: envs/{HELPER_ENV} is promoted (its promote_from is {source}); "
                      f"{who} work only in the env rules are promoted from.")
    return expected_env


# ── Contained file access under an env ──────────────────────────────────────
# Every path below the env that a command reads, writes or removes must have
# no symlink in any component (the rule promote-to and check apply, #88/#119),
# and writes land only inside the named subfolder of the real env.

def rel(env_dir: Path, path: Path) -> str:
    return f"envs/{Path(env_dir).name}/{Path(path).relative_to(env_dir)}"


def contained(env_dir: Path, path: Path, *, under: str | None, directory: bool = False) -> Path:
    """path, after refusing a symlink or wrong file type in any component below env_dir."""
    env_dir, path = Path(env_dir), Path(path)
    relative = path.relative_to(env_dir)
    if ".." in relative.parts or (under and relative.parts[:1] != (under,)):
        raise Refused(f"{path} is not inside envs/{env_dir.name}/{under or ''}")
    current = env_dir
    for i, part in enumerate(relative.parts):
        current = current / part
        if current.is_symlink():
            raise Refused(f"{rel(env_dir, current)} is a symlink; GenieRails reads and writes only real "
                          f"files inside envs/{env_dir.name}. Replace it with the file or directory it points to.")
        want_file = i == len(relative.parts) - 1 and not directory
        if current.exists() and not (current.is_file() if want_file else current.is_dir()):
            raise Refused(f"{rel(env_dir, current)} is not a {'regular file' if want_file else 'directory'}")
    root = os.path.realpath(env_dir / (under or ""))
    real = os.path.realpath(path)
    if not (real.startswith(root + os.sep) or (directory and real == root)):
        raise Refused(f"{path} resolves outside {root}")
    return path


def read_text(env_dir: Path, path: Path, *, under: str | None) -> str:
    """A regular file's text, refusing symlinks (also at open time, O_NOFOLLOW)."""
    contained(env_dir, path, under=under)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, encoding="utf-8") as handle:
        return handle.read()


def make_dirs(env_dir: Path, folder: Path, *, under: str) -> None:
    current = Path(env_dir)
    for part in folder.relative_to(env_dir).parts:
        current = current / part
        if not current.exists() and not current.is_symlink():
            current.mkdir()
        contained(env_dir, current, under=under, directory=True)


def write_files(env_dir: Path, files: dict[Path, str], *, under: str) -> None:
    """Write every file or none, inside envs/<env>/<under>/ only.

    Each file is staged beside its destination (created exclusively, never
    following a link) and moved into place only after every stage succeeded.
    """
    for path in files:
        contained(env_dir, path, under=under)
    staged: list[tuple[Path, Path]] = []
    try:
        for path, text in files.items():
            make_dirs(env_dir, path.parent, under=under)
            temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
            staged.append((temp, path))
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
        for temp, path in staged:
            contained(env_dir, path, under=under)  # nothing swapped in a link meanwhile
        for temp, path in staged:
            os.replace(temp, path)
        staged = []
    finally:
        for temp, _path in staged:
            temp.unlink(missing_ok=True)


def render(value: dict) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ── The mask library's rules, offline ───────────────────────────────────────

def _strip_sql(body: str) -> str:
    """The expression's tokens without comments and string literals, for the token rules.

    It uses the same fail-closed tokenizer as generation, so a '--' or '/*'
    inside a literal can't hide what follows it. SQL that can't be tokenized
    (an open literal or comment) keeps a ';', which every rule refuses.
    """
    from sql_tokenizer import SqlTokenizeError, sql_tokens

    try:
        tokens = sql_tokens(body)
    except SqlTokenizeError:
        return ";"
    return " ".join("''" if token[:1] in {"'", '"', "$"} else token.lower() for token in tokens)


def mask_expression(sql_text: str) -> str:
    """The mask expression in a draft .sql file (comment lines dropped)."""
    return "\n".join(line for line in sql_text.splitlines() if not line.lstrip().startswith("--")).strip()


def expression_problems(expression: str) -> list[str]:
    """The mask library's rules for a mask body, checked offline."""
    if not expression.strip():
        return ["the mask has no expression"]
    bare = _strip_sql(expression).lower()
    problems = []
    if ";" in bare:
        problems.append("the mask must be one expression (no ';', and every literal and comment closed)")
    for word in STATEMENT_WORDS:
        if re.search(rf"\b{word}\b", bare):
            problems.append(f"the mask must be an expression over value, not a query or statement ({word.upper()})")
    if re.search(r"\bai_\w*\s*\(", bare):
        problems.append("the mask must not call a model: ai_* functions are not allowed")
    if not expression.isascii():
        problems.append("the mask must be ASCII (no look-alike letters)")
    for names, rule in ((CALLER_FUNCTIONS, "caller-independent (the tier policies decide who sees which version)"),
                        (NONDETERMINISTIC_FUNCTIONS, "deterministic"),
                        (SIDE_EFFECT_FUNCTIONS, "free of secrets, network and data access")):
        for name in names:
            # current_user and the other current_* forms are valid without parentheses.
            pattern = rf"\b{name}\b" if name.startswith(("current_", "session_")) else rf"\b{name}\s*\("
            if re.search(pattern, bare):
                problems.append(f"the mask must be {rule}: {name} is not allowed")
    if not re.search(r"\bvalue\b", bare):
        problems.append("the mask must read the column as value")
    return problems


def typed_expression(expression: str, sql_type: str) -> str:
    """The mask with the library's outer cast to exactly the column type."""
    return typed_body(expression, sql_type)


def null_literal(sql_type: str) -> str:
    return f"CAST(NULL AS {sql_type})"


def _expected_ok(expected, family: str) -> bool:
    if expected is None:
        return True
    if family == "NUMERIC":
        return isinstance(expected, (int, float, Decimal, str)) and not isinstance(expected, bool) and \
            (not isinstance(expected, str) or re.fullmatch(r"-?\d+(\.\d+)?", expected) is not None)
    return isinstance(expected, str)


# A fixture input is data, never SQL: one literal from this grammar, ASCII
# only (no confusable letters), no identifiers, functions, operators,
# comments or backslash escapes. CAST(<literal> AS <scalar type>) is allowed
# so a case can be typed exactly (CAST('NaN' AS DOUBLE), BIGINT bounds).
_LITERAL_TYPE = (r"(?:STRING|TINYINT|SMALLINT|INT|INTEGER|BIGINT|FLOAT|DOUBLE|DATE|TIMESTAMP|TIMESTAMP_NTZ"
                 r"|DECIMAL\s*\(\s*\d{1,2}\s*(?:,\s*\d{1,2}\s*)?\))")
_NUMBER = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:E[+-]?\d+)?"
_TEXT = r"'(?:[ !#-&(-\[\]-~]|'')*'"  # printable ASCII; a quote only doubled; no backslash
_DATE = r"DATE\s*'\d{4}-\d{2}-\d{2}'"
_TIMESTAMP = r"TIMESTAMP(?:_NTZ)?\s*'\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})?'"
_SCALAR = rf"(?:NULL|{_NUMBER}|{_TEXT}|{_DATE}|{_TIMESTAMP})"
TYPED_LITERAL = re.compile(rf"\s*(?:{_SCALAR}|CAST\s*\(\s*(?:NULL|{_NUMBER}|{_TEXT})\s+AS\s+{_LITERAL_TYPE}\s*\))\s*",
                           re.IGNORECASE | re.ASCII)


def typed_literal(text) -> str | None:
    """text stripped, if it is exactly one typed SQL literal; else None."""
    if not isinstance(text, str) or not text.isascii() or not TYPED_LITERAL.fullmatch(text):
        return None
    return text.strip()


def fixture_problems(fixture, name: str, sql_type: str) -> list[str]:
    if not isinstance(fixture, dict):
        return ["the fixture must be a JSON object"]
    problems = []
    if fixture.get("treatment") != name:
        problems.append(f"the fixture's treatment must be {name}")
    if fixture.get("type") != sql_type:
        problems.append(f"the fixture's type must be {sql_type}")
    cases = fixture.get("cases")
    if not isinstance(cases, list) or not cases:
        return problems + ["the fixture needs cases: [{input, expected}]"]
    family = type_family(sql_type)
    for i, case in enumerate(cases):
        if not isinstance(case, dict) or not isinstance(case.get("input"), str) or "expected" not in case:
            problems.append(f"case {i}: needs input (a SQL literal) and expected")
            continue
        if typed_literal(case["input"]) is None:
            problems.append(f"case {i}: input {case['input']!r} must be one typed SQL literal (NULL, a number, "
                            "'text', DATE'...', TIMESTAMP'...', TIMESTAMP_NTZ'...', or CAST of one of those AS a "
                            "scalar type); fixtures never run functions, identifiers or queries")
        if not _expected_ok(case["expected"], family):
            problems.append(f"case {i}: expected {case['expected']!r} is not a {family} value or null")
    if not any(isinstance(c, dict) and re.sub(r"\s+", " ", str(c.get("input", "")).strip()).upper()
               == null_literal(sql_type).upper() and c.get("expected") is None for c in cases):
        problems.append(f"the fixture must check NULL in gives NULL out: input {null_literal(sql_type)}, expected null")
    if sum(1 for c in cases if isinstance(c, dict) and c.get("expected") is not None) < 2:
        problems.append("the fixture needs at least two cases with a non-null expected value")
    return problems


# ── Drafts (written by scripts/ai_helpers.py; read here only by adopt) ──────

def mask_paths(env_dir: Path, name: str) -> tuple[Path, Path, Path]:
    folder = Path(env_dir) / DRAFTS_DIR / CUSTOM_DIR
    return folder / f"{name}.sql", folder / f"{name}.json", folder / f"{name}.passed.json"


def passed_record(env_dir: Path, name: str) -> dict | None:
    """The test-custom-mask pass record, only if it matches the draft's current bytes."""
    sql_path, json_path, passed = mask_paths(env_dir, name)
    try:
        record = json.loads(read_text(env_dir, passed, under=DRAFTS_DIR))
        return record if record.get("sql_sha256") == digest(read_text(env_dir, sql_path, under=DRAFTS_DIR)) and \
            record.get("fixture_sha256") == digest(read_text(env_dir, json_path, under=DRAFTS_DIR)) else None
    except (OSError, ValueError, AttributeError, Refused):
        return None


# ── Adopted masks: the file rules and the effective library ─────────────────

def name_problems(name, library: dict | None = None) -> list[str]:
    """Why ``name`` can't be a custom treatment (empty if it can)."""
    data = library or load_library()
    if not isinstance(name, str) or not CUSTOM_TREATMENT.fullmatch(name):
        return [f"{name!r} is not a custom mask name: it must be custom_<name> (lowercase letters and digits, "
                "words joined by single '_', no '__')"]
    if len(name) > MAX_NAME_LENGTH:
        return [f"{name} is longer than {MAX_NAME_LENGTH} characters"]
    if name in data["treatments"] or name in data["versions"]:
        return [f"{name} is a mask-library name; a custom mask needs its own"]
    return []


def tested_sha256(treatment: str, tested_type: str, sql: str, cases: list) -> str:
    """The digest of what test-custom-mask proved: the treatment, type, SQL and cases."""
    return digest(json.dumps([treatment, tested_type, sql, cases], sort_keys=True,
                             separators=(",", ":"), ensure_ascii=True))


def mask_file_problems(data, name: str, library: dict | None = None) -> list[str]:
    """The design 18.4.1 rules for one adopted mask file (the same rules adoption checks)."""
    if not isinstance(data, dict):
        return ["must be a JSON object"]
    problems = list(name_problems(name, library))
    if data.get("schema_version") != SCHEMA_VERSION:
        problems.append(f"schema_version must be {SCHEMA_VERSION}")
    if data.get("treatment") != name:
        problems.append(f"treatment must be {name} (the file name)")
    if data.get("never_raw") is not False:
        problems.append(f"never_raw must be false; {NEVER_RAW_ADVICE}")
    if data.get("full") != "redacted":
        problems.append('full must be "redacted": the full version is always the library\'s [REDACTED] or NULL')
    partial = data.get("partial")
    if not isinstance(partial, dict) or not partial:
        return problems + ["partial must map at least one type family to {sql, tested_type, cases, tested_sha256}"]
    for family, entry in sorted(partial.items()):
        label = f"partial[{family!r}]"
        if family not in MASKABLE_FAMILIES:
            problems.append(f"{label}: type families are {', '.join(MASKABLE_FAMILIES)}")
            continue
        if not isinstance(entry, dict) or not isinstance(entry.get("sql"), str):
            problems.append(f"{label} must hold sql, tested_type, cases and tested_sha256")
            continue
        try:
            sql_type = normalize_sql_type(str(entry.get("tested_type") or ""))
        except DeterministicGovernanceError as exc:
            problems.append(f"{label}.tested_type: {exc}")
            continue
        if sql_type != entry.get("tested_type") or type_family(sql_type) != family:
            problems.append(f"{label}.tested_type must be an exact {family} type (it is {entry.get('tested_type')!r})")
            continue
        problems.extend(f"{label}: {p}" for p in expression_problems(entry["sql"]))
        fixture = {"treatment": name, "type": sql_type, "cases": entry.get("cases")}
        problems.extend(f"{label}: {p}" for p in fixture_problems(fixture, name, sql_type))
        if entry.get("tested_sha256") != tested_sha256(name, sql_type, entry["sql"], entry.get("cases")):
            problems.append(f"{label}: tested_sha256 doesn't match its sql and cases, so this isn't what "
                            f"make test-custom-mask passed. Re-test and re-adopt it: make test-custom-mask, "
                            f"then make adopt-custom-mask ENV=dev TREATMENT={name} FORCE=1")
    return problems


def custom_dir(env_dir: Path) -> Path:
    return Path(env_dir) / CUSTOM_DIR


def load_custom_masks(env_dir: Path, library: dict | None = None) -> dict[str, dict]:
    """{treatment: file data} for every adopted mask of an env; refuses any invalid one."""
    env_dir = Path(env_dir)
    folder = custom_dir(env_dir)
    if not folder.exists() and not folder.is_symlink():
        return {}
    problems, masks = [], {}
    try:
        contained(env_dir, folder, under=CUSTOM_DIR, directory=True)
        names = sorted(os.listdir(folder))
    except (Refused, OSError) as exc:
        raise CustomMaskError(f"{rel(env_dir, folder)}: {exc}") from exc
    for entry in names:
        path = folder / entry
        label = rel(env_dir, path)
        if not entry.endswith(".json") or entry.startswith("."):
            problems.append(f"{label}: {CUSTOM_DIR}/ holds only custom_<name>.json files")
            continue
        try:
            data = json.loads(read_text(env_dir, path, under=CUSTOM_DIR))
        except (Refused, OSError, ValueError) as exc:
            problems.append(f"{label}: {exc}")
            continue
        name = entry.removesuffix(".json")
        problems.extend(f"{label}: {p}" for p in mask_file_problems(data, name, library))
        masks[name] = data
    if problems:
        raise CustomMaskError("adopted custom masks are invalid:\n  - " + "\n  - ".join(problems))
    return masks


def effective_library(env_dir: Path | None, library: dict | None = None) -> dict:
    """The mask library plus the env's adopted custom masks (the mask library alone if it has none).

    Each adopted mask becomes a treatment with no classes of its own (classes
    reach it through tag_treatments), whose partial version is named after it
    for every family it was tested on and whose full version is redacted;
    custom_bodies holds its expressions.
    """
    base = library or load_library()
    masks = load_custom_masks(env_dir, base) if env_dir is not None else {}
    if not masks:
        return base
    data = copy.deepcopy(base)
    bodies = data.setdefault("custom_bodies", {})
    for name, mask in sorted(masks.items()):
        families = sorted(mask["partial"])
        data["treatments"][name] = {"classes": [], "partial": name, "partial_by_type": {f: name for f in families},
                                    "full": "redacted", "types": families}
        bodies[name] = {family: entry["sql"] for family, entry in mask["partial"].items()}
    return data


def custom_treatments(library: dict) -> frozenset[str]:
    """The adopted custom masks an effective library holds."""
    return frozenset(library.get("custom_bodies") or ())


def env_errors(env_dir: Path, cfg: dict) -> tuple[dict | None, list[str], list[str]]:
    """(effective library, errors, warnings) of an env's custom_masks/ for validate and check."""
    try:
        library = effective_library(env_dir)
    except CustomMaskError as exc:
        return None, [str(exc)], []
    names = custom_treatments(library)
    if not names:
        return library, [], []
    errors, warnings = [], []
    if cfg.get("governance_mode") != "deterministic":
        errors.append(f"envs/{Path(env_dir).name}/{CUSTOM_DIR}/ needs governance_mode = \"deterministic\"")
    never_raw = set(load_library().get("never_raw_classes") or ())
    mapped = dict(cfg.get("tag_treatments") or {}) if isinstance(cfg.get("tag_treatments"), dict) else {}
    for key, value in sorted(mapped.items()):
        if value in names and str(key).removeprefix("class.") in never_raw:
            errors.append(f'tag_treatments["{key}"] maps a never-raw class to {value}; {NEVER_RAW_ADVICE}')
    used = set(mapped.values()) | {rule.get("treatment") for rule in (cfg.get("column_overrides") or {}).values()
                                   if isinstance(rule, dict)}
    for name in sorted(names - used):
        warnings.append(f"{CUSTOM_DIR}/{name}.json is adopted, but no tag_treatments or column_overrides entry names it")
    return library, errors, warnings


# ── adopt-custom-mask ───────────────────────────────────────────────────────

def _env_cfg(env_dir: Path) -> dict:
    try:
        return hcl2.loads(read_text(env_dir, Path(env_dir) / "env.auto.tfvars", under=None))
    except Refused:
        raise
    except Exception as exc:  # hcl2 raises lark errors
        raise Refused(f"envs/{Path(env_dir).name}/env.auto.tfvars is not valid HCL: {exc}") from exc


def adopt(env_dir: Path, name: str, force: bool = False) -> tuple[Path, str]:
    """Adopt a passed draft into custom_masks/; returns (file, tag_treatments line). Any failure writes nothing."""
    library = load_library()
    problems = name_problems((name or "").strip(), library)
    if problems:
        raise Refused(f"TREATMENT={name!r}: {problems[0]}. Draft it under a custom_<name> name "
                      "(make draft-custom-mask ENV=dev TREATMENT=custom_<name> TYPE=<type>).")
    name = name.strip()
    sql_path, json_path, passed = mask_paths(env_dir, name)
    for path in (sql_path, json_path):
        if not path.exists() and not path.is_symlink():
            raise Refused(f"{rel(env_dir, path)} does not exist; draft it with make draft-custom-mask ENV=dev "
                          f"TREATMENT={name} TYPE=<sql-type>, then make test-custom-mask ENV=dev TREATMENT={name}.")
    record = passed_record(env_dir, name)
    if record is None:
        why = "has no pass record" if not passed.exists() and not passed.is_symlink() else \
            "has a pass record that doesn't match the draft files (they changed after the test, or it was edited)"
        raise Refused(f"{rel(env_dir, sql_path)} {why}. Run make test-custom-mask ENV=dev TREATMENT={name}; "
                      "only exactly what passed can be adopted.")
    sql_text = read_text(env_dir, sql_path, under=DRAFTS_DIR)
    json_text = read_text(env_dir, json_path, under=DRAFTS_DIR)
    try:
        fixture = json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise Refused(f"{rel(env_dir, json_path)} is not valid JSON: {exc}") from exc
    raw_type = str(fixture.get("type") or "") if isinstance(fixture, dict) else ""
    try:
        sql_type = normalize_sql_type(raw_type)
    except DeterministicGovernanceError as exc:
        raise Refused(f"{rel(env_dir, json_path)}: {exc}") from exc
    family = type_family(sql_type)
    expression = mask_expression(sql_text)
    problems = expression_problems(expression) + fixture_problems(fixture, name, sql_type)
    if family not in MASKABLE_FAMILIES:
        problems.append(f"{sql_type} only ever gets the full version; there is nothing to adopt")
    if record.get("type") != sql_type:
        problems.append(f"the pass record tested type {record.get('type')!r}, not {sql_type}")
    if problems:
        raise Refused(f"{name} breaks the mask library's rules, so it can't be adopted:\n  - "
                      + "\n  - ".join(problems))
    cfg = _env_cfg(env_dir)
    mapped = cfg.get("tag_treatments") if isinstance(cfg.get("tag_treatments"), dict) else {}
    never_raw = set(library.get("never_raw_classes") or ())
    clash = sorted(key for key, value in mapped.items() if value == name and str(key).removeprefix("class.") in never_raw)
    if clash:
        raise Refused(f"tag_treatments maps never-raw class(es) {', '.join(clash)} to {name}; {NEVER_RAW_ADVICE}.")

    cases = [{"input": case["input"], "expected": case["expected"]} for case in fixture["cases"]]
    entry = {"sql": expression, "tested_type": sql_type, "cases": cases,
             "tested_sha256": tested_sha256(name, sql_type, expression, cases),
             "adopted_from": {"sql_sha256": digest(sql_text), "fixture_sha256": digest(json_text)}}
    target = custom_dir(env_dir) / f"{name}.json"
    if target.exists() or target.is_symlink():
        try:
            existing = json.loads(read_text(env_dir, target, under=CUSTOM_DIR))
        except (ValueError, OSError) as exc:
            raise Refused(f"{rel(env_dir, target)} is not valid JSON: {exc}") from exc
        problems = mask_file_problems(existing, name, library)
        if problems:
            raise Refused(f"{rel(env_dir, target)} is invalid, so adoption can't merge into it:\n  - "
                          + "\n  - ".join(problems))
        prior = existing["partial"].get(family)
        if prior is not None and {k: prior.get(k) for k in ("sql", "tested_type", "cases")} != \
                {k: entry[k] for k in ("sql", "tested_type", "cases")} and not force:
            raise Refused(f"{rel(env_dir, target)} already has a different {family} mask. Replacing it is a new "
                          "mask generation that make release-plan reports as a weakening to acknowledge "
                          "(custom_mask_changed); re-run with FORCE=1 to replace it.")
        data = existing
    else:
        description = next((line.lstrip("-").strip() for line in sql_text.splitlines()[1:2]
                            if line.lstrip().startswith("--")), "")
        data = {"schema_version": SCHEMA_VERSION, "treatment": name, "description": description,
                "never_raw": False, "full": "redacted", "partial": {}}
    data["partial"][family] = entry
    problems = mask_file_problems(data, name, library)
    if problems:  # unreachable after the checks above; never write a file validate would refuse
        raise Refused("the adopted file would be invalid:\n  - " + "\n  - ".join(problems))
    write_files(env_dir, {target: render(data)}, under=CUSTOM_DIR)
    return target, f'"class.<your_class>" = "{name}"'


# ── live-check: rehearse and release ────────────────────────────────────────

def workspace_client(auth_file: Path):
    """A WorkspaceClient pinned to the env's auth.auto.tfvars."""
    from scripts.auth_configured import workspace_client as explicit_client
    try:
        return explicit_client(auth_file)
    except RuntimeError as exc:
        raise Refused(str(exc)) from None


def run_cases(client, warehouse_id: str, sql_type: str, expression: str, cases: list) -> list[dict]:
    """Every case of one mask on a SQL warehouse, read-only (inline SELECTs); the mismatches.

    Only the reviewed expression is SQL: each input must be one typed literal,
    checked again right before its statement is built.
    """
    from scripts.live_mask_library import TO_JSON_OPTIONS, _execute, comparable

    typed = typed_expression(expression, sql_type)
    mismatches = []
    for case in cases:
        literal = typed_literal(case["input"])
        if literal is None:  # unreachable after fixture_problems; never build a statement from it
            raise Refused(f"fixture input {case['input']!r} is not one typed SQL literal")
        statement = (f"SELECT to_json(named_struct('v', {typed}), {TO_JSON_OPTIONS}) "
                     f"FROM (SELECT {literal} AS value)")
        try:
            rows = _execute(client, warehouse_id, statement)
            actual = json.loads(rows[0][0], parse_float=Decimal).get("v") if rows else "<no row>"
        except Exception as exc:  # a mask that errors fails, like one that returns the wrong value
            actual = f"<error: {exc}>"
        if comparable(actual) != comparable(case["expected"]):
            mismatches.append({"input": literal, "expected": case["expected"], "actual": actual})
    return mismatches


def live_check(env_dir: Path, env: str, warehouse=None) -> int:
    """Re-run every adopted mask's cases on the env's warehouse; 0 when all pass (or there are none)."""
    masks = load_custom_masks(env_dir)
    if not masks:
        return 0
    print(f"=== Custom mask cases ({env}): {len(masks)} adopted mask(s), read-only on the SQL warehouse ===")
    if warehouse is None:
        warehouse_id = str(_env_cfg(env_dir).get("sql_warehouse_id") or "").strip()
        if not warehouse_id:
            raise Refused(f"envs/{env}/{CUSTOM_DIR}/ has adopted masks, whose cases run on a SQL warehouse; "
                          f"set sql_warehouse_id in envs/{env}/env.auto.tfvars")
        warehouse = (workspace_client(Path(env_dir) / "auth.auto.tfvars"), warehouse_id)
    client, warehouse_id = warehouse
    failed = 0
    for name, mask in sorted(masks.items()):
        for family, entry in sorted(mask["partial"].items()):
            misses = run_cases(client, warehouse_id, entry["tested_type"], entry["sql"], entry["cases"])
            for miss in misses:
                print(f"  FAIL {name} ({entry['tested_type']}) input {miss['input']}: expected "
                      f"{miss['expected']!r}, got {miss['actual']!r}", file=sys.stderr)
            if misses:
                failed += 1
            else:
                expected = [case["expected"] for case in entry["cases"]]
                print(f"  PASS {name} ({entry['tested_type']}): {len(entry['cases'])} exact-output cases; "
                      f"expected outputs include {expected[1]!r}")
    if failed:
        print(f"custom mask cases FAILED on envs/{env}'s warehouse (above); nothing was applied. A custom mask "
              "must give exactly its tested output in every env it governs.", file=sys.stderr)
        return 1
    return 0


# ── promote: promote-to copies custom_masks/ verbatim ───────────────────────

def promote(source_dir: Path, dest_dir: Path) -> list[str]:
    """Make dest's custom_masks/ a byte-for-byte copy of source's; the files it changed."""
    source_dir, dest_dir = Path(source_dir), Path(dest_dir)
    src, dest = custom_dir(source_dir), custom_dir(dest_dir)
    if not (src.exists() or src.is_symlink() or dest.exists() or dest.is_symlink()):
        return []
    wanted: dict[str, bytes] = {}
    if src.exists() or src.is_symlink():
        contained(source_dir, src, under=CUSTOM_DIR, directory=True)
        for entry in sorted(os.listdir(src)):
            contained(source_dir, src / entry, under=CUSTOM_DIR)
            wanted[entry] = (src / entry).read_bytes()
    have: dict[str, bytes] = {}
    if dest.exists() or dest.is_symlink():
        contained(dest_dir, dest, under=CUSTOM_DIR, directory=True)
        for entry in sorted(os.listdir(dest)):
            contained(dest_dir, dest / entry, under=CUSTOM_DIR)
            have[entry] = (dest / entry).read_bytes()
    changed = sorted(name for name in wanted.keys() | have.keys() if wanted.get(name) != have.get(name))
    if not changed:
        return []
    if not wanted:
        shutil.rmtree(dest)
        return changed
    write_files(dest_dir, {dest / name: wanted[name].decode("utf-8") for name in changed if name in wanted},
                under=CUSTOM_DIR)
    for name in changed:
        if name not in wanted:
            (dest / name).unlink()
    return changed


# ── CLI ─────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["adopt-custom-mask", "live-check", "promote"])
    parser.add_argument("--env", default="")
    parser.add_argument("--env-dir", type=Path)
    parser.add_argument("--envs-dir", type=Path)
    parser.add_argument("--cloud-dir", default="", help="GENIERAILS_CLOUD_DIR: the dir the cloud Makefile is in")
    parser.add_argument("--treatment", default="")
    parser.add_argument("--source-dir", type=Path)
    parser.add_argument("--dest-dir", type=Path)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "adopt-custom-mask":
            command = args.command
            refuse_ci(command, "custom masks are adopted only on your own machine, never in CI (no opt-out)")
            env_dir = helper_env(args.cloud_dir, args.envs_dir, args.env, args.env_dir, command,
                                 who="custom-mask adoption and the AI helpers")
            if not (args.treatment or "").strip():
                raise Refused("TREATMENT=custom_<name> is required, e.g. make adopt-custom-mask ENV=dev "
                              "TREATMENT=custom_postcode_area")
            path, line = adopt(env_dir, args.treatment, args.force)
            print(f"ADOPTED {rel(env_dir, path)} (governance config; review and commit it).")
            print("  It masks nothing until a class maps to it. Add this line to tag_treatments in "
                  f"envs/{env_dir.name}/env.auto.tfvars (adopt-custom-mask never edits that file):")
            print(f"    {line}")
            print(f"  then make generate ENV={env_dir.name}, and open the PR. {NEVER_RAW_ADVICE[0].upper()}"
                  f"{NEVER_RAW_ADVICE[1:]}.")
            return 0
        if args.command == "live-check":
            return live_check(args.env_dir, args.env or args.env_dir.name)
        changed = promote(args.source_dir, args.dest_dir)
        if changed:
            print(f"  Custom masks (governance-owned): copied envs/{args.source_dir.name}/{CUSTOM_DIR}/ to "
                  f"envs/{args.dest_dir.name}/{CUSTOM_DIR}/ ({', '.join(changed)})")
        return 0
    except (Refused, CustomMaskError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
