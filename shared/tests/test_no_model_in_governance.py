"""generate, rehearse, release, maintain and check never reach a model (decision 5).

Two independent proofs:

* Static: every make target those commands (and make adopt-custom-mask)
  reach (prerequisites and nested $(MAKE) calls, with every variable
  expanded) and every GenieRails Python module their recipes run (with its
  local imports) are walked. None of them
  is an AI-helper target, names scripts/ai_helpers.py, or imports it.
* Runtime: a tripwire (a sitecustomize on PYTHONPATH) records, in every Python
  process make starts, any import of the model client (scripts.ai_helpers),
  of a provider SDK (anthropic, openai), any run of ai_helpers.py, and any
  call to the Databricks SDK's serving_endpoints.query. Real make check (dev
  and prod, CI unset and CI=true), a deterministic generate, and rehearse,
  release and maintain (nested make stubbed, as in their own tests) run under
  it and trip nothing. A positive control shows the tripwire catches the AI
  helpers, so an empty log means something.

Drafts are never read either: check passes with unreadable drafts in place,
and they change no rehearse fingerprint.
"""

import ast
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import ai_helpers
from scripts.release_gate import env_fingerprint
from tests.test_check_target import _commit, _make as _check_make, _state, promoted, repo  # noqa: F401 (fixtures)
from tests.test_deterministic_generate import _generate, env_dir  # noqa: F401 (fixture)

ROOT = Path(__file__).resolve().parents[2]
SHARED = ROOT / "shared"
MAKEFILE = SHARED / "Makefile.shared"
GOVERNANCE_TARGETS = ("generate", "rehearse", "release-plan", "release", "maintain", "check")
# Authoring commands that write governance config and must never call a model either.
NO_MODEL_TARGETS = GOVERNANCE_TARGETS + ("adopt-custom-mask",)
AI_TARGETS = ("suggest-treatment", "draft-genie-content", "draft-custom-mask", "test-custom-mask")
MODEL_MODULES = ("ai_helpers", "anthropic", "openai")


# ── Static: the Makefile and import closure ─────────────────────────────────

def _logical_lines(text):
    return text.replace("\\\n", " ").splitlines()


def _parse_makefile(text):
    rules, variables, current = {}, {}, None
    for line in _logical_lines(text):
        if line.startswith("\t"):
            if current:
                rules[current][1].append(line)
            continue
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        var = re.match(r"^(?:override\s+|export\s+)*([A-Za-z0-9_]+)\s*(?:\?=|:=|\+=|=)\s*(.*)$", line)
        rule = re.match(r"^([A-Za-z0-9_.-]+(?:\s+[A-Za-z0-9_.-]+)*)\s*:(?![=])\s*(.*)$", line)
        if var and not rule:
            variables.setdefault(var.group(1), []).append(var.group(2))
            current = None
        elif rule and not line.startswith("."):
            prereqs = rule.group(2).split("##", 1)[0].split()
            for name in rule.group(1).split():
                rules.setdefault(name, ([], []))[0].extend(prereqs)
            current = rule.group(1).split()[0]
        else:
            current = None
    return rules, variables


def _expand(text, variables, seen):
    """text plus the definitions of every variable it references, recursively."""
    out = [text]
    for name in re.findall(r"\$\((?:call\s+)?([A-Za-z0-9_]+)", text):
        if name in variables and name not in seen:
            seen.add(name)
            out.extend(_expand(value, variables, seen) for value in variables[name])
    return "\n".join(out)


def _closure(target):
    """(make targets reached, all recipe text with variables expanded) for one goal."""
    rules, variables = _parse_makefile(MAKEFILE.read_text())
    reached, texts, queue = set(), [], [target]
    while queue:
        name = queue.pop()
        if name in reached or name not in rules:
            continue
        reached.add(name)
        prereqs, recipe = rules[name]
        text = _expand("\n".join(recipe), variables, set())
        texts.append(text)
        queue.extend(prereqs)
        for call in re.findall(r"\$\(MAKE\)([^;|&\n]*)", text):
            queue.extend(word for word in call.split() if word in rules)
        # Scripts that run make themselves (check_env.py's scratch copy, release_gate.py's digests).
        for script in _scripts_run(text):
            queue.extend(re.findall(r"\.make\(\s*\"([A-Za-z0-9_-]+)\"", script.read_text()))
    return reached, "\n".join(texts)


def _local_module(name):
    for base in (SHARED, SHARED / "scripts"):
        parts = name.split(".")
        for candidate in (base.joinpath(*parts).with_suffix(".py"), base.joinpath(*parts, "__init__.py")):
            if candidate.is_file():
                return candidate
    return None


def _import_closure(paths):
    """{file: module names it imports anywhere} for these files and the local modules they import."""
    imports, queue = {}, list(paths)
    while queue:
        path = queue.pop()
        if path in imports:
            continue
        imports[path] = set()
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                found = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                found = [node.module] + [f"{node.module}.{a.name}" for a in node.names]
            else:
                continue
            for name in found:
                imports[path].add(name)
                local = _local_module(name)
                if local:
                    queue.append(local)
    return imports


def _scripts_run(text):
    return sorted({SHARED / rel for rel in re.findall(r"\$\(SHARED_ROOT\)/([A-Za-z0-9_/]+\.py)", text)
                   if (SHARED / rel).is_file()})


def test_the_parser_finds_the_ai_helpers_from_their_own_targets():
    for target in AI_TARGETS:
        reached, text = _closure(target)
        assert SHARED / "scripts" / "ai_helpers.py" in _scripts_run(text), target
    # It follows prerequisites, nested $(MAKE) calls and the targets a script runs.
    assert {"_guard-not-ci-apply", "_release-manifest", "derive-assignments", "apply", "verify-access"} <= _closure("release")[0]
    assert {"validate", "promote", "coverage-gate", "plan", "promote-to"} <= _closure("check")[0]


@pytest.mark.parametrize("target", NO_MODEL_TARGETS)
def test_governance_targets_never_reach_an_ai_helper_target_or_the_model_client(target):
    reached, text = _closure(target)
    assert len(reached) > 1, f"{target}: the Makefile parser found nothing to walk"
    assert not reached & set(AI_TARGETS), (target, reached & set(AI_TARGETS))
    assert "ai_helpers" not in text and "_AI_HELPERS" not in text and "ai_helper_endpoint" not in text
    scripts = _scripts_run(text)
    assert scripts, target
    imports = _import_closure(scripts)
    assert SHARED / "scripts" / "ai_helpers.py" not in imports, target
    for path, names in imports.items():
        model = {n for n in names if n.split(".")[-1] in MODEL_MODULES or n.split(".")[0] in MODEL_MODULES}
        # Only generate_abac.py's legacy provider functions import a provider SDK
        # (decision 10); the deterministic generate test proves they never run.
        allowed = {"anthropic", "openai"} if path == SHARED / "generate_abac.py" else set()
        assert model <= allowed, (target, str(path), model)


def test_only_the_ai_helpers_and_legacy_generate_call_a_serving_endpoint():
    callers = sorted(str(p.relative_to(SHARED)) for p in SHARED.rglob("*.py")
                     if "tests" not in p.relative_to(SHARED).parts and "examples" not in p.relative_to(SHARED).parts
                     and "serving_endpoints.query" in p.read_text())
    # generate_abac.py's call is the legacy AI-drafted mode (decision 10, a migration source only).
    assert callers == ["generate_abac.py", "scripts/ai_helpers.py"]
    importers = sorted(str(p.relative_to(SHARED)) for p in SHARED.rglob("*.py")
                       if "tests" not in p.relative_to(SHARED).parts and p.name != "ai_helpers.py"
                       and re.search(r"^\s*(?:from|import)\s+(?:scripts\.)?ai_helpers\b", p.read_text(), re.M))
    assert importers == []


# ── Runtime: the tripwire ───────────────────────────────────────────────────

TRIPWIRE = r'''
import importlib.abc, importlib.machinery, os, sys

LOG = os.environ.get("GENIERAILS_MODEL_TRIPWIRE")
BLOCKED = {"ai_helpers", "scripts.ai_helpers", "anthropic", "openai"}


def trip(what):
    with open(LOG, "a") as handle:
        handle.write(what + "\n")


class Tripwire(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name in BLOCKED:
            trip("import " + name)
            raise ImportError("model tripwire: " + name)
        if name != "databricks.sdk.service.serving":
            return None
        spec = importlib.machinery.PathFinder.find_spec(name, path)
        if spec is None or spec.loader is None:
            return spec
        real = spec.loader.exec_module

        def exec_module(module):
            real(module)
            api = getattr(module, "ServingEndpointsAPI", None)
            if api is not None:
                def query(*_a, **_k):
                    trip("serving_endpoints.query")
                    raise RuntimeError("model tripwire: serving_endpoints.query")
                api.query = query

        spec.loader.exec_module = exec_module
        return spec


if LOG:
    if sys.argv and os.path.basename(sys.argv[0]) == "ai_helpers.py":
        trip("run ai_helpers.py")
    sys.meta_path.insert(0, Tripwire())
'''


@pytest.fixture
def tripwire(tmp_path, monkeypatch):
    folder = tmp_path / "tripwire"
    folder.mkdir()
    (folder / "sitecustomize.py").write_text(TRIPWIRE)
    log = tmp_path / "tripwire.log"
    monkeypatch.setenv("PYTHONPATH", str(folder))
    monkeypatch.setenv("GENIERAILS_MODEL_TRIPWIRE", str(log))
    return log


def _tripped(log):
    return log.read_text().splitlines() if log.exists() else []


def test_the_tripwire_catches_the_ai_helpers(tripwire, tmp_path):
    env = dict(os.environ)
    result = subprocess.run([sys.executable, "-c", "import sys; sys.path.insert(0, %r); import scripts.ai_helpers" % str(SHARED)],
                            env=env, capture_output=True, text=True)
    assert result.returncode != 0
    subprocess.run([sys.executable, str(SHARED / "scripts" / "ai_helpers.py"), "--help"], env=env, capture_output=True)
    subprocess.run([sys.executable, "-c", "import databricks.sdk.service.serving as s; s.ServingEndpointsAPI.query(None)"],
                   env=env, capture_output=True)
    assert _tripped(tripwire) == ["import scripts.ai_helpers", "run ai_helpers.py", "serving_endpoints.query"]


DRAFTS = {"treatments/loyalty_id.json": "{ not json", "custom_masks/x.sql": "DROP TABLE everything;",
          "custom_masks/x.json": "[", "genie/Payments.json": "\x00"}


def _write_drafts(env_dir):
    for rel, text in DRAFTS.items():
        path = env_dir / "drafts" / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)


@pytest.mark.parametrize("ci", ["", "true"])
def test_make_check_dev_reaches_no_model_and_reads_no_draft(repo, tmp_path, tripwire, ci):  # noqa: F811
    _write_drafts(repo / "aws" / "envs" / "dev")
    before = _state(repo)
    result = _check_make(repo, tmp_path, "check", "ENV=dev", PYTHONPATH=os.environ["PYTHONPATH"],
                         GENIERAILS_MODEL_TRIPWIRE=str(tripwire), **({"CI": ci} if ci else {}))
    assert _tripped(tripwire) == []
    assert result.returncode == 0, result.stdout + result.stderr
    assert _state(repo) == before


@pytest.mark.parametrize("ci", ["", "true"])
def test_make_check_prod_reaches_no_model(promoted, tmp_path, tripwire, ci):  # noqa: F811
    prod = promoted / "aws" / "envs" / "prod"
    subprocess.run([sys.executable, str(SHARED / "scripts" / "sticky_governance.py"), "record",
                    "--env-dir", str(prod), "--config", str(prod / "generated" / "abac.auto.tfvars")], check=True)
    _write_drafts(promoted / "aws" / "envs" / "dev")
    _commit(promoted, "prod governed tables")
    result = _check_make(promoted, tmp_path, "check", "ENV=prod", PYTHONPATH=os.environ["PYTHONPATH"],
                         GENIERAILS_MODEL_TRIPWIRE=str(tripwire), **({"CI": ci} if ci else {}))
    assert _tripped(tripwire) == []
    out = result.stdout + result.stderr
    # Every step runs up to prod's plan, which reads live classification this
    # offline fixture can't reach; promote-to CHECK=1 and the regeneration pass.
    assert "=== make check ENV=prod: FAILED (plan) ===" in out, out
    assert "FAILED (promote-to" not in out and "FAILED (generated files" not in out


def test_deterministic_generate_never_loads_the_model_client(env_dir, monkeypatch):  # noqa: F811
    for name in ("ai_helpers", "scripts.ai_helpers", "anthropic", "openai"):
        monkeypatch.setitem(sys.modules, name, None)  # importing any of them now fails
    monkeypatch.setattr(ai_helpers.DatabricksModel, "complete", lambda *a, **k: pytest.fail("generate called a model"))
    _write_drafts(env_dir)
    _generate(env_dir, monkeypatch)
    assert (env_dir / "generated" / "abac.auto.tfvars").is_file()


def _stub_make(tmp_path):
    stub = tmp_path / "make-stub"
    stub.write_text("#!/bin/sh\nexit 0\n")
    stub.chmod(0o755)
    return stub


def _clean_env(**extra):
    """The caller's env minus make state and every CI marker: release, maintain and
    rehearse run as on the deployment machine (tests/script_stubs.without_ci)."""
    from tests.script_stubs import without_ci

    env = {k: v for k, v in without_ci(os.environ).items()
           if k not in ("VERIFY_KEY_COLUMN", "APPLY_FLAGS", "MAKEFLAGS", "MAKELEVEL")}
    env.update(extra)
    return env


def test_the_stub_env_clears_every_ci_marker_a_guard_reads():
    from scripts import ci_guard
    from tests.script_stubs import CI_ENV_MARKERS, without_ci

    # The Makefile's guards ask scripts/ci_guard.py; no recipe reads a marker itself.
    makefile = MAKEFILE.read_text()
    markers = {"CI", *ci_guard.CI_MARKERS, *ai_helpers.CI_MARKERS}
    assert not [m for m in markers if re.search(rf"\$\$\{{?{m}\b", makefile)]
    source = (SHARED / "scripts" / "ci_guard.py").read_text()
    assert set(re.findall(r"environ(?:\.get\(|\[)[\"']([A-Za-z0-9_]+)", source)) == {"CI"}
    read = {"CI", *ci_guard.CI_MARKERS, ci_guard.ALLOW_CI_APPLY}
    assert {"CI", "TF_BUILD", "JENKINS_URL", "GITLAB_CI", "BUILDKITE", "CIRCLECI"} <= read
    assert read <= set(CI_ENV_MARKERS), read - set(CI_ENV_MARKERS)
    assert set(ai_helpers.CI_MARKERS) <= set(CI_ENV_MARKERS)
    assert without_ci({"TF_BUILD": "True", "BUILD_BUILDID": "1", "CI": "true", "PATH": "/bin"}) == {"PATH": "/bin"}


@pytest.mark.parametrize("target", ["release", "maintain"])
def test_release_and_maintain_reach_no_model(tmp_path, tripwire, target):
    from tests import script_stubs

    env_dir = tmp_path / "prod"
    (env_dir / "generated").mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text("business_access_enabled = true\n")
    _write_drafts(env_dir)
    audit = tmp_path / "audit.py"
    audit.write_text("raise SystemExit(0)\n")
    bin_dir = script_stubs.install(tmp_path / "stub-bin", {"audit_schema_drift.py": script_stubs.python_file(audit)})
    result = subprocess.run(["make", target, "ENV=prod", f"ENV_DIR={env_dir}", f"MAKE={_stub_make(tmp_path)}"],
                            cwd=ROOT / "aws", text=True, capture_output=True,
                            env=_clean_env(PATH=f"{bin_dir}:{os.environ['PATH']}"))
    assert result.returncode == 0, result.stdout + result.stderr
    assert _tripped(tripwire) == []


def test_rehearse_reaches_no_model(tmp_path, tripwire):
    result = subprocess.run(["make", "rehearse", "ENV=dev", "VERIFY_KEY_COLUMN=customer_id", f"MAKE={_stub_make(tmp_path)}"],
                            cwd=ROOT / "aws", text=True, capture_output=True, env=_clean_env())
    assert result.returncode == 0, result.stdout + result.stderr
    assert _tripped(tripwire) == []


def test_drafts_change_no_rehearse_fingerprint(tmp_path):
    env_dir = tmp_path / "dev"
    (env_dir / "generated").mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    before = env_fingerprint(env_dir)
    _write_drafts(env_dir)
    assert env_fingerprint(env_dir) == before
    (env_dir / "generated" / "abac.auto.tfvars").write_text("groups = {}\n")
    assert env_fingerprint(env_dir) != before  # real inputs still count
