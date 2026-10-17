"""The shipped CI samples load, cover every change and agree with the repo.

- GitHub Actions expressions use only the contexts GitHub allows where they
  sit (actionlint's check): a job-level env can't read runner.*, and a job
  that does is rejected before it starts.
- A dev rehearsal is refused only for a newer commit that changes the
  pipeline's own paths: a later root-only commit starts no dev run, so it must
  not strand the earlier one.
- The PR check runs make check ENV=prod for shared/** too, as the prod
  release does.
- Tool versions match the repository's own CI, and each header names its
  variables where make bootstrap-pipelines prints them.

The dev and PR scripts run here, as shipped, against real commits.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml

from scripts import bootstrap_pipelines as bp
from scripts.bootstrap_pipelines import Config

ROOT = Path(__file__).resolve().parents[2]
CLOUDS = ("aws", "azure")
GITHUB_SAMPLES = sorted(ROOT.glob("*/.github/workflows/*.yml"))
ADO_SAMPLES = sorted(ROOT.glob("*/azure-pipelines/*.yml"))
GIT_ENV = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
           "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com"}


# -- expression contexts (docs: "Context availability") ----------------------

_JOB = {"github", "needs", "strategy", "matrix", "vars", "inputs"}
WORKFLOW_CONTEXTS = {
    "run-name": {"github", "inputs", "vars"},
    "env": {"github", "secrets", "inputs", "vars"},
    "concurrency": {"github", "inputs", "vars"},
}
JOB_CONTEXTS = {
    "if": {"github", "needs", "vars", "inputs"},
    "env": _JOB | {"secrets"},
    "runs-on": _JOB, "environment": _JOB, "concurrency": _JOB, "timeout-minutes": _JOB,
    "continue-on-error": _JOB, "container": _JOB | {"secrets"}, "services": _JOB | {"secrets"},
    "defaults": _JOB | {"env"},
}
_EXPRESSION = re.compile(r"\$\{\{(.*?)\}\}", re.S)
_STRING = re.compile(r"'(?:[^']|'')*'")
_CONTEXT = re.compile(r"(?<![\w.\-])([A-Za-z_][\w-]*)(?=\s*[.\[])")


def _strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from _strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _strings(item)


def _contexts(value, bare_expression=False):
    """Every context an expression in value reads (an if: may omit ${{ }})."""
    found = set()
    for text in _strings(value):
        expressions = _EXPRESSION.findall(text) or ([text] if bare_expression else [])
        for expression in expressions:
            found |= set(_CONTEXT.findall(_STRING.sub("''", expression)))
    return found


def _context_errors(workflow: dict, where: str) -> list[str]:
    errors = []
    for key, allowed in WORKFLOW_CONTEXTS.items():
        for context in sorted(_contexts(workflow.get(key)) - allowed):
            errors.append(f"{where}: context {context!r} is not allowed in workflow {key}")
    for job_id, job in (workflow.get("jobs") or {}).items():
        for key, allowed in JOB_CONTEXTS.items():
            for context in sorted(_contexts(job.get(key), bare_expression=key == "if") - allowed):
                errors.append(f"{where}: context {context!r} is not allowed in jobs.{job_id}.{key}")
    return errors


def test_the_context_check_catches_a_job_level_runner_reference():
    workflow = {"run-name": "${{ github.workflow }} ${{ env.X }}", "jobs": {"j": {
        "if": "needs.a.result == 'success' && steps.s.outputs.x", "runs-on": "ubuntu-latest",
        "env": {"OK": "${{ vars.A }}/${{ format('{0}', github.sha) }}", "BAD": "${{ runner.temp }}/x"},
        "steps": [{"env": {"FINE": "${{ runner.temp }}"}, "run": "true"}]}}}
    assert _context_errors(workflow, "w") == [
        "w: context 'env' is not allowed in workflow run-name",
        "w: context 'steps' is not allowed in jobs.j.if",
        "w: context 'runner' is not allowed in jobs.j.env",
    ]


def test_workflow_expressions_use_only_contexts_available_where_they_sit():
    paths = [*GITHUB_SAMPLES, *sorted((ROOT / ".github/workflows").glob("*.yml"))]
    assert len(paths) >= 9
    errors = []
    for path in paths:
        errors += _context_errors(yaml.safe_load(path.read_text()), str(path.relative_to(ROOT)))
    assert errors == []


# -- dev rehearsal: which commit is obsolete ---------------------------------

def _git(repo, *args):
    return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True,
                          env=GIT_ENV).stdout.strip()


def _commit(repo, touched):
    for rel in touched:
        path = repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a") as f:
            f.write("change\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", ", ".join(touched))
    return _git(repo, "rev-parse", "HEAD")


def _github_dev_refusal(cloud, sha):
    workflow = yaml.safe_load((ROOT / cloud / ".github/workflows/dev-rehearse.yml").read_text())
    assert workflow[True]["push"]["paths"] == [f"{cloud}/**", "shared/**"]
    steps = workflow["jobs"]["rehearse"]["steps"]
    return next(s for s in steps if s.get("name") == "Refuse an obsolete commit")["run"].replace(
        "${{ github.sha }}", sha)


def _devops_dev_refusal(cloud, sha):
    pipeline = yaml.safe_load((ROOT / cloud / "azure-pipelines/dev-rehearse.yml").read_text())
    assert pipeline["trigger"]["paths"]["include"] == [cloud, "shared"]
    steps = pipeline["stages"][0]["jobs"][0]["strategy"]["runOnce"]["deploy"]["steps"]
    step = next(s for s in steps if s.get("displayName", "").startswith("Refuse an obsolete commit"))
    assert step["workingDirectory"] == cloud
    return step["script"].replace("$(Build.SourceVersion)", sha)


@pytest.mark.parametrize("cloud", CLOUDS)
@pytest.mark.parametrize("later, rehearses", [
    ([], True),
    # A later commit outside the pipeline's paths starts no dev run: the
    # earlier commit's run must still rehearse (its tree for the paths is
    # main's).
    ([["README.md"]], True),
    ([["README.md"], [".github/workflows/ci.yml"]], True),
    ([["{other}/envs/dev/env.auto.tfvars"]], True),
    # A later commit in the paths has its own run, which rehearses both.
    ([["{cloud}/envs/dev/env.auto.tfvars"]], False),
    ([["shared/generate_abac.py"]], False),
    ([["shared/generate_abac.py"], ["README.md"]], False),
])
def test_dev_rehearsal_is_obsolete_only_after_a_newer_commit_in_its_paths(tmp_path, cloud, later, rehearses):
    other = "azure" if cloud == "aws" else "aws"
    origin = tmp_path / "origin"
    origin.mkdir()
    _git(origin, "init", "-q", "-b", "main")
    _commit(origin, ["README.md", "shared/generate_abac.py", *(
        f"{c}/envs/{e}/remote_state.hcl" for c in CLOUDS for e in ("dev", "account"))])
    sha = _commit(origin, [f"{cloud}/envs/dev/env.auto.tfvars"])
    # The runner's checkout: shallow, at the run's commit.
    work = tmp_path / "work"
    _git(tmp_path, "clone", "-q", "--depth", "1", "--branch", "main", f"file://{origin}", str(work))
    for touched in later:
        _commit(origin, [rel.format(cloud=cloud, other=other) for rel in touched])
    for script in (_github_dev_refusal(cloud, sha), _devops_dev_refusal(cloud, sha)):
        assert _git(work, "rev-parse", "HEAD") == sha
        result = subprocess.run(["bash", "-e", "-c", script], cwd=work / cloud, text=True,
                                capture_output=True, env={**GIT_ENV, "SYSTEM_ACCESSTOKEN": ""})
        assert (result.returncode == 0) is rehearses, result.stdout + result.stderr
        if not rehearses:
            assert f"{sha} is obsolete" in result.stdout


@pytest.mark.parametrize("cloud", CLOUDS)
def test_dev_rehearsal_keeps_runtime_files_in_the_runner_temp_dir(cloud):
    workflow = yaml.safe_load((ROOT / cloud / ".github/workflows/dev-rehearse.yml").read_text())
    job = workflow["jobs"]["rehearse"]
    assert "GENIERAILS_STATE_DIR" not in job["env"]
    names = [s.get("name") or s.get("uses") for s in job["steps"]]
    setter = next(s for s in job["steps"] if "GENIERAILS_STATE_DIR=" in s.get("run", ""))
    assert setter["run"].splitlines() == ['mkdir -p -m 700 "$RUNNER_TEMP/genierails-state"',
                                          'echo "GENIERAILS_STATE_DIR=$RUNNER_TEMP/genierails-state" >> "$GITHUB_ENV"']
    # Set before anything that reads it.
    assert names.index(setter["name"]) < names.index("Read-only check of the merged commit")
    receipt = next(s for s in job["steps"] if s.get("uses", "").startswith("actions/upload-artifact"))
    assert receipt["with"]["path"].startswith("${{ runner.temp }}/genierails-state/")


# -- PR check: when prod is checked ------------------------------------------

def _github_pr_scope(cloud):
    workflow = yaml.safe_load((ROOT / cloud / ".github/workflows/pr-check.yml").read_text())
    steps = workflow["jobs"]["check"]["steps"]
    prod = next(s for s in steps if s.get("name") == "make check ENV=prod")
    assert prod == {"name": "make check ENV=prod", "if": "steps.scope.outputs.prod == 'true'",
                    "run": "make check ENV=prod"}
    return next(s for s in steps if s.get("id") == "scope")["run"].replace("${{ github.base_ref }}", "main")


def _devops_pr_scope(cloud):
    pipeline = yaml.safe_load((ROOT / cloud / "azure-pipelines/pr-check.yml").read_text())
    prod = next(s for s in pipeline["steps"] if s.get("script") == "make check ENV=prod")
    assert prod["condition"] == "and(succeeded(), eq(variables['CHECK_PROD'], 'true'))"
    step = next(s for s in pipeline["steps"] if "CHECK_PROD" in s.get("script", ""))
    assert step["workingDirectory"] == cloud
    return step["script"]


@pytest.mark.parametrize("cloud", CLOUDS)
@pytest.mark.parametrize("touched, checks_prod", [
    ("shared/generate_abac.py", True),
    ("shared/masking_functions/x.sql", True),
    ("{cloud}/envs/prod/env.auto.tfvars", True),
    ("{cloud}/envs/dev/env.auto.tfvars", False),
    ("{other}/envs/prod/env.auto.tfvars", False),
    ("README.md", False),
])
def test_pr_check_checks_prod_for_shared_and_envs_prod(tmp_path, cloud, touched, checks_prod):
    other = "azure" if cloud == "aws" else "aws"
    origin = tmp_path / "origin"
    origin.mkdir()
    _git(origin, "init", "-q", "-b", "main")
    _commit(origin, ["README.md", "shared/generate_abac.py", "aws/Makefile", "azure/Makefile"])
    work = tmp_path / "work"
    _git(tmp_path, "clone", "-q", f"file://{origin}", str(work))
    _git(work, "checkout", "-qb", "pr")
    _commit(work, [touched.format(cloud=cloud, other=other)])

    output = tmp_path / "github_output"
    output.write_text("")
    result = subprocess.run(["bash", "-e", "-c", _github_pr_scope(cloud)], cwd=work / cloud, text=True,
                            capture_output=True, env={**GIT_ENV, "GITHUB_OUTPUT": str(output)})
    assert result.returncode == 0, result.stderr
    assert ("prod=true" in output.read_text()) is checks_prod

    result = subprocess.run(["bash", "-c", _devops_pr_scope(cloud)], cwd=work / cloud, text=True,
                            capture_output=True,
                            env={**GIT_ENV, "SYSTEM_PULLREQUEST_TARGETBRANCH": "refs/heads/main"})
    assert result.returncode == 0, result.stderr
    assert ("##vso[task.setvariable variable=CHECK_PROD]true" in result.stdout) is checks_prod


# -- versions ------------------------------------------------------------------

def _repo_ci():
    return yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())


def test_samples_pin_the_sdk_and_terraform_the_repository_ci_uses():
    unit = _repo_ci()["jobs"]["unit-tests"]["steps"]
    ci_terraform = next(s for s in unit if s.get("uses", "").startswith("hashicorp/setup-terraform@"))
    version = ci_terraform["with"]["terraform_version"]
    problems = []
    for path in GITHUB_SAMPLES:
        workflow = yaml.safe_load(path.read_text())
        for job in workflow["jobs"].values():
            for step in job.get("steps", []):
                if step.get("uses", "").startswith("hashicorp/setup-terraform"):
                    if step["uses"] != ci_terraform["uses"] or step["with"].get("terraform_version") != version:
                        problems.append(f"{path.relative_to(ROOT)}: {step}")
    for path in ADO_SAMPLES:
        text = path.read_text()
        if "releases.hashicorp.com/terraform" in text:
            pipeline = yaml.safe_load(text)
            pinned = {v["name"]: v["value"] for v in pipeline["variables"] if "name" in v}
            if pinned.get("TERRAFORM_VERSION") != version:
                problems.append(f"{path.relative_to(ROOT)}: TERRAFORM_VERSION {pinned}")
    sdk = set()
    # The repository's own CI installs the SDK only where it needs it.
    repo_ci = ROOT / ".github/workflows/ci.yml"
    for path in [*GITHUB_SAMPLES, *ADO_SAMPLES, repo_ci]:
        for line in path.read_text().splitlines():
            if "pip install" in line:
                specs = re.findall(r"databricks-sdk[^\s']*", line)
                if not specs and path != repo_ci:
                    problems.append(f"{path.relative_to(ROOT)}: no databricks-sdk: {line.strip()}")
                sdk.update(specs)
                if "databricks-sdk" in line and not re.search(r"'databricks-sdk==\d+\.\d+\.\d+'", line):
                    problems.append(f"{path.relative_to(ROOT)}: unpinned: {line.strip()}")
    assert problems == []
    assert len(sdk) == 1, sdk


# -- where each header says the variables live ---------------------------------

def _printed_sections(cloud, ci):
    cfg = Config(cloud=cloud, env="dev", ci=ci, account_id="a", workspace_id=1, repo="acme/gov",
                 catalogs=("c",), ado_org="acme", ado_org_id="11111111-2222-3333-4444-555555555555",
                 ado_project="gov")
    sections, current = {}, None
    for line in bp.settings(cfg, "client", "https://h"):
        if line and not line.startswith(" "):
            current = line
        elif (m := re.match(r"  ([A-Z][A-Z0-9_]+) = ", line)):
            sections[m.group(1)] = current
    return sections


def _section_key(printed: str) -> str:
    if printed.startswith("Repository variables"):
        return "repository"
    if (m := re.match(r'(Environment|Variable group) "([\w-]+)"', printed)):
        return m.group(2)
    raise AssertionError(printed)


def _header_sections(text: str) -> dict[str, str]:
    """Variable name -> where the sample's header says it lives."""
    listed, current = {}, None
    for line in text.splitlines():
        if re.match(r"(on|trigger):", line):
            break
        if re.search(r"repository variables", line, re.I):
            current = "repository"
        elif (m := re.search(r'"([\w-]+)" environment|variable group "([\w-]+)"', line, re.I)):
            current = m.group(1) or m.group(2)
        elif re.fullmatch(r"#\s{3}(?:[A-Z][A-Z0-9_]+\s*)+", line):
            listed.update({name: current for name in line[1:].split()})
    return listed


@pytest.mark.parametrize("cloud", CLOUDS)
def test_headers_name_variables_where_bootstrap_pipelines_prints_them(cloud):
    problems = []
    for ci, samples in (("github", ROOT / cloud / ".github/workflows"), ("azure-devops", ROOT / cloud / "azure-pipelines")):
        printed = _printed_sections(cloud, ci)
        for path in sorted(samples.glob("*.yml")):
            text = path.read_text()
            if ci == "github":
                used = set(re.findall(r"\$\{\{\s*vars\.([A-Za-z0-9_]+)", text))
            else:
                defined = set(re.findall(r"^\s*-\s*name:\s*([A-Za-z_]\w*)", text, re.M))
                used = {n for n in re.findall(r"\$\(([A-Za-z_]\w*)\)", text) if n not in defined}
            header = _header_sections(text)
            for name in sorted(used):
                want = _section_key(printed[name])
                if header.get(name) != want:
                    problems.append(f"{path.relative_to(ROOT)}: {name} is in {want!r}, header says {header.get(name)!r}")
    assert problems == []
