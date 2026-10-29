import os
import re
import signal
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
APPLYING_TARGETS = (
    "enable-classification", "rehearse", "release", "release-plan", "_release-manifest", "maintain", "apply",
    "apply-governance", "apply-genie", "_apply-layer", "integration-test",
    "test-champion", "test-all", "test-ci", "test-ci-parallel",
    "destroy", "destroy-governance", "destroy-genie", "_destroy-layer",
    "import", "migrate-state", "migrate-state-dir", "ungovern", "apply-account", "policy-abandon", "policy-rebind",
)
CI_MARKERS = ("CI", "TF_BUILD", "JENKINS_URL", "GITLAB_CI", "BUILDKITE", "CIRCLECI")


def _clean_env(**updates):
    env = {key: value for key, value in os.environ.items() if key not in (*CI_MARKERS, "GENIERAILS_ALLOW_CI_APPLY")}
    env.update(updates)
    return env


def _run_guarded_make(cloud, target, *extra_args, env, timeout=10):
    proc = subprocess.Popen(
        ["make", "-f", str(ROOT / cloud / "Makefile"), target, "ENV=dev", *extra_args],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
        pytest.fail(f"timed out and killed process group for {cloud} make {target}")
    return subprocess.CompletedProcess(proc.args, proc.returncode, stdout, stderr)


@pytest.mark.parametrize("cloud", ["aws", "azure"])
@pytest.mark.parametrize("target", APPLYING_TARGETS)
def test_every_applying_target_refuses_in_ci(cloud, target):
    proc = _run_guarded_make(cloud, target, env=_clean_env(CI="true"))
    output = proc.stdout + proc.stderr
    assert proc.returncode != 0
    assert "applying targets cannot run in CI" in output


def test_ci_guard_fires_before_integration_test_dest_env_guard():
    proc = _run_guarded_make(
        "aws",
        "integration-test",
        "DEST_ENV=prod",
        env=_clean_env(CI="true"),
    )
    output = proc.stdout + proc.stderr
    assert proc.returncode != 0
    assert "applying targets cannot run in CI" in output
    assert "integration-test reads ITEST_DEST_ENV" not in output


@pytest.mark.parametrize("cloud", ["aws", "azure"])
@pytest.mark.parametrize("target", [
    ["plan"], ["validate"], ["test-unit"], ["coverage-gate"], ["audit-schema"], ["check"],
    ["promote-to", "CHECK=1"],
])
def test_read_only_targets_have_no_ci_refusal(cloud, target):
    proc = subprocess.run(
        ["make", "-n", "-f", str(ROOT / cloud / "Makefile"), *target, "ENV=dev", "CI=true"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert "applying targets cannot run in CI" not in proc.stdout + proc.stderr


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_throwaway_integration_workflow_has_explicit_ci_apply_opt_out(cloud):
    proc = _run_guarded_make(
        cloud,
        "_guard-not-ci-apply",
        env=_clean_env(CI="true", GENIERAILS_ALLOW_CI_APPLY="1"),
    )
    assert proc.returncode == 0


@pytest.mark.parametrize(("marker", "value"), [
    ("CI", "true"), ("CI", "1"), ("CI", "yes"), ("CI", "TrUe"),
    ("TF_BUILD", "True"), ("JENKINS_URL", "https://jenkins.example"),
    ("GITLAB_CI", "true"), ("BUILDKITE", "true"), ("CIRCLECI", "true"),
])
def test_guard_detects_common_ci_markers(marker, value):
    proc = _run_guarded_make("aws", "_guard-not-ci-apply", env=_clean_env(**{marker: value}))
    assert proc.returncode != 0
    assert "applying targets cannot run in CI" in proc.stderr


def test_ci_workflow_scopes_apply_opt_out_to_integration_steps():
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert workflow.count('GENIERAILS_ALLOW_CI_APPLY: "1"') == 2
    unit_job = workflow[workflow.index("  unit-tests:"):workflow.index("  validation:")]
    assert "GENIERAILS_ALLOW_CI_APPLY" not in unit_job


SAMPLE_DIRS = (".github", "azure-pipelines")
# GitHub Actions steps sit at six spaces; Azure DevOps steps start with a step keyword.
STEP_SPLIT = {
    ".github": r"(?m)^      - ",
    "azure-pipelines": r"(?m)^\s*- (?:script|bash|pwsh|powershell|task|checkout|template|download|publish):",
}


def _sample_workflows():
    return sorted(
        (sample_dir, path)
        for cloud in ("aws", "azure")
        for sample_dir in SAMPLE_DIRS
        for suffix in ("*.yml", "*.yaml")
        for path in (ROOT / cloud / sample_dir).rglob(suffix)
    )


def test_shipped_workflows_scope_the_only_applying_command_to_dev_rehearse():
    workflow_paths = _sample_workflows()
    assert {sample_dir for sample_dir, _ in workflow_paths} == set(SAMPLE_DIRS)
    for sample_dir, path in workflow_paths:
        text = path.read_text()
        steps = re.split(STEP_SPLIT[sample_dir], text)[1:]
        assert steps, f"{path}: no steps found"
        executed_targets = set()
        for step in steps:
            targets = re.findall(r"\bmake\s+(?:--no-print-directory\s+)?([A-Za-z_][\w-]*)", step)
            executed_targets.update(targets)
            opted_out = bool(re.search(r"GENIERAILS_ALLOW_CI_APPLY:\s*['\"]?1['\"]?", step))
            guarded = set(targets) & set(APPLYING_TARGETS)
            allowed = {"rehearse"} if "dev-rehearse" in path.name else set()
            assert not guarded or guarded <= allowed, path
        # Customer samples never use the integration-test bypass. Only stage 3
        # may apply, and the make guard independently proves dev locked state.
        code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
        assert "GENIERAILS_ALLOW_CI_APPLY" not in code, path
        allowed = {"rehearse"} if "dev-rehearse" in path.name else set()
        assert (executed_targets & set(APPLYING_TARGETS)) <= allowed


def test_prod_maintain_samples_run_no_applying_target():
    for cloud in ("aws", "azure"):
        for sample_dir in SAMPLE_DIRS:
            path = ROOT / cloud / sample_dir / ("workflows" if sample_dir == ".github" else "") / "prod-maintain.yml"
            text = path.read_text()
            code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
            executed = set(re.findall(r"\bmake\s+(?:--no-print-directory\s+)?([A-Za-z_][\w-]*)", code))
            assert executed & set(APPLYING_TARGETS) == set(), path
            assert "make audit-schema ENV=prod" in code, path


@pytest.mark.parametrize("stage", ["prod-release", "prod-maintain"])
def test_applying_stage_samples_are_marked_as_needing_remote_state(stage):
    for cloud in ("aws", "azure"):
        for sample_dir in SAMPLE_DIRS:
            text = (ROOT / cloud / sample_dir / ("workflows" if sample_dir == ".github" else "") / f"{stage}.yml").read_text()
            assert "REQUIRES DEV REMOTE STATE" in text or "REQUIRES PROD REMOTE STATE" in text
            assert "deployment machine" in text
            assert "make check" in text or "make audit-schema" in text


def test_dev_samples_queue_every_commit_and_never_cancel():
    for cloud in ("aws", "azure"):
        github = (ROOT / cloud / ".github" / "workflows" / "dev-rehearse.yml").read_text()
        assert "concurrency:" not in github and "cancel-in-progress: true" not in github
        assert "needs: queue" in github
        devops = (ROOT / cloud / "azure-pipelines" / "dev-rehearse.yml").read_text()
        assert "lockBehavior: sequential" in devops and "batch: false" in devops
        assert "envs/account/remote_state.hcl" in github + devops
        assert github.count("FETCH_HEAD") and devops.count("FETCH_HEAD")


def test_prod_samples_do_not_consume_unsigned_receipt_artifacts():
    for cloud in ("aws", "azure"):
        github = (ROOT / cloud / ".github/workflows/prod-release.yml").read_text()
        ado = (ROOT / cloud / "azure-pipelines/prod-release.yml").read_text()
        assert "download-artifact" not in github
        assert "download: dev" not in ado
        assert re.search(r"checkout: self\n\s+fetchDepth: 0", ado)
        assert 'changed="$(git diff --name-only "$base" HEAD -- envs/prod ../shared)" || exit $?' in ado


def _concurrency_group(text):
    block = re.search(r"(?m)^concurrency:\n((?:[ \t]+.*\n)+)", text)
    return block and re.search(r"(?m)^\s+group:\s*(\S+)", block.group(1)).group(1)


def test_prod_release_and_maintain_do_not_share_a_concurrency_group():
    # GitHub keeps one pending run per group and cancels the older one, so a
    # shared group (or any group on release) can cancel a pending release.
    for cloud in ("aws", "azure"):
        github = ROOT / cloud / ".github" / "workflows"
        release = (github / "prod-release.yml").read_text()
        maintain = (github / "prod-maintain.yml").read_text()
        assert _concurrency_group(release) is None, cloud
        assert _concurrency_group(maintain) == f"genierails-prod-maintain-{cloud}"
        assert "cancel-in-progress: true" not in release + maintain
        devops = ROOT / cloud / "azure-pipelines"
        release = (devops / "prod-release.yml").read_text()
        assert "lockBehavior: sequential" in release and "lockBehavior: runLatest" not in release
        assert "environment:" not in (devops / "prod-maintain.yml").read_text()


def test_samples_use_oidc_not_secrets():
    for _, path in _sample_workflows():
        text = path.read_text()
        assert "${{ secrets." not in text and "$(secrets" not in text and "CLIENT_SECRET:" not in text, path
        if "make check" in text or "make audit-schema" in text:
            assert "github-oidc" in text or "azure-devops-oidc" in text, path


def test_workspace_guard_runs_env_validator_and_rejects_bad_tfvars(tmp_path):
    cloud_root = tmp_path / "aws"
    env_dir = cloud_root / "envs" / "dev"
    env_dir.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text('governance_mode = "future"\n')
    proc = subprocess.run(
        [
            "make", "-f", str(ROOT / "aws" / "Makefile"),
            "_guard-workspace-config", "ENV=dev",
            f"CLOUD_ROOT={cloud_root}", f"ENV_DIR={env_dir}",
            f"SHARED_ROOT={ROOT / 'shared'}",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert proc.returncode != 0
    assert "env config validation: governance_mode must be legacy or deterministic" in proc.stderr


def test_env_validator_cli_uses_nonzero_exit_and_stderr(tmp_path):
    env_file = tmp_path / "env.auto.tfvars"
    env_file.write_text('treatment_versions = { ssn = { partial = ["bad"] } }\n')
    proc = subprocess.run(
        [sys.executable, str(ROOT / "shared" / "scripts" / "validate_env_config.py"), str(env_file)],
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert proc.returncode == 1
    assert proc.stdout == ""
    assert "treatment_versions" in proc.stderr
    assert "Traceback" not in proc.stderr


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_policy_rebind_ignores_the_ci_apply_opt_out(cloud):
    """GENIERAILS_ALLOW_CI_APPLY=1 lets throwaway integration workflows apply; never policy-rebind."""
    proc = _run_guarded_make(cloud, "policy-rebind", env=_clean_env(CI="true", GENIERAILS_ALLOW_CI_APPLY="1"))
    assert proc.returncode != 0
    assert "policy-rebind cannot run in CI, even with GENIERAILS_ALLOW_CI_APPLY=1" in proc.stdout + proc.stderr


# -- ci_guard.py fails closed at every call site ----------------------------------------------------------------------
#
# Every CI check in Makefile.shared runs scripts/ci_guard.py. A missing script, one that crashes, or one that exits
# with a code that isn't one of its answers must refuse, in CI and outside it, never read as "not CI".

SHARED = ROOT / "shared"
MAKEFILE_SHARED = SHARED / "Makefile.shared"
# Makefile.shared target -> how it calls ci_guard.py. A new call site fails test_every_ci_guard_call_site_is_listed
# until it is added here, which runs it through every broken mode below.
MAKE_CALL_SITES = {
    "_guard-not-ci-apply": "apply",
    "_guard-not-ci-rebind": "rebind",
    "_guard-account-plan-empty": "ci-status",
    "rehearse": "ci-status",
}
# Python modules that import ci_guard (or its wrapper staged_policy_apply._in_ci), each proven fail-closed below.
PY_CALL_SITES = {"scripts/env_status.py": "ci_guard", "scripts/setup_repo.py": "ci_guard",
                 "scripts/policy_generations.py": "_in_ci", "scripts/approval.py": "ci_guard",
                 "scripts/staged_policy_apply.py": "ci_guard", "scripts/commit_envs.py": "ci_guard"}
# Each broken ci_guard.py, as a script body. LINE is the permissive answer for the subcommand it was asked
# (ci-status: not CI; apply/rebind: allowed), so a stub can print the right words with the wrong exit code.
# ci_guard.py's answer protocol (versioned; test_every_ci_guard_call_site_is_listed checks ci_guard.ANSWERS).
STATUS_CI, STATUS_NOT_CI = "genierails-ci-status:v1:ci", "genierails-ci-status:v1:not-ci"
ALLOWED, REFUSED = "genierails-ci-guard:v1:allowed", "genierails-ci-guard:v1:refused"
PRELUDE = ("import sys\nLINE = {{'ci-status': {not_ci!r}, 'apply': {allowed!r}, 'rebind': {allowed!r}}}"
           ".get(sys.argv[1] if len(sys.argv) > 1 else '', '')\n")
BROKEN = {
    "missing": None,
    "crash": "raise RuntimeError('ci_guard is broken')\n",
    "unexpected-exit": "sys.exit(7)\n",
    "exit-1": "sys.exit(1)\n",
    "exit-10": "sys.exit(10)\n",                        # 891e8b3's "CI" answer, from anything
    "exit-0-silent": "sys.exit(0)\n",
    "right-line-nonzero-exit": "print(LINE)\nsys.exit(3)\n",
    "garbage": "print('yes')\nsys.exit(0)\n",
    "extra-output": "print(LINE)\nprint(LINE)\nsys.exit(0)\n",
    "wrong-version": "print(LINE.replace(':v1:', ':v2:'))\nsys.exit(0)\n",
}
# For call sites behind _guard-not-ci-apply: apply answers allowed, every other subcommand is broken as
# ``mode`` says, so the call site itself is what has to refuse. "missing" runs a script path that doesn't exist.
STUB = """import os, runpy, sys
command = sys.argv[1] if len(sys.argv) > 1 else ""
if command == "apply":
    print({allowed!r})
    sys.exit(0)
mode = {mode!r}
if mode != "real":
    if mode == "missing":
        os.execv(sys.executable, [sys.executable, {missing!r}])
    exec(compile({broken!r}, "ci_guard.py", "exec"))
sys.argv[0] = {real!r}
runpy.run_path({real!r}, run_name="__main__")
"""


def _call_sites(makefile_text: str) -> dict[str, set[str]]:
    """Each Makefile target -> the ci_guard subcommands its recipe runs ($(_CI_STATUS) is ci-status)."""
    sites, target = {}, None
    for line in makefile_text.splitlines():
        match = re.match(r"^([A-Za-z_][A-Za-z0-9_-]*):(?!=)", line)
        if match:
            target = match.group(1)
        elif line.startswith("override _CI_"):
            target = None
        if target is None or not line.startswith("\t"):
            continue
        for sub in re.findall(r"\$\(_CI_GUARD\) ([a-z-]+)", line):
            sites.setdefault(target, set()).add(sub)
        if "$(_CI_STATUS)" in line:
            sites.setdefault(target, set()).add("ci-status")
    return sites


def test_every_ci_guard_call_site_is_listed():
    text = MAKEFILE_SHARED.read_text()
    assert _call_sites(text) == {target: {sub} for target, sub in MAKE_CALL_SITES.items()}
    # Never a bare `if` on it: only _CI_STATUS reads ci-status, and it refuses on any code but 0 and 10.
    assert not re.search(r"\bif !? *\$\(_CI_GUARD\)", text)
    assert text.count("$(_CI_GUARD) ci-status") == 1
    # Only the exact (exit code, line) answers of ci_guard.ANSWERS pass; each Makefile matcher lists exactly those.
    from scripts import ci_guard

    for name, command in (("_CI_STATUS", "ci-status"), ("_CI_GUARD_ANSWER", "apply")):
        block = text.split(f"override {name} = ", 1)[1].split("\n# ", 1)[0].split("\n_guard", 1)[0]
        pairs = set(re.findall(r'"(\d+):(genierails-[a-z-]+:v\d+:[a-z-]+)"\)', block))
        assert {(int(rc), line) for rc, line in pairs} == ci_guard.ANSWERS[command]
        assert ") echo \"ERROR: the CI" in block and "exit 1 ;;" in block
    assert ci_guard.ANSWERS == {"ci-status": {(0, STATUS_CI), (0, STATUS_NOT_CI)},
                                "apply": {(0, ALLOWED), (1, REFUSED)}, "rebind": {(0, ALLOWED), (1, REFUSED)}}
    found = {}
    for path in sorted((SHARED / "scripts").glob("*.py")) + sorted(SHARED.glob("*.py")):
        rel = str(path.relative_to(SHARED))
        text = path.read_text()
        if rel == "scripts/ci_guard.py":
            continue
        if re.search(r"\bci_guard\b", text.replace("ci_guard.py", "")):
            found[rel] = "ci_guard"
        elif re.search(r"\b_in_ci\(", text):
            found[rel] = "_in_ci"
    assert found == PY_CALL_SITES


def _shared_copy(tmp_path: Path, mode: str, *, stub: bool) -> Path:
    """shared/ with its own scripts/ (everything else symlinked) and ci_guard.py broken as ``mode`` says."""
    import shutil

    copy = tmp_path / f"shared-{mode}"
    copy.mkdir()
    for entry in SHARED.iterdir():
        if entry.name in ("scripts", "tests", "__pycache__"):
            continue
        (copy / entry.name).symlink_to(entry)
    shutil.copytree(SHARED / "scripts", copy / "scripts", ignore=shutil.ignore_patterns("__pycache__"))
    guard = copy / "scripts" / "ci_guard.py"
    real = tmp_path / "real_ci_guard.py"
    if not real.exists():
        shutil.copyfile(SHARED / "scripts" / "ci_guard.py", real)
        (tmp_path / "state_layout.py").symlink_to(SHARED / "scripts" / "state_layout.py")
    prelude = PRELUDE.format(not_ci=STATUS_NOT_CI, allowed=ALLOWED)
    if stub:
        guard.write_text(STUB.format(mode=mode, missing=str(tmp_path / "no-such-ci_guard.py"), allowed=ALLOWED,
                                     broken=prelude + (BROKEN.get(mode) or ""), real=str(real)))
    elif BROKEN.get(mode, "") is None:
        guard.unlink()
    else:
        guard.write_text(prelude + BROKEN[mode])
    return copy


def _site_run(tmp_path: Path, target: str, mode: str, ci: dict) -> subprocess.CompletedProcess:
    cloud = tmp_path / "cloud"
    dev = cloud / "envs" / "dev"
    account = cloud / "envs" / "account"
    for path in (dev, account):
        path.mkdir(parents=True, exist_ok=True)
    (dev / "env.auto.tfvars").write_text('governance_mode = "legacy"\n')
    (account / "remote_state.hcl").write_text('backend = "s3"\nbucket = "b"\nregion = "r"\ndynamodb_table = "l"\n')
    stub = target == "rehearse"  # behind _guard-not-ci-apply, which must pass for it to be reached
    shared = _shared_copy(tmp_path, mode, stub=stub) if mode != "healthy" else SHARED
    if mode == "healthy" and stub:
        shared = _shared_copy(tmp_path, "real", stub=True)
    env = _clean_env(**ci, HOME=str(tmp_path), PATH=f"{Path(sys.executable).parent}:{os.environ['PATH']}")
    args = ["make", "--no-print-directory", "-f", str(ROOT / "aws" / "Makefile"), target, "ENV=dev",
            f"SHARED_ROOT={shared}", f"CLOUD_ROOT={cloud}", f"ENV_DIR={dev}", f"ACCOUNT_ENV_DIR={account}",
            "ROOT_RUNNER=true"]
    return subprocess.run(args, cwd=cloud, env=env, text=True, capture_output=True, timeout=120)


CI_ENVS = {"ci": {"CI": "true"}, "not-ci": {}, "opted-out": {"CI": "true", "GENIERAILS_ALLOW_CI_APPLY": "1"}}


@pytest.mark.parametrize("ci", list(CI_ENVS))
@pytest.mark.parametrize("mode", list(BROKEN))
@pytest.mark.parametrize("target", list(MAKE_CALL_SITES))
def test_a_broken_ci_guard_refuses_at_every_call_site(tmp_path, target, mode, ci):
    result = _site_run(tmp_path, target, mode, CI_ENVS[ci])
    output = result.stdout + result.stderr
    assert result.returncode != 0, f"make {target} ran with ci_guard.py {mode} ({ci}):\n{output}"
    assert "gave no valid answer (exit" in output and "refusing, as in CI" in output


@pytest.mark.parametrize("target,ci,refused", [
    ("_guard-account-plan-empty", "ci", False),  # CI with account remote state: the plan runs (ROOT_RUNNER=true)
    ("_guard-account-plan-empty", "not-ci", False),
    ("rehearse", "ci", True),                     # a legacy dev never rehearses in CI
])
def test_a_healthy_ci_guard_still_answers_at_the_status_call_sites(tmp_path, target, ci, refused):
    """The control for the test above: with a working ci_guard.py those same runs pass or refuse on their merits."""
    result = _site_run(tmp_path, target, "healthy", CI_ENVS[ci])
    output = result.stdout + result.stderr
    assert "gave no valid answer" not in output
    if refused:
        assert result.returncode != 0 and "uses legacy governance, which the CI dev pipeline doesn't run" in output
    else:
        assert result.returncode == 0, output


def test_ci_guard_answers_one_exact_line():
    from scripts import ci_guard

    def answer(env, *args):
        result = subprocess.run([sys.executable, str(SHARED / "scripts" / "ci_guard.py"), *args],
                                env=_clean_env(**env), capture_output=True, text=True)
        return result.returncode, result.stdout

    not_ci, ci = (0, ci_guard.STATUS_NOT_CI + "\n"), (0, ci_guard.STATUS_CI + "\n")
    assert answer({}, "ci-status") == not_ci
    assert answer({"CI": "true"}, "ci-status") == ci and answer({"TF_BUILD": "True"}, "ci-status") == ci
    assert answer({"CI": "true", "GENIERAILS_ALLOW_CI_APPLY": "1"}, "ci-status", "--allow-opt-out") == not_ci
    assert answer({"CI": "true", "GENIERAILS_ALLOW_CI_APPLY": "1"}, "ci-status") == ci
    assert answer({}, "rebind") == (0, ci_guard.ALLOWED + "\n")
    assert answer({"CI": "true", "GENIERAILS_ALLOW_CI_APPLY": "1"}, "rebind") == (1, ci_guard.REFUSED + "\n")


@pytest.mark.parametrize("failure", ["import", "raise", "garbled-answer", "empty-answer"])
def test_python_call_sites_count_a_broken_ci_guard_as_ci(monkeypatch, failure):
    sys.path.insert(0, str(SHARED))
    from scripts import env_status, policy_generations, staged_policy_apply

    if failure == "import":
        import scripts

        monkeypatch.setitem(sys.modules, "scripts.ci_guard", None)  # import fails
        monkeypatch.delattr(scripts, "ci_guard", raising=False)  # even after an earlier import bound it
    else:
        from scripts import ci_guard

        def boom(*args, **kwargs):
            raise RuntimeError("ci_guard is broken")

        if failure == "raise":
            monkeypatch.setattr(ci_guard, "status_line", boom)
            monkeypatch.setattr(ci_guard, "ci_markers", boom)
        else:  # a status_line that answers anything but its exact lines
            answer = "genierails-ci-status:v2:not-ci" if failure == "garbled-answer" else ""
            monkeypatch.setattr(ci_guard, "status_line", lambda *args, **kwargs: answer)
    assert staged_policy_apply._in_ci({}) is True
    from scripts import approval

    assert approval._in_ci({}) is True  # nobody is asked
    assert staged_policy_apply._in_ci({"GENIERAILS_ALLOW_CI_APPLY": "1"}) is True
    with pytest.raises(policy_generations.StageError, match="can't run in CI"):
        policy_generations.rebind(Path("/nonexistent/envs/dev"), {}, "rebind-protect")
    ci = env_status._ci(Path("/nonexistent/aws"), Path("/nonexistent/aws"))
    assert ci["detected"] is True and ci["allow_ci_apply"] is False
    assert env_status.ci_refusal("make release-plan ENV=prod", ci, Path("/nonexistent/aws"))
