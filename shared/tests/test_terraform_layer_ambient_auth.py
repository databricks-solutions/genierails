"""terraform_layer.sh hands Terraform no Databricks or Azure auth from the shell.

The roots' providers take host and credentials from the env's auth.auto.tfvars,
but the Databricks provider also reads DATABRICKS_*, ARM_* and ~/.databrickscfg:
a stray token fails "more than one authorization method", and a DATABRICKS_HOST
fills a host the env leaves empty. Terraform here is a fake on PATH that records
the auth variables of every call (init included); provisioners inherit its env.
Both the runner and test.sh get that env from scripts/terraform_env.sh, and a
static check keeps every other script from running Terraform itself.
"""

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "shared" / "scripts" / "terraform_layer.sh"
TEST_SH = ROOT / "shared" / "test.sh"
AZURE = 'backend = "azurerm"\nresource_group_name = "r"\nstorage_account_name = "s"\ncontainer_name = "c"\n'
S3 = 'backend = "s3"\nbucket = "b"\nregion = "r"\ndynamodb_table = "locks"\n'
FAKE_TERRAFORM = """#!/usr/bin/env bash
{ printf '== %s\\n' "$*"; env | grep -E '^(DATABRICKS|ARM|ACTIONS_ID_TOKEN|SYSTEM)_|^TF_VAR_databricks_pipeline_auth_type=' | sort; } >> "$FAKE_TF_LOG"
exit 0
"""
AMBIENT = {
    "DATABRICKS_HOST": "https://ambient-wrong-ws.invalid",
    "DATABRICKS_TOKEN": "dapi-ambient",
    "DATABRICKS_CONFIG_PROFILE": "operator",
    "DATABRICKS_CLIENT_ID": "ambient-sp",
    "DATABRICKS_CLIENT_SECRET": "ambient-secret",
    "DATABRICKS_ACCOUNT_ID": "ambient-account",
    "DATABRICKS_AUTH_TYPE": "pat",
    "DATABRICKS_AZURE_RESOURCE_ID": "/subscriptions/x",
    "ARM_CLIENT_ID": "arm-client",
    "ARM_CLIENT_SECRET": "arm-secret",
    "ARM_TENANT_ID": "arm-tenant",
    "ARM_USE_MSI": "true",
    # A pipeline's OIDC token sources, set in a local shell.
    "ACTIONS_ID_TOKEN_REQUEST_URL": "https://spoofed.invalid/token",
    "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "spoofed-gha-token",
    "SYSTEM_ACCESSTOKEN": "spoofed-ado-token",
    "SYSTEM_OIDCREQUESTURI": "https://spoofed.invalid/oidc",
    "SYSTEM_TEAMFOUNDATIONCOLLECTIONURI": "https://dev.azure.com/spoofed/",
}
GITHUB_TOKEN_VARS = {"ACTIONS_ID_TOKEN_REQUEST_URL", "ACTIONS_ID_TOKEN_REQUEST_TOKEN"}
ADO_TOKEN_VARS = {"SYSTEM_ACCESSTOKEN", "SYSTEM_OIDCREQUESTURI", "SYSTEM_TEAMFOUNDATIONCOLLECTIONURI"}


def _calls(log: Path) -> list[tuple[str, dict]]:
    calls: list[tuple[str, dict]] = []
    for line in log.read_text().splitlines():
        if line.startswith("== "):
            calls.append((line[3:], {}))
        else:
            key, _, value = line.partition("=")
            calls[-1][1][key] = value
    return calls


def _run(tmp_path, layer, command, *args, env_dir=None, runner=RUNNER, check=True, **extra):
    env_dir = env_dir or tmp_path / "envs" / "dev"
    env_dir.mkdir(parents=True, exist_ok=True)
    auth = env_dir / "auth.auto.tfvars"
    auth.write_text('databricks_workspace_host = "https://env-file.invalid"\n' + (auth.read_text() if auth.exists() else ""))
    home = tmp_path / "home"
    home.mkdir(exist_ok=True)
    # An operator profile the provider would otherwise read (DATABRICKS_CONFIG_FILE unset).
    (home / ".databrickscfg").write_text("[DEFAULT]\nhost = https://profile.invalid\ntoken = dapi-profile\n")
    bin_dir = tmp_path / "bin"
    if not bin_dir.is_dir():
        bin_dir.mkdir()
        (bin_dir / "terraform").write_text(FAKE_TERRAFORM)
        (bin_dir / "terraform").chmod(0o755)
        for name in ("python3", "python"):
            (bin_dir / name).symlink_to(sys.executable)
    log = tmp_path / "tf.log"
    log.write_text("")
    env = {"PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "HOME": str(home), "LANG": "C",
           "FAKE_TF_LOG": str(log), "LAYER_ENV_DIR": str(env_dir), "ENVS_DIR": str(env_dir.parent),
           "DATABRICKS_USER_AGENT_EXTRA": "genierails/0.1.0", **AMBIENT, **extra}
    proc = subprocess.run([str(runner), layer, "dev", command, *args], env=env, text=True,
                          capture_output=True, timeout=120)
    if not check:
        return proc, _calls(log)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    calls = _calls(log)
    assert calls and calls[0][0].startswith("init "), calls
    return calls


@pytest.mark.parametrize("layer,command,args", [
    ("workspace", "plan", ()),
    ("workspace", "apply", ("-auto-approve",)),
    ("workspace", "import", ("databricks_group.g", "123")),
    ("workspace", "console", ()),
    ("workspace", "state-list", ()),
    ("workspace", "state-pull", ()),
    ("workspace", "show", ("-json", "plan.tfplan")),
    ("workspace", "apply-plan", ("plan.tfplan",)),
    ("data_access", "plan", ()),
    ("data_access", "apply", ("-auto-approve",)),
    ("account", "plan", ()),
    ("account", "apply", ("-auto-approve",)),
])
def test_no_ambient_databricks_or_arm_auth_reaches_terraform(tmp_path, layer, command, args):
    env_dir = tmp_path / "envs" / "dev" / ("data_access" if layer == "data_access" else "")
    if layer == "account":
        env_dir = tmp_path / "envs" / "account"
    calls = _run(tmp_path, layer, command, *args, env_dir=env_dir)
    for argv, auth_env in calls:
        assert auth_env == {"DATABRICKS_CONFIG_FILE": "/dev/null",
                            "DATABRICKS_USER_AGENT_EXTRA": "genierails/0.1.0"}, argv


@pytest.mark.parametrize("auth_type,ci_context,token_vars", [
    ("github-oidc", {"GITHUB_ACTIONS": "true"}, GITHUB_TOKEN_VARS),
    ("azure-devops-oidc", {"TF_BUILD": "True"}, ADO_TOKEN_VARS),
])
def test_pipeline_oidc_auth_reaches_terraform_only_in_its_ci_job(tmp_path, auth_type, ci_context, token_vars):
    # The pipeline samples write an IDs-only auth file and pick OIDC this way;
    # the provider mints the token from the job's own variables.
    calls = _run(tmp_path, "workspace", "plan", DATABRICKS_AUTH_TYPE=auth_type, **ci_context)
    for argv, auth_env in calls:
        assert auth_env.get("DATABRICKS_AUTH_TYPE") == auth_type, argv
        assert "TF_VAR_databricks_pipeline_auth_type" not in auth_env, argv
        # The providers' auth_type (it outranks DATABRICKS_AUTH_TYPE) gets the
        # kept OIDC from the runner, as its own -var.
        if not argv.startswith("init "):
            assert _var_options(argv)[-1] == "-var=databricks_pipeline_auth_type=" + auth_type, argv
        assert {k: auth_env.get(k) for k in token_vars} == {k: AMBIENT[k] for k in token_vars}, argv
        other = (GITHUB_TOKEN_VARS | ADO_TOKEN_VARS) - token_vars
        assert not other & set(auth_env), argv
        assert not {"DATABRICKS_TOKEN", "DATABRICKS_HOST", "DATABRICKS_CONFIG_PROFILE",
                    "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"} & set(auth_env), argv
        assert auth_env["DATABRICKS_CONFIG_FILE"] == "/dev/null", argv


@pytest.mark.parametrize("auth_type,ci_context", [
    ("github-oidc", {}),
    ("azure-devops-oidc", {}),
    ("github-oidc", {"GITHUB_ACTIONS": "false"}),
    ("github-oidc", {"TF_BUILD": "True"}),            # the other CI's context
    ("azure-devops-oidc", {"GITHUB_ACTIONS": "true"}),
    ("azure-devops-oidc", {"CI": "true"}),            # generic CI is not Azure Pipelines
])
def test_a_local_shell_cannot_present_pipeline_oidc(tmp_path, auth_type, ci_context):
    calls = _run(tmp_path, "workspace", "plan", DATABRICKS_AUTH_TYPE=auth_type, **ci_context)
    for argv, auth_env in calls:
        assert "DATABRICKS_AUTH_TYPE" not in auth_env, argv
        if not argv.startswith("init "):
            assert _var_options(argv)[-1] == "-var=databricks_pipeline_auth_type=", argv
        if "GITHUB_ACTIONS" not in ci_context or ci_context["GITHUB_ACTIONS"] != "true":
            assert not GITHUB_TOKEN_VARS & set(auth_env), argv
        if "TF_BUILD" not in ci_context:
            assert not ADO_TOKEN_VARS & set(auth_env), argv


@pytest.mark.parametrize("auth_type", ["pat", "databricks-cli", "oauth-m2m", "azure-cli"])
def test_other_ambient_auth_types_are_dropped(tmp_path, auth_type):
    calls = _run(tmp_path, "workspace", "plan", DATABRICKS_AUTH_TYPE=auth_type)
    assert all("DATABRICKS_AUTH_TYPE" not in auth_env for _, auth_env in calls)


def _var_options(argv: str) -> list[str]:
    return [arg for arg in argv.split() if arg.startswith("-var=")]


@pytest.mark.parametrize("layer,command,args", [
    ("workspace", "plan", ()),
    ("workspace", "apply", ("-auto-approve",)),
    ("workspace", "destroy", ("-auto-approve",)),
    ("workspace", "import", ("databricks_group.g", "123")),
    ("workspace", "console", ()),
    ("data_access", "plan", ()),
    ("data_access", "import", ("databricks_grant.g", "x")),
    ("account", "plan", ()),
    ("account", "apply", ("-auto-approve",)),
])
def test_the_runner_always_sets_its_switches_last(tmp_path, layer, command, args):
    # Every runner variable the root declares, in RUNNER_VARS order, empty or
    # false when inactive: not left to a default a root file could override
    # (override.tf, *_override.tf[.json]), nor to RUNNER_VALUE_* in the caller's shell.
    env_dir = tmp_path / "envs" / {"account": "account", "data_access": "dev/data_access"}.get(layer, "dev")
    forged = {f"RUNNER_VALUE_{name}": "/forged" for name in
              ("env_dir", "workspace_state_file", "data_access_state_file", "genie_id_dir",
               "import_walk", "expected_pre_release_withholding", "structural_check",
               "databricks_pipeline_auth_type")}
    calls = _run(tmp_path, layer, command, *args, env_dir=env_dir, **forged)
    runner_call = [argv for argv, _ in calls if argv.startswith(command + " ")]
    assert len(runner_call) == 1, calls
    walk = "true" if command == "import" else "false"
    expected = {
        "workspace": [f"-var=env_dir={env_dir}", "-var=workspace_state_file=", "-var=data_access_state_file=",
                      "-var=genie_id_dir=", "-var=expected_pre_release_withholding=false",
                      f"-var=import_walk={walk}", f"-var={PIPELINE_VAR}="],
        "data_access": [f"-var=env_dir={env_dir}", "-var=data_access_state_file=",
                        "-var=structural_check=false", f"-var=import_walk={walk}", f"-var={PIPELINE_VAR}="],
        "account": [f"-var={PIPELINE_VAR}="],
    }[layer]
    argv = runner_call[0].split()
    if command == "import":
        assert argv[-2:] == list(args)
        argv = argv[:-2]
    assert argv[-len(expected):] == expected, argv
    assert "/forged" not in runner_call[0]


def _project(tmp_path) -> Path:
    """A copy of the runner and its scripts, with roots of its own (the real
    roots' .tf files: the runner passes the runner variables they declare)."""
    project = tmp_path / "project"
    shutil.copytree(RUNNER.parent, project / "scripts", ignore=shutil.ignore_patterns("__pycache__"))
    for layer in ("account", "data_access", "workspace"):
        (project / "roots" / layer).mkdir(parents=True)
        for tf in (RUNNER.parents[1] / "roots" / layer).glob("*.tf"):
            shutil.copy(tf, project / "roots" / layer)
    return project


PIPELINE_VAR = "databricks_pipeline_auth_type"


def _injection(tmp_path, project, route):
    """Set the pipeline auth type the way ``route`` does; returns (args, env)."""
    env_dir = tmp_path / "envs" / "dev"
    env_dir.mkdir(parents=True, exist_ok=True)
    root = project / "roots" / "workspace"
    hcl = f'{PIPELINE_VAR} = "azure-devops-oidc"\n'
    if route == "-var=":
        return [f"-var={PIPELINE_VAR}=azure-devops-oidc"], {}
    if route == "-var split":
        return ["-var", f"{PIPELINE_VAR}=azure-devops-oidc"], {}
    if route == "--var=":
        return [f"--var={PIPELINE_VAR}=azure-devops-oidc"], {}
    if route in ("-var-file", "-var-file split", "-var-file json"):
        path = tmp_path / ("caller.tfvars.json" if route.endswith("json") else "caller.tfvars")
        path.write_text(json.dumps({PIPELINE_VAR: "azure-devops-oidc"}) if route.endswith("json") else hcl)
        return (["-var-file", str(path)] if route.endswith("split") else [f"-var-file={path}"]), {}
    if route in ("env auth.auto.tfvars", "env env.auto.tfvars"):
        name = route.split()[1]
        (env_dir / name).write_text(hcl)
        return [], {}
    if route.startswith("root "):
        name = route.split()[1]
        (root / name).write_text(json.dumps({PIPELINE_VAR: "azure-devops-oidc"}) if name.endswith(".json") else hcl)
        return [], {}
    if route in ("TF_CLI_ARGS", "TF_CLI_ARGS_plan"):
        return [], {route: f"-var={PIPELINE_VAR}=azure-devops-oidc"}
    if route == "TF_VAR_":
        return [], {f"TF_VAR_{PIPELINE_VAR}": "azure-devops-oidc"}
    raise AssertionError(route)


INJECTION_ROUTES = ["-var=", "-var split", "--var=", "-var-file", "-var-file split", "-var-file json",
                    "env auth.auto.tfvars", "env env.auto.tfvars", "root terraform.tfvars",
                    "root terraform.tfvars.json", "root caller.auto.tfvars", "root caller.auto.tfvars.json",
                    "TF_CLI_ARGS", "TF_CLI_ARGS_plan", "TF_VAR_"]


@pytest.mark.parametrize("ci_context", [{}, {"GITHUB_ACTIONS": "true", "DATABRICKS_AUTH_TYPE": "github-oidc"}],
                         ids=["local", "github-actions"])
@pytest.mark.parametrize("route", INJECTION_ROUTES)
def test_a_caller_cannot_set_the_pipeline_auth_type(tmp_path, route, ci_context):
    # The providers trust databricks_pipeline_auth_type over the env's secret;
    # only the runner sets it, from the OIDC terraform_env.sh kept.
    project = _project(tmp_path)
    args, extra = _injection(tmp_path, project, route)
    proc, calls = _run(tmp_path, "workspace", "plan", *args, runner=project / "scripts" / RUNNER.name,
                       check=False, **ci_context, **extra)
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert f"sets {PIPELINE_VAR}" in proc.stderr, proc.stderr
    assert f"{PIPELINE_VAR} only by it" in proc.stderr, proc.stderr
    assert calls == [], calls  # refused before Terraform ran (init included)


@pytest.mark.parametrize("name,text", [
    # Bare labels are valid HCL (terraform fmt accepts them, and would quote them).
    ("extra.tf", 'variable workspace_state_file {\n  default = ""\n}\n\nvariable "genie_id_dir" {\n  default = ""\n}\n'),
    ("extra.tf.json", json.dumps({"variable": {"workspace_state_file": {"default": ""}, "genie_id_dir": {"default": ""}}})),
], ids=["bare-label-tf", "tf-json"])
def test_the_runner_finds_every_runner_variable_a_root_declares(tmp_path, name, text):
    # The account root declares only the pipeline auth type; one more file
    # declaring two runner variables gets them both from the runner, empty.
    project = _project(tmp_path)
    (project / "roots" / "account" / name).write_text(text)
    calls = _run(tmp_path, "account", "plan", env_dir=tmp_path / "envs" / "account",
                 runner=project / "scripts" / RUNNER.name)
    assert _var_options(calls[1][0])[-3:] == ["-var=workspace_state_file=", "-var=genie_id_dir=",
                                              f"-var={PIPELINE_VAR}="], calls[1][0]


@pytest.mark.parametrize("name,text", [
    ("broken.tf", 'variable "workspace_state_file" {\n  default = ""\n'),
    ("broken.tf.json", '{"variable": {"workspace_state_file": '),
])
def test_a_root_file_that_cannot_be_parsed_refuses(tmp_path, name, text):
    # What it declares is unknown, so the runner can't set all its variables.
    project = _project(tmp_path)
    (project / "roots" / "workspace" / name).write_text(text)
    proc, calls = _run(tmp_path, "workspace", "plan", runner=project / "scripts" / RUNNER.name, check=False)
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert f"{name} can't be parsed" in proc.stderr and "can't all be set" in proc.stderr, proc.stderr
    assert calls == [], calls  # refused before Terraform ran (init included)


def test_every_shipped_root_parses_and_declares_its_runner_variables():
    # The runner parses each root with python-hcl2 on every run; a root it
    # can't parse refuses every command, so the shipped ones must parse.
    import sys
    sys.path.insert(0, str(RUNNER.parent))
    from state_layout import RUNNER_VARS, declared_runner_vars
    roots = RUNNER.parents[1] / "roots"
    assert declared_runner_vars(roots / "workspace") == [
        name for name in RUNNER_VARS if name != "structural_check"]
    assert declared_runner_vars(roots / "data_access") == [
        "env_dir", "data_access_state_file", "structural_check", "import_walk", "databricks_pipeline_auth_type"]
    assert declared_runner_vars(roots / "account") == ["databricks_pipeline_auth_type"]


def test_a_root_dir_tfvars_without_runner_variables_still_runs(tmp_path):
    project = _project(tmp_path)
    (project / "roots" / "workspace" / "caller.auto.tfvars").write_text('coverage_gate_max_age = "1h"\n')
    calls = _run(tmp_path, "workspace", "plan", runner=project / "scripts" / RUNNER.name)
    assert _var_options(calls[1][0])[-1] == f"-var={PIPELINE_VAR}="


def test_azurerm_backend_keeps_the_arm_identity_it_reads_state_with(tmp_path):
    env_dir = tmp_path / "azure" / "envs" / "dev"
    env_dir.mkdir(parents=True)
    (env_dir / "remote_state.hcl").write_text(AZURE)
    calls = _run(tmp_path, "workspace", "state-list", env_dir=env_dir, ARM_USE_OIDC="true")
    assert "-backend-config=storage_account_name=s" in calls[0][0]
    for argv, auth_env in calls:
        assert {k: v for k, v in auth_env.items() if k.startswith("ARM_")} == {
            "ARM_CLIENT_ID": "arm-client", "ARM_CLIENT_SECRET": "arm-secret",
            "ARM_TENANT_ID": "arm-tenant", "ARM_USE_MSI": "true", "ARM_USE_OIDC": "true"}, argv
        assert not {"DATABRICKS_TOKEN", "DATABRICKS_HOST", "DATABRICKS_CONFIG_PROFILE"} & set(auth_env), argv
        assert auth_env["DATABRICKS_CONFIG_FILE"] == "/dev/null", argv


def test_s3_backend_drops_arm_variables(tmp_path):
    env_dir = tmp_path / "aws" / "envs" / "dev"
    env_dir.mkdir(parents=True)
    (env_dir / "remote_state.hcl").write_text(S3)
    calls = _run(tmp_path, "workspace", "state-list", env_dir=env_dir)
    assert "-backend-config=bucket=b" in calls[0][0]
    assert all(not any(k.startswith("ARM_") for k in auth_env) for _, auth_env in calls)


def test_shared_test_sh_validates_with_no_ambient_auth(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "terraform").write_text(FAKE_TERRAFORM)
    (bin_dir / "terraform").chmod(0o755)
    for name in ("python3", "python"):
        (bin_dir / name).symlink_to(sys.executable)
    home = tmp_path / "home"
    home.mkdir()
    log = tmp_path / "tf.log"
    log.write_text("")
    env = {"PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "HOME": str(home), "LANG": "C",
           "FAKE_TF_LOG": str(log), "TMPDIR": str(tmp_path), "DATABRICKS_AUTH_TYPE": "github-oidc", **AMBIENT}
    subprocess.run(["bash", str(TEST_SH)], env=env, text=True, capture_output=True, timeout=300)
    calls = _calls(log)
    assert [argv.split()[1] for argv, _ in calls] == ["validate"], calls
    assert calls[0][1] == {"DATABRICKS_CONFIG_FILE": "/dev/null"}


# A command-position "terraform" (shell, Makefile recipe) or a "terraform"
# argv[0] (Python). Strings and comments are blanked first, so a message that
# names a terraform command isn't one.
_SHELL_CALL = re.compile(r"(?:^|[;&|(`{!]|\$\(|\b(?:if|then|else|do|exec|command|env|time|while|until)\s)\s*terraform\b(?!_)")
_PY_CALL = re.compile(r"""[\[(]\s*(?:str\()?["']terraform["']\s*[,)\]]""")


def _blank_strings(line: str) -> str:
    out, quote, i = [], None, 0
    while i < len(line):
        c = line[i]
        if quote:
            if c == "\\":
                i += 2
                continue
            if c == quote:
                quote = None
                out.append(c)
            i += 1
            continue
        if c == "#":
            break
        if c in "'\"":
            quote = c
        out.append(c)
        i += 1
    return "".join(out)


def _terraform_callers() -> dict[str, list[int]]:
    files = subprocess.run(["git", "-C", str(ROOT), "ls-files"], text=True, capture_output=True,
                           check=True).stdout.split()
    found: dict[str, list[int]] = {}
    for rel in files:
        if re.search(r"(^|/)tests?/", rel):
            continue
        path = ROOT / rel
        name = path.name
        if name.endswith(".py"):
            pattern, blank = _PY_CALL, False
        elif name.endswith((".sh", ".mk", ".bash")) or name.startswith("Makefile"):
            pattern, blank = _SHELL_CALL, True
        else:
            continue
        for number, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
            if name.startswith("Makefile") or name.endswith(".mk"):
                if not line.startswith("\t"):
                    continue
                line = line.lstrip("\t@-+ ")
            text = _blank_strings(line) if blank else line
            if pattern.search(text):
                found.setdefault(rel, []).append(number)
    return found


def test_only_the_runner_and_test_sh_run_terraform_and_both_scrub_auth_first():
    callers = _terraform_callers()
    # The scanner sees the calls it guards against (else it could pass vacuously).
    assert "shared/scripts/terraform_layer.sh" in callers and "shared/test.sh" in callers, callers
    # move_scheduled_job.py runs only from terraform_layer.sh, after the scrub,
    # and inherits its environment.
    assert set(callers) == {"shared/scripts/terraform_layer.sh", "shared/test.sh",
                            "shared/scripts/move_scheduled_job.py"}, callers
    for rel in ("shared/scripts/terraform_layer.sh", "shared/test.sh"):
        lines = (ROOT / rel).read_text().splitlines()
        scrub = next(i for i, line in enumerate(lines, 1) if re.match(r"\s*genierails_terraform_env\b", line))
        # INIT_CMD=( only builds the init command; it runs after the scrub.
        runs = [n for n in callers[rel] if not lines[n - 2].rstrip().endswith("INIT_CMD=(")]
        assert runs and scrub < min(runs), (rel, scrub, runs)
        assert scrub < next(i for i, line in enumerate(lines, 1) if '"${INIT_CMD[@]}"' in line) \
            if rel.endswith("terraform_layer.sh") else True
    mover = (ROOT / "shared/scripts/terraform_layer.sh").read_text()
    assert mover.index("genierails_terraform_env") < mover.index("move_scheduled_job.py")
