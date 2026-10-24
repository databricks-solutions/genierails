"""terraform_layer.sh hands Terraform no Databricks or Azure auth from the shell.

The roots' providers take host and credentials from the env's auth.auto.tfvars,
but the Databricks provider also reads DATABRICKS_*, ARM_* and ~/.databrickscfg:
a stray token fails "more than one authorization method", and a DATABRICKS_HOST
fills a host the env leaves empty. Terraform here is a fake on PATH that records
the auth variables of every call (init included); provisioners inherit its env.
Both the runner and test.sh get that env from scripts/terraform_env.sh, and a
static check keeps every other script from running Terraform itself.
"""

import os
import re
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
{ printf '== %s\\n' "$*"; env | grep -E '^(DATABRICKS|ARM|ACTIONS_ID_TOKEN|SYSTEM)_' | sort; } >> "$FAKE_TF_LOG"
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


def _run(tmp_path, layer, command, *args, env_dir=None, **extra):
    env_dir = env_dir or tmp_path / "envs" / "dev"
    env_dir.mkdir(parents=True, exist_ok=True)
    (env_dir / "auth.auto.tfvars").write_text('databricks_workspace_host = "https://env-file.invalid"\n')
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
    proc = subprocess.run([str(RUNNER), layer, "dev", command, *args], env=env, text=True,
                          capture_output=True, timeout=120)
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
        if "GITHUB_ACTIONS" not in ci_context or ci_context["GITHUB_ACTIONS"] != "true":
            assert not GITHUB_TOKEN_VARS & set(auth_env), argv
        if "TF_BUILD" not in ci_context:
            assert not ADO_TOKEN_VARS & set(auth_env), argv


@pytest.mark.parametrize("auth_type", ["pat", "databricks-cli", "oauth-m2m", "azure-cli"])
def test_other_ambient_auth_types_are_dropped(tmp_path, auth_type):
    calls = _run(tmp_path, "workspace", "plan", DATABRICKS_AUTH_TYPE=auth_type)
    assert all("DATABRICKS_AUTH_TYPE" not in auth_env for _, auth_env in calls)


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
