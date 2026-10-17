"""Real Terraform: the runner's commands that take positional arguments.

Terraform reads a command's options only before its positionals (import:
ADDR ID; state mv: SOURCE DESTINATION), so the runner puts its own variables
(env_dir and the state snapshots) after the caller's options but before
those. The variables stay refused from every caller form for import, as for
plan. These run real Terraform through the real runner on a workspace root
with only the built-in terraform_data resource (no provider download), in a
copy of shared/; a terraform stand-in that parses as Terraform does covers
the argv where terraform isn't installed.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.terraform_helpers import TIMEOUT, shared_copy

real_terraform = pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")

RUNNER_VARS = ("env_dir", "workspace_state_file", "data_access_state_file", "genie_id_dir")
ADDR = "terraform_data.adopted"
ID = "0f6e2c4a-1111-2222-3333-444455556666"

# env_dir has no default: an import that never got the runner's variables fails.
ROOT_TF = """
terraform {
  backend "local" {}
}

variable "env_dir" {
  type = string
}

variable "workspace_state_file" {
  type    = string
  default = null
}

variable "data_access_state_file" {
  type    = string
  default = null
}

variable "genie_id_dir" {
  type    = string
  default = ""
}

resource "terraform_data" "adopted" {}
"""


@pytest.fixture
def layer(tmp_path):
    shared = shared_copy(tmp_path)
    root = shared / "roots" / "workspace"
    for path in root.glob("*.tf"):
        path.unlink()
    shutil.rmtree(root / "tests", ignore_errors=True)  # init would install the tftests' modules
    (root / "main.tf").write_text(ROOT_TF)
    env_dir = tmp_path / "envs" / "dev"
    (env_dir / "data_access").mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text("# no settings\n")
    runner = shared / "scripts" / "terraform_layer.sh"

    def run(*args, state_dir=None, **extra):
        environ = {k: v for k, v in os.environ.items() if not k.startswith("TF_CLI_ARGS")}
        environ |= {"LAYER_ENV_DIR": str(env_dir), "ENVS_DIR": str(env_dir.parent), "TF_IN_AUTOMATION": "1",
                    "GIT_CONFIG_GLOBAL": os.devnull, **extra}
        environ.pop("GENIERAILS_STATE_DIR", None)
        if state_dir is not None:
            Path(state_dir).mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
            environ["GENIERAILS_STATE_DIR"] = str(state_dir)
        return subprocess.run([str(runner), "workspace", "dev", *args], text=True, capture_output=True,
                              env=environ, timeout=TIMEOUT)

    return env_dir, run


def _state(path: Path) -> dict:
    return {r["type"] + "." + r["name"]: r["instances"][0]["attributes"]["id"]
            for r in json.loads(path.read_text())["resources"]}


def _state_path(env_dir: Path, state_dir: Path | None) -> Path:
    if state_dir is None:
        return env_dir / "terraform.tfstate"
    return next(state_dir.rglob("terraform.tfstate"))


def _logged(result, command: str) -> list[str]:
    """The argv the runner ran (its "+ terraform <command> ..." line)."""
    lines = [line for line in result.stdout.splitlines() if line.startswith(f"+ terraform {command} ")]
    assert lines, result.stdout + result.stderr
    return lines[-1][2:].split(" ")


@real_terraform
@pytest.mark.parametrize("layout", ["in-checkout", "state dir"])
@pytest.mark.parametrize("options", [[], ["-input=false", "-no-color"], ["-input=false", "--"]],
                         ids=["bare", "options", "double-dash"])
def test_import_puts_every_option_before_addr_and_id(layer, tmp_path, layout, options):
    env_dir, run = layer
    state_dir = tmp_path / "state" if layout == "state dir" else None
    result = run("import", *options, ADDR, ID, state_dir=state_dir)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Import successful!" in result.stdout
    assert _state(_state_path(env_dir, state_dir)) == {ADDR: ID}
    argv = _logged(result, "import")
    tail = ["--", ADDR, ID] if "--" in options else [ADDR, ID]
    assert argv[-len(tail):] == tail, argv
    options_part = argv[2:-len(tail)]
    assert all(a.startswith("-") for a in options_part), argv
    runner = [i for i, a in enumerate(options_part) if a.startswith("-var=") and a[5:].split("=")[0] in RUNNER_VARS]
    caller = [i for i, a in enumerate(options_part) if a in options]
    assert len(runner) == (4 if state_dir else 1), argv  # + genie_id_dir
    assert runner == list(range(len(options_part) - len(runner), len(options_part))), argv  # last option
    assert not caller or max(caller) < min(runner), argv


@real_terraform
@pytest.mark.parametrize("layout", ["in-checkout", "state dir"])
def test_state_mv_and_rm_after_an_import(layer, tmp_path, layout):
    env_dir, run = layer
    state_dir = tmp_path / "state" if layout == "state dir" else None
    assert run("import", ADDR, ID, state_dir=state_dir).returncode == 0
    moved = "terraform_data.renamed"
    result = run("state-mv", "-lock=false", ADDR, moved, state_dir=state_dir)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _logged(result, "state")[-3:] == ["-lock=false", ADDR, moved]
    assert _state(_state_path(env_dir, state_dir)) == {moved: ID}
    listed = run("state-list", state_dir=state_dir)
    assert listed.returncode == 0 and moved in listed.stdout.splitlines()
    result = run("state-rm", moved, state_dir=state_dir)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(_state_path(env_dir, state_dir).read_text())["resources"] == []


def _override(form: str, name: str, forged: Path, var_file: Path, env_dir: Path) -> tuple[list[str], dict]:
    var_file.write_text(f'{name} = "{forged}"\n')
    if form == "-var=NAME=":
        return [f"-var={name}={forged}"], {}
    if form == "-var NAME=":
        return ["-var", f"{name}={forged}"], {}
    if form == "--var=NAME=":
        return [f"--var={name}={forged}"], {}
    if form == "-var-file=FILE":
        return [f"-var-file={var_file}"], {}
    if form == "-var-file FILE":
        return ["-var-file", str(var_file)], {}
    if form == "after ID":
        return ["AFTER", f"-var={name}={forged}"], {}
    if form == "env tfvars":
        (env_dir / "abac.auto.tfvars").write_text(f'{name} = "{forged}"\n')
        return [], {}
    return [], {form: f"-var={name}={forged}"}


OVERRIDE_FORMS = ("-var=NAME=", "-var NAME=", "--var=NAME=", "-var-file=FILE", "-var-file FILE", "after ID",
                  "env tfvars", "TF_CLI_ARGS", "TF_CLI_ARGS_import")


@real_terraform
@pytest.mark.parametrize("name", RUNNER_VARS)
def test_import_still_refuses_a_caller_setting_the_runners_variables(layer, tmp_path, name):
    env_dir, run = layer
    state_dir = tmp_path / "state"
    # The same runner imports when nothing overrides (the refusal is not a blanket one) ...
    assert run("import", ADDR, ID, state_dir=state_dir).returncode == 0
    state_path = _state_path(env_dir, state_dir)
    before = state_path.read_bytes()
    forged = tmp_path / "forged.json"
    forged.write_text("{}")
    for form in OVERRIDE_FORMS:
        args, extra = _override(form, name, forged, tmp_path / "caller.tfvars", env_dir)
        if args[:1] == ["AFTER"]:
            argv = ["import", "terraform_data.other", "other-id", *args[1:]]
        else:
            argv = ["import", *args, "terraform_data.other", "other-id"]
        result = run(*argv, state_dir=state_dir, **extra)
        (env_dir / "abac.auto.tfvars").unlink(missing_ok=True)
        assert result.returncode == 2, (form, result.stdout + result.stderr)
        assert f"sets {name}" in result.stderr and "set only by" in result.stderr, (form, result.stderr)
        assert "+ terraform" not in result.stdout, form  # refused before terraform init
        assert state_path.read_bytes() == before, form


def test_print_cmd_shows_the_import_argv_terraform_accepts(tmp_path):
    shared = Path(__file__).resolve().parents[1]
    env_dir = tmp_path / "envs" / "dev"
    env_dir.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text("# no settings\n")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "terraform").write_text("#!/bin/sh\nexit 0\n")
    (bin_dir / "terraform").chmod(0o755)
    environ = {k: v for k, v in os.environ.items() if not k.startswith("TF_CLI_ARGS")} | {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "LAYER_ENV_DIR": str(env_dir),
        "ENVS_DIR": str(env_dir.parent), "TF_DATA_DIR": str(tmp_path / ".terraform")}
    environ.pop("GENIERAILS_STATE_DIR", None)
    result = subprocess.run([str(shared / "scripts" / "terraform_layer.sh"), "workspace", "dev", "print-cmd",
                             "import", "-input=false", ADDR, ID], text=True, capture_output=True, env=environ)
    assert result.returncode == 0, result.stdout + result.stderr
    argv = shlex.split(result.stdout.split(")", 1)[1])
    assert argv == [f"-var-file={env_dir}/env.auto.tfvars", "-input=false", f"-var=env_dir={env_dir}", ADDR, ID]


# A terraform stand-in that parses import as Terraform's flag package does:
# options end at the first argument that isn't one (or at "--"), and import
# takes exactly two after them.
PARSING_TERRAFORM = r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ["FAKE_TF_LOG"], "a") as log:
    log.write(json.dumps(args) + "\n")
if args[:1] == ["import"]:
    rest = args[1:]
    while rest and rest[0].startswith("-") and rest[0] != "-":
        if rest.pop(0) == "--":
            break
    if len(rest) != 2:
        sys.exit("The import command expects two arguments.")
'''


@pytest.mark.parametrize("layout", ["in-checkout", "state dir"])
def test_import_argv_parses_as_terraform_parses_it(tmp_path, layout):
    shared = Path(__file__).resolve().parents[1]
    env_dir = tmp_path / "envs" / "dev"
    (env_dir / "data_access").mkdir(parents=True)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "terraform").write_text(PARSING_TERRAFORM)
    (bin_dir / "terraform").chmod(0o755)
    log = tmp_path / "terraform.log"
    environ = {k: v for k, v in os.environ.items() if not k.startswith("TF_CLI_ARGS")} | {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}", "LAYER_ENV_DIR": str(env_dir),
        "ENVS_DIR": str(env_dir.parent), "FAKE_TF_LOG": str(log)}
    environ.pop("GENIERAILS_STATE_DIR", None)
    if layout == "state dir":
        (tmp_path / "state").mkdir(parents=True, exist_ok=True)  # GenieRails never creates a missing state dir
        environ["GENIERAILS_STATE_DIR"] = str(tmp_path / "state")
    result = subprocess.run([str(shared / "scripts" / "terraform_layer.sh"), "workspace", "dev", "import",
                             "-input=false", ADDR, ID], text=True, capture_output=True, env=environ)
    assert result.returncode == 0, result.stdout + result.stderr
    argv = [json.loads(line) for line in log.read_text().splitlines() if json.loads(line)[:1] == ["import"]][-1]
    runner = [a for a in argv if a.startswith("-var=") and a[5:].split("=")[0] in RUNNER_VARS]
    assert argv[-2:] == [ADDR, ID] and argv[-2 - len(runner):-2] == runner, argv
    assert len(runner) == (4 if layout == "state dir" else 1), argv  # + genie_id_dir
