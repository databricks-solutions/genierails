"""A Genie agent GenieRails created must keep its ID file.

Terraform state records only the ID file's path (terraform_data.genie_space
input.id_file), never the agent ID. With the file gone Terraform plans no
change and make's workspace apply skipped as "inputs unchanged", while the next
config or ACL change and make destroy could no longer find the agent. Found on
the live prod env: make apply-genie with the ID file moved aside returned 0.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
ROOT = SHARED.parent
SCRIPT = SHARED / "scripts" / "genie_adopt_preflight.py"
sys.path.insert(0, str(SHARED / "scripts"))

import genie_adopt_preflight as pf  # noqa: E402

KEY = "sales"
AGENT_ID = "01f1agent"


def _instance(key=KEY, **extra):
    return {"index_key": key, "schema_version": 0,
            "attributes": {"id": "x", "input": {"value": {"id_file": f"/old/path/.genie_space_id_{key}"}}}, **extra}


def _resource(instances, *, rtype="terraform_data", name="genie_space", module="module.workspace", mode="managed"):
    return {"module": module, "mode": mode, "type": rtype, "name": name, "instances": instances}


def _env(tmp_path, resources, id_files=None):
    env = tmp_path / "prod"
    env.mkdir(parents=True, exist_ok=True)
    (env / "terraform.tfstate").write_text(json.dumps({"version": 4, "resources": resources}))
    for key, content in (id_files or {}).items():
        (env / f".genie_space_id_{key}").write_text(content)
    return env


def test_created_agents_are_the_current_genie_space_objects(tmp_path):
    env = _env(tmp_path, [
        _resource([_instance("a"), _instance("tainted", status="tainted"), _instance("deposed", deposed="00ab")]),
        _resource([_instance("legacy")], rtype="null_resource", name="genie_space_create"),
        _resource([_instance("config")], rtype="null_resource", name="genie_space_config"),
        _resource([_instance("other_module")], module="module.other"),
        _resource([_instance("data")], mode="data"),
    ])
    assert pf.created_agents(env) == ["a"]


def test_no_state_means_nothing_to_check(tmp_path):
    env = tmp_path / "fresh"
    env.mkdir()
    assert pf.created_agents(env) == []
    assert pf.check_id_files(env, warn=False) == 0


@pytest.mark.parametrize("content", [f"{AGENT_ID}\n", AGENT_ID])
def test_present_id_file_passes(tmp_path, capsys, content):
    env = _env(tmp_path, [_resource([_instance()])], {KEY: content})
    assert pf.main([str(env), "--id-files"]) == 0
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("id_files", [{}, {KEY: ""}, {KEY: "  \n"}])
def test_missing_or_empty_id_file_fails_with_recovery_steps(tmp_path, capsys, id_files):
    env = _env(tmp_path, [_resource([_instance()])], id_files)
    assert pf.main([str(env), "--id-files"]) == 1
    err = capsys.readouterr().err
    assert "lost their ID file; nothing was applied" in err
    assert f".genie_space_id_{KEY} is missing or empty" in err
    assert "it is in the agent URL" in err and f"make plan ENV={env.name}" in err


def test_warn_reports_without_failing(tmp_path, capsys):
    env = _env(tmp_path, [_resource([_instance()])])
    assert pf.main([str(env), "--id-files", "--warn"]) == 0
    err = capsys.readouterr().err
    assert err.startswith("WARNING: 1 Genie agent(s) created by GenieRails lost their ID file:")
    assert "nothing was applied" not in err


def test_a_replaced_agent_is_not_blocked(tmp_path):
    # Tainted: Terraform replaces it and create re-adopts by ID file or title.
    env = _env(tmp_path, [_resource([_instance(status="tainted")])])
    assert pf.main([str(env), "--id-files"]) == 0


def test_id_files_mode_makes_no_workspace_call(tmp_path, monkeypatch):
    env = _env(tmp_path, [_resource([_instance()])])
    monkeypatch.setattr(pf, "get_status", lambda *a: pytest.fail("--id-files must not call the workspace"))
    monkeypatch.setattr(pf, "load_auth", lambda *a: pytest.fail("--id-files must not read credentials"))
    assert pf.main([str(env), "--id-files"]) == 1


# ── make: the workspace apply checks before its unchanged-inputs skip ──────

def _clean_env():
    return {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV")}


def _stubs(tmp_path):
    log = tmp_path / "calls"
    stubs = {}
    for name, body in {
        "gate": 'printf "gate %s\\n" "$1" >> "{log}"; [ "$1" = skip-key ] && echo key; exit 0',
        "runner": 'printf "runner %s\\n" "$3" >> "{log}"; exit 0',
        "importer": 'exit 0',
    }.items():
        path = tmp_path / name
        path.write_text("#!/bin/sh\n" + body.format(log=log) + "\n")
        path.chmod(0o755)
        stubs[name] = path
    return log, [f"_COVERAGE_GATE={stubs['gate']}", f"ROOT_RUNNER={stubs['runner']}",
                 f"IMPORT_EXISTING_SCRIPT={stubs['importer']}"]


def _make(target, env, overrides):
    return subprocess.run(["make", "--no-print-directory", target, "LAYER=workspace", "TARGET_ENV=prod",
                           f"LAYER_ENV_DIR={env}", *overrides],
                          cwd=ROOT / "aws", text=True, capture_output=True, env=_clean_env(), timeout=120)


def _calls(log):
    return log.read_text().splitlines() if log.exists() else []


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
def test_apply_refuses_a_lost_id_file_even_when_inputs_are_unchanged(tmp_path):
    env = _env(tmp_path, [_resource([_instance()])], {KEY: AGENT_ID})
    (env / "abac.auto.tfvars").write_text("# workspace config\n")
    log, overrides = _stubs(tmp_path)
    first = _make("_apply-layer", env, overrides)
    assert first.returncode == 0, first.stdout + first.stderr
    assert "runner apply" in _calls(log)
    assert (env / ".workspace.apply.sha").is_file()  # the next run with these inputs would skip

    (env / f".genie_space_id_{KEY}").unlink()  # the live test: ID file moved aside
    log.unlink()
    second = _make("_apply-layer", env, overrides)
    assert second.returncode != 0
    assert "lost their ID file; nothing was applied" in second.stderr
    assert "inputs unchanged" not in second.stdout
    assert _calls(log) == []  # refused before the coverage gate, the skip and Terraform


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
def test_plan_warns_about_a_lost_id_file_and_still_plans(tmp_path):
    env = _env(tmp_path, [_resource([_instance()])])
    (env / "abac.auto.tfvars").write_text("# workspace config\n")
    log, overrides = _stubs(tmp_path)
    result = _make("_plan-layer", env, overrides)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "WARNING: 1 Genie agent(s) created by GenieRails lost their ID file" in result.stderr
    assert "runner plan" in _calls(log)


def test_both_layer_recipes_run_the_check_before_terraform():
    makefile = (SHARED / "Makefile.shared").read_text()
    apply = makefile[makefile.index("\n_apply-layer:"):]
    apply = apply[:apply.index("\n\n")]
    check = apply.index('"$(GENIE_ADOPT_PREFLIGHT_SCRIPT)" "$$env_dir" --id-files;')
    assert check < apply.index("skip-key") < apply.index("inputs unchanged")
    plan = makefile[makefile.index("\n_plan-layer:"):]
    plan = plan[:plan.index("\n\n")]
    assert plan.index("--id-files --warn") < plan.index('"$$layer" "$$target_env" plan')
