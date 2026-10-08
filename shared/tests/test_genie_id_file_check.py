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
    assert err.startswith("WARNING: 1 Genie agent(s) created by GenieRails lost their ID file; make apply would refuse:")
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
        "runner": ('printf "runner %s\\n" "$3" >> "{log}"; '
                   'if [ "$3" = console ]; then cat >/dev/null; [ -n "$DESIRED" ] || exit 1; '
                   'printf \'"%s"\\n\' "$(printf %s "$DESIRED" | base64)"; fi; exit 0'),
        "importer": 'exit 0',
    }.items():
        path = tmp_path / name
        path.write_text("#!/bin/sh\n" + body.format(log=log) + "\n")
        path.chmod(0o755)
        stubs[name] = path
    return log, [f"_COVERAGE_GATE={stubs['gate']}", f"ROOT_RUNNER={stubs['runner']}",
                 f"IMPORT_EXISTING_SCRIPT={stubs['importer']}"]


def _make(target, env, overrides, desired=None):
    extra = {"DESIRED": json.dumps(desired)} if desired is not None else {}
    return subprocess.run(["make", "--no-print-directory", target, "LAYER=workspace", "TARGET_ENV=prod",
                           f"LAYER_ENV_DIR={env}", *overrides],
                          cwd=ROOT / "aws", text=True, capture_output=True, env={**_clean_env(), **extra},
                          timeout=120)


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
    second = _make("_apply-layer", env, overrides, desired=[KEY])
    assert second.returncode != 0
    assert "lost their ID file; nothing was applied" in second.stderr
    assert "(still in config)" in second.stderr
    assert "inputs unchanged" not in second.stdout
    # Refused before the coverage gate, the skip and any plan or apply; only
    # the console question (which agents the config keeps) ran.
    assert _calls(log) == ["runner console"]


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
def test_apply_without_workspace_config_still_refuses(tmp_path):
    # The no-abac.auto.tfvars early exit used to return 0 before the check.
    env = _env(tmp_path, [_resource([_instance()])])
    log, overrides = _stubs(tmp_path)
    result = _make("_apply-layer", env, overrides)
    assert result.returncode != 0
    assert "lost their ID file; nothing was applied" in result.stderr
    assert "Skipping terraform apply" not in result.stdout
    assert "runner apply" not in _calls(log)


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
def test_removing_an_agent_without_its_id_file_is_refused_with_recovery(tmp_path):
    state = [_resource([_instance()]),
             _resource([{"index_key": KEY, "attributes": {"triggers": {"tables": "cat.sch.t"}}}],
                       rtype="null_resource", name="genie_space_config")]
    state[0]["instances"][0]["attributes"]["triggers_replace"] = {"value": {"host": "https://ws.example"},
                                                                  "type": "object"}
    env = _env(tmp_path, state)
    (env / "abac.auto.tfvars").write_text("# the agent is no longer configured\n")
    log, overrides = _stubs(tmp_path)
    result = _make("_apply-layer", env, overrides, desired=[])
    assert result.returncode != 0
    err = result.stderr
    assert "(removed from config, so this apply would trash it; on https://ws.example; tables cat.sch.t)" in err
    assert "the removal is refused too" in err and "a 404 counts as" in err
    assert "put it back in config" in err
    assert "still in config" not in err
    assert "runner apply" not in _calls(log) and not any(c.startswith("gate") for c in _calls(log))


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
def test_when_terraform_cant_answer_both_cases_are_described(tmp_path):
    env = _env(tmp_path, [_resource([_instance()])])
    (env / "abac.auto.tfvars").write_text("# config\n")
    log, overrides = _stubs(tmp_path)
    result = _make("_apply-layer", env, overrides)  # console fails (no DESIRED)
    assert result.returncode != 0
    err = result.stderr
    assert "(in config, or removed from it (could not ask Terraform))" in err
    assert "the removal is refused too" in err and "a config or ACL change" in err


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
def test_plan_warns_before_its_early_exit(tmp_path):
    env = _env(tmp_path, [_resource([_instance()])])  # no abac.auto.tfvars
    log, overrides = _stubs(tmp_path)
    result = _make("_plan-layer", env, overrides, desired=[KEY])
    assert result.returncode == 0, result.stdout + result.stderr
    assert "WARNING: 1 Genie agent(s) created by GenieRails lost their ID file; make apply would refuse" in result.stderr
    assert "Skipping terraform plan" in result.stdout


def test_an_unreadable_state_fails_closed(tmp_path, capsys):
    env = tmp_path / "prod"
    env.mkdir()
    (env / "terraform.tfstate").write_text("{not json")
    assert pf.main([str(env), "--id-files"]) == 1
    assert "cannot read" in capsys.readouterr().err
    assert pf.main([str(env), "--id-files", "--warn"]) == 0


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
def test_plan_warns_about_a_lost_id_file_and_still_plans(tmp_path):
    env = _env(tmp_path, [_resource([_instance()])])
    (env / "abac.auto.tfvars").write_text("# workspace config\n")
    log, overrides = _stubs(tmp_path)
    result = _make("_plan-layer", env, overrides)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "WARNING: 1 Genie agent(s) created by GenieRails lost their ID file" in result.stderr
    assert "runner plan" in _calls(log)


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
def test_maintain_warns_but_still_runs_governance(tmp_path):
    # maintain never applies the workspace layer: a lost Genie ID file must
    # not block masking new columns, so it warns and carries on.
    env = _env(tmp_path, [_resource([_instance()])])
    (env / "generated").mkdir()
    (env / "env.auto.tfvars").write_text("\n")
    log = tmp_path / "calls"
    make_stub = tmp_path / "make-stub"
    make_stub.write_text(f"#!/bin/sh\nprintf 'make %s\\n' \"$*\" >> '{log}'\nexit 0\n")
    make_stub.chmod(0o755)
    audit = tmp_path / "audit.py"
    audit.write_text("raise SystemExit(0)\n")
    result = subprocess.run(["make", "--no-print-directory", "maintain", "ENV=prod", f"ENV_DIR={env}",
                             f"MAKE={make_stub}", f"AUDIT_SCHEMA_SCRIPT={audit}"],
                            cwd=ROOT / "aws", text=True, capture_output=True, env=_clean_env(), timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "WARNING: 1 Genie agent(s) created by GenieRails lost their ID file; make apply would refuse" in result.stderr
    assert any("apply-governance" in c for c in _calls(log))


def _recipe(makefile, target):
    body = makefile[makefile.index(f"\n{target}:"):]
    return body[:body.index("\n\n")]


def test_the_check_runs_before_every_early_exit_of_both_layer_recipes():
    makefile = (SHARED / "Makefile.shared").read_text()
    apply = _recipe(makefile, "_apply-layer")
    check = apply.index('then $(_GENIE_ID_FILE_CHECK); fi')
    exits = [i for i in range(len(apply)) if apply.startswith("exit 0", i)]
    assert exits and all(check < i for i in exits)
    assert check < apply.index("skip-key") < apply.index("inputs unchanged")
    plan = _recipe(makefile, "_plan-layer")
    warn = plan.index("then $(_GENIE_ID_FILE_CHECK) --warn; fi")
    assert all(warn < i for i in range(len(plan)) if plan.startswith("exit 0", i))
    # maintain only warns (never refuses governance over Genie bookkeeping).
    maintain = _recipe(makefile, "maintain")
    assert '--id-files --warn;' in maintain and maintain.count("--id-files") == 1


# ── which agents the config keeps, asked of the real workspace root ────────
# terraform_data.genie_space manages managed_created_spaces: the create path's
# new_spaces plus #87's verified create-to-ID handoffs. A handoff is verified
# only while its ID file holds the configured ID, so the question is asked as
# if the files were restored: a handoff that lost its ID file is still kept
# (the missing-file refusal), and only a space gone from config is "removed".

HOST = "https://example.invalid"
CREATE_ID = "2afb2175-create"


def _console_env(tmp_path, spaces_hcl):
    from tests.terraform_helpers import shared_copy

    copy = shared_copy(tmp_path / "copy")
    env = tmp_path / "prod"
    env.mkdir()
    (env / "abac.auto.tfvars").write_text(
        'databricks_account_id = "account"\ndatabricks_client_id = "sp"\n'
        'databricks_client_secret = "secret"\ndatabricks_workspace_id = "123"\n'
        f'databricks_workspace_host = "{HOST}"\nsql_warehouse_id = "warehouse"\n'
        'groups = { analysts = {} }\n'
        f"genie_spaces = {spaces_hcl}\n"
    )
    attrs_data = {
        "id": CREATE_ID,
        "input": {"value": {"id_file": f"{env}/.genie_space_id_{KEY}"}, "type": ["object", {"id_file": "string"}]},
        "output": {"value": {"id_file": f"{env}/.genie_space_id_{KEY}"}, "type": ["object", {"id_file": "string"}]},
        "triggers_replace": {"value": {"host": HOST}, "type": ["object", {"host": "string"}]},
    }
    (env / "terraform.tfstate").write_text(json.dumps({
        "version": 4, "terraform_version": "1.11.4", "serial": 1, "lineage": "test", "outputs": {},
        "resources": [
            {"module": "module.workspace", "mode": "managed", "type": "terraform_data", "name": "genie_space",
             "provider": 'provider["terraform.io/builtin/terraform"]',
             "instances": [{"index_key": KEY, "schema_version": 0, "attributes": attrs_data,
                            "sensitive_attributes": []}]},
            {"module": "module.workspace", "mode": "managed", "type": "null_resource",
             "name": "genie_space_acls_created", "provider": 'provider["registry.terraform.io/hashicorp/null"]',
             "instances": [{"index_key": KEY, "schema_version": 0, "sensitive_attributes": [],
                            "attributes": {"id": "1", "triggers": {"space_create_id": CREATE_ID,
                                                                   "groups": "analysts"}}}]},
        ],
    }))
    return env, copy / "scripts" / "terraform_layer.sh"


def _real_check(tmp_path, monkeypatch, capsys, spaces_hcl):
    from tests.terraform_helpers import tf_env

    env, runner = _console_env(tmp_path, spaces_hcl)
    for name, value in tf_env(tmp_path).items():
        if name.startswith("TF_"):
            monkeypatch.setenv(name, value)
    monkeypatch.delenv("TF_DATA_DIR", raising=False)  # terraform_layer.sh sets its own
    desired = pf.desired_created_keys(env, "prod", str(runner))
    rc = pf.check_id_files(env, False, "prod", str(runner))
    return desired, rc, capsys.readouterr().err


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_a_handoff_that_lost_its_id_file_is_kept_not_removed(tmp_path, monkeypatch, capsys):
    # #87: the created agent is now configured by its explicit ID; with the
    # ID file gone Terraform's own handoff set drops it, but it is kept.
    spaces = f'[{{ name = "Sales", genie_space_id = "01handoff", uc_tables = ["cat.sch.t"] }}]'
    desired, rc, err = _real_check(tmp_path, monkeypatch, capsys, spaces)
    assert desired == {KEY}
    assert rc == 1
    assert "(still in config" in err and "a config or ACL change" in err
    assert "removed from config" not in err and "the removal is refused too" not in err


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_a_genuinely_removed_agent_gets_the_removal_message(tmp_path, monkeypatch, capsys):
    spaces = '[{ name = "Other", genie_space_id = "01other", uc_tables = ["cat.sch.o"] }]'
    desired, rc, err = _real_check(tmp_path, monkeypatch, capsys, spaces)
    assert desired == set()
    assert rc == 1
    assert "(removed from config, so this apply would trash it; on https://example.invalid)" in err
    assert "the removal is refused too" in err and "still in config" not in err


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_a_create_path_agent_is_kept(tmp_path, monkeypatch, capsys):
    spaces = '[{ name = "Sales", genie_space_id = "", uc_tables = ["cat.sch.t"] }]'
    desired, rc, err = _real_check(tmp_path, monkeypatch, capsys, spaces)
    assert desired == {KEY}
    assert rc == 1 and "(still in config" in err


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_the_handoff_split_keeps_terraform_s_own_handoff_set(tmp_path, monkeypatch):
    # created_acl_handoffs = candidates whose ID file holds the configured ID.
    from tests.terraform_helpers import tf_env

    spaces = f'[{{ name = "Sales", genie_space_id = "01handoff", uc_tables = ["cat.sch.t"] }}]'
    env, runner = _console_env(tmp_path, spaces)
    for name, value in tf_env(tmp_path).items():
        if name.startswith("TF_"):
            monkeypatch.setenv(name, value)
    monkeypatch.delenv("TF_DATA_DIR", raising=False)

    def ask(expression):
        result = subprocess.run([str(runner), "workspace", "prod", "console"], input=expression + "\n",
                                capture_output=True, text=True, timeout=600,
                                env={**os.environ, "LAYER_ENV_DIR": str(env)})
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads([l for l in result.stdout.splitlines() if l.strip()][-1])

    expression = 'jsonencode([keys(local.created_acl_handoffs), keys(local.created_acl_handoff_candidates)])'
    assert json.loads(ask(expression)) == [[], [KEY]]  # file missing: candidate, not a handoff
    (env / f".genie_space_id_{KEY}").write_text("01other\n")
    assert json.loads(ask(expression)) == [[], [KEY]]  # wrong ID: still not a handoff
    (env / f".genie_space_id_{KEY}").write_text("01handoff\n")
    assert json.loads(ask(expression)) == [[KEY], [KEY]]
