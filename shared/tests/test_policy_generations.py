"""The versioned create/switch/retire hooks the step-6 staged-apply controller runs."""

import json
import os
import stat
from pathlib import Path

import hcl2
import pytest

import generate_abac
from governance_policies import apply_model, build_model
from scripts import policy_generations as pg

SHARED = Path(__file__).resolve().parents[1]
TIERS = ["raw", "regional", "masked"]


def _generation(env_dir: Path, versions=None, tiers=TIERS, columns=("email",)):
    """Write a generated deterministic config and return its generation."""
    model = build_model({f"cat.sch.t.{c}": ["ip_address"] for c in columns},
                        {f"cat.sch.t.{c}": "STRING" for c in columns},
                        {"access_tier_groups": tiers, "treatment_versions": versions or {}})
    generated = env_dir / "generated"
    generated.mkdir(parents=True, exist_ok=True)
    (generated / "abac.auto.tfvars").write_text("tag_policies = [\n]\ntag_assignments = [\n]\nfgac_policies = [\n]\n")
    generate_abac.write_deterministic_model(generated / "abac.auto.tfvars", generated / "masking_functions.sql", model)
    layer = env_dir / "data_access"
    layer.mkdir(exist_ok=True)
    (layer / "abac.auto.tfvars").write_text(
        'tag_assignments = [\n  { entity_type = "tables", entity_name = "cat.sch.t", tag_key = "row_scope", tag_value = "anz" }\n]\n'
        'fgac_policies = [\n  { name = "rf", policy_type = "POLICY_TYPE_ROW_FILTER", catalog = "cat", '
        'to_principals = ["regional"], function_name = "f", function_catalog = "cat", function_schema = "sch" }\n]\n')
    return pg.desired_generation(env_dir)


def test_desired_generation_is_stable_and_refuses_legacy_values(tmp_path):
    first = _generation(tmp_path)
    assert pg.desired_generation(tmp_path) == first
    (tmp_path / "generated" / "abac.auto.tfvars").write_text(
        'tag_assignments = [{ entity_type = "columns", entity_name = "c.s.t.x", tag_key = "gr_treatment", tag_value = "redact" }]\n')
    with pytest.raises(pg.StageError, match="not a deterministic generation"):
        pg.desired_generation(tmp_path)


def test_create_adds_the_replacement_and_changes_nothing_live(tmp_path):
    old = _generation(tmp_path)
    new = _generation(tmp_path, versions={"ip_address": {"partial": "redacted"}})
    rendered = pg.render_stage("create", new, old, None)
    names = {p["name"] for p in rendered["fgac_policies"]}
    assert names == {p["name"] for p in old["fgac_policies"] + new["fgac_policies"]}
    assert rendered["tags"] == old["tags"]
    for policy in old["fgac_policies"]:
        assert policy["function_name"] in rendered["functions_sql"]
    for policy in new["fgac_policies"]:
        assert policy["function_name"] in rendered["functions_sql"]


def test_create_keeps_old_principals_and_switch_moves_them_in_place(tmp_path):
    old = _generation(tmp_path)
    moved = _generation(tmp_path, tiers=["raw", "analysts", "masked"])
    assert old["tags"] == moved["tags"]  # a tier move needs no new function or value
    create = {p["name"]: p for p in pg.render_stage("create", moved, old, None)["fgac_policies"]}
    switch = {p["name"]: p for p in pg.render_stage("switch", moved, old, None)["fgac_policies"]}
    partial = next(name for name in create if name.endswith("_partial"))
    assert create[partial]["to_principals"] == ["regional"]
    assert switch[partial]["to_principals"] == ["analysts"]


def test_adopted_unknown_principals_are_unrenderable_until_tighten(tmp_path):
    desired = _generation(tmp_path)
    adopted = json.loads(json.dumps(desired))
    for policy in adopted["fgac_policies"]:
        policy["to_principals"] = ["account users" if p == "account users" else pg.UNKNOWN
                                   for p in policy.get("to_principals") or []]
        policy["except_principals"] = ["account users" if p == "account users" else pg.UNKNOWN
                                       for p in policy.get("except_principals") or []]

    create = pg.plan_stage("create", desired, adopted, None)
    assert any(
        pg.UNKNOWN in ((policy.get("to_principals") or []) + (policy.get("except_principals") or []))
        for policy in create["rendered"]["fgac_policies"]
    )
    assert not any(
        pg.UNKNOWN in ((policy.get("to_principals") or []) + (policy.get("except_principals") or []))
        for policy in create["coverage_rendered"]["fgac_policies"]
    )
    for stage in ("tighten", "switch"):
        rendered = pg.render_stage(stage, desired, adopted, None)
        assert not any(
            pg.UNKNOWN in ((policy.get("to_principals") or []) + (policy.get("except_principals") or []))
            for policy in rendered["fgac_policies"]
        )


def test_adopted_unknown_coverage_view_swaps_only_principals(tmp_path):
    desired = _generation(tmp_path)
    adopted = json.loads(json.dumps(desired))
    retained = next(p for p in adopted["fgac_policies"] if p["name"].endswith("_partial"))
    generated = next(p for p in desired["fgac_policies"] if p["name"] == retained["name"])
    retained["to_principals"] = [pg.UNKNOWN]
    retained["function_name"] = "live_mask_function"

    create = pg.plan_stage("create", desired, adopted, None)
    coverage = {p["name"]: p for p in create["coverage_rendered"]["fgac_policies"]}[retained["name"]]
    rendered = {p["name"]: p for p in create["rendered"]["fgac_policies"]}[retained["name"]]

    assert rendered["to_principals"] == [pg.UNKNOWN]
    assert rendered["function_name"] == "live_mask_function"
    assert coverage["to_principals"] == generated["to_principals"]
    assert coverage["function_name"] == "live_mask_function"


def test_tighten_refuses_empty_to_after_stripping_unknown(tmp_path):
    desired = _generation(tmp_path)
    old = json.loads(json.dumps(desired))
    first = desired["fgac_policies"][0]["name"]
    for generation, to_principals in ((desired, []), (old, [pg.UNKNOWN])):
        for policy in generation["fgac_policies"]:
            if policy["name"] == first:
                policy["to_principals"] = to_principals
                policy["except_principals"] = []

    planned = pg.plan_stage("tighten", desired, old, None)
    assert any(f"{first} would have no TO principals" == problem for problem in planned["problems"])


def test_switch_retags_and_keeps_retained_generations_until_retire(tmp_path):
    retired = _generation(tmp_path, versions={"ip_address": {"partial": "redacted"}})
    old = _generation(tmp_path)
    new = _generation(tmp_path, versions={"ip_address": {"partial": "ip_network"}}, columns=("email", "ip"))
    switch = pg.render_stage("switch", new, old, retired)
    assert switch["tags"] == new["tags"]
    kept = {p["name"] for g in (old, new, retired) for p in g["fgac_policies"]}
    assert {p["name"] for p in switch["fgac_policies"]} == kept
    retire = pg.render_stage("retire", new, old, retired)
    assert {p["name"] for p in retire["fgac_policies"]} == {p["name"] for g in (old, new) for p in g["fgac_policies"]}
    # Functions are never dropped by a stage.
    assert all(p["function_name"] in retire["functions_sql"] for p in retired["fgac_policies"])


def test_write_stage_keeps_row_filters_and_other_tags(tmp_path):
    old = _generation(tmp_path)
    out = pg.write_stage(tmp_path / "data_access", pg.render_stage("create", old, None, None))
    assert out == tmp_path / "generated" / ".runtime" / "data_access" == pg.stage_config_dir(tmp_path)
    cfg = hcl2.loads((out / "abac.auto.tfvars").read_text())
    assert [p["name"] for p in cfg["fgac_policies"]][0] == "rf"
    assert {a["tag_key"] for a in cfg["tag_assignments"]} == {"row_scope"}
    sql = (out / "masking_functions.sql").read_text()
    assert "USE CATALOG cat;\nUSE SCHEMA sch;" in sql


def test_write_stage_never_rewrites_the_split_config(tmp_path):
    """The split layer files are committed in a legacy or migrating env; a render only reads them."""
    old = _generation(tmp_path)
    layer = tmp_path / "data_access"
    (layer / "masking_functions.sql").write_text("-- committed\n")
    before = {name: (layer / name).read_bytes() for name in ("abac.auto.tfvars", "masking_functions.sql")}
    for stage in ("create", "switch"):
        pg.write_stage(layer, pg.render_stage(stage, old, None, None))
        assert {name: (layer / name).read_bytes() for name in before} == before
    assert sorted(p.name for p in layer.iterdir()) == sorted(before)


def _change(rtype, index, *actions, address=None):
    return {"type": rtype, "index": index, "address": address or f"module.data_access.{rtype}.x",
            "change": {"actions": list(actions)}}


def test_plan_check_refuses_every_protected_destroy():
    plan = {"resource_changes": [
        _change("databricks_policy_info", "p1", "delete"),
        _change("databricks_policy_info", "p2", "delete", "create"),
        _change("databricks_entity_tag_assignment", "t", "delete"),
        _change("terraform_data", None, "delete", address="module.data_access.terraform_data.masking_functions_drop"),
        _change("terraform_data", None, "delete", "create", address="module.data_access.terraform_data.masking_functions"),
        _change("databricks_policy_info", "p3", "update"),
    ]}
    problems = pg.plan_violations(plan, "create")
    assert len(problems) == 4 and not any("masking_functions:" in p or "p3" in p for p in problems)
    # retire may delete exactly the retire generation's policies, never replace them.
    assert len(pg.plan_violations(plan, "retire", {"p1", "p2"})) == 3


def _stub_runner(tmp_path: Path, plan: dict) -> Path:
    log = tmp_path / "runner.log"
    runner = tmp_path / "runner.sh"
    runner.write_text(
        "#!/bin/sh\n"
        f"echo \"$@\" >> '{log}'\n"
        f"if [ \"$3\" = show ]; then echo '+ terraform show'; echo '{json.dumps(plan)}'; fi\n"
    )
    runner.chmod(runner.stat().st_mode | stat.S_IEXEC)
    return runner


def _environ(env_dir, fingerprint, old=""):
    return {**os.environ, "GENIERAILS_ENV_DIR": str(env_dir), "GENIERAILS_ENV_NAME": "dev",
            "GENIERAILS_POLICY_FINGERPRINT": fingerprint, "GENIERAILS_OLD_POLICY_FINGERPRINT": old,
            "GENIERAILS_RETIRE_FINGERPRINT": ""}


def test_hook_plans_checks_and_applies_exactly_that_plan(tmp_path, monkeypatch):
    env_dir = tmp_path / "dev"
    gen = _generation(env_dir)
    monkeypatch.setattr(pg, "RUNNER", _stub_runner(tmp_path, {"resource_changes": []}))
    assert pg.run_hook("create", _environ(env_dir, gen["fingerprint"])) == 0
    calls = (tmp_path / "runner.log").read_text().splitlines()
    assert [c.split()[2] for c in calls] == ["plan", "show", "apply-plan"]
    assert (env_dir / "data_access" / ".policy_generations" / f"{gen['fingerprint']}.json").is_file()


def test_hook_refuses_a_destroying_plan_and_applies_nothing(tmp_path, monkeypatch):
    env_dir = tmp_path / "dev"
    gen = _generation(env_dir)
    monkeypatch.setattr(pg, "RUNNER", _stub_runner(
        tmp_path, {"resource_changes": [_change("databricks_policy_info", "live", "delete")]}))
    assert pg.run_hook("switch", _environ(env_dir, gen["fingerprint"])) == 1
    assert "apply-plan" not in (tmp_path / "runner.log").read_text()


def test_hook_refuses_a_stale_fingerprint_or_an_unknown_live_generation(tmp_path, monkeypatch):
    env_dir = tmp_path / "dev"
    gen = _generation(env_dir)
    monkeypatch.setattr(pg, "RUNNER", _stub_runner(tmp_path, {"resource_changes": []}))
    with pytest.raises(pg.StageError, match="controller is applying"):
        pg.run_hook("create", _environ(env_dir, "0" * 16))
    with pytest.raises(pg.StageError, match="no snapshot of live generation"):
        pg.run_hook("create", _environ(env_dir, gen["fingerprint"], old="f" * 16))
    assert not (tmp_path / "runner.log").exists()


@pytest.mark.parametrize("stage", pg.STAGES)
def test_hook_executables_are_argv_programs_for_each_stage(stage):
    hook = SHARED / "scripts" / "policy_hooks" / stage
    assert os.access(hook, os.X_OK)
    assert hook.read_text().rstrip().endswith(f"policy_generations.py\" hook {stage}")


def test_layer_runner_supports_show_and_saved_plan_apply():
    runner = (SHARED / "scripts" / "terraform_layer.sh").read_text()
    assert "  show)\n    CMD=(terraform show \"$@\")" in runner
    assert "  apply-plan)\n" in runner and 'CMD=(terraform apply -input=false "$@")' in runner


def test_snapshot_is_kept_next_to_state_not_in_generated(tmp_path):
    gen = _generation(tmp_path)
    path = pg.save_snapshot(tmp_path, gen)
    assert path.parent == tmp_path / "data_access" / ".policy_generations"
    assert pg.load_snapshot(tmp_path, gen["fingerprint"]) == json.loads(json.dumps(gen))
    assert pg.load_snapshot(tmp_path, "") is None


def test_apply_model_output_is_what_desired_generation_reads(tmp_path):
    gen = _generation(tmp_path)
    cfg = hcl2.loads((tmp_path / "generated" / "abac.auto.tfvars").read_text())
    assert [p["name"] for p in gen["fgac_policies"]] == [p["name"] for p in apply_model(cfg, build_model(
        {"cat.sch.t.email": ["ip_address"]}, {"cat.sch.t.email": "STRING"}, {"access_tier_groups": TIERS},
    )).get("fgac_policies")]


# -- step 6: the stages the staged controller drives ------------------------------------------
def _coverage(policies, value, who):
    return sorted(p["name"] for p in policies if pg.policy_value(p) == value and pg._covers(p, who))


def test_create_keeps_the_active_generations_principals_over_a_retained_older_one(tmp_path):
    # G1 (old tiers) waits to retire; G2 moved tiers in place (same names); G3 changes a treatment.
    retire = _generation(tmp_path)
    old = _generation(tmp_path, tiers=["raw", "analysts", "masked"])
    new = _generation(tmp_path, tiers=["raw", "analysts", "masked"], versions={"ip_address": {"partial": "redacted"}})
    create = {p["name"]: p for p in pg.render_stage("create", new, old, retire)["fgac_policies"]}
    for policy in old["fgac_policies"]:
        # Re-applying G1's principals here would be an unstaged tier move back.
        assert create[policy["name"]]["to_principals"] == policy["to_principals"]
        assert create[policy["name"]]["except_principals"] == policy["except_principals"]


@pytest.mark.parametrize("before, after", [
    (["raw", "regional", "masked"], ["raw", "analysts", "masked"]),
    (["raw", "masked"], ["raw", "regional", "masked"]),
    (["raw", "regional", "masked"], ["raw", "masked"]),
    (["raw", "regional", "masked"], ["regional", "raw", "masked"]),
])
def test_tighten_never_uncovers_anyone_the_old_or_new_generation_masks(tmp_path, before, after):
    old = _generation(tmp_path, tiers=before)
    new = _generation(tmp_path, tiers=after)
    assert pg.has_tier_move(new, old, None)
    universe = pg._universe(old, new)
    stages = {s: pg.plan_stage(s, new, old, None) for s in ("create", "tighten", "switch")}
    for stage, planned in stages.items():
        assert planned["problems"] == [], (stage, planned["problems"])
    for value in set(new["tags"].values()):
        for who in universe:
            tight = _coverage(stages["tighten"]["rendered"]["fgac_policies"], value, who)
            if _coverage(old["fgac_policies"], value, who) or _coverage(new["fgac_policies"], value, who):
                assert tight, (who, value)
            # create changes nobody's mask; switch gives everyone exactly the new one.
            assert _coverage(stages["create"]["rendered"]["fgac_policies"], value, who) == \
                _coverage(old["fgac_policies"], value, who)
            assert _coverage(stages["switch"]["rendered"]["fgac_policies"], value, who) == \
                _coverage(new["fgac_policies"], value, who)


def test_a_new_policy_on_a_value_columns_carry_waits_for_tighten(tmp_path):
    old = _generation(tmp_path, tiers=["raw", "masked"])
    new = _generation(tmp_path, tiers=["raw", "regional", "masked"])
    partial = next(p["name"] for p in new["fgac_policies"] if p["name"].endswith("_partial"))
    assert partial not in {p["name"] for p in pg.render_stage("create", new, old, None)["fgac_policies"]}
    assert partial in {p["name"] for p in pg.render_stage("tighten", new, old, None)["fgac_policies"]}
    # A restore after tighten removes it again (allowed), and nothing else.
    assert pg.plan_stage("restore", new, old, None)["allowed_policy_deletes"] == [partial]


def test_switch_drops_a_policy_the_new_generation_no_longer_has_on_a_value_in_use(tmp_path):
    old = _generation(tmp_path, tiers=["raw", "regional", "masked"])
    new = _generation(tmp_path, tiers=["raw", "masked"])
    partial = next(p["name"] for p in old["fgac_policies"] if p["name"].endswith("_partial"))
    switch = pg.plan_stage("switch", new, old, None)
    assert partial not in {p["name"] for p in switch["rendered"]["fgac_policies"]}
    assert switch["allowed_policy_deletes"] == [partial] and switch["problems"] == []


def test_switch_keeps_obsolete_values_policies_for_a_later_retire(tmp_path):
    old = _generation(tmp_path)
    new = _generation(tmp_path, versions={"ip_address": {"partial": "redacted"}})
    switch = pg.plan_stage("switch", new, old, None)
    assert switch["allowed_policy_deletes"] == []
    assert {p["name"] for p in old["fgac_policies"]} <= {p["name"] for p in switch["rendered"]["fgac_policies"]}
    retire = pg.plan_stage("retire", new, None, old)
    assert set(retire["allowed_policy_deletes"]) == {p["name"] for p in old["fgac_policies"]}


def test_only_switch_untags_a_column_that_left_governance(tmp_path):
    old = _generation(tmp_path, columns=("email", "ip"))
    new = _generation(tmp_path, columns=("email",))
    key = "columns|cat.sch.t.ip|gr_treatment"
    assert pg.plan_stage("switch", new, old, None)["allowed_tag_deletes"] == [key]
    for stage in ("create", "tighten"):
        assert pg.plan_stage(stage, new, old, None)["allowed_tag_deletes"] == []
        assert "cat.sch.t.ip" in pg.render_stage(stage, new, old, None)["tags"]


def test_render_check_refuses_an_unmasked_value_and_a_double_mask(tmp_path):
    old = _generation(tmp_path)
    broken = dict(old, fgac_policies=[p for p in old["fgac_policies"] if p["name"].endswith("_partial")])
    assert any("which no policy masks" in p or "unmask" in p or "masks on" in p
               for p in pg.stage_problems("switch", {**old, "fgac_policies": []}, old, None))
    doubled = [dict(p, to_principals=["account users"], except_principals=[]) for p in old["fgac_policies"]]
    problems = pg.stage_problems("switch", {**old, "fgac_policies": doubled}, old, old)
    assert any("would get 2 masks" in p for p in problems)
    assert pg.stage_problems("create", pg.render_stage("create", old, broken, None), old, broken) == []


def test_hook_refuses_an_unsafe_render_before_planning(tmp_path, monkeypatch):
    env_dir = tmp_path / "dev"
    gen = _generation(env_dir)
    monkeypatch.setattr(pg, "RUNNER", _stub_runner(tmp_path, {"resource_changes": []}))
    monkeypatch.setattr(pg, "stage_problems", lambda *a: ["x would get 2 masks"])
    assert pg.run_hook("switch", _environ(env_dir, gen["fingerprint"])) == 1
    assert not (tmp_path / "runner.log").exists()


def test_hook_passes_its_stage_to_terraform_and_records_it(tmp_path, monkeypatch):
    env_dir = tmp_path / "dev"
    gen = _generation(env_dir)
    monkeypatch.setattr(pg, "RUNNER", _stub_runner(tmp_path, {"resource_changes": []}))
    assert pg.run_hook("tighten", _environ(env_dir, gen["fingerprint"])) == 0
    assert "-var=policy_stage=tighten" in (tmp_path / "runner.log").read_text().splitlines()[0]
    record = pg.load_stage_record(env_dir)
    assert record["stage"] == "tighten" and record["desired"] == gen["fingerprint"]


def test_plan_check_allows_only_the_stage_own_deletes_and_no_grant_before_the_switch():
    plan = {"resource_changes": [
        _change("databricks_policy_info", "gone", "delete"),
        _change("databricks_entity_tag_assignment", "columns|c.s.t.x|gr_treatment", "delete"),
        dict(_change("databricks_grant", "c.s.t|g", "create"), name="table_access"),
        dict(_change("databricks_grant", "cat", "update"), name="terraform_sp_manage_catalog"),
    ]}
    assert len(pg.plan_violations(plan, "create")) == 3
    switch = pg.plan_violations(plan, "switch", {"gone"}, {"columns|c.s.t.x|gr_treatment"})
    assert switch == []
    assert len(pg.plan_violations(plan, "restore", {"gone"}, {"columns|c.s.t.x|gr_treatment"})) == 1


def test_abandon_removes_only_the_replacement_and_keeps_old_tags(tmp_path):
    old = _generation(tmp_path)
    failed = _generation(tmp_path, versions={"ip_address": {"partial": "redacted"}})
    planned = pg.plan_stage("abandon", failed, old, None)
    assert planned["rendered"]["tags"] == old["tags"]
    assert {p["name"] for p in planned["rendered"]["fgac_policies"]} == {p["name"] for p in old["fgac_policies"]}
    assert set(planned["allowed_policy_deletes"]) == {p["name"] for p in failed["fgac_policies"]}


def test_an_abandon_plan_may_only_delete_the_unfinished_changes_policies():
    tag = "columns|c.s.t.x|gr_treatment"
    allowed = pg.plan_violations({"resource_changes": [
        _change("databricks_policy_info", "gr_mask_new", "delete"),
        _change("databricks_policy_info", "gr_mask_kept", "no-op"),
        _change("databricks_entity_tag_assignment", tag, "no-op"),
        # Deleting a policy restarts the enforcement wait (its trigger hashes the policy map).
        _change("time_sleep", "", "delete", "create",
                address="module.data_access.time_sleep.wait_for_policy_enforcement"),
    ]}, "abandon", {"gr_mask_new"})
    assert allowed == []
    refused = [
        _change("databricks_entity_tag_assignment", tag, "update"),  # a column retagged since the restore
        _change("databricks_policy_info", "gr_mask_kept", "update"),  # TO / EXCEPT / match changed
        _change("databricks_policy_info", "gr_mask_kept", "delete"),  # not the change's policy
        _change("databricks_policy_info", "gr_mask_new", "delete", "create"),
        _change("databricks_policy_info", "gr_mask_other", "create"),
        _change("databricks_entity_tag_assignment", tag, "delete"),
        _change("terraform_data", "", "update", address="module.data_access.terraform_data.masking_functions"),
        _change("databricks_grant", "x", "delete", address='module.data_access.databricks_grant.table_access["x"]'),
        _change("time_sleep", "", "create", address="module.data_access.time_sleep.wait_for_tag_propagation"),
    ]
    for change in refused:
        assert pg.plan_violations({"resource_changes": [change]}, "abandon", {"gr_mask_new"}), change
    # Another stage keeps its own rules: a create plan may retag nothing it doesn't delete.
    assert pg.plan_violations({"resource_changes": [refused[0]]}, "create") == []


def test_an_abandon_plan_records_the_policies_it_deletes():
    plan = {"resource_changes": [_change("databricks_policy_info", "gr_mask_b", "delete"),
                                 _change("databricks_policy_info", "gr_mask_a", "delete"),
                                 _change("databricks_policy_info", "gr_mask_c", "no-op")]}
    assert pg.planned_policy_deletes(plan) == ["gr_mask_a", "gr_mask_b"]


def test_drop_functions_drops_only_functions_no_kept_generation_defines(tmp_path):
    env_dir = tmp_path / "dev"
    retired = _generation(env_dir, versions={"ip_address": {"partial": "redacted"}})
    pg.save_snapshot(env_dir, retired)
    gen = _generation(env_dir)
    pg.save_snapshot(env_dir, gen)
    calls = []

    def run(argv, check=False):
        sql = Path(argv[argv.index("--sql-file") + 1]).read_text()
        # The deployer refuses a drop with no identity; this one is the SQL's own hash.
        assert "--drop" in argv and argv[argv.index("--expect-sql-hash") + 1] == pg.masking_sql_hash(sql)
        calls.append(sql)
        return type("R", (), {"returncode": 0})()

    environ = _environ(env_dir, gen["fingerprint"]) | {"GENIERAILS_RETIRE_FINGERPRINT": retired["fingerprint"],
                                                       "GENIERAILS_WAREHOUSE_ID": "wh"}
    assert pg.drop_functions(env_dir, environ, live_users=lambda *a: set(), run=run) == 0
    dropped = {p["function_name"] for p in retired["fgac_policies"]} - {p["function_name"] for p in gen["fgac_policies"]}
    assert dropped and all(f in calls[0] for f in dropped)
    assert not any(p["function_name"] in calls[0] for p in gen["fgac_policies"])
    # A live policy still calling one stops the drop.
    calls.clear()
    assert pg.drop_functions(env_dir, environ, live_users=lambda *a: {next(iter(dropped))}, run=run) == 1
    assert calls == []


def test_grant_plan_may_change_access_only():
    plan = {"resource_changes": [
        dict(_change("databricks_grant", "c.s.t|g", "create"), name="table_access"),
        dict(_change("databricks_grant", "c.s.t|h", "delete"), name="table_access"),
        _change("databricks_policy_info", "p", "no-op"),
    ]}
    assert pg.plan_violations(plan, "grant") == []
    for rtype, key in (("databricks_policy_info", "p"), ("databricks_entity_tag_assignment", "columns|c.s.t.x|gr_treatment")):
        for action in ("update", "create", "delete"):
            assert pg.plan_violations({"resource_changes": [_change(rtype, key, action)]}, "grant"), (rtype, action)


def test_grant_hook_reapplies_the_last_render_with_new_access_allowed(tmp_path, monkeypatch):
    env_dir = tmp_path / "dev"
    gen = _generation(env_dir)
    monkeypatch.setattr(pg, "RUNNER", _stub_runner(tmp_path, {"resource_changes": []}))
    with pytest.raises(pg.StageError, match="last applied stage"):
        pg.run_hook("grant", _environ(env_dir, gen["fingerprint"]))
    assert pg.run_hook("switch", _environ(env_dir, gen["fingerprint"])) == 0
    assert pg.run_hook("grant", _environ(env_dir, gen["fingerprint"])) == 0
    plans = [line for line in (tmp_path / "runner.log").read_text().splitlines() if " plan " in f" {line} "]
    assert "-var=policy_stage=grant" in plans[-1]
