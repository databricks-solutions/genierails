from __future__ import annotations

import dataclasses
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
from types import SimpleNamespace
import types

import pytest

from scripts.e2e import driver


ROOT = Path(__file__).resolve().parents[2]


def test_dry_run_is_offline_and_writes_reports(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("dry-run must not invoke subprocess or snapshots")
    monkeypatch.setattr(subprocess, "run", forbidden)
    rc = driver.main(["--cloud", "aws", "--repo", str(ROOT), "--dry-run",
                      "--scenarios", "champion,s1", "--reports", str(tmp_path)])
    assert rc == 0
    champion = json.loads((tmp_path / "champion.json").read_text())
    assert champion["status"] == "dry-run"
    assert any(item["command"] == "make setup ENV=dev" for item in champion["steps"])
    assert not any("terraform" in item["command"].split()[:1] for item in champion["steps"])
    assert (tmp_path / "s1.md").is_file()
    s1 = json.loads((tmp_path / "s1.json").read_text())
    assert s1["human_gates"] and "agent_a" in s1["missing_values"]
    assert s1["target_source"].endswith("aws/Makefile")


def test_champion_needs_agent_a_its_dev_id_and_acl_groups(tmp_path):
    def dry_run(*values):
        rc = driver.main(["--cloud", "aws", "--repo", str(ROOT), "--dry-run", "--scenarios", "champion",
                          "--reports", str(tmp_path), *values])
        assert rc == 0
        return json.loads((tmp_path / "champion.json").read_text())

    champion = dry_run()
    assert champion["steps"][1] == {**champion["steps"][1], "action": "write_agent_file", "status": "planned"}
    assert {"agent_a", "dev_agent_id", "acl_groups"} <= set(champion["missing_values"])
    assert "missing value: acl_groups" in champion["would_not_run"]
    bad = dry_run("--value", "agent_a=Finance Analytics", "--value", "dev_agent_id=<id>",
                  "--value", "acl_groups=<group>", "--value", "prod_tables=cat.s.t", "--value", "dev_tables=cat.s.t")
    assert bad["missing_values"] == sorted([
        "agent_a (must be the agent's Terraform key: lowercase letters, digits and '_')",
        "dev_agent_id (must be a Genie agent ID: letters, digits, '_' or '-')",
        "acl_groups (must be comma-separated group names)"])
    good = dry_run(*CHAMPION_VALUES)
    assert good["missing_values"] == []


def test_write_agent_file_writes_once_and_never_retargets(tmp_path):
    values = {"agent_a": "finance_analytics", "dev_agent_id": "01ef7b3c", "acl_groups": "payments_ops, viewers"}
    driver.write_agent_file(tmp_path, values)
    path = tmp_path / "agents" / "finance_analytics.auto.tfvars"
    assert driver._load_hcl(path) == {"genie_space_id": "01ef7b3c", "acl_groups": ["payments_ops", "viewers"]}
    driver.write_agent_file(tmp_path, values)  # the same settings: unchanged
    before = path.read_text()
    for changed in ({"dev_agent_id": "01other"}, {"acl_groups": "viewers"}):
        with pytest.raises(RuntimeError, match="already sets a different genie_space_id or acl_groups"):
            driver.write_agent_file(tmp_path, {**values, **changed})
    assert path.read_text() == before
    with pytest.raises(RuntimeError, match="missing value: acl_groups"):
        driver.write_agent_file(tmp_path / "new", {**values, "acl_groups": ""})
    assert not (tmp_path / "new").exists()


def test_champion_prod_can_run_is_exactly_its_acl_groups():
    champion = next(s for s in driver.SCENARIOS if s.key == "champion")
    values = {"acl_groups": "a1,a2", "agent_a": "a", "prod_tables": "cat.s.t", "dev_tables": "cat.s.t"}
    prod = next(driver.render_check(c, values) for c in champion.checks
                if isinstance(c, driver.GrantDelta) and c.env == "prod")

    def released(*can_run, readers=("a1", "a2")):
        acl = {"access_control_list": [
            {"group_name": g, "all_permissions": [{"permission_level": "CAN_RUN", "inherited": False}]}
            for g in can_run]}
        return {"prod": with_agents(agent("a", "space-a", acl=acl), grants=readers)}

    before = {"prod": with_agents(agent("a", "space-a"))}
    assert driver._eval_grant_delta(prod, ctx(before, released("a1", "a2"))) == []
    extra = driver._eval_grant_delta(prod, ctx(before, released("a1", "a2", "eve")))
    assert any("unexpected grant(s) added: eve CAN RUN genie:space-a" in p for p in extra), extra
    missing = driver._eval_grant_delta(prod, ctx(before, released("a1")))
    assert any("expected grant(s) not newly added: a2 CAN RUN genie:space-a" in p for p in missing), missing


def test_snapshot_diff_is_stable_and_order_insensitive():
    before = {"grants": [{"p": "b"}, {"p": "a"}], "tags": ["x"], "serial": 4}
    after = {"grants": [{"p": "a"}, {"p": "b"}], "tags": ["y"], "serial": 5}
    assert driver.snapshot_diff(before, after) == {
        "serial": {"before": 4, "after": 5},
        "tags": {"before": ["x"], "after": ["y"]},
    }


def test_missing_target_has_step_marker():
    s1 = next(s for s in driver.SCENARIOS if s.key == "s1")
    fake = dataclasses.replace(s1.steps[1], command=("make", "future-target", "ENV=dev"),
                               target="future-target")
    plan = driver.scenario_plan(dataclasses.replace(s1, steps=(s1.steps[0], fake, *s1.steps[2:])), REAL_TARGETS)
    item = next(item for item in plan if item["command"].startswith("make future-target"))
    assert item["status"] == "skip"
    assert item["skip_reason"] == "target not implemented"


def test_every_executable_step_uses_public_make_target():
    for scenario in driver.SCENARIOS:
        for step in scenario.steps:
            if step.command:
                assert step.command[0] == "make"
                assert "terraform" not in step.command


def test_secret_fields_are_redacted():
    assert driver.redact({"client_secret": "nope", "nested": {"token": "bad", "ok": 1}}) == {
        "client_secret": "<redacted>", "nested": {"token": "<redacted>", "ok": 1}
    }


def test_report_redacts_secrets(tmp_path):
    scenario = driver.SCENARIOS[0]
    driver.write_report(tmp_path, scenario, {
        "status": "passed", "steps": [], "snapshot_diff": {}, "client_token": "never-write-me",
        "error": "failed with plain-secret-value inside",
    }, secrets={"plain-secret-value"})
    text = (tmp_path / "champion.json").read_text() + (tmp_path / "champion.md").read_text()
    assert "never-write-me" not in text and "plain-secret-value" not in text


def test_scenario_catalog_contains_plan_matrix_and_governance_only():
    keys = {s.key for s in driver.SCENARIOS}
    assert {f"s{i}" for i in range(1, 17)} <= keys
    assert {"champion", "manifest", "migration", "migration-cutover"} <= keys
    assert next(s for s in driver.SCENARIOS if s.key == "s14").title == "Governance-only environment"


def test_preconditions_are_not_split_into_characters():
    assert all(all(len(item) > 1 for item in scenario.preconditions) for scenario in driver.SCENARIOS)


# --- live loop with fake git/make/snapshots -------------------------------------------

COMMIT = "a" * 40
OTHER = "b" * 40
ALL_TARGETS = {step.target for scenario in driver.SCENARIOS for step in scenario.steps if step.target}
REAL_TARGETS = set(re.findall(r"^([a-zA-Z0-9][\w.-]*):(?:\s|$)",
                              (ROOT / "shared" / "Makefile.shared").read_text(), re.M))


def snapshot(masks=("mask_ssn",), tags=(("t", "ssn", "class.ssn"),), grants=(), policy_except=()):
    return {"uc": {"cat": {
        "governed_tags": [{"catalog_name": "cat", "schema_name": "s", "table_name": t, "column_name": c,
                           "tag_name": "pii", "tag_value": v} for t, c, v in tags],
        "mask_functions": [{"routine_catalog": "cat", "routine_schema": "s", "routine_name": m,
                            "definition_hash": f"hash:{m}"} for m in masks],
        "abac_policies": [{"table": "cat.s.t", "policies": [{"name": "mask_ssn_p", "to_principals": ["account users"],
                           "except_principals": list(policy_except), "column_mask": {"function_name": "cat.s.mask_ssn"}}]}],
        "table_grants": [{"grantee": g, "privilege_type": "SELECT", "table_catalog": "cat", "table_schema": "s",
                          "table_name": "t"} for g in grants],
    }}, "agents": []}


class FakeSnapshots:
    mode = "steady"

    def __init__(self, *_):
        self.n = 0

    def capture(self, env):
        self.n += 1
        if self.mode == "boom" and self.n == 3:
            raise RuntimeError("snapshot exploded")
        if self.mode == "weaken" and env == "prod" and self.n > 2:
            return snapshot(masks=())
        if self.mode == "widen_after" and env == "prod" and self.n > 2:
            return snapshot(grants=("stranger",))
        return snapshot()


CHAMPION_VALUES = ("--value", "agent_a=a", "--value", "acl_groups=analysts", "--value", "dev_agent_id=space-a",
                   "--value", "prod_tables=cat.s.t", "--value", "dev_tables=cat.s.t")
# The columns VERIFY_PASS proves, as the scenario values that name them.
CHECKED_COLUMNS = ("--value", "new_column=cat.s.t.ssn", "--value", "secret_column=cat.s.t.cvv")


def verify_entry(column, principals, *, status="PASS", full_function=None):
    """One per-check entry of the verify-access result file (verify_effective_access.result_checks)."""
    return {"kind": "column-mask", "table": "cat.s.t", "column": column, "policy": "p", "status": status,
            "mask_function": "", "partial_function": "",
            "full_function": full_function or "cat.s.gr_mask_email_partial__string__11111111_full",
            "principals": {p: {"expected": e, "observed": o} for p, (e, o) in principals.items()}}


VERIFY_PASS = {"passed": True, "checks": [
    verify_entry("ssn", {"analysts": ("partial", "partial"), "__admin__": ("raw", "raw")}),
    verify_entry("cvv", {"analysts": ("full", "full"), "__admin__": ("full", "full")},
                 full_function="cat.s.gr_mask_unmapped_redact__string__22222222_full"),
]}
VERIFYING = ("rehearse", "release", "verify-access")


def write_verify_result(cloud_root, argv, payload):
    env = next((a.split("=", 1)[1] for a in argv if a.startswith("ENV=")), "dev")
    path = Path(cloud_root) / "envs" / env / "generated" / ".verify_access.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def live_args(tmp_path, scenario, *extra):
    checkout = tmp_path / "checkout"
    (checkout / "aws").mkdir(parents=True, exist_ok=True)
    return ["--cloud", "aws", "--repo", str(checkout), "--checkout", str(checkout),
            "--commit", COMMIT, "--state-dir", str(tmp_path / "state"),
            "--reports", str(tmp_path / "reports"), "--scenarios", scenario,
            "--confirm-live", *CHECKED_COLUMNS, *extra]


def fake_subprocess(calls, *, unrelated=False, no_manifest=False, secrets=False, refuse=None,
                    drift_after_make=False, on_make=None, staged_release=False,
                    staged_releases=None, migrate_rc=0, import_unset=False, verify_result=VERIFY_PASS):
    state = {"makes": 0, "releases": 0, "plans": 0}

    def run(argv, **kwargs):
        argv = list(argv); calls.append((argv, kwargs.get("env", {})))
        if argv[1:2] == ["--no-optional-locks"]:
            argv = argv[:1] + argv[2:]
        if argv[:2] == ["git", "ls-tree"]:
            paths = argv[argv.index("--") + 1:]
            return subprocess.CompletedProcess(argv, 0, "".join(
                f"{'100644 blob' if p.endswith('.tfvars') else '040000 tree'} {'0' * 40}\t{p}\0" for p in paths), "")
        if argv[:2] == ["git", "rev-parse"]:
            head = OTHER if drift_after_make and state["makes"] and argv[2:] == ["HEAD"] else COMMIT
            return subprocess.CompletedProcess(argv, 0, head + "\n", "")
        if argv[:3] == ["git", "remote", "get-url"]:
            return subprocess.CompletedProcess(argv, 2, "error: No such remote\n", "")
        # A customer-shaped checkout: the managed block is committed, envs/ is not ignored.
        if argv[:2] == ["git", "show"] and argv[2].endswith("/.gitignore"):
            return subprocess.CompletedProcess(argv, 0, driver.CUSTOMER_GITIGNORE_MARKER + ") >>>\n", "")
        if argv[:2] == ["git", "check-ignore"]:
            return subprocess.CompletedProcess(argv, 1, "", "")
        if argv and argv[0] == "git":
            return subprocess.CompletedProcess(argv, 0, "", "")
        text, rc = "ok\n", 0
        if argv and argv[0] == "make":
            state["makes"] += 1
            if on_make: on_make(argv)
            if "release-plan" in argv:
                state["plans"] += 1
                text = "ok\n" if no_manifest else f"  Manifest (immutable): /tmp/approved-{state['plans']}.json\n"
            if "migrate-plan" in argv:
                text = ("=== Migration dry run\n"
                        "  cat.s.t.ssn: masked_ssn -> email_partial__string__11111111\n"
                        "    mask today: mask_masked_ssn: legacy sql\n"
                        "Result: ready\n")
                rc = migrate_rc
            if argv[1:2] == ["release"]:
                state["releases"] += 1
                phase_count = 1 if staged_release else staged_releases
                if phase_count is not None and state["releases"] <= phase_count:
                    text = "=== Release phase applied; need another release-plan\n"
            if import_unset:
                text += "  Cleared tag gr_treatment on cat.s.t.ssn\n"
            if "ungovern" in argv and unrelated:
                rc, text = 2, "unrelated syntax error\n"
            if refuse and refuse[0](argv):
                rc, text = 2, refuse[1]
            if rc == 0 and argv[1] in VERIFYING and verify_result is not None:
                write_verify_result(kwargs["cwd"], argv, verify_result)
            if secrets:
                text += ('payload {"token": "dapiSECRET123"}\nAuthorization: Bearer dapiSECRET456\n'
                         "client secret is file-secret-value\n")
        return subprocess.CompletedProcess(argv, rc, text, "")
    return run


def makes(calls):
    return [argv for argv, _ in calls if argv and argv[0] == "make"]


@pytest.fixture
def live(monkeypatch):
    calls: list = []

    def setup(targets=ALL_TARGETS, **fake):
        monkeypatch.setattr(driver, "make_targets", lambda *_: set(targets))
        monkeypatch.setattr(driver.subprocess, "run", fake_subprocess(calls, **fake))
        return calls
    return setup


# Blocker 1: a scenario needing a genuinely unbuilt target runs nothing at all.
def test_missing_fake_target_scenario_is_not_run_and_issues_no_make(tmp_path, live, monkeypatch):
    s1 = next(s for s in driver.SCENARIOS if s.key == "s1")
    fake = dataclasses.replace(s1.steps[1], command=("make", "future-target", "ENV=dev"),
                               target="future-target")
    monkeypatch.setattr(driver, "SCENARIOS", (dataclasses.replace(s1, steps=(s1.steps[0], fake, *s1.steps[2:])),))
    calls = live(REAL_TARGETS)
    assert driver.main(live_args(tmp_path, "s1"), snapshot_factory=FakeSnapshots) == 0
    report = json.loads((tmp_path / "reports" / "s1.json").read_text())
    assert report["status"] == "NOT RUN"
    assert all(item["status"] in ("NOT RUN", "skip") for item in report["steps"])
    assert not makes(calls)


def test_step_10_11_scenarios_are_runnable_in_this_repo_and_dry_run_is_offline(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("dry-run must not invoke subprocess or snapshots")
    monkeypatch.setattr(subprocess, "run", forbidden)
    keys = "s1,s3,s7,s13,migration"
    assert driver.main(["--cloud", "aws", "--repo", str(ROOT), "--dry-run",
                        "--scenarios", keys, "--reports", str(tmp_path)]) == 0
    for key in keys.split(","):
        report = json.loads((tmp_path / f"{key}.json").read_text())
        assert report["status"] == "dry-run"
        assert not [x for x in report["would_not_run"] if x == "target not implemented"]


# Blocker 2: refusals, placeholders, CI and unrelated failures.
def test_unresolved_placeholder_is_error_without_make(tmp_path, live):
    calls = live()
    assert driver.main(live_args(tmp_path, "manifest"), snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports" / "manifest.json").read_text())
    assert report["status"] == "ERROR"
    assert "unresolved placeholder" in report["error"]
    assert not makes(calls)


def test_late_placeholder_is_caught_before_any_step_runs(tmp_path, live):
    calls = live()
    assert driver.main(live_args(tmp_path, "s5"), snapshot_factory=FakeSnapshots) == 1
    assert "treatment" in json.loads((tmp_path / "reports" / "s5.json").read_text())["error"]
    assert not makes(calls)


@pytest.mark.parametrize("arg", ["MANIFEST=", "MANIFEST=<from make release-plan>", "TABLE={table}"])
def test_placeholder_or_empty_argument_never_reaches_make(arg):
    with pytest.raises(ValueError, match="unresolved placeholder"):
        driver.check_argument(arg)
    with pytest.raises(ValueError):
        driver.render(arg, {"table": ""})


def test_unrelated_nonzero_is_not_a_passing_refusal(tmp_path, live):
    live(unrelated=True)
    assert driver.main(live_args(tmp_path, "s8", "--value", "table=c.s.t", "--value", "uncovered_table=c.s.u"),
                       snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports" / "s8.json").read_text())
    assert report["status"] == "ERROR"
    assert report["steps"][0]["status"] == "ERROR"


def test_refusal_message_with_changed_snapshot_is_an_error(tmp_path, live):
    class Mutates(FakeSnapshots):
        def capture(self, env):
            self.n += 1
            return snapshot(grants=("x",) if self.n > 4 else ())
    live(refuse=(lambda argv: "release-plan" in argv,
                 "release-plan: refused by the reader or weakening check (above)\n"))
    assert driver.main(live_args(tmp_path, "s6", "--value", "ack_entry=prod:reader:c.s.t:g"),
                       snapshot_factory=Mutates) == 1
    report = json.loads((tmp_path / "reports" / "s6.json").read_text())
    assert report["steps"][0]["assertion"] == "refusal changed the snapshot"


def test_s13_sets_ci_unsets_override_and_passes_on_exact_refusals(tmp_path, live):
    calls = live(refuse=(lambda argv: True, "ERROR: applying targets cannot run in CI. ...\n"))
    assert driver.main(live_args(tmp_path, "s13"), snapshot_factory=FakeSnapshots) == 0
    envs = [env for argv, env in calls if argv and argv[0] == "make"]
    assert len(envs) == 5 and all(env.get("CI") == "true" for env in envs)
    assert all("GENIERAILS_ALLOW_CI_APPLY" not in env for env in envs)
    assert all("<" not in " ".join(argv) for argv in makes(calls))
    report = json.loads((tmp_path / "reports" / "s13.json").read_text())
    assert report["status"] == "passed" and report["restored_lifecycle"] is False


def test_s10_refusals_get_default_ci_environment(tmp_path, live):
    calls = live(refuse=(lambda argv: True, "committed files differ from what the commit regenerates\n"))
    assert driver.main(live_args(tmp_path, "s10"), snapshot_factory=FakeSnapshots) == 0
    envs = [env for argv, env in calls if argv and argv[0] == "make"]
    assert len(envs) == 2 and all(env.get("CI") == "true" for env in envs)
    assert all("GENIERAILS_ALLOW_CI_APPLY" not in env for env in envs)


@pytest.mark.parametrize(("scenario", "values", "message", "call_count"), [
    ("s11", ("u=" + "b" * 40, "o=" + "c" * 40, "manifest=/tmp/foreign.json"),
     "release-plan refused: make release refused\n", 2),
    ("manifest", ("tampered_manifest=/tmp/tampered.json",), "make release refused: x no longer matches\n", 1),
])
def test_non_ci_refusals_clear_every_ci_marker(tmp_path, live, monkeypatch,
                                               scenario, values, message, call_count):
    markers = (*driver.CI_MARKERS, "GENIERAILS_ALLOW_CI_APPLY")
    for marker in markers:
        monkeypatch.setenv(marker, "inherited")
    calls = live(refuse=(lambda argv: True, message))
    value_args = tuple(part for value in values for part in ("--value", value))
    args = live_args(tmp_path, scenario, *value_args)
    assert driver.main(args, snapshot_factory=FakeSnapshots) == 0
    envs = [env for argv, env in calls if argv and argv[0] == "make"]
    assert len(envs) == call_count
    assert all(all(marker not in env for marker in markers) for env in envs)


def test_read_only_scenarios_match_structure():
    actual = {scenario.key for scenario in driver.SCENARIOS if scenario.read_only}
    assert actual == {"s10", "s11", "s13", "manifest", "migration"}
    for scenario in driver.SCENARIOS:
        applying_is_refusal_only = all(
            step.expect_failure for step in scenario.steps if step.target in driver.APPLYING)
        has_successful_authoring_or_merge = any(
            (step.target in driver.AUTHORING and not step.expect_failure) or step.action == "local_merge"
            for step in scenario.steps)
        assert scenario.read_only == (applying_is_refusal_only and not has_successful_authoring_or_merge), scenario.key


@pytest.mark.parametrize(("scenario", "values", "first_target"), [
    ("s13", (), "release-plan"),
    ("s11", ("u=" + "b" * 40, "o=" + "c" * 40, "manifest=/tmp/foreign.json"), "release-plan"),
    ("manifest", ("tampered_manifest=/tmp/tampered.json",), "release"),
])
def test_missed_read_only_refusal_never_runs_applying_restore(tmp_path, live, scenario, values, first_target):
    calls = live()  # The first refusal unexpectedly exits zero.
    value_args = tuple(part for value in values for part in ("--value", value))
    assert driver.main(live_args(tmp_path, scenario, *value_args), snapshot_factory=FakeSnapshots) == 1
    assert [argv[1] for argv in makes(calls)] == [first_target]
    assert json.loads((tmp_path / f"reports/{scenario}.json").read_text())["restored_lifecycle"] is False


def test_read_only_drift_reports_manual_restore_and_stops(tmp_path, live):
    class Drift(FakeSnapshots):
        def capture(self, env):
            self.n += 1
            return snapshot(grants=("unexpected",) if self.n > 4 else ())
    calls = live()
    assert driver.main(live_args(tmp_path, "s13,migration"), snapshot_factory=Drift) == 1
    report = json.loads((tmp_path / "reports/s13.json").read_text())
    assert report["status"] == "RESTORE NEEDED"
    assert report["manual_restore_commands"] == [
        "make rehearse ENV=dev",
        "make release-plan ENV=prod",
        "make release ENV=prod MANIFEST=<path-from-release-plan>",
        "make maintain ENV=prod",
    ]
    assert report["snapshot_diff"]
    markdown = (tmp_path / "reports/s13.md").read_text()
    assert "Manual restore required" in markdown and '"unexpected"' in markdown
    assert [argv[1] for argv in makes(calls)] == ["release-plan"]
    assert not (tmp_path / "reports/migration.json").exists()


def test_read_only_post_reset_drift_reports_restore_needed(tmp_path, live):
    class DriftAfterReset(FakeSnapshots):
        def capture(self, env):
            self.n += 1
            return snapshot(grants=("post-reset",) if self.n > 4 else ())
    live()
    assert driver.main(live_args(tmp_path, "migration"), snapshot_factory=DriftAfterReset) == 1
    report = json.loads((tmp_path / "reports/migration.json").read_text())
    assert report["status"] == "RESTORE NEEDED"
    assert report["manual_restore_commands"]
    assert report["snapshot_diff"] == {} and report["post_reset_snapshot_diff"]


def test_missing_release_plan_manifest_errors_before_release(tmp_path, live, monkeypatch):
    calls = live(no_manifest=True)
    monkeypatch.setattr(driver, "governance_only_env", lambda *_: True)
    assert driver.main(live_args(tmp_path, "s14"), snapshot_factory=FakeSnapshots) == 1
    scenario_makes = makes(calls)
    assert any("release-plan" in argv for argv in scenario_makes)
    assert not any("release" in argv for argv in scenario_makes)


# Blocker 3: snapshots read this repo's layout and pin the env's host.
def _fake_sdk(monkeypatch, seen, *, resolved_host=None, tables=(("prodcat", "s", "t"),)):
    class Statement:
        def execute_statement(self, *, statement, **kwargs):
            seen.setdefault("statements", []).append(statement)
            rows = [list(t) for t in tables] if "information_schema.tables" in statement else []
            cols = ["table_catalog", "table_schema", "table_name"]
            return SimpleNamespace(status=SimpleNamespace(state="SUCCEEDED"), statement_id="s",
                manifest=SimpleNamespace(schema=SimpleNamespace(columns=[SimpleNamespace(name=c) for c in cols])),
                result=SimpleNamespace(data_array=rows, next_chunk_index=None))
    class Client:
        def __init__(self, **kwargs):
            seen.update(kwargs); seen["ambient"] = os.environ.get("DATABRICKS_HOST")
            self.config = SimpleNamespace(host=resolved_host or kwargs["host"])
            self.statement_execution = Statement()
            self.policies = SimpleNamespace(list_policies=lambda *a, **k: [])
            self.genie = SimpleNamespace(); self.permissions = SimpleNamespace()
    package = types.ModuleType("databricks"); sdk = types.ModuleType("databricks.sdk"); sdk.WorkspaceClient = Client
    monkeypatch.setitem(sys.modules, "databricks", package); monkeypatch.setitem(sys.modules, "databricks.sdk", sdk)


def _env_files(tmp_path):
    cloud = tmp_path / "aws"; env = cloud / "envs" / "prod"; env.mkdir(parents=True)
    (env / "auth.auto.tfvars").write_text('databricks_workspace_host = "https://pinned.example"\n'
        'databricks_client_id = "id"\ndatabricks_client_secret = "actual-secret"\n')
    (env / "env.auto.tfvars").write_text('sql_warehouse_id = "wh"\nuc_tables = ["prodcat.s.t"]\n'
        'catalog_map = { devcat = "prodcat" }\ngenie_spaces = []\n')
    return cloud


def test_live_snapshot_uses_env_auth_file_and_never_the_shell(tmp_path, monkeypatch):
    pytest.importorskip("hcl2")
    cloud = _env_files(tmp_path); seen = {}
    monkeypatch.setenv("DATABRICKS_HOST", "https://shell-default.example")
    _fake_sdk(monkeypatch, seen)
    provider = driver.LiveSnapshotProvider(cloud, tmp_path / "state")
    snap = provider.capture("prod")
    assert seen["host"] == "https://pinned.example" and seen["auth_type"] == "oauth-m2m"
    assert seen["ambient"] is None and os.environ["DATABRICKS_HOST"] == "https://shell-default.example"
    assert "profile" not in seen
    assert "actual-secret" in provider.secret_values
    assert snap["uc"]["prodcat"]["abac_policies"] == [{"table": "prodcat.s.t", "policies": []}]


def test_live_snapshot_refuses_a_client_resolved_to_another_host(tmp_path, monkeypatch):
    cloud = _env_files(tmp_path)
    _fake_sdk(monkeypatch, {}, resolved_host="https://elsewhere.example")
    with pytest.raises(RuntimeError, match="not the env's"):
        driver.LiveSnapshotProvider(cloud, tmp_path / "state").client("prod")


def test_live_snapshot_read_failures_are_errors_not_data(tmp_path, monkeypatch):
    pytest.importorskip("hcl2")
    cloud = _env_files(tmp_path)
    _fake_sdk(monkeypatch, {}, tables=())
    with pytest.raises(RuntimeError, match="no visible tables"):
        driver.LiveSnapshotProvider(cloud, tmp_path / "state").capture("prod")
    response = SimpleNamespace(status=SimpleNamespace(state="FAILED"), result=None, manifest=None)
    with pytest.raises(RuntimeError, match="did not succeed"):
        driver._require_succeeded(response, "tags")
    response.status.error = SimpleNamespace(error_code="BAD_REQUEST", message="bad SQL containing actual-secret")
    with pytest.raises(RuntimeError, match=r"code=BAD_REQUEST.*bad SQL containing <redacted>"):
        driver._require_succeeded(response, "tags", secrets={"actual-secret"})
    state = tmp_path / "state"; state.mkdir(); (state / "x.tfstate").write_text("{not json")
    with pytest.raises(RuntimeError, match="Terraform serial"):
        driver._state_serials(state)
    with pytest.raises(RuntimeError, match="missing authentication file"):
        driver.LiveSnapshotProvider(cloud, state).client("dev")


def test_policy_function_snapshot_filters_names_in_python(monkeypatch, tmp_path):
    pytest.importorskip("hcl2")
    cloud = _env_files(tmp_path); seen = {}
    _fake_sdk(monkeypatch, seen)
    driver.LiveSnapshotProvider(cloud, tmp_path / "state").capture("prod")
    mask_sql = next(sql for sql in seen["statements"] if "information_schema.routines" in sql)
    assert "routine_definition" in mask_sql
    assert "startswith" not in mask_sql and " LIKE " not in mask_sql and " ESCAPE " not in mask_sql


def test_agent_ids_include_hcl_state_and_id_file(tmp_path):
    env = tmp_path / "env"; env.mkdir(); state = tmp_path / "state"; state.mkdir()
    (env / ".genie_space_id").write_text("file-id\n")
    (state / "workspace.tfstate").write_text(json.dumps({"resources": [{"type": "databricks_genie_space",
        "name": "state_agent", "instances": [{"attributes": {"id": "state-id"}}]}]}))
    ids = driver._agent_ids(env, {"genie_spaces": [{"name": "HCL", "genie_space_id": "hcl-id"}]}, state, tmp_path)
    assert {value for _, value in ids} == {"file-id", "state-id", "hcl-id"}


# Blocker 4: restore after every scenario, on errors and signals; weaker = failure.
def test_exception_writes_report_and_attempts_public_restore(tmp_path, live):
    class Boom(FakeSnapshots):
        def capture(self, env):
            self.n += 1
            if self.n == 5:  # after the first make has started, so restore is mandatory
                raise RuntimeError("snapshot exploded")
            return snapshot()
    calls = live()
    assert driver.main(live_args(tmp_path, "s4"), snapshot_factory=Boom) == 1
    report = json.loads((tmp_path / "reports" / "s4.json").read_text())
    assert report["status"] in ("ERROR", "RESTORE FAILED")
    assert [a[1] for a in makes(calls)][-4:] == ["rehearse", "release-plan", "release", "maintain"]


def test_sigterm_mid_step_restores_writes_partial_report_and_stops(tmp_path, live):
    fired = []
    def on_make(argv):
        if "maintain" in argv and not fired:
            fired.append(1); os.kill(os.getpid(), signal.SIGTERM)
    calls = live(on_make=on_make)
    assert driver.main(live_args(tmp_path, "s4,s10"), snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports" / "s4.json").read_text())
    assert report["status"] == "interrupted" and "SIGTERM" in report["error"]
    assert report["restored_lifecycle"] is True
    assert [a[1] for a in makes(calls)] == ["maintain", "rehearse", "release-plan", "release", "maintain"]
    assert not (tmp_path / "reports" / "s10.json").exists()
    assert signal.getsignal(signal.SIGTERM) is not driver._raise_interrupt


def test_weaker_protection_fails_scenario_and_final_check(tmp_path, live):
    class Weaken(FakeSnapshots): mode = "weaken"
    live()
    assert driver.main(live_args(tmp_path, "s4"), snapshot_factory=Weaken) == 1
    assert json.loads((tmp_path / "reports" / "s4.json").read_text())["status"] == "RESTORE FAILED"
    final = json.loads((tmp_path / "reports" / "final-protection.json").read_text())
    assert final["status"] == "ERROR" and final["weaker_than_baseline"]["prod"]["masks"] == ["cat.s.mask_ssn"]


def test_protection_diff_is_per_column_and_principal():
    base = snapshot()
    assert driver.protection_diff(base, snapshot()) == {}
    assert driver.protection_diff(base, snapshot(tags=()))["columns"] == ["cat.s.t.ssn pii=class.ssn"]
    assert driver.protection_diff(base, snapshot(policy_except=("analysts",)))["policies"] == [
        "cat.s.t mask_ssn_p: newly exempts analysts"]
    assert driver.protection_diff(base, snapshot(grants=("eve",)), include_grants=True)["widened_grants"] == [
        "eve SELECT cat.s.t"]


def azure_legacy_mid_migration_snapshot(*, treatment="redact", policy_except=()):
    """Relevant shape captured from the legacy Azure environment in migration."""
    catalog = "louis_serverless"
    table = f"{catalog}.dev_to_prod_demo.payments"
    return {"uc": {catalog: {
        "governed_tags": [{
            "catalog_name": catalog,
            "schema_name": "dev_to_prod_demo",
            "table_name": "payments",
            "column_name": "cvv",
            "tag_name": "gr_treatment",
            "tag_value": treatment,
        }],
        "mask_functions": [{
            "routine_catalog": catalog,
            "routine_schema": "dev_to_prod_demo",
            "routine_name": "mask_redact",
            "definition_hash": "hash:mask_redact",
        }],
        "abac_policies": [{"table": table, "policies": [{
            "name": f"{catalog}_gr_mask_{catalog}_redact",
            "to_principals": ["account users"],
            "except_principals": list(policy_except),
            "match_columns": [{
                "alias": "gr_treatment_redact",
                "condition": f"hasTagValue('gr_treatment', '{treatment}')",
            }],
            "column_mask": {"function_name": f"{catalog}.dev_to_prod_demo.mask_redact"},
        }]}],
    }}, "agents": []}


def test_unchanged_azure_legacy_never_raw_snapshot_is_not_weaker():
    baseline = azure_legacy_mid_migration_snapshot()
    # Regression: main reported "never-raw redact -> redact" even though the
    # driver's before/after snapshots and the live state hashes were identical.
    assert driver.snapshot_diff(baseline, baseline) == {}
    assert driver.protection_diff(baseline, baseline) == {}


def test_azure_legacy_never_raw_policy_gaining_exception_is_weaker():
    baseline = azure_legacy_mid_migration_snapshot()
    current = azure_legacy_mid_migration_snapshot(policy_except=("payments_ops",))
    assert driver.protection_diff(baseline, current) == {"policies": [
        "louis_serverless.dev_to_prod_demo.payments "
        "louis_serverless_gr_mask_louis_serverless_redact: newly exempts payments_ops"
    ]}


def test_azure_legacy_policy_dropping_matched_value_is_weaker():
    baseline = azure_legacy_mid_migration_snapshot()
    current = azure_legacy_mid_migration_snapshot(treatment="email_partial")
    current["uc"]["louis_serverless"]["governed_tags"][0]["tag_value"] = "redact"
    assert driver.protection_diff(baseline, current) == {"policies": [
        "louis_serverless.dev_to_prod_demo.payments "
        "louis_serverless_gr_mask_louis_serverless_redact: no longer matches redact"
    ]}


def test_live_policy_mask_function_definition_change_is_weaker():
    baseline = azure_legacy_mid_migration_snapshot()
    current = azure_legacy_mid_migration_snapshot()
    current["uc"]["louis_serverless"]["mask_functions"][0]["definition_hash"] = "changed"
    assert driver.protection_diff(baseline, current) == {"masks": [
        "louis_serverless.dev_to_prod_demo.mask_redact: definition changed"
    ]}


@pytest.mark.parametrize("field", ["when_condition", "match_columns"])
def test_live_policy_condition_rewrite_is_weaker_unless_exactly_reviewed(field):
    baseline = azure_legacy_mid_migration_snapshot()
    current = azure_legacy_mid_migration_snapshot()
    policy_before = baseline["uc"]["louis_serverless"]["abac_policies"][0]["policies"][0]
    policy_after = current["uc"]["louis_serverless"]["abac_policies"][0]["policies"][0]
    policy_before[f"{field}_hash"] = "a" * 64
    policy_after[f"{field}_hash"] = "b" * 64
    weak = driver.protection_diff(baseline, current)
    assert weak == {"policies": [
        "louis_serverless.dev_to_prod_demo.payments "
        "louis_serverless_gr_mask_louis_serverless_redact: "
        f"louis_serverless_gr_mask_louis_serverless_redact {field} changed or became unreadable"
    ]}
    adoption = driver.Adoption("redact", "redact", "mask_redact", policy_conditions=((
        "louis_serverless_gr_mask_louis_serverless_redact", field, "b" * 64),))
    assert driver.protection_diff(
        baseline, current, allowed_adoptions={"louis_serverless.dev_to_prod_demo.payments.cvv": adoption}) == {}


def test_reviewed_mask_body_change_requires_function_and_new_hash():
    baseline = azure_legacy_mid_migration_snapshot()
    current = azure_legacy_mid_migration_snapshot()
    function = "louis_serverless.dev_to_prod_demo.mask_redact"
    current["uc"]["louis_serverless"]["mask_functions"][0]["definition_hash"] = "b" * 64
    assert driver.protection_diff(baseline, current)["masks"] == [f"{function}: definition changed"]
    adoption = driver.Adoption("redact", "redact", "mask_redact",
                               mask_definitions=((function, "b" * 64),))
    assert driver.protection_diff(
        baseline, current, allowed_adoptions={"louis_serverless.dev_to_prod_demo.payments.cvv": adoption}) == {}
    for declared_name, declared_hash in ((function, "c" * 64), ("mask_redact", "b" * 64),
                                          ("x.y.mask_redact", "b" * 64)):
        wrong = driver.Adoption("redact", "redact", "mask_redact",
                                mask_definitions=((declared_name, declared_hash),))
        assert driver.protection_diff(
            baseline, current,
            allowed_adoptions={"louis_serverless.dev_to_prod_demo.payments.cvv": wrong})["masks"] == [
                f"{function}: definition changed"]


def test_policy_condition_declaration_requires_exact_name_field_and_hash():
    name = "louis_serverless_gr_mask_louis_serverless_redact"
    location = "louis_serverless.dev_to_prod_demo.payments.cvv"
    baseline = azure_legacy_mid_migration_snapshot()
    current = azure_legacy_mid_migration_snapshot()
    before = baseline["uc"]["louis_serverless"]["abac_policies"][0]["policies"][0]
    after = current["uc"]["louis_serverless"]["abac_policies"][0]["policies"][0]
    before["when_condition_hash"], after["when_condition_hash"] = "a" * 64, "b" * 64
    for declared_name, field, digest in ((name, "when_condition", "c" * 64),
                                          ("other", "when_condition", "b" * 64),
                                          (name, "match_columns", "b" * 64)):
        adoption = driver.Adoption("redact", "redact", "mask_redact",
                                   policy_conditions=((declared_name, field, digest),))
        assert driver.protection_diff(baseline, current, allowed_adoptions={location: adoption})["policies"]


def test_saved_prod_policy_gaining_false_when_condition_is_weaker():
    baseline = azure_legacy_mid_migration_snapshot()
    current = azure_legacy_mid_migration_snapshot()
    for snapshot_value in (baseline, current):
        entry = snapshot_value["uc"]["louis_serverless"]["abac_policies"][0]
        raw = dict(entry["policies"][0])
        raw.setdefault("when_condition", None)
        raw.setdefault("match_columns", None)
        entry["policies"] = [driver._snapshot_policy(raw, entry["table"])]
    entry = current["uc"]["louis_serverless"]["abac_policies"][0]
    raw = azure_legacy_mid_migration_snapshot()["uc"]["louis_serverless"]["abac_policies"][0]["policies"][0]
    raw.update({"when_condition": "false", "match_columns": raw.get("match_columns")})
    entry["policies"] = [driver._snapshot_policy(raw, entry["table"])]
    assert "when_condition changed" in driver.protection_diff(baseline, current)["policies"][0]


def test_migration_report_parses_exact_reviewed_live_changes():
    digest = "b" * 64
    mapping = driver.parse_migration_mappings(
        "  cat.s.t.ssn: masked_ssn -> email_partial__string__11111111\n"
        "    mask today: mask_ssn: legacy sql\n"
        f"    reviewed policy condition: policy_name when_condition {digest}\n"
        f"    reviewed mask definition: cat.s.mask_ssn {digest}\n")
    adoption = mapping["cat.s.t.ssn"]
    assert adoption.policy_conditions == (("policy_name", "when_condition", digest),)
    assert adoption.mask_definitions == (("cat.s.mask_ssn", digest),)


def test_reviewed_lines_only_bind_to_their_contiguous_mapping_block():
    digest = "b" * 64
    mapping = driver.parse_migration_mappings(
        "  cat.s.t.ssn: masked_ssn -> email_partial__string__11111111\n"
        "    mask today: mask_ssn: legacy sql\n"
        "    account users: redacted -> redacted\n"
        f"    reviewed mask definition: cat.s.mask_ssn {digest}\n")
    assert mapping["cat.s.t.ssn"].mask_definitions == ()


@pytest.mark.parametrize("field,value", [
    ("when_condition", ""), ("match_columns", [{"alias": "x"}]),
])
def test_policy_snapshot_fails_closed_when_condition_is_unreadable(field, value):
    policy = {"name": "governed", "column_mask": {"function_name": "cat.s.mask_ssn"}, field: value}
    with pytest.raises(RuntimeError, match="policy condition is unreadable"):
        driver._snapshot_policy(policy, "cat.s.t")


def test_policy_snapshot_hashes_conditions_without_persisting_sql():
    policy = driver._snapshot_policy({
        "name": "governed", "when_condition": "true",
        "match_columns": [{"alias": "sensitive", "condition": "hasTagValue('pii', 'ssn')"}],
        "column_mask": {"function_name": "cat.s.mask_ssn"},
    }, "cat.s.t")
    assert all(policy[f"{field}_hash"] for field in driver.CONDITION_FIELDS)
    assert policy["match_values"] == []
    assert "hasTagValue" not in json.dumps(policy)


def test_policy_snapshot_records_absent_conditions_explicitly():
    policy = driver._snapshot_policy({"name": "governed"}, "cat.s.t")
    assert {policy[f"{field}_hash"] for field in driver.CONDITION_FIELDS} == {driver.ABSENT_CONDITION}


def test_generated_policy_condition_declaration_uses_real_policy_name():
    def snap(condition):
        policy = driver._snapshot_policy({
            "name": "gr_mask_real_policy", "when_condition": condition, "match_columns": None,
            "to_principals": ["account users"],
            "column_mask": {"function_name": "cat.s.gr_mask_email_partial__string__11111111_partial"},
        }, "cat.s.t")
        return {"uc": {"cat": {"governed_tags": [], "mask_functions": [],
            "abac_policies": [{"table": "cat.s.t", "policies": [policy]}]}}}
    baseline, current = snap("x"), snap("y")
    digest = driver.hashlib.sha256(b"y").hexdigest()
    exact = driver.Adoption("old", "new", "legacy",
        policy_conditions=(("gr_mask_real_policy", "when_condition", digest),))
    treatment = driver.Adoption("old", "new", "legacy",
        policy_conditions=(("email_partial", "when_condition", digest),))
    assert driver.protection_diff(baseline, current, allowed_adoptions={"cat.s.t.c": exact}) == {}
    assert driver.protection_diff(baseline, current, allowed_adoptions={"cat.s.t.c": treatment})["policies"]


def test_live_policy_missing_definition_hash_is_weaker_when_snapshot_has_hashes():
    baseline = snapshot(masks=("mask_ssn", "mask_other"))
    current = snapshot(masks=("mask_ssn", "mask_other"))
    current["uc"]["cat"]["mask_functions"][0].pop("definition_hash")
    assert driver.protection_diff(baseline, current) == {"masks": [
        "cat.s.mask_ssn: definition unreadable"
    ]}


def test_live_row_filter_function_definition_change_is_weaker():
    baseline = snapshot(masks=("filter_restricted",), tags=())
    baseline["uc"]["cat"]["abac_policies"][0]["policies"] = [{
        "name": "restricted_rows",
        "to_principals": ["account users"],
        "except_principals": [],
        "row_filter": {"function_name": "cat.s.filter_restricted"},
    }]
    current = json.loads(json.dumps(baseline))
    current["uc"]["cat"]["mask_functions"][0]["definition_hash"] = "changed"
    assert driver.protection_diff(baseline, current) == {"masks": [
        "cat.s.filter_restricted: definition changed"
    ]}


def test_old_reports_without_definition_hashes_do_not_gain_findings():
    baseline = snapshot()
    current = snapshot()
    for report in (baseline, current):
        for routine in report["uc"]["cat"]["mask_functions"]:
            routine.pop("definition_hash")
    assert driver.protection_diff(baseline, current) == {}


def test_azure_legacy_never_raw_replaced_by_raw_capable_treatment_is_weaker():
    baseline = azure_legacy_mid_migration_snapshot()
    current = azure_legacy_mid_migration_snapshot(treatment="email_partial")
    assert driver.protection_diff(baseline, current) == {"columns": [
        "louis_serverless.dev_to_prod_demo.payments.cvv "
        "gr_treatment never-raw redact -> email_partial"
    ]}


def test_widened_grant_after_restore_fails(tmp_path, live):
    class Widen(FakeSnapshots): mode = "widen_after"
    live()
    assert driver.main(live_args(tmp_path, "s4"), snapshot_factory=Widen) == 1
    assert json.loads((tmp_path / "reports" / "s4.json").read_text())["status"] == "RESTORE FAILED"


# Blocker 6: human gates.
def test_human_scenario_without_tty_is_not_run(tmp_path, live):
    calls = live()
    assert driver.main(live_args(tmp_path, "champion"), snapshot_factory=FakeSnapshots) == 0
    report = json.loads((tmp_path / "reports" / "champion.json").read_text())
    assert report["status"] == "NOT RUN"
    assert not makes(calls)


def test_closed_stdin_at_gate_is_not_run_and_aborts_the_run(tmp_path, live, monkeypatch):
    calls = live()
    monkeypatch.setattr(driver.sys.stdin, "isatty", lambda: True)
    def eof(_): raise EOFError
    monkeypatch.setattr("builtins.input", eof)
    driver.main(live_args(tmp_path, "s16,manifest", "--value", "agent_a=a"), snapshot_factory=FakeSnapshots)
    report = json.loads((tmp_path / "reports" / "s16.json").read_text())
    assert report["status"] == "NOT RUN" and "stdin closed" in report["error"]
    assert report["steps"][1]["status"] == "planned"
    assert not makes(calls) and not (tmp_path / "reports" / "manifest.json").exists()


def test_wrong_human_confirmation_is_error_and_reported(tmp_path, live, monkeypatch):
    live()
    monkeypatch.setattr(driver.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "not-champion")
    assert driver.main(live_args(tmp_path, "champion", *CHAMPION_VALUES), snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports" / "champion.json").read_text())
    assert report["status"] == "ERROR"
    assert "exact token" in report["error"]


# Blocker 7: redaction.
def test_logs_redact_json_bearer_token_and_auth_file_values(tmp_path, live):
    live(secrets=True)
    env = tmp_path / "checkout" / "aws" / "envs" / "dev"; env.mkdir(parents=True)
    (env / "auth.auto.tfvars").write_text('databricks_client_secret = "file-secret-value"\n')
    driver.main(live_args(tmp_path, "s4"), snapshot_factory=FakeSnapshots)
    logs = "\n".join(path.read_text() for path in (tmp_path / "reports" / "logs").glob("*.log"))
    assert "maintain" in logs
    assert "dapiSECRET" not in logs and "file-secret-value" not in logs
    assert "Authorization: Bearer" not in logs


def test_redact_text_covers_env_assignments_and_bearer():
    text = driver.redact_text("DATABRICKS_TOKEN=abc123 curl -H 'Authorization: Bearer xyz' x Bearer abcdefghijkl")
    assert "abc123" not in text and "xyz" not in text and "abcdefghijkl" not in text


# Blocker 8: the checkout is pinned to the requested commit.
def test_head_drift_mid_scenario_is_an_error_before_the_next_step(tmp_path, live):
    calls = live(drift_after_make=True)
    assert driver.main(live_args(tmp_path, "s4"), snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports" / "s4.json").read_text())
    assert "not the pinned" in report["error"]
    assert "exit_code" not in report["steps"][1]
    assert makes(calls) == [["make", "maintain", "ENV=prod"]]


def test_every_scenario_starts_by_pinning_the_requested_commit(tmp_path, live):
    calls = live()
    assert driver.main(live_args(tmp_path, "s4,s14"), snapshot_factory=FakeSnapshots) == 0
    pins = [argv for argv, _ in calls if argv[:4] == ["git", "checkout", "--force", "--detach"]]
    first_make = next(i for i, (argv, _) in enumerate(calls) if argv[0] == "make")
    # The initial checkout and the scenario pin both precede the first command.
    assert sum(1 for argv, _ in calls[:first_make] if argv[:4] == ["git", "checkout", "--force", "--detach"]) == 2
    assert all(argv[4] == COMMIT for argv in pins)


# Blocker 5 and the reviewer's simulated live run, offline: real git, fake make.
FAKE_MAKE = r'''#!{python}
import json, os, pathlib, subprocess, sys
args = sys.argv[1:]
log = pathlib.Path(os.environ["FAKE_MAKE_LOG"])
if args[:1] == ["-qp"]:
    print("help:"); [print(t + ":") for t in json.loads(os.environ["FAKE_TARGETS"])]; sys.exit(1)
target = args[0]; kv = dict(a.split("=", 1) for a in args[1:] if "=" in a)
def git(*a): return subprocess.run(["git", *a], capture_output=True, text=True).stdout.strip()
head = git("rev-parse", "HEAD"); dirty = git("status", "--porcelain")
with log.open("a") as fh:
    fh.write(json.dumps({{"argv": args, "head": head, "dirty": bool(dirty),
                         "remote": os.environ.get("RELEASE_REMOTE")}}) + "\n")
state = pathlib.Path(os.environ["GENIERAILS_STATE_DIR"]); flag = state / "widening"
if target in ("rehearse", "release-plan", "release", "maintain") and dirty:
    print(f"{{target}}: the checkout is not clean"); sys.exit(1)
if target == "generate":
    p = pathlib.Path("envs/dev/generated/abac.auto.tfvars"); p.write_text(p.read_text() + "# regenerated\n")
elif target == "promote-to" and kv.get("CHECK") != "1":
    pathlib.Path("envs/prod/generated/abac.auto.tfvars").write_text(
        pathlib.Path("envs/dev/generated/abac.auto.tfvars").read_text())
elif target == "release-plan":
    subprocess.run(["git", "fetch", "--quiet", os.environ["RELEASE_REMOTE"], "main"], check=True)
    upstream = git("rev-parse", "FETCH_HEAD")
    if subprocess.run(["git", "merge-base", "--is-ancestor", head, upstream]).returncode:
        print(f"release-plan refused:\n  - HEAD {{head[:12]}} is not on {{os.environ['RELEASE_REMOTE']}}/main"); sys.exit(1)
    ack = pathlib.Path("envs/prod/ack.txt")
    if flag.exists() and "prod:reader:" not in (ack.read_text() if ack.exists() else ""):
        print("ERROR: unacknowledged reader prod:reader:cat.s.t:g: widens SELECT")
        print("release-plan: refused by the reader or weakening check (above); No manifest was written."); sys.exit(1)
    manifest = state / f"manifest-{{head}}.json"; manifest.write_text("{{}}")
    print(f"  Manifest (immutable): {{manifest}}")
elif target == "release":
    if not pathlib.Path(kv["MANIFEST"]).is_file(): print("release: no manifest"); sys.exit(1)
    flag.unlink(missing_ok=True)
    ack = pathlib.Path("envs/prod/ack.txt")
    (state / "released.json").write_text(json.dumps({{
        "ack": ack.read_text() if ack.exists() else "",
        "abac": pathlib.Path("envs/prod/generated/abac.auto.tfvars").read_text()}}))
if target in ("rehearse", "release", "verify-access"):
    result = pathlib.Path("envs") / kv.get("ENV", "dev") / "generated" / ".verify_access.json"
    result.parent.mkdir(parents=True, exist_ok=True); result.write_text(os.environ["FAKE_VERIFY_RESULT"])
print(f"{{target}} ok")
'''


CAN_RUN_ACL = {"access_control_list": [
    {"group_name": "analysts", "all_permissions": [{"permission_level": "CAN_RUN", "inherited": False}]}]}


class ReleasedSnapshots(FakeSnapshots):
    """Prod access follows what the fake make last released: the ack's reader, or champion's agent."""
    def __init__(self, _cloud_root, state):
        super().__init__()
        self.released = Path(state).parent / "released.json"

    def capture(self, env):
        if env != "prod" or not self.released.exists():
            return snapshot()
        released = json.loads(self.released.read_text())
        if "regenerated" in released["abac"]:
            snap = snapshot(grants=("analysts",))
            snap["agents"] = [{"name": "a.auto", "space_id": "space-a", "export": {}, "acl": CAN_RUN_ACL}]
            return snap
        return snapshot(grants=("g",) if "prod:reader:cat.s.t:g" in released["ack"] else ())


@pytest.fixture
def sim(tmp_path, monkeypatch):
    # Hermetic git: no global hooks, signing or identity from the developer's machine.
    (tmp_path / "gitconfig").write_text("")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "gitconfig"))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    checkout = tmp_path / "checkout"
    baseline = _genierails_checkout(checkout, customer=True)
    bin_dir = tmp_path / "bin"; bin_dir.mkdir()
    (bin_dir / "make").write_text(FAKE_MAKE.format(python=sys.executable)); (bin_dir / "make").chmod(0o755)
    state = tmp_path / "state"; state.mkdir()
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_MAKE_LOG", str(tmp_path / "make.log"))
    monkeypatch.setenv("FAKE_TARGETS", json.dumps(sorted(REAL_TARGETS)))
    monkeypatch.setenv("FAKE_VERIFY_RESULT", json.dumps(VERIFY_PASS))
    monkeypatch.delenv("CI", raising=False)
    args = ["--cloud", "aws", "--repo", str(checkout), "--checkout", str(checkout), "--commit", baseline,
            "--state-dir", str(state), "--reports", str(tmp_path / "reports"), "--confirm-live", *CHECKED_COLUMNS]

    def log():
        path = tmp_path / "make.log"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
    return SimpleNamespace(checkout=checkout, baseline=baseline, args=args, log=log, state=state,
                           reports=tmp_path / "reports")


def _git(repo, *argv):
    return subprocess.run(["git", "-C", str(repo), *argv], capture_output=True, text=True, check=True).stdout.strip()


UPSTREAM_GITIGNORE = Path(__file__).parents[2] / ".gitignore"
IDENTITY = ("-c", "user.name=genierails-e2e", "-c", "user.email=genierails-e2e@localhost")


def _genierails_checkout(checkout, *, customer, commit_envs=True):
    """A GenieRails checkout with the upstream root .gitignore (which ignores envs/).

    customer: the cloud dir has the block `make setup GITIGNORE=customer` writes.
    """
    from scripts import customer_gitignore
    cloud = checkout / "aws"
    for env in ("dev", "prod"):
        (cloud / "envs" / env / "generated").mkdir(parents=True)
        (cloud / "envs" / env / "env.auto.tfvars").write_text('uc_catalog = "cat"\n')
        (cloud / "envs" / env / "generated" / "abac.auto.tfvars").write_text("# baseline\n")
    (cloud / "Makefile").write_text("include ../shared/Makefile.shared\n")
    (checkout / ".gitignore").write_text(UPSTREAM_GITIGNORE.read_text())
    if customer:
        (cloud / ".gitignore").write_text(customer_gitignore.block(cloud))
    git_env = {**os.environ, **driver.GIT_IDENTITY}
    subprocess.run(["git", "init", "-q", "-b", "main", str(checkout)], check=True, env=git_env)
    subprocess.run(["git", "-C", str(checkout), "add", "-A"], check=True, env=git_env)
    if commit_envs and not customer:
        # Even force-added env files leave new authored files ignored.
        subprocess.run(["git", "-C", str(checkout), "add", "-f", "aws/envs"], check=True, env=git_env)
    subprocess.run(["git", "-C", str(checkout), "commit", "-qm", "baseline"], check=True, env=git_env)
    return _git(checkout, "rev-parse", "HEAD")


def test_simulated_live_s6_merges_ack_on_driver_branch_and_restores(sim):
    (sim.state / "widening").write_text("")
    rc = driver.main([*sim.args, "--scenarios", "s6", "--value", "ack_entry=prod:reader:cat.s.t:g"],
                     snapshot_factory=ReleasedSnapshots)
    report = json.loads((sim.reports / "s6.json").read_text())
    assert rc == 0, report["error"]
    assert report["status"] == "passed" and report["restored_lifecycle"] is True
    log = sim.log()
    targets = [entry["argv"][0] for entry in log]
    assert targets[:2] == ["release-plan", "check"] and targets[-4:] == ["rehearse", "release-plan", "release", "maintain"]
    assert all(entry["remote"] == driver.E2E_REMOTE for entry in log)
    applying = [e for e in log if e["argv"][0] in ("rehearse", "release-plan", "release", "maintain")]
    assert not any(e["dirty"] for e in applying)
    # The released commit is a merge on the driver's main that carries the ack.
    released = next(e for e in log if e["argv"][0] == "release")["head"]
    assert "prod:reader:cat.s.t:g" in _git(sim.checkout, "show", f"{released}:aws/envs/prod/ack.txt")
    assert len(_git(sim.checkout, "rev-list", "--parents", "-n1", released).split()) == 3
    assert _git(sim.checkout, "branch", "--list", f"{driver.E2E_BRANCH_PREFIX}s6-*")
    # Restored: checkout pinned and clean; the driver's main is a forward fix with the baseline tree.
    assert _git(sim.checkout, "rev-parse", "HEAD") == sim.baseline
    assert _git(sim.checkout, "status", "--porcelain") == ""
    tip = _git(sim.checkout, "rev-parse", f"{driver.E2E_BRANCH_PREFIX}main")
    assert tip != sim.baseline and _git(sim.checkout, "rev-parse", f"{tip}^{{tree}}") == _git(
        sim.checkout, "rev-parse", f"{sim.baseline}^{{tree}}")
    assert "origin" not in _git(sim.checkout, "remote").split()
    assert json.loads((sim.reports / "final-protection.json").read_text())["status"] == "passed"


def _remotes(checkout):
    return _git(checkout, "remote").split()


def test_driver_remote_is_removed_after_a_passing_run_and_a_rerun_works(sim, tmp_path):
    for run in ("first", "second"):
        (sim.state / "widening").write_text("")
        args = list(sim.args); args[args.index("--reports") + 1] = str(tmp_path / f"reports-{run}")
        rc = driver.main([*args, "--scenarios", "s6", "--value", "ack_entry=prod:reader:cat.s.t:g"],
                         snapshot_factory=ReleasedSnapshots)
        report = json.loads((tmp_path / f"reports-{run}" / "s6.json").read_text())
        assert rc == 0 and report["status"] == "passed", report["error"]
        assert driver.E2E_REMOTE not in _remotes(sim.checkout)
        # The bare repo stays with the run's reports for inspection.
        assert (tmp_path / f"reports-{run}" / "e2e-remote.git" / "HEAD").is_file()


def test_driver_remote_is_removed_after_a_failing_run(sim):
    # No widening: the first release-plan passes instead of refusing, so s6 fails after the remote exists.
    rc = driver.main([*sim.args, "--scenarios", "s6", "--value", "ack_entry=prod:reader:cat.s.t:g"],
                     snapshot_factory=FakeSnapshots)
    report = json.loads((sim.reports / "s6.json").read_text())
    assert rc == 1 and report["status"] == "ERROR" and "did not satisfy its contract" in report["error"]
    assert any(e["argv"][0] == "release-plan" and e["remote"] == driver.E2E_REMOTE for e in sim.log())
    assert driver.E2E_REMOTE not in _remotes(sim.checkout)


def test_driver_remote_is_removed_when_final_protection_capture_is_interrupted(sim):
    class InterruptFinalCapture(FakeSnapshots):
        def capture(self, env):
            # The scenario's report is written before the run's final-protection capture.
            if (sim.reports / "s4.json").exists(): raise KeyboardInterrupt("final protection")
            return super().capture(env)

    with pytest.raises(KeyboardInterrupt, match="final protection"):
        driver.main([*sim.args, "--scenarios", "s4"], snapshot_factory=InterruptFinalCapture)
    assert json.loads((sim.reports / "s4.json").read_text())["status"] == "passed"
    assert not (sim.reports / "final-protection.json").exists()
    assert driver.E2E_REMOTE not in _remotes(sim.checkout)


def test_foreign_driver_remote_is_refused_and_kept(sim, tmp_path):
    foreign = tmp_path / "someone-else.git"
    _git(sim.checkout, "remote", "add", driver.E2E_REMOTE, str(foreign))
    rc = driver.main([*sim.args, "--scenarios", "s6", "--value", "ack_entry=prod:reader:cat.s.t:g"],
                     snapshot_factory=FakeSnapshots)
    report = json.loads((sim.reports / "s6.json").read_text())
    assert rc == 1 and report["status"] == "ERROR"
    assert "points elsewhere" in report["error"] and str(foreign) in report["error"]
    assert f"git remote remove {driver.E2E_REMOTE}" in report["error"]
    assert sim.log() == []
    assert _git(sim.checkout, "remote", "get-url", driver.E2E_REMOTE) == str(foreign)


def test_simulated_live_champion_commits_generate_then_promotion(sim, monkeypatch):
    monkeypatch.setattr(driver.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "champion")
    rc = driver.main([*sim.args, "--scenarios", "champion,s4", *CHAMPION_VALUES], snapshot_factory=ReleasedSnapshots)
    champion = json.loads((sim.reports / "champion.json").read_text())
    assert rc == 0, champion["error"]
    assert champion["status"] == "passed" and len(champion["confirmations"]) == 2
    log = sim.log()
    rehearses = [e for e in log if e["argv"][0] == "rehearse"]
    assert rehearses and not any(e["dirty"] for e in rehearses)
    assert len({e["head"] for e in rehearses[:2]}) == 2  # generate merge, then promotion merge
    # The generate merge commits agent A's file, written from the required values.
    agent_file = _git(sim.checkout, "show", f"{rehearses[0]['head']}:aws/envs/dev/agents/a.auto.tfvars")
    assert agent_file == 'genie_space_id = "space-a"\nacl_groups     = ["analysts"]'

    assert any(e["argv"][:2] == ["maintain", "ENV=prod"] and e["head"] == sim.baseline for e in log)
    assert json.loads((sim.reports / "s4.json").read_text())["status"] == "passed"
    assert _git(sim.checkout, "rev-parse", "HEAD") == sim.baseline


def test_customer_marker_matches_the_block_make_setup_writes():
    from scripts import customer_gitignore
    assert customer_gitignore.BEGIN.startswith(driver.CUSTOMER_GITIGNORE_MARKER)


@pytest.mark.parametrize("commit_envs", [False, True])
def test_upstream_checkout_is_refused_before_anything_runs(sim, tmp_path, commit_envs):
    checkout = tmp_path / "upstream"
    baseline = _genierails_checkout(checkout, customer=False, commit_envs=commit_envs)
    # The failure the refusal prevents: an authored env file can't be staged.
    (checkout / "aws/envs/prod/ack.txt").write_text("prod:reader:cat.s.t:g\n")
    staged = subprocess.run(["git", "-C", str(checkout), "add", "-A", "--", "aws/envs"],
                            capture_output=True, text=True)
    assert staged.returncode == 1 and "ignored" in staged.stderr
    (checkout / "aws/envs/prod/ack.txt").unlink()
    reports = tmp_path / "upstream-reports"
    args = [a.replace(str(sim.checkout), str(checkout)).replace(sim.baseline, baseline)
            for a in sim.args]
    args[args.index("--reports") + 1] = str(reports)

    with pytest.raises(SystemExit) as exc:
        driver.main([*args, "--scenarios", "s6"], snapshot_factory=FakeSnapshots)

    message = str(exc.value)
    assert "customer-shaped checkout that commits envs/" in message
    assert "aws/.gitignore at" in message and "has no GenieRails customer block" in message
    assert "make setup ENV=dev GITIGNORE=customer" in message and "Nothing was run." in message
    if not commit_envs:
        assert "aws/envs/dev/env.auto.tfvars is not committed" in message
    assert sim.log() == [] and not reports.exists()
    assert "genierails-e2e" not in _git(checkout, "remote").split()
    assert _git(checkout, "rev-parse", "HEAD") == baseline and _git(checkout, "status", "--porcelain") == ""


def test_customer_block_without_committed_envs_is_refused(sim, tmp_path):
    checkout = tmp_path / "half"
    baseline = _genierails_checkout(checkout, customer=True)
    _git(checkout, "rm", "-q", "aws/envs/prod/env.auto.tfvars")
    _git(checkout, *IDENTITY, "commit", "-qm", "untrack prod")
    head = _git(checkout, "rev-parse", "HEAD")
    assert head != baseline
    problems = driver.customer_checkout_problems(checkout, "aws", head)
    assert problems == [f"aws/envs/prod/env.auto.tfvars is not committed at {head[:12]}"]


def test_customer_checkout_passes_and_unknown_commit_is_refused(sim, tmp_path):
    assert driver.customer_checkout_problems(sim.checkout, "aws", sim.baseline) == []
    missing = "f" * 40
    assert driver.customer_checkout_problems(sim.checkout, "aws", missing) == [
        f"commit {missing[:12]} is not in {sim.checkout}"]


def test_ignored_env_file_in_the_checked_out_tree_is_refused(sim):
    # A hand-edited block that still ignores envs/ (here: an extra rule after it).
    gitignore = sim.checkout / "aws/.gitignore"
    gitignore.write_text(gitignore.read_text() + "/envs/dev/\n")
    _git(sim.checkout, *IDENTITY, "commit", "-qam", "ignore dev")
    head = _git(sim.checkout, "rev-parse", "HEAD")
    assert driver.customer_checkout_problems(sim.checkout, "aws", head) == [
        "aws/envs/dev/env.auto.tfvars is git-ignored, so authored env changes can't be committed"]


def _assert_nothing_ran(sim, reports, checkout, head):
    assert sim.log() == [] and not reports.exists() and not (reports / "logs").exists()
    assert "genierails-e2e" not in _git(checkout, "remote").split()
    assert _git(checkout, "rev-parse", "HEAD") == head and _git(checkout, "status", "--porcelain") == ""


def _live_args(sim, checkout, commit, reports):
    args = [a.replace(str(sim.checkout), str(checkout)).replace(sim.baseline, commit) for a in sim.args]
    args[args.index("--reports") + 1] = str(reports)
    return [*args, "--scenarios", "s6"]


class _NoSnapshots:
    def __init__(self, *_):
        raise AssertionError("no snapshot provider before the preflight passes")


def test_non_head_commit_with_a_post_marker_ignore_rule_is_refused(sim, tmp_path):
    # The bad commit (marker, then /envs/dev/ ignored) is not checked out: HEAD is the good baseline.
    gitignore = sim.checkout / "aws/.gitignore"
    gitignore.write_text(gitignore.read_text() + "/envs/dev/\n")
    _git(sim.checkout, *IDENTITY, "commit", "-qam", "ignore dev")
    bad = _git(sim.checkout, "rev-parse", "HEAD")
    _git(sim.checkout, "checkout", "-q", "--detach", sim.baseline)
    reports = tmp_path / "non-head-reports"
    with pytest.raises(SystemExit) as exc:
        driver.main(_live_args(sim, sim.checkout, bad, reports), snapshot_factory=_NoSnapshots)
    assert f"is at {sim.baseline[:12]}, not {bad[:12]}" in str(exc.value)
    _assert_nothing_ran(sim, reports, sim.checkout, sim.baseline)
    # Checked out, the same commit is refused on its ignore rule, for dev only.
    _git(sim.checkout, "checkout", "-q", "--detach", bad)
    assert driver.customer_checkout_problems(sim.checkout, "aws", bad) == [
        "aws/envs/dev/env.auto.tfvars is git-ignored, so authored env changes can't be committed"]


def test_prod_ignore_rule_is_refused_too(sim):
    gitignore = sim.checkout / "aws/.gitignore"
    gitignore.write_text(gitignore.read_text() + "/envs/prod/\n")
    _git(sim.checkout, *IDENTITY, "commit", "-qam", "ignore prod")
    head = _git(sim.checkout, "rev-parse", "HEAD")
    assert driver.customer_checkout_problems(sim.checkout, "aws", head) == [
        "aws/envs/prod/env.auto.tfvars is git-ignored, so authored env changes can't be committed"]


@pytest.mark.parametrize("link", ["file", "env_dir", "envs_dir"])
def test_committed_symlinked_env_files_are_refused(sim, tmp_path, link):
    checkout = tmp_path / f"links-{link}"
    _genierails_checkout(checkout, customer=True)
    elsewhere = checkout / "elsewhere"
    if link == "file":
        (elsewhere / "dev").mkdir(parents=True)
        target = checkout / "aws/envs/dev/env.auto.tfvars"
        (elsewhere / "dev/env.auto.tfvars").write_text(target.read_text())
        target.unlink(); target.symlink_to("../../../elsewhere/dev/env.auto.tfvars")
        bad = "aws/envs/dev/env.auto.tfvars (120000)"
    elif link == "env_dir":
        (checkout / "aws/envs/dev").rename(elsewhere)
        (checkout / "aws/envs/dev").symlink_to("../../elsewhere")
        bad = "aws/envs/dev (120000)"
    else:
        (checkout / "aws/envs").rename(elsewhere)
        (checkout / "aws/envs").symlink_to("../elsewhere")
        bad = "aws/envs (120000)"
    _git(checkout, "add", "-A")
    _git(checkout, *IDENTITY, "commit", "-qm", "symlinked envs")
    head = _git(checkout, "rev-parse", "HEAD")
    reports = tmp_path / f"links-{link}-reports"
    with pytest.raises(SystemExit) as exc:
        driver.main(_live_args(sim, checkout, head, reports), snapshot_factory=_NoSnapshots)
    assert "must be a regular committed file in real directories" in str(exc.value)
    assert bad in str(exc.value)
    _assert_nothing_ran(sim, reports, checkout, head)


def test_dirty_checkout_is_refused_before_anything_runs(sim, tmp_path):
    (sim.checkout / "aws/.gitignore").write_text("# local edit\n")
    reports = tmp_path / "dirty-reports"
    with pytest.raises(SystemExit) as exc:
        driver.main(_live_args(sim, sim.checkout, sim.baseline, reports), snapshot_factory=_NoSnapshots)
    assert "is not clean at" in str(exc.value)
    assert sim.log() == [] and not reports.exists()
    _git(sim.checkout, "checkout", "-q", "--", "aws/.gitignore")


# D1-D12 regression coverage. Each assertion distinguishes this driver from
# origin/main 16c9797 and remains entirely offline.
def test_d1_detach_refusal_matches_wrong_agent_only():
    pattern = driver.REFUSALS["detach"]
    assert re.search(pattern, "ERROR: detach: missing not found: detach-agent works on an agent file")
    assert not re.search(pattern, "make detach-agent refused before taking the lock")


def test_d2_ci_scenario_needs_no_value_and_omits_allowed_dev_rehearse():
    scenario = next(s for s in driver.SCENARIOS if s.key == "s13")
    assert driver.unresolved_values(scenario, {}) == []
    assert not any(s.target == "rehearse" for s in scenario.steps)
    assert any("SPACE=ci-guard-fixture" in s.command for s in scenario.steps)


def test_d3_staged_release_replans_with_fresh_manifest(tmp_path, live):
    calls = live(staged_release=True)
    assert driver.main(live_args(tmp_path, "migration-cutover"), snapshot_factory=FakeSnapshots) == 0
    commands = makes(calls)
    assert [argv[1] for argv in commands[:7]] == [
        "migrate-plan", "migrate-plan", "rehearse", "release-plan", "release", "release-plan", "release"]
    manifests = [next(a for a in argv if a.startswith("MANIFEST=")) for argv in commands if argv[1] == "release"]
    assert manifests[:2] == ["MANIFEST=/tmp/approved-1.json", "MANIFEST=/tmp/approved-2.json"]
    report = json.loads((tmp_path / "reports/migration-cutover.json").read_text())
    # No known_failure marker: a pass is a plain pass.
    assert report["status"] == "passed" and report["known_failure"] is None


def test_d4_snapshot_query_includes_deterministic_masks():
    class Statement:
        def execute_statement(self, *, statement, **_):
            if "routines" in statement:
                assert "routine_definition" in statement
                assert "startswith" not in statement and " LIKE " not in statement
            cols = [SimpleNamespace(name=n) for n in ("table_catalog", "table_schema", "table_name")]
            rows = [["cat", "s", "t"]] if "information_schema.tables" in statement else []
            return SimpleNamespace(status=SimpleNamespace(state="SUCCEEDED"), statement_id="x",
                manifest=SimpleNamespace(schema=SimpleNamespace(columns=cols)),
                result=SimpleNamespace(data_array=rows, next_chunk_index=None))
    client = SimpleNamespace(statement_execution=Statement(),
                             policies=SimpleNamespace(list_policies=lambda *_a, **_k: []))
    driver._sql_snapshots(client, "wh", "cat")


@pytest.mark.parametrize("definition", [None, ""])
def test_mask_snapshot_fails_closed_when_definition_is_unreadable(definition):
    class Statement:
        def execute_statement(self, *, statement, **_):
            if "information_schema.routines" in statement:
                names = ("routine_catalog", "routine_schema", "routine_name", "routine_type",
                         "routine_definition")
                rows = [["cat", "s", "mask_ssn", "FUNCTION", definition]]
            elif "information_schema.tables" in statement:
                names = ("table_catalog", "table_schema", "table_name")
                rows = [["cat", "s", "t"]]
            else:
                names, rows = (), []
            return SimpleNamespace(status=SimpleNamespace(state="SUCCEEDED"), statement_id="x",
                manifest=SimpleNamespace(schema=SimpleNamespace(
                    columns=[SimpleNamespace(name=n) for n in names])),
                result=SimpleNamespace(data_array=rows, next_chunk_index=None))
    client = SimpleNamespace(statement_execution=Statement(),
                             policies=SimpleNamespace(list_policies=lambda *_a, **_k: []))
    with pytest.raises(RuntimeError, match=r"cat\.s\.mask_ssn: policy function definition is unreadable"):
        driver._sql_snapshots(client, "wh", "cat")


@pytest.mark.parametrize("routine_rows", [
    pytest.param([], id="no-visible-routines"),
    pytest.param([["cat", "s", "mask_other", "FUNCTION", "RETURN 'masked'"]],
                 id="partial-routine-inventory"),
])
def test_policy_function_missing_from_routines_fails_closed(routine_rows):
    class Statement:
        def execute_statement(self, *, statement, **_):
            if "information_schema.routines" in statement:
                names = ("routine_catalog", "routine_schema", "routine_name", "routine_type",
                         "routine_definition")
                rows = routine_rows
            elif "information_schema.tables" in statement:
                names = ("table_catalog", "table_schema", "table_name")
                rows = [["cat", "s", "t"]]
            else:
                names, rows = (), []
            return SimpleNamespace(status=SimpleNamespace(state="SUCCEEDED"), statement_id="x",
                manifest=SimpleNamespace(schema=SimpleNamespace(
                    columns=[SimpleNamespace(name=n) for n in names])),
                result=SimpleNamespace(data_array=rows, next_chunk_index=None))
    policy = SimpleNamespace(when_condition=None, match_columns=None, as_dict=lambda: {
        "name": "secure_rows", "row_filter": {"function_name": "cat.s.secure_rows"}})
    client = SimpleNamespace(statement_execution=Statement(),
                             policies=SimpleNamespace(list_policies=lambda *_a, **_k: [policy]))
    with pytest.raises(RuntimeError, match=r"cat\.s\.secure_rows: policy function definition is unreadable"):
        driver._sql_snapshots(client, "wh", "cat")


def test_mask_snapshot_hashes_definition_without_persisting_body():
    body = "RETURN 'redacted'"

    class Statement:
        def execute_statement(self, *, statement, **_):
            if "information_schema.routines" in statement:
                names = ("routine_catalog", "routine_schema", "routine_name", "routine_type",
                         "routine_definition")
                rows = [["cat", "s", "secure_rows", "FUNCTION", body]]
            elif "information_schema.tables" in statement:
                names = ("table_catalog", "table_schema", "table_name")
                rows = [["cat", "s", "t"]]
            else:
                names, rows = (), []
            return SimpleNamespace(status=SimpleNamespace(state="SUCCEEDED"), statement_id="x",
                manifest=SimpleNamespace(schema=SimpleNamespace(
                    columns=[SimpleNamespace(name=n) for n in names])),
                result=SimpleNamespace(data_array=rows, next_chunk_index=None))
    row_policy = SimpleNamespace(when_condition="false", match_columns=None, as_dict=lambda: {
        "name": "secure_rows", "row_filter": {"function_name": "cat.s.secure_rows"}})
    client = SimpleNamespace(statement_execution=Statement(),
                             policies=SimpleNamespace(list_policies=lambda *_a, **_k: [row_policy]))
    routine = driver._sql_snapshots(client, "wh", "cat")["mask_functions"][0]
    assert routine["routine_name"] == "secure_rows"
    assert routine["definition_hash"] == driver.hashlib.sha256(body.encode()).hexdigest()
    assert "routine_definition" not in routine


def test_live_snapshot_hashes_sdk_condition_fields_omitted_by_as_dict():
    body = "RETURN 'redacted'"
    class Statement:
        def execute_statement(self, *, statement, **_):
            if "information_schema.routines" in statement:
                names = ("routine_catalog", "routine_schema", "routine_name", "routine_type", "routine_definition")
                rows = [["cat", "s", "mask_ssn", "FUNCTION", body]]
            elif "information_schema.tables" in statement:
                names, rows = ("table_catalog", "table_schema", "table_name"), [["cat", "s", "t"]]
            else:
                names, rows = (), []
            return SimpleNamespace(status=SimpleNamespace(state="SUCCEEDED"), statement_id="x",
                manifest=SimpleNamespace(schema=SimpleNamespace(columns=[SimpleNamespace(name=n) for n in names])),
                result=SimpleNamespace(data_array=rows, next_chunk_index=None))
    from databricks.sdk.service.catalog import PolicyInfo
    raw = dict(azure_legacy_mid_migration_snapshot()["uc"]["louis_serverless"]
               ["abac_policies"][0]["policies"][0])
    raw.update({"name": "mask_ssn_p", "when_condition": "false",
                "column_mask": {"function_name": "cat.s.mask_ssn"}})
    policy = PolicyInfo.from_dict(raw)
    client = SimpleNamespace(statement_execution=Statement(),
        policies=SimpleNamespace(list_policies=lambda *_a, **_k: [policy]))
    captured = driver._sql_snapshots(client, "wh", "cat")["abac_policies"][0]["policies"][0]
    assert captured["when_condition_hash"] == driver.hashlib.sha256(b"false").hexdigest()
    assert len(captured["match_columns_hash"]) == 64
    assert captured["match_values"] == ["redact"]
    assert "when_condition" not in captured


def _deterministic_snapshot(value, *, policy_value=None):
    policy_value = policy_value or value
    snap = snapshot(masks=(f"gr_mask_{value}_full",), tags=())
    evidence = snap["uc"]["cat"]
    evidence["governed_tags"] = [{"catalog_name": "cat", "schema_name": "s", "table_name": "t",
        "column_name": "ssn", "tag_name": "gr_treatment", "tag_value": value}]
    evidence["abac_policies"][0]["policies"][0].update({
        "name": f"gr_mask_cat_{policy_value}_full",
        "column_mask": {"function_name": f"cat.s.gr_mask_{policy_value}_full"}})
    return snap


def test_d5_generation_changes_normalize_but_legacy_and_failsafe_are_weaker():
    old = _deterministic_snapshot("email_partial__string__11111111")
    new = _deterministic_snapshot("email_partial__string__22222222")
    assert driver.protection_diff(old, new) == {}
    legacy = _deterministic_snapshot("email_partial")
    assert "columns" in driver.protection_diff(old, legacy)
    failsafe = _deterministic_snapshot("unmapped_redact__string__33333333")
    assert "columns" in driver.protection_diff(old, failsafe)
    downgrade = _deterministic_snapshot("ssn__string__44444444")
    assert "columns" in driver.protection_diff(old, downgrade)
    legacy_old = _deterministic_snapshot("masked_ssn")
    legacy_new = _deterministic_snapshot("public")
    assert "columns" in driver.protection_diff(legacy_old, legacy_new)


def _two_type_policy_snapshot(*, exempt_int=False, include_int=True):
    snap = _deterministic_snapshot("unmapped_redact__string__11111111")
    policies = snap["uc"]["cat"]["abac_policies"][0]["policies"]
    if include_int:
        policies.append({"name": "int", "to_principals": ["account users"],
            "except_principals": ["analysts"] if exempt_int else [],
            "column_mask": {"function_name": "cat.s.gr_mask_unmapped_redact__int__22222222_full"}})
    return snap


def test_d5_policy_types_do_not_collide_on_exemption_or_removal():
    before = _two_type_policy_snapshot()
    exempted = driver.protection_diff(before, _two_type_policy_snapshot(exempt_int=True))
    assert any("newly exempts analysts" in item for item in exempted["policies"])
    removed = driver.protection_diff(before, _two_type_policy_snapshot(include_int=False))
    assert any("unmapped_redact int full: removed" in item for item in removed["policies"])


def test_same_key_policy_aggregation_catches_exemption_on_first_policy():
    def staged(exempt_first):
        snap = _deterministic_snapshot("email_partial__string__11111111")
        snap["uc"]["cat"]["abac_policies"][0]["policies"] = [
            {"name": generation, "to_principals": ["account users"],
             "except_principals": ["analysts"] if exempt_first and index == 0 else [],
             "column_mask": {"function_name": f"cat.s.gr_mask_email_partial__string__{generation}_full"}}
            for index, generation in enumerate(("11111111", "22222222"))]
        return snap
    weak = driver.protection_diff(staged(False), staged(True))
    assert any("newly exempts analysts" in item for item in weak["policies"])


def _policy(name, function, to, exempt=(), value=None):
    policy = {"name": name, "to_principals": list(to), "except_principals": list(exempt),
              "column_mask": {"function_name": function}}
    if value:
        policy["match_columns"] = [{"alias": "c", "condition": f"hasTagValue('gr_treatment', '{value}')"}]
    return policy


def _cutover_snapshots(old="masked_email", new="email_partial__string__11111111", *, columns=("ssn",)):
    """A legacy policy -> the generator's tiered layout: partial to tier 2, full
    to everyone else (tiers and deployer exempt), as governance_policies builds it."""
    def tagged(value_by_column):
        return [{"catalog_name": "cat", "schema_name": "s", "table_name": "t", "column_name": column,
                 "tag_name": "gr_treatment", "tag_value": value} for column, value in value_by_column.items()]
    before = snapshot(masks=(f"mask_{old}",), tags=())
    before["uc"]["cat"]["governed_tags"] = tagged({c: old for c in columns})
    before["uc"]["cat"]["abac_policies"][0]["policies"] = [
        _policy(f"legacy_{old}", f"cat.s.mask_{old}", ["account users"], ["admins", "deployer"], old)]
    levels = ("full",) if driver.is_never_raw(new) else ("partial", "full")
    after = snapshot(masks=tuple(f"gr_mask_{new}_{level}" for level in levels), tags=())
    after["uc"]["cat"]["governed_tags"] = tagged({c: new for c in columns})
    after["uc"]["cat"]["abac_policies"][0]["policies"] = [
        _policy(f"gr_mask_cat_{new}_{level}", f"cat.s.gr_mask_{new}_{level}",
                ["analysts"] if level == "partial" else ["account users"],
                [] if driver.is_never_raw(new) else ["admins", "deployer"] if level == "partial"
                else ["admins", "analysts", "deployer"], new)
        for level in levels]
    adoption = {f"cat.s.t.{c}": driver.Adoption(old, new, f"mask_{old}") for c in columns}
    return before, after, adoption


def test_migration_correct_tiered_cutover_passes():
    for old, new in (("masked_email", "email_partial__string__11111111"), ("redact", "secret__string__11111111")):
        before, after, adoption = _cutover_snapshots(old, new)
        assert driver.protection_diff(before, after, allowed_adoptions=adoption) == {}


def test_migration_mapping_requires_exact_reviewed_pair():
    before, after, _ = _cutover_snapshots()
    wrong = {"cat.s.t.ssn": driver.Adoption("masked_email", "email_partial__string__99999999", "mask_masked_email")}
    assert "columns" in driver.protection_diff(before, after, allowed_adoptions=wrong)
    never_raw = _deterministic_snapshot("secret")
    unsafe = {"cat.s.t.ssn": driver.Adoption("secret", "email_partial__string__11111111", "mask_secret")}
    assert "columns" in driver.protection_diff(never_raw, after, allowed_adoptions=unsafe)


def test_migration_retag_as_planned_without_deterministic_policy_is_flagged():
    for old, new in (("masked_email", "email_partial__string__11111111"), ("redact", "secret__string__11111111")):
        before, after, adoption = _cutover_snapshots(old, new)
        after["uc"]["cat"]["mask_functions"] = []
        after["uc"]["cat"]["abac_policies"][0]["policies"] = []
        weak = driver.protection_diff(before, after, allowed_adoptions=adoption)
        assert any("-> ('" in item for item in weak["columns"]), weak
        assert weak["masks"] == [f"cat.s.mask_{old}"]
        assert weak["policies"] == [f"cat.s.t legacy_{old}: removed"]


def test_migration_replacement_must_exist_at_every_level_with_its_function():
    before, after, adoption = _cutover_snapshots()
    no_partial = {**after, "uc": {"cat": {**after["uc"]["cat"], "abac_policies": [{"table": "cat.s.t",
        "policies": after["uc"]["cat"]["abac_policies"][0]["policies"][1:]}]}}}
    assert "policies" in driver.protection_diff(before, no_partial, allowed_adoptions=adoption)
    no_function = {**after, "uc": {"cat": {**after["uc"]["cat"],
        "mask_functions": after["uc"]["cat"]["mask_functions"][:1]}}}
    assert "policies" in driver.protection_diff(before, no_function, allowed_adoptions=adoption)


def test_migration_replacement_must_cover_every_legacy_masked_principal():
    before, after, adoption = _cutover_snapshots()
    after["uc"]["cat"]["abac_policies"][0]["policies"][1]["except_principals"].append("contractors")
    weak = driver.protection_diff(before, after, allowed_adoptions=adoption)
    assert weak["policies"] == ["cat.s.t legacy_masked_email: removed"]


def test_migration_does_not_excuse_unrelated_failsafe_mask_loss():
    before, after, adoption = _cutover_snapshots("redact", "secret__string__11111111")
    before["uc"]["cat"]["mask_functions"].append({"routine_catalog": "cat", "routine_schema": "s",
        "routine_name": "gr_mask_unmapped_redact__string__aaaaaaaa_full"})
    weak = driver.protection_diff(before, after, allowed_adoptions=adoption)
    assert weak == {"masks": ["cat.s.unmapped_redact__string__full"]}


def test_migration_legacy_policy_stays_while_any_old_column_is_unadopted():
    before, after, adoption = _cutover_snapshots(columns=("ssn", "email"))
    after["uc"]["cat"]["governed_tags"][1]["tag_value"] = "masked_email"
    adoption.pop("cat.s.t.email")
    weak = driver.protection_diff(before, after, allowed_adoptions=adoption)
    assert weak["policies"] == ["cat.s.t legacy_masked_email: removed"]
    assert weak["masks"] == ["cat.s.mask_masked_email"]


def test_migration_inherited_legacy_policy_on_untagged_table_is_excused_but_shared_function_is_not():
    before, after, adoption = _cutover_snapshots()
    legacy = before["uc"]["cat"]["abac_policies"][0]["policies"][0]
    before["uc"]["cat"]["abac_policies"].append({"table": "cat.s.u", "policies": [dict(legacy)]})
    assert driver.protection_diff(before, after, allowed_adoptions=adoption) == {}
    # cat.s.u still masks with the legacy function: dropping it is a loss.
    after["uc"]["cat"]["abac_policies"].append({"table": "cat.s.u", "policies": [dict(legacy)]})
    assert driver.protection_diff(before, after, allowed_adoptions=adoption) == {"masks": [
        "cat.s.mask_masked_email",
        "cat.s.mask_masked_email: definition unreadable",
    ]}


def test_migration_legacy_match_value_must_be_the_adopted_old_value():
    before, after, adoption = _cutover_snapshots()
    before["uc"]["cat"]["abac_policies"][0]["policies"][0]["match_columns"][0]["condition"] = \
        "hasTagValue('gr_treatment', 'masked_phone')"
    assert "policies" in driver.protection_diff(before, after, allowed_adoptions=adoption)


def test_legacy_redact_to_partial_treatment_is_always_weakening():
    before, after, adoption = _cutover_snapshots("redact")
    weak = driver.protection_diff(before, after, allowed_adoptions=adoption)
    assert weak["columns"] == ["cat.s.t.ssn gr_treatment never-raw redact -> email_partial__string__11111111"]


def test_d6_prod_keyed_agent_id_files_are_snapshotted(tmp_path):
    env = tmp_path / "env"; env.mkdir()
    (env / ".genie_space_id_agent_a").write_text("prod-a\n")
    assert ("agent_a", "prod-a") in driver._agent_ids(env, {}, tmp_path / "state", tmp_path)


def test_d7_remote_state_is_never_silently_empty(tmp_path):
    env = tmp_path / "env"; env.mkdir(); (env / "remote_state.hcl").write_text("address = \"x\"\n")
    assert driver._state_serials(tmp_path / "state", env) == {"remote_state": "remote, not snapshotted"}


def test_d8_dry_run_reports_values_gates_refusals_and_cloud_entrypoint(tmp_path):
    driver.main(["--cloud", "azure", "--repo", str(ROOT), "--dry-run", "--scenarios", "s7",
                 "--reports", str(tmp_path)])
    report = json.loads((tmp_path / "s7.json").read_text())
    assert report["target_source"].endswith("azure/Makefile")
    assert {"agent_a", "wrong_agent"} <= set(report["missing_values"])
    assert report["refusal_patterns"] and report["refusal_patterns"][0]["found_in_source"]


def test_d8_refusal_discovery_does_not_scan_tests_or_the_driver(tmp_path):
    fake = tmp_path / "repo"
    for relative in ("shared/tests/test_fake.py", "shared/scripts/e2e/driver.py"):
        path = fake / relative; path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not found: detach-agent works on an agent file\n")
    assert not driver.refusal_found_in_source(fake, driver.REFUSALS["detach"])


def test_d9_migration_is_read_only_and_cutover_is_explicit():
    assert driver.ALLOWED_CHANGES["migration"] == frozenset()
    assert "migration" not in driver.WEAKENING_EXPECTED
    cutover = next(s for s in driver.SCENARIOS if s.key == "migration-cutover")
    assert [s.target for s in cutover.steps] == ["migrate-plan", "migrate-plan", "rehearse", "release-plan", "release"]
    # Its marker named fix/migration-adopt-order (#146); with the staged coverage-check fix a pass is a PASS.
    assert "fix/migration-adopt-order" not in " ".join(cutover.preconditions)
    assert cutover.known_failure is None
    assert not [s.key for s in driver.SCENARIOS if s.known_failure]


def _first_rehearse_refused():
    refused = {"done": False}
    def first_rehearse(argv):
        match = argv[1:2] == ["rehearse"] and not refused["done"]
        refused["done"] |= match
        return match
    return first_rehearse


def test_a_failing_migration_cutover_is_a_plain_error(tmp_path, live):
    calls = live(refuse=(_first_rehearse_refused(), "engine migration order failure\n"))
    assert driver.main(live_args(tmp_path, "migration-cutover"), snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports/migration-cutover.json").read_text())
    assert report["known_failure"] is None and report["status"] == "ERROR"
    assert "expected until" not in report["error"]
    assert any(argv[1] == "rehearse" for argv in makes(calls))


def test_migration_cutover_dry_run_carries_no_known_failure(tmp_path, capsys):
    assert driver.main(["--cloud", "aws", "--repo", str(ROOT), "--dry-run",
        "--scenarios", "migration-cutover", "--reports", str(tmp_path)]) == 0
    report = json.loads((tmp_path / "migration-cutover.json").read_text())
    assert report["known_failure"] is None
    assert "KNOWN FAILURE" not in capsys.readouterr().out


def _with_known_failure(monkeypatch, marker="fix/some-branch"):
    """The marker mechanism, on the cutover scenario (no real scenario carries one now)."""
    scenarios = tuple(dataclasses.replace(s, known_failure=marker) if s.key == "migration-cutover" else s
                      for s in driver.SCENARIOS)
    monkeypatch.setattr(driver, "SCENARIOS", scenarios)
    return marker


def test_a_known_failure_marker_is_visible_and_still_fails(tmp_path, live, monkeypatch, capsys):
    marker = _with_known_failure(monkeypatch)
    live(refuse=(_first_rehearse_refused(), "engine migration order failure\n"))
    assert driver.main(live_args(tmp_path, "migration-cutover"), snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports/migration-cutover.json").read_text())
    assert report["known_failure"] == marker and report["status"] == f"ERROR (expected until {marker})"
    assert driver.main(["--cloud", "aws", "--repo", str(ROOT), "--dry-run",
        "--scenarios", "migration-cutover", "--reports", str(tmp_path / "dry")]) == 0
    assert json.loads((tmp_path / "dry/migration-cutover.json").read_text())["known_failure"] == marker
    assert f"KNOWN FAILURE until {marker}" in capsys.readouterr().out


def test_a_known_failure_that_passes_fails_the_run(tmp_path, live, monkeypatch):
    marker = _with_known_failure(monkeypatch)
    live(staged_release=True)
    assert driver.main(live_args(tmp_path, "migration-cutover"), snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports/migration-cutover.json").read_text())
    assert report["status"] == f"UNEXPECTED PASS — remove known_failure {marker}"


def test_d10_s14_is_blocked_unless_dev_is_governance_only(tmp_path):
    cloud = tmp_path / "aws"; env = cloud / "envs/dev"; env.mkdir(parents=True)
    (env / "env.auto.tfvars").write_text('governance_mode = "deterministic"\ngenie_spaces = []\n')
    assert driver.governance_only_env(cloud, "dev")
    (env / "agents").mkdir(); (env / "agents/a.auto.tfvars").write_text("")
    assert not driver.governance_only_env(cloud, "dev")


def test_d11_s5_releases_before_maintain_and_commits_scaffold():
    targets = [s.target or s.action for s in next(s for s in driver.SCENARIOS if s.key == "s5").steps]
    assert targets == ["release-plan", "release", "maintain", "scaffold-treatments", "local_merge"]


def test_d12_readme_prepares_both_environments():
    text = (ROOT / "shared/scripts/e2e/README.md").read_text()
    assert "make setup ENV=dev GITIGNORE=customer" in text
    assert "make setup ENV=prod GITIGNORE=customer" in text


def test_phase_bound_is_an_error_after_eight_releases(tmp_path, live):
    live(staged_releases=8)
    assert driver.main(live_args(tmp_path, "migration-cutover"), snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports/migration-cutover.json").read_text())
    assert report["status"] == "ERROR"
    assert "release exceeded 8 phases" in report["error"]


def test_unbounded_restore_is_restore_failed(tmp_path, live):
    live(staged_releases=100)
    assert driver.main(live_args(tmp_path, "s4"), snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports/s4.json").read_text())
    assert report["status"] == "RESTORE FAILED"
    assert "restore exceeded 8 release phases" in report["error"]


def test_restore_runs_n_fresh_plan_release_pairs(tmp_path, live):
    calls = live(staged_releases=2)
    assert driver.main(live_args(tmp_path, "s4"), snapshot_factory=FakeSnapshots) == 0
    restore = [argv for argv in makes(calls) if argv[1] in ("release-plan", "release")]
    assert [argv[1] for argv in restore] == ["release-plan", "release"] * 3
    manifests = [next(a for a in argv if a.startswith("MANIFEST=")) for argv in restore if argv[1] == "release"]
    assert manifests == [f"MANIFEST=/tmp/approved-{i}.json" for i in range(1, 4)]


def test_successful_release_plan_that_drops_tag_is_error(tmp_path, live):
    state = {"weak": False}
    def on_make(argv):
        if argv[1] == "release-plan" and not state["weak"]: state["weak"] = True
        elif argv[1] == "rehearse": state["weak"] = False
    class Dynamic(FakeSnapshots):
        def capture(self, env): return snapshot(tags=() if state["weak"] and env == "prod" else (("t", "ssn", "class.ssn"),))
    live(on_make=on_make)
    assert driver.main(live_args(tmp_path, "s5", "--value", "treatment=reviewed"), snapshot_factory=Dynamic) == 1
    report = json.loads((tmp_path / "reports/s5.json").read_text())
    assert report["steps"][0]["assertion"] == "successful release-plan changed the snapshot"


def test_successful_apply_that_weakens_is_error(tmp_path, live):
    state = {"weak": False, "first": True}
    def on_make(argv):
        if argv[1] == "maintain" and state["first"]:
            state.update(weak=True, first=False)
        elif argv[1] == "rehearse": state["weak"] = False
    class Dynamic(FakeSnapshots):
        def capture(self, env): return snapshot(masks=() if state["weak"] and env == "prod" else ("mask_ssn",))
    live(on_make=on_make)
    assert driver.main(live_args(tmp_path, "s4"), snapshot_factory=Dynamic) == 1
    report = json.loads((tmp_path / "reports/s4.json").read_text())
    assert "successful apply weakened protection" in report["steps"][0]["assertion"]


def test_migrate_plan_exit_one_with_report_is_ok_but_exit_two_is_error(tmp_path, live):
    live(migrate_rc=1)
    assert driver.main(live_args(tmp_path, "migration"), snapshot_factory=FakeSnapshots) == 0
    assert json.loads((tmp_path / "reports/migration.json").read_text())["status"] == "passed"

    other = tmp_path / "other"; calls = []
    # A second isolated fake avoids sharing the live fixture's call log only;
    # the runner itself is fully reconstructed by main.
    live(migrate_rc=2)
    assert driver.main(live_args(other, "migration"), snapshot_factory=FakeSnapshots) == 1
    assert json.loads((other / "reports/migration.json").read_text())["status"] == "ERROR"


def test_deterministic_apply_log_that_clears_gr_treatment_is_error(tmp_path, live):
    checkout = tmp_path / "checkout/aws/envs/prod"; checkout.mkdir(parents=True)
    (checkout / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    live(import_unset=True)
    assert driver.main(live_args(tmp_path, "s4"), snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports/s4.json").read_text())
    assert report["steps"][0]["assertion"] == "deterministic apply attempted to clear gr_treatment"


def test_import_unset_log_guard_is_anchored_to_import_script_literals():
    assert driver.import_unset_source_anchored(ROOT)
    source = (ROOT / "shared/scripts/import_existing.sh").read_text()
    assert all(anchor in source for anchor in driver.IMPORT_UNSET_SOURCE_ANCHORS)
    assert driver.import_unset_treatment("  Cleared tag gr_treatment on cat.s.t.ssn")
    assert driver.import_unset_treatment("lookup failed for x/gr_treatment: boom; clearing tag")


def test_another_phase_signal_accepts_needs_form():
    assert driver.release_needs_another_phase("adoption needs another release-plan")


# --- Typed, evaluated checks -------------------------------------------------------

def ctx(before=None, after=None, cloud_root=Path("/nonexistent"), **kwargs):
    return driver.CheckContext(before or {}, after or {}, cloud_root, **kwargs)


def agent(name, space_id, export=None, acl=None):
    return {"name": name, "space_id": space_id, "export": export or {"title": name}, "acl": acl or {}}


def with_agents(*agents, grants=()):
    snap = snapshot(grants=grants)
    snap["agents"] = list(agents)
    return snap


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_every_real_scenario_check_has_an_evaluator_and_dry_run_passes(tmp_path, cloud):
    assert all(driver.check_problems(s) == [] for s in driver.SCENARIOS)
    assert driver.main(["--cloud", cloud, "--repo", str(ROOT), "--dry-run", "--reports", str(tmp_path)]) == 0
    assert set(driver.EVALUATORS) == {driver.GrantDelta, driver.AccessLevel, driver.AccessVerified,
                                      driver.DiffLimitedTo, driver.Unchanged, driver.AgentIdUnchanged}


def test_dry_run_fails_on_a_free_text_assertion(tmp_path, monkeypatch, capsys):
    s1 = next(s for s in driver.SCENARIOS if s.key == "s1")
    steps = (dataclasses.replace(s1.steps[0], assertions=("Only A changes",)), *s1.steps[1:])
    monkeypatch.setattr(driver, "SCENARIOS", (dataclasses.replace(s1, steps=steps),))
    assert driver.main(["--cloud", "aws", "--repo", str(ROOT), "--dry-run", "--scenarios", "s1",
                        "--reports", str(tmp_path)]) == 1
    report = json.loads((tmp_path / "s1.json").read_text())
    assert report["status"] == "ERROR" and "has no evaluator" in report["check_problems"][0]
    assert "'Only A changes' has no evaluator" in capsys.readouterr().out


def test_dry_run_fails_on_an_unknown_check_type_at_scenario_level(tmp_path, monkeypatch):
    @dataclasses.dataclass(frozen=True)
    class ConversationsRetained:  # typed, but nothing evaluates it
        env: str = "prod"
        kind = "conversations-retained"
    s16 = next(s for s in driver.SCENARIOS if s.key == "s16")
    monkeypatch.setattr(driver, "SCENARIOS", (dataclasses.replace(s16, checks=(ConversationsRetained(),)),))
    assert driver.main(["--cloud", "aws", "--repo", str(ROOT), "--dry-run", "--scenarios", "s16",
                        "--reports", str(tmp_path)]) == 1


@pytest.mark.parametrize("check, problem", [
    (driver.GrantDelta("prod", added=(("*", "SELECT", "cat.s.t"),)), "exactly"),
    (driver.GrantDelta("prod", may_add=(("g", "SELECT", "cat.s.*"),)), "exactly"),
    (driver.GrantDelta("prod", added=(("g", "SELECT"),)), "(principal, privilege, securable)"),
    (driver.GrantDelta("qa"), "env must be"),
    (driver.DiffLimitedTo("prod", "agents", ("*",)), "limits nothing"),
    (driver.Unchanged("prod", "grants"), "component must be"),
    (driver.AccessLevel("prod", "cat.s.t.c", "*", "hidden"), "level must be"),
    (driver.AgentIdUnchanged("prod", ""), "agent key"),
])
def test_malformed_checks_are_refused_before_anything_runs(tmp_path, live, monkeypatch, check, problem):
    s4 = next(s for s in driver.SCENARIOS if s.key == "s4")
    monkeypatch.setattr(driver, "SCENARIOS", (dataclasses.replace(s4, checks=(check,)),))
    assert problem in driver.check_problems(driver.SCENARIOS[0])[0]
    calls = live()
    assert driver.main(live_args(tmp_path, "s4"), snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports/s4.json").read_text())
    assert report["status"] == "ERROR" and problem in report["error"]
    assert not makes(calls)


def test_check_placeholders_are_required_values_before_anything_runs(tmp_path, live):
    s6 = next(s for s in driver.SCENARIOS if s.key == "s6")
    assert {"reader_principal", "reader_table"} <= set(driver.unresolved_values(s6, {}))
    assert driver.unresolved_values(s6, driver.derived_values({"ack_entry": "prod:reader:cat.s.t:g"})) == []
    calls = live()
    args = live_args(tmp_path, "s4")
    at = args.index("new_column=cat.s.t.ssn"); del args[at - 1:at + 1]
    assert driver.main(args, snapshot_factory=FakeSnapshots) == 1
    assert "new_column" in json.loads((tmp_path / "reports/s4.json").read_text())["error"]
    assert not makes(calls)


def test_reader_ack_is_the_s6_grant_allowance():
    assert driver.derived_values({"ack_entry": "prod:reader:cat.s.t:account users"}) == {
        "ack_entry": "prod:reader:cat.s.t:account users", "reader_table": "cat.s.t",
        "reader_principal": "account users"}
    # A non-reader ack names no grant, so S6's reader grant stays unresolved.
    assert "reader_table" not in driver.derived_values({"ack_entry": "prod:weaken:cat.s.t.c:etl"})


# GrantDelta and grant widening (gap 1).
def test_grant_delta_exact_added_and_may_add():
    check = driver.GrantDelta("prod", added=(("g", "SELECT", "cat.s.t"),),
                              may_add=(("g", "USE_SCHEMA", "cat.s.t"),))
    base = {"prod": snapshot()}
    assert driver._eval_grant_delta(check, ctx(base, {"prod": snapshot(grants=("g",))})) == []
    with_schema = snapshot(grants=("g",))
    with_schema["uc"]["cat"]["schema_grants"] = [
        {"grantee": "g", "privilege_type": "USE SCHEMA", "catalog_name": "cat", "schema_name": "s"}]
    assert driver._eval_grant_delta(check, ctx(base, {"prod": with_schema})) == []


@pytest.mark.parametrize("after_grants, expected", [
    ((), "expected grant(s) not newly added: g SELECT cat.s.t"),
    (("g", "eve"), "unexpected grant(s) added: eve SELECT cat.s.t"),
])
def test_grant_delta_deliberate_breaks(after_grants, expected):
    check = driver.GrantDelta("prod", added=(("g", "SELECT", "cat.s.t"),))
    problems = driver._eval_grant_delta(check, ctx({"prod": snapshot()}, {"prod": snapshot(grants=after_grants)}))
    assert any(expected in p for p in problems)


def test_empty_grant_delta_refuses_revocation_and_widening():
    check = driver.GrantDelta("prod")
    revoked = driver._eval_grant_delta(check, ctx({"prod": snapshot(grants=("g",))}, {"prod": snapshot()}))
    assert revoked == ["prod: unexpected grant(s) revoked: g SELECT cat.s.t"]
    assert driver._eval_grant_delta(check, ctx({"prod": snapshot()}, {"prod": snapshot()})) == []


def test_grant_delta_lists_expand_and_agent_securable_is_can_run():
    check = driver.GrantDelta("prod", added=(("a1,a2", "CAN_RUN", "agent:a"),))
    acl = {"access_control_list": [
        {"group_name": g, "all_permissions": [{"permission_level": "CAN_RUN", "inherited": False}]}
        for g in ("a1", "a2")]}
    before = {"prod": with_agents(agent("a.auto", "space-a"))}
    after = {"prod": with_agents(agent("a.auto", "space-a", acl=acl))}
    assert driver._eval_grant_delta(check, ctx(before, after)) == []
    one = {"prod": with_agents(agent("a.auto", "space-a", acl={"access_control_list": acl["access_control_list"][:1]}))}
    assert "a2 CAN RUN genie:space-a" in driver._eval_grant_delta(check, ctx(before, one))[0]


def test_inherited_can_run_is_not_a_grant_and_new_can_run_is_widening():
    inherited = {"access_control_list": [
        {"group_name": "admins", "all_permissions": [{"permission_level": "CAN_RUN", "inherited": True}]}]}
    base = with_agents(agent("a", "space-a"))
    assert driver.protection_diff(base, with_agents(agent("a", "space-a", acl=inherited)), include_grants=True) == {}
    widened = driver.protection_diff(base, with_agents(agent("a", "space-a", acl=CAN_RUN_ACL)), include_grants=True)
    assert widened == {"widened_grants": ["analysts CAN_RUN genie:space-a"]}


def test_allowed_grants_permit_only_the_named_grant():
    widened = driver.protection_diff(snapshot(), snapshot(grants=("g", "eve")), include_grants=True,
                                     allowed_grants={("g", "SELECT", "cat.s.t")})
    assert widened == {"widened_grants": ["eve SELECT cat.s.t"]}


def test_weakening_scenarios_are_still_held_to_their_grant_allowance():
    s8 = next(s for s in driver.SCENARIOS if s.key == "s8")
    before, after = {"prod": snapshot()}, {"prod": snapshot(masks=(), grants=("eve",))}
    assert driver.live_protection_diff(s8, before, after, allowed_adoptions={}, allowed_grants={}) == {
        "prod": {"widened_grants": ["eve SELECT cat.s.t"]}}
    assert driver.live_protection_diff(s8, before, {"prod": snapshot(masks=())},
                                       allowed_adoptions={}, allowed_grants={}) == {}


def test_successful_apply_that_widens_a_grant_fails_the_step(tmp_path, live):
    state = {"widened": False}
    def on_make(argv):
        if argv[1] == "maintain" and not state.get("done"): state.update(widened=True, done=True)
        elif argv[1] == "rehearse": state["widened"] = False
    class Dynamic(FakeSnapshots):
        def capture(self, env): return snapshot(grants=("eve",) if state["widened"] and env == "prod" else ())
    live(on_make=on_make)
    assert driver.main(live_args(tmp_path, "s4"), snapshot_factory=Dynamic) == 1
    report = json.loads((tmp_path / "reports/s4.json").read_text())
    assert report["steps"][0]["status"] == "ERROR"
    assert "widened_grants" in report["steps"][0]["assertion"] and "eve SELECT cat.s.t" in report["steps"][0]["assertion"]


def test_simulated_s6_widening_to_another_principal_fails(sim):
    class WrongReader(ReleasedSnapshots):
        def capture(self, env):
            snap = super().capture(env)
            return snapshot(grants=("h",)) if snap["uc"]["cat"]["table_grants"] else snap
    (sim.state / "widening").write_text("")
    rc = driver.main([*sim.args, "--scenarios", "s6", "--value", "ack_entry=prod:reader:cat.s.t:g"],
                     snapshot_factory=WrongReader)
    report = json.loads((sim.reports / "s6.json").read_text())
    assert rc == 1 and report["status"] == "ERROR"
    step = next(item for item in report["steps"] if item["name"] == driver.APPLY_RELEASE)
    assert "h SELECT cat.s.t" in step["assertion"]


# AccessLevel / AccessVerified on verify-access results (gap 2).
def access_ctx(tmp_path, payload, env="prod", *, stale=False):
    write_verify_result(tmp_path, [f"ENV={env}"], payload)
    before = driver.verify_signatures(tmp_path) if stale else {}
    return ctx(cloud_root=tmp_path, verify_before=before)


def test_access_level_redacted_for_every_principal(tmp_path):
    check = driver.AccessLevel("prod", "cat.s.t.cvv", "*", "redacted")
    assert driver._eval_access_level(check, access_ctx(tmp_path, VERIFY_PASS)) == []
    # Full but through a raw-capable treatment is not redaction.
    partial = driver.AccessLevel("prod", "cat.s.t.ssn", "analysts", "redacted")
    assert "not redacted" in driver._eval_access_level(partial, access_ctx(tmp_path, VERIFY_PASS))[0]


@pytest.mark.parametrize("observed", ["raw", "mismatch", "unproven"])
def test_access_level_deliberate_break_wrong_tier(tmp_path, observed):
    payload = {"checks": [verify_entry("cvv", {"analysts": ("full", observed), "__admin__": ("full", "full")},
                                       status="FAIL", full_function="cat.s.gr_mask_unmapped_redact__string__22222222_full")]}
    problems = driver._eval_access_level(driver.AccessLevel("prod", "cat.s.t.cvv", "*", "redacted"),
                                         access_ctx(tmp_path, payload))
    assert problems == [f"column-mask cat.s.t.cvv: not redacted for {{'analysts': '{observed}'}}"]


def test_access_level_for_a_named_principal_and_masked(tmp_path):
    assert driver._eval_access_level(driver.AccessLevel("prod", "cat.s.t.ssn", "analysts", "partial"),
                                     access_ctx(tmp_path, VERIFY_PASS)) == []
    assert driver._eval_access_level(driver.AccessLevel("prod", "cat.s.t.ssn", "analysts", "masked"),
                                     access_ctx(tmp_path, VERIFY_PASS)) == []
    assert driver._eval_access_level(driver.AccessLevel("prod", "cat.s.t.ssn", "__admin__", "masked"),
                                     access_ctx(tmp_path, VERIFY_PASS))
    assert "no principal 'nobody'" in driver._eval_access_level(
        driver.AccessLevel("prod", "cat.s.t.ssn", "nobody", "raw"), access_ctx(tmp_path, VERIFY_PASS))[0]


@pytest.mark.parametrize("payload, stale, expected", [
    (VERIFY_PASS, True, "not rewritten during this span (stale)"),
    ({"passed": True, "masked_tables": ["cat.s.t"]}, False, "no per-check access results"),
    ({"checks": [VERIFY_PASS["checks"][1]]}, False, "did not check cat.s.t.ssn"),
    (None, False, "no verify-access result"),
])
def test_access_verified_deliberate_breaks(tmp_path, payload, stale, expected):
    check = driver.AccessVerified("prod", "cat.s.t.ssn")
    context = access_ctx(tmp_path, payload, stale=stale) if payload is not None else ctx(cloud_root=tmp_path)
    assert expected in driver._eval_access_verified(check, context)[0]


def test_access_verified_requires_pass_and_every_expected_tier(tmp_path):
    check = driver.AccessVerified("prod", "cat.s.t")
    assert driver._eval_access_verified(check, access_ctx(tmp_path, VERIFY_PASS)) == []
    inconclusive = {"checks": [verify_entry("ssn", {"analysts": ("partial", "unproven")}, status="INCONCLUSIVE")]}
    problems = driver._eval_access_verified(check, access_ctx(tmp_path, inconclusive))
    assert problems[0].endswith("verify-access status INCONCLUSIVE") and "unproven (expected partial)" in problems[1]


def test_access_verified_adopted_columns_come_from_migrate_plan(tmp_path):
    adoption = driver.Adoption("masked_ssn", "email_partial__string__11111111", "mask_masked_ssn")
    check = driver.AccessVerified("prod", driver.ADOPTED_COLUMNS)
    write_verify_result(tmp_path, ["ENV=prod"], VERIFY_PASS)
    assert driver._eval_access_verified(check, ctx(cloud_root=tmp_path, adoptions={"prod": {"cat.s.t.ssn": adoption}})) == []
    other = ctx(cloud_root=tmp_path, adoptions={"prod": {"cat.s.u.ssn": adoption}})
    assert "did not check cat.s.u.ssn" in driver._eval_access_verified(check, other)[0]
    assert "no migrate-plan adoption" in driver._eval_access_verified(check, ctx(cloud_root=tmp_path))[0]


def test_s5_release_must_prove_redaction_for_every_principal(tmp_path, live):
    leaked = {"checks": [verify_entry("cvv", {"analysts": ("full", "raw")}, status="FAIL",
                                      full_function="cat.s.gr_mask_unmapped_redact__string__22222222_full")]}
    live(verify_result=leaked)
    assert driver.main(live_args(tmp_path, "s5", "--value", "treatment=reviewed"), snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports/s5.json").read_text())
    step = report["steps"][1]
    assert step["status"] == "ERROR" and "not redacted for {'analysts': 'raw'}" in step["assertion"]
    other = tmp_path / "ok"
    live()
    assert driver.main(live_args(other, "s5", "--value", "treatment=reviewed"), snapshot_factory=FakeSnapshots) == 0
    passed = json.loads((other / "reports/s5.json").read_text())
    assert passed["steps"][1]["checks"][0]["problems"] == []
    assert [c["problems"] for c in passed["checks"]] == [[], []]


def test_s4_verify_step_fails_when_the_new_column_was_not_checked(tmp_path, live):
    live(verify_result={"checks": [VERIFY_PASS["checks"][1]]})
    assert driver.main(live_args(tmp_path, "s4"), snapshot_factory=FakeSnapshots) == 1
    report = json.loads((tmp_path / "reports/s4.json").read_text())
    assert report["steps"][1]["command"] == "make verify-access ENV=prod"
    assert "did not check cat.s.t.ssn" in report["steps"][1]["assertion"]


def test_s4_verify_step_fails_when_verify_access_wrote_nothing(tmp_path, live):
    stale = tmp_path / "checkout/aws"
    write_verify_result(stale, ["ENV=prod"], VERIFY_PASS)
    live(verify_result=None)
    assert driver.main(live_args(tmp_path, "s4"), snapshot_factory=FakeSnapshots) == 1
    assert "stale" in json.loads((tmp_path / "reports/s4.json").read_text())["steps"][1]["assertion"]


# DiffLimitedTo / Unchanged / AgentIdUnchanged (gap 3).
def test_diff_limited_to_agent_a_passes_and_b_change_fails():
    before = {"prod": with_agents(agent("a.auto", "space-a"), agent("b.auto", "space-b"))}
    a_changed = {"prod": with_agents(agent("a.auto", "space-a", {"title": "new"}), agent("b.auto", "space-b"))}
    b_changed = {"prod": with_agents(agent("a.auto", "space-a"), agent("b.auto", "space-b", {"title": "ui"}))}
    check = driver.DiffLimitedTo("prod", "agents", ("a",))
    assert driver._eval_diff_limited_to(check, ctx(before, a_changed)) == []
    assert driver._eval_diff_limited_to(check, ctx(before, b_changed)) == [
        "prod agents changed outside ['a']: space-b"]


def test_one_space_named_by_several_sources_is_one_resource():
    before = {"prod": with_agents(agent("a.auto", "space-a"), agent("genie_space", "space-a", {"title": "a.auto"}))}
    detached = {"prod": with_agents(agent("a.auto", "space-a"))}
    assert driver._eval_unchanged(driver.Unchanged("prod", "agents"), ctx(before, detached)) == []


def test_uc_selectors_cover_resources_under_them():
    before = {"prod": snapshot()}
    after = {"prod": snapshot(tags=(("t", "ssn", "class.email"),))}
    assert driver._eval_diff_limited_to(driver.DiffLimitedTo("prod", "uc", ("cat.s.t",)), ctx(before, after)) == []
    assert driver._eval_diff_limited_to(driver.DiffLimitedTo("prod", "uc", ("cat.s.u",)), ctx(before, after)) == [
        "prod uc changed outside ['cat.s.u']: cat.s.t.ssn"]
    assert driver._eval_unchanged(driver.Unchanged("prod", "uc", ("cat.s.*",)), ctx(before, after))


def test_unchanged_receipts_and_state_paths():
    before = {"prod": {"state_serials": {"a.tfstate": 1, "b.tfstate": 4}, "receipts": []}}
    after = {"prod": {"state_serials": {"a.tfstate": 2, "b.tfstate": 4},
                      "receipts": [{"path": "r1.json", "sha256": "x", "bytes": 1}]}}
    assert driver._eval_unchanged(driver.Unchanged("prod", "state_serials", ("b.tfstate",)), ctx(before, after)) == []
    assert driver._eval_unchanged(driver.Unchanged("prod", "state_serials"), ctx(before, after)) == [
        "prod state_serials changed: a.tfstate"]
    assert driver._eval_diff_limited_to(driver.DiffLimitedTo("prod", "receipts", ("r*.json",)), ctx(before, after)) == []


@pytest.mark.parametrize("after, expected", [
    ([agent("a.auto", "space-a", {"title": "overwritten"})], []),
    ([agent("a.auto", "space-new")], ["prod agent 'a' space ID ['space-a'] -> ['space-new']: recreated or lost"]),
    ([], ["prod agent 'a' space ID ['space-a'] -> []: recreated or lost"]),
])
def test_agent_id_unchanged(after, expected):
    before = {"prod": with_agents(agent("a.auto", "space-a"))}
    assert driver._eval_agent_id_unchanged(driver.AgentIdUnchanged("prod", "a"),
                                           ctx(before, {"prod": with_agents(*after)})) == expected


def test_agent_id_unknown_at_baseline_is_unproven_not_a_pass():
    problems = driver._eval_agent_id_unchanged(driver.AgentIdUnchanged("prod", "a"),
                                               ctx({"prod": with_agents()}, {"prod": with_agents()}))
    assert "unproven" in problems[0]


def test_s4_genie_change_fails_through_its_unchanged_check(tmp_path, live):
    class AgentDrift(FakeSnapshots):
        def capture(self, env):
            self.n += 1
            return with_agents(agent("a", "space-a", {"v": int(self.n > 2)})) if env == "prod" else snapshot()
    live()
    assert driver.main(live_args(tmp_path, "s4"), snapshot_factory=AgentDrift) == 1
    report = json.loads((tmp_path / "reports/s4.json").read_text())
    assert "unchanged(env='prod', component='agents'" in report["error"] and "space-a" in report["error"]
