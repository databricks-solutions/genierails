"""A first deterministic deploy through verify-switched, with verify-access answered as Unity Catalog would.

The live AWS shape (dev-2, run 21fce9f7649e, after #222): make rehearse ENV=dev
passed create, verify-created and switch, then verify-switched failed: every
tier test principal got ``INSUFFICIENT_PERMISSIONS: User does not have USE
CATALOG on Catalog 'genierails_dev_2'``. #222 holds the tier groups' new
USE_CATALOG/USE_SCHEMA (with SELECT) until the grant phase, but verify-access
reads as temporary test principals that only join a tier group; only the
out-of-tier probe got temporary access to the checked tables. The restore then
changed nothing (a first deploy has no previous protection, so it keeps the new
masks and tags), and make policy-status called that "the previous protection
(none), restored".

Now verify-created and verify-switched run verify-access with
GENIERAILS_VERIFY_PRE_GRANT=1: each temporary test principal itself (never a
tier group) gets USE CATALOG/USE SCHEMA/SELECT on the tables it checks,
revoked when verify-access exits, also when it fails or is interrupted. The
status says what the restore left live.

These tests run the real controller, hooks and live policy check on the fake
layer of tests/test_staged_policy_apply.py with the business grants on, and
the real verify_effective_access CLI with a verifier whose SQL goes to a fake
Unity Catalog on that state: a principal reads a table only with USE CATALOG,
USE SCHEMA and SELECT through a tier group's applied grant or its own direct
grant, and gets the mask the live policies give its group memberships.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

import verify_effective_access as vea
from scripts import staged_policy_apply as spa
from tests import test_staged_policy_apply as staged
from tests.test_first_deploy_create import BUSINESS, DEPLOYER_GRANT, TIERS, Recording, _refresh, fresh
from tests.test_policy_abandon_coverage import OLD_SP, _promoted
from tests.test_policy_rebind import grant_keys
from tests.test_staged_policy_apply import _value_of, live_policies, live_state, live_tags, write_env

CI_MARKERS = ("CI", "TF_BUILD", "JENKINS_URL", "GITLAB_CI", "BUILDKITE", "CIRCLECI", "GENIERAILS_ALLOW_CI_APPLY")
MIXED_DDL = "CREATE TABLE cat.sch.t (\n  email STRING,\n  notes STRING\n);\n"
# A free-text column with several classes whose partial treatments differ: deterministic redact (#223).
MULTI_CLASS = ["email_address", "us_ssn", "phone_number"]
PRINCIPALS = {f"app-{t}" for t in TIERS + (vea.OUT_OF_TIER_PRINCIPAL, vea.DUAL_TIER_PRINCIPAL)}
FIRST_DEPLOY = ["create", "inventory:create/restore", "switch", "inventory:switch", "verify-access:switch", "grant"]
ADMIN = OLD_SP  # the deployer: verify-access's admin baseline, excepted from every mask
KEYS = range(6)
STATEMENT = re.compile(r"^(GRANT|REVOKE) (SELECT|USE SCHEMA|USE CATALOG) ON (TABLE|SCHEMA|CATALOG) (\S+) (TO|FROM) "
                       r"`([^`]+)`$")


class FakeUnityCatalog:
    """What verify-access's principals can read on the fake data_access state, and the direct grants it made."""

    def __init__(self, env_dir: Path):
        self.env_dir = env_dir
        self.direct: set[tuple[str, str, str]] = set()  # (grantee, privilege, securable)
        self.memberships: dict[str, tuple[str, ...]] = {ADMIN: (ADMIN,)}
        self.statements: list[str] = []
        self.grantees_while_reading: set[str] = set()
        self.seen: dict[tuple[str, str], str] = {}  # (tier, column) -> the mask it got ("raw" when none)
        self.leak: set[str] = set()  # tiers whose mask the fake drops (a broken policy)
        self.interrupt = ""  # tier whose query is interrupted (Ctrl-C)
        self.killed = False  # the verify-access process dies before its cleanup: its REVOKEs never run

    def execute(self, sql: str) -> None:
        self.statements.append(sql)
        verb, privilege, _, securable, _, grantee = STATEMENT.match(sql).groups()
        entry = (grantee, privilege, securable.replace("`", ""))
        if verb == "GRANT":
            self.direct.add(entry)
        elif not self.killed:
            self.direct.discard(entry)

    def _can(self, app: str, privilege: str, securable: str) -> bool:
        if app == ADMIN or (app, privilege, securable) in self.direct:
            return True
        kind = {"USE CATALOG": "catalog_access", "USE SCHEMA": "schema_access", "SELECT": "table_access"}[privilege]
        applied = grant_keys(live_state(self.env_dir))
        return any(f"{kind}|{securable}|{group}" in applied for group in self.memberships[app])

    def read(self, app: str, tier: str, table: str, column: str, keys) -> list[tuple[int, str]]:
        self.grantees_while_reading |= {grantee for grantee, _, _ in self.direct}
        if tier == self.interrupt:
            raise KeyboardInterrupt("interrupted")
        catalog, schema, _ = table.split(".")
        for privilege, securable, kind in (("USE CATALOG", catalog, "Catalog"),
                                           ("USE SCHEMA", f"{catalog}.{schema}", "Schema"),
                                           ("SELECT", table, "Table")):
            if not self._can(app, privilege, securable):
                raise RuntimeError(f"[INSUFFICIENT_PERMISSIONS] User does not have {privilege} on {kind} "
                                   f"'{securable}'. SQLSTATE: 42501")
        state = live_state(self.env_dir)
        value = live_tags(state).get(f"{table}.{column}")
        groups = set(self.memberships[app])
        masks = [p for p in live_policies(state) if value and _value_of(p) == value
                 and ("account users" in p["to_principals"] or groups & set(p["to_principals"]))
                 and not groups & set(p["except_principals"])]
        if len(masks) > 1:
            raise RuntimeError("more than one mask applies")
        function = masks[0]["column_mask"]["function_name"] if masks and tier not in self.leak else ""
        self.seen[(tier, column)] = function.rsplit("_", 1)[1] if function else "raw"
        return [(k, f"{function}({column}-{k})" if function else f"{column}-{k}") for k in keys]


def verifier_on(uc: FakeUnityCatalog):
    class Verifier(vea.EffectiveAccessVerifier):
        """The real verifier's grant SQL; queries and provisioning answered by the fake Unity Catalog."""

        def __init__(self, auth, warehouse_id="", **kwargs):
            super().__init__(auth, warehouse_id, **kwargs)
            self._admin_ws = object()

        def run_query(self, ws, sql, parameters=None):
            uc.execute(sql)
            return []

        def resolve_warehouse(self):
            return "wh"

        def resolve_principal_groups(self, memberships):
            return list(memberships)

        def provision_principal(self, tier, memberships=None, *, resolved_groups=None):
            principal = vea.TestPrincipal(tier, f"genierails-verify-{tier}", f"app-{tier}", "secret", f"sp-{tier}")
            uc.memberships[principal.application_id] = tuple(resolved_groups or ())
            return principal

        def grant_query_entitlement(self, principal):
            pass

        def grant_warehouse_use(self, principal):
            pass

        def assert_query_capable(self, principal):
            pass

        def deprovision_principal(self, principal):
            uc.memberships.pop(principal.application_id, None)

        def key_mask_metadata(self, principal, check):
            return []

        def collect_column_values(self, principal, check, limit=25, keys=None, salt=None):
            return uc.read(principal.application_id, principal.tier, check.table, check.column,
                           list(KEYS) if keys is None else keys)

        def prove_key_unique(self, principal, check, keys):
            return ""

        def count_rows_with_keys(self, principal, check, keys):
            return len(keys)

        def expected_tier_values(self, principal, check, keys):
            return {"raw": [(k, f"{check.column}-{k}") for k in keys],
                    "partial": [(k, f"{check.partial_function}({check.column}-{k})") for k in keys],
                    "full": [(k, f"{check.full_function}({check.column}-{k})") for k in keys]}

        def table_policies(self, principal, table):
            return []

        def fixed_point_problem(self, principal, check, policies):
            return "the fake proves no fixed point"

    return Verifier


class LiveVerify(Recording):
    """The first-deploy harness, verify-access run through the real CLI against the fake Unity Catalog."""

    def __init__(self, env_dir, monkeypatch, tmp_path):
        super().__init__(env_dir, monkeypatch, tmp_path)
        self.uc = FakeUnityCatalog(env_dir)
        self.auth = tmp_path / "auth.auto.tfvars"
        self.verifies: list[dict] = []
        monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
        monkeypatch.setenv("GENIERAILS_VERIFY_PROPAGATION_SLEEP", "0")
        monkeypatch.setenv("GENIERAILS_VERIFY_PROPAGATION_TIMEOUT", "0")
        monkeypatch.setattr(vea, "EffectiveAccessVerifier", verifier_on(self.uc))
        monkeypatch.setattr(vea, "load_auth", lambda _path: {"client_id": ADMIN, "client_secret": "s"})
        monkeypatch.setattr(vea, "pick_pairing_keys", lambda _v, _p, checks, **_kw: {
            c.table.lower(): vea.KeyPick(c.table, "id", "explicit") for c in checks})

    def run(self, argv, env=None, check=False):
        if argv[:2] != ["/bin/sh", "-c"]:
            return super().run(argv, env, check)
        stage = spa.pg.load_stage_record(self.env_dir)["stage"]
        self.calls.append(f"verify-access:{stage}")
        self.uc.grantees_while_reading = set()
        self.uc.seen = {}
        before = grant_keys(live_state(self.env_dir))
        # What make's verify-access recipe passes (_VERIFY_PRE_GRANT_FLAG from VERIFY_PRE_GRANT).
        cli = ["--from-tfvars", env["GENIERAILS_VERIFY_TFVARS"], "--env-file", env["GENIERAILS_VERIFY_ENV_FILE"],
               "--key-column", "id", "--require-mask-checks", "--gate", "--auth-file", str(self.auth), "--live"]
        if env.get("GENIERAILS_VERIFY_PRE_GRANT") == "1":
            cli.append("--pre-grant-access")
        try:
            rc = vea.main(cli)
        except KeyboardInterrupt:
            rc = 130  # the shell's exit status for an interrupted verify-access
        self.verifies.append({"killed": self.uc.killed, "stage": stage, "rc": rc, "pre_grant": env.get("GENIERAILS_VERIFY_PRE_GRANT"),
                              "business": before & BUSINESS, "grantees": set(self.uc.grantees_while_reading),
                              "left": set(self.uc.direct), "seen": dict(self.uc.seen)})
        assert grant_keys(live_state(self.env_dir)) == before, "verify-access changed a Terraform grant"
        return staged.SimpleNamespace(returncode=rc)


@pytest.fixture
def harness(tmp_path, monkeypatch):
    for marker in CI_MARKERS:
        monkeypatch.delenv(marker, raising=False)
    env_dir = tmp_path / "envs" / "dev"
    env_dir.mkdir(parents=True)
    h = LiveVerify(env_dir, monkeypatch, tmp_path)
    yield h
    assert not h.invariant_problems, h.invariant_problems
    # Every temporary grant is revoked when verify-access exits, whatever the outcome (short of a kill).
    assert [v["left"] for v in h.verifies if not v.get("killed")] == [set()] * len([
        v for v in h.verifies if not v.get("killed")])


def mixed(h, tmp_path, monkeypatch) -> dict:
    """A fresh env: cat.sch.t.email one class, cat.sch.t.notes a multi-class free-text column (redact)."""
    real = staged.build_model
    monkeypatch.setattr(staged, "build_model", lambda columns, *args, **kwargs: real(
        {c: (MULTI_CLASS if c.endswith(".notes") else classes) for c, classes in columns.items()}, *args, **kwargs))
    gen = fresh(h, tmp_path, monkeypatch, ddl_text=MIXED_DDL, columns=("email", "notes"))
    assert gen["tags"]["cat.sch.t.notes"].startswith("redact__") and gen["tags"]["cat.sch.t.email"].startswith(
        "email")
    return gen


def test_pre_grant_is_set_for_the_verifies_before_grant_only(harness, tmp_path, monkeypatch):
    monkeypatch.setenv("GENIERAILS_VERIFY_PRE_GRANT", "1")  # a stray value never reaches verify-rebound
    assert spa.PRE_GRANT_VERIFY == {"verify-created", "verify-switched"}
    assert {p for p in spa.VERIFY_PHASES if spa.VERIFY_PHASES[p][1]} - spa.PRE_GRANT_VERIFY == {"verify-rebound"}
    gen = fresh(harness, tmp_path, monkeypatch)
    envs = []
    controller = harness.controller()
    controller.verification_inputs = lambda t, stages, **_: (Path("/t"), Path("/e"))
    controller._inventory = lambda t, stages: 0
    controller.run_cmd = lambda argv, env=None, check=False: envs.append(env) or staged.SimpleNamespace(returncode=0)
    t = {"to": gen["fingerprint"], "from": "", "retain": "", "switch_attempted": False, "run_id": "r"}
    for phase in ("verify-created", "verify-switched", "verify-rebound"):
        assert controller._verify(t, phase) == 0
    assert [e["GENIERAILS_VERIFY_PRE_GRANT"] for e in envs] == ["1", "1", ""]


# -- (1) the first deploy, phase by phase -------------------------------------------------------------
def test_a_first_deploy_verifies_every_tier_s_masks_before_any_business_access(harness, tmp_path, monkeypatch,
                                                                                capfd):
    gen = mixed(harness, tmp_path, monkeypatch)
    assert harness.apply() == 0, capfd.readouterr()
    assert harness.calls == FIRST_DEPLOY
    after = harness.after

    # create: policies, functions and the deployer's grant only.
    assert after["create"]["tags"] == {} and after["create"]["grants"] == DEPLOYER_GRANT
    assert after["create"]["policies"] == {p["name"] for p in gen["fgac_policies"]}
    # switch: every column tagged and masked; still no business grant.
    assert after["switch"]["tags"] == gen["tags"] and after["switch"]["grants"] == DEPLOYER_GRANT

    # verify-switched: verify-access passed, every tier read through its own temporary grants, as the
    # test principals (never a tier group), while no business group held anything.
    [verify] = harness.verifies
    assert (verify["stage"], verify["rc"], verify["pre_grant"], verify["business"]) == ("switch", 0, "1", set())
    assert verify["grantees"] == PRINCIPALS and not verify["grantees"] & set(TIERS)
    assert sorted(set(harness.uc.statements)) == sorted(
        f"{verb} {priv} ON {kind} {name} {prep} `{app}`" for app in PRINCIPALS
        for verb, prep in (("GRANT", "TO"), ("REVOKE", "FROM"))
        for priv, kind, name in (("USE CATALOG", "CATALOG", "`cat`"), ("USE SCHEMA", "SCHEMA", "`cat`.`sch`"),
                                 ("SELECT", "TABLE", "`cat`.`sch`.`t`")))
    # Each tier's mask was checked for real: the partial tier gets the redact mask on the multi-class
    # free-text column and the email partial mask on email; the lowest tier and the outsider are fully masked.
    assert verify["seen"] == {
        **{(t, c): "raw" for t in ("raw", vea.DUAL_TIER_PRINCIPAL, vea.DEFAULT_ADMIN_TIER) for c in ("email", "notes")},
        ("regional", "email"): "partial", ("regional", "notes"): "partial",
        ("masked", "email"): "full", ("masked", "notes"): "full",
        (vea.OUT_OF_TIER_PRINCIPAL, "email"): "full", (vea.OUT_OF_TIER_PRINCIPAL, "notes"): "full"}
    regional = {p["name"] for p in live_policies(live_state(harness.env_dir)) if "regional" in p["to_principals"]}
    assert regional == {f"gr_mask_cat_{gen['tags']['cat.sch.t.notes']}_partial",
                        f"gr_mask_cat_{gen['tags']['cat.sch.t.email']}_partial"}

    # grant: only now do the tier groups get USE and SELECT; protection unchanged.
    assert after["grant"]["grants"] == BUSINESS | DEPLOYER_GRANT
    assert (after["grant"]["tags"], after["grant"]["policies"]) == (gen["tags"], after["switch"]["policies"])
    journal = harness.journal()
    assert journal["active"] == gen["fingerprint"] and journal["transition"] is None


def test_a_broken_mask_still_fails_verify_switched_with_the_temporary_access(harness, tmp_path, monkeypatch,
                                                                             capsys):
    """The temporary access must not make verify-access pass: a tier that sees raw still fails it."""
    gen = mixed(harness, tmp_path, monkeypatch)
    harness.uc.leak = {"regional"}
    assert harness.apply() == 1
    [verify] = harness.verifies
    assert (verify["stage"], verify["rc"], verify["business"]) == ("switch", 1, set())
    assert verify["grantees"] == PRINCIPALS and verify["seen"][("regional", "email")] == "raw"
    # Fail-closed: the restore keeps the masks, and no business group got anything.
    assert harness.calls == FIRST_DEPLOY[:-1] + ["restore", "inventory:restore"]
    state = live_state(harness.env_dir)
    assert live_tags(state) == gen["tags"] and grant_keys(state) == DEPLOYER_GRANT


def test_an_interrupted_verify_switched_revokes_the_temporary_access(harness, tmp_path, monkeypatch):
    mixed(harness, tmp_path, monkeypatch)
    harness.uc.interrupt = "masked"
    assert harness.apply() == 1
    [verify] = harness.verifies
    # Interrupted mid-read with every test principal granted; the fixture checks all of it was revoked.
    assert verify["rc"] == 130 and verify["grantees"] == PRINCIPALS
    assert grant_keys(live_state(harness.env_dir)) == DEPLOYER_GRANT
    t = harness.journal()["transition"]
    assert (t["failed_phase"], t["completed"]) == ("verify-switched", ["create"])


# -- (2) the live run's state: failed at verify-switched, restored, masks and tags live -----------------------
def _failed_like_the_live_run(h, tmp_path, monkeypatch, capsys) -> dict:
    """make rehearse before this fix: verify-switched without temporary tier access, then the restore."""
    gen = mixed(h, tmp_path, monkeypatch)
    monkeypatch.setattr(spa, "PRE_GRANT_VERIFY", set(), raising=False)
    assert h.apply() == 1
    h.output = capsys.readouterr()
    assert "User does not have USE CATALOG on Catalog 'cat'" in h.output.out + h.output.err
    monkeypatch.setattr(spa, "PRE_GRANT_VERIFY", {"verify-created", "verify-switched"}, raising=False)
    [verify] = h.verifies
    assert verify["rc"] == 1 and verify["grantees"] == {f"app-{vea.OUT_OF_TIER_PRINCIPAL}"}
    t = h.journal()["transition"]
    assert (t["status"], t["failed_phase"], t["completed"], t["from"]) == ("failed", "verify-switched", ["create"], "")
    assert t["restored"]["generation"] == "" and t["restored"]["tags"] == gen["tags"]
    state = live_state(h.env_dir)
    assert live_tags(state) == gen["tags"] and grant_keys(state) == DEPLOYER_GRANT
    assert {p["name"] for p in live_policies(state)} == {p["name"] for p in gen["fgac_policies"]}
    h.calls.clear()
    h.after.clear()
    h.verifies.clear()
    return gen


def test_the_status_says_the_new_masks_are_live_after_a_first_deploy_restore(harness, tmp_path, monkeypatch,
                                                                             capsys):
    gen = _failed_like_the_live_run(harness, tmp_path, monkeypatch, capsys)
    out, err = harness.output
    run_id = harness.journal()["transition"]["run_id"]
    assert (f"Policy stage restore: applied the previous protection (none); 2 newly governed column(s) keep "
            f"generation {gen['fingerprint']}'s masks and treatment tags") in out
    assert ("A first deploy has no previous protection to go back to: the restore keeps generation "
            f"{gen['fingerprint']}'s masks and treatment tags on 2 column(s), verified live (fail-closed). "
            "No business USE or SELECT was granted.") in out
    assert (f"Next: fix what stopped it (above), then re-run the same command: it resumes run {run_id} after "
            "create (verify-created -> switch -> verify-switched -> grant).") in err
    assert "make policy-abandon" not in err.split("Next:")[-1]

    assert spa.main(["status", "--env-dir", str(harness.env_dir)]) == 0
    status = capsys.readouterr().out
    assert "the previous protection (none), restored" not in status
    assert "active generation:  none" in status
    assert (f"  live:      generation {gen['fingerprint']}'s masks and treatment tags on 2 column(s), kept by the "
            "restore after verify-switched: a first deploy has no previous protection (fail-closed; no business "
            "USE or SELECT is granted until the grant phase)") in status
    assert ("  next:      re-run the same command to resume (verify-created -> switch -> verify-switched -> "
            "grant); it can't be abandoned while columns keep its masks") in status

    # Abandon would drop masks that are live on every column: it refuses, nothing changes, and it points at
    # resuming (fail-closed: the masks stay, and no business group can read anything yet).
    before = live_state(harness.env_dir)
    with pytest.raises(spa.Refused, match="keep the new generation's treatment") as refused:
        harness.controller().abandon()
    assert "resume the change by re-running the command that started it" in str(refused.value)
    assert live_state(harness.env_dir) == before and harness.calls == []


def test_re_running_rehearse_resumes_the_live_run_s_journal_to_the_end(harness, tmp_path, monkeypatch, capsys):
    gen = _failed_like_the_live_run(harness, tmp_path, monkeypatch, capsys)
    run_id = harness.journal()["transition"]["run_id"]
    capsys.readouterr()
    assert harness.apply() == 0, capsys.readouterr().err
    assert f"resuming run {run_id} (failed at verify-switched)" in capsys.readouterr().out
    # verify-created re-checks the restore (every column already carries its mask), then switch (no change),
    # verify-switched, and only then grant.
    assert harness.calls == ["inventory:create/restore", "verify-access:restore", "switch", "inventory:switch",
                             "verify-access:switch", "grant"]
    assert [(v["stage"], v["rc"], v["pre_grant"], v["business"]) for v in harness.verifies] == [
        ("restore", 0, "1", set()), ("switch", 0, "1", set())]
    for verify in harness.verifies:
        assert not verify["grantees"] & set(TIERS)
        assert verify["seen"][("regional", "notes")] == "partial"
    assert harness.after["switch"]["grants"] == DEPLOYER_GRANT
    assert harness.after["grant"]["grants"] == BUSINESS | DEPLOYER_GRANT
    journal = harness.journal()
    assert journal["active"] == gen["fingerprint"] and journal["transition"] is None
    assert journal["history"][-1]["run_id"] == run_id


# -- (3) an already-deployed env -----------------------------------------------------------------------
def test_a_det_to_det_change_verifies_the_same_way_and_leaves_the_tier_grants_alone(harness, tmp_path,
                                                                                    monkeypatch):
    old = fresh(harness, tmp_path, monkeypatch)
    assert harness.apply() == 0 and harness.controller().confirm() == 0
    granted = grant_keys(live_state(harness.env_dir))
    assert granted == BUSINESS | DEPLOYER_GRANT
    new = write_env(harness.env_dir, deployer=OLD_SP, versions={"email_partial": {"partial": "redacted"}})
    _promoted(harness, old, new)
    _refresh(harness.env_dir)
    harness.verifies.clear()
    harness.uc.statements.clear()
    assert harness.apply() == 0
    assert [(v["stage"], v["rc"], v["pre_grant"]) for v in harness.verifies] == [("create", 0, "1"),
                                                                                ("switch", 0, "1")]
    # The temporary grants name only test principals; no tier group's grant is touched.
    assert all(s.rsplit("`", 2)[1].startswith("app-") for s in harness.uc.statements)
    assert grant_keys(live_state(harness.env_dir)) == granted
    assert harness.journal()["active"] == new["fingerprint"]


# -- (4) a pre-grant verify killed before its cleanup -------------------------------------------------
def _killed_then_granted(h, tmp_path, monkeypatch) -> list[str]:
    """A first deploy whose verify-switched was SIGKILLed after its reads: its direct grants stay.

    The controller saw it pass, so grant ran; the inputs a post-grant verify-access (rehearse's gate) uses.
    """
    mixed(h, tmp_path, monkeypatch)
    h.uc.killed = True
    assert h.apply() == 0
    h.uc.killed = False
    left = {grantee for grantee, _, _ in h.uc.direct}
    assert left == PRINCIPALS  # what the killed run never revoked
    t = {"from": "", "to": h.journal()["active"], "tier_move": False}
    tfvars, env_file = h.controller().verification_inputs(t, ("switch",))
    h.uc.statements.clear()
    return ["--from-tfvars", str(tfvars), "--env-file", str(env_file), "--key-column", "id", "--require-mask-checks",
            "--gate", "--auth-file", str(h.auth), "--live"]


def _drop_group_grant(h, key: str) -> None:
    path = h.env_dir / "data_access" / "terraform.tfstate"
    state = json.loads(path.read_text())
    for resource in state["resources"]:
        if resource["type"] == "databricks_grant":
            resource["instances"] = [i for i in resource["instances"] if f"{resource['name']}|{i['index_key']}" != key]
    path.write_text(json.dumps(state))


def test_a_post_grant_verify_revokes_a_killed_run_s_grants_and_reads_through_the_groups(harness, tmp_path,
                                                                                         monkeypatch, capsys):
    cli = _killed_then_granted(harness, tmp_path, monkeypatch)
    harness.uc.grantees_while_reading = set()
    assert vea.main(cli) == 0, capsys.readouterr()
    # Every leftover grant was revoked before any read; only the out-of-tier probe (in no group) read
    # through a direct grant, and it was revoked again on exit.
    assert harness.uc.grantees_while_reading == {f"app-{vea.OUT_OF_TIER_PRINCIPAL}"}
    assert harness.uc.direct == set()
    first_grant = next(i for i, sql in enumerate(harness.uc.statements) if sql.startswith("GRANT"))
    assert {sql.rsplit("`", 2)[1] for sql in harness.uc.statements[:first_grant]} == PRINCIPALS
    assert all(sql.startswith("REVOKE") for sql in harness.uc.statements[:first_grant])


def test_a_post_grant_verify_fails_when_a_tier_group_lacks_its_real_grant_despite_leftovers(harness, tmp_path,
                                                                                            monkeypatch, capsys):
    cli = _killed_then_granted(harness, tmp_path, monkeypatch)
    _drop_group_grant(harness, "table_access|cat.sch.t|regional")
    capsys.readouterr()
    assert vea.main(cli) == 1
    out = capsys.readouterr()
    assert "User does not have SELECT on Table 'cat.sch.t'" in out.out + out.err
    assert harness.uc.direct == set()


def test_a_leftover_grant_that_can_t_be_revoked_refuses_the_post_grant_verify(harness, tmp_path, monkeypatch):
    cli = _killed_then_granted(harness, tmp_path, monkeypatch)
    real = harness.uc.execute

    def revoke_denied(sql):
        if sql.startswith("REVOKE") and "app-regional" in sql and not harness.uc.grantees_while_reading:
            raise RuntimeError("PERMISSION_DENIED")
        real(sql)

    harness.uc.grantees_while_reading = set()
    harness.uc.seen = {}
    monkeypatch.setattr(harness.uc, "execute", revoke_denied)
    with pytest.raises(RuntimeError, match="refusing to verify: test principal app-regional"):
        vea.main(cli)
    # Refused before any grant or read.
    assert harness.uc.seen == {} and not [sql for sql in harness.uc.statements if sql.startswith("GRANT")]


# -- (4) the live run after #226: verify-created stopped on ssn (partial = full) and never-raw cvv ------------
# AWS dev-2 run 21fce9f7649e, resumed after #226: verify-created ran the real per-tier checks, 6 PASS and 3
# INCONCLUSIVE. ssn gets partial = full = "redacted" under two differently named functions with identical
# bodies; cvv is never raw, so its one policy masks everyone, the deployer too, and no raw value exists to
# compare. Now identical functions verify by exact output and a never-raw column by its proven constant mask.
REDACTED_DDL = "CREATE TABLE cat.sch.t (\n  ssn STRING,\n  cvv STRING\n);\n"
REDACTED_CLASSES = {"ssn": ["us_ssn"], "cvv": ["card_security_code"]}


def redacted_env(h, tmp_path, monkeypatch) -> dict:
    """A fresh env: cat.sch.t.ssn (partial = full = redacted, two functions) and cat.sch.t.cvv (never raw)."""
    real = staged.build_model
    monkeypatch.setattr(staged, "build_model", lambda columns, *args, **kwargs: real(
        {c: REDACTED_CLASSES[c.rsplit(".", 1)[1]] for c in columns}, *args, **kwargs))
    gen = fresh(h, tmp_path, monkeypatch, ddl_text=REDACTED_DDL, columns=("ssn", "cvv"))
    functions = {p["name"]: p["function_name"] for p in gen["fgac_policies"]}
    ssn = gen["tags"]["cat.sch.t.ssn"]
    assert (functions[f"gr_mask_cat_{ssn}_partial"], functions[f"gr_mask_cat_{ssn}_full"]) == (
        f"gr_mask_{ssn}_partial", f"gr_mask_{ssn}_full")
    [cvv] = [p for p in gen["fgac_policies"] if p["name"].startswith("gr_mask_cat_card_security_code")]
    assert (cvv["to_principals"], cvv.get("except_principals") or []) == (["account users"], [])
    return gen


def redacting(h, monkeypatch) -> None:
    """Unity Catalog's real answers for these masks: both bodies are redact_string ('[REDACTED]', NULL kept).

    ``h.mask_proofs`` False is the verifier before this fix: it proves neither the identical functions nor
    the never-raw mask, so both checks stay INCONCLUSIVE exactly as on main.
    """
    h.mask_proofs = False
    read = FakeUnityCatalog.read
    monkeypatch.setattr(FakeUnityCatalog, "read", lambda self, app, tier, table, column, keys: [
        (k, "[REDACTED]" if ".gr_mask_" in value else value)
        for k, value in read(self, app, tier, table, column, keys)])
    base = vea.EffectiveAccessVerifier

    class Verifier(base):
        def expected_tier_values(self, principal, check, keys):
            # The admin reads the column (the masked constant when it is never raw), then applies each function.
            raw = h.uc.read(principal.application_id, principal.tier, check.table, check.column, keys)
            return {"raw": raw, **{tier: [(k, None if v is None else "[REDACTED]") for k, v in raw]
                                   for tier in ("partial", "full")}}

        def function_equivalence_problem(self, principal, left, right):
            if not h.mask_proofs:
                raise AttributeError("no function_equivalence_problem before this fix")
            assert {left.rsplit("_", 1)[1], right.rsplit("_", 1)[1]} == {"partial", "full"}
            return ""

        def never_raw_mask_proof(self, principal, check, policies):
            if not h.mask_proofs:
                raise AttributeError("no never_raw_mask_proof before this fix")
            value = live_tags(live_state(h.env_dir))[f"{check.table}.{check.column}"]
            masks = [p for p in live_policies(live_state(h.env_dir)) if _value_of(p) == value]
            attached = [p for p in masks if p["to_principals"] == ["account users"] and not p["except_principals"]
                        and p["column_mask"]["function_name"].lower() == check.full_function.lower()]
            return (vea.NeverRawProof(vea.PASS, "", "[REDACTED]") if len(masks) == len(attached) == 1
                    else vea.NeverRawProof(vea.FAIL, f"{len(attached)} of {len(masks)} live masks attached"))

    monkeypatch.setattr(vea, "EffectiveAccessVerifier", Verifier)


def _stopped_at_verify_created_like_the_live_run(h, tmp_path, monkeypatch, capsys) -> tuple[dict, str]:
    """The live sequence: verify-switched without temporary access (#226), then the resume under main's verifier."""
    gen = redacted_env(h, tmp_path, monkeypatch)
    monkeypatch.setattr(spa, "PRE_GRANT_VERIFY", set(), raising=False)
    assert h.apply() == 1
    monkeypatch.setattr(spa, "PRE_GRANT_VERIFY", {"verify-created", "verify-switched"}, raising=False)
    run_id = h.journal()["transition"]["run_id"]
    capsys.readouterr()
    h.calls.clear()
    h.verifies.clear()
    redacting(h, monkeypatch)
    assert h.apply() == 1
    out = capsys.readouterr()
    assert f"resuming run {run_id} (failed at verify-switched)" in out.out
    assert "INCONCLUSIVE" in out.out + out.err and "cannot be distinguished" in out.out + out.err
    assert "could not prove the mask as the admin baseline" in out.out + out.err
    t = h.journal()["transition"]
    assert (t["run_id"], t["status"], t["failed_phase"]) == (run_id, "failed", "verify-created")
    [verify] = h.verifies
    assert (verify["stage"], verify["rc"], verify["pre_grant"], verify["business"]) == ("restore", 1, "1", set())
    assert grant_keys(live_state(h.env_dir)) == DEPLOYER_GRANT  # fail-closed: nothing granted
    h.calls.clear()
    h.after.clear()
    h.verifies.clear()
    return gen, run_id


def test_ssn_and_never_raw_cvv_verify_and_the_live_run_resumes_from_verify_created(harness, tmp_path,
                                                                                  monkeypatch, capsys):
    gen, run_id = _stopped_at_verify_created_like_the_live_run(harness, tmp_path, monkeypatch, capsys)
    before = harness.journal()["transition"]
    harness.mask_proofs = True
    # Unchanged config: the same run resumes from verify-created to the end.
    rc = harness.apply()
    out = capsys.readouterr().out
    assert rc == 0, out
    assert f"resuming run {run_id} (failed at verify-created)" in out
    assert harness.calls == ["inventory:create/restore", "verify-access:restore", "switch", "inventory:switch",
                             "verify-access:switch", "grant"]
    assert [(v["stage"], v["rc"], v["pre_grant"], v["business"]) for v in harness.verifies] == [
        ("restore", 0, "1", set()), ("switch", 0, "1", set())]
    for verify in harness.verifies:
        # Every tier read through its own temporary grant; cvv is masked for everyone, the deployer included.
        assert verify["grantees"] == PRINCIPALS and not verify["grantees"] & set(TIERS)
        assert {tier for (tier, column), mask in verify["seen"].items() if column == "cvv" and mask == "raw"} == set()
        assert verify["seen"][(vea.DEFAULT_ADMIN_TIER, "cvv")] == "full"
        assert verify["seen"][("raw", "ssn")] == "raw" and verify["seen"][("regional", "ssn")] == "partial"
    for column in ("ssn", "cvv"):
        assert out.count(f"✓ [PASS] column-mask cat.sch.t.{column} ") == 2
    assert harness.after["grant"]["grants"] == BUSINESS | DEPLOYER_GRANT
    journal = harness.journal()
    assert journal["active"] == gen["fingerprint"] and journal["transition"] is None
    assert journal["history"][-1]["run_id"] == run_id == before["run_id"]


@pytest.mark.parametrize("broken", ["cvv-deployer-leak", "cvv-tier-leak", "ssn-leak"])
def test_a_broken_ssn_or_cvv_mask_still_stops_the_resumed_run(harness, tmp_path, monkeypatch, capsys, broken):
    _gen, run_id = _stopped_at_verify_created_like_the_live_run(harness, tmp_path, monkeypatch, capsys)
    harness.mask_proofs = True
    # A mask the fake drops: the tier (or the deployer baseline) reads raw.
    harness.uc.leak = {"ssn-leak": {"regional"}, "cvv-tier-leak": {"masked"},
                       "cvv-deployer-leak": {vea.DEFAULT_ADMIN_TIER}}[broken]
    assert harness.apply() == 1
    out = capsys.readouterr()
    assert f"resuming run {run_id} (failed at verify-created)" in out.out
    column = broken.split("-")[0]
    assert f"✗ [FAIL] column-mask cat.sch.t.{column} " in out.out + out.err
    t = harness.journal()["transition"]
    assert (t["run_id"], t["failed_phase"]) == (run_id, "verify-created")
    assert [(v["stage"], v["rc"]) for v in harness.verifies] == [("restore", 1)]
    assert grant_keys(live_state(harness.env_dir)) == DEPLOYER_GRANT
