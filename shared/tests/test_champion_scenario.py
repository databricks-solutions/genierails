"""Static / dry-run checks for the `champion` live integration scenario.

The scenario itself needs a live account. Here every Databricks-facing helper is
stubbed and `make` is replaced by a fake that reproduces each target's file
effects (using the real placeholder guard, access_tier_groups persister and
remap_env_config.py), so the README step order and the scenario's own
assertions are exercised without credentials.
"""

import json
import os
import shlex
import shutil
import socket
import subprocess
import sys
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED / "scripts"))
sys.path.insert(0, str(SHARED))

import run_integration_tests as rit  # noqa: E402
import run_parallel_tests as rpt  # noqa: E402
from access_tier_groups import persist_access_tier_groups  # noqa: E402
from scripts import environment_lock as env_lock  # noqa: E402

SPACE_ID = "01champion0dev"
WAREHOUSE = "wh0champion"
DEV_TABLES = [f"dev_fin.finance.{t}" for t in rit.CHAMPION_TABLES]
DEV_TABLES_SORTED = sorted(DEV_TABLES, key=str.lower)


def _var(args, key):
    return next((a.split("=", 1)[1] for a in args if a.startswith(f"{key}=")), None)


class FakeMake:
    """Records make invocations and applies each target's essential effects."""

    def __init__(self, envs: Path):
        self.envs = envs
        self.calls: list[list[str]] = []

    def _done(self, args, code=0, out=""):
        return subprocess.CompletedProcess(["make", *args], code, stdout=out)

    def __call__(self, *args, check=True, capture=False, retries=0,
                 retry_delay_seconds=0, suffix_account_names=True, **_):
        assert suffix_account_names is False, "champion must not suffix account names"
        args = list(args)
        self.calls.append(args)
        target = args[0]
        env = _var(args, "ENV")
        env_dir = self.envs / env if env else None
        result = getattr(self, "_" + target.replace("-", "_"))(args, env_dir)
        if check and result.returncode != 0:
            raise RuntimeError(f"Command failed: make {' '.join(args)}\n{result.stdout}")
        return result

    def _setup(self, args, env_dir):
        for sub in ("generated", "data_access", "ddl"):
            (env_dir / sub).mkdir(parents=True, exist_ok=True)
        (env_dir / "auth.auto.tfvars").write_text(
            'databricks_workspace_host = ""\n'
            'databricks_client_id = "d0000000-0000-0000-0000-00000000d001"\n'
        )
        if env_dir.name == "dev" and not (env_dir / "env.auto.tfvars").exists():
            # What make setup ENV=dev copies from the walkthrough (README Dev step 1).
            shutil.copy2(rit.DEV_TO_PROD_TEMPLATE, env_dir / "env.auto.tfvars")
            (env_dir / "agents").mkdir(exist_ok=True)
            if not any((env_dir / "agents").glob("*.auto.tfvars")):
                shutil.copy2(rit.DEV_TO_PROD_AGENT_TEMPLATE, env_dir / "agents" / rit.SETUP_AGENT_FILE)
        if env_dir.name == "prod":  # what make setup ENV=prod writes for a promoted env
            (env_dir / "env.auto.tfvars").write_text(
                'genie_spaces = []\nsql_warehouse_id = ""\ngovernance_mode = "deterministic"\n'
                'deployer_principal_id = "<your-deployer-principal-id>"\n'
                'access_tier_groups = ["<most-privileged-group>", "<least-privileged-group>"]\n'
                'promote_from = "dev"\ncatalog_map = { "<dev_catalog>" = "<prod_catalog>" }\n')
        (self.envs / "account").mkdir(exist_ok=True)
        return self._done(args)

    def _guard(self, args, env_dir):
        allow_groups = "1" if "--groups" in str(_var(args, "GENERATE_ARGS") or "") else "0"
        guard = subprocess.run(
            [sys.executable, str(SHARED / "genie_space_placeholder.py"),
             str(env_dir / "env.auto.tfvars")],
            capture_output=True, text=True,
            env=dict(os.environ, GENIERAILS_ALLOW_ACCESS_TIER_PLACEHOLDERS=allow_groups),
        )
        return self._done(args, guard.returncode, guard.stdout + guard.stderr)

    def _generate(self, args, env_dir):
        guarded = self._guard(args, env_dir)
        if guarded.returncode:
            return guarded
        gen_args = _var(args, "GENERATE_ARGS")
        title = rit.CHAMPION_SPACE_TITLE
        if _var(args, "MODE") == "genie":
            groups = shlex.split(gen_args)[1].split(",")
            persist_access_tier_groups(env_dir / "env.auto.tfvars", groups)
            rows = "".join(f'  "{t}",\n' for t in DEV_TABLES)
            agents = "".join(f'  "{t}" = ["{title}"]\n' for t in DEV_TABLES)
            (env_dir / "data_access" / "discovered_uc_tables.auto.tfvars").write_text(
                f"discovered_uc_tables = [\n{rows}]\ndiscovered_table_agents = {{\n{agents}}}\n"
            )
        else:
            assert gen_args is None, "only the first generate may pass --groups"
            cfg = rit._load_tfvars(env_dir / "env.auto.tfvars")
            groups = ", ".join(f'"{g}"' for g in cfg["access_tier_groups"])
            (env_dir / "generated" / "abac.auto.tfvars").write_text(
                f'groups = [{groups}]\ntag_policies = [{{ key = "gr_treatment" }}]\n'
                f'genie_space_id_to_name = {{ "{SPACE_ID}" = "{title}" }}\n'
            )
            (env_dir / "generated" / "masking_functions.sql").write_text("-- masks\n")
        # import-new: the first dev generate captures each agent file's agent.
        capture = env_dir / "agents" / f"{rit.CHAMPION_AGENT}.space.json"
        if (env_dir / "agents" / f"{rit.CHAMPION_AGENT}.auto.tfvars").is_file() and not capture.exists():
            capture.write_text(json.dumps({"data_sources": {"tables": [{"identifier": t} for t in DEV_TABLES]}}))
        return self._record(args, env_dir)

    def _record(self, args, env_dir):
        """What the real generate recipe runs right after generate_abac.py: in a
        deterministic env with a generated config, the real sticky_governance.py
        record, which commits each ID-only agent's discovered tables."""
        config = env_dir / "generated" / "abac.auto.tfvars"
        cfg = rit._load_tfvars(env_dir / "env.auto.tfvars")
        if not (config.is_file() and cfg.get("governance_mode") == "deterministic"):
            return self._done(args)
        record = subprocess.run(
            [sys.executable, str(SHARED / "scripts" / "sticky_governance.py"), "record",
             "--env-dir", str(env_dir), "--config", str(config)],
            capture_output=True, text=True,
        )
        return self._done(args, record.returncode, record.stdout + record.stderr)

    def _enable_classification(self, args, env_dir):
        return self._guard(args, env_dir)

    def _rehearse(self, args, env_dir):
        # rehearse's access check reads the committed attribution generate wrote.
        assert self._agent_tables(env_dir) == {rit.CHAMPION_AGENT: DEV_TABLES_SORTED}, \
            "rehearse before generate recorded"
        assert _var(args, "VERIFY_KEY_COLUMN") is None  # no key flag: picked per table
        # A passing verify-access saves the keys it proved per table.
        with (env_dir / "env.auto.tfvars").open("a") as f:
            f.write('verify_key_columns = { "dev_fin.finance.customers" = "customer_id" }\n')
        return self._done(args, 0, "  ✓ [PASS] column-mask x\n  RESULT: ALL EFFECTIVE (1 passed / 1)\n")

    @staticmethod
    def _agent_tables(env_dir):
        path = env_dir / "generated" / "governed_tables.json"
        return json.loads(path.read_text()).get("agent_tables") if path.is_file() else None

    def _promote_to(self, args, env_dir):
        guard = subprocess.run([sys.executable, str(SHARED / "genie_space_placeholder.py"),
                                str(self.envs / "dev" / "env.auto.tfvars"), str(env_dir / "env.auto.tfvars")],
                               capture_output=True, text=True)
        assert guard.returncode == 0, guard.stdout + guard.stderr
        src, dest = self.envs / "dev", env_dir
        catalog_map = ",".join(f"{k}={v}" for k, v in rit._load_tfvars(dest / "env.auto.tfvars")["catalog_map"].items())
        for sub in ("generated", "data_access"):
            (dest / sub).mkdir(parents=True, exist_ok=True)
        # promote-to's agent step, then promote's env remap (the real scripts).
        for command in (["agent_promote.py", "--source-dir", str(src), "--dest-dir", str(dest),
                         "--catalog-map", catalog_map],
                        ["remap_env_config.py", str(src), str(dest), catalog_map]):
            result = subprocess.run([sys.executable, str(SHARED / "scripts" / command[0]), *command[1:]],
                                    capture_output=True, text=True)
            assert result.returncode == 0, result.stdout + result.stderr
            print(result.stdout, end="")
        (dest / "generated" / "masking_functions.sql").write_text("-- prod masks\n")
        return self._done(args)

    def _release(self, args, env_dir):
        lock_path = env_dir / env_lock.LOCK_RELPATH
        lock_path.write_text(json.dumps({"pid": os.getpid(), "host": socket.gethostname()}))
        lock_path.unlink()
        (env_dir / ".genie_space_id_champion_finance_analytics").write_text("01champion0prod\n")
        return self._done(args, 0, (
            "  ✓ [PASS] column-mask x\n  RESULT: ALL EFFECTIVE (1 passed / 1)\n"
            "=== Release complete (prod) ===\n"))

    def _maintain(self, args, env_dir):
        return self._done(args)


@pytest.fixture
def champion(tmp_path, monkeypatch):
    envs = tmp_path / "envs"
    envs.mkdir()
    fake = FakeMake(envs)
    waits, teardown = [], []
    monkeypatch.setattr(rit, "ENVS_DIR", envs)
    monkeypatch.setattr(rit, "_make", fake)
    monkeypatch.setattr(rit, "_preamble_cleanup", lambda *a, **k: None)
    monkeypatch.setattr(rit, "_setup_data", lambda *a, **k: WAREHOUSE)
    monkeypatch.setattr(rit, "_get_or_find_warehouse", lambda _auth, wh: wh)
    monkeypatch.setattr(rit, "_create_account_groups",
                        lambda _auth, names, created: created.update({n: f"id-{n}" for n in names}))
    # This branch's remap_env_config.py predates PR #59, which renames a space
    # whose title names the dev catalog; the live run (after #59) covers that.
    monkeypatch.setattr(rit, "CHAMPION_SPACE_TITLE", "Champion Finance Analytics")
    monkeypatch.setattr(rit, "_account_state_tag_policy_keys", lambda: set())
    monkeypatch.setattr(rit, "_create_genie_space_via_api", lambda *a, **k: SPACE_ID)
    monkeypatch.setattr(rit, "_wait_for_class_tags", lambda _a, _w, catalog: waits.append(catalog))
    monkeypatch.setattr(rit, "_force_account_reapply", lambda *_: None)
    monkeypatch.setattr(rit, "_get_genie_space_via_api",
                        lambda _a, sid: {"space_id": sid, "title": rit.CHAMPION_SPACE_TITLE})
    # A release grants CAN_RUN on the prod agent to exactly its agent file's acl_groups.
    monkeypatch.setattr(rit, "_get_genie_space_acl_groups", lambda _a, _sid: {"admins", *rit._load_tfvars(
        envs / "prod" / "agents" / f"{rit.CHAMPION_AGENT}.auto.tfvars")["acl_groups"]})
    for name in ("_try_destroy", "_try_destroy_account", "_delete_genie_space_via_api",
                 "_delete_account_groups", "_teardown_data"):
        monkeypatch.setattr(rit, name, lambda *a, _n=name, **k: teardown.append(_n))
    monkeypatch.setattr(rit, "_TEST_SUFFIX", "abc123")
    return fake, envs, waits, teardown


def test_champion_is_registered_everywhere():
    assert "champion" in rit.SCENARIOS
    assert "champion" in rpt.SCENARIOS
    assert "test-champion:" in (SHARED / "Makefile.shared").read_text()
    assert "**champion**" in (SHARED / "docs" / "integration-testing.md").read_text()


def test_champion_dry_run_follows_the_readme_order(champion):
    fake, envs, waits, teardown = champion
    rit.scenario_champion(envs / "dev" / "auth.auto.tfvars", "", keep_data=False)

    groups = '--groups "champion_full_abc123,champion_analyst_abc123"'
    assert fake.calls == [
        ["setup", "ENV=dev"],
        ["generate", "ENV=dev", "MODE=genie"],                       # placeholder: refused
        ["generate", "ENV=dev", "MODE=genie", f"GENERATE_ARGS={groups}"],
        ["enable-classification", "ENV=dev"],
        ["enable-classification", "ENV=dev"],
        ["generate", "ENV=dev"],
        ["rehearse", "ENV=dev"],
        ["setup", "ENV=prod"],
        ["promote-to", "ENV=prod"],
        ["enable-classification", "ENV=prod"],
        ["enable-classification", "ENV=prod"],
        ["release", "ENV=prod"],
        ["maintain", "ENV=prod"],
    ]
    assert waits == ["dev_fin", "prod_fin"]
    assert teardown[:3] == ["_try_destroy", "_try_destroy", "_try_destroy_account"]

    dev_cfg = rit._load_tfvars(envs / "dev" / "env.auto.tfvars")
    # The agent is in its own file, with who may run it (decision 4).
    assert dev_cfg["genie_spaces"] == []
    groups = ["champion_full_abc123", "champion_analyst_abc123"]
    dev_agent = rit._load_tfvars(envs / "dev" / "agents" / "Champion Finance Analytics.auto.tfvars")
    assert dev_agent == {"genie_space_id": SPACE_ID, "acl_groups": groups, "name": rit.CHAMPION_SPACE_TITLE}
    prod_agent = rit._load_tfvars(envs / "prod" / "agents" / "Champion Finance Analytics.auto.tfvars")
    assert prod_agent["acl_groups"] == groups and prod_agent["genie_space_id"] == ""
    assert "uc_tables" not in dev_cfg
    assert dev_cfg["enable_auto_tagging"] is True
    assert dev_cfg["verify_key_columns"] == {"dev_fin.finance.customers": "customer_id"}
    assert "verify_key_column" not in dev_cfg
    prod_cfg = rit._load_tfvars(envs / "prod" / "env.auto.tfvars")
    assert prod_cfg["verify_key_columns"] == {"prod_fin.finance.customers": "customer_id"}
    # Promote and release neither write nor need the retired exposure flag.
    assert "business_access_enabled" not in prod_cfg
    assert prod_cfg["sql_warehouse_id"] == WAREHOUSE
    # The agent has its own file: generate committed its captured tables under its name.
    assert FakeMake._agent_tables(envs / "dev") == {rit.CHAMPION_AGENT: DEV_TABLES_SORTED}
    # promote-to carries the capture with its tables remapped, not env uc_tables.
    prod_capture = json.loads((envs / "prod" / "agents" / f"{rit.CHAMPION_AGENT}.space.json").read_text())
    assert sorted(t["identifier"] for t in prod_capture["data_sources"]["tables"]) == sorted(
        t.replace("dev_fin.", "prod_fin.") for t in DEV_TABLES)
    assert (envs / "prod" / "generated" / "masking_functions.sql").read_text() == "-- prod masks\n"


def test_champion_tears_down_when_a_step_fails(champion, monkeypatch):
    fake, envs, _waits, teardown = champion
    monkeypatch.setattr(FakeMake, "_release",
                        lambda self, args, env_dir: self._done(args, 2, "coverage check FAILED\n"))
    with pytest.raises(RuntimeError, match="make release ENV=prod"):
        rit.scenario_champion(envs / "dev" / "auth.auto.tfvars", "", keep_data=False)
    assert {"_try_destroy", "_try_destroy_account", "_delete_genie_space_via_api",
            "_delete_account_groups", "_teardown_data"} <= set(teardown)


def test_missing_class_tags_fail_by_default_without_seeding(monkeypatch):
    monkeypatch.delenv(rit.CHAMPION_SEED_ENV, raising=False)
    monkeypatch.setenv(rit.CHAMPION_TIMEOUT_ENV, "0")
    monkeypatch.setattr(rit, "_class_tag_rows", lambda *a: [])
    seeded = []
    monkeypatch.setattr(rit, "_seed_class_tags", lambda *a: seeded.append(a))
    with pytest.raises(AssertionError, match="CHAMPION_SEED_CLASS_TAGS=1"):
        rit._wait_for_class_tags(Path("auth"), WAREHOUSE, "dev_fin")
    assert seeded == []


def test_seeding_is_opt_in_and_loud(monkeypatch, capsys):
    monkeypatch.setenv(rit.CHAMPION_SEED_ENV, "1")
    monkeypatch.setenv(rit.CHAMPION_TIMEOUT_ENV, "0")
    rows = []
    monkeypatch.setattr(rit, "_class_tag_rows", lambda *a: list(rows))
    statements = []
    monkeypatch.setattr(rit, "_sdk_run_sql", lambda _auth, sql, warehouse_id="": (
        statements.append(sql), rows.append(["customers", "email", "class.email_address"])))
    rit._wait_for_class_tags(Path("auth"), WAREHOUSE, "prod_fin")
    assert len(statements) == len(rit.CHAMPION_SEED_TAGS)
    assert all(s.startswith("ALTER TABLE prod_fin.finance.") for s in statements)
    assert "SEEDING class.* tags on prod_fin" in capsys.readouterr().out


def test_fixture_title_names_the_dev_catalog():
    # Exercises the promote rename fixed in PR #59 on the live run.
    assert f"{rit.DEV_FIN_CAT}." in rit.CHAMPION_SPACE_TITLE


def test_partial_group_creation_is_recorded_for_teardown(monkeypatch):
    class Groups:
        def create(self, display_name):
            if display_name == "second":
                raise RuntimeError("quota")
            return type("G", (), {"id": f"id-{display_name}"})()

    monkeypatch.setattr(rit, "_account_client", lambda _auth: type("A", (), {"groups": Groups()})())
    created = {}
    with pytest.raises(RuntimeError, match="quota"):
        rit._create_account_groups(Path("auth"), ["first", "second"], created)
    assert created == {"first": "id-first"}


def test_teardown_reports_but_never_deletes_leftover_tag_policies(champion, monkeypatch, capsys):
    fake, envs, _waits, teardown = champion
    # The account destroy "failed": its state still lists a tag policy.
    monkeypatch.setattr(rit, "_account_state_tag_policy_keys", lambda: {"gr_treatment"})

    class NoDirectDeletes:
        def __init__(self, *a, **k):
            raise AssertionError("teardown must not call the tag-policy API directly")

    monkeypatch.setattr("databricks.sdk.WorkspaceClient", NoDirectDeletes)
    rit.scenario_champion(envs / "dev" / "auth.auto.tfvars", "", keep_data=False)
    out = capsys.readouterr().out
    assert "LEFTOVER" in out and "- gr_treatment" in out and "NOT deleted directly" in out
    assert not hasattr(rit, "_delete_tag_policies")
    assert teardown.index("_try_destroy_account") < teardown.index("_delete_genie_space_via_api")


def test_teardown_is_quiet_when_destroy_removed_the_tag_policies(champion, capsys):
    fake, envs, _waits, _teardown = champion
    rit.scenario_champion(envs / "dev" / "auth.auto.tfvars", "", keep_data=False)
    assert "LEFTOVER" not in capsys.readouterr().out


def test_account_state_tag_policy_keys_reads_only_tag_policies(tmp_path, monkeypatch):
    monkeypatch.setattr(rit, "ENVS_DIR", tmp_path)
    (tmp_path / "account").mkdir()
    (tmp_path / "account" / "terraform.tfstate").write_text(json.dumps({"resources": [
        {"type": "databricks_tag_policy", "instances": [
            {"attributes": {"tag_key": "gr_treatment"}}]},
        {"type": "databricks_group", "instances": [{"attributes": {"tag_key": "nope"}}]},
    ]}))
    assert rit._account_state_tag_policy_keys() == {"gr_treatment"}


def _remap_renames_spaces() -> bool:
    import scripts.remap_env_config as remap
    return hasattr(remap, "remap_hcl")  # added by PR #59


@pytest.mark.skipif(not _remap_renames_spaces(),
                    reason="needs PR #59's space rename in remap_env_config.py")
def test_promote_renames_the_catalog_named_title(champion, monkeypatch, capsys):
    fake, envs, _waits, _teardown = champion
    monkeypatch.setattr(rit, "CHAMPION_SPACE_TITLE",
                        f"Champion Finance Analytics ({rit.DEV_FIN_CAT}.finance)")
    rit.scenario_champion(envs / "dev" / "auth.auto.tfvars", "", keep_data=False)
    assert "renamed to 'Champion Finance Analytics (prod_fin.finance)'" in capsys.readouterr().out


# ── The real generate recipe, with only generate_abac.py stubbed ─────────────

# What generate_abac.py writes for an agent given by ID in a deterministic env
# (the Genie API reads are the stubbed part): every mode persists the discovery,
# then write_deterministic_draft writes generated/abac.auto.tfvars with the
# ID -> canonical name map (MODE=genie too, with no ABAC blocks; see
# test_genie_mode_draft_writes_the_id_to_name_map); --groups is persisted.
GENERATE_STUB = f"""import sys, pathlib
sys.path.insert(0, {str(SHARED)!r})
from access_tier_groups import persist_access_tier_groups
args = sys.argv[1:]
env = pathlib.Path.cwd()
if "--groups" in args:
    persist_access_tier_groups(env / "env.auto.tfvars", args[args.index("--groups") + 1].split(","))
rows = "".join(f'  "{{t}}",\\n' for t in {DEV_TABLES!r})
owners = "".join(f'  "{{t}}" = ["Champion Finance Analytics"]\\n' for t in {DEV_TABLES!r})
(env / "data_access" / "discovered_uc_tables.auto.tfvars").write_text(
    f"discovered_uc_tables = [\\n{{rows}}]\\ndiscovered_table_agents = {{{{\\n{{owners}}}}}}\\n")
abac = 'genie_space_id_to_name = {{ "{SPACE_ID}" = "Champion Finance Analytics" }}\\n'
if "genie" not in args:
    abac = 'groups = {{ full = {{}}, analyst = {{}} }}\\ntag_policies = []\\ntag_assignments = []\\n' + abac
(env / "generated" / "abac.auto.tfvars").write_text(abac)
"""


def _real_make(cloud, shared_root, *args):
    env = {k: v for k, v in os.environ.items()
           if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "MODE", "GENERATE_ARGS", "SOURCE_ENV", "DEST_ENV")}
    return subprocess.run(["make", "--no-print-directory", *args, f"CLOUD_ROOT={cloud}",
                           f"SHARED_ROOT={shared_root}"],
                          cwd=SHARED.parent / "aws", text=True, capture_output=True, env=env, timeout=300)


@pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")
def test_real_generate_commits_the_id_only_agents_tables_before_promotion(tmp_path):
    """README order with the real make recipes: import by ID, generate, then promote from a clean clone."""
    overlay = tmp_path / "shared"
    overlay.mkdir()
    for entry in SHARED.iterdir():
        if entry.name != "generate_abac.py":
            (overlay / entry.name).symlink_to(entry)
    (overlay / "generate_abac.py").write_text(GENERATE_STUB)
    cloud = tmp_path / "aws"
    cloud.mkdir()
    dev = cloud / "envs" / "dev"
    assert _real_make(cloud, overlay, "setup", "ENV=dev").returncode == 0
    template = (SHARED / "examples" / "dev_to_prod" / "env.auto.tfvars.example").read_text()
    # An inline agent given by ID (no agent file). A deterministic agent must name
    # its acl_groups (decision 4); make generate then proposes table_readers for its tables.
    assert template.count("\ngenie_spaces = []\n") == 1
    (dev / "env.auto.tfvars").write_text(template.replace(
        "\ngenie_spaces = []\n",
        f'\ngenie_spaces = [{{ genie_space_id = "{SPACE_ID}", acl_groups = ["analyst"] }}]\n').replace(
        '"<your-deployer-principal-id>"', '"d0000000-0000-0000-0000-00000000d001"'))
    (dev / "agents" / rit.SETUP_AGENT_FILE).unlink()  # README: with the inline list, delete setup's agent file
    (dev / "auth.auto.tfvars").write_text(
        'databricks_workspace_host = "https://dev.example.invalid"\n'
        'databricks_client_id = "d0000000-0000-0000-0000-00000000d001"\n'
        'databricks_client_secret = "test-only-secret"\n'
    )
    record = dev / "generated" / "governed_tables.json"

    # README step 3: import by ID. The recipe records the agent's tables right
    # after generate_abac.py, before its access check and table_readers proposal.
    imported = _real_make(cloud, overlay, "generate", "ENV=dev", "MODE=genie", 'GENERATE_ARGS=--groups full,analyst')
    assert imported.returncode == 0, imported.stdout + imported.stderr
    assert json.loads(record.read_text())["agent_tables"] == {SPACE_ID: DEV_TABLES_SORTED}
    readers = rit._load_tfvars(dev / "env.auto.tfvars").get("table_readers") or {}
    assert set(readers) == set(DEV_TABLES), readers  # proposed from the committed tables
    # README step 5: the full generate keeps it.
    generated = _real_make(cloud, overlay, "generate", "ENV=dev")
    assert generated.returncode == 0, generated.stdout + generated.stderr
    assert json.loads(record.read_text())["agent_tables"] == {SPACE_ID: DEV_TABLES_SORTED}
    # A clean clone has no ignored discovery file; promotion reads the committed record.
    (dev / "data_access" / "discovered_uc_tables.auto.tfvars").unlink()
    prod = cloud / "envs" / "prod"
    for sub in ("generated", "data_access"):
        (prod / sub).mkdir(parents=True, exist_ok=True)
    remap = subprocess.run([sys.executable, str(SHARED / "scripts" / "remap_env_config.py"), str(dev), str(prod),
                            "dev_fin=prod_fin"], capture_output=True, text=True)
    assert remap.returncode == 0, remap.stdout + remap.stderr
    assert sorted(rit._load_tfvars(prod / "env.auto.tfvars")["uc_tables"]) == sorted(
        t.replace("dev_fin.", "prod_fin.") for t in DEV_TABLES)

    # Without the committed entry, promotion still refuses (fail closed).
    record.write_text(json.dumps({"tables": []}))
    refused = subprocess.run([sys.executable, str(SHARED / "scripts" / "remap_env_config.py"), str(dev), str(prod),
                              "dev_fin=prod_fin"], capture_output=True, text=True)
    assert refused.returncode != 0
    assert f"no tables found for agent {SPACE_ID}" in refused.stdout
    assert "run `make generate ENV=dev` and commit generated/" in refused.stdout


def test_genie_mode_draft_writes_the_id_to_name_map(tmp_path):
    """The real deterministic MODE=genie draft writes generated/abac.auto.tfvars with
    genie_space_id_to_name, so generate's record step runs at import (the stub above relies on it)."""
    import generate_abac

    out = tmp_path / "generated"
    out.mkdir()
    tfvars, _sql = generate_abac.write_deterministic_draft(
        out, {"genie_spaces": [{"genie_space_id": SPACE_ID}]}, mode="genie", group_names=[],
        catalog_schemas=[], api_genie_configs={"Champion Finance Analytics": {"title": "Champion Finance Analytics"}},
        genie_space_id_to_name={SPACE_ID: "Champion Finance Analytics"}, bootstrap_spaces=False,
    )
    assert tfvars == out / "abac.auto.tfvars" and tfvars.is_file()
    assert rit._load_tfvars(tfvars)["genie_space_id_to_name"] == {SPACE_ID: "Champion Finance Analytics"}
