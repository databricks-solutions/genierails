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
        (env_dir / "auth.auto.tfvars").write_text('databricks_workspace_host = ""\n')
        (self.envs / "account").mkdir(exist_ok=True)
        return self._done(args)

    def _guard(self, args, env_dir):
        guard = subprocess.run(
            [sys.executable, str(SHARED / "genie_space_placeholder.py"),
             str(env_dir / "env.auto.tfvars")],
            capture_output=True, text=True,
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
        return self._done(args)

    def _enable_classification(self, args, env_dir):
        return self._guard(args, env_dir)

    def _rehearse(self, args, env_dir):
        assert _var(args, "VERIFY_KEY_COLUMN") == rit.CHAMPION_KEY_COLUMN
        return self._done(args, 0, "  ✓ [PASS] column-mask x\n  RESULT: ALL EFFECTIVE (1 passed / 1)\n")

    def _promote(self, args, env_dir):
        src = self.envs / _var(args, "SOURCE_ENV")
        dest = self.envs / _var(args, "DEST_ENV")
        for sub in ("generated", "data_access"):
            (dest / sub).mkdir(parents=True, exist_ok=True)
        remap = subprocess.run(
            [sys.executable, str(SHARED / "scripts" / "remap_env_config.py"),
             str(src), str(dest), _var(args, "DEST_CATALOG_MAP")],
            capture_output=True, text=True,
        )
        assert remap.returncode == 0, remap.stdout + remap.stderr
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
        ["rehearse", "ENV=dev", "VERIFY_KEY_COLUMN=customer_id"],
        ["promote", "SOURCE_ENV=dev", "DEST_ENV=prod", "DEST_CATALOG_MAP=dev_fin=prod_fin"],
        ["enable-classification", "ENV=prod"],
        ["enable-classification", "ENV=prod"],
        ["release", "ENV=prod", "VERIFY_KEY_COLUMN=customer_id"],
        ["maintain", "ENV=prod"],
    ]
    assert waits == ["dev_fin", "prod_fin"]
    assert teardown[:3] == ["_try_destroy", "_try_destroy", "_try_destroy_account"]

    dev_cfg = rit._load_tfvars(envs / "dev" / "env.auto.tfvars")
    assert dev_cfg["genie_spaces"] == [{"genie_space_id": SPACE_ID}]
    assert "uc_tables" not in dev_cfg
    assert dev_cfg["enable_auto_tagging"] is True
    assert dev_cfg["verify_key_column"] == "customer_id"
    prod_cfg = rit._load_tfvars(envs / "prod" / "env.auto.tfvars")
    # Promote and release neither write nor need the retired exposure flag.
    assert "business_access_enabled" not in prod_cfg
    assert prod_cfg["sql_warehouse_id"] == WAREHOUSE
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
