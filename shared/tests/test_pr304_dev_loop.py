"""PR #304 review regressions for the ordinary deterministic dev loop."""

import json
import sys

import generate_abac
from scripts import sticky_governance as sticky
from tests.test_deterministic_generate import _CaptureGenerateEnv, TABLES


def _make_generate(fake):
    fake.monkeypatch.setattr(
        sys, "argv", ["generate_abac.py", "--auth-file", str(fake.env / "auth.auto.tfvars")]
    )
    assert generate_abac.main() in (None, 0)
    assert sticky.main([
        "record", "--env-dir", str(fake.env),
        "--config", str(fake.env / "generated/abac.auto.tfvars"),
    ]) == 0


def test_dev_rename_captured_agent_then_generate(tmp_path, monkeypatch):
    fake = _CaptureGenerateEnv(tmp_path, monkeypatch)
    fake.add("Alpha")
    fake.add("Beta")
    _make_generate(fake)
    fake.capture("Alpha")
    fake.capture("Beta")
    assert sticky.main([
        "record", "--env-dir", str(fake.env),
        "--config", str(fake.env / "generated/abac.auto.tfvars"),
    ]) == 0

    fake.rename("Alpha", "Alpha2")

    _make_generate(fake)
    assert set(json.loads((fake.env / "generated/governed_tables.json").read_text())["agent_tables"]) == {
        "Alpha2", "Beta",
    }


def test_dev_remove_agent_then_generate(tmp_path, monkeypatch):
    fake = _CaptureGenerateEnv(tmp_path, monkeypatch)
    fake.add("Alpha")
    fake.add("Beta")
    _make_generate(fake)

    fake.remove("Beta")

    _make_generate(fake)
    assert set(json.loads((fake.env / "generated/governed_tables.json").read_text())["agent_tables"]) == {
        "Alpha",
    }


def test_dev_new_agent_from_tables_add_table_then_generate(tmp_path, monkeypatch):
    fake = _CaptureGenerateEnv(tmp_path, monkeypatch)
    fake.add("Alpha")
    path = fake.env / "agents/Alpha.auto.tfvars"
    path.write_text(path.read_text() + f"uc_tables = {json.dumps(TABLES[:1])}\n")
    _make_generate(fake)

    path.write_text(path.read_text().replace(json.dumps(TABLES[:1]), json.dumps(TABLES)))

    _make_generate(fake)
    assert json.loads((fake.env / "generated/governed_tables.json").read_text())["agent_tables"] == {
        "Alpha": sorted(TABLES),
    }


def test_dev_wildcard_tables_generate_twice_then_rebuild(tmp_path, monkeypatch):
    fake = _CaptureGenerateEnv(tmp_path, monkeypatch)
    env_file = fake.env / "env.auto.tfvars"
    if "uc_catalog" not in env_file.read_text():
        env_file.write_text(env_file.read_text() + 'uc_catalog = "dev_fin"\n')
    fake.add("Alpha")
    path = fake.env / "agents/Alpha.auto.tfvars"
    path.write_text(path.read_text() + 'uc_tables = ["dev_fin.payments.*"]\n')

    _make_generate(fake)
    _make_generate(fake)
    assert sticky.main(["rebuild", "--env-dir", str(fake.env)]) == 0
    _make_generate(fake)

    record = json.loads((fake.env / "generated/governed_tables.json").read_text())
    assert record["agent_tables"] == {"Alpha": []}
