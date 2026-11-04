"""PR #304 regression: promoted agent tables reach every footprint consumer."""

import importlib.util
import json

import hcl2

from tests import test_deterministic_check_promote as base
from tests import test_persona_files as persona


SPEC = importlib.util.spec_from_file_location(
    "derive_pr304_consumers", persona.SHARED / "scripts/derive_assignments.py"
)
DERIVE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(DERIVE)


def _consumer_sets(env):
    import evidence_report
    import table_readers
    from agent_files import merged_config
    from scripts import agent_coverage, external_mask_inventory, release_gate
    from scripts.footprint import resolve_footprint
    from scripts.sticky_governance import load_governed_tables

    raw = hcl2.loads((env / "env.auto.tfvars").read_text())
    spaces = merged_config(env).get("genie_spaces") or []
    agents = {
        space.get("name") or space.get("genie_space_id"): {
            "name": space.get("name"),
            "genie_space_id": space.get("genie_space_id"),
            "tables": space.get("uc_tables"),
        }
        for space in spaces
    }
    return {
        "sticky": load_governed_tables(env),
        "resolved": resolve_footprint(env),
        "derive": DERIVE._governed_footprint(
            env / "auth.auto.tfvars", env / "env.auto.tfvars"
        )[2],
        "readers": table_readers.governed_tables(env, raw),
        "evidence": evidence_report.configured_tables(env),
        "external masks": external_mask_inventory.governed_columns_and_tables(env)[1],
        "coverage/status": [
            table
            for agent in agent_coverage.agent_tables(env, agents).values()
            for table in agent["tables"]
        ],
        "release gate": list(release_gate._desired(env)["table_readers"]),
    }


def test_promoted_agent_tables_reach_every_footprint_consumer(tmp_path, monkeypatch):
    repo = persona._agent_repo(tmp_path, monkeypatch)
    prod = base._envs(repo) / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text(persona.PROD_ENV + 'enable_classification = true\n')
    (prod / "auth.auto.tfvars").write_text(base._auth(base.DEPLOYER))
    result = base._make(repo, tmp_path, "promote-to", "ENV=prod")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads((prod / "generated/governed_tables.json").read_text()) == {
        "tables": [], "agent_tables": {"Payments": ["ppay.s.t"]},
    }

    for consumer, tables in _consumer_sets(prod).items():
        assert "ppay.s.t" in tables, f"{consumer} missed the promoted agent table: {tables}"
