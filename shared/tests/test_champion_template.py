"""The champion README, run through the real make recipes with the shipped templates.

Only generate_abac.py (its Genie and Databricks reads) and the SDK's one Genie
GET that capture makes are stubbed. The env and agent templates are copied
as the README says and only their placeholders are filled, so a template that
omits a required setting (an agent's acl_groups, decision 4) fails here.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import hcl2
import pytest

SHARED = Path(__file__).resolve().parents[1]
EXAMPLES = SHARED / "examples" / "dev_to_prod"
SPACE_ID = "01champion0dev"
AGENT = "Finance Analytics"  # the README's agent file name
TABLES = ["dev_fin.finance.credit_cards", "dev_fin.finance.customers", "dev_fin.finance.transactions"]
DEPLOYER = "d0000000-0000-0000-0000-00000000d001"
TIERS = ["payments_ops", "viewers"]
GITIGNORE = ("auth.auto.tfvars\n*.tfstate*\n.terraform/\n.*.json\n!.genie_space_id\n"
             "classification.auto.tfvars\ndiscovered_uc_tables.auto.tfvars\n")

if shutil.which("make") is None or shutil.which("git") is None:
    pytest.skip("make and git are required", allow_module_level=True)

# generate_abac.py as a deterministic env runs it, minus the live reads: it
# discovers the agent's tables (the ignored discovery file) and writes generated/.
GENERATE_STUB = f"""import pathlib, sys
env = pathlib.Path.cwd()
tables = {TABLES!r}
rows = "".join(f'  "{{t}}",\\n' for t in tables)
owners = "".join(f'  "{{t}}" = ["{AGENT}"]\\n' for t in tables)
(env / "data_access" / "discovered_uc_tables.auto.tfvars").write_text(
    f"discovered_uc_tables = [\\n{{rows}}]\\ndiscovered_table_agents = {{{{\\n{{owners}}}}}}\\n")
(env / "generated" / "abac.auto.tfvars").write_text(
    'groups = {{ payments_ops = {{}}, viewers = {{}} }}\\ntag_policies = []\\ntag_assignments = []\\n'
    'genie_space_id_to_name = {{ "{SPACE_ID}" = "{AGENT}" }}\\n')
(env / "generated" / "masking_functions.sql").write_text("-- masks\\n")
"""
# capture's one read-only GET (scripts/agent_ownership.py) returns the agent.
PAYLOAD = {"version": 1, "data_sources": {"tables": [{"identifier": t} for t in TABLES]}}
FAKE_SDK = f"""import json
class _Api:
    def do(self, method, path, query=None, **_):
        assert method == "GET" and path.endswith("/{SPACE_ID}"), (method, path)
        return {{"space_id": "{SPACE_ID}", "serialized_space": json.dumps({PAYLOAD!r})}}
class WorkspaceClient:
    def __init__(self, **_):
        self.api_client = _Api()
"""


@pytest.fixture
def cloud(tmp_path):
    overlay = tmp_path / "shared"
    overlay.mkdir()
    for entry in SHARED.iterdir():
        if entry.name != "generate_abac.py":
            (overlay / entry.name).symlink_to(entry)
    (overlay / "generate_abac.py").write_text(GENERATE_STUB)
    sdk = tmp_path / "sdk" / "databricks" / "sdk"
    sdk.mkdir(parents=True)
    (sdk.parent / "__init__.py").write_text("")
    (sdk / "__init__.py").write_text(FAKE_SDK)
    root = tmp_path / "aws"
    root.mkdir()

    def make(*args, cwd_root=root, **extra):
        env = {k: v for k, v in os.environ.items()
               if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "MODE", "GENERATE_ARGS", "CI", "FROM", "CATALOG_MAP")}
        env.update(PYTHONPATH=str(tmp_path / "sdk"), PYTHONDONTWRITEBYTECODE="1", **extra)
        return subprocess.run(["make", "--no-print-directory", *args, f"CLOUD_ROOT={cwd_root}",
                               f"SHARED_ROOT={overlay}"],
                              cwd=SHARED.parent / "aws", text=True, capture_output=True, env=env, timeout=300)

    make.root = root
    return make


def _git(root, *args):
    return subprocess.run(["git", "-C", str(root), "-c", "user.email=t@example.com", "-c", "user.name=t", *args],
                          text=True, capture_output=True, check=True).stdout


def _commit(root, message):
    if not (root / ".git").exists():
        (root / ".gitignore").write_text(GITIGNORE)
        _git(root, "init", "-q")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "--allow-empty", "-m", message)


def _readme_dev_setup(cloud, acl_groups='["viewers"]'):
    """README Dev step 1: copy both templates; fill only their placeholders."""
    assert cloud("setup", "ENV=dev").returncode == 0
    dev = cloud.root / "envs" / "dev"
    shutil.copy(EXAMPLES / "env.auto.tfvars.example", dev / "env.auto.tfvars")
    (dev / "agents").mkdir()
    agent = dev / "agents" / f"{AGENT}.auto.tfvars"
    shutil.copy(EXAMPLES / "agent.auto.tfvars.example", agent)
    env_file = dev / "env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace("<your-deployer-principal-id>", DEPLOYER)
                        .replace('"<most-privileged-group>", "<least-privileged-group>"',
                                 ", ".join(json.dumps(t) for t in TIERS)))
    agent.write_text(agent.read_text().replace("<your-genie-space-id>", SPACE_ID)
                     .replace('["<group-that-may-run-the-agent>"]', acl_groups))
    (dev / "auth.auto.tfvars").write_text(f'databricks_client_id = "{DEPLOYER}"\n')
    return dev, agent


def _out(result):
    return result.stdout + result.stderr


def test_readme_dev_flow_generates_once_and_check_passes(cloud):
    dev, _agent = _readme_dev_setup(cloud)

    generated = cloud("generate", "ENV=dev")

    assert generated.returncode == 0, _out(generated)
    assert f"Captured: {AGENT} ({SPACE_ID})" in generated.stdout
    # The first run records the agent's tables (it captures before recording).
    record = json.loads((dev / "generated" / "governed_tables.json").read_text())
    assert record["agent_tables"] == {AGENT: TABLES}
    # table_readers proposed from acl_groups for each of the agent's tables.
    assert hcl2.loads((dev / "env.auto.tfvars").read_text())["table_readers"] == {t: ["viewers"] for t in TABLES}
    _commit(cloud.root, "dev")
    check = cloud("check", "ENV=dev", CI="true")
    assert "OK: generated files (envs/dev): committed files match." in _out(check), _out(check)
    # A second generate changes nothing that was committed.
    assert cloud("generate", "ENV=dev").returncode == 0
    assert _git(cloud.root, "status", "--porcelain") == ""


def test_readme_prod_flow_seeds_acl_groups_and_a_clean_clone_checks(cloud, tmp_path):
    dev, _agent = _readme_dev_setup(cloud)
    assert cloud("generate", "ENV=dev").returncode == 0
    # README Prod step 1: setup, fill its placeholders and catalog_map, promote-to.
    assert cloud("setup", "ENV=prod").returncode == 0
    prod = cloud.root / "envs" / "prod"
    env_file = prod / "env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace("<your-deployer-principal-id>", DEPLOYER)
                        .replace('"<most-privileged-group>", "<least-privileged-group>"',
                                 ", ".join(json.dumps(t) for t in TIERS))
                        .replace('{ "<dev_catalog>" = "<prod_catalog>" }', '{ "dev_fin" = "prod_fin" }'))

    promoted = cloud("promote-to", "ENV=prod")

    assert promoted.returncode == 0, _out(promoted)
    seeded = hcl2.loads((prod / "agents" / f"{AGENT}.auto.tfvars").read_text())
    assert seeded["acl_groups"] == ["viewers"] and seeded["genie_space_id"] == ""
    _commit(cloud.root, "promote")
    clone = tmp_path / "clone"
    subprocess.run(["git", "clone", "-q", str(cloud.root), str(clone)], check=True, capture_output=True)
    for target in (["promote-to", "ENV=prod", "CHECK=1"], ["check", "ENV=prod"]):
        result = cloud(*target, cwd_root=clone, CI="true")
        assert "OK: promote-to ENV=prod CHECK=1: committed files match." in _out(result), _out(result)


def _tree(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*"))
            if p.is_file() and ".git" not in p.parts}


def test_generate_refuses_a_deleted_committed_capture_and_writes_nothing(cloud):
    dev, _agent = _readme_dev_setup(cloud)
    assert cloud("generate", "ENV=dev").returncode == 0
    _commit(cloud.root, "dev")
    capture = dev / "agents" / f"{AGENT}.space.json"
    capture.unlink()
    before = _tree(cloud.root)

    result = cloud("generate", "ENV=dev")

    assert result.returncode != 0
    assert (f'agents/{AGENT}.space.json: restore it from git, or replace it with the live dev agent: '
            f'make capture ENV=dev SPACE="{AGENT}"') in result.stderr, _out(result)
    assert "Captured:" not in result.stdout
    assert _tree(cloud.root) == before  # generated/, the record and the capture: nothing written
    # make capture is the explicit way back; then generate runs again.
    assert cloud("capture", "ENV=dev", f"SPACE={AGENT}").returncode == 0
    assert cloud("generate", "ENV=dev").returncode == 0
    assert _git(cloud.root, "status", "--porcelain") == ""


def test_unfilled_acl_groups_is_refused_before_anything_runs(cloud):
    dev, _agent = _readme_dev_setup(cloud, acl_groups='["<group-that-may-run-the-agent>"]')

    result = cloud("generate", "ENV=dev")

    assert result.returncode != 0
    assert (f"replace placeholder <group-that-may-run-the-agent> in envs/dev/agents/{AGENT}.auto.tfvars "
            "with the groups that may run the agent") in result.stderr, _out(result)
    assert "===" not in result.stdout
    assert not (dev / "agents" / f"{AGENT}.space.json").exists()


def test_an_agent_without_acl_groups_still_stops_generate(cloud):
    # Decision 4 is unchanged: deleting the line is refused, not derived.
    dev, agent = _readme_dev_setup(cloud)
    agent.write_text("".join(line for line in agent.read_text().splitlines(keepends=True)
                             if not line.startswith("acl_groups")))

    result = cloud("generate", "ENV=dev")

    assert result.returncode != 0
    assert f"agent {AGENT} has no acl_groups: list the groups that may run it ([] means nobody)" in _out(result)
