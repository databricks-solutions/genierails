"""make setup ENV=dev copies the walkthrough's env and agent templates (README Dev step 1).

Only files that don't exist yet are written: a re-run, an existing env or agent file and a
symlinked agents/ are left alone, and promoted envs (prod) get neither template.
"""

import os
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[2]
SHARED_ROOT = ROOT / "shared"
EXAMPLES = SHARED_ROOT / "examples" / "dev_to_prod"
AGENT_FILE = "Finance Analytics.auto.tfvars"
CLOUDS = ("aws", "azure")


def _make(cloud, tmp_path, *args):
    cloud_root = tmp_path / cloud
    cloud_root.mkdir(exist_ok=True)
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "_SEED_WALKTHROUGH")}
    return subprocess.run(
        ["make", "--no-print-directory", *args, f"CLOUD_ROOT={cloud_root}", f"SHARED_ROOT={SHARED_ROOT}"],
        cwd=ROOT / cloud, text=True, capture_output=True, env=env, stdin=subprocess.DEVNULL)


def _tree(root):
    """Every path under root: file bytes, symlink targets, and directories."""
    out = {}
    for path in sorted(root.rglob("*")):
        rel = str(path.relative_to(root))
        if path.is_symlink():
            out[rel] = ("link", os.readlink(path))
        elif path.is_dir():
            out[rel] = ("dir",)
        else:
            out[rel] = ("file", path.read_bytes(), path.stat().st_mtime_ns)
    return out


@pytest.mark.parametrize("cloud", CLOUDS)
@pytest.mark.parametrize("target", ["setup", "init-env"])
def test_setup_dev_copies_the_walkthrough_env_and_agent_templates(cloud, target, tmp_path):
    result = _make(cloud, tmp_path, target, "ENV=dev")

    assert result.returncode == 0, result.stdout + result.stderr
    dev = tmp_path / cloud / "envs/dev"
    assert (dev / "env.auto.tfvars").read_bytes() == (EXAMPLES / "env.auto.tfvars.example").read_bytes()
    assert (dev / "agents" / AGENT_FILE).read_bytes() == (EXAMPLES / "agent.auto.tfvars.example").read_bytes()
    assert sorted(p.name for p in (dev / "agents").iterdir()) == [AGENT_FILE]
    # The auth file still comes from the auth template, never from the walkthrough's files.
    auth = (dev / "auth.auto.tfvars").read_text()
    assert "databricks_client_secret" in auth and "genie_space_id" not in auth
    # The shared account layer keeps its own defaults.
    account = (tmp_path / cloud / "envs/account/env.auto.tfvars").read_text()
    assert "manage_groups = false" in account and "GenieRails Dev-to-Prod Walkthrough" not in account
    assert not (tmp_path / cloud / "envs/account/agents").exists()


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_dev_banner_names_the_copied_agent_file_and_no_cp_step(cloud, tmp_path):
    result = _make(cloud, tmp_path, "setup", "ENV=dev")

    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "cp ../shared/examples/dev_to_prod/env.auto.tfvars.example" not in out
    assert "cp ../shared/examples/dev_to_prod/agent.auto.tfvars.example" not in out
    assert "mkdir -p envs/dev/agents" not in out
    assert ("  2. Per agent, in its file in envs/dev/agents/ (the file name is the agent's name), set\n"
            "     genie_space_id to its Genie agent ID and acl_groups to the groups that may run it "
            "([] means nobody; required).\n"
            f'     Setup copied the agent template to "envs/dev/agents/{AGENT_FILE}": before the first\n'
            "     make generate, rename it to your agent's name; for another agent, copy it under that agent's name.\n"
            ) in out
    # Setup names the agent file's placeholders with the env file's, above the steps.
    advice = out[:out.index("Next steps")]
    assert f"<your-genie-space-id> in envs/dev/agents/{AGENT_FILE}" in advice
    assert f"<group-that-may-run-the-agent> in envs/dev/agents/{AGENT_FILE}" in advice


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_dev_rerun_is_a_no_op(cloud, tmp_path):
    assert _make(cloud, tmp_path, "setup", "ENV=dev").returncode == 0
    before = _tree(tmp_path / cloud / "envs")

    again = _make(cloud, tmp_path, "setup", "ENV=dev")

    assert again.returncode == 0, again.stderr
    assert _tree(tmp_path / cloud / "envs") == before


def test_setup_dev_rerun_after_renaming_or_removing_the_agent_file_adds_none(tmp_path):
    assert _make("aws", tmp_path, "setup", "ENV=dev").returncode == 0
    agents = tmp_path / "aws/envs/dev/agents"
    (agents / AGENT_FILE).rename(agents / "Sales.auto.tfvars")
    assert _make("aws", tmp_path, "setup", "ENV=dev").returncode == 0
    assert sorted(p.name for p in agents.iterdir()) == ["Sales.auto.tfvars"]

    (agents / "Sales.auto.tfvars").unlink()
    result = _make("aws", tmp_path, "setup", "ENV=dev")
    assert result.returncode == 0, result.stderr
    assert list(agents.iterdir()) == []
    # With no agent file the banner still shows how to create one.
    assert ('       cp ../shared/examples/dev_to_prod/agent.auto.tfvars.example '
            '"envs/dev/agents/<agent name>.auto.tfvars"\n') in result.stdout


def test_setup_dev_never_overwrites_an_existing_env_file_or_adds_an_agent_to_it(tmp_path):
    dev = tmp_path / "aws/envs/dev"
    dev.mkdir(parents=True)
    mine = 'governance_mode = "deterministic"\ngenie_spaces = []\n# mine\n'
    (dev / "env.auto.tfvars").write_text(mine)

    result = _make("aws", tmp_path, "setup", "ENV=dev")

    assert result.returncode == 0, result.stderr
    assert (dev / "env.auto.tfvars").read_text() == mine
    assert not (dev / "agents").exists()


def test_setup_dev_keeps_existing_agent_files_and_adds_none(tmp_path):
    agents = tmp_path / "aws/envs/dev/agents"
    agents.mkdir(parents=True)
    mine = 'genie_space_id = "01ef0000000000000000000000000000"\nacl_groups = ["viewers"]\n'
    (agents / "Sales.auto.tfvars").write_text(mine)

    result = _make("aws", tmp_path, "setup", "ENV=dev")

    assert result.returncode == 0, result.stderr
    assert sorted(p.name for p in agents.iterdir()) == ["Sales.auto.tfvars"]
    assert (agents / "Sales.auto.tfvars").read_text() == mine
    # The env file is new, so it still starts from the walkthrough template.
    assert ((tmp_path / "aws/envs/dev/env.auto.tfvars").read_bytes()
            == (EXAMPLES / "env.auto.tfvars.example").read_bytes())
    assert "Setup copied the agent template" not in result.stdout


def test_setup_dev_never_writes_through_a_symlinked_agents_dir(tmp_path):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    dev = tmp_path / "aws/envs/dev"
    dev.mkdir(parents=True)
    (dev / "agents").symlink_to(elsewhere, target_is_directory=True)

    result = _make("aws", tmp_path, "setup", "ENV=dev")

    assert result.returncode == 0, result.stdout + result.stderr
    assert list(elsewhere.iterdir()) == []
    assert (dev / "agents").is_symlink()
    assert "Not copying the agent template:" in result.stdout


def test_setup_dev_never_follows_a_dangling_agent_symlink(tmp_path):
    agents = tmp_path / "aws/envs/dev/agents"
    agents.mkdir(parents=True)
    target = tmp_path / "outside.auto.tfvars"
    (agents / AGENT_FILE).symlink_to(target)

    # init-env runs setup's copy without its banner (whose placeholder advice reads each agent file).
    result = _make("aws", tmp_path, "init-env", "ENV=dev")

    assert result.returncode == 0, result.stdout + result.stderr
    assert not target.exists()
    assert (agents / AGENT_FILE).is_symlink()
    assert sorted(p.name for p in agents.iterdir()) == [AGENT_FILE]


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_prod_after_dev_gets_no_walkthrough_template_and_no_agent_files(cloud, tmp_path):
    assert _make(cloud, tmp_path, "setup", "ENV=dev").returncode == 0
    dev_before = _tree(tmp_path / cloud / "envs/dev")

    result = _make(cloud, tmp_path, "setup", "ENV=prod")

    assert result.returncode == 0, result.stderr
    prod = tmp_path / cloud / "envs/prod"
    text = (prod / "env.auto.tfvars").read_text()
    # promote-to stays the source of prod's rules and agent files.
    assert text.startswith((SHARED_ROOT / "env.auto.tfvars.example").read_text())
    assert 'promote_from = "dev"' in text
    assert "GenieRails Dev-to-Prod Walkthrough" not in text
    assert not (prod / "agents").exists()
    assert "Setup copied the agent template" not in result.stdout
    assert _tree(tmp_path / cloud / "envs/dev") == dev_before

    again = _make(cloud, tmp_path, "setup", "ENV=prod")
    assert again.returncode == 0, again.stderr
    assert (prod / "env.auto.tfvars").read_text() == text
    assert not (prod / "agents").exists()


@pytest.mark.parametrize("env", ["stg", "bu2"])
def test_setup_other_envs_keep_the_generic_template(env, tmp_path):
    result = _make("aws", tmp_path, "setup", f"ENV={env}")

    assert result.returncode == 0, result.stderr
    target = tmp_path / "aws/envs" / env
    assert (target / "env.auto.tfvars").read_text().startswith((SHARED_ROOT / "env.auto.tfvars.example").read_text())
    assert not (target / "agents").exists()


def test_bootstrap_outside_setup_keeps_the_generic_template(tmp_path):
    """Only setup / init-env seed the walkthrough: other targets' bootstrap is unchanged."""
    cloud_root = tmp_path / "aws"
    cloud_root.mkdir()
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "_SEED_WALKTHROUGH")}
    result = subprocess.run(
        ["make", "--no-print-directory", "_bootstrap", "ENV=dev", f"CLOUD_ROOT={cloud_root}",
         f"SHARED_ROOT={SHARED_ROOT}"],
        cwd=ROOT / "aws", text=True, capture_output=True, env=env)

    assert result.returncode == 0, result.stdout + result.stderr
    assert ((cloud_root / "envs/dev/env.auto.tfvars").read_text()
            == (SHARED_ROOT / "env.auto.tfvars.example").read_text())
    assert not (cloud_root / "envs/dev/agents").exists()


def test_setup_fails_when_the_agent_template_cannot_be_copied(tmp_path):
    """A failed copy fails setup before the banner (|| exit 1), as every bootstrap step does."""
    import shutil

    shared = tmp_path / "shared-copy"
    shutil.copytree(SHARED_ROOT, shared, symlinks=True,
                    ignore=shutil.ignore_patterns("tests", "__pycache__", ".terraform"))
    (shared / "examples/dev_to_prod/agent.auto.tfvars.example").unlink()
    cloud_root = tmp_path / "aws"
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "_SEED_WALKTHROUGH")}

    result = subprocess.run(
        ["make", "--no-print-directory", "setup", "ENV=dev", f"CLOUD_ROOT={cloud_root}", f"SHARED_ROOT={shared}"],
        cwd=ROOT / "aws", text=True, capture_output=True, env=env)

    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert "Next steps" not in out and "environment ready" not in out
    # Nothing half-seeded: a re-run with the template back copies both files.
    assert not (cloud_root / "envs/dev/env.auto.tfvars").exists()
    assert not (cloud_root / "envs/dev/agents" / AGENT_FILE).exists()
