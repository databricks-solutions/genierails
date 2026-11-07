"""An envs/<env>/agents/ entry that is not a regular file is refused with one clear message.

A dangling, looping or directory-target link named <agent>.auto.tfvars or
<agent>.space.json (or a dangling agents/ itself) used to crash make setup, and
every guard behind genie_space_placeholder.py, with a Python traceback; the
shared loader (agent_files.names) read it as an agent with no settings. A real
directory with such a name crashed the same way (IsADirectoryError), and a FIFO
made setup hang reading it. Now setup, init-env and every command that scans
agents/ refuse it before writing anything, naming the path, and never open it;
the entry is left exactly as it was. Every make call has a timeout, so a FIFO
regression fails instead of hanging.
"""

import os
import signal
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
SHARED_ROOT = ROOT / "shared"
sys.path.insert(0, str(SHARED_ROOT))

import agent_files  # noqa: E402
from genie_space_placeholder import agent_files_placeholder_errors  # noqa: E402

MESSAGE = "fix or remove it, then run the command again"
TIMEOUT = 120  # seconds; refusals take a few. A FIFO that is opened blocks forever.


def _make(cloud_root: Path, *args, ci: bool | None = None):
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "_SEED_WALKTHROUGH")}
    if ci is not None:
        env["CI"] = "true" if ci else ""
    proc = subprocess.Popen(
        ["make", "--no-print-directory", *args, f"CLOUD_ROOT={cloud_root}", f"SHARED_ROOT={SHARED_ROOT}"],
        cwd=ROOT / cloud_root.name, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
        stdin=subprocess.DEVNULL, start_new_session=True)
    try:
        out, err = proc.communicate(timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)  # make and the python blocked on the FIFO
        out, err = proc.communicate()
        pytest.fail(f"make {' '.join(args)} hung for {TIMEOUT}s (opened a FIFO?):\n{out}{err}")
    return subprocess.CompletedProcess(proc.args, proc.returncode, out, err)


def _tree(root: Path) -> dict:
    """Every path under root by lstat: symlinks by target, FIFOs by kind (never followed or opened)."""
    out = {}
    for dirpath, dirnames, filenames in os.walk(root):
        for name in dirnames + filenames:
            path = Path(dirpath) / name
            rel = str(path.relative_to(root))
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                out[rel] = ("link", os.readlink(path))
            elif stat.S_ISDIR(mode):
                out[rel] = ("dir",)
            elif stat.S_ISREG(mode):
                out[rel] = ("file", path.read_bytes())
            else:
                out[rel] = ("special", stat.S_IFMT(mode))
    return out


def _break(agents: Path, kind: str, outside: Path) -> Path:
    """Make one bad entry of ``kind`` in (or as) ``agents``; return it."""
    if kind == "agents-dir":
        agents.parent.mkdir(parents=True, exist_ok=True)
        agents.symlink_to(outside, target_is_directory=True)
        return agents
    if kind == "agents-file":
        agents.parent.mkdir(parents=True, exist_ok=True)
        agents.write_text("not a directory\n")
        return agents
    agents.mkdir(parents=True, exist_ok=True)
    if kind == "dangling-settings":
        link = agents / "Sales.auto.tfvars"
        link.symlink_to(outside)
    elif kind == "dangling-capture":
        link = agents / "Sales.space.json"
        link.symlink_to(outside)
    elif kind == "loop":
        link = agents / "Self.auto.tfvars"
        link.symlink_to("Self.auto.tfvars")
    elif kind == "directory":
        (agents / "real.d").mkdir()
        link = agents / "Dir.auto.tfvars"
        link.symlink_to("real.d", target_is_directory=True)
    elif kind == "directory-entry":
        link = agents / "Dir.auto.tfvars"
        link.mkdir()
    elif kind == "fifo-settings":
        link = agents / "Pipe.auto.tfvars"
        os.mkfifo(link)
    elif kind == "fifo-capture":
        link = agents / "Pipe.space.json"
        os.mkfifo(link)
    else:
        raise AssertionError(kind)
    return link


KINDS = ("dangling-settings", "dangling-capture", "loop", "directory", "agents-dir",
         "directory-entry", "fifo-settings", "fifo-capture", "agents-file")
# The bad entries a command meets while it reads agents/ (each scanner path differs).
COMMAND_KINDS = ("dangling-settings", "directory-entry", "fifo-settings")


def _assert_refused(result, link: Path):
    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert "Traceback" not in out, out
    lines = [line for line in out.splitlines() if MESSAGE in line]
    assert len(lines) == 1, out
    assert lines[0].startswith("ERROR: ") and "fix or remove it" in lines[0], out
    rel = f"envs/{link.parent.parent.name}/agents/{link.name}" if link.name != "agents" else \
        f"envs/{link.parent.name}/agents"
    assert rel in lines[0], out


# ── make setup / init-env ──────────────────────────────────────────────────

@pytest.mark.parametrize("cloud", ("aws", "azure"))
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("target", ("setup", "init-env"))
def test_setup_refuses_a_broken_agent_symlink_and_writes_nothing(cloud, kind, target, tmp_path):
    cloud_root = tmp_path / cloud
    outside = tmp_path / "outside"
    link = _break(cloud_root / "envs/dev/agents", kind, outside)
    before = _tree(tmp_path)

    result = _make(cloud_root, target, "ENV=dev")

    _assert_refused(result, link)
    assert _tree(tmp_path) == before  # no env, auth, account env, template copy or write through the link
    assert not outside.exists()
    assert os.path.lexists(link)


def test_setup_with_a_valid_symlinked_agent_file_is_unchanged(tmp_path):
    """Only broken links are refused: a link to a real agent file is read as before."""
    cloud_root = tmp_path / "aws"
    agents = cloud_root / "envs/dev/agents"
    agents.mkdir(parents=True)
    real = tmp_path / "Sales.auto.tfvars"
    real.write_text('genie_space_id = "abc"\nacl_groups = ["analysts"]\n')
    (agents / "Sales.auto.tfvars").symlink_to(real)  # outside the repo
    (cloud_root / "envs/dev/shared-agent.tfvars").write_text('genie_space_id = "def"\nacl_groups = ["ops"]\n')
    (agents / "Ops.auto.tfvars").symlink_to("../shared-agent.tfvars")  # inside it, relative

    result = _make(cloud_root, "setup", "ENV=dev")

    assert result.returncode == 0, result.stdout + result.stderr
    assert MESSAGE not in result.stdout + result.stderr
    assert sorted(p.name for p in agents.iterdir()) == ["Ops.auto.tfvars", "Sales.auto.tfvars"]
    assert (agents / "Sales.auto.tfvars").is_symlink() and (agents / "Ops.auto.tfvars").is_symlink()
    assert agent_files.names(cloud_root / "envs/dev") == ["Ops", "Sales"]
    assert real.read_text() == 'genie_space_id = "abc"\nacl_groups = ["analysts"]\n'


def test_setup_rerun_after_removing_the_broken_link_seeds_as_usual(tmp_path):
    cloud_root = tmp_path / "aws"
    link = _break(cloud_root / "envs/dev/agents", "dangling-settings", tmp_path / "outside")
    assert _make(cloud_root, "setup", "ENV=dev").returncode != 0
    link.unlink()

    result = _make(cloud_root, "setup", "ENV=dev")

    assert result.returncode == 0, result.stdout + result.stderr
    assert sorted(p.name for p in link.parent.iterdir()) == ["my_agent.auto.tfvars"]


# ── every other command that scans agents/ ────────────────────────────────

def _envs(cloud_root: Path, broken_env: str, kind: str, outside: Path) -> Path:
    dev, prod = cloud_root / "envs/dev", cloud_root / "envs/prod"
    for env in (dev, prod):
        (env / "agents").mkdir(parents=True)
    (dev / "env.auto.tfvars").write_text('governance_mode = "deterministic"\nuc_tables = ["c.s.t"]\n')
    (prod / "env.auto.tfvars").write_text(
        'governance_mode = "deterministic"\npromote_from = "dev"\ncatalog_map = { "c" = "p" }\n')
    agents = cloud_root / f"envs/{broken_env}/agents"
    if kind == "agents-dir":
        agents.rmdir()
    return _break(agents, kind, outside)


COMMANDS = [
    ("generate", "dev", ("generate", "ENV=dev")),
    ("validate", "dev", ("validate", "ENV=dev")),
    ("plan", "dev", ("plan", "ENV=dev")),
    ("check", "dev", ("check", "ENV=dev")),
    ("promote-to source", "dev", ("promote-to", "ENV=prod")),
    ("promote-to destination", "prod", ("promote-to", "ENV=prod")),
]


@pytest.mark.parametrize("ci", (False, True), ids=("local", "ci"))
@pytest.mark.parametrize("kind", COMMAND_KINDS)
@pytest.mark.parametrize("label,broken_env,args", COMMANDS, ids=[c[0] for c in COMMANDS])
def test_agent_scanning_commands_refuse_a_bad_entry_without_a_traceback(label, broken_env, args, kind, ci, tmp_path):
    cloud_root = tmp_path / "aws"
    outside = tmp_path / "outside"
    link = _envs(cloud_root, broken_env, kind, outside)
    before = _tree(cloud_root / "envs")

    result = _make(cloud_root, *args, ci=ci)

    _assert_refused(result, link)
    assert not outside.exists() and os.path.lexists(link)
    after = _tree(cloud_root / "envs")
    assert {k: v for k, v in after.items() if k.startswith(f"{broken_env}/agents")} == \
        {k: v for k, v in before.items() if k.startswith(f"{broken_env}/agents")}


@pytest.mark.parametrize("kind", ("loop", "directory-entry", "fifo-settings"))
@pytest.mark.parametrize("target", ("status", "next"))
def test_status_and_next_report_without_a_traceback(target, kind, tmp_path):
    cloud_root = tmp_path / "aws"
    _envs(cloud_root, "dev", kind, tmp_path / "outside")

    result = _make(cloud_root, target, "ENV=dev")

    assert "Traceback" not in result.stdout + result.stderr, result.stdout + result.stderr
    assert result.returncode == 0, result.stdout + result.stderr


# ── the shared loader and scanners ────────────────────────────────────────

@pytest.mark.parametrize("kind", KINDS)
def test_agent_files_loader_refuses_instead_of_reading_an_empty_agent(kind, tmp_path):
    env_dir = tmp_path / "envs/dev"
    env_dir.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    if kind not in ("agents-dir", "agents-file"):
        (env_dir / "agents").mkdir()
        (env_dir / "agents/Real.auto.tfvars").write_text('acl_groups = ["a"]\n')
    outside = tmp_path / "outside"
    link = _break(env_dir / "agents", kind, outside)

    for read in (agent_files.names, agent_files.load_all, agent_files.merged_config,
                 agent_files.refuse_bad_entries):
        with pytest.raises(agent_files.AgentFileError, match=MESSAGE) as caught:
            read(env_dir)
        assert str(link) in str(caught.value)
    assert agent_files.main(["check-entries", "--env-dir", str(env_dir)]) == 1
    errors = agent_files_placeholder_errors(env_dir)
    assert len(errors) == 1 and MESSAGE in errors[0]
    assert ("envs/dev/agents" + ("" if kind.startswith("agents-") else f"/{link.name}")) in errors[0]
    assert not outside.exists() and os.path.lexists(link)


def test_check_links_passes_a_normal_env_and_ignores_other_names(tmp_path):
    env_dir = tmp_path / "envs/dev"
    (env_dir / "agents").mkdir(parents=True)
    (env_dir / "agents/Sales.auto.tfvars").write_text('acl_groups = ["a"]\n')
    (env_dir / "agents/notes.md").symlink_to(tmp_path / "missing")  # not an agent or capture file
    os.mkfifo(env_dir / "agents/pipe.txt")  # nor is this
    (env_dir / "agents/Linked.auto.tfvars").symlink_to(tmp_path / "Linked.auto.tfvars")  # valid, outside the env
    (tmp_path / "Linked.auto.tfvars").write_text('acl_groups = ["b"]\n')
    assert agent_files.bad_entries(env_dir) == []
    assert agent_files.main(["check-entries", "--env-dir", str(env_dir)]) == 0
    assert agent_files.main(["check-entries", "--env-dir", str(tmp_path / "envs/none")]) == 0
    assert agent_files.names(env_dir) == ["Linked", "Sales"]


def test_promote_never_writes_through_a_broken_destination_link(tmp_path, capsys):
    sys.path.insert(0, str(SHARED_ROOT / "scripts"))
    import agent_promote

    dev, prod = tmp_path / "envs/dev", tmp_path / "envs/prod"
    (dev / "agents").mkdir(parents=True)
    (prod / "agents").mkdir(parents=True)
    (dev / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    (prod / "env.auto.tfvars").write_text('governance_mode = "deterministic"\npromote_from = "dev"\n')
    (dev / "agents/Sales.auto.tfvars").write_text('genie_space_id = "abc"\nacl_groups = ["a"]\n')
    (dev / "agents/Sales.space.json").write_text('{"data_sources": {"tables": [{"identifier": "c.s.t"}]}}\n')
    outside = tmp_path / "outside"
    (prod / "agents/Sales.auto.tfvars").symlink_to(outside)
    before = _tree(prod)

    rc = agent_promote.main(["--source-dir", str(dev), "--dest-dir", str(prod), "--catalog-map", "c=p"])

    err = capsys.readouterr().err
    assert rc == 1 and MESSAGE in err and str(prod / "agents/Sales.auto.tfvars") in err
    assert _tree(prod) == before  # not even the capture beside it
    assert not outside.exists()
