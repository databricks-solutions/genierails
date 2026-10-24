"""make setup's customer-repo .gitignore (design section 9, rollout step 15).

The GenieRails repo ignores envs/ everywhere; a customer repo commits it. A
plain (auto) make setup writes the managed block into <cloud>/.gitignore in any
git checkout whose origin isn't github.com/databricks-solutions/genierails:
forks, internal mirrors and checkouts with no origin, even ones that keep a
GenieRails upstream remote. GenieRails' own repo (origin at GenieRails) and a
directory outside git get nothing but the opt-in hint; GITIGNORE=customer
forces the write and GITIGNORE=off skips it, and an existing block is always
refreshed. A real `git status --ignored` over a sample legacy + deterministic
env proves every runtime file is ignored and every committed file is tracked.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
SHARED = ROOT / "shared"
sys.path.insert(0, str(SHARED))

from scripts import customer_gitignore as cg  # noqa: E402
from scripts.release_gate import _is_input  # noqa: E402

CLOUDS = ("aws", "azure")
USER_LINES = "# my own ignores\n*.log\n!keep.log\n"


def _env():
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "GITIGNORE")}
    env["PIP_NO_INDEX"] = "1"
    return env


def _setup(cloud, cloud_root, *args):
    return subprocess.run(
        ["make", "--no-print-directory", "setup", *args, f"CLOUD_ROOT={cloud_root}", f"SHARED_ROOT={SHARED}"],
        cwd=ROOT / cloud, text=True, capture_output=True, env=_env())


@pytest.mark.parametrize("cloud", CLOUDS)
def test_upstream_gitignore_has_no_customer_block(cloud):
    text = (ROOT / cloud / ".gitignore").read_text()
    assert cg.BEGIN not in text and cg.END not in text
    # ...and the repo root still ignores envs/, so upstream never tracks an env.
    root = (ROOT / ".gitignore").read_text().splitlines()
    assert {"envs/", "generated/", "masking_functions.sql"} <= set(root)


@pytest.mark.parametrize("cloud", CLOUDS)
def test_plain_setup_outside_git_never_writes_the_gitignore(cloud, tmp_path):
    cloud_root = tmp_path / cloud
    cloud_root.mkdir()
    (cloud_root / ".gitignore").write_text(USER_LINES)
    result = _setup(cloud, cloud_root, "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    assert (cloud_root / ".gitignore").read_text() == USER_LINES
    # tmp_path is not a git checkout, so auto detects no customer repo and prints the hint.
    assert f"make setup ENV=dev writes {cloud}/.gitignore" in result.stdout
    assert "git add .gitignore envs/" in result.stdout and "GITIGNORE=customer" in result.stdout
    # No .gitignore at all: still nothing written.
    (cloud_root / ".gitignore").unlink()
    assert _setup(cloud, cloud_root, "ENV=dev").returncode == 0
    assert not (cloud_root / ".gitignore").exists()


@pytest.mark.parametrize("cloud", CLOUDS)
def test_customer_opt_in_is_sticky_idempotent_and_keeps_user_lines(cloud, tmp_path):
    cloud_root = tmp_path / cloud
    cloud_root.mkdir()
    gitignore = cloud_root / ".gitignore"
    gitignore.write_text(USER_LINES)

    first = _setup(cloud, cloud_root, "ENV=dev", "GITIGNORE=customer")
    assert first.returncode == 0, first.stdout + first.stderr
    text = gitignore.read_text()
    assert text.startswith(USER_LINES)
    assert text.count(cg.BEGIN) == 1 and text.endswith(cg.END + "\n")
    assert f"+++ b/{cloud}/.gitignore" in first.stdout and "+!/envs/" in first.stdout
    assert "/envs/dev/data_access/" in text  # new workspace envs are deterministic

    again = _setup(cloud, cloud_root, "ENV=dev", "GITIGNORE=customer")
    assert again.returncode == 0 and gitignore.read_text() == text
    assert f"{cloud}/.gitignore: up to date" in again.stdout and "+++" not in again.stdout

    # A user line added after the block survives a refresh, and a plain setup
    # (no GITIGNORE) refreshes the block once prod turns deterministic.
    gitignore.write_text(text + "my-notes.txt\n")
    assert _setup(cloud, cloud_root, "ENV=prod").returncode == 0
    env_file = cloud_root / "envs/prod/env.auto.tfvars"
    assert 'governance_mode       = "deterministic"' in env_file.read_text()
    refreshed = _setup(cloud, cloud_root, "ENV=prod")
    assert refreshed.returncode == 0, refreshed.stdout + refreshed.stderr
    new = gitignore.read_text()
    assert new.startswith(USER_LINES) and new.endswith("my-notes.txt\n")
    assert "/envs/prod/data_access/" in new
    assert f"{cloud}/.gitignore: up to date" in refreshed.stdout
    assert "/envs/account/abac.auto.tfvars" in new  # new dev is deterministic

    assert _setup(cloud, cloud_root, "ENV=prod", "GITIGNORE=off").returncode == 0
    assert gitignore.read_text() == new


def test_damaged_markers_and_bad_values_refuse_without_writing(tmp_path, capsys):
    gitignore = tmp_path / ".gitignore"
    gitignore.write_text(USER_LINES + cg.BEGIN + "\nenvs/\n")
    assert cg.main(["--cloud-dir", str(tmp_path), "--mode", "customer"]) == 1
    assert "markers are damaged" in capsys.readouterr().err
    assert gitignore.read_text() == USER_LINES + cg.BEGIN + "\nenvs/\n"
    assert cg.main(["--cloud-dir", str(tmp_path), "--mode", "yes"]) == 2
    assert "use customer, auto (the default) or off" in capsys.readouterr().err


# ── auto detects the repo kind from the origin remote ────────────────────────

UPSTREAM_HTTPS = "https://github.com/databricks-solutions/genierails.git"
UPSTREAM_SSH = "git@github.com:databricks-solutions/genierails.git"
UPSTREAM_HTTPS_BARE = "https://github.com/databricks-solutions/genierails"
UPSTREAM_SSH_URL = "ssh://git@github.com/databricks-solutions/genierails.git"
UPSTREAM_MIXED_CASE = "https://GitHub.com/Databricks-Solutions/GenieRails.git"
CUSTOMER_FORK = "https://github.com/acme-corp/genierails.git"
CUSTOMER_MIRROR = "git@gitlab.acme.internal:data/genierails.git"
FORK_NOTE = "pass GITIGNORE=off"


def _isolate_git(monkeypatch, ceiling: Path):
    # The machine's git config and any repo above the tmp dir stay out of detection.
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(ceiling))


def _git_init(path: Path, *remotes: tuple[str, str]):
    env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
    subprocess.run(["git", "init", "-q", str(path)], check=True, env=env)
    for name, url in remotes:
        subprocess.run(["git", "-C", str(path), "remote", "add", name, url], check=True, env=env)


def _assert_written(cloud: Path, out: str):
    text = (cloud / ".gitignore").read_text()
    assert text.count(cg.BEGIN) == 1 and text.endswith(cg.END + "\n")
    assert "+!/envs/" in out and "wrote the GenieRails block" in out


@pytest.mark.parametrize("url", [UPSTREAM_HTTPS, UPSTREAM_SSH, UPSTREAM_HTTPS_BARE, UPSTREAM_SSH_URL,
                                 UPSTREAM_MIXED_CASE])
def test_auto_leaves_genierails_own_repo_untouched(url, tmp_path, monkeypatch, capsys):
    cloud = tmp_path / "aws"
    _git_init(cloud, ("origin", url))
    _isolate_git(monkeypatch, tmp_path)
    assert cg.main(["--cloud-dir", str(cloud), "--mode", "auto"]) == 0
    assert not (cloud / ".gitignore").exists()
    assert "make setup ENV=dev writes aws/.gitignore" in capsys.readouterr().out


def test_auto_judges_by_origin_alone_when_origin_is_genierails(tmp_path, monkeypatch, capsys):
    # A GenieRails clone that also pushes to a personal remote is still GenieRails' own repo.
    cloud = tmp_path / "aws"
    _git_init(cloud, ("origin", UPSTREAM_HTTPS), ("mine", CUSTOMER_FORK))
    _isolate_git(monkeypatch, tmp_path)
    assert cg.main(["--cloud-dir", str(cloud), "--mode", "auto"]) == 0
    assert not (cloud / ".gitignore").exists()


@pytest.mark.parametrize("remotes", [
    (("origin", CUSTOMER_FORK), ("upstream", UPSTREAM_HTTPS)),
    (("origin", CUSTOMER_MIRROR), ("genierails", UPSTREAM_SSH)),
], ids=["fork-with-upstream", "mirror-with-genierails-remote"])
def test_auto_writes_in_a_customer_repo_that_keeps_a_genierails_remote(remotes, tmp_path, monkeypatch, capsys):
    cloud = tmp_path / "aws"
    _git_init(cloud, *remotes)
    _isolate_git(monkeypatch, tmp_path)
    (cloud / ".gitignore").write_text(USER_LINES)
    assert cg.main(["--cloud-dir", str(cloud), "--mode", "auto"]) == 0
    out = capsys.readouterr().out
    _assert_written(cloud, out)
    assert (cloud / ".gitignore").read_text().startswith(USER_LINES)
    assert FORK_NOTE not in out
    # Idempotent: a second run changes nothing.
    first = (cloud / ".gitignore").read_text()
    assert cg.main(["--cloud-dir", str(cloud), "--mode", "auto"]) == 0
    assert (cloud / ".gitignore").read_text() == first
    assert "up to date" in capsys.readouterr().out


def test_readme_customer_clone_with_renamed_upstream_prints_no_contributor_note(
    tmp_path, monkeypatch, capsys
):
    # README flow: clone GenieRails (so aws/.gitignore is tracked), retain that
    # remote as "genierails", and make the customer's repository the origin.
    cloud = tmp_path / "aws"
    _git_init(cloud, ("origin", UPSTREAM_HTTPS))
    _isolate_git(monkeypatch, tmp_path)
    (cloud / ".gitignore").write_text(USER_LINES)
    subprocess.run(["git", "-C", str(cloud), "add", ".gitignore"], check=True)
    subprocess.run(
        ["git", "-C", str(cloud), "-c", "user.name=Test", "-c", "user.email=test@example.com",
         "commit", "-qm", "upstream cloud gitignore"], check=True,
    )
    subprocess.run(["git", "-C", str(cloud), "remote", "rename", "origin", "genierails"], check=True)
    subprocess.run(["git", "-C", str(cloud), "remote", "add", "origin", CUSTOMER_FORK], check=True)

    assert cg.main(["--cloud-dir", str(cloud), "--mode", "auto"]) == 0
    out = capsys.readouterr().out
    _assert_written(cloud, out)
    assert FORK_NOTE not in out


@pytest.mark.parametrize("url", [
    UPSTREAM_HTTPS, UPSTREAM_SSH, UPSTREAM_HTTPS_BARE, UPSTREAM_SSH_URL, UPSTREAM_MIXED_CASE,
    "https://github.com/databricks-solutions/genierails/",
    "https://github.com/databricks-solutions/genierails.git/",
    "http://github.com/databricks-solutions/genierails.git",
    "https://user:token@github.com/databricks-solutions/genierails.git",
    "ssh://git@github.com:22/databricks-solutions/genierails.git",
])
def test_upstream_url_forms(url):
    assert cg._is_upstream(url)


EVIL_GITHUB_HOST = "https://evilgithub.com/databricks-solutions/genierails.git"
EVIL_PATH_EMBEDDED = "https://evil.com/github.com/databricks-solutions/genierails.git"


@pytest.mark.parametrize("url", [
    EVIL_GITHUB_HOST, EVIL_PATH_EMBEDDED,
    "https://github.com/databricks-solutions/genierails-fork.git",
    "https://github.com.evil.com/databricks-solutions/genierails.git",
    "https://github.com@evil.com/databricks-solutions/genierails.git",
    "ssh://git@github.com@evil.com/databricks-solutions/genierails.git",
    "git@evilgithub.com:databricks-solutions/genierails.git",
    "github.com@evil.com:databricks-solutions/genierails.git",
    "git@github.com.evil.com:databricks-solutions/genierails.git",
    "https://evil.com/databricks-solutions/genierails",
    "https://github.com/acme/databricks-solutions/genierails.git",
    "https://github.com/databricks-solutions/genierails.git?x=1",
    "file:///srv/github.com/databricks-solutions/genierails.git",
    "/srv/github.com/databricks-solutions/genierails.git",
    "",
])
def test_lookalike_urls_are_not_upstream(url):
    assert not cg._is_upstream(url)


@pytest.mark.parametrize("url", [EVIL_GITHUB_HOST, EVIL_PATH_EMBEDDED])
def test_auto_writes_when_origin_only_looks_like_genierails(url, tmp_path, monkeypatch, capsys):
    cloud = tmp_path / "aws"
    _git_init(cloud, ("origin", url))
    _isolate_git(monkeypatch, tmp_path)
    assert cg.main(["--cloud-dir", str(cloud), "--mode", "auto"]) == 0
    out = capsys.readouterr().out
    _assert_written(cloud, out)
    assert FORK_NOTE not in out


@pytest.mark.parametrize("remotes", [(("origin", CUSTOMER_FORK),), (("origin", CUSTOMER_MIRROR),), ()],
                         ids=["fork", "mirror", "no-remote"])
def test_auto_writes_in_a_customer_repo(remotes, tmp_path, monkeypatch, capsys):
    cloud = tmp_path / "aws"
    _git_init(cloud, *remotes)
    _isolate_git(monkeypatch, tmp_path)
    assert cg.main(["--cloud-dir", str(cloud), "--mode", "auto"]) == 0
    out = capsys.readouterr().out
    _assert_written(cloud, out)
    assert FORK_NOTE not in out  # no GenieRails remote, no note


def test_auto_does_not_write_outside_a_git_checkout(tmp_path, monkeypatch, capsys):
    cloud = tmp_path / "aws"
    cloud.mkdir()
    _isolate_git(monkeypatch, tmp_path)
    assert cg.main(["--cloud-dir", str(cloud), "--mode", "auto"]) == 0
    assert not (cloud / ".gitignore").exists()
    assert "make setup ENV=dev writes aws/.gitignore" in capsys.readouterr().out


def test_customer_and_off_overrides_win_over_detection(tmp_path, monkeypatch, capsys):
    # customer forces the write even in GenieRails' own repo; off never writes,
    # even in a customer fork auto would write in.
    clone = tmp_path / "aws"
    _git_init(clone, ("origin", UPSTREAM_HTTPS))
    _isolate_git(monkeypatch, tmp_path)
    assert cg.main(["--cloud-dir", str(clone), "--mode", "customer"]) == 0
    _assert_written(clone, out := capsys.readouterr().out)
    assert FORK_NOTE not in out

    fork = tmp_path / "azure"
    _git_init(fork, ("origin", CUSTOMER_FORK), ("upstream", UPSTREAM_HTTPS))
    (fork / ".gitignore").write_text(USER_LINES)
    assert cg.main(["--cloud-dir", str(fork), "--mode", "off"]) == 0
    assert (fork / ".gitignore").read_text() == USER_LINES
    assert capsys.readouterr().out == ""


def test_auto_always_refreshes_an_existing_block_regardless_of_remote(tmp_path, monkeypatch, capsys):
    # Even in GenieRails' own repo (where auto would not write a new block), an
    # existing block is always refreshed, so a stale block never lingers.
    cloud = tmp_path / "aws"
    _git_init(cloud, ("origin", UPSTREAM_HTTPS))
    _isolate_git(monkeypatch, tmp_path)
    (cloud / ".gitignore").write_text(USER_LINES + cg.BEGIN + "\nstale\n" + cg.END + "\n")
    assert cg.main(["--cloud-dir", str(cloud), "--mode", "auto"]) == 0
    text = (cloud / ".gitignore").read_text()
    assert text.startswith(USER_LINES) and "stale" not in text
    assert text.count(cg.BEGIN) == 1 and "!/envs/" in text


@pytest.mark.parametrize("pattern", [
    "auth.auto.tfvars", "*.tfstate", "*.tfstate.backup", "*.tfplan", ".terraform/", "__pycache__/",
    # every dot-file under envs/: Genie ID and adoption markers, locks,
    # generated/.runtime/, live snapshots, apply records, gate records
    "/envs/**/.*",
    "/envs/**/classification.auto.tfvars", "/envs/**/discovered_uc_tables.auto.tfvars",
    "/envs/**/ddl/_fetched.sql", "/envs/*/data_access/env.auto.tfvars", "/.genierails-state/",
])
def test_block_holds_every_secret_state_and_runtime_pattern(pattern, tmp_path):
    lines = cg.block(tmp_path).splitlines()
    assert pattern in lines
    # Ignores come after the re-includes, so no re-include can undo one.
    assert lines.index(pattern) > lines.index("!/envs/**/masking_functions.sql")


def test_block_lists_deterministic_layer_files_per_env(tmp_path):
    for env, mode in (("dev", "legacy"), ("prod", "deterministic")):
        (tmp_path / "envs" / env).mkdir(parents=True)
        (tmp_path / "envs" / env / "env.auto.tfvars").write_text(f'governance_mode = "{mode}"\n')
    lines = cg.block(tmp_path).splitlines()
    assert {"/envs/prod/abac.auto.tfvars", "/envs/prod/data_access/",
            "/envs/prod/generated/genie_space_derived_acl_groups.auto.tfvars"} <= set(lines)
    assert not any(line.startswith("/envs/dev/") for line in lines)
    assert "/envs/account/abac.auto.tfvars" not in lines  # legacy dev still commits it
    (tmp_path / "envs/dev/env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    assert "/envs/account/abac.auto.tfvars" in cg.block(tmp_path).splitlines()


# ── git status --ignored over a real sample env ──────────────────────────────

COMMITTED_ANY = [
    "env.auto.tfvars", "ack.txt", "agents/sales.auto.tfvars", "agents/sales.space.json",
    # Upstream's root .gitignore has *conf*.json; an agent capture is still committed.
    "agents/confidential_deals.auto.tfvars", "agents/confidential_deals.space.json",
    "generated/abac.auto.tfvars", "generated/masking_functions.sql", "generated/governed_tables.json",
    "generated/expected_classification.json", "generated/TUNING.md",
    "generated/spaces/sales/abac.auto.tfvars", "ddl/customers.sql",
]
LAYER_FILES = ["abac.auto.tfvars", "data_access/abac.auto.tfvars", "data_access/masking_functions.sql"]
RUNTIME_ANY = [
    "auth.auto.tfvars", "terraform.tfstate", "terraform.tfstate.backup", ".terraform.tfstate.lock.info",
    ".terraform/providers/registry.terraform.io/x", "workspace.tfplan",
    ".genie_space_id", ".genie_space_id_sales", ".genie_adopted_sales", ".genie_adopt_required_sales",
    ".workspace.apply.sha", ".can_run_check.json", ".agent_coverage.json", ".group_map_applied.json",
    "generated/.live_refresh.json", "generated/.governance.lock", "generated/.governance.guard",
    "generated/.runtime/abac.auto.tfvars", "generated/.verify_access.json",
    "generated/.verify_key_columns.saved.json", "generated/.history/1.json",
    "generated/.certified.json",
    "ddl/_fetched.sql", "classification.auto.tfvars", "discovered_uc_tables.auto.tfvars",
    "data_access/terraform.tfstate", "data_access/terraform.tfstate.backup",
    "data_access/.terraform/terraform.tfstate", "data_access/.data_access.apply.sha",
    "data_access/.coverage_gate.json", "data_access/.policy_generations/policies.json",
    "data_access/classification.auto.tfvars", "data_access/discovered_uc_tables.auto.tfvars",
    "__pycache__/x.pyc",
]
ACCOUNT_RUNTIME = ["terraform.tfstate", ".account.apply.sha", ".terraform/x"]


def _git(repo, *args):
    # The machine's own git config (global excludes, hooks) stays out of the sample repo.
    env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
    return subprocess.run(["git", *args], cwd=repo, text=True, capture_output=True, check=True, env=env).stdout


def _touch(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x\n")


def _status(repo):
    out = _git(repo, "status", "--porcelain", "--ignored", "--untracked-files=all")
    untracked = {line[3:] for line in out.splitlines() if line.startswith("?? ")}
    ignored = {line[3:].rstrip("/") for line in out.splitlines() if line.startswith("!! ")}
    return untracked, ignored


def _covered(path: str, ignored: set[str]) -> bool:
    return any(path == entry or path.startswith(entry + "/") for entry in ignored)


@pytest.mark.parametrize("cloud", CLOUDS)
@pytest.mark.parametrize("dev_mode", ["legacy", "deterministic"])
@pytest.mark.parametrize("root_gitignore", ["none", "upstream"])
def test_git_status_ignores_every_runtime_file_and_tracks_every_committed_one(
        cloud, dev_mode, root_gitignore, tmp_path):
    repo = tmp_path / "repo"
    cloud_root = repo / cloud
    cloud_root.mkdir(parents=True)
    # "none": no repo-root .gitignore, so the generated cloud .gitignore alone
    # must ignore every runtime file (upstream's root already ignores some of
    # them, e.g. auth.auto.tfvars, and would hide a gap in the block).
    # "upstream": the customer repo started as a copy of GenieRails, whose root
    # ignores envs/ and generated/; the block must re-include them.
    if root_gitignore == "upstream":
        (repo / ".gitignore").write_text((ROOT / ".gitignore").read_text())
    (cloud_root / ".gitignore").write_text((ROOT / cloud / ".gitignore").read_text())
    _git(repo, "init", "-q")
    envs = {"dev": dev_mode, "prod": "deterministic"}
    for env, mode in envs.items():
        assert _setup(cloud, cloud_root, f"ENV={env}").returncode == 0
        env_file = cloud_root / "envs" / env / "env.auto.tfvars"
        env_file.write_text(env_file.read_text().replace(
            'governance_mode       = "deterministic"', f'governance_mode       = "{mode}"'
        ))
    result = _setup(cloud, cloud_root, "ENV=prod", "GITIGNORE=customer")
    assert result.returncode == 0, result.stdout + result.stderr

    committed, runtime = [f"{cloud}/.gitignore"], []
    for env, mode in envs.items():
        base = f"{cloud}/envs/{env}"
        committed += [f"{base}/{f}" for f in COMMITTED_ANY]
        runtime += [f"{base}/{f}" for f in RUNTIME_ANY]
        (runtime if mode == "deterministic" else committed).extend(f"{base}/{f}" for f in LAYER_FILES)
        if mode == "deterministic":
            runtime.append(f"{base}/generated/genie_space_derived_acl_groups.auto.tfvars")
        # setup's symlinks: credentials and the data_access copy of env.auto.tfvars.
        runtime += [f"{base}/data_access/auth.auto.tfvars", f"{base}/data_access/env.auto.tfvars"]
    account = f"{cloud}/envs/account"
    committed.append(f"{account}/env.auto.tfvars")
    (runtime if dev_mode == "deterministic" else committed).append(f"{account}/abac.auto.tfvars")
    runtime += [f"{account}/{f}" for f in ACCOUNT_RUNTIME] + [f"{account}/auth.auto.tfvars"]
    runtime += [f"{cloud}/.genierails-state/dev/receipts/abc.json"]
    for rel in committed + runtime:
        if not (repo / rel).is_symlink() and not (repo / rel).exists():
            _touch(repo / rel)

    untracked, ignored = _status(repo)
    tracked_files = {p for p in untracked if p.startswith(f"{cloud}/")}
    assert tracked_files == set(committed), (
        f"not committable: {sorted(set(committed) - tracked_files)}; "
        f"leaked runtime files: {sorted(tracked_files - set(committed))}")
    assert all(_covered(rel, ignored) for rel in runtime), sorted(r for r in runtime if not _covered(r, ignored))

    # The deterministic env agrees exactly with release_gate's definition of a
    # committed input (what rehearse fingerprints): ignored iff not an input.
    prod = cloud_root / "envs/prod"
    for path in sorted(prod.rglob("*")):
        if path.is_file() and not path.is_symlink():
            rel = path.relative_to(repo).as_posix()
            assert _covered(rel, ignored) != _is_input(path.relative_to(prod)), rel

    # git add -A (what a user would run) stages no runtime file.
    _git(repo, "add", "-A")
    staged = set(_git(repo, "diff", "--cached", "--name-only").splitlines())
    assert not staged & set(runtime)


@pytest.mark.parametrize("cloud", CLOUDS)
def test_an_agent_capture_named_like_conf_is_committed_even_with_ignorecase(cloud, tmp_path):
    """macOS checkouts set core.ignorecase, so the root *conf*.json matches "Confidential"."""
    repo = tmp_path / "repo"
    cloud_root = repo / cloud
    cloud_root.mkdir(parents=True)
    (repo / ".gitignore").write_text((ROOT / ".gitignore").read_text())
    (cloud_root / ".gitignore").write_text((ROOT / cloud / ".gitignore").read_text())
    _git(repo, "init", "-q")
    _git(repo, "config", "core.ignorecase", "true")
    assert _setup(cloud, cloud_root, "ENV=dev").returncode == 0
    result = _setup(cloud, cloud_root, "ENV=dev", "GITIGNORE=customer")
    assert result.returncode == 0, result.stdout + result.stderr
    agents = cloud_root / "envs" / "dev" / "agents"
    for name in ("Confidential Deals.space.json", "Confidential Deals.auto.tfvars"):
        _touch(agents / name)
    # Other *conf*.json files stay ignored.
    _touch(cloud_root / "envs" / "dev" / "app_config.json")

    def ignored(rel):
        env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
        return subprocess.run(["git", "check-ignore", "-q", rel], cwd=repo, env=env).returncode == 0

    assert not ignored(f"{cloud}/envs/dev/agents/Confidential Deals.space.json")
    assert not ignored(f"{cloud}/envs/dev/agents/Confidential Deals.auto.tfvars")
    assert ignored(f"{cloud}/envs/dev/app_config.json")
